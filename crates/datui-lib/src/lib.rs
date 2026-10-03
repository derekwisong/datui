use color_eyre::Result;
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use polars::datatypes::DataType;
#[cfg(feature = "cloud")]
use polars::io::cloud::{AmazonS3ConfigKey, CloudOptions};
use polars::prelude::{DataFrame, LazyFrame, Schema, col};
#[cfg(feature = "cloud")]
use polars::prelude::{PlRefPath, ScanArgsParquet};
use std::collections::HashMap;

/// Rows measured per background pass. Small enough that a slow filesystem shows
/// progress rather than a long silence.
const MEASURE_BATCH: usize = 12;

/// Rows a probe measures while it is already reading a remote directory.
const PROBE_MEASURE_LIMIT: usize = 24;

/// Rows one classification pass looks into.
///
/// A cap on work in flight rather than a budget spent per directory: what gets looked
/// into is what is on screen, and the next pass is chosen from the viewport as it is
/// when the previous one lands. Sized like [`MEASURE_BATCH`], for the same reason —
/// on a share that answers in milliseconds per row, a screenful arriving in pieces
/// reads as filling in, and one long silence reads as broken.
const CLASSIFY_BATCH: usize = 16;

/// Probes allowed at once. A probe of a share that has gone away holds its thread
/// until the process exits, so the number of them has to be bounded.
const MAX_CONCURRENT_PROBES: usize = 4;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, mpsc::Sender};
use widgets::info::{FileFacts, InfoModal};

use ratatui::style::{Color, Style};
use ratatui::{buffer::Buffer, layout::Rect, widgets::Widget};

use ratatui::widgets::{Block, Clear};

mod analysis_keys;
pub mod analysis_modal;
pub mod audio;
pub mod avro_types;
#[cfg(feature = "cloud")]
pub mod aws_profiles;
#[cfg(feature = "cloud")]
pub mod azure;
mod background;
pub mod cache;
pub mod candump;
pub mod canonical;
pub mod chart_data;
pub mod chart_export;
pub mod chart_export_modal;
mod chart_jobs;
mod chart_keys;
pub mod chart_modal;
pub mod cli;
pub mod clipboard;
#[cfg(feature = "cloud")]
mod cloud_arrow;
#[cfg(feature = "cloud")]
pub mod cloud_browse;
#[cfg(feature = "cloud")]
pub mod cloud_command;
pub mod cloud_env;
#[cfg(feature = "cloud")]
mod cloud_hive;
#[cfg(feature = "cloud")]
pub mod cloud_sources;
pub mod config;
mod copy_keys;
pub mod copy_modal;
pub mod csv_dialect;
pub mod data_quality;
pub mod dataflash;
pub mod dbc;
pub mod delimited_spec;
pub mod discover;
pub mod distribution_fit;
pub mod download;
mod editing_keys;
pub mod elf;
pub mod error_display;
pub mod event_pump;
pub mod exact;
pub mod export;
mod export_keys;
pub mod export_modal;
pub mod external_open;
mod feedback;
pub mod filter_modal;
pub mod find;
mod first_rows_trace;
pub mod fix;
pub mod fixed_records;
pub mod follow;
pub mod formats;
pub mod framed_records;
pub mod fuzzy;
#[cfg(feature = "cloud")]
pub mod gcloud;
pub mod glyphs;
pub mod gps;
pub(crate) mod help_strings;
mod hex_keys;
pub mod hex_view;
pub mod hf_splits;
pub mod home;
pub mod home_preview;
pub mod indexed;
mod info_keys;
pub mod inspector_bytes;
pub mod inspector_drill;
pub mod inspector_modal;
pub mod inspector_reader;
pub mod intent_modal;
pub mod ipc_stream;
mod jobs;
mod loading;
pub mod local_copy;
pub mod locality;
pub mod logging;
pub mod measurements;
pub mod members;
pub mod midi;
pub mod model_files;
pub mod nested_json;
pub mod notes;
pub mod numfmt;
pub mod numpy;
mod open_options;
pub mod output_file;
pub mod past_calendar;
mod pivot_melt_keys;
pub mod pivot_melt_modal;
pub mod pointer;
pub mod pushdown;
pub mod python_script;
pub mod quality_export;
pub mod quality_intent;
mod quality_memory;
pub mod quality_report;
pub mod quality_trends;
#[cfg(any(feature = "http", feature = "cloud"))]
mod remote_model;
pub mod row_index;
#[cfg(feature = "cloud")]
pub mod s3_tools;
pub mod sample_modal;
pub mod sampling;
// Public so the fuzz targets in `fuzz/` can reach `parse_query`. The parser is
// hand-written and runs on whatever the user types, so it is fuzzed directly.
pub mod query;
mod readers;
mod render;
pub mod sanitize;
mod scan;
pub mod schema_union;
pub mod sdf;
pub mod search;
pub(crate) mod segments;
mod sort_filter_keys;
pub mod sort_filter_modal;
pub mod sort_modal;
pub mod source;
mod sql_assist;
pub mod sqlite;
// Public so the fuzz target `sql_group_plan` can reach `plan`, which reads every SQL
// statement the prompt runs.
#[cfg(feature = "sql")]
pub mod sql_group;
pub mod startup;
pub mod statistics;
pub mod stdin;
pub mod tee;
pub mod template;
mod template_keys;
mod terminal;
pub mod terminal_input;
pub mod text_formats;
pub mod ulog;
mod unfinished;
pub mod value_counts;
pub mod value_counts_modal;
pub mod vcd;
pub mod widgets;

pub use cache::CacheManager;
pub use cli::Args;
pub use config::{
    AppConfig, ColorParser, ConfigManager, QueryMode, Theme, rgb_to_256_color, rgb_to_basic_ansi,
};

use analysis_modal::{AnalysisModal, AnalysisProgress};
use background::{InflightCollect, LenCount, OwedAnswer, OwedCount};
use chart_export::{
    BoxPlotExportBounds, ChartExportBounds, ChartExportFormat, ChartExportRequest,
    ChartExportSeries,
};
use chart_export_modal::{ChartExportFocus, ChartExportModal};
use chart_jobs::{
    ChartCache, ChartExportJob, ChartInflight, ChartPrepared, ChartRequest, ChartResultSlot,
    log_series,
};
use chart_modal::{ChartColumns, ChartKind, ChartModal, ChartType};
pub use error_display::{ErrorKindForPython, error_for_python};
pub use export::{ExportOptions, ExportRequest};
use export_modal::{ExportFocus, ExportFormat, ExportModal};
pub use feedback::{ConfirmationModal, ErrorModal, Flash};
use filter_modal::FilterStatement;
use jobs::{Answer, Job, Jobs, Outcome};
pub use jobs::{JobKind, Progress, Ticket};
use numfmt::NumberFormatSettings;
pub use open_options::{
    OpenOptions, ParseStringsTarget, ReadReport, SqliteOpen, TypedDialect, UnaskedDownload,
};
use output_file::Overwrite;
use pivot_melt_modal::{MeltSpec, PivotMeltModal, PivotSpec};
pub use quality_memory::{KeptQualitySample, QUALITY_MEMORY_BUDGET, RetainedCopy};
use quality_memory::{QUALITY_RELEASED_REMEMBERED, QualityCacheEntry, QualityCopyJob};
use scan::Scan;
use sort_filter_modal::{SortFilterFocus, SortFilterModal, SortFilterTab};
use sort_modal::{SortColumn, SortFocus, order_with_hidden};
pub use template::{Template, TemplateManager, Templates};
use terminal::{QuietTerminal, TakenTerminal, push_keyboard_flags, restore_terminal};
pub use unfinished::ExitSweep;
use widgets::column_widths::WidthChoice;
use widgets::controls::Controls;
use widgets::datatable::{DataTableState, DatasetAtOpen, DrillRow, OpenFacts};
use widgets::debug::DebugState;
use widgets::template_modal::{FormFocus, TemplateModal, TemplateModalMode, ViewRow};
use widgets::text_input::TextInput;

/// Application name used for cache directory and other app-specific paths
pub const APP_NAME: &str = "datui";

/// What a file no reader takes, and no hex view can show, is told.
pub(crate) const UNSUPPORTED: &str =
    "Unsupported file type. --format names the format to read it as.";

/// Re-export compression format and file format from CLI module
pub use cli::{CompressionFormat, FileFormat, ReadMode, RemoteRead, Stored};

#[cfg(test)]
mod text_input_flows;

#[cfg(test)]
pub mod tests;

pub enum AppEvent {
    Key(KeyEvent),
    /// Read from the terminal by [`terminal_input::TerminalInput`]: a key press or a
    /// resize. [`event_pump::EventPump`] takes it off the channel and decides what a
    /// key does while the app is busy; the app itself only ever sees `Key`/`Resize`.
    Terminal(crossterm::event::Event),
    /// Something polled rather than sent changed (a background panic, a Polars
    /// warning): the loop should look. Handled as nothing.
    Wake,
    /// The settings `run` reads on a worker before it can build the app. Never reaches
    /// the app: `run` waits for it before there is one.
    SettingsRead(Box<Result<startup::Settings>>),
    /// The paths named on the command line or by the Python binding: whether each is
    /// there and whether one is a directory is asked on a worker
    /// ([`JobKind::OpenNamed`]), a local-looking path being no promise of a fast
    /// mount.
    OpenNamed(Vec<PathBuf>, OpenOptions),
    /// A path named on the command line is not there: the session ends as a missing
    /// file always has. The continuation of the look's answer.
    NamedPathMissing(PathBuf),
    /// Open these paths. Its phases are the loading controller's: each worker's answer
    /// starts the next, and the dataset is installed when its schema is read.
    Open(Vec<PathBuf>, OpenOptions),
    /// Open with an existing LazyFrame (e.g. from Python binding); no file load.
    OpenLazyFrame(Box<LazyFrame>, OpenOptions),
    /// A home listing built off-thread is ready.
    HomeListingReady {
        generation: u64,
        listing: Box<crate::home::Listing>,
        /// What earlier runs measured, read from the cache with the listing.
        known: std::collections::HashMap<PathBuf, crate::cache::DatasetFacts>,
        /// How often and how lately each recent was opened.
        visits: std::collections::HashMap<PathBuf, crate::cache::Visits>,
        /// The recent opened last, where the cursor lands.
        newest: Option<PathBuf>,
        /// The saved folds, when entering the home screen asked for them.
        folds: Option<std::collections::HashMap<String, bool>>,
    },
    /// The worker building a home listing panicked, so no listing is coming. The panic
    /// is flashed like any other raw worker's.
    HomeListingFailed,
    /// The directory the `~` prompt is typing, read off-thread.
    HomePathListed {
        listing: Box<crate::home::PathListing>,
    },
    /// A completed path, worked out off-thread.
    HomePathCompleted {
        generation: u64,
        /// What was typed when completion was asked for; a later keystroke makes the
        /// answer stale.
        typed: String,
        completed: String,
        candidates: usize,
    },
    /// The first rows of the highlighted file, read off-thread the way its open reads
    /// them, and the dataset that read built, for the open to install.
    HomePreviewReady {
        path: PathBuf,
        /// The row's stamp when it was asked for: what the rows are kept under.
        stamp: crate::home_preview::Stamp,
        /// The file's stamp when it was read: what the dataset is installed under.
        read_at: Option<crate::home_preview::Stamp>,
        rows: Option<Arc<crate::home_preview::PreviewRows>>,
        prepared: crate::home_preview::Handoff,
    },
    /// A schema read off-thread for the highlighted dataset.
    HomeSchemaReady {
        generation: u64,
        path: PathBuf,
        preview: Option<crate::discover::SchemaPreview>,
    },
    /// Measurements for rows the home screen asked about, sent as each row is read so
    /// a slow row does not hold back the ones before it. `done` marks the end of the
    /// batch and frees the slot for the next one.
    ///
    /// No generation, unlike its neighbors: what a look found is keyed by path and
    /// true of that path whichever listing asked, so an answer that outlives its
    /// listing is still the answer.
    HomeMeasured {
        measured: Vec<(PathBuf, crate::home::Measured)>,
        done: bool,
    },
    /// What the rows on screen turned out to be. The same payload as
    /// [`AppEvent::HomeMeasured`] and folded in the same way: a kind is one of the
    /// things a look into a row produces.
    HomeClassified {
        measured: Vec<(PathBuf, crate::home::Measured)>,
        done: bool,
    },
    /// A batch of datasets found by the background search below the working
    /// directory. Sent repeatedly while the walk runs, so a cold tree fills in
    /// rather than arriving all at once at the end.
    HomeSearchBatch {
        generation: u64,
        root: PathBuf,
        found: Vec<crate::discover::Entry>,
        scanned: usize,
    },
    /// The filter scored against the search's files, for the walk `epoch` names.
    HomeSearchScored {
        epoch: u64,
        /// `None` from a worker that died.
        matches: Option<Box<crate::search::Matches>>,
    },
    /// The background search has stopped, with `limited` saying why if it stopped
    /// short of walking everything.
    HomeSearchDone {
        generation: u64,
        root: PathBuf,
        scanned: usize,
        limited: Option<String>,
    },
    /// The cloud sources on this machine, with whatever was listed on an earlier run.
    /// Sent before anything is fetched, so the rows are there on the first frame.
    #[cfg(feature = "cloud")]
    HomeCloudSources {
        sources: Vec<crate::home::CloudSource>,
    },
    /// One source's buckets have been listed, or could not be. Each source reports on
    /// its own, so a slow endpoint holds up nobody else's row.
    #[cfg(feature = "cloud")]
    HomeCloudListed {
        id: String,
        buckets: Vec<PathBuf>,
        /// Lines for the details pane of each listed place that has any.
        details: Vec<(PathBuf, Vec<(String, String)>)>,
        /// `(short, detail)` when the listing failed.
        failure: Option<(String, String)>,
        listed_at: std::time::SystemTime,
    },
    /// A network root has been listed off-thread, or could not be.
    HomeProbeReady {
        root: PathBuf,
        rows: Option<Vec<crate::discover::Entry>>,
        /// The listing stopped at [`crate::discover::MAX_ENTRIES_PER_DIR`].
        cut_short: bool,
    },
    /// The rows of a network directory read so far, while its listing goes on.
    HomeProbeProgress {
        root: PathBuf,
        rows: Vec<crate::discover::Entry>,
    },
    /// What peeking inside some directories of a cloud listing found: the ones that are
    /// partitioned or Parquet datasets.
    HomeCloudKinds {
        kinds: Vec<(
            PathBuf,
            (crate::discover::EntryKind, crate::discover::Holds),
        )>,
        /// Directories whose peek failed or was lost: not answered, so not labelled as
        /// if they were.
        failed: Vec<PathBuf>,
    },
    /// A cloud listing was refused, with the service's reason.
    HomeProbeFailed {
        root: PathBuf,
        message: String,
    },
    /// Run the export, from plan to committed file, once the UI has drawn its
    /// progress.
    DoExport(ExportRequest),
    /// A followed file's watcher found more rows, or that the file went.
    Followed(crate::follow::News),
    Exit,
    Crash(String),
    Search(String),
    SqlSearch(String),
    FuzzySearch(String),
    Filter(Vec<FilterStatement>),
    Sort(Vec<String>, Vec<bool>), // Columns, and per column whether it runs descending
    ColumnOrder(Vec<String>, usize), // Column order, locked columns count
    Pivot(PivotSpec),
    Melt(MeltSpec),
    Export(ExportRequest),
    /// Collect and format the whole view off-thread for a table-scope copy.
    CopyTable {
        format: crate::clipboard::CopyFormat,
        header: bool,
    },
    ChartExport(ChartExportRequest),
    /// Deferred: run the chart export once its phase is drawn.
    DoChartExport(ChartExportRequest),
    Collect,
    Update,
    Reset,
    Resize(u16, u16), // resized (width, height)
    DoScrollDown,     // Deferred scroll: perform page_down after one frame (throbber)
    DoScrollUp,       // Deferred scroll: perform page_up
    DoScrollNext,     // Deferred scroll: perform select_next (one row down)
    DoScrollPrev,     // Deferred scroll: perform select_previous (one row up)
    DoScrollEnd,      // Deferred scroll: jump to last page (throbber)
    DoScrollHome,     // Deferred scroll: jump to first page (throbber)
    DoScrollHalfDown, // Deferred scroll: half page down
    DoScrollHalfUp,   // Deferred scroll: half page up
    GoToLine(usize),  // Deferred: jump to line number (when collect needed)
    /// Run the next chunk of analysis (describe/distribution); drives per-column progress.
    AnalysisChunk,
    /// Run distribution analysis (deferred so progress overlay can show first).
    AnalysisDistributionCompute,
    /// Run correlation matrix (deferred so progress overlay can show first).
    AnalysisCorrelationCompute,
    /// Run the configured data-quality plan off the UI thread.
    AnalysisDataQualityCompute,
    /// A Data Quality run that stopped short had already read its sample: kept, so
    /// the read it paid for is not thrown away.
    BackgroundQualitySampleKept {
        kept: KeptQualitySample,
    },
    /// A full scan finished copying a remote dataset's objects locally: kept for the
    /// dataset it was fetched for, whatever becomes of the run. `None` when the copy
    /// did not read as the source, so later runs read the source and say why.
    BackgroundQualityCopyKept {
        dataset_generation: u64,
        copy: Option<Arc<crate::local_copy::LocalCopy>>,
    },
    /// Background task completed: exact row count for the current LazyFrame. Applied to
    /// `data_table_state` only if `len_generation` still matches (the data is unchanged).
    /// Runs concurrently with — and independently of — the first buffer paint, so the
    /// count fills in the scrollbar/total without ever blocking the initial render.
    BackgroundLenReady {
        len_generation: u64,
        num_rows: usize,
        /// For a remote dataset of many files, the rows in each row group of each file,
        /// from their footers.
        file_row_groups: Option<Vec<Vec<usize>>>,
    },
    /// Background row count failed. Clears the in-flight marker; the total stays
    /// provisional and is shown as unknown. Scrolling does not count again; End does.
    BackgroundLenFailed {
        len_generation: u64,
    },
    /// A frame was painted. The run loop calls [`App::frame_painted`] itself; a harness
    /// that paints nothing sends this when [`App::count_waits_for_a_frame`].
    FramePainted,
    /// Every footer of a dataset that opened from two of them has now been read. What
    /// they say is in `App::pending_footers_result`; the columns they add join the
    /// dataset already on screen.
    BackgroundFootersJoined {
        generation: u64,
    },
    /// Background task completed: chart data for one selection is prepared. The data is
    /// in `App::pending_chart_result`; it belongs to `App::chart_inflight`, which says
    /// whether it is still wanted.
    BackgroundChartReady,
    /// Write the Data Quality report on screen to a file, in a form. From the
    /// results in memory: nothing is read.
    QualityReportExport(PathBuf, crate::quality_export::ReportFormat, Overwrite),
    /// A directory named on the command line: look at it on a worker, then do with it
    /// whatever `Enter` on its row would do.
    ///
    /// The look reads footers, or the front of a spread of files, which for a directory
    /// of large Parquet is seconds. It is an event rather than a call so the first frame
    /// is drawn before it starts, and the wait has the directory's name on it, a spinner
    /// and a way out.
    LookThenOpenDirectory(PathBuf, OpenOptions),
    /// Look at a path off the interface thread, then do with it whatever it turns out to
    /// need — browse into it, say it is a lake table, or open it.
    ///
    /// `exists`, `is_dir` and `classify_directory` are all filesystem calls, and the home
    /// screen is full of paths on mounts that may not answer. Doing them where the keys
    /// are read is an uninterruptible freeze with Ctrl+C on the same thread.
    ClassifyThenOpen {
        path: PathBuf,
        /// A jump — a path typed at `~` — rather than a row that was already listed. Esc
        /// then comes back from there to the listing, not up through wherever the path
        /// happens to sit.
        jump: bool,
    },
    /// A background job's outcome is in its record: [`jobs::Jobs::end`] takes it.
    JobEnded(Ticket),
    /// A report from a background job still running.
    JobProgress {
        ticket: Ticket,
        progress: Progress,
    },
}

impl AppEvent {
    /// A report from work still going, sent many times while it runs: the run loop may
    /// fold several into one frame. Anything else is drawn as soon as it is handled.
    pub fn is_progress(&self) -> bool {
        matches!(
            self,
            AppEvent::HomeMeasured { done: false, .. }
                | AppEvent::HomeClassified { done: false, .. }
                | AppEvent::HomeSearchBatch { .. }
                | AppEvent::HomeProbeProgress { .. }
                | AppEvent::JobProgress {
                    progress: Progress::QualityPhase(_),
                    ..
                }
        )
    }
}

/// Picks the home-screen workers that panic before their work starts, by the answer
/// they would owe; see `App::home_worker_dies`.
#[cfg(test)]
type HomeWorkerDies = Box<dyn FnMut(&AppEvent) -> bool + Send>;

/// Stands in for [`FileFacts::read`]; see `App::file_facts_reader`.
#[cfg(test)]
type FileFactsReader =
    Arc<dyn Fn(&Path, bool) -> std::result::Result<FileFacts, String> + Send + Sync>;

/// What [`App::handle`] did with an event: `Ok` carries the follow-up event to send,
/// if any; `Err` returns a key that arrived while the app was busy. Nothing was done
/// with that key and it was not dropped: the caller keeps it and offers it again once
/// the app is idle.
pub type EventOutcome = Result<Option<AppEvent>, KeyEvent>;

/// What <kbd>Enter</kbd> will do on the highlighted row.
///
/// Written so the control bar and the details pane can say it before it happens.
/// Every directory has two doors and the labels no longer decide access, which is only
/// worth anything if the screen says which key is which — a bar reading `Enter Open` on
/// a row where `Enter` goes inside teaches the wrong thing on the first try, and the
/// first try is the one that forms the impression.
///
/// A prediction, so it can drift from [`App::home_open_selected`], which is the thing
/// that actually decides. `test_the_bar_says_what_enter_will_really_do` pumps `Enter`
/// on one row of every shape and asserts the two agreed; that test is the reason this
/// is safe to read from the renderer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WhatEnter {
    /// Load the file on the row.
    OpensFile,
    /// Read the whole directory as one table: a hive root, a directory whose files are
    /// one table, or the `(all files)` row.
    OpensDirectory,
    /// Step into the directory. What `→` does too, on these rows.
    GoesInside,
    /// Look at the row first, then do whichever of the above the answer calls for.
    LooksFirst,
    /// Fold or unfold a section.
    FoldsSection,
    /// Show the rest of `RECENT`.
    ShowsMore,
    /// Show the files datui cannot open, as `Ctrl+A` does.
    ShowsHidden,
    /// Nothing to open and nowhere to go: an HTTP place, which has no listing to
    /// browse and says so.
    Explains,
    /// A local file datui has no reader for: Enter shows its bytes in the hex view.
    OpensHex,
    /// A remote file datui has no reader for. Enter says so, and the bar offers nothing.
    Nothing,
}

impl App {
    /// See [`WhatEnter`].
    pub fn what_enter_does(&self) -> WhatEnter {
        // One walk of the list, not four. Every `selected_*` helper rebuilds it, and this
        // runs from the control bar on every frame, beside a
        // `selected_directory_to_enter` that walks it once more.
        let rows = self.home.visible();
        let entry = match rows.get(self.home.selected) {
            // A place row browses into the place, which is what `→` does on it too, so
            // it is labelled the same and offered once. An HTTP place has no listing to
            // browse and says so instead.
            Some(home::Row::Place { path, .. }) => {
                return if home::place_is_browsable(path) {
                    WhatEnter::GoesInside
                } else {
                    WhatEnter::Explains
                };
            }
            Some(home::Row::Header { .. }) => return WhatEnter::FoldsSection,
            Some(home::Row::More { .. }) => return WhatEnter::ShowsMore,
            Some(home::Row::Hidden { .. }) => return WhatEnter::ShowsHidden,
            None => return WhatEnter::Explains,
            // The door reads the directory it names whatever that directory is labelled —
            // the lake tables included, which is the one row that reads them at all.
            Some(home::Row::Door { .. }) => return WhatEnter::OpensDirectory,
            Some(home::Row::Entry { entry, .. }) => *entry,
        };
        match entry.kind {
            discover::EntryKind::Unknown => WhatEnter::LooksFirst,
            // A database of several tables lists them.
            discover::EntryKind::File if entry.enter_lists_tables() => WhatEnter::GoesInside,
            discover::EntryKind::File => WhatEnter::OpensFile,
            discover::EntryKind::Other
                if matches!(
                    source::input_source(&entry.path),
                    source::InputSource::Local(_)
                ) =>
            {
                WhatEnter::OpensHex
            }
            discover::EntryKind::Other => WhatEnter::Nothing,
            discover::EntryKind::Hive | discover::EntryKind::MultiFile => WhatEnter::OpensDirectory,
            // A plain directory, and a lake table, whose files are not its rows.
            discover::EntryKind::Directory
            | discover::EntryKind::Delta
            | discover::EntryKind::Iceberg
            | discover::EntryKind::Hudi => WhatEnter::GoesInside,
        }
    }
}

impl App {
    /// Whether a dataset held `download` because it came from a remote `path` (the
    /// URL it is shown by): a local stream's conversion and standard input's spool are
    /// held the same way.
    fn fetched(download: Option<&crate::download::TempDownload>, path: Option<&Path>) -> bool {
        download.is_some() && path.is_some_and(source::is_remote_url)
    }
}

/// Input for the shared run loop: open from file paths or from an existing LazyFrame (e.g. Python binding).
#[derive(Clone)]
pub enum RunInput {
    /// The command line as parsed. The configuration is read, and the flags applied
    /// over it, behind the first frame ([`startup`]).
    Cli(Box<Args>),
    Paths(Vec<PathBuf>, OpenOptions),
    LazyFrame(Box<LazyFrame>, OpenOptions),
}

#[derive(Debug, Default, PartialEq, Eq)]
pub enum InputMode {
    #[default]
    Normal,
    /// The home screen: pick a dataset to open. Reachable at startup with no
    /// arguments, and from inside a session, which is what makes datui a place you
    /// stay rather than a command you re-run.
    Home,
    SortFilter,
    PivotMelt,
    Editing,
    Export,
    /// The copy dialog over the table.
    Copy,
    /// The row inspector over the table.
    Inspect,
    /// The column picker over the table: type a column's name to go to it.
    GoToColumn,
    /// The format picker over a table read through a spec: read it with another.
    PickFormat,
    Info,
    Chart,
    /// Value Counts: how often each value of one column occurs in the view.
    ValueCounts,
    /// The hex view: a file's bytes.
    Hex,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputType {
    Search,
    GoToLine,
    Find,
}

/// A query whose first rows are being read. It planned, but can still fail on the
/// data — a value that will not cast — and until the rows are in, the view it
/// replaced is kept to go back to.
struct QueryRun {
    origin: RunOrigin,
    /// The `len_generation` of the frame the query installed. Once that frame is gone
    /// (a sort, a filter, another dataset) the rollback no longer applies.
    frame: u64,
    rollback: crate::widgets::datatable::ViewRollback,
    /// The App's count markers as they were, for the frame the rollback restores.
    /// A count of that frame still running when the query began lands while the
    /// query's frame is installed; its answer goes into `rollback`.
    len_count_inflight: Option<u64>,
    count_after_paint: Option<u64>,
    len_count_failed: Option<u64>,
    /// Rows `df` holds, when known, so a failure can say "of N".
    rows: Option<usize>,
}

/// Where a running query came from, which decides where its failure is said.
enum RunOrigin {
    /// The query prompt, or its event sent directly: inline under the prompt while
    /// it is open in this mode, else a dialog.
    Query(QueryMode),
    /// A view applied. Its failure is a dialog, and the view marked applied before
    /// it is marked again.
    View { previous: Option<String> },
}

/// Focus within the query prompt: the tab bar or the current mode's input.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum QueryFocus {
    TabBar,
    #[default]
    Input,
}

/// What the bar says of a recording (`--tee`): `rec` with its size and rate while it
/// goes on, `saved` with its size, length and file once it ended (`sent` and no file
/// for `--tee -`), or `stopped` and why, in the warning color, when it ended in an
/// error. The second value is that last.
fn recording_label(spool: &crate::follow::Spool) -> (String, bool) {
    let dot = crate::glyphs::get().middot;
    let size = crate::discover::format_size(spool.bytes());
    match spool.ended() {
        None => (
            format!(
                "rec {size} {dot} {}/s",
                crate::discover::format_size(spool.rate() as u64)
            ),
            false,
        ),
        Some(None) => {
            let secs = spool.duration().as_secs();
            let length = if secs >= 3600 {
                format!("{}:{:02}:{:02}", secs / 3600, secs / 60 % 60, secs % 60)
            } else {
                format!("{}:{:02}", secs / 60, secs % 60)
            };
            match spool.tee().filter(|tee| !tee.to_stdout()) {
                Some(tee) => (
                    format!("saved {size} {dot} {length} {dot} {}", tee.name()),
                    false,
                ),
                None => (format!("sent {size} {dot} {length}"), false),
            }
        }
        Some(Some(reason)) => (format!("stopped: {reason}"), true),
    }
}

/// Where a key was taking the user when leaving was asked about.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Leaving {
    Quit,
    Home,
}

/// An export under way, for the control bar: the file, its phase, and the bytes
/// written once writing has started.
#[derive(Clone, Debug)]
pub struct ExportProgress {
    pub file_path: PathBuf,
    pub current_phase: String,
    pub written: Option<u64>,
}

/// In-progress analysis computation state (orchestration in App; modal only displays progress).
#[allow(dead_code)]
struct AnalysisComputationState {
    df: Option<DataFrame>,
    schema: Option<Arc<Schema>>,
    partial_stats: Vec<crate::statistics::ColumnStatistics>,
    current: usize,
    total: usize,
    total_rows: usize,
    sample_seed: u64,
    sample_size: Option<usize>,
}

/// At most one query type can be active. Returns (query, sql_query, fuzzy_query) with only the
/// active one set (SQL takes precedence over fuzzy over DSL query). Used when saving template settings.
fn active_query_settings(
    dsl_query: &str,
    sql_query: &str,
    fuzzy_query: &str,
) -> (Option<String>, Option<String>, Option<String>) {
    let sql_trimmed = sql_query.trim();
    let fuzzy_trimmed = fuzzy_query.trim();
    let dsl_trimmed = dsl_query.trim();
    if !sql_trimmed.is_empty() {
        (None, Some(sql_trimmed.to_string()), None)
    } else if !fuzzy_trimmed.is_empty() {
        (None, None, Some(fuzzy_trimmed.to_string()))
    } else if !dsl_trimmed.is_empty() {
        (Some(dsl_trimmed.to_string()), None, None)
    } else {
        (None, None, None)
    }
}

/// How far planning a view's steps got.
enum Replayed {
    /// Every step is planned; the view's rows are still to be read.
    Planned,
    /// Stopped at the pivot, which has to be read before the steps after it can be
    /// planned.
    Pivot(Box<crate::widgets::datatable::PivotJob>),
}

/// What a cloud open was pointed at: the URL as the user gave it, the prefix to list,
/// and the glob to keep, where they named one.
///
/// Together because they are one thought — where to look — and apart they put this
/// function's signature past the point where a reader can hold it.
#[cfg(feature = "cloud")]
struct CloudTarget<'a> {
    /// The URL as typed, which is where the bucket and scheme come from.
    full: &'a str,
    /// The literal prefix to list: the whole key, or the part of a glob before its star.
    key: String,
    /// The glob the user named, where they named one. The listing keeps only the keys
    /// it matches, so everything downstream sees a plain list of files.
    pattern: Option<&'a globset::GlobMatcher>,
}

/// Why Data Quality's Run did not start: a cancelled run has not exited yet. Said
/// on Setup's line until it has.
const QUALITY_RUN_WAITS: &str = "Run waits: the cancelled run is still stopping";

/// Why another read of the source did not start: a cancelled analysis has not
/// exited yet, and a second read beside it is how memory runs out.
const ANALYSIS_READ_WAITS: &str = "A cancelled run is still finishing; try again shortly";

/// How long a cancelled run that stops within a batch may take before the screen
/// says it is still going: time for a batch to finish, and no longer.
const CANCEL_GRACE: std::time::Duration = std::time::Duration::from_secs(1);

pub struct App {
    pub data_table_state: Option<DataTableState>,
    /// The footer counter of the dataset on screen, which its pass behind the open
    /// reports to. Handed over by the load that installed it; a load in flight counts on
    /// its own until then. See [`Self::footer_progress`].
    footer_progress: Arc<crate::schema_union::FooterProgress>,
    /// The count as it stood when this frame began, or `None` if no pass was running.
    ///
    /// Taken once because the pass is running on other threads while the frame is
    /// drawn. The loading body and the control bar are painted a millisecond apart,
    /// and when each read the counter for itself they printed different numbers for
    /// one wait — and the bar could print a phase's flat percentage beside a count
    /// that had finished between the two reads.
    footers_this_frame: Option<(usize, usize)>,
    /// Network roots currently being listed off-thread, so a probe is not started
    /// twice. Entries are never removed for a root that never answers — that thread
    /// is unreclaimable, and retrying it would only block another one.
    home_probes_inflight: Vec<PathBuf>,
    /// True once cloud discovery has been started. Enumeration costs a request per
    /// provider, so it happens once and its result is kept for the session.
    #[cfg(feature = "cloud")]
    cloud_discovery_started: bool,
    /// True while a recursive search below the working directory is out. One at a
    /// time: the walk is bounded, and a second one would only compete for the disk.
    home_search_inflight: bool,
    /// Set while the confirmation modal is asking about forgetting every recent.
    pending_clear_recents: bool,
    /// The place whose recents the confirmation modal is asking about forgetting.
    pending_forget_place: Option<PathBuf>,
    /// Why the last open failed, shown on the home screen when the error is dismissed
    /// and there is nothing to fall back to.
    last_load_error: Option<String>,
    /// Schema reads currently out, so the same one is not requested every frame.
    home_schema_inflight: Vec<PathBuf>,
    /// Invalidates listings and measurements from a request the user has moved past.
    home_generation: u64,
    /// Home screen state. Rebuilt from the filesystem whenever home is entered;
    /// nothing here is persisted beyond the recents list.
    pub home: home::HomeState,
    /// Schema previews, memoised for the session only. Persisting these would be a
    /// catalogue by another name, and it would go stale.
    home_schema_cache: HashMap<PathBuf, Option<discover::SchemaPreview>>,
    /// The home screen's `ROWS` previews, and the dataset the newest one built.
    pub home_previews: crate::home_preview::Previews,
    /// The reads of data started this session, by kind.
    pub reads: crate::home_preview::ReadCounts,
    /// The dataset whose downloaded shape is kept already. See
    /// [`Self::remember_a_downloads_shape`].
    shape_remembered: Option<u64>,
    path: Option<PathBuf>,
    original_file_format: Option<ExportFormat>, // Track original file format for default export
    original_file_delimiter: Option<u8>, // Track original file delimiter for CSV export default
    /// What `-` reads in place of standard input: a test's pipe.
    stdin_reader: Option<Box<dyn std::io::Read + Send>>,
    /// Where `--tee -` passes the stream on: standard output as the process got it.
    stdout_pass: Option<Box<dyn std::io::Write + Send>>,
    /// The follow mark as last drawn, so its clock redraws only when it changes.
    follow_drawn: Option<crate::widgets::controls::FollowMark>,
    /// Leaving was asked about while recording: what the user was doing.
    pending_leave: Option<Leaving>,
    /// A recording kept going after the user went home or quit, until its stream ends.
    recording_on: Option<Arc<crate::follow::SpoolHandle>>,
    /// A recording's end has been said: once, in the bar or the error dialog.
    recording_end_said: bool,
    events: Sender<AppEvent>,
    debug: DebugState,
    pub info_modal: InfoModal,
    /// What the Info panel's read found about the open file, and the
    /// `dataset_generation` it belongs to. Asked for when the panel opens, read on a
    /// worker ([`Job::FileFacts`], whose record says it is reading), and kept for the
    /// dataset however the read ended, so neither drawing nor reopening reads again.
    file_facts: Option<(u64, FileFacts)>,
    // One input per query mode, each with its own history. The history ids
    // ("query", "sql", "fuzzy") name files already on disk; they stay as they
    // are so no history is lost or read as another mode's.
    query_input: TextInput, // q-style, history id "query"; also borrowed by go-to-line
    sql_input: TextInput,   // SQL, history id "sql"
    fuzzy_input: TextInput, // Search, history id "fuzzy"
    /// The find prompt (`f`) and the find `n` and `N` repeat; history id "find".
    pub find: find::Find,
    pub input_mode: InputMode,
    input_type: Option<InputType>,
    query_mode: QueryMode,
    query_focus: QueryFocus,
    /// The columns of `df`, for the SQL prompt's list and completion. Taken from the
    /// schema when the prompt opens.
    sql_columns: Vec<(String, DataType)>,
    /// A Tab completion in progress in the SQL input.
    sql_completion: Option<sql_assist::Cycle>,
    /// A query whose first collect is running, and the view to go back to if it
    /// fails. From the prompt, the prompt stays open until it is done.
    query_running: Option<QueryRun>,
    /// Why the last statement failed once it ran, shown under it in the prompt.
    query_run_error: Option<String>,
    /// Bumped when a running statement's failure lands in the prompt. Keys typed while
    /// it ran were not answers to it; see `EventPump`.
    inline_failures: u64,
    pub sort_filter_modal: SortFilterModal,
    pub pivot_melt_modal: PivotMeltModal,
    pub template_modal: TemplateModal,
    /// Whether the open dataset was reached through the home screen. `q` pops
    /// the context: opened from home it returns there, launched straight onto
    /// a file it quits — the user's mental stack, not a mode.
    opened_from_home: bool,
    /// `--template NAME`, waiting for the dataset from the command line to land.
    /// Taken on the first install, so datasets opened later are not re-dressed.
    startup_template: Option<String>,
    pub analysis_modal: AnalysisModal,
    /// Reports, newest first, within [`QUALITY_MEMORY_BUDGET`].
    quality_cache: Vec<QualityCacheEntry>,
    /// See [`KeptQualitySample`]. Newest first, within [`QUALITY_MEMORY_BUDGET`].
    quality_samples: Vec<KeptQualitySample>,
    /// Acquisitions the budget released, newest first: (dataset, view, sample).
    quality_released: Vec<(u64, u64, sampling::Sample)>,
    /// [`QUALITY_MEMORY_BUDGET`], smaller in a test that fills it.
    quality_memory_budget: usize,
    /// Local copies Data Quality's full scans read instead of a remote source, newest
    /// first, within `performance.quality_local_copy_mb`. Removed from disk when
    /// released, when the dataset is opened again or replaced, and at exit.
    quality_copies: Vec<RetainedCopy>,
    /// The dataset whose copy was released, so Setup says why Run fetches again.
    quality_copy_released: Option<u64>,
    /// The dataset whose copy did not read as its source: its full scans read the
    /// source, and Setup says why.
    quality_copy_unusable: Option<u64>,
    /// Free bytes in the cache directory, and when they were asked: Setup redraws
    /// often, and the answer only feeds a line of text until Run asks again.
    quality_copy_free: std::sync::Mutex<Option<(std::time::Instant, Option<u64>)>>,
    /// The table an analysis drill left behind: Data Quality's matching rows or the
    /// sample's, shown in its place until Esc brings it back.
    quality_evidence_return: Option<Box<DataTableState>>,
    pub(crate) quality_evidence_label: Option<String>,
    pub chart_modal: ChartModal,
    pub chart_export_modal: ChartExportModal,
    pub export_modal: ExportModal,
    pub copy_modal: copy_modal::CopyModal,
    pub inspector_modal: inspector_modal::InspectorModal,
    /// A value the inspector wrote for another program, for the run loop to open:
    /// it owns the terminal that a waiting program takes over.
    external_open: Option<external_open::ExternalOpen>,
    /// Where those values are written; removed when the app is.
    open_dir: Option<tempfile::TempDir>,
    /// The shown columns, narrowed by what is typed, while `g` is choosing one.
    pub go_to_column: crate::widgets::ui::PickerState,
    /// The Value Counts screen (`F`).
    pub value_counts: value_counts_modal::ValueCountsModal,
    /// The counts the export dialog writes, when it was opened from Value Counts.
    export_counts: Option<polars::prelude::DataFrame>,
    /// The specs `b` offers for the dataset on screen.
    pub format_picker: crate::widgets::ui::PickerState,
    /// The hex view (`InputMode::Hex`), kept while it is up.
    pub hex: Option<hex_view::HexView>,
    /// Bumped per hex view opened, so a find's answer for another is dropped.
    hex_serial: u64,
    /// Where copies go. Built at the first copy and kept for the run: on
    /// Wayland and X11 the clipboard offer dies with the process that owns it,
    /// so this handle must live as long as the copy should.
    clipboard: Option<Box<dyn clipboard::Destination>>,
    /// A table-scope copy waiting on the size confirmation.
    pending_copy: Option<(clipboard::CopyFormat, bool)>,
    pub(crate) chart_cache: ChartCache,
    /// The one chart preparation allowed to run at a time. Render draws only what is in
    /// `chart_cache`; this drives the throbber while it is current. Its result is
    /// installed only if the record is still current (not `stale`) and the dataset is
    /// the one it was computed from. Deliberately not
    /// `busy`: the sidebar stays live while the data is computed, and the newest
    /// selection is prepared once this one lands.
    chart_inflight: Option<ChartInflight>,
    /// The result of the background chart preparation, like `pending_collect_result`:
    /// the data stays out of the event.
    pending_chart_result: ChartResultSlot,
    /// A chart export that asked for data still being prepared. `BackgroundChartReady`
    /// picks it up; `busy` stays set until then.
    chart_export_waiting: Option<ChartExportRequest>,
    error_modal: ErrorModal,
    flash: Option<Flash>,
    pub confirmation_modal: ConfirmationModal,
    /// An export waiting on the overwrite confirmation.
    pending_export: Option<ExportRequest>,
    pending_chart_export: Option<ChartExportRequest>,
    /// A Data Quality report export waiting on the overwrite confirmation.
    pending_quality_export: Option<(PathBuf, crate::quality_export::ReportFormat)>,
    show_help: bool,
    help_scroll: usize, // Scroll position for help content
    /// What the mouse can land on in the last frame, and the last click.
    pointer: pointer::Pointing,
    cache: CacheManager,
    template_manager: Templates,
    active_template_id: Option<String>, // ID of currently applied template
    /// An export under way, which the control bar reports.
    export_progress: Option<ExportProgress>,
    theme: Theme, // Color theme for UI rendering
    /// `a` is waiting on the confirmation to read every row.
    pending_read_all: bool,
    history_limit: usize, // History limit for all text inputs (from config.query.history_limit)
    table_cell_padding: u16, // Spaces between columns (from config.display.table_cell_padding)
    column_colors: bool, // When true, colorize table cells by column type (from config.display.column_colors)
    /// Second header row of column types. Starts from `display.dtype_row`; `D` flips it.
    dtype_row: bool,
    // Resolved display-time number formatting. `enabled` is flipped by the F key.
    number_format: NumberFormatSettings,
    runtime: tokio::runtime::Handle, // Tokio runtime handle for background tasks
    /// Every general background operation, and the generation their answers are judged
    /// by. See [`jobs`].
    jobs: Jobs,
    /// The open in flight, from the request to its first rows: its phase, what the
    /// loading screen says, and what it holds. See [`loading`]. Going home abandons it;
    /// an answer from an open it no longer holds is dropped.
    loading: loading::Loader,
    /// The paths the dataset on screen was opened from, with the options it installed
    /// with: what `H` opens again with its header turned the other way.
    opened: Option<(Vec<PathBuf>, OpenOptions)>,
    /// Where the last load-ahead was asked from. See [`App::load_ahead`].
    loaded_ahead_from: Option<(u64, usize, usize, usize)>,
    // `len_generation` of the in-flight background row-count, if any. Prevents re-spawning
    // the (potentially minutes-long) count on every scroll while it's still running.
    len_count_inflight: Option<u64>,
    /// The `len_generation` of a count `len_count_inflight` promises that has not
    /// started. A full count of a local frame competes with reading its first page for
    /// the disk and the Polars workers, and that page's rows can make it unnecessary,
    /// so it starts once a frame has painted them. See [`App::frame_painted`].
    count_after_paint: Option<u64>,
    /// Counts started, so a test can say none began before the page was painted.
    #[cfg(test)]
    counts_spawned: std::cell::Cell<usize>,
    /// Times an installed dataset's own first rows were asked for, so a test can say a
    /// view applied on open read them instead.
    #[cfg(test)]
    first_rows_asked: usize,
    // `len_generation` whose background row-count failed. While this matches the current
    // generation (and the count is still invalid) the row count is shown as "?" rather than a
    // misleading provisional total.
    len_count_failed: Option<u64>,
    /// End was pressed on a remote dataset before its rows were counted: go there when
    /// the count for this generation arrives, rather than to a guess.
    end_after_count: Option<u64>,
    /// What the pass behind a staged open found, for the frame that applies it: large
    /// enough to be worth keeping out of the event, and discarded if the dataset it
    /// belongs to has been replaced.
    pending_footers_result: std::sync::Arc<std::sync::Mutex<FootersReported>>,
    /// Bumped once per dataset put on screen, which the jobs' generation is not: a collect
    /// bumps that, and the pass reading the rest of a dataset's footers outlives
    /// several. It is what says whether the columns arriving belong to the dataset the
    /// user is looking at.
    dataset_generation: u64,
    /// End was pressed while a dataset was still reading its footers, which is where
    /// its end is coming from. Jump when they land — and only for that dataset, which
    /// is what the generation is for: a directory the user pressed End on and then walked
    /// away from must not move the view of the one they opened next. `end_after_count`
    /// alongside keys itself the same way, to `len_generation`.
    end_when_the_footers_land: Option<u64>,
    /// What a dataset's footers found while the user was looking at a query, a pivot or
    /// a drill-down rather than at the data. Held rather than applied, because widening
    /// the scan under a query takes the query's own columns away, and offered again the
    /// moment the view comes back to the dataset itself.
    footers_held: Option<(u64, crate::widgets::datatable::FootersFound)>,
    /// A re-read the dataset is owed by a footer pass that came back empty-handed, held
    /// back because the collect it goes through would bump the generation out from
    /// under work already running. The pass that failed brings no columns to hold, so
    /// `footers_held` has nothing to say about it, and the dataset still needs the
    /// ordinary count the pass was going to save it — hence an errand of its own, tried
    /// again after every event until the work it would cancel is done.
    reread_owed: Option<u64>,
    /// Which home screen workers panic before their work starts, for tests of what a
    /// dying worker leaves behind. The jobs' own is [`Jobs::worker_dies`].
    #[cfg(test)]
    home_worker_dies: Option<HomeWorkerDies>,
    /// Reads the open file's facts in place of [`FileFacts::read`], for tests of a
    /// read that is slow or fails.
    #[cfg(test)]
    file_facts_reader: Option<FileFactsReader>,
    /// When true, show the throbber and defer keys (see [`App::handle`]); the main loop
    /// holds them until this clears.
    busy: bool,
    /// Bumped whenever the screen the user was typing at is replaced without a key of
    /// theirs asking for it: going home, abandoning a load. Keys held while busy carry
    /// the value they were typed under and are dropped if it has moved on.
    screen_generation: u64,
    /// Set by the main loop when it had to drop a key typed while busy, shown beside a
    /// status message while work is running. Cleared once the held keys have been
    /// replayed.
    input_dropped: bool,
    throbber_frame: u8, // Spinner frame index (0..3) for control bar
    /// Status text for the control bar, at the table view. Shown whether or not the app
    /// is busy: an End waiting on a remote row count parks without setting `busy`.
    status_message: Option<String>,
    analysis_computation: Option<AnalysisComputationState>,
    app_config: AppConfig,
    /// The format specs on the search path, read when the app was built.
    formats: Arc<crate::formats::Registry>,
}

impl App {
    /// The finding under the cursor's rows: from the rows the run kept, at once, or
    /// staged as a read that says what it reads and waits for Enter. A finding with
    /// no rows to show opens nothing.
    fn open_quality_evidence(&mut self) -> Option<AppEvent> {
        let (_, finding) = self.analysis_modal.selected_finding()?;
        let results = self.analysis_modal.data_quality_results.as_ref()?;
        let rows = finding.evidence(results).ok()?;
        let sampled = results.precision == data_quality::QualityPrecision::Sampled;
        let count = finding.evidence_count(results);
        let label = format!(
            "Data Quality / {} / {}",
            finding.title,
            quality_report::columns_label(&finding.columns, 40)
        );
        let what = format!(
            "{} {} {}",
            finding.title,
            glyphs::get().middot,
            quality_report::columns_label(&finding.columns, 40)
        );
        self.show_quality_rows(rows, label, sampled, what, count)
    }

    /// The rows an interval's count under the cursor counted: from the rows the run
    /// kept, or staged as a read when it kept none. Nothing opens for a count of none.
    fn open_interval_evidence(&mut self) -> Option<AppEvent> {
        let (predicate, label, count) = self.analysis_modal.interval_evidence()?;
        let sampled = self
            .analysis_modal
            .data_quality_results
            .as_ref()
            .is_some_and(|results| results.precision == data_quality::QualityPrecision::Sampled);
        let what = label
            .trim_start_matches("Data Quality / ")
            .replace(" / ", &format!(" {} ", glyphs::get().middot));
        self.show_quality_rows(
            quality_report::EvidenceRows::Matching(predicate),
            label,
            sampled,
            what,
            Some(count),
        )
    }

    /// The rows the report on screen measured, while they are kept: a sampled run's
    /// rows, same dataset, view and sample. `None` after a full scan, which keeps
    /// none, and once they are released.
    pub(crate) fn quality_rows_kept(&self) -> Option<std::sync::Arc<data_quality::QualitySample>> {
        self.analysis_modal.data_quality_results.as_ref()?;
        let plan = self.analysis_modal.quality_result_plan();
        if plan.compute != data_quality::QualityCompute::Sample {
            return None;
        }
        self.kept_quality_sample(&plan.sample())
    }

    /// Open rows a Data Quality result counted. Kept rows are cut in memory and
    /// shown; anything else would read the source, so it is staged with what it
    /// reads, and only Enter on that reads.
    fn show_quality_rows(
        &mut self,
        rows: quality_report::EvidenceRows,
        label: String,
        sampled: bool,
        what: String,
        count: Option<usize>,
    ) -> Option<AppEvent> {
        let plan = self.analysis_modal.quality_result_plan().clone();
        let by_files = matches!(rows, quality_report::EvidenceRows::Files(_));
        let label = if sampled {
            format!("{label} / sampled")
        } else {
            label
        };
        if !by_files && self.quality_rows_kept().is_some() {
            return self.read_sample_rows(plan.sample(), Some((rows, label)));
        }
        let state = self.data_table_state.as_ref()?;
        let g = glyphs::get();
        let rows_label = |rows: usize| {
            format!(
                "{} {}",
                numfmt::group_chrome(rows),
                if rows == 1 { "row" } else { "rows" }
            )
        };
        let scope = match &rows {
            quality_report::EvidenceRows::Files(files) => files.clone(),
            _ => plan.scope.clone(),
        };
        // A sample as large as the scope reads every row too, but it was not a full
        // scan: its rows were kept, and since released.
        let not_kept = if plan.compute == data_quality::QualityCompute::Full {
            "a full scan keeps no rows"
        } else {
            "the rows read are no longer kept"
        };
        let (why, reads) = match &rows {
            quality_report::EvidenceRows::Files(data_quality::QualityScope::SourceFiles(files)) => {
                (
                    "their rows are in the files, not the report".to_string(),
                    format!(
                        "the {} named {}",
                        files.len(),
                        if files.len() == 1 { "file" } else { "files" }
                    ),
                )
            }
            _ if sampled => (
                "the sampled rows are no longer kept".to_string(),
                format!(
                    "the sample again: {} {} {}",
                    widgets::data_quality::compute_label(&plan),
                    g.middot,
                    widgets::data_quality::planned_read_label(state, &plan)
                ),
            ),
            quality_report::EvidenceRows::Duplicates => (
                not_kept.to_string(),
                format!(
                    "every row of {}, once {} {}",
                    plan.scope.label(),
                    g.middot,
                    widgets::data_quality::scope_read_label(state, &plan)
                ),
            ),
            // The table counts what matches, which reads every row; then it reads the
            // rows it shows.
            _ => (
                not_kept.to_string(),
                format!(
                    "every row of {} to count them, then the rows on screen {} {}",
                    plan.scope.label(),
                    g.middot,
                    widgets::data_quality::scope_read_label(state, &plan)
                ),
            ),
        };
        let shows = match (&rows, count) {
            (quality_report::EvidenceRows::Duplicates, Some(count)) => {
                format!("{}, copies together", rows_label(count))
            }
            (_, Some(count)) => rows_label(count),
            (_, None) => "the rows that match".to_string(),
        };
        let source = if state.is_remote_source() {
            "remote, read only"
        } else {
            "local, read only"
        };
        self.analysis_modal.data_quality_evidence_read = Some(analysis_modal::EvidenceRead {
            summary: vec![
                ("Rows", what),
                ("Why", why),
                ("Reads", reads),
                ("Shows", shows),
                ("Source", source.to_string()),
            ],
            sample: (sampled && !by_files).then(|| plan.sample()),
            scope,
            rows,
            label,
        });
        None
    }

    /// Enter on a staged read: read the rows it named, as it said.
    fn confirm_evidence_read(&mut self) -> Option<AppEvent> {
        // Beside a cancelled run still reading, the read stays staged for later.
        let staged = self.analysis_modal.data_quality_evidence_read.as_ref()?;
        let kept = staged
            .sample
            .as_ref()
            .is_some_and(|sample| self.kept_quality_sample(sample).is_some());
        if !kept && self.read_waits_for_cancelled() {
            return None;
        }
        let read = self.analysis_modal.data_quality_evidence_read.take()?;
        if let Some(sample) = read.sample {
            return self.read_sample_rows(sample, Some((read.rows, read.label)));
        }
        let predicate = match read.rows {
            quality_report::EvidenceRows::Matching(predicate) => predicate,
            // A column its file never had, or holds in a type the scan cannot read,
            // has no value to filter on: its rows are the ones those files hold.
            quality_report::EvidenceRows::Files(_) => polars::prelude::lit(true),
            quality_report::EvidenceRows::Duplicates => {
                return self.read_duplicate_rows(read.scope, read.label);
            }
        };
        self.open_quality_scope_rows(&read.scope, predicate, read.label)
    }

    /// Every row of `scope` that repeats, read in one pass off the UI thread and
    /// shown as a table: what a full scan's duplicate finding opens once asked to.
    fn read_duplicate_rows(
        &mut self,
        scope: data_quality::QualityScope,
        label: String,
    ) -> Option<AppEvent> {
        let state = self.data_table_state.as_ref()?;
        let (lf, schema) = match state.quality_scope_frame(&scope) {
            Ok(frame) => frame,
            Err(error) => {
                self.error_modal
                    .show(format!("Cannot open matching rows: {error}"));
                return None;
            }
        };
        let keys = schema.iter_names().cloned().collect::<Vec<_>>();
        // Binary values as the run grouped them: one stub for all, so they never
        // split a group the check counted as copies.
        let columns = schema
            .iter()
            .map(|(name, dtype)| {
                if matches!(dtype, polars::prelude::DataType::Binary) {
                    polars::prelude::lit(widgets::datatable::binary_stub()).alias(name.clone())
                } else {
                    polars::prelude::col(name.clone())
                }
            })
            .collect::<Vec<_>>();
        let streaming = self.app_config.performance.polars_streaming;
        self.analysis_modal.computing = Some(AnalysisProgress::new("Reading the rows that repeat"));
        self.spawn_job(
            Job::SampleRows,
            Some("Reading the rows that repeat..."),
            move |_| {
                let df = data_quality::duplicate_rows(lf.select(columns), &keys, streaming)
                    .map_err(|error| format!("{error}"))?;
                Ok(Answer::Sample { df, label })
            },
        );
        None
    }

    /// The rows of `scope` matching `predicate`, in the table viewer in place of the
    /// table; Esc brings the table and the report back.
    fn open_quality_scope_rows(
        &mut self,
        scope: &data_quality::QualityScope,
        predicate: polars::prelude::Expr,
        label: String,
    ) -> Option<AppEvent> {
        let state = self.data_table_state.as_ref()?;
        let view = match state.quality_evidence_view(scope, predicate) {
            Ok(view) => view,
            Err(error) => {
                self.error_modal
                    .show(format!("Cannot open matching rows: {error}"));
                return None;
            }
        };
        if let Some(original) = self.data_table_state.replace(view) {
            self.quality_evidence_return = Some(Box::new(original));
            self.quality_evidence_label = Some(label);
            self.analysis_modal.active = false;
            self.forget_the_rows_read();
            self.spawn_async_collect("Loading matching rows...");
        }
        None
    }

    fn return_from_quality_evidence(&mut self, reopen_analysis: bool) -> bool {
        let Some(original) = self.quality_evidence_return.take() else {
            return false;
        };
        self.jobs.advance();
        self.len_count_inflight = None;
        self.data_table_state = Some(*original);
        self.quality_evidence_label = None;
        self.analysis_modal.active = reopen_analysis;
        self.busy = false;
        self.status_message = None;
        true
    }

    fn restore_recent_quality_plan(&mut self) {
        let Some(view_generation) = self
            .data_table_state
            .as_ref()
            .map(DataTableState::len_generation)
        else {
            return;
        };
        if self.analysis_modal.data_quality_plan != data_quality::DataQualityPlan::default() {
            return;
        }
        if let Some(cached) = self.quality_cache.iter().find(|entry| {
            entry.dataset_generation == self.dataset_generation
                && entry.view_generation == view_generation
        }) {
            self.analysis_modal.data_quality_plan = cached.plan.clone();
        }
    }

    /// What the data offers Setup's choices. Read from the schema and the rows
    /// already on screen: nothing here reads the source.
    fn quality_plan_context(&self) -> analysis_modal::PlanContext {
        let Some(state) = self.data_table_state.as_ref() else {
            return analysis_modal::PlanContext::default();
        };
        let plan = &self.analysis_modal.data_quality_plan;
        let scope = &plan.scope;
        let schema = state.schema();
        let mut partitions = state.partition_columns().unwrap_or_default().to_vec();
        // A directory whose files agree opens as one scan and names no partition
        // columns; its directory names still do.
        if partitions.is_empty()
            && let Some(dir) = self.path.as_ref().filter(|path| path.is_dir())
        {
            partitions = DataTableState::discover_hive_partition_columns(dir)
                .into_iter()
                .filter(|column| schema.get(column).is_some())
                .collect();
        }
        // Date and time columns, then text read as time: a window can split by either.
        let mut time_columns: Vec<(String, bool)> = state
            .quality_temporal_columns(scope)
            .into_iter()
            .map(|column| {
                let has_time =
                    !matches!(schema.get(&column), Some(polars::prelude::DataType::Date));
                (column, has_time)
            })
            .collect();
        for format in &plan.time_formats {
            if !time_columns
                .iter()
                .any(|(column, _)| *column == format.column)
            {
                time_columns.push((
                    format.column.clone(),
                    format.kind == data_quality::TimeKind::Datetime,
                ));
            }
        }
        analysis_modal::PlanContext {
            partitions,
            time_columns,
            files: state.quality_source_file_count() > 1,
            text_columns: state
                .quality_text_columns(scope)
                .into_iter()
                .map(|column| {
                    let examples = state.buffered_values(&column, 3);
                    (column, examples)
                })
                .collect(),
        }
    }

    /// The columns a time role can be given: date and time columns, then text,
    /// which a role reads through its Text as time format.
    pub(crate) fn quality_time_candidates(&self) -> Vec<String> {
        let Some(state) = self.data_table_state.as_ref() else {
            return Vec::new();
        };
        let scope = &self.analysis_modal.data_quality_plan.scope;
        let mut columns = state.quality_temporal_columns(scope);
        columns.extend(state.quality_text_columns(scope));
        columns
    }

    /// The columns Column intent lists: the draft's scope's, from the schema.
    pub(crate) fn quality_intent_columns(&self) -> Vec<(String, polars::prelude::DataType)> {
        self.data_table_state
            .as_ref()
            .map(|state| {
                crate::widgets::quality_intent::intent_columns(
                    state.quality_schema(&self.analysis_modal.data_quality_plan.scope),
                )
            })
            .unwrap_or_default()
    }

    /// Open the intent form on the column under the cursor of the Column intent list.
    fn open_intent_form(&mut self) {
        let columns = self.quality_intent_columns();
        let modal = &mut self.analysis_modal;
        let Some((column, dtype)) = columns.get(modal.data_quality_plan_field) else {
            return;
        };
        let plan = &modal.data_quality_plan;
        modal.data_quality_intent_form = Some(intent_modal::IntentForm::new(
            column,
            dtype.clone(),
            plan.time_format(column).cloned(),
            &plan.intent,
            &self.theme,
        ));
    }

    /// Keys while the intent form is open: Tab and ↑↓ walk its rows, Space and ←→
    /// change a choice, text fields type, Enter stages the declaration in Setup's
    /// draft and Esc drops the form's edits. Nothing here reads.
    fn intent_form_key(&mut self, event: &KeyEvent) {
        let modal = &mut self.analysis_modal;
        let Some(form) = modal.data_quality_intent_form.as_mut() else {
            return;
        };
        let typing = form.typing();
        match event.code {
            KeyCode::Esc => modal.data_quality_intent_form = None,
            KeyCode::Enter => match form.apply(&mut modal.data_quality_plan.intent) {
                Ok(()) => modal.data_quality_intent_form = None,
                Err(error) => form.error = Some(error),
            },
            KeyCode::Tab | KeyCode::Down => form.move_field(true),
            KeyCode::BackTab | KeyCode::Up => form.move_field(false),
            KeyCode::Char('j') if !typing => form.move_field(true),
            KeyCode::Char('k') if !typing => form.move_field(false),
            KeyCode::Char(' ') if !typing => form.adjust(true),
            KeyCode::Left | KeyCode::Char('h') if !typing => form.adjust(false),
            KeyCode::Right | KeyCode::Char('l') if !typing => form.adjust(true),
            _ if typing => {
                if let Some(input) = form.input_mut() {
                    let _ = input.handle_key(event, None);
                }
                form.error = None;
            }
            _ => {}
        }
    }

    /// What a Data Quality run reads from, as far as the app knows without reading:
    /// where it was opened from, its files, and what the view does to the rows when
    /// the scope is the view.
    fn quality_source_identity(
        &self,
        state: &DataTableState,
        scope: &data_quality::QualityScope,
    ) -> crate::quality_export::SourceIdentity {
        let format = self
            .original_file_format
            .map(|format| format.as_str().to_string())
            .or_else(|| {
                self.path
                    .as_ref()
                    .and_then(|path| path.extension())
                    .and_then(|extension| extension.to_str())
                    .map(str::to_string)
            });
        let mut view = Vec::new();
        if !scope.uses_source() {
            if !state.get_active_query().is_empty() {
                view.push(format!("query: {}", state.get_active_query()));
            }
            if !state.get_active_sql_query().is_empty() {
                view.push(format!("SQL: {}", state.get_active_sql_query()));
            }
            if !state.get_active_fuzzy_query().is_empty() {
                view.push(format!("search: {}", state.get_active_fuzzy_query()));
            }
            for (index, filter) in state.view_filters().iter().enumerate() {
                let join = if index == 0 {
                    String::new()
                } else {
                    format!("{} ", filter.logical_op.as_str())
                };
                view.push(format!(
                    "filter: {join}{} {} {}",
                    filter.column,
                    filter.operator.as_str(),
                    filter.value
                ));
            }
            if state.reshape_source().is_some() {
                view.push("reshaped: pivot or melt".to_string());
            }
        }
        let remote = state.is_remote_source();
        // A local path made whole, so the report names the file wherever it is read;
        // no file system access.
        let piped = self.reads_stdin();
        let location = self.path.as_ref().map(|path| {
            match std::path::absolute(path).ok().filter(|_| !remote && !piped) {
                Some(path) => path.display().to_string(),
                None => path.display().to_string(),
            }
        });
        crate::quality_export::SourceIdentity {
            location,
            remote,
            format,
            view,
            ..crate::quality_export::SourceIdentity::default()
        }
        .with_files(state.quality_source_file_names())
    }

    /// The export dialog, on a name made from the dataset's.
    fn open_quality_export(&mut self) {
        let stem = self
            .path
            .as_ref()
            .and_then(|path| path.file_stem())
            .and_then(|stem| stem.to_str())
            .filter(|stem| !stem.is_empty())
            .unwrap_or("data")
            .to_string();
        self.analysis_modal.data_quality_export =
            Some(crate::quality_export::ExportForm::new(&stem, &self.theme));
    }

    /// Keys while the export dialog is open: Tab moves between the path and the
    /// form, the arrows or Space change the form, Enter writes (asking first over a
    /// file that exists), Esc closes it.
    fn quality_export_key(&mut self, event: &KeyEvent) -> Option<AppEvent> {
        let form = self.analysis_modal.data_quality_export.as_mut()?;
        match event.code {
            KeyCode::Esc => self.analysis_modal.data_quality_export = None,
            KeyCode::Tab | KeyCode::BackTab | KeyCode::Up | KeyCode::Down => form.toggle_focus(),
            KeyCode::Left
            | KeyCode::Right
            | KeyCode::Char(' ')
            | KeyCode::Char('h')
            | KeyCode::Char('l')
                if form.on_format =>
            {
                form.cycle_format();
            }
            KeyCode::Enter => match form.target() {
                Err(error) => form.error = Some(error),
                Ok((path, format)) => {
                    if path.exists() {
                        let shown = path.display().to_string();
                        self.pending_quality_export = Some((path, format));
                        self.confirmation_modal.show_destructive(
                            format!("File already exists:\n{shown}\n\nOverwrite it?"),
                            "Overwrite",
                        );
                    } else {
                        self.analysis_modal.data_quality_export = None;
                        return Some(AppEvent::QualityReportExport(
                            path,
                            format,
                            Overwrite::Forbid,
                        ));
                    }
                }
            },
            _ if !form.on_format => {
                let _ = form.path.handle_key(event, None);
                form.error = None;
            }
            _ => {}
        }
        None
    }

    /// Space on a Setup row: the Sample form, the role editor, or the row's choices.
    fn open_setup_row(&mut self) -> Option<AppEvent> {
        use analysis_modal::SetupRow;
        self.analysis_modal.data_quality_setup_note = None;
        match self.analysis_modal.setup_row() {
            SetupRow::Sample => self.open_quality_sample_form(),
            SetupRow::TimeRoles => {
                // With no date, time or text column there is no role to assign.
                if !self.quality_time_candidates().is_empty() {
                    self.analysis_modal.data_quality_plan_before_edit =
                        Some(self.analysis_modal.data_quality_plan.clone());
                    self.analysis_modal
                        .set_quality_page(data_quality::QualityPage::TimeRoles);
                    self.analysis_modal.data_quality_plan_field = 0;
                }
            }
            SetupRow::Intent => {
                if !self.quality_intent_columns().is_empty() {
                    self.analysis_modal.data_quality_plan_before_edit =
                        Some(self.analysis_modal.data_quality_plan.clone());
                    self.analysis_modal
                        .set_quality_page(data_quality::QualityPage::Intent);
                    self.analysis_modal.data_quality_plan_field = 0;
                }
            }
            SetupRow::Intervals => {
                // Two assigned roles make the first pair to choose.
                if !self
                    .analysis_modal
                    .data_quality_plan
                    .candidate_pairs()
                    .is_empty()
                {
                    self.analysis_modal.data_quality_plan_before_edit =
                        Some(self.analysis_modal.data_quality_plan.clone());
                    self.analysis_modal
                        .set_quality_page(data_quality::QualityPage::IntervalPairs);
                    self.analysis_modal.data_quality_plan_field = 0;
                }
            }
            SetupRow::Expected => {
                // Windows are what a gap is counted in; with no time-window grain
                // there is nothing to expect yet.
                if matches!(
                    self.analysis_modal.data_quality_plan.grain,
                    data_quality::QualityGrain::TimeWindows { .. }
                ) {
                    self.analysis_modal.data_quality_expected_form =
                        Some(analysis_modal::ExpectedForm::new(
                            &self.analysis_modal.data_quality_plan,
                            &self.theme,
                        ));
                    self.analysis_modal
                        .set_quality_page(data_quality::QualityPage::ExpectedWindows);
                }
            }
            row => {
                let context = self.quality_plan_context();
                self.analysis_modal.open_plan_picker(row, &context);
            }
        }
        None
    }

    /// Keys in the Expected editor: ↑↓ the row, ←→ the cadence, typing in From and
    /// Before. Enter writes it into the draft, or says on its own line why it cannot;
    /// Esc leaves the draft as it was. Either way back to Setup's Expected row.
    fn expected_form_key(&mut self, event: &KeyEvent) {
        let every = match &self.analysis_modal.data_quality_plan.grain {
            data_quality::QualityGrain::TimeWindows { every, .. } => every.clone(),
            _ => String::new(),
        };
        let Some(form) = self.analysis_modal.data_quality_expected_form.as_mut() else {
            return;
        };
        let typing = form.typing();
        match event.code {
            KeyCode::Esc => {}
            KeyCode::Enter => match form.expected() {
                Ok(expected) => self.analysis_modal.data_quality_plan.expected = expected,
                Err(problem) => {
                    form.error = Some(problem);
                    return;
                }
            },
            KeyCode::Down | KeyCode::Tab => {
                form.move_field(true);
                return;
            }
            KeyCode::Up | KeyCode::BackTab => {
                form.move_field(false);
                return;
            }
            KeyCode::Char('j') if !typing => {
                form.move_field(true);
                return;
            }
            KeyCode::Char('k') if !typing => {
                form.move_field(false);
                return;
            }
            KeyCode::Left | KeyCode::Char('h') if !typing => {
                form.cycle(&every, false);
                return;
            }
            KeyCode::Right | KeyCode::Char('l') | KeyCode::Char(' ') if !typing => {
                form.cycle(&every, true);
                return;
            }
            _ => {
                if let Some(input) = form.input_mut() {
                    let _ = input.handle_key(event, None);
                    form.error = None;
                }
                return;
            }
        }
        self.analysis_modal.data_quality_expected_form = None;
        self.analysis_modal.data_quality_setup_note = None;
        self.analysis_modal
            .set_quality_page(data_quality::QualityPage::Setup);
        self.analysis_modal.data_quality_plan_field = analysis_modal::SetupRow::Expected.index();
    }

    /// `w` on Trends: the next coarser grain, staged in Setup with the Grain row under
    /// the cursor, for segments the sample reached too thinly. Nothing runs until
    /// Enter, and Setup's Read says what that run reads; Esc puts the grain back.
    fn stage_coarser_grain(&mut self) {
        let Some(coarser) = self.analysis_modal.quality_result_plan().coarser_grain() else {
            return;
        };
        self.open_quality_setup();
        let plan = &mut self.analysis_modal.data_quality_plan;
        plan.grain = coarser;
        plan.baseline_segment = None;
        self.analysis_modal.data_quality_plan_field = analysis_modal::SetupRow::Grain.index();
    }

    /// Enter in a Setup row's list: take the choice, and after a text column, ask
    /// for its format with the values on screen beside each one.
    fn choose_setup_picker(&mut self) {
        self.analysis_modal.data_quality_setup_note = None;
        if let Some(column) = self.analysis_modal.choose_plan_picker() {
            let examples = self
                .data_table_state
                .as_ref()
                .map(|state| state.buffered_values(&column, 3))
                .unwrap_or_default();
            self.analysis_modal.open_format_picker(&column, &examples);
        }
    }

    /// The Setup setting the Data Quality page on screen lacks before it can show
    /// anything; Enter opens it, and the control bar says so.
    pub(crate) fn quality_page_setup(&self) -> Option<data_quality::QualitySetup> {
        let modal = &self.analysis_modal;
        data_quality::page_setup(
            modal.data_quality_page,
            modal.quality_result_plan(),
            modal.data_quality_results.as_ref(),
            self.has_quality_time_columns(),
        )
    }

    /// Whether the plan's scope has a column it reads as time: a date or time
    /// column, or text given a format. What an empty Trends page points to.
    pub(crate) fn has_quality_time_columns(&self) -> bool {
        !self
            .analysis_modal
            .data_quality_plan
            .time_formats
            .is_empty()
            || self.data_table_state.as_ref().is_some_and(|state| {
                !state
                    .quality_temporal_columns(&self.analysis_modal.data_quality_plan.scope)
                    .is_empty()
            })
    }

    /// Whether Data Quality's retained rows are the rows `plan` reads, so a run
    /// starts from them rather than from the source. They serve any grain: every
    /// column is kept, and where each row sat.
    pub(crate) fn quality_kept_serves(&self, plan: &data_quality::DataQualityPlan) -> bool {
        plan.compute == data_quality::QualityCompute::Sample
            && self.kept_quality_sample(&plan.sample()).is_some()
    }

    /// Where a run of `plan` gets its exact segment totals: from the retained rows'
    /// counts, from the pass that reads a new sample, or from a read of their own.
    pub(crate) fn quality_segment_count(
        &self,
        plan: &data_quality::DataQualityPlan,
    ) -> data_quality::SegmentCount {
        if plan.compute != data_quality::QualityCompute::Sample {
            return data_quality::SegmentCount::NotNeeded;
        }
        match self.kept_quality_sample(&plan.sample()) {
            Some(kept) => kept.segment_count(plan),
            None => data_quality::fresh_segment_count(plan, self.quality_may_read_blocks(plan)),
        }
    }

    /// Whether the rows `plan` reads were read this session and released to the
    /// memory budget, so a Run reads them again.
    pub(crate) fn quality_released(&self, plan: &data_quality::DataQualityPlan) -> bool {
        let Some(view_generation) = self
            .data_table_state
            .as_ref()
            .map(DataTableState::len_generation)
        else {
            return false;
        };
        let sample = plan.sample();
        plan.compute == data_quality::QualityCompute::Sample
            && self
                .quality_released
                .iter()
                .any(|(dataset, view, released)| {
                    *dataset == self.dataset_generation
                        && *view == view_generation
                        && *released == sample
                })
    }

    /// Whether the dataset is one Parquet or IPC file, the one kind the sampler may
    /// read seeded runs of.
    fn quality_one_columnar_file(&self) -> bool {
        let Some(state) = self.data_table_state.as_ref() else {
            return false;
        };
        let columnar = matches!(
            self.original_file_format,
            Some(ExportFormat::Parquet | ExportFormat::Ipc)
        ) || self.path.as_ref().is_some_and(|path| {
            path.extension()
                .and_then(|extension| extension.to_str())
                .is_some_and(|extension| {
                    matches!(
                        extension.to_ascii_lowercase().as_str(),
                        "parquet" | "pq" | "arrow" | "arrows" | "ipc" | "feather"
                    )
                })
        });
        columnar && state.loaded_file_count() == 1
    }

    /// Whether a random sample of `plan` reads seeded runs of one file rather than
    /// stream every row, as Setup's Read says. Told from the path and the view, since
    /// the sampler's own test needs the plan built. Yes only where the scan is read as
    /// loaded: the whole source whatever the view, or a view that picks no rows (a
    /// sort does not count: samples read the view unsorted). Where it is not sure,
    /// Setup names the longer read.
    pub(crate) fn quality_reads_blocks(&self, plan: &data_quality::DataQualityPlan) -> bool {
        let Some(state) = self.data_table_state.as_ref() else {
            return false;
        };
        self.quality_one_columnar_file()
            && match plan.scope {
                data_quality::QualityScope::WholeSource => true,
                data_quality::QualityScope::CurrentView => !state.changes_rows(),
                // Read in the order on screen, sort included.
                data_quality::QualityScope::FirstRows(_)
                | data_quality::QualityScope::ViewRows { .. } => {
                    state.source_file_count() == Some(1)
                }
                _ => false,
            }
    }

    /// Whether a random sample of `plan` may read seeded runs: where
    /// [`Self::quality_reads_blocks`] is sure, and wherever the view may still read
    /// the scan as loaded, a query's included.
    ///
    /// Leans to yes: seeded runs see too few rows to count segments, so a yes is what
    /// makes Setup name a count pass, and a run that streams after all counts in its
    /// one pass and reads less than Setup said, never more.
    pub(crate) fn quality_may_read_blocks(&self, plan: &data_quality::DataQualityPlan) -> bool {
        let Some(state) = self.data_table_state.as_ref() else {
            return false;
        };
        self.quality_reads_blocks(plan)
            || (self.quality_one_columnar_file()
                && state.may_keep_scan_rows()
                && matches!(
                    plan.scope,
                    data_quality::QualityScope::CurrentView
                        | data_quality::QualityScope::FirstRows(_)
                        | data_quality::QualityScope::ViewRows { .. }
                ))
    }

    /// Whether the session cache holds a report measuring what `plan` measures on
    /// this view: the windows it expects are checked against the report, not read.
    pub(crate) fn quality_cached(&self, plan: &data_quality::DataQualityPlan) -> bool {
        let Some(view_generation) = self
            .data_table_state
            .as_ref()
            .map(DataTableState::len_generation)
        else {
            return false;
        };
        self.quality_cache.iter().any(|entry| {
            entry.dataset_generation == self.dataset_generation
                && entry.view_generation == view_generation
                && entry.plan.same_measurement(plan)
        })
    }

    /// Everything Data Quality's runs kept for reuse on this dataset, as `d` in Setup
    /// would release it: sampled rows in memory and a full scan's local copy on
    /// disk. `None` when there is neither.
    pub(crate) fn quality_kept_rows(&self) -> Option<widgets::data_quality::KeptRows> {
        let kept = self
            .quality_samples
            .iter()
            .filter(|kept| kept.dataset_generation == self.dataset_generation)
            .collect::<Vec<_>>();
        let copy_bytes = self
            .quality_copies
            .iter()
            .filter(|kept| kept.dataset_generation == self.dataset_generation)
            .map(|kept| kept.copy.bytes())
            .sum::<u64>();
        (!kept.is_empty() || copy_bytes > 0).then(|| widgets::data_quality::KeptRows {
            samples: kept.len(),
            rows: kept.iter().map(|kept| kept.rows.df().height()).sum(),
            bytes: kept.iter().map(|kept| kept.rows.estimated_bytes()).sum(),
            copy_bytes,
        })
    }

    /// `d` in Setup: let go of every row runs kept, as the memory budget would, and
    /// the local copy a full scan fetched, whose files go from disk. A run that would
    /// have reused either reads again, and Setup's Read says so before Run. Reports
    /// stay: they are results, and showing one reads nothing.
    fn release_quality_rows(&mut self) {
        let Some(kept) = self.quality_kept_rows() else {
            self.flash_note("Nothing kept to release".to_string());
            return;
        };
        for released in std::mem::take(&mut self.quality_samples) {
            self.quality_released.retain(|(dataset, view, sample)| {
                !(*dataset == released.dataset_generation
                    && *view == released.view_generation
                    && *sample == released.sample)
            });
            self.quality_released.insert(
                0,
                (
                    released.dataset_generation,
                    released.view_generation,
                    released.sample,
                ),
            );
        }
        self.quality_released.truncate(QUALITY_RELEASED_REMEMBERED);
        // A run still reading the copy holds it until it ends; then the files go.
        let generation = self.dataset_generation;
        self.quality_copies
            .retain(|kept| kept.dataset_generation != generation);
        if kept.copy_bytes > 0 {
            self.quality_copy_released = Some(generation);
        }
        let rows = format!(
            "{} kept {} ({})",
            numfmt::group_chrome(kept.rows),
            if kept.rows == 1 { "row" } else { "rows" },
            widgets::info::format_bytes(kept.bytes as u64)
        );
        let copy = format!(
            "the local copy ({})",
            widgets::info::format_bytes(kept.copy_bytes)
        );
        self.flash_note(match (kept.samples > 0, kept.copy_bytes > 0) {
            (true, true) => format!("Released {rows} and {copy}; the next run reads again"),
            (false, true) => format!("Released {copy}; the next full scan fetches again"),
            _ => format!("Released {rows}; the next run reads again"),
        });
    }

    /// Rows a sampled Data Quality run read, when they are the rows `sample` names
    /// now: same dataset, same view, same sample.
    fn kept_quality_sample(
        &self,
        sample: &sampling::Sample,
    ) -> Option<std::sync::Arc<data_quality::QualitySample>> {
        self.kept_quality_entry(sample)
            .map(|kept| kept.rows.clone())
    }

    fn kept_quality_entry(&self, sample: &sampling::Sample) -> Option<&KeptQualitySample> {
        let view_generation = self.data_table_state.as_ref()?.len_generation();
        self.quality_samples.iter().find(|kept| {
            kept.dataset_generation == self.dataset_generation
                && kept.view_generation == view_generation
                && &kept.sample == sample
        })
    }

    /// Keep what a run read, newest first, in place of any earlier copy of the same
    /// rows: a later run returns them with the counts it added.
    fn retain_quality_sample(&mut self, kept: &KeptQualitySample) {
        if kept.dataset_generation != self.dataset_generation {
            return;
        }
        self.quality_samples.retain(|entry| !entry.same_rows(kept));
        self.quality_released.retain(|(dataset, view, sample)| {
            !(*dataset == kept.dataset_generation
                && *view == kept.view_generation
                && *sample == kept.sample)
        });
        self.quality_samples.insert(0, kept.clone());
        self.trim_quality_memory();
    }

    /// Hold Data Quality's reports and retained rows to [`QUALITY_MEMORY_BUDGET`].
    ///
    /// A report whose rows are still retained goes first: remaking it reads nothing.
    /// Then the oldest rows, whose next run reads them again, which Setup says. A
    /// report with no rows behind it, a full scan's, goes last: it is the dearest to
    /// remake. The newest report and the newest rows always stay, whatever their size,
    /// so a finished read is never thrown away to make room for itself.
    fn trim_quality_memory(&mut self) {
        loop {
            let used = self
                .quality_cache
                .iter()
                .map(|entry| entry.bytes)
                .sum::<usize>()
                + self
                    .quality_samples
                    .iter()
                    .map(|kept| kept.rows.estimated_bytes())
                    .sum::<usize>();
            if used <= self.quality_memory_budget {
                return;
            }
            let remakeable = self
                .quality_cache
                .iter()
                .enumerate()
                .skip(1)
                .rev()
                .find(|(_, entry)| {
                    entry.plan.compute == data_quality::QualityCompute::Sample
                        && self.quality_samples.iter().any(|kept| {
                            kept.dataset_generation == entry.dataset_generation
                                && kept.view_generation == entry.view_generation
                                && kept.sample == entry.plan.sample()
                        })
                })
                .map(|(index, _)| index);
            if let Some(index) = remakeable {
                self.quality_cache.remove(index);
            } else if self.quality_samples.len() > 1 {
                if let Some(released) = self.quality_samples.pop() {
                    self.quality_released.insert(
                        0,
                        (
                            released.dataset_generation,
                            released.view_generation,
                            released.sample,
                        ),
                    );
                    self.quality_released.truncate(QUALITY_RELEASED_REMEMBERED);
                }
            } else if self.quality_cache.len() > 1 {
                self.quality_cache.pop();
            } else {
                return;
            }
        }
    }

    fn restore_cached_quality(&mut self) -> bool {
        let Some(view_generation) = self
            .data_table_state
            .as_ref()
            .map(DataTableState::len_generation)
        else {
            return false;
        };
        let plan = self.analysis_modal.data_quality_plan.clone();
        let Some(cached) = self.quality_cache.iter().find(|entry| {
            entry.dataset_generation == self.dataset_generation
                && entry.view_generation == view_generation
                && entry.plan.same_measurement(&plan)
        }) else {
            return false;
        };
        let mut results = cached.results.clone();
        if cached.plan != plan {
            // Compared as the plan compares, and kept under the windows it expects now.
            if cached.plan.compares_differently(&plan) {
                results.compare_segments(&plan);
            }
            self.cache_quality_result(&results, plan.clone());
        }
        self.analysis_modal.data_quality_results = Some(results);
        self.analysis_modal.data_quality_last_plan = Some(plan);
        self.analysis_modal.data_quality_from_cache = true;
        self.analysis_modal
            .set_quality_page(data_quality::QualityPage::Overview);
        true
    }

    fn cache_quality_result(
        &mut self,
        results: &data_quality::DataQualityResults,
        plan: data_quality::DataQualityPlan,
    ) {
        let Some(view_generation) = self
            .data_table_state
            .as_ref()
            .map(DataTableState::len_generation)
        else {
            return;
        };
        // One report per measurement: a plan that only expects other windows
        // replaces it.
        self.quality_cache.retain(|entry| {
            !(entry.dataset_generation == self.dataset_generation
                && entry.view_generation == view_generation
                && entry.plan.same_measurement(&plan))
        });
        self.quality_cache.insert(
            0,
            QualityCacheEntry {
                dataset_generation: self.dataset_generation,
                view_generation,
                plan,
                bytes: results.estimated_bytes(),
                results: results.clone(),
            },
        );
        self.trim_quality_memory();
    }

    /// The scope a full scan's passes read: `lf` over a local copy in place of its
    /// remote objects, fetched first when `job` says so. `kept` hears whether the
    /// copy stands in for the source: the copy to keep, or `None`, after which the
    /// dataset's full scans read the source. The copy comes back too, for the caller
    /// to hold while the passes read it.
    fn quality_scope_on_copy(
        lf: LazyFrame,
        job: QualityCopyJob,
        watch: &data_quality::QualityWatch,
        fetch: impl FnOnce(
            &[crate::local_copy::RemoteObject],
            &Path,
        ) -> Result<crate::local_copy::LocalCopy>,
        kept: impl FnOnce(Option<Arc<crate::local_copy::LocalCopy>>),
    ) -> Result<(LazyFrame, Option<Arc<crate::local_copy::LocalCopy>>)> {
        let (copy, fetched) = match job {
            QualityCopyJob::Source => return Ok((lf, None)),
            QualityCopyJob::Kept(copy) => (copy, false),
            QualityCopyJob::Fetch { objects, root } => {
                watch.stage(data_quality::QualityStage::CopyingSource, true, true)?;
                let copy = fetch(&objects, &root).map_err(|error| {
                    if watch.cancelled() {
                        color_eyre::eyre::eyre!(crate::sampling::CANCELLED)
                    } else {
                        error
                    }
                })?;
                (Arc::new(copy), true)
            }
        };
        // The copy must read as the source does, or the source is read as before.
        let local = copy.redirect(&lf).filter(|local| {
            let schemas = (local.clone().collect_schema(), lf.clone().collect_schema());
            matches!(schemas, (Ok(local), Ok(source)) if local == source)
        });
        let Some(local) = local else {
            log::warn!(target: "datui", "local copy does not read as the source; reading the source");
            kept(None);
            return Ok((lf, None));
        };
        if fetched {
            kept(Some(copy.clone()));
        }
        watch.use_copy(data_quality::CopyRead {
            bytes: copy.bytes(),
            objects: copy.objects(),
            fetched,
        });
        Ok((local, Some(copy)))
    }

    /// Copy `objects` under `root`, each streamed from its store and written as it
    /// arrives; a cancel stops it at the next chunk and the partial copy is removed.
    #[cfg(feature = "cloud")]
    fn fetch_quality_copy(
        objects: &[crate::local_copy::RemoteObject],
        root: &Path,
        cloud: &crate::config::CloudConfig,
        runtime: &tokio::runtime::Handle,
        stop: &crate::sampling::ReadWatch,
    ) -> Result<crate::local_copy::LocalCopy> {
        use crate::download::StreamError;
        use object_store::ObjectStoreExt;

        crate::local_copy::LocalCopy::fetch(root, objects, stop, |object, write| {
            let url = object.url.as_str();
            let (_, _, store) = Self::cloud_store_for(Path::new(url), cloud, runtime)?;
            let (_, key) = Self::cloud_bucket_and_key(url)?;
            let path = crate::cloud_browse::object_path(&key);
            let listed = object.etag.clone();
            let open = async move {
                let got = store.get(&path).await.map_err(|e| e.to_string())?;
                // Rewritten since it opened, perhaps at the same size: the copy would
                // not be the dataset on screen.
                if let (Some(listed), Some(fetched)) = (&listed, &got.meta.e_tag)
                    && !crate::local_copy::same_etag(listed, fetched)
                {
                    return Err("it changed since it opened. Open the dataset again".to_string());
                }
                Ok((got.into_stream(), None))
            };
            let watch = stop.clone();
            crate::download::stream_into(runtime, open, move || watch.stopped(), write)
                .map(drop)
                .map_err(|error| match error {
                    StreamError::Write(report) => report,
                    StreamError::Open(e) | StreamError::Read(e) => {
                        color_eyre::eyre::eyre!("Could not copy {url}: {e}")
                    }
                    StreamError::Short { expected, got } => color_eyre::eyre::eyre!(
                        "Could not copy {url}: it ended after {got} of {expected} bytes"
                    ),
                    StreamError::Cut => color_eyre::eyre::eyre!(crate::sampling::CANCELLED),
                })
        })
    }

    /// Where Data Quality's local copies are written.
    fn quality_copies_root(&self) -> PathBuf {
        self.cache.cache_dir().join(crate::local_copy::COPIES_DIR)
    }

    /// `performance.quality_local_copy_mb`, in bytes.
    fn quality_copy_limit(&self) -> u64 {
        self.app_config
            .performance
            .quality_local_copy_mb
            .saturating_mul(1024 * 1024)
    }

    /// Bytes on disk in the copies kept.
    pub fn quality_copy_bytes(&self) -> u64 {
        self.quality_copies
            .iter()
            .map(|kept| kept.copy.bytes())
            .sum()
    }

    /// The copy this dataset's objects were fetched into this session, while kept.
    fn quality_copy_kept(&self) -> Option<&Arc<crate::local_copy::LocalCopy>> {
        let state = self.data_table_state.as_ref()?;
        self.quality_copies
            .iter()
            .find(|kept| {
                kept.dataset_generation == self.dataset_generation
                    && state.each_remote_object().is_some_and(|mut objects| {
                        objects.all(|object| {
                            object.is_some_and(|object| kept.copy.covers(&object.url))
                        })
                    })
            })
            .map(|kept| &kept.copy)
    }

    /// Free bytes where copies are written, asked at most every few seconds.
    fn quality_copy_free_space(&self) -> Option<u64> {
        let root = self.quality_copies_root();
        let Ok(mut cached) = self.quality_copy_free.lock() else {
            return crate::local_copy::free_space(&root);
        };
        match *cached {
            Some((asked, free)) if asked.elapsed() < std::time::Duration::from_secs(5) => free,
            _ => {
                let free = crate::local_copy::free_space(&root);
                *cached = Some((std::time::Instant::now(), free));
                free
            }
        }
    }

    /// How a run of `plan` gets its rows from a remote source, from what the open
    /// learned: no read, and no more than a stat of the cache directory.
    pub(crate) fn quality_copy_plan(
        &self,
        plan: &data_quality::DataQualityPlan,
    ) -> data_quality::CopyPlan {
        use data_quality::{CopyPlan, NoCopy};
        let Some(state) = self.data_table_state.as_ref() else {
            return CopyPlan::NotApplicable;
        };
        if plan.compute != data_quality::QualityCompute::Full || !state.is_remote_source() {
            return CopyPlan::NotApplicable;
        }
        if !state.quality_reads_whole_source(&plan.scope) {
            return CopyPlan::Passes(NoCopy::PartOfTheSource);
        }
        if let Some(copy) = self.quality_copy_kept() {
            return CopyPlan::Kept {
                bytes: copy.bytes(),
                objects: copy.objects(),
            };
        }
        let limit = self.quality_copy_limit();
        if limit == 0 {
            return CopyPlan::Passes(NoCopy::Off);
        }
        if self.quality_copy_unusable == Some(self.dataset_generation) {
            return CopyPlan::Passes(NoCopy::Unusable);
        }
        let Some((bytes, objects)) = state.remote_objects_size() else {
            return CopyPlan::Passes(NoCopy::SizeUnknown);
        };
        if bytes > limit {
            return CopyPlan::Passes(NoCopy::TooLarge { bytes, limit });
        }
        let free = self.quality_copy_free_space();
        if free.is_none_or(|free| bytes > free) {
            return CopyPlan::Passes(NoCopy::NoRoom { bytes, free });
        }
        CopyPlan::Fetch { bytes, objects }
    }

    /// Whether this dataset's copy was released this session, so Run fetches again.
    pub(crate) fn quality_copy_released(&self) -> bool {
        self.quality_copy_released == Some(self.dataset_generation)
    }

    /// Keep a copy a run fetched, newest first. Older copies go past the budget;
    /// the newest stays, so a finished fetch is never thrown away for itself. With
    /// none, the dataset's copy did not read as its source: any kept one goes too.
    fn retain_quality_copy(
        &mut self,
        dataset_generation: u64,
        copy: Option<Arc<crate::local_copy::LocalCopy>>,
    ) {
        if dataset_generation != self.dataset_generation {
            return;
        }
        let Some(copy) = copy else {
            self.quality_copies
                .retain(|kept| kept.dataset_generation != dataset_generation);
            self.quality_copy_unusable = Some(dataset_generation);
            return;
        };
        self.quality_copies.insert(
            0,
            RetainedCopy {
                dataset_generation,
                copy,
            },
        );
        self.quality_copy_released = None;
        let limit = self.quality_copy_limit();
        while self.quality_copies.len() > 1 && self.quality_copy_bytes() > limit {
            self.quality_copies.pop();
        }
    }

    /// Whether keys wait: a job the user is waiting on is running or owed, an errand
    /// is between its phases, or an open is on its way to its dataset.
    pub fn is_busy(&self) -> bool {
        self.busy || self.jobs.holds_keys() || self.loading.waits()
    }

    /// The generation background answers are judged by. Advanced each time work starts
    /// that replaces what is in flight.
    pub fn task_generation(&self) -> u64 {
        self.jobs.generation()
    }

    /// Whether the job `ticket` names is still running and its answer still wanted.
    pub fn job_is_current(&self, ticket: Ticket) -> bool {
        self.jobs.is_current(ticket)
    }

    /// Keep recents, histories and measurements in `cache` from now on. For a test that
    /// reads its store back: every test in a process shares one, and fifty opens
    /// elsewhere push its entries out of the capped recents list.
    pub fn use_cache(&mut self, cache: CacheManager) {
        self.cache = cache;
    }

    /// Path of the dataset currently installed, if any. Exposed for tests that need to
    /// assert an abandoned load did not swap a dataset in after the fact.
    pub fn open_path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    /// Whether the dataset on screen was piped in: named `stdin`, with no file behind
    /// that name.
    fn reads_stdin(&self) -> bool {
        self.opened
            .as_ref()
            .is_some_and(|(paths, _)| matches!(paths.as_slice(), [path] if stdin::is_stdin(path)))
    }

    /// What views are matched against: the dataset's path, or for what was piped in
    /// `-`, which no path criterion fits, so it matches by its columns alone.
    fn view_path(&self) -> Option<&Path> {
        if self.reads_stdin() {
            Some(Path::new(stdin::PATH))
        } else {
            self.path.as_deref()
        }
    }

    /// Whether any leased background work, current or abandoned, has yet to report
    /// back. Exposed for tests that wait for abandoned work to finish rather than
    /// guessing how long it takes.
    pub fn background_work_in_flight(&self) -> bool {
        self.jobs.in_flight()
    }

    /// See the `screen_generation` field.
    pub fn screen_generation(&self) -> u64 {
        self.screen_generation
    }

    /// True while a message is in front of the user that has to be dismissed.
    pub fn modal_showing(&self) -> bool {
        self.error_modal.active || self.confirmation_modal.active
    }

    /// The analysis on screen was run on a sample: what `r` and `a` act on.
    fn analysis_results_are_sampled(&self) -> bool {
        self.analysis_modal.view == analysis_modal::AnalysisView::Main
            && self.analysis_modal.computing.is_none()
            && self
                .analysis_modal
                .current_results()
                .is_some_and(|r| r.sample_size.is_some())
    }

    /// An analysis cancelled whose worker has not exited, and when it was cancelled.
    /// While it runs, Data Quality does not start another beside it, and says so.
    pub(crate) fn cancelled_analysis_running(&self) -> Option<std::time::Instant> {
        self.cancelled_analysis().map(|(since, _)| since)
    }

    /// The newest cancelled analysis or sample read still running, and whether it was
    /// cancelled during a read nothing can stop.
    fn cancelled_analysis(&self) -> Option<(std::time::Instant, bool)> {
        let (since, job) = self.jobs.cancelled_running(Self::reads_for_analysis)?;
        let runs_out = match job {
            Job::Analysis(run) => run.runs_out,
            _ => true,
        };
        Some((since, runs_out))
    }

    /// A read for the Analysis tools: a run, or the sample read to show as a table.
    fn reads_for_analysis(job: &Job) -> bool {
        matches!(job, Job::Analysis(_) | Job::SampleRows)
    }

    /// A cancelled run still going that the screen should say is: at once when the
    /// cancel came during a read nothing can stop, and otherwise only once it has
    /// outlasted the batch it was to stop after.
    pub(crate) fn cancelled_run_shown(&self) -> Option<crate::widgets::data_quality::Cancelling> {
        let (since, read_runs_out) = self.cancelled_analysis()?;
        (read_runs_out || since.elapsed() >= CANCEL_GRACE).then_some(
            crate::widgets::data_quality::Cancelling {
                since,
                read_runs_out,
            },
        )
    }

    /// Work a cancel passed that is still running: leased on a generation since left.
    fn cancelled_work_running(&self) -> bool {
        self.jobs.running_behind()
    }

    /// Where the value tools (Describe, Distribution, Correlation) read the shared
    /// sample from, and what the table already knows of its size. Row ranges are
    /// counted in the order the table shows; every other view scope reads without the
    /// sort, which no statistic needs and which makes a sampled read read everything.
    fn sample_source(&self, state: &DataTableState) -> (sampling::SampleSource, Option<usize>) {
        Self::sample_source_for(state, &self.analysis_modal.sample.scope)
    }

    fn sample_source_for(
        state: &DataTableState,
        scope: &data_quality::QualityScope,
    ) -> (sampling::SampleSource, Option<usize>) {
        if scope.uses_source() {
            let (lf, source) = state.data_quality_source_scan();
            return (sampling::SampleSource::loaded(lf, source), None);
        }
        let lf = match scope {
            data_quality::QualityScope::FirstRows(_)
            | data_quality::QualityScope::ViewRows { .. } => state.lf().clone(),
            _ => state.analysis_lf(),
        };
        (
            sampling::SampleSource::view(lf.select(state.binary_stub_exprs())),
            sampling::view_scope_rows(state.num_rows_if_valid(), scope),
        )
    }

    /// Open the Sample form on a copy of the shared sample. A per-partition sample
    /// splits by a column; partition columns lead the choices, then the columns a
    /// partition is usually made of (text, integers, dates), never floats.
    fn open_sample_form(&mut self) {
        self.open_sample_form_as(false);
    }

    /// A tool with nothing to show yet: the Sample form is its pane, as it stands.
    /// Where the cursor goes is the caller's: into the form when the tool is picked,
    /// back to the tool list when Esc leaves it.
    fn open_first_run_form(&mut self) {
        self.open_sample_form_as(true);
        self.sync_sample_form_focus();
    }

    /// The scope field shows its cursor only while the form has the cursor.
    fn sync_sample_form_focus(&mut self) {
        let focused = self.analysis_modal.focus == analysis_modal::AnalysisFocus::Main;
        if let Some(form) = self.analysis_modal.sample_form.as_mut() {
            let has_cursor = focused || !form.inline;
            form.sync_focus(has_cursor);
        }
    }

    /// Run the tool on screen with the Sample form's sample, or say on the form why
    /// its scope does not parse. In Data Quality the sample is staged in Setup's
    /// draft instead: the form applies, and Run reads.
    fn run_sample_form(&mut self) -> Option<AppEvent> {
        let quality =
            self.analysis_modal.selected_tool == Some(analysis_modal::AnalysisTool::DataQuality);
        let finished = self.analysis_modal.sample_form.as_mut()?.finish();
        match finished {
            Ok(sample) if quality => {
                self.analysis_modal.sample_form = None;
                self.analysis_modal.data_quality_plan.adopt_sample(&sample);
                self.analysis_modal.data_quality_setup_note = None;
                None
            }
            // The form stays open, as filled, while a cancelled run finishes.
            Ok(_) if self.read_waits_for_cancelled() => None,
            Ok(sample) => {
                self.analysis_modal.sample_form = None;
                self.apply_sample(sample)
            }
            Err(error) => {
                if let Some(form) = self.analysis_modal.sample_form.as_mut() {
                    form.error = Some(error);
                }
                None
            }
        }
    }

    fn open_sample_form_as(&mut self, inline: bool) {
        let sample = self.analysis_modal.sample.clone();
        self.open_sample_form_on(&sample, inline);
    }

    /// `s` in Data Quality: the Sample form over Setup, on the draft's sample. Its
    /// Enter stages the sample in the draft and returns to Setup; only Run reads.
    fn open_quality_sample_form(&mut self) {
        self.open_quality_setup();
        let sample = self.analysis_modal.data_quality_plan.sample();
        self.open_sample_form_on(&sample, false);
    }

    fn open_sample_form_on(&mut self, sample: &sampling::Sample, inline: bool) {
        let Some(state) = self.data_table_state.as_ref() else {
            return;
        };
        let mut partition_columns = state.partition_columns().unwrap_or_default().to_vec();
        let mut partition_values = Vec::new();
        // A directory whose files agree opens as one scan and names no partition
        // columns; its directory names still do. One branch of the tree is walked for
        // the columns and one listing read for the first column's values: local,
        // and small next to opening the dataset.
        if let Some(dir) = self.path.as_ref().filter(|path| path.is_dir()) {
            if partition_columns.is_empty() {
                partition_columns = DataTableState::discover_hive_partition_columns(dir)
                    .into_iter()
                    .filter(|column| state.schema().get(column).is_some())
                    .collect();
            }
            if let Some(first) = partition_columns.first() {
                let prefix = format!("{first}=");
                let mut values: Vec<String> = std::fs::read_dir(dir)
                    .into_iter()
                    .flatten()
                    .flatten()
                    .filter_map(|entry| {
                        let name = entry.file_name().to_string_lossy().to_string();
                        name.strip_prefix(&prefix).map(str::to_string)
                    })
                    .collect();
                values.sort();
                if !values.is_empty() {
                    partition_values.push((first.clone(), values));
                }
            }
        }
        // An equal-per-value sample splits by a column: partition columns first, then
        // text, the usual stuff of a group (a ticker, a region), then dates and
        // integers. Never floats.
        let mut value_columns = partition_columns.clone();
        for kind in 0..3 {
            for (name, dtype) in state.schema().iter() {
                let rank = match dtype {
                    DataType::String | DataType::Categorical(..) | DataType::Boolean => 0,
                    DataType::Date => 1,
                    dtype if dtype.is_integer() => 2,
                    _ => continue,
                };
                if rank == kind && !value_columns.iter().any(|column| column == name.as_str()) {
                    value_columns.push(name.to_string());
                }
            }
        }
        let context = sample_modal::SampleContext {
            view_rows: state.num_rows_if_valid(),
            filtered: state.changes_rows(),
            files: state.quality_source_file_names().to_vec(),
            partition_columns,
            partition_values,
            time_columns: state.quality_temporal_columns(&data_quality::QualityScope::WholeSource),
            value_columns,
        };
        let mut form = sample_modal::SampleForm::new(sample, context, &self.theme);
        form.inline = inline;
        self.analysis_modal.sample_form = Some(form);
        self.sync_sample_form_focus();
    }

    fn sample_form_key(&mut self, event: &KeyEvent) -> Option<AppEvent> {
        let form = self.analysis_modal.sample_form.as_mut()?;
        let typing = form.field.is_text();
        let on_files = form.field == sample_modal::SampleField::Files;
        let file_count = form.context.files.len();
        match event.code {
            // In a tool's empty pane the form stays, as it was: Esc discards the
            // edit and hands the cursor back to the tool list.
            KeyCode::Esc if form.inline => {
                self.analysis_modal.focus = analysis_modal::AnalysisFocus::Sidebar;
                self.open_first_run_form();
            }
            KeyCode::Esc => self.analysis_modal.sample_form = None,
            KeyCode::Enter => return self.run_sample_form(),
            KeyCode::Down | KeyCode::Tab => form.move_field(true),
            KeyCode::Up | KeyCode::BackTab => form.move_field(false),
            KeyCode::Char('j') if !typing => form.move_field(true),
            KeyCode::Char('k') if !typing => form.move_field(false),
            KeyCode::Left | KeyCode::Char('h') if !typing => form.adjust(false),
            KeyCode::Right | KeyCode::Char('l') if !typing => form.adjust(true),
            KeyCode::PageDown if on_files => {
                form.file_offset = (form.file_offset + crate::widgets::sample_form::FILES_SHOWN)
                    .min(file_count.saturating_sub(1));
            }
            KeyCode::PageUp if on_files => {
                form.file_offset = form
                    .file_offset
                    .saturating_sub(crate::widgets::sample_form::FILES_SHOWN);
            }
            _ if typing => {
                if let Some(input) = form.input_mut(form.field) {
                    let _ = input.handle_key(event, None);
                }
                form.error = None;
            }
            _ => {}
        }
        None
    }

    /// Read the shared sample, as the tool on screen reads it, to show as a table.
    ///
    /// Data Quality's last sample is kept and cut when it is these rows; any other is
    /// drawn again from its seed, which makes it the same rows the tool measured.
    fn read_sample_view(&mut self) -> Option<AppEvent> {
        let sample = self.analysis_modal.sample.clone();
        self.read_sample_rows(sample, None)
    }

    /// Read `sample` off the UI thread and show its rows: all of them, or only a
    /// finding's, under the finding's label. The sample is drawn again from its seed,
    /// so these are the rows the tool measured.
    fn read_sample_rows(
        &mut self,
        sample: sampling::Sample,
        evidence: Option<(quality_report::EvidenceRows, String)>,
    ) -> Option<AppEvent> {
        let state = self.data_table_state.as_ref()?;
        let (source, known_total) = Self::sample_source_for(state, &sample.scope);
        let streaming = self.app_config.performance.polars_streaming;
        // The rows Data Quality just measured, when they are the rows asked for: cut
        // from memory rather than drawn again from the files.
        let kept = self.kept_quality_sample(&sample).map(|kept| {
            let columns: Vec<_> = state
                .schema()
                .iter_names()
                .filter(|name| kept.df().column(name.as_str()).is_ok())
                .map(|name| polars::prelude::col(name.clone()))
                .collect();
            (kept, columns)
        });
        if kept.is_none() && self.read_waits_for_cancelled() {
            return None;
        }
        self.analysis_modal.computing = Some(AnalysisProgress::new(if evidence.is_some() {
            "Reading the matching sampled rows"
        } else {
            "Reading the sample"
        }));
        self.spawn_job(Job::SampleRows, Some("Reading the sample..."), move |_| {
            // The columns shown are the table's; a finding is cut from every column
            // the run read first, so duplicates are judged as the run judged them.
            let (rows, columns) = match kept {
                Some((kept, columns)) => (Ok(kept.analysis_rows(kept.df().clone())), Some(columns)),
                None => (
                    source
                        .cut(&sample.scope)
                        .and_then(|lf| sampling::read(&lf, &sample, known_total, streaming)),
                    None,
                ),
            };
            let shown = |df: polars::prelude::DataFrame| match &columns {
                Some(columns) => polars::prelude::IntoLazy::lazy(df)
                    .select(columns.clone())
                    .collect()
                    .map_err(color_eyre::eyre::Report::from),
                None => Ok(df),
            };
            let read = rows.and_then(|rows| {
                let label = format!(
                    "Sample {} {}",
                    crate::glyphs::get().middot,
                    sample.outcome(
                        rows.total_rows,
                        rows.sample_size,
                        rows.per_value.as_ref().map(|per_value| per_value.kept),
                    )
                );
                match evidence {
                    Some((quality_report::EvidenceRows::Duplicates, label)) => {
                        // Every column the run grouped by: the scope's own, not the
                        // row numbers kept beside them.
                        let keys = rows
                            .df
                            .get_column_names()
                            .into_iter()
                            .filter(|name| !name.starts_with("__datui"))
                            .cloned()
                            .collect::<Vec<_>>();
                        let df = data_quality::duplicate_rows(
                            polars::prelude::IntoLazy::lazy(rows.df),
                            &keys,
                            streaming,
                        )?;
                        Ok((shown(df)?, label))
                    }
                    Some((quality_report::EvidenceRows::Matching(predicate), label)) => {
                        let df = polars::prelude::IntoLazy::lazy(rows.df)
                            .filter(predicate)
                            .collect()?;
                        Ok((shown(df)?, label))
                    }
                    // Files are read from the scope, never from a sample.
                    Some((quality_report::EvidenceRows::Files(_), label)) => {
                        Ok((shown(rows.df)?, label))
                    }
                    None => Ok((shown(rows.df)?, label)),
                }
            });
            let (df, label) = read.map_err(|error| format!("{error}"))?;
            Ok(Answer::Sample { df, label })
        });
        None
    }

    /// Put the sample's rows in the table viewer in place of the table, as Data
    /// Quality's drill-in does; Esc brings the table and Analysis back.
    fn show_sample_view(&mut self, df: polars::prelude::DataFrame, label: String) {
        let Some(state) = self.data_table_state.as_ref() else {
            return;
        };
        let view = match state.sample_view(df) {
            Ok(view) => view,
            Err(error) => {
                self.error_modal
                    .show(format!("Cannot show the sample: {error}"));
                return;
            }
        };
        if let Some(original) = self.data_table_state.replace(view) {
            self.quality_evidence_return = Some(Box::new(original));
            self.quality_evidence_label = Some(label);
            self.analysis_modal.active = false;
            self.forget_the_rows_read();
            self.spawn_async_collect("Loading the sample...");
        }
    }

    /// Mirror the shared sample into the Data Quality plan, which carries it into the
    /// engine and into the session cache's key. Metadata-only stays metadata-only.
    fn sync_quality_plan(&mut self) {
        let sample = self.analysis_modal.sample.clone();
        self.analysis_modal.data_quality_plan.adopt_sample(&sample);
    }

    /// Open Data Quality Setup: the plan, staged. Edits wait for Run, and Esc puts
    /// back the plan as it stood here. Opening it again while open changes nothing.
    fn open_quality_setup(&mut self) {
        use data_quality::QualityPage;
        let modal = &mut self.analysis_modal;
        if !modal.data_quality_page.is_setup() {
            modal.data_quality_setup_return = modal.data_quality_page.tab();
        }
        if modal.data_quality_setup_before.is_none() {
            modal.data_quality_setup_before = Some(modal.data_quality_plan.clone());
        }
        if modal.data_quality_page != QualityPage::Setup {
            modal.set_quality_page(QualityPage::Setup);
            modal.data_quality_plan_field = 0;
        }
        modal.focus = analysis_modal::AnalysisFocus::Main;
    }

    /// Esc on Setup: every staged edit goes, and the report it came from comes back.
    /// With no report yet, Setup stays in the pane and the cursor goes to the tools.
    fn leave_quality_setup(&mut self) {
        use data_quality::QualityPage;
        let modal = &mut self.analysis_modal;
        if let Some(before) = modal.data_quality_setup_before.take() {
            modal.data_quality_plan = before;
        }
        modal.data_quality_setup_note = None;
        modal.data_quality_confirm_run = false;
        modal.data_quality_picker = None;
        if modal.data_quality_results.is_some() {
            let back = match modal.data_quality_setup_return {
                page if page.is_setup() => QualityPage::Overview,
                page => page,
            };
            modal.set_quality_page(back);
        } else {
            modal.set_quality_page(QualityPage::Setup);
            modal.focus = analysis_modal::AnalysisFocus::Sidebar;
        }
    }

    /// What stops Setup from running as it stands, said on its own line: a time
    /// window on text that has no format to read it with.
    fn quality_setup_problem(&self) -> Option<String> {
        let plan = &self.analysis_modal.data_quality_plan;
        let schema = self.data_table_state.as_ref()?.quality_schema(&plan.scope);
        match &plan.grain {
            data_quality::QualityGrain::TimeWindows { column, .. }
                if plan.compute != data_quality::QualityCompute::Metadata
                    && !plan.reads_as_time(column, schema) =>
            {
                Some(format!(
                    "{column} is text: choose its format under Text as time"
                ))
            }
            _ => None,
        }
    }

    /// Run, from Setup: the one place a Data Quality run starts. The draft becomes
    /// the plan, and its sample the one every tool reads; then the report for it is
    /// shown if one is already here, and otherwise read, once.
    ///
    /// Waits, with the reason on Setup, while a cancelled run is still stopping: a
    /// second read beside it is how memory runs out. A full scan asks first, and
    /// Esc there leaves the draft staged and the last report as it was.
    fn run_quality_setup(&mut self) -> Option<AppEvent> {
        use data_quality::QualityPage;
        if self.cancelled_analysis_running().is_some() {
            self.analysis_modal.data_quality_confirm_run = false;
            self.analysis_modal.data_quality_setup_note = Some(QUALITY_RUN_WAITS.to_string());
            return None;
        }
        if let Some(problem) = self.quality_setup_problem() {
            self.analysis_modal.data_quality_setup_note = Some(problem);
            return None;
        }
        // A report already here, on screen or cached, reads nothing: nothing to confirm.
        let plan = &self.analysis_modal.data_quality_plan;
        let here = (self.analysis_modal.data_quality_results.is_some()
            && self
                .analysis_modal
                .data_quality_last_plan
                .as_ref()
                .is_some_and(|last| last.same_measurement(plan)))
            || self.quality_cached(plan);
        if plan.requires_confirmation() && !here && !self.analysis_modal.data_quality_confirm_run {
            // The prompt is answered with Enter, which only the main pane hears.
            self.analysis_modal.data_quality_confirm_run = true;
            self.analysis_modal.focus = analysis_modal::AnalysisFocus::Main;
            return None;
        }
        self.analysis_modal.data_quality_confirm_run = false;
        self.commit_quality_plan();
        let modal = &mut self.analysis_modal;
        if modal.data_quality_results.is_some()
            && modal.data_quality_last_plan.as_ref() == Some(&modal.data_quality_plan)
        {
            let back = match modal.data_quality_setup_return {
                page if page.is_setup() => QualityPage::Overview,
                page => page,
            };
            modal.set_quality_page(back);
            return None;
        }
        // Only the expected windows or the comparison changed: the report on screen
        // holds every count the windows are checked against and every segment the
        // comparison is worked out from, so it is relabeled, not read again.
        if let (Some(results), Some(last)) = (
            modal.data_quality_results.as_ref(),
            modal.data_quality_last_plan.as_ref(),
        ) && last.same_measurement(&modal.data_quality_plan)
        {
            let mut results = results.clone();
            let plan = modal.data_quality_plan.clone();
            let page = if last.compares_differently(&plan) {
                results.compare_segments(&plan);
                QualityPage::Segments
            } else {
                QualityPage::Trends
            };
            modal.data_quality_results = Some(results.clone());
            modal.data_quality_last_plan = Some(plan.clone());
            modal.set_quality_page(page);
            self.cache_quality_result(&results, plan);
            return None;
        }
        if self.restore_cached_quality() {
            return None;
        }
        self.analysis_modal.data_quality_from_cache = false;
        let mut progress = AnalysisProgress::new("Preparing the plan");
        if self.quality_kept_serves(&self.analysis_modal.data_quality_plan) {
            progress.reuse = Some("Starts from rows a run already read".to_string());
        }
        self.analysis_modal.computing = Some(progress);
        self.busy = true;
        Some(AppEvent::AnalysisDataQualityCompute)
    }

    /// The draft is the plan now: Setup closes on it, and its sample becomes the
    /// one every tool reads. The other tools' results were of the old sample, so
    /// they go; Data Quality's last report stays, labeled with what it measured,
    /// until the run replaces it.
    fn commit_quality_plan(&mut self) {
        let modal = &mut self.analysis_modal;
        let sample = modal.data_quality_plan.sample();
        if sample != modal.sample {
            modal.describe_results = None;
            modal.distribution_results = None;
            modal.correlation_results = None;
        }
        modal.sample = sample;
        modal.sample_dataset = Some(self.dataset_generation);
        modal.sample_run_for = Some(self.dataset_generation);
        modal.data_quality_setup_before = None;
        modal.data_quality_setup_note = None;
        modal.data_quality_picker = None;
    }

    /// Adopt a new shared sample: every tool's results were of the old one, so all of
    /// them go, and the tool on screen runs again. Data Quality only takes it into
    /// its plan: nothing reads until its Run.
    fn apply_sample(&mut self, sample: sampling::Sample) -> Option<AppEvent> {
        // A run it would start waits for a cancelled one, with every result kept.
        if self.analysis_modal.selected_tool != Some(analysis_modal::AnalysisTool::DataQuality)
            && self.read_waits_for_cancelled()
        {
            return None;
        }
        // A first run on the sample as it stands takes nothing from the other tools.
        if sample != self.analysis_modal.sample {
            self.analysis_modal.describe_results = None;
            self.analysis_modal.distribution_results = None;
            self.analysis_modal.correlation_results = None;
            self.analysis_modal.data_quality_results = None;
            self.analysis_modal.data_quality_last_plan = None;
            self.analysis_modal.data_quality_from_cache = false;
        }
        self.analysis_modal.sample = sample;
        self.analysis_modal.sample_dataset = Some(self.dataset_generation);
        self.analysis_modal.sample_run_for = Some(self.dataset_generation);
        self.sync_quality_plan();
        if self.analysis_modal.selected_tool == Some(analysis_modal::AnalysisTool::DataQuality) {
            return None;
        }
        self.start_analysis_run()
    }

    /// A cancelled analysis is still reading: say so, and start no read beside it.
    fn read_waits_for_cancelled(&mut self) -> bool {
        if self.cancelled_analysis_running().is_none() {
            return false;
        }
        self.flash_note(ANALYSIS_READ_WAITS.to_string());
        true
    }

    /// Run the selected tool again from scratch, as `r` and `a` do.
    fn start_analysis_run(&mut self) -> Option<AppEvent> {
        let tool = self.analysis_modal.selected_tool?;
        if tool != analysis_modal::AnalysisTool::DataQuality && self.read_waits_for_cancelled() {
            return None;
        }
        let (phase, event) = match tool {
            analysis_modal::AnalysisTool::Describe => {
                self.analysis_modal.describe_results = None;
                self.analysis_computation = Some(AnalysisComputationState {
                    df: None,
                    schema: None,
                    partial_stats: Vec::new(),
                    current: 0,
                    total: 0,
                    total_rows: 0,
                    sample_seed: self.analysis_modal.sample.seed,
                    sample_size: None,
                });
                ("Describing data", AppEvent::AnalysisChunk)
            }
            analysis_modal::AnalysisTool::DistributionAnalysis => {
                self.analysis_modal.distribution_results = None;
                (
                    "Analyzing distributions",
                    AppEvent::AnalysisDistributionCompute,
                )
            }
            analysis_modal::AnalysisTool::CorrelationMatrix => {
                self.analysis_modal.correlation_results = None;
                (
                    "Computing correlations",
                    AppEvent::AnalysisCorrelationCompute,
                )
            }
            analysis_modal::AnalysisTool::DataQuality => return None,
        };
        self.analysis_modal.computing = Some(AnalysisProgress::new(phase));
        self.busy = true;
        Some(event)
    }

    /// Stop waiting for the analysis in flight.
    ///
    /// The worker's answer is dropped: the bump makes it stale, and its lease no longer
    /// holds the table up. A Data Quality run stops at its next batch or stage; a
    /// collect nothing watches runs to its end. The tool is put back unchosen, so its
    /// view does not sit on a spinner for a run that is not coming; Enter on it runs
    /// it again.
    fn cancel_analysis(&mut self) {
        // The run's record says it is still running until its worker ends, superseded
        // below. Only a Data Quality run has a watch to stop it by, and only its stages
        // say whether they stop partway.
        let reads_out = self
            .analysis_modal
            .computing
            .as_ref()
            .is_some_and(AnalysisProgress::read_runs_out);
        let mut read_runs_out = true;
        if let Some(Job::Analysis(run)) =
            self.jobs.current_mut(|job| matches!(job, Job::Analysis(_)))
        {
            read_runs_out = run.watch.is_none() || reads_out;
            run.runs_out = read_runs_out;
            if let Some(watch) = &run.watch {
                watch.cancel();
            }
        }
        let reading_sample = self
            .jobs
            .current(|job| matches!(job, Job::SampleRows))
            .is_some();
        self.jobs.cancel(Self::reads_for_analysis);
        self.jobs.advance();
        // Keys typed while it ran were typed at the run, which is gone: an impatient
        // second Enter replayed now would start it again behind the Esc.
        self.screen_generation = self.screen_generation.wrapping_add(1);
        self.analysis_modal.computing = None;
        self.analysis_computation = None;
        self.busy = false;
        self.status_message = None;
        // Reading the sample to look at changed nothing on screen; the tool stays.
        if reading_sample {
            self.flash_note("Row view cancelled".to_string());
            return;
        }
        // Data Quality keeps its last report and goes back to Setup, where the plan
        // can be edited while the run winds down. A read that runs to its end is
        // state the header and Setup hold until the worker exits, which a flash could
        // not; a run that stops at its next batch is done, and a flash says so.
        if self.analysis_modal.selected_tool == Some(analysis_modal::AnalysisTool::DataQuality) {
            self.open_quality_setup();
            if !read_runs_out {
                self.flash_note("Run cancelled".to_string());
            }
            return;
        }
        self.analysis_modal.selected_tool = None;
        self.analysis_modal.focus = analysis_modal::AnalysisFocus::Sidebar;
        self.flash_note("Analysis cancelled".to_string());
    }

    /// Whether the Pivot & Melt modal is waiting on a pivot it started.
    pub(crate) fn pivot_computing(&self) -> bool {
        self.input_mode == InputMode::PivotMelt
            && self.jobs.current(|job| matches!(job, Job::Pivot)).is_some()
    }

    /// Stop waiting for the pivot in flight. As with an analysis, the worker runs to
    /// the end and the bump drops its answer. The form stays open with its spec.
    fn cancel_pivot(&mut self) {
        self.jobs.advance();
        self.screen_generation = self.screen_generation.wrapping_add(1);
        self.busy = false;
        self.status_message = None;
        self.flash_note("Pivot cancelled".to_string());
    }

    /// Drill into the group on row `group_index` of the table, whose values are `row`,
    /// and fetch its rows off the UI thread. A drill that fails says why on the control
    /// bar and leaves the grouped view as it was.
    fn drill_into(&mut self, group_index: usize, row: &DataFrame) {
        let Some(state) = self.data_table_state.as_mut() else {
            return;
        };
        let drilled = state.deferred(|s| s.drill_down_with_row(group_index, row));
        match drilled {
            Ok(()) => {
                self.sync_sort_filter_modal();
                self.spawn_async_collect(Self::LOADING_BUFFER);
            }
            Err(e) => self.flash_note(format!(
                "Could not drill in: {}",
                crate::error_display::user_message_from_report(&e, None)
            )),
        }
    }

    /// Read `reader` where `-` reads standard input: what a test pipes in.
    #[doc(hidden)]
    pub fn read_stdin_from(&mut self, reader: impl std::io::Read + Send + 'static) {
        self.stdin_reader = Some(Box::new(reader));
    }

    /// Pass the stream on to `out` for `--tee -`: standard output as the process got
    /// it, or a test's pipe.
    #[doc(hidden)]
    pub fn pass_stdout_to(&mut self, out: impl std::io::Write + Send + 'static) {
        self.stdout_pass = Some(Box::new(out));
    }

    /// The follow of the dataset on screen, while it is followed.
    pub fn follow(&self) -> Option<&crate::follow::Follow> {
        self.data_table_state.as_ref()?.follow()
    }

    /// Whether the follow's rows are on hand: none read in the background is still out,
    /// and the view has taken what was counted. A refresh holds no keys, so a test waits
    /// on this rather than on `is_busy`.
    #[doc(hidden)]
    pub fn follow_settled(&self) -> bool {
        self.rows_in_flight().is_none() && !self.follow().is_some_and(|f| f.behind())
    }

    /// Ask the follow's watcher to look now rather than at the end of its interval.
    #[doc(hidden)]
    pub fn check_follow_now(&self) {
        if let Some(follow) = self.follow() {
            follow.check_now();
        }
    }

    /// A followed file's watcher reported: what it counted waits for the view to take
    /// it, and what the user has to know is flashed.
    fn followed(&mut self, news: &crate::follow::News) {
        let Some(state) = self.data_table_state.as_mut() else {
            return;
        };
        let Some(follow) = state.follow_mut().filter(|f| f.id() == news.id) else {
            return;
        };
        let message = follow.take(&news.change);
        if let Some(handle) = follow.take_held() {
            state.read_followed_through(&handle);
        }
        if let Some(message) = message {
            self.flash_note(message);
        }
        self.catch_up_follow();
    }

    /// Show the rows a follow counted, when the table is on screen with nothing
    /// running: a query, a sidebar, a takeover or a read in progress keeps the view it
    /// has until it is done. The cursor on the last row stays on the last row; anywhere
    /// else it stays put, and the rows below it are counted for the bar.
    fn catch_up_follow(&mut self) {
        if !self.in_normal_table_view()
            || self.is_busy()
            || self.loading.awaiting_dataset()
            || self.rows_in_flight().is_some()
        {
            return;
        }
        self.take_follow_rows(true);
    }

    /// Give the view's frames the rows the follow counted. With `read`, the rows on
    /// screen are read too; without, the table reads them once it is back on screen.
    /// Returns whether there were rows to take.
    fn take_follow_rows(&mut self, read: bool) -> bool {
        let Some(state) = self.data_table_state.as_mut() else {
            return false;
        };
        let on_last_row = state.on_last_row();
        let counted = state.is_num_rows_valid();
        let drawn = state.visible_rows > 0 && counted;
        let Some(follow) = state.follow_mut() else {
            return false;
        };
        // The cursor goes to the last row once the rows that put it there are on hand:
        // moved before, the frame drawn meanwhile has fewer rows than the cursor's
        // place, and the table puts the cursor back on the last row it has.
        let settle = read && drawn && std::mem::take(&mut follow.settle_at_end);
        let to_end = settle || (read && counted && std::mem::take(&mut follow.end_pending));
        let stale = read && std::mem::take(&mut follow.stale_view);
        let at_bottom = to_end || on_last_row || follow.end_pending;
        if at_bottom {
            follow.new_below = 0;
        }
        if !follow.behind() {
            if (to_end && state.scroll_to_end()) || stale {
                self.spawn_collect(None);
            }
            return false;
        }
        let before = follow.shown();
        let (rows, restarted) = follow.catch_up();
        if at_bottom {
            follow.end_pending = true;
        } else if !restarted {
            follow.new_below += rows.saturating_sub(before);
        }
        follow.stale_view = !read;
        state.follow_to(rows, restarted);
        if at_bottom && read {
            if state.is_num_rows_valid() {
                state.aim_at_end();
            } else {
                // A filtered view's end is known once its count lands.
                self.end_after_count = Some(state.len_generation());
            }
        }
        if read && !self.spawn_collect(None) {
            // Nothing to read: the rows on hand already reach the end.
            self.catch_up_follow();
        }
        true
    }

    /// `t` over a surface that keeps the rows it was opened on (Value Counts, Analysis,
    /// a chart): whether the follow has rows for it to take.
    fn follow_rows_waiting(&self) -> bool {
        self.follow().is_some_and(|f| f.behind()) && !self.loading.awaiting_dataset()
    }

    /// Standard input being recorded to the file `--tee` named, for the dataset on
    /// screen.
    pub fn recording(&self) -> Option<&Arc<crate::follow::Spool>> {
        self.data_table_state.as_ref()?;
        self.opened
            .as_ref()
            .and_then(|(_, options)| options.spool.as_ref())
            .map(|handle| handle.spool())
            .filter(|spool| spool.tee().is_some())
    }

    /// Where `key` takes the user out of the dataset: quitting, or home.
    fn leaves(&self, key: &KeyEvent) -> Option<Leaving> {
        if !key.is_press() {
            return None;
        }
        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        match key.code {
            KeyCode::Char('q') if ctrl => Some(Leaving::Quit),
            KeyCode::Char('c') if ctrl => Some(Leaving::Quit),
            KeyCode::Char('o') if ctrl => Some(Leaving::Home),
            KeyCode::Char('Q') if !ctrl && self.in_normal_table_view() => Some(Leaving::Quit),
            KeyCode::Char('q') if !ctrl && self.in_normal_table_view() => {
                Some(if self.opened_from_home {
                    Leaving::Home
                } else {
                    Leaving::Quit
                })
            }
            _ => None,
        }
    }

    /// Ask whether to stop the recording or keep it going while the user leaves.
    fn ask_about_recording(&mut self, leaving: Leaving) {
        let Some(tee) = self.recording().and_then(|spool| spool.tee()) else {
            return;
        };
        let doing = if tee.to_stdout() {
            "passed on"
        } else {
            "recorded"
        };
        let message = format!(
            "Standard input is still being {doing} to {}. Stop recording, or keep \
             recording until the stream ends?",
            tee.name()
        );
        self.pending_leave = Some(leaving);
        self.confirmation_modal
            .show_choice(message, "Stop recording", "Keep recording");
    }

    /// Leave as asked: the recording stopped and its file finished, or kept going
    /// until its stream ends, while datui goes home or quits.
    fn leave_recording(&mut self, stop: bool) -> Option<AppEvent> {
        let leaving = self.pending_leave.take()?;
        let handle = self
            .opened
            .as_ref()
            .and_then(|(_, options)| options.spool.clone());
        if stop {
            if let Some(handle) = &handle {
                handle.spool().stop();
            }
        } else {
            // Held past the dataset, so letting it go does not stop the copy.
            self.recording_on = handle;
        }
        match leaving {
            Leaving::Quit => Some(AppEvent::Exit),
            Leaving::Home => {
                self.enter_home();
                None
            }
        }
    }

    /// The recording to wait for once the terminal is handed back: kept going when
    /// the user quit, until its stream ends.
    pub fn recording_after_exit(
        &mut self,
    ) -> Option<(crate::follow::Tee, Arc<crate::follow::SpoolHandle>)> {
        let handle = self.recording_on.take()?;
        let spool = handle.spool();
        let tee = spool.tee()?.clone();
        spool.live().then_some((tee, handle))
    }

    /// Say once that the recording ended: saved, or stopped by an error, which the
    /// error dialog says too. Returns true when the frame must redraw.
    fn notice_recording_end(&mut self) -> bool {
        let Some(spool) = self.recording().cloned() else {
            return false;
        };
        let Some(ended) = spool.ended() else {
            self.recording_end_said = false;
            return false;
        };
        if std::mem::replace(&mut self.recording_end_said, true) {
            return false;
        }
        let said = match spool.tee() {
            Some(tee) if tee.to_stdout() => "Standard input ended".to_string(),
            Some(tee) => format!("Saved {}", tee.path.display()),
            None => String::new(),
        };
        match ended {
            Some(reason) => self.error_modal.show(reason),
            None => self.flash_note(said),
        }
        true
    }

    /// Show a completion flash on the control bar.
    fn flash_note(&mut self, message: String) {
        self.flash = Some(Flash::new(message));
    }

    /// The completion flash on the control bar, if one is showing.
    pub fn flash_message(&self) -> Option<&str> {
        self.flash.as_ref().map(|f| f.message.as_str())
    }

    /// The error dialog's message, if one is showing.
    pub fn error_message(&self) -> Option<&str> {
        self.error_modal
            .active
            .then_some(self.error_modal.message.as_str())
    }

    /// When the screen next changes on its own, with no event to say so: the flash
    /// expiring. The run loop sleeps until then at most.
    pub fn next_deadline(&self) -> Option<std::time::Instant> {
        let flash = self.flash.as_ref().map(|f| f.expires);
        let clock = self
            .follow()
            .filter(|f| f.standing == crate::follow::Standing::Following)
            .and_then(|f| f.last_append)
            .map(crate::follow::next_tick);
        // A recording's size and rate move every second until it ends, and its end is
        // noticed on that tick.
        let recording = self
            .recording()
            .filter(|spool| spool.live())
            .map(|_| std::time::Instant::now() + std::time::Duration::from_secs(1));
        flash.into_iter().chain(clock).chain(recording).min()
    }

    /// Whether the follow chip's clock reads differently now: the frame must redraw.
    pub fn tick_follow_clock(&mut self) -> bool {
        let ended = self.notice_recording_end();
        let now = self.follow_mark();
        if now == self.follow_drawn {
            return ended;
        }
        self.follow_drawn = now;
        true
    }

    /// What the control bar says about the follow of the dataset on screen.
    fn follow_mark(&self) -> Option<crate::widgets::controls::FollowMark> {
        use crate::follow::Standing;
        // The hex view shows a file's bytes, not the table the follow moves.
        if self.input_mode == InputMode::Hex {
            return None;
        }
        let state = self.data_table_state.as_ref()?;
        let rows = |n: usize, what: &str| format!("{} {what}", crate::numfmt::group_chrome(n));
        let (rec, rec_stopped) = match self.recording().map(|spool| recording_label(spool)) {
            Some((label, stopped)) => (Some(label), stopped),
            None => (None, false),
        };
        let Some(follow) = state.follow().filter(|f| f.standing != Standing::Ended) else {
            return rec.is_some().then(|| crate::widgets::controls::FollowMark {
                rec,
                rec_stopped,
                ..Default::default()
            });
        };
        let waiting = follow.waiting();
        let (chip, note) = match follow.standing {
            Standing::Following => {
                let chip = match follow.last_append {
                    Some(at) => format!(
                        "following {} {}",
                        crate::glyphs::get().middot,
                        crate::follow::age(at.elapsed())
                    ),
                    None => "following".to_string(),
                };
                let note = if waiting > 0 && !self.in_normal_table_view() {
                    // A takeover keeps the rows it was opened on.
                    Some(rows(waiting, "new rows"))
                } else if follow.new_below > 0 && !state.on_last_row() {
                    Some(rows(follow.new_below, "new below"))
                } else {
                    None
                };
                (chip, note)
            }
            _ => (
                "paused".to_string(),
                (waiting > 0).then(|| rows(waiting, "new rows")),
            ),
        };
        let misfits = follow.misfits();
        // What `t` does on this screen: pause or resume at the table; over a surface
        // that keeps the rows it was opened on, read the new ones.
        let refreshes = self.input_mode == InputMode::ValueCounts
            || (self.input_mode == InputMode::Chart
                && self.chart_modal.picker.is_none()
                && !self.chart_export_modal.active)
            || (self.analysis_modal.active && self.analysis_modal.current_results().is_some());
        let key = if self.in_normal_table_view() {
            Some(match follow.standing {
                Standing::Paused => "Resume",
                _ => "Pause",
            })
        } else if refreshes && follow.behind() {
            Some("Refresh")
        } else {
            None
        };
        Some(crate::widgets::controls::FollowMark {
            key,
            chip: Some(chip),
            note,
            warning: (misfits > 0).then(|| {
                if misfits == 1 {
                    "1 row does not fit".to_string()
                } else {
                    rows(misfits, "rows do not fit")
                }
            }),
            rec,
            rec_stopped,
        })
    }

    /// `t` at the table: pause or resume the follow, or follow the file, reading it
    /// again as `H` does.
    fn toggle_follow(&mut self) -> Option<AppEvent> {
        use crate::follow::Standing;
        if let Some(follow) = self.data_table_state.as_mut().and_then(|s| s.follow_mut()) {
            match follow.standing {
                Standing::Following => follow.pause(),
                Standing::Paused => {
                    follow.resume();
                    self.catch_up_follow();
                }
                Standing::Ended => {}
            }
            return None;
        }
        let (paths, options) = self.opened.clone()?;
        if paths.iter().any(|path| crate::stdin::is_stdin(path)) {
            self.flash_note("Standard input is followed from the start: datui -f -".to_string());
            return None;
        }
        if let Some(refusal) = crate::follow::refuse_paths(&paths, &options) {
            self.flash_note(refusal);
            return None;
        }
        let options = OpenOptions {
            follow: true,
            ..options
        };
        self.set_loading_phase("Scanning input", 10);
        self.name_what_is_loading(paths[0].clone());
        Some(AppEvent::Open(paths, options))
    }

    /// Drop an expired flash. Returns true when the frame must redraw.
    pub fn tick_flash(&mut self) -> bool {
        if self.flash.as_ref().is_some_and(Flash::expired) {
            self.flash = None;
            return true;
        }
        false
    }

    /// Show the next Polars user warning on the control bar, once per session, when the
    /// bar is free. Returns true when the frame must redraw.
    pub fn flash_polars_warning(&mut self) -> bool {
        if !self.bar_is_free() {
            return false;
        }
        match logging::next_polars_warning() {
            Some(warning) => {
                self.flash_note(format!("Polars: {warning}"));
                true
            }
            None => false,
        }
    }

    /// Say that a background thread panicked when nothing else did; a job's panic ends
    /// the job instead, the way its error would. Returns true when the frame must
    /// redraw.
    pub fn flash_background_panic(&mut self) -> bool {
        if !self.bar_is_free() {
            return false;
        }
        match logging::take_unreported_panic() {
            Some(message) => {
                self.flash_note(message);
                true
            }
            None => false,
        }
    }

    /// Whether a flash would be seen: a busy message outranks it, and a modal would
    /// hide it until it expired.
    fn bar_is_free(&self) -> bool {
        !self.is_busy()
            && self.flash.is_none()
            && !self.error_modal.active
            && !self.confirmation_modal.active
    }

    /// See the `input_dropped` field.
    pub fn set_input_dropped(&mut self, dropped: bool) {
        self.input_dropped = dropped;
    }

    /// The escapes that act at once while busy and jump ahead of anything queued: Ctrl-Q
    /// and Ctrl-C quit, Ctrl-O goes home, so a slow load never
    /// traps the user; a confirmation modal keeps its keys so it can be answered; and the
    /// home screen is never busy on its own account (only work left running behind it sets
    /// `busy`), so it keeps every key.
    pub fn hard_escape_while_busy(&self, key: &KeyEvent) -> bool {
        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        let quit = ctrl && matches!(key.code, KeyCode::Char('q' | 'c'));
        let home = ctrl && key.code == KeyCode::Char('o');
        let cancel_analysis = self.analysis_modal.active
            && self.analysis_modal.computing.is_some()
            && key.code == KeyCode::Esc;
        let cancel_pivot = self.pivot_computing() && key.code == KeyCode::Esc;
        let leave_quality_evidence = self.quality_evidence_return.is_some()
            && self.input_mode == InputMode::Normal
            && key.code == KeyCode::Esc;
        let cancel_view = key.code == KeyCode::Esc && self.view_applying();
        let cancel_find = key.code == KeyCode::Esc && self.finding();
        quit || home
            || cancel_analysis
            || cancel_pivot
            || cancel_view
            || cancel_find
            || leave_quality_evidence
            || self.confirmation_modal.active
            || self.input_mode == InputMode::Home
    }

    /// The table's column cursor keys: `h` `l` (←→) a column, `[` `]` (or Shift+←→)
    /// a page of columns, `{` `}` the first and last. Never with Ctrl or Alt: Ctrl+[
    /// is Esc on a terminal.
    fn column_cursor_key(key: &KeyEvent) -> Option<crate::widgets::column_paging::CursorMove> {
        use crate::widgets::column_paging::CursorMove;
        if key
            .modifiers
            .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT)
        {
            return None;
        }
        let shift = key.modifiers.contains(KeyModifiers::SHIFT);
        match key.code {
            KeyCode::Char('[') => Some(CursorMove::PageLeft),
            KeyCode::Char(']') => Some(CursorMove::PageRight),
            KeyCode::Left if shift => Some(CursorMove::PageLeft),
            KeyCode::Right if shift => Some(CursorMove::PageRight),
            KeyCode::Left | KeyCode::Char('h') => Some(CursorMove::Left),
            KeyCode::Right | KeyCode::Char('l') => Some(CursorMove::Right),
            KeyCode::Char('{') => Some(CursorMove::First),
            KeyCode::Char('}') => Some(CursorMove::Last),
            _ => None,
        }
    }

    /// Whether a key may act while the app is busy. `App::handle` gates on this; the main
    /// loop applies the extra "nothing queued" condition for the second group.
    ///
    /// The hard escapes always qualify. Beyond them, in the plain Normal-mode table view
    /// (no text field, no modal), the harmless view keys act — quit, the column cursor
    /// and help — because the first key held in that view cannot be part of a typed
    /// `/query`. Harmless means reads nothing: the column cursor re-slices the buffer
    /// it already holds through `rescroll_columns`, never `collect`, which counts the rows
    /// when the count has not landed. Admitting a key that can count would put a
    /// metadata read per object of a cloud hive on this very thread —
    /// `a_key_that_acts_while_busy_reads_nothing` holds the line. Everything else,
    /// letters included, is type-ahead and waits; a bare Enter or Esc there confirms
    /// nothing and is dropped by the caller. Nothing is classified by keycode alone:
    /// the `h` in a typed `/hello` never scrolls.
    pub fn key_acts_while_busy(&self, key: &KeyEvent) -> bool {
        if self.hard_escape_while_busy(key) {
            return true;
        }
        if !self.in_normal_table_view() {
            return false;
        }
        // One row up or down inside the rows held, while all that is awaited is more
        // rows (#646): the table on screen is the one they are for.
        let step = match key.code {
            KeyCode::Down | KeyCode::Char('j') => Some(1),
            KeyCode::Up | KeyCode::Char('k') => Some(-1),
            _ => None,
        };
        if let Some(step) = step {
            return !self.busy
                && !self.loading.waits()
                && self
                    .jobs
                    .keys_held_only_by(|job| matches!(job, Job::Rows(_) | Job::OwedRows { .. }))
                && self
                    .data_table_state
                    .as_ref()
                    .is_some_and(|s| !s.scroll_would_trigger_collect(step));
        }
        matches!(
            key.code,
            KeyCode::Char('q')
                    | KeyCode::Char('Q')
                    // Drawn from what the table holds: row numbers, digit grouping, the
                    // type row (#646), a column's width (#647).
                    | KeyCode::Char('#')
                    | KeyCode::Char('<')
                    | KeyCode::Char('>')
                    | KeyCode::Char('=')
                    | KeyCode::Char('w')
                    | KeyCode::Char(',')
                    | KeyCode::Char('D')
                    | KeyCode::Left
                    | KeyCode::Right
                    | KeyCode::Char('h')
                    | KeyCode::Char('l')
                    | KeyCode::Char('[')
                    | KeyCode::Char(']')
                    | KeyCode::Char('{')
                    | KeyCode::Char('}')
                    | KeyCode::F(1)
                    | KeyCode::Char('?')
        )
    }

    /// The plain table view: Normal mode with no help overlay, modal, or in-view modal
    /// (template, analysis) drawn over it.
    pub fn in_normal_table_view(&self) -> bool {
        self.input_mode == InputMode::Normal
            && !self.show_help
            && !self.template_modal.active
            && !self.analysis_modal.active
            && !self.error_modal.active
            && !self.confirmation_modal.active
    }

    /// Whether a text field currently owns typed characters, so the wheel and `?` leave
    /// it alone. The home filter is deliberately excluded.
    pub fn text_field_focused(&self) -> bool {
        match self.input_mode {
            InputMode::Editing => true,
            InputMode::Export => matches!(
                self.export_modal.focus,
                ExportFocus::PathInput | ExportFocus::CsvDelimiter
            ),
            // The Picker narrows by typing, so it types.
            InputMode::Copy => self.copy_modal.picker.is_some(),
            // The find line types.
            InputMode::Inspect => self.inspector_modal.finding,
            // The Picker narrows by typing, so it types.
            InputMode::GoToColumn => true,
            InputMode::PickFormat => true,
            InputMode::SortFilter => {
                self.sort_filter_modal.focus == SortFilterFocus::Body
                    && match self.sort_filter_modal.active_tab {
                        // The whole inline editor types: pickers narrow, the value edits.
                        SortFilterTab::Filter => self.sort_filter_modal.filter.editor.is_some(),
                        SortFilterTab::Sort => {
                            self.sort_filter_modal.sort.focus == SortFocus::Filter
                        }
                    }
            }
            InputMode::PivotMelt => {
                // The Picker narrows by typing, so it types too.
                self.pivot_melt_modal.picker.is_some()
                    || self
                        .pivot_melt_modal
                        .is_text_row(self.pivot_melt_modal.focus)
            }
            InputMode::Chart => {
                if self.chart_export_modal.active {
                    matches!(
                        self.chart_export_modal.focus,
                        ChartExportFocus::PathInput
                            | ChartExportFocus::TitleInput
                            | ChartExportFocus::WidthInput
                            | ChartExportFocus::HeightInput
                    )
                } else {
                    // The open column Picker narrows by typing, so it types.
                    self.chart_modal.picker.is_some()
                }
            }
            InputMode::Normal => {
                self.analysis_modal.sample_scope_typing()
                    || self.analysis_modal.quality_expected_typing()
                    || self.analysis_modal.intent_typing()
                    || self.analysis_modal.export_typing()
                    || (self.template_modal.active
                        && self.template_modal.mode != TemplateModalMode::List
                        && matches!(
                            self.template_modal.form_focus,
                            FormFocus::Name
                                | FormFocus::Description
                                | FormFocus::ExactPath
                                | FormFocus::RelativePath
                                | FormFocus::PathPattern
                                | FormFocus::FilenamePattern
                        ))
            }
            InputMode::Home | InputMode::Info | InputMode::ValueCounts => false,
            // The prompt types, and so does the spec picker's filter.
            InputMode::Hex => self
                .hex
                .as_ref()
                .is_some_and(|view| view.prompt.is_some() || view.picker.is_some()),
        }
    }

    pub fn send_event(&mut self, event: AppEvent) -> Result<()> {
        self.events.send(event)?;
        Ok(())
    }

    /// Whether the dataset on screen is one that opened before its footers were read
    /// and is still waiting for them.
    ///
    /// Not the same question as whether a pass is running. The counter is shared with
    /// every open, and abandoning one does not stop it: without this, giving up on a
    /// large local directory and going back to the dataset you had would leave that
    /// dataset's control bar counting footers belonging to the directory you left.
    /// Whether the row count on the control bar is on its way, so a spinner stands in
    /// for it. Asked by the bar, and by the run loop, which turns the spinner: the
    /// two disagreed while a dataset read its own footers, and the spinner sat still.
    pub fn row_count_pending(&self) -> bool {
        // A load in flight counts as pending: the number `data_table_state` still holds
        // belongs to the dataset being replaced, and printing it beside the incoming
        // file's name would read as the new one's.
        // A dataset still reading its own footers counts too: it declines the ordinary
        // count because that pass is bringing one, so nothing is "in flight" — and the
        // number it holds meanwhile is only as far as the buffer reaches. Printed
        // plainly, a prefix of six thousand files reads `Rows: 70`.
        self.len_count_inflight.is_some()
            || self.loading.awaiting_dataset()
            // A re-read owed to a dataset whose footers could not be read is a count
            // that is coming: the collect it is waiting to run is what starts one. The
            // dataset has already stopped saying it counts itself later (it gave up on
            // the pass the moment that pass failed), so without this the bar falls
            // through to printing the number it happens to hold — which is only as far
            // as the buffer reached. A prefix of six thousand files reads `Rows: 70`,
            // plainly, for as long as the work in front of the errand takes.
            || self.reread_owed.is_some()
            || self
                .data_table_state
                .as_ref()
                .is_some_and(|state| state.counts_itself_later())
    }

    /// Whether a spinner is on screen, so the run loop turns it and redraws.
    pub fn something_is_spinning(&self) -> bool {
        self.is_busy()
            || (self.row_count_pending() && !self.awaiting_download_confirmation())
            // The clock beside "source read finishing" keeps time until it has.
            || (self.analysis_modal.active && self.cancelled_analysis_running().is_some())
            || self.chart_preparing()
            || self.value_counts_computing()
            || (self.input_mode == InputMode::Home
                && (self.home.awaiting_listing().is_some()
                    || self.home.sections_waiting()
                    || !self.home.peeking.is_empty()))
    }

    fn dataset_is_still_reading_its_footers(&self) -> bool {
        self.data_table_state
            .as_ref()
            .is_some_and(|state| state.footers_pending().is_some())
    }

    /// Take the numbers the whole frame will be drawn from.
    ///
    /// Only one so far: the footer count. It is read here rather than where it is
    /// shown because two parts of the screen show it, they are painted at different
    /// moments, and a background thread is moving it between them.
    fn begin_frame(&mut self) {
        self.footers_this_frame = self.footer_progress().reading();
        // Whatever this frame does not draw cannot be clicked.
        self.pointer.forget_drawn();
        if let Some(state) = self.data_table_state.as_mut() {
            state.forget_drawn();
        }
    }

    /// The footer counter the screen reads: the open's own while one is on its way to
    /// a dataset, else the dataset on screen's. Each open counts on its own, so one
    /// replaced half way through cannot count under the name of the file that replaced
    /// it.
    pub fn footer_progress(&self) -> &Arc<crate::schema_union::FooterProgress> {
        self.loading.progress().unwrap_or(&self.footer_progress)
    }

    /// Hold past the app: dropped after it, it removes the temp files the app's opens
    /// were still writing, giving their workers up to a second to stop first. Without
    /// it, a quit mid-download or mid-decompression can end the process before the
    /// worker removes its partial file.
    pub fn exit_sweep(&self) -> ExitSweep {
        ExitSweep(self.loading.unfinished().clone())
    }

    /// Whether an open is on its way and its dataset not installed yet: whatever table
    /// `data_table_state` holds meanwhile belongs to the dataset being replaced, so the
    /// main view shows the open's progress instead of it.
    pub(crate) fn awaiting_dataset(&self) -> bool {
        self.loading.awaiting_dataset()
    }

    /// What the loading screen and the control bar say about the open in flight: its
    /// phase, the flat percentage beside it, the path it names and that path's size.
    pub(crate) fn load_shown(&self) -> Option<(&str, u16, Option<&Path>, u64)> {
        self.loading.current().map(|load| {
            let (phase, percent) = load.phase().label();
            (phase, percent, load.path(), load.size())
        })
    }

    /// What the load is doing, for whichever part of the screen is saying so.
    ///
    /// The footer count stands in for the phase while a pass is running: it says the
    /// same thing and says how far along it is. Both callers read it from
    /// [`Self::footers_this_frame`], one number taken once a frame, so they cannot say
    /// two different things about one wait.
    pub(crate) fn loading_phase<'a>(&self, phase: &'a str) -> std::borrow::Cow<'a, str> {
        match self.footers_this_frame {
            Some((read, total)) => std::borrow::Cow::Owned(format!(
                "Reading footers: {} of {}",
                crate::numfmt::group_chrome(read),
                crate::numfmt::group_chrome(total)
            )),
            None => std::borrow::Cow::Borrowed(phase),
        }
    }

    /// An open is on its way: the loading screen takes over now, saying `phase`, and keys
    /// wait for it. Called before the event that carries the open out — by `run` before
    /// the first frame, and by a key before the `Open` it returns — because a frame is
    /// drawn between the two and would otherwise show the outgoing dataset.
    pub fn set_loading_phase(&mut self, phase: impl Into<String>, progress_percent: u16) {
        self.announce_open(false, phase.into(), progress_percent);
    }

    /// As [`Self::set_loading_phase`], for an open chosen on the home screen when
    /// `from_home`: that is where its failure is reported.
    fn announce_open(&mut self, from_home: bool, phase: String, percent: u16) {
        self.make_way_for_an_open();
        self.loading.announce(from_home, phase, percent);
    }

    /// Put the path on the loading screen, so a wait says what it is waiting for.
    pub(crate) fn name_what_is_loading(&mut self, path: PathBuf) {
        self.loading.name(path);
    }

    /// An open is being asked for: a load already doing work is put down for it, unless
    /// it has not started any (the look or the frame that leads to this open).
    fn make_way_for_an_open(&mut self) {
        if let Some(retired) = self.loading.make_way() {
            self.put_down_load(retired);
        }
    }

    /// An open has its request: make way for it, and stop what the dataset on screen
    /// was still reading for itself.
    fn begin_new_dataset(&mut self) {
        self.make_way_for_an_open();
        // A preview's dataset this open did not take is a page nobody is opening.
        self.home_previews.drop_prepared();
        self.reset_chart_state();
        self.jobs.advance();
        // The dataset's footer pass is no longer wanted, and unread, unpaid-for is better
        // than read and dropped. The open counts its own footers on a counter of its
        // own, which the dataset takes over if it installs.
        //
        // The meter needs no equivalent: it belongs to the dataset rather than to the
        // app, so a load that never reaches the screen never has one installed. See
        // `DataTableState::measurements`.
        self.footer_progress.cancel();
        *self
            .pending_footers_result
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = None;
    }

    /// Put down what the app keeps for a load the loader has retired: its jobs, whose
    /// answers are for a screen nobody is on, their lines on the control bar, and the
    /// question about its download.
    fn put_down_load(&mut self, retired: loading::Retired) {
        let id = retired.id;
        let lines = self.jobs.quiet(|job| job.load() == Some(id));
        self.jobs.supersede(|job| job.load() == Some(id));
        if self
            .status_message
            .as_ref()
            .is_some_and(|status| lines.contains(status))
        {
            self.status_message = None;
        }
        if retired.asking {
            self.confirmation_modal.hide();
        }
    }

    /// Carry out what the open needs next.
    fn run_load_step(&mut self, step: loading::Step) -> Option<AppEvent> {
        use loading::Step;
        let load = self.loading.id();
        match step {
            Step::Nothing => None,
            Step::Crash(message) => Some(AppEvent::Crash(message)),
            Step::Failed(failed) => {
                self.load_failed(failed);
                None
            }
            Step::Tables(tables) => {
                self.land_on_tables(tables);
                None
            }
            Step::Hex(hex) => {
                self.land_on_hex(hex);
                None
            }
            Step::Install(loaded) => {
                // The view an open applies reads its own first rows, so the dataset's are
                // not read.
                if self.install_dataset(*loaded) {
                    return None;
                }
                #[cfg(test)]
                {
                    self.first_rows_asked += 1;
                }
                if !self.spawn_async_collect(Self::LOADING_BUFFER) {
                    // Nothing to read: the buffer already serves the view.
                    if self.status_message.as_deref() == Some(Self::LOADING_BUFFER) {
                        self.status_message = None;
                    }
                    self.first_rows_settled();
                }
                None
            }
            #[cfg(any(feature = "http", feature = "cloud"))]
            Step::Ask(pending) => {
                // Nothing runs while the question is up: datui waits on a key, and a
                // spinner would read as progress. The loader holds the generation
                // meanwhile.
                self.confirmation_modal
                    .show(Self::download_confirmation_message(
                        &pending,
                        self.loading.download_note(),
                    ));
                None
            }
            step => {
                let load = load.expect("a step that runs work belongs to the open in flight");
                self.spawn_load_phase(load, step);
                None
            }
        }
    }

    /// Keep the shape of a downloaded dataset under the URL it was opened from, once its
    /// rows are counted: nothing lists a web file, so this is the only way its recent,
    /// and its catalog row, can say `344 × 9` (#547 D12). Once per dataset.
    fn remember_a_downloads_shape(&mut self) {
        if self.shape_remembered == Some(self.dataset_generation) {
            return;
        }
        let Some(url) = self.path.clone().filter(|p| source::is_remote_url(p)) else {
            return;
        };
        let Some(state) = self.data_table_state.as_ref().filter(|s| s.fetched()) else {
            return;
        };
        let Some(rows) = state.num_rows_if_valid().filter(|_| !state.changes_rows()) else {
            return;
        };
        self.shape_remembered = Some(self.dataset_generation);
        let columns: Vec<String> = state
            .source_schema()
            .iter_names()
            .map(|name| name.to_string())
            .collect();
        let facts = crate::cache::DatasetFacts {
            mtime: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or_default(),
            size: 0,
            rows: Some(rows),
            cols: Some(columns.len()),
            cols_sampled: false,
            columns,
            kind: Some(discover::EntryKind::File),
            classified_by: discover::CLASSIFIER_VERSION,
            cost: Default::default(),
            holds: Default::default(),
        };
        // Off the UI thread: the index takes a lock other instances may hold.
        let cache = self.cache.clone();
        std::thread::spawn(move || cache.record_dataset_facts(&[(url, facts)]));
    }

    /// The first rows of an open are on screen, or will not be read: its wait is over.
    fn first_rows_settled(&mut self) {
        self.loading.first_rows_settled();
    }

    /// Read the rows on screen again, now that the frame they were read through has
    /// been replaced.
    ///
    /// The dataset's own errand, not its open's: this happens long after the open has
    /// finished, and after a glance at the home screen just the same. The join has
    /// already dropped the buffer, so nothing dropping this leaves the table with no
    /// rows to show at the moment it was to show more of them.
    fn reread_after_the_footers_joined(&mut self) {
        // Any re-read satisfies one that was owed: this is the collect the errand was
        // waiting to run, whoever asked for it.
        self.reread_owed = None;
        // End was pressed while the footers were still coming, and they are what the
        // end was waiting on. Taken either way: a flag left from a dataset that is gone
        // is not this one's to act on. The jump reads the page it lands on, so reading
        // the page here first would be one fetched to be thrown away.
        if self.end_when_the_footers_land.take() == Some(self.dataset_generation) {
            self.status_message = None;
            if let Some(next) = self.jump_key(AppEvent::DoScrollEnd) {
                // The jump reads the page it lands on, so reading this one first would
                // be a page fetched to be thrown away.
                let _ = self.events.send(next);
                return;
            }
            // Unless it asked for no read: the view was already at the end, or the pass
            // brought no count and the jump is waiting on the ordinary one. The join has
            // dropped the buffer either way, so falling through is the difference
            // between a table and an empty one.
        }
        self.spawn_async_collect(Self::LOADING_BUFFER);
    }

    /// Run a buffer collect that was asked for while other work was waiting on the
    /// generation.
    ///
    /// The same shape as `reread_when_the_work_allows` below, and for the same reason:
    /// the collect bumps `task_generation`, so it waits its turn and is tried again
    /// after every event.
    fn collect_when_the_work_allows(&mut self) {
        let Some(&Job::OwedRows { dataset, .. }) = self.jobs.owed(Self::owed_rows) else {
            return;
        };
        if dataset != self.dataset_generation {
            // The dataset it was owed to is gone, and so is the view it was filling.
            // Only the errand is put down, and the keys it held with it; the status line
            // belongs to whatever replaced the dataset.
            self.jobs.take_owed(Self::owed_rows);
            return;
        }
        if self.work_a_bump_would_strand() {
            return;
        }
        let Some(Job::OwedRows { status, .. }) = self.jobs.take_owed(Self::owed_rows) else {
            return;
        };
        if !self.spawn_async_collect(&status) {
            self.busy = false;
            self.status_message = None;
            // The collect that was owed may have been an open's first rows. Left waiting
            // on them, the bar would read "Loading buffer... 70%" with the app idle for
            // the rest of the session.
            self.first_rows_settled();
        }
    }

    /// Run the re-read a failed footer pass owes the dataset, once it can be run
    /// without throwing another answer away.
    ///
    /// The failure branch of `BackgroundFootersJoined` used to re-read on the spot,
    /// which bumped `task_generation` with no check at all — the one path into the
    /// collect that never asked `work_the_join_would_cancel`. An export in its collect
    /// phase then never wrote its file and said nothing about it. So the errand waits
    /// its turn, the way held columns already do.
    fn reread_when_the_work_allows(&mut self) {
        let Some(generation) = self.reread_owed else {
            return;
        };
        if generation != self.dataset_generation {
            // The dataset it was owed to is gone; so is the errand.
            self.reread_owed = None;
            return;
        }
        if self.work_the_join_would_cancel() {
            return;
        }
        self.reread_after_the_footers_joined();
    }

    /// Retire an End that was waiting on a count which can no longer answer it.
    ///
    /// Only the flag and the message it put up: the jump itself is not re-issued. See
    /// the caller in `BackgroundLenReady` for why asking again is the wrong repair.
    fn retire_the_end_that_was_waiting(&mut self) {
        self.end_after_count = None;
        self.take_down_the_counting_status();
    }

    /// Take down "Counting rows to find the end...", and only that.
    ///
    /// Clearing the status outright would wipe whatever else is using the line — a
    /// load's phase, an export's progress — on behalf of a key pressed somewhere else.
    fn take_down_the_counting_status(&mut self) {
        if self.status_message.as_deref() == Some(Self::COUNTING_FOR_END) {
            self.status_message = None;
        }
    }

    /// What the status line says while an End is waiting on a row count. Named so the
    /// paths that retire such an End can take the message back down without reaching
    /// for a literal, and without clearing a message that belongs to something else.
    const COUNTING_FOR_END: &'static str = "Counting rows to find the end...";

    /// What the control bar says while a path is being looked at. Named so the answer can
    /// take down its own line without clearing one that belongs to something else.
    const LOOKING: &'static str = "Looking...";

    /// The wait while a directory named on the command line is looked at: which files it
    /// holds, and whether they are one table. Seconds, for a directory of large Parquet.
    pub const LOOKING_AT_A_DIRECTORY: &'static str = "Looking at the directory";

    /// The wait while the rows for the view are fetched.
    pub const LOADING_BUFFER: &'static str = "Loading buffer...";

    /// The wait while a view's pivot or first rows are read.
    const APPLYING_VIEW: &'static str = "Applying view...";

    /// The wait while a grouped row the buffer does not hold is read to drill into.
    const READING_GROUP: &'static str = "Reading the group...";

    /// The wait while the inspector reads a row's hidden and binary fields.
    const READING_FIELDS: &'static str = "Reading fields...";

    /// Above this a field is copied off the UI thread: a long list's JSON can take
    /// a moment to write.
    const FIELD_COPY_INLINE_BYTES: usize = 1024 * 1024;
    /// The most JSON a copy of a JSON value writes where the clipboard sets no cap.
    const JSON_COPY_MAX_BYTES: usize = 64 * 1024 * 1024;
    /// The wait while the inspector parses long text as JSON.
    const READING_JSON: &'static str = "Reading JSON...";

    /// The wait while a pivot reads the view.
    const COMPUTING_PIVOT: &'static str = "Computing pivot...";

    /// How long a fetch goes unmentioned. A local page lands well inside it, and the key
    /// chips staying put is the difference between paging and a bar that blinks a
    /// sentence on every screen.
    const A_FETCH_WORTH_SAYING: std::time::Duration = std::time::Duration::from_millis(300);

    /// Grow the buffer before the view reaches its end, rather than once it has.
    ///
    /// Nothing waits on it: no `busy`, no message, and keys go on paging through the
    /// rows on hand. One at a time — a scroll that outruns it either waits on it, when it
    /// is bringing the rows asked for, or supersedes it by the generation, as any newer
    /// collect does. Never when a bump would strand other work.
    fn load_ahead(&mut self) {
        // The generation is asked about before the position is marked asked. Held while
        // the app is idle (a download waiting on the user), a frame would otherwise
        // spend the position on that refusal and never ask again (#490).
        if self.is_busy()
            || self.jobs.owed(Self::owed_rows).is_some()
            || self.rows_in_flight().is_some()
            || self.work_a_bump_would_strand()
        {
            return;
        }
        let Some(state) = self.data_table_state.as_ref() else {
            return;
        };
        // Asked of each position once. Planning can give back the buffer on hand — a
        // row group too large to add under the caps — and asking again every frame
        // would plan it again every frame.
        let position = state.buffer_position();
        if !state.wants_to_load_ahead() || self.loaded_ahead_from == Some(position) {
            return;
        }
        self.loaded_ahead_from = Some(position);
        self.spawn_collect(None);
    }

    /// Whether the bar is still keeping quiet about a fetch for the view.
    fn fetch_too_young_to_mention(&self) -> bool {
        self.status_message.as_deref() == Some(Self::LOADING_BUFFER)
            && self
                .rows_in_flight()
                .is_some_and(|inflight| inflight.began.elapsed() < Self::A_FETCH_WORTH_SAYING)
    }

    /// Work already running that the re-read after a join would cancel.
    ///
    /// The re-read goes through the ordinary collect, which bumps `task_generation`, so
    /// everything a bump would strand has to be done first — and that is
    /// [`Jobs::would_strand`]'s job now, rather than a list of the kinds of work that
    /// might be running.
    ///
    /// One thing more than a bump, though: a join takes a fresh `len_generation` too. A
    /// chart is prepared against the frame rather than the generation
    /// (`BackgroundChartReady` carries no generation at all), so a bump cannot strand
    /// one but changing the frame under it can.
    fn work_the_join_would_cancel(&self) -> bool {
        self.work_a_bump_would_strand() || self.chart_preparing()
    }

    /// Give the dataset what its footers found, if it can take it now.
    ///
    /// It cannot while the user is looking at a query, a pivot, a melt or a drill-down:
    /// those make their own result the root, and widening the scan underneath one takes
    /// away the columns it is built from. So the columns wait — held, not dropped — and
    /// this is tried again after every event, which is the cheapest way to catch the
    /// moment the view comes back to the data.
    ///
    /// Returns whether the dataset took them, so the caller can re-read the rows on
    /// screen through the wider frame.
    fn join_held_footers(&mut self) -> bool {
        let Some((generation, _)) = self.footers_held.as_ref() else {
            return false;
        };
        if *generation != self.dataset_generation {
            // The dataset they belong to is gone; so are they.
            self.footers_held = None;
            return false;
        }
        if self.data_table_state.is_none() || self.work_the_join_would_cancel() {
            return false;
        }
        let Some((generation, found)) = self.footers_held.take() else {
            return false;
        };
        let state = self
            .data_table_state
            .as_mut()
            .expect("checked just above, and nothing since takes it");
        // Whether this is the moment is the dataset's call, not this one's: it is the
        // frame on screen that knows whether it still grows from the scan.
        match state.join_dataset_schema(found) {
            Ok(()) => true,
            Err(found) => {
                self.footers_held = Some((generation, *found));
                false
            }
        }
    }

    /// Start the pass that reads the rest of a staged open's footers.
    ///
    /// Not a job, which the user would wait on: the whole point of opening
    /// before every footer is read is that the dataset works while they are read. The
    /// generation is the dataset's rather than the task's, because a collect bumps the
    /// task's and this pass outlives several of them.
    fn start_pending_footers(&mut self) {
        let Some(join) = self
            .data_table_state
            .as_ref()
            .and_then(|state| state.footers_pending())
        else {
            return;
        };
        let generation = self.dataset_generation;
        let slot = self.pending_footers_result.clone();
        let tx = self.events.clone();
        let progress = self.footer_progress.clone();
        self.runtime.spawn_blocking(move || {
            // Reported either way. A pass that could not read them has to say so, or
            // the dataset waits for it for the rest of the session — and a waiting
            // dataset is one that will not count itself, because the count was what
            // the pass was bringing back. A pass that panicked could not read them.
            let found = logging::catch_panic(|| join(&progress)).unwrap_or(None);
            if !Self::record_footers(&slot, generation, found) {
                return;
            }
            let _ = tx.send(AppEvent::BackgroundFootersJoined { generation });
        });
    }

    /// Put what a pass found in the slot, unless a later dataset's pass has answered
    /// first. Returns whether it went in, so a pass that lost does not also announce
    /// itself.
    ///
    /// Two passes can be in flight at once — opening a second large prefix does not
    /// stop the first one reading — and they finish in whatever order the network
    /// gives. Without this the slower, older one overwrites the newer entry, and the
    /// generation the event carries then disagrees with the generation in the slot,
    /// so both are discarded and the dataset on screen never gets its columns.
    fn record_footers(
        slot: &std::sync::Mutex<FootersReported>,
        generation: u64,
        found: Option<crate::widgets::datatable::FootersFound>,
    ) -> bool {
        let mut slot = slot.lock().unwrap_or_else(|e| e.into_inner());
        if slot.as_ref().is_some_and(|(held, _)| *held > generation) {
            return false;
        }
        *slot = Some((generation, found));
        true
    }

    /// Install the dataset an open read, and apply the view it opens with, if any.
    /// Returns whether that view is reading the first rows, which the caller then leaves
    /// to it.
    ///
    /// Only the loader hands one over, and only for the open in flight: an abandoned or
    /// replaced open's answer never gets this far.
    fn install_dataset(&mut self, loaded: loading::Loaded) -> bool {
        let loading::Loaded {
            state,
            path,
            options,
            debug_label,
            paths,
            recent,
            from_home,
            footers,
        } = loaded;
        let options = &options;
        // A key pressed at the dataset being replaced belongs to it, not to this one.
        self.end_when_the_footers_land = None;
        // Its companion, for the same reason. This one keys itself to a
        // `len_generation`, which says nothing about which dataset it belonged to, so
        // without clearing it here an End pressed on the directory the user walked away
        // from is still live against the one they opened next.
        self.end_after_count = None;
        // One per dataset that reaches the screen, rather than one per open started:
        // an open that fails leaves the last dataset up, and the pass still reading its
        // footers has to be able to finish into it.
        self.dataset_generation = self.dataset_generation.wrapping_add(1);
        self.quality_cache.clear();
        self.quality_samples.clear();
        self.quality_released.clear();
        // The objects may have changed since they were copied; a run on the dataset
        // opened now fetches them again.
        self.quality_copies.clear();
        self.quality_copy_released = None;
        self.quality_copy_unusable = None;
        self.quality_evidence_return = None;
        self.quality_evidence_label = None;
        // The findings narrowed to the last dataset's columns would hide this one's.
        self.analysis_modal.data_quality_findings = quality_report::FindingsView::default();
        self.analysis_modal.data_quality_evidence_read = None;
        // A query still running was over the dataset being replaced; its rollback
        // is that dataset's view. So was a view waiting on its pivot.
        self.query_running = None;
        self.jobs.supersede(|job| matches!(job, Job::ViewPivot(_)));
        // Whatever chart state survived belongs to the dataset being replaced.
        self.reset_chart_state();
        self.debug.schema_load = debug_label;
        // Home is now in the stack, so q pops back to it; never unset, since a
        // reread from the table (H) is not a new place.
        if from_home {
            self.opened_from_home = true;
        }
        // A frame handed over has no path to go back to.
        // Without the spec read: it holds the file's map, and a decompressed copy's map
        // keeps its disk space until the map goes, so it goes with the dataset.
        self.opened = paths.map(|paths| {
            let options = OpenOptions {
                format_read: None,
                sqlite: None,
                // Counted afresh by the next read.
                tail: None,
                prepared: None,
                ..options.clone()
            };
            (paths, options)
        });
        // Recorded once the dataset is installed: a file that fails to load is not one
        // anybody wants to get back to.
        if let Some(path) = recent {
            // Off the opening path. Recording a recent is a convenience that nothing
            // waits on, and it takes a lock several instances may be contending for --
            // opening a dataset must not queue behind another instance's bookkeeping.
            let cache = self.cache.clone();
            std::thread::spawn(move || cache.push_recent(&path));
        }
        self.forget_the_rows_read();
        self.file_facts = None;
        // The footers it still has to read are counted on the open's counter, which is
        // the dataset's now; the last dataset's pass, if any is left, stops.
        self.footer_progress.cancel();
        self.footer_progress = footers;
        self.data_table_state = Some(state);
        // A followed file's watcher starts with its dataset and stops with it.
        if options.follow
            && let Some(state) = self.data_table_state.as_mut()
        {
            match options.tail.as_deref() {
                Some(tail) => {
                    state.start_following(crate::follow::Follow::start(
                        tail.clone(),
                        self.app_config.file_loading.follow_interval(),
                        self.events.clone(),
                        options.spool.clone(),
                    ));
                    // Counted already, as the scan reads them: no count of its own.
                    state.follow_to(tail.rows(), false);
                }
                // A recording of something that cannot be read as it grows.
                None => self.flash_note(
                    "Only text and Arrow streams are followed: this shows what had arrived, and recording goes on"
                        .to_string(),
                ),
            }
        }
        // A count still waiting for the last dataset's rows to paint is not owed now.
        self.retire_a_count_the_rows_answered();
        self.path = path.clone();
        if let Some(ref p) = path {
            let read_as = self
                .data_table_state
                .as_ref()
                .and_then(DataTableState::read_as);
            self.original_file_format = Self::export_format_for(p, read_as.or(options.format));
            // CSV's delimiter: a comma unless the user named a separator. A `.tsv`
            // exports as TSV, whose preset is the tab; a tab in a `.csv` would reopen
            // as one column.
            self.original_file_delimiter = Some(options.separator_or(b','));
        } else {
            self.original_file_format = None;
            self.original_file_delimiter = None;
        }
        // A panel still up says what it says about the dataset on screen.
        if self.info_modal.active {
            self.read_file_facts();
        }
        // The dataset is on screen now; whatever it still has to learn about itself is
        // read behind it.
        self.start_pending_footers();
        self.sort_filter_modal = SortFilterModal::new();
        self.pivot_melt_modal = PivotMeltModal::new();
        self.status_message = Some(Self::LOADING_BUFFER.to_string());

        // The dataset is installed and its schema known, so this is where a template
        // meets it. `--template` names one and applies to this first open alone;
        // `[templates] auto_apply` dresses every open that has a matching template.
        // A fresh dataset starts with no view applied: the previous file's view
        // must not wear the check mark here, nor count as applied when edited.
        self.active_template_id = None;
        let template = match self.startup_template.take() {
            Some(name) => match self.template_manager.get_template_by_name(&name).cloned() {
                Some(template) => Some(template),
                None => {
                    self.error_modal.show(format!("No view named \"{name}\""));
                    None
                }
            },
            None if self.app_config.templates.auto_apply => self.view_path().and_then(|path| {
                self.data_table_state.as_ref().and_then(|state| {
                    self.template_manager
                        .get_most_relevant(path, state.source_schema())
                })
            }),
            None => None,
        };
        let Some(template) = template else {
            return false;
        };
        match self.apply_template(&template) {
            // The view reads its own first rows, so the dataset's are never read.
            Ok(()) => true,
            Err(e) => {
                self.error_modal
                    .show(format!("Error applying view \"{}\": {e}", template.name));
                false
            }
        }
    }

    /// Ensures file path has an extension when user did not provide one; only adds
    /// compression suffix (e.g. .gz) when compression is selected. If the user
    /// provided a path with an extension (e.g. foo.feather), that extension is kept.
    /// Spawn an async buffer collect if needed. Returns true if a background task was spawned.
    /// Increments task_generation to invalidate any in-flight collect from a prior call.
    ///
    /// When the LazyFrame's row count is unknown (e.g. fresh load, or just after a
    /// filter/sort/pivot/melt that invalidates the cache), the exact `len()` is computed
    /// in the background and applied later via `BackgroundLenReady` — it never gates the
    /// buffer paint. `prepare_async_collect` plans a top-of-data window when the count is
    /// still unknown, so the first screen renders immediately. For large/partitioned/remote
    /// datasets the count can take a long time; it runs silently and concurrently.
    pub fn spawn_async_collect(&mut self, status: &str) -> bool {
        self.spawn_collect(Some(status))
    }

    /// As [`Self::spawn_async_collect`]; with no `status`, a load-ahead that nothing
    /// waits on: its job holds no keys. See [`InflightCollect`].
    fn spawn_collect(&mut self, status: Option<&str>) -> bool {
        let Some(state) = self.data_table_state.as_mut() else {
            return false;
        };

        // The exact row count, when it isn't known and none is already coming for this
        // data version. Independent of `task_generation` (a scroll must not restart it)
        // and does not set `busy`. A count that reads only footers runs now: it reads no
        // data. On an object store a data count rides in the collect spawned below,
        // which answers it outright when the read comes back short and otherwise gets
        // the row groups to itself first. On a local frame it waits until the page is
        // painted (`count_after_paint`), and is not needed at all when that page came
        // back short.
        let mut count = None;
        let generation = state.len_generation();
        if !state.is_num_rows_valid()
            && self.len_count_inflight != Some(generation)
            // Marked as running only once it is going to run. A dataset still reading
            // its own footers declines this count, because that pass is bringing it —
            // and the marker is cleared by a count coming back, so setting it for one
            // that was never started leaves it set for the rest of the session: a
            // spinner where the row count goes, a redraw on its account every frame,
            // and `End` waiting on nothing.
            && !state.counts_itself_later()
            // A count that failed is not tried again on every scroll. End asks again.
            && self.len_count_failed != Some(generation)
        {
            self.len_count_inflight = Some(generation);
            count = Some(LenCount::for_state(state));
        }
        let footers = count.take_if(|job| job.reads_footers());
        if count.take_if(|_| !state.is_remote_source()).is_some() {
            self.count_after_paint = Some(generation);
        }
        if let Some(job) = footers {
            self.spawn_count(job);
        }

        // Read before the frame is borrowed: the predicate is over the whole App.
        let a_bump_would_strand = self.work_a_bump_would_strand();
        let inflight = self.rows_in_flight();

        // Plan and spawn the buffer collect. With the count unknown this is a top-of-data
        // window (`slice(0, N)`) that touches only the first file(s) of a partitioned set.
        let Some(state) = self.data_table_state.as_mut() else {
            return false;
        };
        let covered = inflight.is_some_and(|inflight| inflight.covers(state));
        // The rows asked for are already on the way in a load-ahead: wait on that one
        // rather than fetch them twice. The keys are its now.
        if covered
            && let Some(status) = status
            && !self.jobs.waited_on(Self::reading_rows)
        {
            self.jobs.wait_on(Self::reading_rows, status);
            self.busy = false;
            self.status_message = Some(status.to_string());
        }
        let request = (!covered).then(|| state.prepare_async_collect(None));
        let Some(Some(request)) = request else {
            // Nothing to ride in: the view is covered, or the buffer on hand serves it.
            if let Some(job) = count {
                self.spawn_count(job);
            }
            return covered;
        };
        // Everything past here advances the generation, so everything that holds it has
        // to be done first. The collect the user asked for is queued rather than
        // refused: the throbber that was already turning goes on turning, and it is
        // tried again after every event until the work in front of it finishes.
        //
        // This is the door #238 was about. `BackgroundLenReady` answers a count by
        // jumping to the end, which reaches here with no key pressed and minutes after
        // the one that was — long enough for a dataset to have been opened meanwhile.
        // The bump cancelled that open's phase in flight, whose answer was then thrown
        // away with the open still waiting on it, and the file never opened, silently,
        // for the rest of the session.
        if a_bump_would_strand {
            // The count that was going to ride in this collect is put down rather than
            // run on its own. On an object store it answers itself out of the short read
            // the collect comes back with; spawned standalone it is a full remote
            // `len()`, which is the expensive thing the riding exists to avoid. Putting
            // the marker down with it is what lets the retry ask again.
            if count.is_some() {
                self.len_count_inflight = None;
            }
            // A load-ahead is not owed: nobody asked for it.
            let Some(status) = status else {
                return false;
            };
            // One page is owed at a time, the newest; the user waits on it.
            self.jobs.take_owed(Self::owed_rows);
            let owed = Job::OwedRows {
                dataset: self.dataset_generation,
                status: status.to_string(),
            };
            self.jobs.owe(owed, Some(status));
            self.busy = false;
            self.status_message = Some(status.to_string());
            return true;
        }
        self.jobs.advance();
        self.reads.pages += 1;
        let inflight = InflightCollect {
            began: std::time::Instant::now(),
            files: state.files_a_page_reads(
                request.buffer_start,
                request.buffer_end.saturating_sub(request.buffer_start),
            ),
            dataset: state.len_generation(),
            columns: InflightCollect::columns_of(state),
            start: request.buffer_start,
            end: request.buffer_end,
        };
        // Owed from here, so a worker that dies before it reaches the count still
        // answers it.
        let count = count.map(|job| OwedCount::new(job, self.events.clone()));
        self.spawn_job(Job::Rows(inflight), status, move |_| {
            let plan = request.plan;
            // The count is answered once the page has gone out: it may need a pass of
            // its own, which must not hold the page back.
            Ok(
                match crate::statistics::collect_lazy(request.lf, request.polars_streaming) {
                    Ok(df) => {
                        let returned = df.height();
                        let requested = request.buffer_end - request.buffer_start;
                        let start = request.buffer_start;
                        // Stitched and cut here rather than where it lands: a cut may
                        // copy up to the byte budget, which the UI thread would stall on
                        // (#483).
                        Answer::Rows(plan.fit(df)).then(move || {
                            if let Some(count) = count {
                                count.answer(|job| job.after_collect(start, returned, requested));
                            }
                        })
                    }
                    // A pass over a frame that just failed to collect would fail too: the
                    // count goes unanswered and so reports itself failed, leaving the
                    // retry to a later interaction (see `len_count_failed`).
                    Err(e) => Answer::RowsFailed {
                        message: crate::error_display::user_message_from_polars(&e),
                        conversion: crate::error_display::conversion_failure(&e).map(Box::new),
                    }
                    .then(move || drop(count)),
                },
            )
        });
        true
    }

    /// Count the rows off the UI thread; the answer comes back as `BackgroundLenReady`
    /// or `BackgroundLenFailed`.
    fn spawn_count(&self, job: LenCount) {
        #[cfg(test)]
        self.counts_spawned.set(self.counts_spawned.get() + 1);
        let count = OwedCount::new(job, self.events.clone());
        self.runtime
            .spawn_blocking(move || count.answer(LenCount::run));
    }

    /// Whether the rows of the frame on screen that someone is waiting for are still
    /// being read: the page an open, a query or a scroll asked for. A load-ahead is
    /// nobody's wait, so a count does not queue behind one.
    fn waited_on_rows_pending(&self, generation: u64) -> bool {
        self.loading.awaiting_dataset()
            || self.jobs.owed(Self::owed_rows).is_some()
            || (self.rows_waited_on()
                && self
                    .rows_in_flight()
                    .is_some_and(|inflight| inflight.dataset == generation))
    }

    /// Whether a frame painted now would start, or retire, the count waiting on one.
    /// The run loop paints after every update; a test harness, which paints nothing,
    /// asks this to know when to say a frame was painted.
    pub fn count_waits_for_a_frame(&self) -> bool {
        self.count_after_paint
            .is_some_and(|generation| !self.waited_on_rows_pending(generation))
    }

    /// A frame has been painted. Start the count that was waiting for its rows to be on
    /// screen, unless they are still being read; retire it if the frame it was for has
    /// gone or its rows already said how many there are.
    pub fn frame_painted(&mut self) {
        self.pointer.painted();
        let Some(generation) = self.count_after_paint else {
            return;
        };
        if self.waited_on_rows_pending(generation) {
            return;
        }
        self.count_after_paint = None;
        let wanted = self
            .data_table_state
            .as_ref()
            .filter(|state| state.len_generation() == generation && !state.is_num_rows_valid());
        match wanted {
            Some(state) => {
                self.len_count_inflight = Some(generation);
                self.spawn_count(LenCount::for_state(state));
            }
            None => {
                if self.len_count_inflight == Some(generation) {
                    self.len_count_inflight = None;
                }
            }
        }
    }

    /// The page just installed may have said how many rows there are, or belong to a
    /// frame other than the one a count is waiting on: either way that count is not
    /// owed any more.
    fn retire_a_count_the_rows_answered(&mut self) {
        let Some(generation) = self.count_after_paint else {
            return;
        };
        let answered = self
            .data_table_state
            .as_ref()
            .is_none_or(|state| state.len_generation() != generation || state.is_num_rows_valid());
        if answered {
            self.count_after_paint = None;
            if self.len_count_inflight == Some(generation) {
                self.len_count_inflight = None;
            }
        }
    }

    /// Start `job` on a worker. With a `status` the app is busy with it: the control
    /// bar says so and keys wait. The worker returns its answer, or `Err` with a
    /// message for the user; that, or a panic, is the job's one outcome, which
    /// [`AppEvent::JobEnded`] hands to [`App::job_ended`].
    ///
    /// Does not advance the generation. A caller replacing work in flight advances it
    /// first.
    fn spawn_job<F, R>(&mut self, job: Job, status: Option<&str>, work: F) -> Ticket
    where
        F: FnOnce(&jobs::Worker) -> std::result::Result<R, String> + Send + 'static,
        R: Into<jobs::Answered>,
    {
        let started = self.start_job(job, status);
        let ticket = started.ticket();
        started.run(&self.runtime, work);
        ticket
    }

    /// As [`Self::spawn_job`], for a caller that needs the ticket before the work is
    /// built: it runs the job with [`jobs::Started::run`].
    fn start_job(&mut self, job: Job, status: Option<&str>) -> jobs::Started {
        if let Some(status) = status {
            // The errand that led here, if one did, is this job's now: its record holds
            // the keys until it ends.
            self.busy = false;
            self.status_message = Some(status.to_string());
        }
        self.jobs.start(job, status)
    }

    fn owed_rows(job: &Job) -> bool {
        matches!(job, Job::OwedRows { .. })
    }

    fn reading_rows(job: &Job) -> bool {
        matches!(job, Job::Rows(_))
    }

    /// The read of the table's rows in flight, if one is and is still wanted: what it
    /// will fill, and whether anyone waits on it.
    fn rows_in_flight(&self) -> Option<InflightCollect> {
        match self.jobs.current(Self::reading_rows) {
            Some((_, Job::Rows(inflight))) => Some(*inflight),
            _ => None,
        }
    }

    /// Whether the user waits on the read of the table's rows in flight.
    fn rows_waited_on(&self) -> bool {
        self.jobs.waited_on(Self::reading_rows)
    }

    /// The rows being read, or owed, are not for the table on screen any more: a view
    /// has been put in its place. Their answer is dropped when it comes.
    fn forget_the_rows_read(&mut self) {
        self.jobs
            .supersede(|job| Self::reading_rows(job) || Self::owed_rows(job));
    }

    /// Hold the generation: a continuation waiting to run, or an errand waiting on the
    /// user. See [`jobs::Hold`].
    pub(crate) fn hold_the_generation(&self) -> jobs::Hold {
        self.jobs.hold()
    }

    /// A job in flight with no worker, started as `spawn_job` starts one, for tests
    /// that decide how it ends.
    #[cfg(test)]
    pub(crate) fn job_for_tests(&mut self, job: Job, status: Option<&str>) -> jobs::Started {
        self.start_job(job, status)
    }

    /// Whether the bar has no open and no export to report.
    #[cfg(test)]
    pub(crate) fn nothing_loading(&self) -> bool {
        self.loading.current().is_none() && self.export_progress.is_none()
    }

    /// An open on the loading screen, saying `phase` about `path` of `size` bytes, with
    /// nothing running: for tests of what the screen says.
    #[cfg(test)]
    pub(crate) fn loading_for_tests(
        &mut self,
        path: Option<PathBuf>,
        size: u64,
        phase: &str,
        percent: u16,
    ) {
        self.announce_open(false, phase.to_string(), percent);
        if let Some(path) = path {
            self.loading.name(path);
        }
        self.loading.size_for_tests(size);
    }

    /// An open of `path` begun and scanning, with no worker: for tests that decide how
    /// its phases answer. Its jobs are [`Job::Load`] with the id returned.
    #[cfg(test)]
    pub(crate) fn open_for_tests(&mut self, path: &str) -> loading::LoadId {
        self.make_way_for_an_open();
        let _ = self.loading.open(loading::OpenRequest {
            paths: vec![PathBuf::from(path)],
            options: OpenOptions::default(),
            size: 0,
            recent: None,
            shown: None,
        });
        self.loading.id().expect("an open was begun")
    }

    /// Install `state` as the dataset on screen, the way an open of a frame does: through
    /// the loader, so what the load hands over (its footer counter) is handed over for
    /// real. Its first rows are not read; the open is done once it is installed.
    #[cfg(test)]
    pub(crate) fn install_for_tests(
        &mut self,
        state: DataTableState,
        path: Option<PathBuf>,
        options: &OpenOptions,
        debug_label: Option<String>,
    ) -> bool {
        self.make_way_for_an_open();
        let _ = self
            .loading
            .open_frame(LazyFrame::default(), options.clone());
        let load = self.loading.id().expect("an open was begun");
        let loading::Step::Install(loaded) = self.loading.answered(
            load,
            loading::LoadAnswer::SchemaRead {
                state: Box::new(state),
                path,
                options: options.clone(),
                debug_label,
            },
            #[cfg(any(feature = "http", feature = "cloud"))]
            &self.jobs,
        ) else {
            unreachable!("a schema read installs");
        };
        let view = self.install_dataset(*loaded);
        self.first_rows_settled();
        view
    }

    /// A page owed to the dataset on screen, as one asked for while the generation was
    /// held is.
    #[cfg(test)]
    pub(crate) fn owe_rows_for_tests(&mut self, status: &str) {
        let owed = Job::OwedRows {
            dataset: self.dataset_generation,
            status: status.to_string(),
        };
        self.jobs.owe(owed, Some(status));
    }

    /// Whether a page is owed.
    #[cfg(test)]
    pub(crate) fn rows_owed(&self) -> bool {
        self.jobs.owed(Self::owed_rows).is_some()
    }

    /// A current `job` answers `answer` at once, and the app handles it: for tests of
    /// what an answer does.
    #[cfg(test)]
    pub(crate) fn answer_for_tests(&mut self, job: Job, answer: Answer) -> Option<AppEvent> {
        let started = self.jobs.start(job, None);
        let ticket = started.ticket();
        started.end(Outcome::answered(answer));
        self.job_ended(ticket)
    }

    /// Whether anything is waiting on the current generation, so that advancing it
    /// would throw away an answer nothing will ask for again. See
    /// [`Jobs::would_strand`].
    fn work_a_bump_would_strand(&self) -> bool {
        self.jobs.would_strand()
    }

    /// Run a scroll on `data_table_state` and resolve the busy/spawn cycle.
    /// `scroll` returns true when its movement leaves the buffered window (caller must collect).
    /// We clear `busy` ourselves when no collect is needed or the spawn no-ops, otherwise
    /// the busy flag set by the key handler would gate further input forever.
    /// Home, End and G. A jump may need a fill, so it is deferred behind a frame that
    /// shows the throbber — setting `start_row` alone used to leave the old buffer on
    /// screen, drawn from its first row — unless the view is already there, in which
    /// case only the selection settles and no frame or key is spent.
    fn jump_key(&mut self, jump: AppEvent) -> Option<AppEvent> {
        // The end of a remote dataset is not known until its rows are counted, and a
        // jump to a guess reads every file up to it. Wait for the count instead; keys
        // keep working meanwhile.
        // A dataset still reading its own footers is already getting a count, and its
        // end is known as soon as that lands. Starting one here would read every footer
        // a second time — and the join takes a fresh `len_generation` on its way past,
        // so the count that came back would be answering a question nobody could match
        // it to and the jump would never happen. Wait for the pass instead.
        // `scan_is_the_root`, not `counts_itself_later`: the question here is whether a
        // join is going to land underneath this frame and take a fresh `len_generation`
        // with it, which is what would leave a count answering a question nothing could
        // match it to. A filter and a sort are rebuilt over the joined scan, so they are
        // on this side of it even though they are not pristine.
        if matches!(jump, AppEvent::DoScrollEnd)
            && let Some(state) = self.data_table_state.as_ref()
            && state.footers_pending().is_some()
            && state.scan_is_the_root()
            && !state.is_num_rows_valid()
        {
            self.end_when_the_footers_land = Some(self.dataset_generation);
            self.status_message = Some(Self::COUNTING_FOR_END.to_string());
            return None;
        }
        // Any other frame whose end is not known yet waits for its count too, rather than
        // jumping to the end of the rows read so far. A count waiting on a paint starts
        // now; one already running or riding in a collect is waited on.
        if matches!(jump, AppEvent::DoScrollEnd)
            && let Some(state) = self.data_table_state.as_ref()
            && !state.is_num_rows_valid()
        {
            let generation = state.len_generation();
            self.end_after_count = Some(generation);
            self.status_message = Some(Self::COUNTING_FOR_END.to_string());
            let held = self.count_after_paint == Some(generation);
            if held {
                self.count_after_paint = None;
            }
            if held || self.len_count_inflight != Some(generation) {
                self.len_count_inflight = Some(generation);
                self.spawn_count(LenCount::for_state(state));
            }
            return None;
        }
        let state = self.data_table_state.as_mut()?;
        let (already_there, settle): (bool, fn(&mut DataTableState) -> bool) = match jump {
            AppEvent::DoScrollHome => (state.start_row() == 0, DataTableState::scroll_to_start),
            _ => (state.at_end(), DataTableState::scroll_to_end),
        };
        if already_there {
            settle(state);
            return None;
        }
        self.busy = true;
        Some(jump)
    }

    fn handle_scroll<F>(&mut self, scroll: F) -> Option<AppEvent>
    where
        F: FnOnce(&mut crate::widgets::datatable::DataTableState) -> bool,
    {
        let needs = self.data_table_state.as_mut().is_some_and(scroll);
        if !needs || !self.spawn_async_collect(Self::LOADING_BUFFER) {
            self.busy = false;
            self.status_message = None;
        }
        None
    }

    /// Hand the export modal's path input a key, and when the value changed, follow
    /// the typed extension with the format radio — the alternative was Parquet bytes
    /// in a file named `out.csv`, with nothing on screen saying so. Cursor-only keys
    /// change nothing and re-pick nothing, so a format chosen after typing stands.
    fn export_path_key(&mut self, event: &KeyEvent) {
        let before = self.export_modal.path_input.value().to_string();
        self.export_modal.path_input.handle_key(event, None);
        if self.export_modal.path_input.value() != before {
            self.export_modal.sync_format_to_path();
            // Typing is the correction the message asked for.
            self.export_modal.path_error = None;
        }
    }

    fn ensure_file_extension(
        path: &Path,
        format: ExportFormat,
        compression: Option<CompressionFormat>,
    ) -> PathBuf {
        let current_ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
        let mut new_path = path.to_path_buf();

        if current_ext.is_empty() {
            // No extension: use default for format (and add compression if selected)
            let desired_ext = if let Some(comp) = compression {
                format!("{}.{}", format.extension(), comp.extension())
            } else {
                format.extension().to_string()
            };
            new_path.set_extension(&desired_ext);
        } else {
            // User provided an extension: keep it. Only add compression suffix when compression is selected.
            let is_compression_only = matches!(
                current_ext.to_lowercase().as_str(),
                "gz" | "zst" | "bz2" | "xz"
            ) && ExportFormat::from_extension(current_ext).is_none();

            if is_compression_only {
                // Path has only compression ext (e.g. file.gz); stem may have format (file.csv.gz)
                let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("");
                let stem_has_format = stem
                    .split('.')
                    .next_back()
                    .and_then(ExportFormat::from_extension)
                    .is_some();
                if stem_has_format {
                    if let Some(comp) = compression
                        && let Some(format_ext) = stem
                            .split('.')
                            .next_back()
                            .and_then(ExportFormat::from_extension)
                            .map(|f| f.extension())
                    {
                        new_path =
                            PathBuf::from(stem.rsplit_once('.').map(|x| x.0).unwrap_or(stem));
                        new_path.set_extension(format!("{}.{}", format_ext, comp.extension()));
                    }
                } else if let Some(comp) = compression {
                    new_path.set_extension(format!("{}.{}", format.extension(), comp.extension()));
                } else {
                    new_path.set_extension(format.extension());
                }
            } else if let Some(comp) = compression
                && format.supports_compression()
            {
                new_path.set_extension(format!("{}.{}", current_ext, comp.extension()));
            }
            // else: path stays as-is (e.g. foo.feather stays foo.feather)
            // else: path with format extension stays as-is
        }

        new_path
    }

    pub fn new(events: Sender<AppEvent>, runtime: tokio::runtime::Handle) -> App {
        // Create default theme for backward compatibility
        let theme = Theme::from_config(&AppConfig::default().theme).unwrap_or_else(|_| {
            // Create a minimal fallback theme
            Theme {
                colors: std::collections::HashMap::new(),
            }
        });

        Self::new_with_config(events, runtime, theme, AppConfig::default())
    }

    pub fn new_with_theme(
        events: Sender<AppEvent>,
        runtime: tokio::runtime::Handle,
        theme: Theme,
    ) -> App {
        Self::new_with_config(events, runtime, theme, AppConfig::default())
    }

    /// An app with its saved views read here, before it is returned.
    pub fn new_with_config(
        events: Sender<AppEvent>,
        runtime: tokio::runtime::Handle,
        theme: Theme,
        app_config: AppConfig,
    ) -> App {
        let templates = TemplateManager::load_or_empty().into();
        Self::new_with_templates(events, runtime, theme, app_config, templates)
    }

    /// An app whose saved views may still be on their way ([`Templates`]).
    pub fn new_with_templates(
        events: Sender<AppEvent>,
        runtime: tokio::runtime::Handle,
        theme: Theme,
        app_config: AppConfig,
        template_manager: Templates,
    ) -> App {
        let cache = CacheManager::new(APP_NAME).unwrap_or_else(|_| CacheManager {
            cache_dir: std::env::temp_dir().join(APP_NAME),
        });
        let jobs = Jobs::new(events.clone());
        let formats = Arc::new(crate::formats::Registry::load(
            &crate::formats::search_path_for(&app_config),
        ));
        for error in &formats.errors {
            log::warn!("format spec skipped: {error}");
        }

        App {
            path: None,
            data_table_state: None,
            footer_progress: Arc::new(crate::schema_union::FooterProgress::default()),
            footers_this_frame: None,
            home: home::HomeState {
                hide_unreadable: !app_config.data.show_unreadable_files,
                formats: formats.clone(),
                ..Default::default()
            },
            home_probes_inflight: Vec::new(),
            #[cfg(feature = "cloud")]
            cloud_discovery_started: false,
            home_search_inflight: false,
            home_generation: 0,
            home_schema_inflight: Vec::new(),
            last_load_error: None,
            pending_clear_recents: false,
            pending_forget_place: None,
            home_schema_cache: HashMap::new(),
            home_previews: crate::home_preview::Previews::default(),
            reads: crate::home_preview::ReadCounts::default(),
            shape_remembered: None,
            original_file_format: None,
            original_file_delimiter: None,
            stdin_reader: None,
            stdout_pass: None,
            follow_drawn: None,
            pending_leave: None,
            recording_on: None,
            recording_end_said: false,
            events,
            debug: DebugState::default(),
            info_modal: InfoModal::new(),
            file_facts: None,
            query_input: TextInput::new()
                .with_history_limit(app_config.query.history_limit)
                .with_theme(&theme)
                .with_history("query".to_string()),
            sql_input: TextInput::statement()
                .with_history_limit(app_config.query.history_limit)
                .with_theme(&theme)
                .with_history("sql".to_string()),
            fuzzy_input: TextInput::new()
                .with_history_limit(app_config.query.history_limit)
                .with_theme(&theme)
                .with_history("fuzzy".to_string()),
            find: find::Find::new(
                TextInput::new()
                    .with_history_limit(app_config.query.history_limit)
                    .with_theme(&theme)
                    .with_history("find".to_string()),
            ),
            input_mode: InputMode::Normal,
            input_type: None,
            query_mode: QueryMode::default().resolve(),
            query_focus: QueryFocus::Input,
            sql_columns: Vec::new(),
            sql_completion: None,
            query_running: None,
            query_run_error: None,
            inline_failures: 0,
            sort_filter_modal: SortFilterModal::new(),
            pivot_melt_modal: PivotMeltModal::new(),
            template_modal: TemplateModal::new(),
            opened_from_home: false,
            startup_template: None,
            analysis_modal: AnalysisModal::with_sample_rows(
                app_config.performance.analysis_sample_rows,
            ),
            quality_cache: Vec::new(),
            quality_samples: Vec::new(),
            quality_released: Vec::new(),
            quality_memory_budget: QUALITY_MEMORY_BUDGET,
            quality_copies: Vec::new(),
            quality_copy_released: None,
            quality_copy_unusable: None,
            quality_copy_free: std::sync::Mutex::new(None),
            quality_evidence_return: None,
            quality_evidence_label: None,
            chart_modal: ChartModal::new(),
            chart_export_modal: ChartExportModal::new(),
            export_modal: ExportModal::new(),
            copy_modal: copy_modal::CopyModal::new(),
            inspector_modal: inspector_modal::InspectorModal::new(),
            external_open: None,
            open_dir: None,
            go_to_column: crate::widgets::ui::PickerState::default(),
            value_counts: value_counts_modal::ValueCountsModal::default(),
            hex: None,
            hex_serial: 0,
            export_counts: None,
            format_picker: crate::widgets::ui::PickerState::default(),
            clipboard: None,
            pending_copy: None,
            chart_cache: ChartCache::default(),
            chart_inflight: None,
            pending_chart_result: Arc::new(Mutex::new(None)),
            chart_export_waiting: None,
            error_modal: ErrorModal::new(),
            flash: None,
            confirmation_modal: ConfirmationModal::new(),
            pending_export: None,
            pending_chart_export: None,
            pending_quality_export: None,
            show_help: false,
            help_scroll: 0,
            pointer: pointer::Pointing::default(),
            cache,
            template_manager,
            active_template_id: None,
            export_progress: None,
            theme,
            pending_read_all: false,
            history_limit: app_config.query.history_limit,
            table_cell_padding: app_config.display.table_cell_padding.cells(),
            column_colors: app_config.display.column_colors,
            dtype_row: app_config.display.dtype_row,
            number_format: app_config
                .display
                .number_format
                .resolve(app_config.display.align_numeric_right)
                // AppConfig::load validates this, but App can be built from an
                // unvalidated config (e.g. the Python API): fall back to no
                // formatting while still honouring the alignment setting.
                .unwrap_or_else(|_| NumberFormatSettings {
                    align_numeric_right: app_config.display.align_numeric_right,
                    ..Default::default()
                }),
            jobs,
            runtime,
            loading: loading::Loader::default(),
            opened: None,
            loaded_ahead_from: None,
            pending_footers_result: std::sync::Arc::new(std::sync::Mutex::new(None)),
            dataset_generation: 0,
            footers_held: None,
            reread_owed: None,
            #[cfg(test)]
            home_worker_dies: None,
            #[cfg(test)]
            file_facts_reader: None,
            end_when_the_footers_land: None,
            len_count_inflight: None,
            count_after_paint: None,
            #[cfg(test)]
            counts_spawned: std::cell::Cell::new(0),
            #[cfg(test)]
            first_rows_asked: 0,
            len_count_failed: None,
            end_after_count: None,
            busy: false,
            throbber_frame: 0,
            screen_generation: 0,
            input_dropped: false,
            status_message: None,
            analysis_computation: None,
            app_config,
            formats,
        }
    }

    /// Use `registry` as the format specs on the search path, for hosts and tests that
    /// have their specs in hand.
    pub fn set_formats(&mut self, registry: crate::formats::Registry) {
        let registry = Arc::new(registry);
        self.home.formats = registry.clone();
        self.formats = registry;
    }

    pub fn enable_debug(&mut self) {
        self.debug.enabled = true;
    }

    // ---- Home screen -----------------------------------------------------

    /// Schema for a home-screen entry, read from Parquet metadata and memoised for
    /// the session. `None` means "not knowable without a scan", which the UI reports
    /// rather than papering over.
    pub fn home_schema(&mut self, entry: &discover::Entry) -> Option<discover::SchemaPreview> {
        // A schema preview reads a local file. Nothing in an object store is read before
        // it is opened: asking would only come back empty, again on every rebuild.
        if home::is_cloud_place(&entry.path) || home::is_object_store_url(&entry.path) {
            return None;
        }
        if let Some(cached) = self.home_schema_cache.get(&entry.path) {
            return cached.clone();
        }
        // Reading a schema opens a file, so it is requested rather than done here.
        // Until it arrives the preview says so; it never blocks the frame.
        self.request_home_schema(entry.clone());
        None
    }

    /// What a home-screen worker owes in place of its answer if it panics.
    fn owed_answer(&mut self, instead: AppEvent) -> OwedAnswer {
        OwedAnswer {
            tx: self.events.clone(),
            #[cfg(test)]
            dies: self
                .home_worker_dies
                .as_mut()
                .is_some_and(|dies| dies(&instead)),
            instead: Some(instead),
        }
    }

    /// List the directory the `~` prompt is typing, when it is not the one listed. A
    /// URL is listed from what the screen already knows; a local directory is read on
    /// a worker.
    fn list_the_typed_directory(&mut self) {
        if !self.home.path_input_active {
            return;
        }
        let dir = home::typed_dir(&self.home.path_input).to_string();
        if self
            .home
            .path_listing
            .as_ref()
            .is_some_and(|l| l.dir == dir)
        {
            return;
        }
        if home::typed_dir_is_url(&dir) {
            self.home.path_listing = Some(home::names_under(&dir, self.home.known_urls()));
            return;
        }
        // Read off the UI thread: a typed path is where a dead mount gets named.
        let tx = self.events.clone();
        let owed = self.owed_answer(AppEvent::HomePathListed {
            listing: Box::new(home::PathListing {
                dir: dir.clone(),
                names: Vec::new(),
                failed: true,
            }),
        });
        std::thread::spawn(move || {
            owed.run(|| {
                let listing = home::list_typed_dir(&dir);
                let _ = tx.send(AppEvent::HomePathListed {
                    listing: Box::new(listing),
                });
            })
        });
    }

    /// Complete the path being typed, on a worker.
    fn request_path_completion(&mut self) {
        let typed = self.home.path_input.clone();
        if typed.is_empty() {
            return;
        }
        let generation = self.home_generation;
        let tx = self.events.clone();
        std::thread::spawn(move || {
            let (completed, candidates) = home::complete_path(&typed);
            let _ = tx.send(AppEvent::HomePathCompleted {
                generation,
                typed,
                completed,
                candidates,
            });
        });
    }

    /// The first rows of a home-screen file for its `ROWS` preview, read on a worker
    /// the way its open reads them. `None` until they land, and for a row that is not
    /// previewed. `screen_height` sizes the page to the one the table will ask for.
    pub fn home_preview_rows(
        &mut self,
        entry: &discover::Entry,
        screen_height: u16,
    ) -> Option<Arc<crate::home_preview::PreviewRows>> {
        let max = self
            .app_config
            .data
            .preview_max_mb
            .saturating_mul(1024 * 1024);
        if !crate::home_preview::previewable(entry, max) {
            return None;
        }
        let stamp = crate::home_preview::Stamp::of_entry(entry);
        if let Some(known) = self.home_previews.rows(&entry.path, stamp) {
            return known;
        }
        if self.home_previews.inflight.is_none() {
            self.request_home_preview(entry.path.clone(), stamp, screen_height);
        }
        None
    }

    /// Whether `entry` is one the preview reads, before its rows are in.
    pub fn home_preview_pending(&self, path: &Path) -> bool {
        self.home_previews.reading(path)
    }

    /// Read a file's first page on a worker, through the open's own scan and schema
    /// read, so the open can install what it built.
    fn request_home_preview(
        &mut self,
        path: PathBuf,
        stamp: crate::home_preview::Stamp,
        screen_height: u16,
    ) {
        self.home_previews.inflight = Some(path.clone());
        self.reads.previews += 1;
        let tx = self.events.clone();
        let cloud = self.app_config.cloud.clone();
        let formats = self.formats.clone();
        let runtime = self.runtime.clone();
        let cache = self.cache.clone();
        // The table's rows: the screen less the title, the header and the control bar.
        let visible = (screen_height as usize).saturating_sub(3).max(1);
        let owed = self.owed_answer(AppEvent::HomePreviewReady {
            path: path.clone(),
            stamp,
            read_at: None,
            rows: None,
            prepared: crate::home_preview::Handoff::default(),
        });
        self.runtime.spawn_blocking(move || {
            owed.run(|| {
                let began = std::time::Instant::now();
                let read_at = crate::home_preview::Stamp::of_file(&path);
                let read =
                    Self::read_home_preview(&path, &cloud, &formats, &runtime, cache, visible);
                log::debug!(
                    target: "datui",
                    "home preview of {}: {:.1?}",
                    path.display(),
                    began.elapsed()
                );
                let (rows, prepared) = match read {
                    Some((rows, prepared)) => (Some(Arc::new(rows)), Some(Box::new(prepared))),
                    None => (None, None),
                };
                let _ = tx.send(AppEvent::HomePreviewReady {
                    path,
                    stamp,
                    read_at,
                    rows,
                    prepared: Arc::new(Mutex::new(prepared)),
                });
            })
        });
    }

    /// What the open of `path` from the home screen reads first: its scan, its schema
    /// and the page the table asks for when `visible` rows show. Built by the open's
    /// own steps with the options the home screen opens a file with, so the dataset is
    /// the one the open would build.
    fn read_home_preview(
        path: &Path,
        cloud: &crate::config::CloudConfig,
        formats: &crate::formats::Registry,
        runtime: &tokio::runtime::Handle,
        cache: CacheManager,
        visible: usize,
    ) -> Option<(
        crate::home_preview::PreviewRows,
        crate::home_preview::Prepared,
    )> {
        let paths = [path.to_path_buf()];
        let scanned = Self::scan_for_open(
            cloud,
            formats,
            &paths,
            OpenOptions::default(),
            Some(path.to_path_buf()),
        )
        .ok()?;
        let loading::LoadAnswer::Scanned { lf, path, options } = scanned else {
            return None;
        };
        let progress = Arc::<crate::schema_union::FooterProgress>::default();
        let report = crate::measurements::OpenReport {
            progress: progress.clone(),
            meter: Arc::new(crate::measurements::Meter::default()),
            remembered: Some(cache),
        };
        let read = Self::read_schema_for_open(
            *lf,
            path,
            options,
            cloud,
            runtime,
            &report,
            loading::Made::default(),
        )
        .ok()?;
        let loading::LoadAnswer::SchemaRead {
            mut state,
            options,
            debug_label,
            ..
        } = read
        else {
            return None;
        };
        // Planned as the table plans its first page, so the page is the one it wants.
        state.visible_rows = visible;
        let began = std::time::Instant::now();
        let request = state.prepare_async_collect(None)?;
        let df = crate::statistics::collect_lazy(request.lf, request.polars_streaming).ok()?;
        let result = request.plan.fit(df);
        let rows = crate::home_preview::PreviewRows::from_frame(result.rows());
        state.measurements().read_page(began.elapsed(), Some(1));
        state.apply_async_collect(result);
        Some((
            rows,
            crate::home_preview::Prepared {
                state,
                options,
                debug_label,
                progress,
            },
        ))
    }

    /// Whether a schema read is currently out for this path.
    pub fn home_schema_pending(&self, path: &Path) -> bool {
        self.home_schema_inflight.iter().any(|p| p == path)
    }

    /// Read the selected dataset's schema on a worker.
    fn request_home_schema(&mut self, entry: discover::Entry) {
        if self.home_schema_inflight.contains(&entry.path) {
            return;
        }
        self.home_schema_inflight.push(entry.path.clone());

        let generation = self.home_generation;
        let tx = self.events.clone();
        // Remembered as having none, so the preview is not asked for again.
        let owed = self.owed_answer(AppEvent::HomeSchemaReady {
            generation,
            path: entry.path.clone(),
            preview: None,
        });
        self.runtime.spawn_blocking(move || {
            owed.run(|| {
                let preview = discover::schema_preview(&entry);
                let _ = tx.send(AppEvent::HomeSchemaReady {
                    generation,
                    path: entry.path,
                    preview,
                });
            })
        });
    }

    /// Start listing any network roots that have not answered yet.
    ///
    /// Nothing here waits on the result. A share that has gone away leaves its thread
    /// blocked in the kernel — on a `hard` NFS mount that is uninterruptible and the
    /// thread never returns — so the task is abandoned rather than joined, exactly as
    /// an abandoned dataset load is.
    fn spawn_home_probes(&mut self) {
        for root in self.home.pending_probes() {
            if self.home_probes_inflight.contains(&root) {
                continue;
            }
            // Each probe of an unreachable share costs a thread that will never come
            // back. A handful is a rounding error; an unbounded number, on a machine
            // with a page of dead mounts, is not.
            //
            // Except the directory browsed into, which is the whole screen and has
            // nothing else to show. Held behind the cap, it waited on roots the user
            // had left — a few slow bucket listings kept a share's directory on a
            // spinner long after it could have been read. One more thread per
            // directory the user opens is bounded by the user.
            let browsed = self.home.browsing.as_ref() == Some(&root);
            if !browsed && self.home_probes_inflight.len() >= MAX_CONCURRENT_PROBES {
                continue;
            }
            self.home_probes_inflight.push(root.clone());
            let tx = self.events.clone();
            let cache = self.cache.clone();
            let owed = self.owed_answer(AppEvent::HomeProbeFailed {
                root: root.clone(),
                message: "Could not read it; see the log".to_string(),
            });
            #[cfg(feature = "cloud")]
            let cloud = self.app_config.cloud.clone();
            #[cfg(feature = "cloud")]
            let runtime = self.runtime.clone();
            // A detached OS thread, not the runtime's blocking pool. A thread wedged
            // on an unreachable `hard` mount never returns, and the pool is shared with
            // the work that actually loads data — a few dead shares must not eat into
            // the capacity that opening a dataset depends on.
            std::thread::spawn(move || {
                owed.run(|| {
                    // A bucket or a prefix inside one. It looks like a network root to
                    // everything above, and it is, but it is read with an object-store
                    // listing rather than `read_dir` — which on a `gs://` path fails, which
                    // is why descending into a bucket used to show nothing at all.
                    //
                    // Deliberately metadata-only. A delimited listing returns names, sizes
                    // and modification times for one level, and nothing here reads an
                    // object's contents: no footers, no schemas, no row counts. Those are
                    // what a local listing fills in for free from bytes already on the
                    // machine, and what would cost a ranged read per row against an object
                    // store somebody pays egress on.
                    #[cfg(feature = "cloud")]
                    if let Some((id, account)) = home::cloud_account(&root) {
                        let listed = wait_on_runtime(&runtime, async move {
                            crate::cloud_browse::list_account(&id, &account, &cloud).await
                        });
                        match listed {
                            Some(Ok(rows)) => {
                                let _ = tx.send(AppEvent::HomeProbeReady {
                                    root,
                                    rows: Some(rows),
                                    cut_short: false,
                                });
                            }
                            Some(Err(message)) => {
                                log::warn!(
                                    target: "datui::cloud",
                                    "listing {} failed: {message}",
                                    root.display()
                                );
                                let _ = tx.send(AppEvent::HomeProbeFailed { root, message });
                            }
                            None => {
                                let _ = tx.send(AppEvent::HomeProbeReady {
                                    root,
                                    rows: None,
                                    cut_short: false,
                                });
                            }
                        }
                        return;
                    }
                    #[cfg(feature = "cloud")]
                    if crate::cloud_browse::split_bucket_url(&root.to_string_lossy()).is_some()
                        || source::azure_parts(&root.to_string_lossy()).is_some()
                    {
                        let url = root.to_string_lossy().into_owned();
                        let listed = wait_on_runtime(&runtime, async move {
                            crate::cloud_browse::list_objects(&url, &cloud).await
                        });
                        // A refused listing says why, rather than reading as a place that
                        // stopped answering.
                        match listed {
                            Some(Err(message)) => {
                                log::warn!(
                                    target: "datui::cloud",
                                    "listing {} failed: {message}",
                                    root.display()
                                );
                                let _ = tx.send(AppEvent::HomeProbeFailed { root, message });
                            }
                            other => {
                                let _ = tx.send(AppEvent::HomeProbeReady {
                                    root,
                                    rows: other.and_then(Result::ok),
                                    cut_short: false,
                                });
                            }
                        }
                        return;
                    }
                    // Nothing to list a bucket with, and `read_dir` on its URL would
                    // only call it unavailable.
                    #[cfg(not(feature = "cloud"))]
                    if source::is_remote_url(&root) {
                        let message = "cloud support not in this build".to_string();
                        let _ = tx.send(AppEvent::HomeProbeFailed { root, message });
                        return;
                    }
                    let mut cut_short = false;
                    let rows = if std::fs::read_dir(&root).is_ok() {
                        // What has been read shows while the rest is read: a share can take
                        // seconds over a directory of thousands.
                        let scan = crate::discover::scan_dir_progressive(&root, |so_far| {
                            let _ = tx.send(AppEvent::HomeProbeProgress {
                                root: root.clone(),
                                rows: so_far.to_vec(),
                            });
                        });
                        cut_short = scan.truncated;
                        let mut rows = scan.entries;
                        // Measuring happens here too: it is the same remote filesystem,
                        // and this thread is already the one allowed to block on it.
                        for row in rows.iter_mut().take(PROBE_MEASURE_LIMIT) {
                            crate::discover::enrich(row);
                        }
                        // Remote datasets are measured nowhere else, so this is the only
                        // chance to remember them. Without it a remote row is blank on
                        // every run, which is exactly backwards: the hardest things to
                        // reach are the ones most worth remembering.
                        let mounts = crate::locality::Mounts::current();
                        for row in rows.iter_mut() {
                            row.cost.source = Some(mounts.describe(&row.path).fstype);
                        }
                        let facts: Vec<_> = rows.iter().filter_map(home::facts_for).collect();
                        cache.record_dataset_facts(&facts);
                        Some(rows)
                    } else {
                        None
                    };
                    let _ = tx.send(AppEvent::HomeProbeReady {
                        root,
                        rows,
                        cut_short,
                    });
                })
            });
        }
    }

    /// Find the cloud sources this machine and the config describe, and list their
    /// buckets when `[cloud] list_on_start` asks. Once per session; Ctrl+R asks again.
    #[cfg(feature = "cloud")]
    fn spawn_cloud_discovery(&mut self) {
        if self.cloud_discovery_started {
            return;
        }
        self.cloud_discovery_started = true;
        let list = self.app_config.cloud.list_on_start == Some(true);
        self.list_cloud_sources(None, list);
    }

    /// List the source being browsed, when it has not been asked this session.
    /// Entering a source is the request to list it.
    #[cfg(feature = "cloud")]
    fn list_browsed_cloud_source(&mut self) {
        let Some(id) = self
            .home
            .browsing
            .as_deref()
            .and_then(home::cloud_source_id)
        else {
            return;
        };
        let Some(source) = self.home.cloud.iter_mut().find(|s| s.id == id) else {
            return;
        };
        if source.asked {
            return;
        }
        source.begin_listing();
        self.list_cloud_sources(Some(id), true);
    }

    /// Send the rows of every source, or list the buckets of the one named.
    ///
    /// The rows go out first, filled from the last run's listing when the source still
    /// points at the same place, so the home screen has its counts before any request
    /// is made. With `list`, the sources are then listed side by side, a few at a
    /// time, and each result is sent the moment it arrives. Without it nothing leaves
    /// the machine: no request, and no credential command.
    ///
    /// Runs on the runtime rather than a detached thread. Unlike a probe of a dead
    /// `hard` mount, an HTTP request cannot wedge forever: every call here is bounded
    /// by a global timeout, so the task is guaranteed to end.
    #[cfg(feature = "cloud")]
    fn list_cloud_sources(&mut self, only: Option<String>, list: bool) {
        let tx = self.events.clone();
        let cloud = self.app_config.cloud.clone();
        let cache = self.cache.clone();
        self.runtime.spawn(async move {
            let mut hidden = cache.load_hidden_cloud_sources();
            hidden.extend(cloud.hide.iter().cloned());
            let listings = cache.load_cloud_listings();
            let cached_for = |source: &crate::cloud_sources::Source| {
                listings
                    .get(&source.id)
                    .filter(|l| l.fingerprint == source.fingerprint())
            };
            let found = {
                let env = crate::cloud_browse::Environment::current();
                crate::cloud_sources::discover(&cloud, &env)
            };
            // Shown or not: a bucket under Recent opens with the login that listed it
            // whatever `discover` says. Not a hidden source, which may be hidden for a
            // login that no longer works; the default login opens its buckets instead.
            // Only with the rows, so an old listing never overrides one made since.
            if only.is_none() {
                for source in found.iter().filter(|s| !hidden.contains(&s.id)) {
                    if let Some(cached) = cached_for(source) {
                        crate::cloud_sources::remember_listed(source, &cached.buckets);
                    }
                }
            }
            let sources: Vec<crate::cloud_sources::Source> =
                crate::cloud_sources::on_home(found, &cloud)
                    .into_iter()
                    .filter(|s| !hidden.contains(&s.id))
                    .collect();

            match &only {
                None => {
                    let rows = sources
                        .iter()
                        .map(|source| home_cloud_source(source, cached_for(source), list))
                        .collect();
                    let _ = tx.send(AppEvent::HomeCloudSources { sources: rows });
                }
                // Gone since its row was drawn: a profile removed, a source hidden
                // elsewhere. Said, so the row does not wait on an answer never coming.
                Some(id) if !sources.iter().any(|s| &s.id == id) => {
                    let _ = tx.send(AppEvent::HomeCloudListed {
                        id: id.clone(),
                        buckets: Vec::new(),
                        details: Vec::new(),
                        failure: Some((
                            "not found".to_string(),
                            format!("{id} is gone or hidden. Ctrl+R at the top looks again."),
                        )),
                        listed_at: std::time::SystemTime::now(),
                    });
                    return;
                }
                Some(_) => {}
            }
            if !list {
                return;
            }

            // Enough to keep one slow endpoint from delaying the rest, few enough that a
            // long list of sources does not open a connection storm.
            const LISTING_AT_ONCE: usize = 4;
            let permits = std::sync::Arc::new(tokio::sync::Semaphore::new(LISTING_AT_ONCE));
            let mut listings = tokio::task::JoinSet::new();
            for source in sources
                .into_iter()
                .filter(|s| only.as_ref().is_none_or(|id| &s.id == id))
            {
                let permits = permits.clone();
                listings.spawn(async move {
                    let _permit = permits.acquire_owned().await;
                    let result = crate::cloud_browse::list_first_level(&source).await;
                    (source, result)
                });
            }
            while let Some(joined) = listings.join_next().await {
                let Ok((source, result)) = joined else {
                    continue;
                };
                let listed_at = std::time::SystemTime::now();
                // Buckets named in the config are shown whether or not the login can
                // list them; that is what naming them is for.
                let mut names = source.buckets.clone();
                let mut details = Vec::new();
                let failure = match result {
                    Ok(listed) => {
                        for item in listed {
                            crate::cloud_sources::remember_bucket(&source, &item.name);
                            if !item.details.is_empty() {
                                details.push((item.place.clone(), item.details));
                            }
                            let name = item.name;
                            if !names.contains(&name) {
                                names.push(name);
                            }
                        }
                        cache.save_cloud_listing(
                            &source.id,
                            crate::cache::CloudListing {
                                fingerprint: source.fingerprint(),
                                buckets: names.clone(),
                                listed_at: listed_at
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .map(|d| d.as_secs())
                                    .unwrap_or(0),
                            },
                        );
                        None
                    }
                    Err(e) => {
                        log::warn!(target: "datui::cloud", "listing {} failed: {e}", source.id);
                        Some(summarize_cloud_failure(&e))
                    }
                };
                let _ = tx.send(AppEvent::HomeCloudListed {
                    id: source.id.clone(),
                    buckets: names
                        .iter()
                        .map(|b| PathBuf::from(source.bucket_url(b)))
                        .collect(),
                    details,
                    failure,
                    listed_at,
                });
            }
        });
    }

    /// Ask again for what is on screen, ignoring what is cached: the buckets of the
    /// source being browsed, the contents of the bucket or directory being browsed, or
    /// every source's buckets from the home listing.
    fn home_reload(&mut self) {
        #[cfg(feature = "cloud")]
        {
            let browsing = self.home.browsing.clone();
            match browsing.as_deref().and_then(home::cloud_source_id) {
                Some(id) => {
                    if let Some(source) = self.home.cloud.iter_mut().find(|s| s.id == id) {
                        source.begin_listing();
                    }
                    self.list_cloud_sources(Some(id), true);
                }
                None if browsing.is_none() && !self.home.cloud.is_empty() => {
                    for source in &mut self.home.cloud {
                        source.begin_listing();
                    }
                    self.list_cloud_sources(None, true);
                }
                None => {}
            }
        }
        if let Some(dir) = self.home.browsing.clone() {
            self.home.probed.remove(&dir);
            self.home.unreachable.remove(&dir);
            self.home.cut_short.remove(&dir);
        }
        // A peek that failed is asked again: Ctrl+R is the request to try.
        self.home.peek_failed.clear();
        self.home.status = None;
        self.home_refresh();
    }

    /// Start the recursive search below the working directory, if it is wanted and
    /// not already running.
    ///
    /// Triggered by typing rather than by opening the home screen: typing is the
    /// signal that someone is looking for something. Launching datui, pressing Enter
    /// on a recent dataset and leaving costs no walk at all.
    fn spawn_home_search(&mut self) {
        if self.home_search_inflight || self.home.search.done {
            return;
        }
        let config = self.app_config.data.search.clone();
        if !config.enabled {
            return;
        }
        let Some(root) =
            crate::search::search_root(self.home.browsing.as_ref(), self.home.network_check)
        else {
            return;
        };

        self.home.search.reset();
        self.home.search.root = Some(root.clone());
        self.home.search.running = true;
        self.home.search.epoch = next_search_epoch();
        self.home.search_limit = config.max_results;
        self.home_search_inflight = true;

        let generation = self.home_generation;
        let tx = self.events.clone();
        // Ended, with what the batches already found kept.
        let owed = self.owed_answer(AppEvent::HomeSearchDone {
            generation,
            root: root.clone(),
            scanned: 0,
            limited: Some("partial · failed".to_string()),
        });
        // A detached thread for the same reason the probes use one: the walk touches
        // a filesystem, and nothing that touches a filesystem may run where a stall
        // would stop the screen from drawing.
        std::thread::spawn(move || {
            owed.run(|| {
                let walk_root = root.clone();
                let batch_tx = tx.clone();
                let batch_gen = generation;
                let batch_root = root.clone();
                let outcome = crate::search::walk(&walk_root, &config, move |found, outcome| {
                    // Sent even when empty: it carries the progress count, and it is the
                    // only place the walk learns that nobody is listening any more.
                    batch_tx
                        .send(AppEvent::HomeSearchBatch {
                            generation: batch_gen,
                            root: batch_root.clone(),
                            found,
                            scanned: outcome.scanned,
                        })
                        // A closed channel means the app is gone; stop walking.
                        .is_ok()
                });
                let _ = tx.send(AppEvent::HomeSearchDone {
                    generation,
                    root,
                    scanned: outcome.scanned,
                    limited: outcome.note().map(str::to_string),
                });
            })
        });
    }

    /// Score the filter against the search's files on a worker, when a scoring is owed.
    ///
    /// Asked after every event. Over a tree of tens of thousands of files the scoring
    /// is what held each keystroke's echo back, so it runs where a stall cannot hold
    /// the screen, one at a time; each answer asks for the next if the filter moved on.
    fn home_score_search(&mut self) {
        if self.input_mode != InputMode::Home {
            return;
        }
        let Some(job) = self.home.score_job() else {
            return;
        };
        let epoch = job.epoch;
        let tx = self.events.clone();
        // A worker that dies answers with nothing.
        let owed = self.owed_answer(AppEvent::HomeSearchScored {
            epoch,
            matches: None,
        });
        self.runtime.spawn_blocking(move || {
            owed.run(move || {
                let matches =
                    crate::search::score(&job.results, &job.query, job.base.as_ref(), job.limit);
                let _ = tx.send(AppEvent::HomeSearchScored {
                    epoch,
                    matches: Some(Box::new(matches)),
                });
            })
        });
    }

    /// Rebuild the home listing from the filesystem.
    fn home_refresh(&mut self) {
        // Every way into a source comes through here: Enter, Backspace up from a
        // bucket, a jump, and rows arriving while the source is already open.
        #[cfg(feature = "cloud")]
        self.list_browsed_cloud_source();
        self.home_generation = self.home_generation.wrapping_add(1);
        let generation = self.home_generation;

        self.home.collections = home::collections(&self.app_config);
        let mut request = home::ListingRequest {
            config_dirs: self.app_config.data.resolved_directories(),
            // Filled in on the worker, from the cache and the desktop's recents: files
            // all the same, and the first frame does not wait on a file.
            remembered_dirs: Vec::new(),
            recents: Vec::new(),
            desktop_dirs: Vec::new(),
            browsing: self.home.browsing.clone(),
            probed: self.home.probed.clone(),
            unreachable: self.home.unreachable.clone(),
            listing_so_far: self.home.listing_so_far.clone(),
            cut_short: self.home.cut_short.clone(),
            probe_errors: self.home.probe_errors.clone(),
            network_check: self.home.network_check,
            cloud: self.home.cloud.clone(),
            collections: self.home.collections.clone(),
            known: Default::default(),
            formats: self.formats.clone(),
        };
        let read_folds = std::mem::take(&mut self.home.folds_owed);
        let desktop = self.app_config.data.use_desktop_recents;
        let cache = self.cache.clone();

        self.home.listing_in_flight = true;
        let tx = self.events.clone();
        let owed = self.owed_answer(AppEvent::HomeListingFailed);
        self.runtime.spawn_blocking(move || {
            owed.run(move || {
                // Ranked by frecency; the newest is where the cursor lands, so the
                // last file is still one Enter away.
                let recents = cache.load_recents();
                let visits = cache.load_visits();
                let newest = recents.first().cloned();
                request.recents = crate::cache::by_frecency(recents, &visits);
                request.remembered_dirs = cache.load_remembered_places();
                request.known = cache.load_dataset_facts();
                if desktop {
                    request.desktop_dirs = home::desktop_recent_dirs();
                }
                let listing = home::build_listing(&request);
                let _ = tx.send(AppEvent::HomeListingReady {
                    generation,
                    listing: Box::new(listing),
                    known: request.known,
                    visits,
                    newest,
                    folds: read_folds.then(|| cache.load_folds()),
                });
            })
        });
    }

    /// Ask the worker to measure rows that are on screen and not yet known.
    ///
    /// Reading a Parquet footer opens a file. That is the call that blocks on a FIFO,
    /// a device node, a wedged mount or a failing disk, so it never happens on the
    /// thread that draws.
    fn request_home_measurements(&mut self) {
        if self.home.measure_in_flight {
            return;
        }
        let wanted = self.home.unmeasured_visible(MEASURE_BATCH);
        if wanted.is_empty() {
            return;
        }

        self.home.measure_in_flight = true;
        let tx = self.events.clone();
        let cache = self.cache.clone();
        self.runtime.spawn_blocking(move || {
            home::look_into_batch(wanted, &cache, |path, m| {
                let _ = tx.send(AppEvent::HomeMeasured {
                    measured: vec![(path, m)],
                    done: false,
                });
            });
            let _ = tx.send(AppEvent::HomeMeasured {
                measured: Vec::new(),
                done: true,
            });
        });
    }

    /// Ask for what the frame just drawn needs and did not have: counts for the rows
    /// on screen that have none, and kinds for the rows nothing has looked into.
    ///
    /// After the frame, never during it. Scrolling is what brings new rows into view,
    /// and the reading is a worker's job — this thread only decides what is worth
    /// asking about.
    pub fn request_what_the_frame_needs(&mut self) {
        if self.input_mode == InputMode::Normal {
            self.load_ahead();
            self.catch_up_follow();
        }
        self.inspector_needs();
        if self.input_mode != InputMode::Home {
            return;
        }
        if std::mem::take(&mut self.home.pending_enrich) {
            self.request_home_measurements();
        }
        if std::mem::take(&mut self.home.pending_classify) {
            self.request_home_classifications();
        }
        #[cfg(feature = "cloud")]
        if std::mem::take(&mut self.home.pending_peek) {
            self.peek_cloud_directories();
        }
    }

    /// Ask a worker what the rows on screen are.
    ///
    /// Classifying a row means reading the directory it names, which on a share is a
    /// round trip and on a wedged mount never returns — so it happens here rather
    /// than while the listing is built, where it was paid for in directory order and
    /// bought a label for the first sixty-four rows and a wrong one for the rest.
    ///
    /// A detached thread, not the runtime's blocking pool, for the reason the probes
    /// give: a thread stuck on an unreachable `hard` mount never comes back, and the
    /// pool is shared with the work that actually loads data. One at a time, so a
    /// share that has stopped answering costs one thread and then stops asking.
    ///
    /// This measures remote rows as well as classifying them, which
    /// [`HomeState::unmeasured_visible`] deliberately refuses to do — it leaves them to
    /// their root's probe, so that a share gets one thread and not two. That reasoning
    /// no longer reaches: the probe scans a remote directory before anything has looked
    /// into it, so every row it returns is `Unknown` and there is nothing for it to
    /// measure. This pass is the only thing left that can, and it makes the same bargain
    /// the probe made — one detached thread, on a filesystem it is already reading.
    fn request_home_classifications(&mut self) {
        if self.home.classify_in_flight {
            return;
        }
        let wanted = self.home.unclassified_visible(CLASSIFY_BATCH);
        if wanted.is_empty() {
            return;
        }

        self.home.classify_in_flight = true;
        let tx = self.events.clone();
        let cache = self.cache.clone();
        std::thread::spawn(move || {
            home::look_into_batch(wanted, &cache, |path, m| {
                let _ = tx.send(AppEvent::HomeClassified {
                    measured: vec![(path, m)],
                    done: false,
                });
            });
            let _ = tx.send(AppEvent::HomeClassified {
                measured: Vec::new(),
                done: true,
            });
        });
    }

    /// Enter the home screen, rebuilding it, abandoning any in-flight load.
    ///
    /// Returning home puts the cursor on whatever you currently have open, so the
    /// round trip out and back lands where you left rather than at the top.
    ///
    /// Abandoning puts the open in flight down at once ([`loading::Loader::retire`]):
    /// its jobs are superseded, so their answers are dropped on arrival and none can
    /// install a dataset or take the user off the screen they went to; its stop flag
    /// is raised, so a download stops and an in-flight cloud pass stops issuing paid
    /// reads within a wave. Work that is not the open's — an export, an analysis, the
    /// footer pass of the dataset already on screen — is deliberately left alone, so
    /// its progress indicator and its completion modal must survive this.
    pub fn abandon_load(&mut self) {
        let retired = self.loading.retire();
        if let Some(retired) = retired {
            self.put_down_load(retired);
        }
        // A chart being prepared for the dataset we are leaving would otherwise keep
        // the throbber up on the home screen, and its result could later land in a
        // different dataset with the same column names.
        self.reset_chart_state();
        // A look that is out belongs to the home screen being left, and the thread it is
        // on may never come back — a share that has gone away is the case it exists for.
        // Superseded, its answer touches nothing, and the keyboard does not wait for it.
        if self.jobs.supersede(|job| matches!(job, Job::Classify(_))) {
            self.home.status = None;
        }
        // And a collect that was waiting behind this load goes with it. Left standing,
        // it runs the moment the generation is free — reading the dataset the user
        // walked away from, at the home screen, with every key held.
        self.jobs.take_owed(Self::owed_rows);
        // Only an open's own wait is put down. An export holds keys too, and it keeps
        // running. The rows the open's last step is reading still land; nobody waits on
        // them.
        if retired.is_some() {
            self.busy = false;
            let quieted = self.jobs.quiet(Self::reading_rows);
            if self
                .status_message
                .as_ref()
                .is_some_and(|status| quieted.contains(status))
            {
                self.status_message = None;
            }
        }
        // Keys typed at the frozen screen were meant for the load, not for home:
        // replayed there they could open a dataset nobody asked for.
        self.screen_generation = self.screen_generation.wrapping_add(1);
    }

    pub fn enter_home(&mut self) {
        if self.return_from_quality_evidence(false) {
            self.analysis_modal.close();
        }
        // The template modal keys and renders off its own `active`, not the input
        // mode, so left open here it would come back as a zombie over the next
        // dataset opened.
        self.template_modal.close();
        self.inspector_modal.close();
        self.stop_find();
        self.hex = None;
        // A count of the dataset being left is read for nobody.
        self.stop_value_count();
        self.export_counts = None;
        self.abandon_load();
        // Nobody is watching the file any more.
        if let Some(state) = self.data_table_state.as_mut() {
            state.stop_following();
        }
        self.home.status = None;
        self.home.folds_owed = true;
        self.home_refresh();
        if let Some(open_path) = self.path.clone() {
            let target =
                crate::canonical::canonicalize(&open_path).unwrap_or_else(|_| open_path.clone());
            if let Some(idx) = self.home.visible().iter().position(|row| match row {
                home::Row::Entry { entry, .. } => {
                    crate::canonical::canonicalize(&entry.path)
                        .unwrap_or_else(|_| entry.path.clone())
                        == target
                }
                // Not the door: its path is the directory's, so an open file whose
                // directory is being browsed would put the cursor on the row that
                // opens the whole directory rather than on the file itself.
                home::Row::Header { .. }
                | home::Row::Place { .. }
                | home::Row::More { .. }
                | home::Row::Hidden { .. }
                | home::Row::Door { .. } => false,
            }) {
                self.home.selected = idx;
            }
        }
        self.input_mode = InputMode::Home;
    }

    /// Esc backs out one layer of context at a time: the filter, then the directory
    /// descended into, then back to the data that was open. At the top level it does
    /// nothing. It used to quit there, which made a reflexive Esc close the program
    /// while the same key one level down merely went up; Ctrl+C quits, from anywhere.
    fn home_escape(&mut self) -> Option<AppEvent> {
        if !self.home.filter.is_empty() {
            self.home.filter.clear();
            self.home.sync_search_section();
            self.home.selected = 0;
            self.home.clamp_selection();
            return None;
        }
        if self.home.browsing.is_some() {
            if self.home.below_browse_start() {
                self.home_ascend();
            } else {
                // Climbing past where the browse began would take Esc somewhere the
                // user never was; it returns to the listing they started from instead.
                self.home_leave_browsing(None);
            }
            return None;
        }
        if self.data_table_state.is_some() {
            self.input_mode = InputMode::Normal;
            // Said on arrival: Esc pressed once too often to clear the home screen lands
            // here, and the keys typed next act on the table (#547 D14).
            let name = self
                .path
                .as_deref()
                .and_then(|p| p.file_name())
                .map(|n| n.to_string_lossy().into_owned());
            if let Some(name) = name {
                self.flash_note(format!("Back to {name}"));
            }
        }
        None
    }

    /// Drop the highlighted dataset from the recents list.
    ///
    /// Only from the Recent section: a row under a directory is a file on disk, and
    /// forgetting it there would either do nothing or imply a deletion datui is not
    /// going to perform.
    fn home_forget_selected(&mut self) {
        // A place row stands for every recent under it. Forgetting them all is one
        // keystroke from forgetting one, so it asks first, the way Shift+Delete does.
        if let Some(home::Row::Place { path, held, .. }) = self.home.selected_row() {
            self.pending_forget_place = Some(path.clone());
            self.confirmation_modal.show(format!(
                "Forget {held} recently opened {} under {}?",
                if held == 1 { "dataset" } else { "datasets" },
                home::display_path(&path)
            ));
            return;
        }
        // The heading of a remembered place stands for the place. No question first:
        // Ctrl+D puts it back. A row under it is a file on disk, as anywhere else.
        if self.home.browsing.is_none()
            && let Some(section) = self.home.selected_section().map(|i| &self.home.sections[i])
            && let Some(root) = section.root.clone()
        {
            let header = self.home.selection_is_header();
            match section.origin {
                Some(o) if o == home::RootOrigin::Remembered.note() => {
                    if header {
                        self.home_set_remembered(&root, false);
                    } else {
                        self.home.status = Some(format!(
                            "Delete on the heading forgets {}",
                            home::display_path(&root)
                        ));
                    }
                    return;
                }
                Some(o) if o == home::RootOrigin::Configured.note() && header => {
                    self.home.status = Some(Self::configured_place_note(&root));
                    return;
                }
                _ => {}
            }
        }
        let section_title = self
            .home
            .selected_section()
            .and_then(|i| self.home.sections.get(i))
            .map(|s| s.title.clone())
            .unwrap_or_default();
        if self.home.browsing.is_none()
            && section_title == home::HomeState::CLOUD_SECTION
            && let Some(id) = self
                .home
                .selected_entry()
                .and_then(|e| home::cloud_source_id(&e.path))
        {
            self.cache.hide_cloud_source(&id);
            self.home.cloud.retain(|s| s.id != id);
            self.home_refresh();
            return;
        }
        let in_recents = section_title == "Recent";
        if !in_recents {
            self.home.status = Some("Only recents and remembered places can be forgotten".into());
            return;
        }
        let Some(entry) = self.home.selected_entry() else {
            return;
        };
        self.cache.forget_recent(&entry.path);
        // Nothing to say: the row going is the answer.
        self.home.status = None;
        self.home_refresh();
    }

    /// The directory the highlighted row stands for, as Ctrl+D sees it: a directory
    /// row is itself, a file is the directory it is in, and a heading is the directory
    /// its section lists.
    fn home_place_under_cursor(&self) -> Option<PathBuf> {
        match self.home.selected_row()? {
            home::Row::Place { path, .. } => Some(path),
            home::Row::Door { entry, .. } => Some(entry.path.clone()),
            home::Row::Entry { entry, .. } => match entry.kind {
                discover::EntryKind::File | discover::EntryKind::Other => {
                    entry.path.parent().map(Path::to_path_buf)
                }
                _ => Some(entry.path.clone()),
            },
            home::Row::Header { section, .. } => self.home.sections.get(section)?.root.clone(),
            home::Row::More { .. } | home::Row::Hidden { .. } => None,
        }
    }

    /// How a place is compared and stored: resolved when it is local, as spelled when
    /// it is remote, since resolving a path on a share that has stopped answering is
    /// the stat that hangs. The door's trailing slash goes either way.
    fn place_key(&self, path: &Path) -> PathBuf {
        if (self.home.network_check)(path) {
            path.components().collect()
        } else {
            canonical::canonicalize(path).unwrap_or_else(|_| path.components().collect())
        }
    }

    fn configured_place_note(path: &Path) -> String {
        format!(
            "{} is in [data] directories; edit the config to remove it",
            home::display_path(path)
        )
    }

    /// Ctrl+D: keep the place under the cursor on the home screen, or stop keeping it.
    fn home_toggle_remembered(&mut self) {
        let Some(path) = self.home_place_under_cursor() else {
            self.home.status = Some("Move to a directory to remember it".into());
            return;
        };
        // Roots are directories on a filesystem. A bucket already has its source's
        // row, and an HTTP place has nothing to list.
        if home::is_object_store_url(&path)
            || home::is_cloud_place(&path)
            || !matches!(source::input_source(&path), source::InputSource::Local(_))
        {
            self.home.status = Some("Only directories on a filesystem can be remembered".into());
            return;
        }
        let key = self.place_key(&path);
        let configured = self
            .app_config
            .data
            .resolved_directories()
            .iter()
            .any(|dir| self.place_key(dir) == key);
        if configured {
            self.home.status = Some(Self::configured_place_note(&key));
            return;
        }
        let remembered = self.cache.load_remembered_places().contains(&key);
        self.home_set_remembered(&key, !remembered);
    }

    fn home_set_remembered(&mut self, place: &Path, keep: bool) {
        if keep {
            self.cache.remember_place(place);
        } else {
            self.cache.forget_place(place);
        }
        let verb = if keep { "Remembered" } else { "Forgot" };
        self.flash_note(format!("{verb} {}", home::display_path(place)));
        self.home_refresh();
    }

    /// Collapse or expand the section the cursor is in.
    ///
    /// Collapsing moves the cursor to the header, so the section the user just folded
    /// is what stays selected rather than whatever row happens to fall into place.
    /// Fold or unfold the section whose header is highlighted.
    fn home_toggle_fold(&mut self) {
        if let Some(section) = self.home.selected_section() {
            self.home.toggle_collapsed(section);
            self.home.clamp_selection();
            self.cache.save_folds(&self.home.folds);
        }
    }

    fn home_collapse(&mut self, collapse: bool) {
        // The listing browsed into is the whole screen. It never folds, and the fold
        // must not be remembered for its path either — see `set_collapsed`.
        if self.home.browsing.is_some() {
            return;
        }
        let Some(section) = self.home.selected_section() else {
            return;
        };
        if collapse && !self.home.is_collapsed(section) {
            self.home.set_collapsed(section, true);
            if let Some(idx) = self
                .home
                .visible()
                .iter()
                .position(|row| row.section() == section)
            {
                self.home.selected = idx;
            }
        } else if !collapse {
            self.home.set_collapsed(section, false);
        }
        self.home.clamp_selection();
        self.cache.save_folds(&self.home.folds);
    }

    /// Step out of a directory that was descended into.
    fn home_ascend(&mut self) {
        let Some(current) = self.home.browsing.clone() else {
            return;
        };
        let parent = self.home.parent_of(&current);
        self.home_leave_browsing(parent);
    }

    /// Move the browse up to `to`, or back to the root listing when `None`.
    fn home_leave_browsing(&mut self, to: Option<PathBuf>) {
        // Whatever the last place said about itself, it said about that place. "these
        // are the files under it" is wrong the moment "it" is somewhere else.
        self.home.status = None;
        let from = std::mem::replace(&mut self.home.browsing, to);
        // Backspace can climb above where the browse began; the start follows, so a
        // later Esc still has a place to stop.
        if !self.home.below_browse_start() {
            self.home.browse_start = self.home.browsing.clone();
        }
        // The filter, search and row the user left here, if they were here; the
        // listing lands later, and the cursor goes back to that row when it does.
        // Somewhere new, a search of the old place no longer answers the question.
        self.home.come_back(from);
        self.home.sync_search_section();
        self.home.selected = 0;
        self.home_refresh();
        if !self.home.filter.is_empty() {
            self.spawn_home_search();
        }
    }

    /// Whether a peek's answer changes anything a row draws.
    ///
    /// Every answer that tells a row something, not only the ones that change the kind:
    /// a prefix of twelve CSV objects is a `Directory` — only Parquet is read in place —
    /// and it is still `12 csv`, which is the count the row is labelled from.
    ///
    /// An answer that says neither is replaced by "a directory, and nothing to say
    /// about it" rather than dropped. The directory still has to come back — that is what
    /// takes it out of `peeking` and holds the one-request-per-directory promise — and
    /// once the request has been made, "nothing to say" is a real answer rather than
    /// the claim it was when it was being written before the request. What this decides
    /// is whether the peek's own words are kept.
    ///
    /// "Says something" is `Holds::is_empty`, not the formats alone. The row is not the
    /// only thing an answer reaches: the details pane draws the whole `holds` line, so a
    /// prefix of a README and two PDFs has `3 not read` to report, and one of twelve
    /// sub-prefixes has `12 directories`. Testing the formats dropped both, and the same
    /// directories on disk said both things.
    ///
    /// The cost is real and is the reason the distinction is kept: each batch rebuilds
    /// the listing on the thread drawing the frame. `Holds::is_empty` is the line
    /// because it is the same question the pane asks before drawing the line at all.
    ///
    /// A `Holds` whose only field is `truncated` is let through too: it is what turns
    /// a cloud row's `dir` into `dir+`.
    #[cfg(feature = "cloud")]
    fn peek_tells_a_row_something(answer: &(discover::EntryKind, discover::Holds)) -> bool {
        answer.0 != discover::EntryKind::Directory || !answer.1.is_empty()
    }

    /// Look inside the cloud directories the cursor is on or near, so the ones that are
    /// datasets say `hive` or `multi` and open as one. One small listing request per
    /// directory, and each directory is peeked at once per session.
    ///
    /// A directory the listing takes for `multi` costs a little more: up to three ranged
    /// reads of a few kilobytes each, to ask the footers whether its files are really
    /// one table. Nothing else reads an object, and nothing reads a whole one.
    ///
    /// Driven by the cursor rather than by the listing. It used to take the first
    /// forty-eight directories of each listing, once: a bucket of two hundred prefixes
    /// had forty-eight labelled and the rest reading `dir` for the session however long
    /// you spent on them, and paging straight past those forty-eight spent the requests
    /// on rows nobody saw. The budget is the same shape as the local classify pass now —
    /// what is on screen, a batch at a time, the highlighted row first.
    #[cfg(feature = "cloud")]
    fn peek_cloud_directories(&mut self) {
        const PEEKS_AT_ONCE: usize = 4;
        let directories = self.home.cloud_directories_to_peek(PEEKS_AT_ONCE);
        if directories.is_empty() {
            return;
        }
        // Out, not answered. A second pass before these land must not ask again, and an
        // answer written here instead would be a claim — `dir` on a row that has a
        // count, and "never again this session" staked on a request that may fail.
        for directory in &directories {
            self.home.peeking.insert(directory.clone());
        }
        let tx = self.events.clone();
        let cloud = self.app_config.cloud.clone();
        self.runtime.spawn(async move {
            let permits = Arc::new(tokio::sync::Semaphore::new(PEEKS_AT_ONCE));
            let mut peeks = tokio::task::JoinSet::new();
            // By task, so a peek that panics is still sent back, as failed, and does
            // not stay in `peeking` spinning for good.
            let mut asked = std::collections::HashMap::new();
            for directory in directories {
                let (permits, cloud) = (permits.clone(), cloud.clone());
                let task_directory = directory.clone();
                let task = peeks.spawn(async move {
                    let directory = task_directory;
                    let _permit = permits.acquire_owned().await;
                    let kind =
                        crate::cloud_browse::peek_kind(&directory.to_string_lossy(), &cloud).await;
                    (directory, kind)
                });
                asked.insert(task.id(), directory);
            }
            // Sent a few at a time: the labels fill in as they are found, without a
            // rebuild per directory.
            //
            // Every directory asked about is sent back, including the ones whose peek
            // decided nothing and the ones whose request failed. That is what takes
            // them out of `peeking` and what holds the one-request-per-directory promise
            // — and an answer of "a directory, and nothing to say about it" is a real
            // answer once the request has been made, which is what it was not while it
            // was being written before the request.
            let mut found = Vec::new();
            let mut failed = Vec::new();
            while let Some(joined) = peeks.join_next_with_id().await {
                match joined {
                    Ok((_, (directory, Ok(answer)))) => {
                        let answer = Some(answer)
                            .filter(Self::peek_tells_a_row_something)
                            .unwrap_or((discover::EntryKind::Directory, Default::default()));
                        found.push((directory, answer));
                    }
                    Ok((_, (directory, Err(_)))) => failed.push(directory),
                    Err(error) => failed.extend(asked.remove(&error.id())),
                }
                if found.len() + failed.len() >= PEEKS_AT_ONCE {
                    let _ = tx.send(AppEvent::HomeCloudKinds {
                        kinds: std::mem::take(&mut found),
                        failed: std::mem::take(&mut failed),
                    });
                }
            }
            if !found.is_empty() || !failed.is_empty() {
                let _ = tx.send(AppEvent::HomeCloudKinds {
                    kinds: found,
                    failed,
                });
            }
        });
    }

    /// Browse into a directory or bucket, local or remote.
    fn home_browse_into(&mut self, path: PathBuf) {
        self.home.leave_mark();
        if self.home.browsing.is_none() {
            self.home.browse_start = Some(path.clone());
        } else if self.home.browse_start.is_none() {
            self.home.browse_start = self.home.browsing.clone();
        }
        self.home.browsing = Some(path);
        // "Below here" now means somewhere else. Whatever the last walk found
        // describes a different place, and a fresh one starts on the next
        // keystroke. The status line goes with them: "these are the files under it"
        // is about wherever "it" was. A caller with something to say about the place
        // it is going says it after this returns.
        self.home.status = None;
        self.home.search.reset();
        self.home.filter.clear();
        self.home.sync_search_section();
        self.home.selected = 0;
        self.home_refresh();
    }

    /// Whether the highlighted row is the `(all files)` row: the one that opens the
    /// directory being browsed, and so is already inside it.
    ///
    /// `Enter` on it opens the directory whatever the label says, and → on it would
    /// descend into where it already is.
    /// Asked of the row's variant rather than of a flag on the entry it carries: the
    /// door is a `Row::Door` now, so this is one match instead of a clone.
    fn selection_opens_the_whole_directory(&self) -> bool {
        self.home.selection_is_the_door()
    }

    /// The highlighted row, when → goes inside it.
    ///
    /// Every directory, whatever its label. A label describes what is directly inside; it
    /// no longer decides what can be reached, so the exception list this used to carry —
    /// hive, multi and the three lake markers — is gone, and with it the directories that
    /// had no way in because datui did not recognize how they were stored. What is left
    /// out is what is not a directory: a file, a section header, and the row that opens
    /// the directory you are already in.
    ///
    /// Local or remote. The split this used to carry — remote only — was never about
    /// where the directory was: a cloud prefix simply could not be descended into until
    /// there was a listing to descend with.
    fn selected_directory_to_enter(&self) -> Option<PathBuf> {
        // A place under `RECENT` is a directory to go inside, and → is one of its two
        // doors. It has no entry to ask about, so it is answered before one is looked for.
        if let Some(home::Row::Place { path, .. }) = self.home.selected_row() {
            return home::place_is_browsable(&path).then_some(path);
        }
        let entry = self.home.selected_entry()?;
        if self.selection_opens_the_whole_directory() || self.home.missing.contains(&entry.path) {
            return None;
        }
        // A SQLite database lists its tables, however many it has.
        if entry.cost.tables.is_some() {
            return Some(entry.path);
        }
        (!matches!(
            entry.kind,
            discover::EntryKind::File | discover::EntryKind::Other
        ))
        .then_some(entry.path)
    }

    /// Why a prefix in an object store cannot be read as one table, when it cannot.
    ///
    /// Every cloud path is scanned as Parquet — the directory-format dispatch is local
    /// only — so a prefix of anything else comes back "Could not read from S3. Check
    /// credentials and URL", which is a false statement about a login that is fine.
    /// What the prefix holds is already counted and on screen, so saying so costs no
    /// request. #275 phase 4 is where these read.
    ///
    /// `None` for a prefix that may yet be Parquet: one holding Parquet, and one
    /// holding no data files at all, whose data may be a level down.
    /// Why Enter on a bucket directory's `(all files)` row reads nothing, by the rule
    /// Enter itself applies: a hive root or a directory of one table is read through
    /// its files, and one with a reader for what it holds is read with that.
    #[cfg(feature = "cloud")]
    pub(crate) fn why_a_door_reads_nothing(entry: &discover::Entry) -> Option<String> {
        if !home::is_object_store_url(&entry.path)
            || matches!(
                entry.kind,
                discover::EntryKind::Hive | discover::EntryKind::MultiFile
            )
            || Self::cloud_prefix_format(&entry.holds).is_some()
        {
            return None;
        }
        Self::why_a_cloud_prefix_cannot_be_read(&entry.holds)
    }

    #[cfg(feature = "cloud")]
    fn why_a_cloud_prefix_cannot_be_read(holds: &discover::Holds) -> Option<String> {
        let reads_parquet =
            |name: &str| crate::FileFormat::from_name(name) == Some(crate::FileFormat::Parquet);
        if holds.formats.iter().any(|(name, _)| reads_parquet(name)) {
            return None;
        }
        match holds.formats.as_slice() {
            // Data files, none of them Parquet. `label()` says `mixed` for more than
            // one format, which is a word rather than a count, so the line is spelled
            // out from the formats themselves.
            [] => {
                // Nothing datui has a reader for. Only a refusal when there is also
                // nothing below: a prefix of sub-prefixes may hold Parquet a level
                // down, and nothing here has looked.
                (holds.not_read > 0 && holds.directories == 0).then(|| {
                    "this prefix holds nothing datui can read — datui reads a directory in \
                     an object store as Parquet only."
                        .to_string()
                })
            }
            formats => {
                let held = formats
                    .iter()
                    .map(|(name, count)| format!("{count} {name}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                Some(format!(
                    "this prefix holds {held} — datui reads a directory in an object store \
                     as Parquet only. Open one of the files below instead."
                ))
            }
        }
    }

    /// The reader a prefix in an object store calls for, from what its listing counted.
    ///
    /// The commonest format, which is the same rule a directory on disk follows — and
    /// `rank_formats` is the same order, so a prefix and the directory it mirrors pick
    /// the same reader. `None` when nothing there has a multi-file reader, which is where
    /// the refusal that names what is there belongs.
    ///
    /// Parquet included and returned as itself: the cloud branches compare against it
    /// and take their own path, which is the one every cloud dataset took before any of
    /// this, and the only one with hive partitioning behind it.
    #[cfg(feature = "cloud")]
    fn cloud_prefix_format(
        holds: &discover::Holds,
    ) -> Option<(FileFormat, Vec<(FileFormat, usize)>)> {
        // A saved DatasetDict: its splits are Arrow, read one at a time.
        if holds.dataset_dict {
            return Some((FileFormat::Arrow, Vec::new()));
        }
        // A model's weights beside its config and tokenizer JSON: the prefix is the
        // model, as a directory on disk is, and the JSON is not data passed over.
        if let Some((name, _)) = holds.model_weights() {
            return FileFormat::from_name(name).map(|format| (format, Vec::new()));
        }
        let (name, _) = holds.formats.first()?;
        // A GPS log is read whole from disk; a bucket's logs are opened one at a time.
        let format =
            FileFormat::from_name(name).filter(|f| f.reads_many_files() && !f.reads_into())?;
        // And what taking the commonest passes over. The local read reports its own —
        // it is the pass that decides — but here Polars does the listing and never sees
        // the other formats, so the note has to be written from the listing on screen.
        let left_out = holds
            .formats
            .iter()
            .skip(1)
            .filter_map(|(name, n)| FileFormat::from_name(name).map(|f| (f, *n)))
            .collect();
        Some((format, left_out))
    }

    /// Open the highlighted entry: toggle a section, descend into a directory, or
    /// load a dataset.
    fn home_open_selected(&mut self) -> Option<AppEvent> {
        match self.home.selected_row() {
            // Into the directory or prefix the recents under it live in: the way back
            // to a place found by hand, now that recents no longer make roots.
            Some(home::Row::Place { path, .. }) => {
                if home::place_is_browsable(&path) {
                    self.home_browse_into(path);
                } else {
                    self.home.status = Some(
                        "An HTTP server has no listing to browse. Open a file under it".into(),
                    );
                }
                return None;
            }
            // The rest of `RECENT`, for the session.
            Some(home::Row::More { .. }) => {
                self.home.recent_expanded = true;
                return None;
            }
            // What Ctrl+A shows. The cursor goes to the first of them, where the row
            // that stood for them was.
            Some(home::Row::Hidden { .. }) => {
                self.home.hide_unreadable = false;
                if let Some(idx) = self.home.visible().iter().position(|row| {
                    matches!(row, home::Row::Entry { entry, .. }
                        if entry.hidden_by_default())
                }) {
                    self.home.selected = idx;
                }
                return None;
            }
            _ => {}
        }
        if self.home.selection_is_header() {
            self.home_toggle_fold();
            return None;
        }
        let entry = self.home.selected_entry()?;
        // A collection's local dataset that is not there: said here, where it was named.
        if self.home.missing.contains(&entry.path) {
            self.home.status = Some(format!(
                "{} does not exist",
                home::display_path(&entry.path)
            ));
            return None;
        }
        // The `(all files)` row opens the directory it names, whatever the directory is
        // labelled. That is the whole of what it is for: the label describes, and this
        // row is the promise that the description cannot lock you out. Sent straight to
        // the open, because `open_what_it_is` would read the label back and send a
        // `dir` row inside the directory it is already in.
        if self.selection_opens_the_whole_directory() {
            // A lake table is not a directory of Parquet files however much it looks like
            // one: reading it as one counts tombstoned rows, every rewritten version
            // and both sides of a compaction. So the read is labelled rather than
            // refused. Refusing it left a directory the user could see and could not read
            // at all — this row is the promise that no label locks you out, and a
            // refusal here is that promise broken on the one directory that needed it.
            // Until datui reads the log, its files are what there is, and what makes
            // that honest is that nothing about it is silent: a note in the panel, a
            // chip in the control bar, and `Enter` on the row one level up still goes
            // inside and says datui does not read the table itself yet.
            let lake = entry.kind.lake_name();
            // A prefix in an object store used to be scanned as Parquet whatever was
            // in it — every cloud path returns before the directory-format dispatch is
            // reached — so a prefix of CSV answered "Could not read from S3. Check
            // credentials and URL", a false statement about the user's login. What the
            // prefix holds was counted by the listing and is on screen, so the reader
            // is picked from it, which costs no request. Only a prefix the listing
            // already calls a dataset is left alone: a hive root is read through its
            // partitions, and one stray `manifest.csv` beside them is not what it
            // holds — but it is the only thing in `formats`.
            #[cfg(feature = "cloud")]
            let reader = if home::is_object_store_url(&entry.path)
                && !matches!(
                    entry.kind,
                    discover::EntryKind::Hive | discover::EntryKind::MultiFile
                ) {
                let reader = Self::cloud_prefix_format(&entry.holds);
                // Nothing here datui has a reader for. The listing is on screen, so the
                // refusal names what is there rather than blaming the connection.
                if reader.is_none()
                    && let Some(what) = Self::why_a_cloud_prefix_cannot_be_read(&entry.holds)
                {
                    self.home.status = Some(what);
                    return None;
                }
                reader
            } else {
                None
            };
            #[cfg(not(feature = "cloud"))]
            let reader = None;
            // `hive: true` says read this as one, which is the whole of what the row
            // promises — it is also what carries partition columns through, for a
            // directory the dispatch sends down the hive route. The cloud route returns
            // before the dispatch is reached.
            let directory = home::directory_dataset_url(&entry.path);
            return Some(self.home_open_directory_as(directory, true, lake, reader));
        }
        // A row nothing has looked at is looked at before it is opened, rather than
        // opened as whatever it turns out to be. `EntryKind::Unknown` is offered as
        // openable, so without this a lake root reached this way is read as one table:
        // #237 through the door #249 leaves open.
        let mut entry = entry;
        if entry.kind == discover::EntryKind::Unknown {
            if self.looking_could_block(&entry.path) {
                return Some(AppEvent::ClassifyThenOpen {
                    path: entry.path,
                    jump: false,
                });
            }
            if entry.path.is_dir() {
                entry.kind = discover::classify_directory(&entry.path);
            }
        }
        // A database of several tables lists them rather than opening; one not yet
        // measured is opened, and the open lands on its tables the same way.
        if entry.enter_lists_tables() {
            self.home_browse_into(entry.path);
            return None;
        }
        self.open_what_it_is(entry.path, entry.kind, false)
    }

    /// Whether finding out what a path is could sit on a mount that never answers.
    ///
    /// Two halves. An object-store or HTTP URL names something no mount is responsible
    /// for — what is behind it is the scan's business, and stat'ing it only ever asks the
    /// working directory about a file called `s3:` — and an ordinary local path answers at
    /// once, so making the user wait a round trip for it would be a delay bought with
    /// nothing.
    ///
    /// What is left is a path on a mount the home screen calls a network one, which is
    /// the case `is_remote_path` exists to name and the only one worth a worker.
    fn looking_could_block(&self, path: &Path) -> bool {
        // `cloud://<id>` is a place, not a path: `input_source` calls the unknown scheme
        // local and `is_remote_path` calls it remote, so without this a worker would be
        // sent to stat it and come back with "No such path".
        !home::is_cloud_place(path)
            && matches!(source::input_source(path), source::InputSource::Local(_))
            && (self.home.network_check)(path)
    }

    /// Do with a path whatever its kind calls for: browse into it, say it is a lake
    /// table, or open it.
    ///
    /// `jump` is a path typed at `~` rather than a row already listed, which starts a new
    /// browse so Esc comes back from there to the listing.
    fn open_what_it_is(
        &mut self,
        path: PathBuf,
        kind: discover::EntryKind,
        jump: bool,
    ) -> Option<AppEvent> {
        let go_inside = |app: &mut Self, path: PathBuf| {
            if jump {
                app.home_jump_into(path);
            } else {
                app.home_browse_into(path);
            }
        };
        if kind == discover::EntryKind::Directory {
            go_inside(self, path);
            return None;
        }
        // No reader: a local file's bytes, in the hex view. A remote one is dimmed and
        // its details pane says why.
        if kind == discover::EntryKind::Other {
            if matches!(source::input_source(&path), source::InputSource::Local(_)) {
                self.open_hex(path, crate::hex_view::Origin::Home, true, None);
            }
            return None;
        }
        // A lake table's files are not its rows: the ones a delete or an update
        // tombstoned are still on disk, every rewritten version is here together, and
        // compaction leaves both sides in place. Going inside is what datui can honestly
        // do with one, and saying so is better than a silent wrong answer.
        if let Some(format) = kind.lake_name() {
            self.home.lake_here = Some((path.clone(), format));
            go_inside(self, path);
            return None;
        }
        let directory = matches!(
            kind,
            discover::EntryKind::Hive | discover::EntryKind::MultiFile
        );
        // A directory typed at `~` is a place to go, as → makes it, whatever it holds:
        // its door is one row in, and naming a directory never starts a read of all of it.
        if directory && jump {
            go_inside(self, path);
            return None;
        }
        // A cloud directory that is a dataset opens as one: its URL as a prefix, which is
        // what makes the open a scan of every file under it.
        if directory && home::is_object_store_url(&path) {
            // A prefix, not a directory: the scan is what walks it.
            return Some(self.home_open_path(home::directory_dataset_url(&path), false));
        }
        // Said here, where the file was named, rather than after a download and a load
        // that could only end the same way. A row would be dimmed; a typed path has no
        // row, so the line says it.
        // A format spec may read it: by its glob, or by magic the open looks for.
        let a_spec_may_read = !self.formats.by_glob(&path, false).is_empty()
            || self.formats.specs.iter().any(|f| !f.spec.magic.is_empty());
        // A table inside a file of tables (`flight.ulg/sensor_accel.1`) has the file's
        // name in front, and a log found by its first bytes (`00000042.BIN`) a name
        // that says nothing.
        if kind == discover::EntryKind::File
            && discover::unreadable_by_name(&path)
            && !a_spec_may_read
            && crate::members::split(&path).is_none()
            && crate::members::holder(&path).is_none()
            && crate::members::split_variant(&path, &self.formats).is_none()
        {
            self.home.status = Some(discover::NO_READER.to_string());
            return None;
        }
        // The preview read this file's first page through the open's own steps: the
        // open installs that dataset rather than reading it again.
        let prepared = (!directory)
            .then(|| self.home_previews.take_prepared(&path))
            .flatten();
        // A small file of the built-in catalog is fetched without a question: the row
        // already said what it is and what it weighs. A URL the user typed still asks.
        let unasked = self
            .home
            .collection_dataset(&path)
            .filter(|(collection, _)| {
                !jump
                    && collection.builtin
                    && matches!(source::input_source(&path), source::InputSource::Http(_))
            })
            .map(|(_, dataset)| UnaskedDownload {
                limit: UnaskedDownload::LIMIT,
                listed: dataset.size,
            });
        match self.home_open_path(path, directory) {
            AppEvent::Open(paths, mut options) => {
                options.prepared = prepared.map(|p| Arc::new(Mutex::new(Some(p))));
                options.download_unasked = unasked;
                Some(AppEvent::Open(paths, options))
            }
            event => Some(event),
        }
    }

    /// What `datui <path>` does with a directory: the same rule as `Enter` on its row.
    ///
    /// A directory used to be `Unsupported file type` unless `--hive` was passed, while
    /// pyarrow, Polars, pandas and Spark all open one. Naming a directory *is* the
    /// request to read it, so the three doors onto a path — the highlighted row, the `~`
    /// prompt and the command line — now answer the same: a hive root or a directory
    /// whose files are one table opens as one table, and a directory that is a place to
    /// look inside opens the home screen browsed into it, one keystroke from either file
    /// or union.
    ///
    /// The directory is looked into rather than guessed at, because that is what the
    /// rule is: [`home::look_into`] is the same call the home screen's background pass
    /// makes, footers and all.
    ///
    /// `--hive` is untouched. It names a glob or forces partition columns, and it is
    /// still the only way to say "read this as partitioned" about something whose
    /// layout does not say so itself.
    ///
    /// Asks the filesystem whether a local path is a directory, so `run` calls it on a
    /// worker ([`AppEvent::OpenNamed`]). Returns the event that carries the open on:
    /// `LookThenOpenDirectory` or `Open`.
    pub fn route_named_paths(paths: Vec<PathBuf>, options: OpenOptions) -> AppEvent {
        Self::route_named_paths_with(paths, options, &crate::formats::Registry::default())
    }

    /// [`Self::route_named_paths`], with the format specs on the search path: a
    /// directory a spec reads as column files is opened, not looked at.
    pub fn route_named_paths_with(
        paths: Vec<PathBuf>,
        options: OpenOptions,
        formats: &crate::formats::Registry,
    ) -> AppEvent {
        if let Some(event) = Self::route_named_without_looking(&paths, &options) {
            return event;
        }
        // Several paths are a list of files to read together, and `--hive` is an answer
        // already given. Neither is a question about what one directory is.
        let single = (paths.len() == 1 && !options.hive).then(|| paths[0].clone());
        let Some(dir) = single.filter(|p| p.is_dir()) else {
            return AppEvent::Open(paths, options);
        };
        // A format spec named for it, or one whose glob names it, reads it as columns.
        if options.spec_file.is_some()
            || options.spec_name.is_some()
            || !formats.by_glob(&dir, true).is_empty()
        {
            return AppEvent::Open(paths, options);
        }
        // Looking at a directory reads its footers, or the front of a spread of its
        // files. For a directory of large Parquet that is seconds — 4.6 of them on a real
        // one — so it goes to a worker, and the answer comes back as an event like every
        // other read.
        AppEvent::LookThenOpenDirectory(dir, options)
    }

    /// The part of [`Self::route_named_paths`] that needs no filesystem: a cloud
    /// directory is looked at too, by one page of its listing — what is in it picks the
    /// reader, as it does for the `(all files)` row. Scanned blind, it was read as
    /// Parquet whatever it held. A glob, a file name or `--format` already says what to
    /// read.
    fn route_named_without_looking(paths: &[PathBuf], options: &OpenOptions) -> Option<AppEvent> {
        #[cfg(feature = "cloud")]
        if let [dir] = paths
            && !options.hive
            && home::is_object_store_url(dir)
            && options.format.is_none()
            && !dir.to_string_lossy().contains('*')
            && !home::names_a_file(dir)
        {
            return Some(AppEvent::LookThenOpenDirectory(
                dir.clone(),
                options.clone(),
            ));
        }
        let _ = (paths, options);
        None
    }

    /// The first named local path that is not there. A URL or a glob is left to the
    /// open, which says what it found, and standard input is no path.
    pub fn missing_named_path(
        paths: &[PathBuf],
        formats: &crate::formats::Registry,
    ) -> Option<PathBuf> {
        paths
            .iter()
            .find(|path| {
                !source::is_remote_url(path)
                    && !crate::stdin::is_stdin(path)
                    && !source::expands_as_glob(path)
                    && !path.exists()
                    && crate::members::split(path).is_none()
                    && crate::members::split_variant(path, formats).is_none()
            })
            .cloned()
    }

    /// Act on what the look at a directory named on the command line found.
    ///
    /// The other half of [`Self::route_named_paths`], which is
    /// where the reasoning for the rule itself is.
    fn open_the_directory_looked_at(
        &mut self,
        dir: PathBuf,
        kind: discover::EntryKind,
        holds: Option<&discover::Holds>,
        mut options: OpenOptions,
    ) -> Option<AppEvent> {
        #[cfg(feature = "cloud")]
        if home::is_object_store_url(&dir) {
            return self.open_the_cloud_directory_looked_at(dir, kind, holds, options);
        }
        let _ = holds;
        // No override for the user's reader settings here, and none needed: the look
        // read every file the way this open will, so `--no-header` and the skips have
        // already been accounted for by the rule rather than around it. Overriding
        // instead took three goes to get wrong in three different ways — it fired on
        // config values, it fired on directories with nothing readable in them, and it
        // fired on Parquet, which no CSV setting can affect.
        //
        // A lake table's files are not its rows, so the home screen is opened on it and
        // says why — the same sentence the row gives, because it is the same refusal.
        if let Some(format) = kind.lake_name() {
            self.enter_home();
            self.home.lake_here = Some((dir.clone(), format));
            self.home_jump_into(dir);
            return None;
        }
        // One table: read it. `hive` is what puts the open on the directory route, where
        // what the directory holds picks the reader.
        if matches!(
            kind,
            discover::EntryKind::Hive | discover::EntryKind::MultiFile
        ) {
            options.hive = true;
            self.set_loading_phase("Scanning input", 10);
            self.name_what_is_loading(dir.clone());
            return Some(AppEvent::Open(vec![dir], options));
        }
        // A place to look inside. `datui .` is this, and so is a directory of separate
        // tables — where the `(all files)` row inside is the one keystroke that unions
        // them anyway.
        self.enter_home();
        self.home_jump_into(dir);
        None
    }

    /// As [`Self::open_the_directory_looked_at`], for a cloud directory: what `Enter`
    /// on its `(all files)` row does, or a browse into it when there is no data
    /// directly inside to read.
    #[cfg(feature = "cloud")]
    fn open_the_cloud_directory_looked_at(
        &mut self,
        dir: PathBuf,
        kind: discover::EntryKind,
        holds: Option<&discover::Holds>,
        options: OpenOptions,
    ) -> Option<AppEvent> {
        let open = |app: &mut Self, path: PathBuf, options: OpenOptions| {
            app.set_loading_phase("Scanning input", 10);
            app.name_what_is_loading(path.clone());
            Some(AppEvent::Open(vec![path], options))
        };
        // The listing was refused. The open says why, in the words of whatever
        // refused it, which is what happened before anything looked.
        let Some(holds) = holds else {
            return open(self, dir, options);
        };
        if let Some(format) = kind.lake_name() {
            self.enter_home();
            self.home.lake_here = Some((dir.clone(), format));
            self.home_jump_into(dir);
            return None;
        }
        let directory = home::directory_dataset_url(&dir);
        if matches!(
            kind,
            discover::EntryKind::Hive | discover::EntryKind::MultiFile
        ) {
            let options = OpenOptions {
                hive: true,
                ..options
            };
            return open(self, directory, options);
        }
        if let Some((format, left_out)) = Self::cloud_prefix_format(holds) {
            let options = OpenOptions {
                hive: true,
                format: Some(format),
                left_out,
                ..options
            };
            return open(self, directory, options);
        }
        // Only directories, or nothing datui reads: somewhere to look inside, with
        // the reason when there is one.
        self.enter_home();
        self.home_jump_into(dir);
        self.home.status = Self::why_a_cloud_prefix_cannot_be_read(holds);
        None
    }

    /// Browse into `path` as a jump, from wherever the user was.
    ///
    /// Unlike `home_browse_into`, the browse *starts* here: Esc comes back from here to
    /// the listing rather than up through whatever the path happens to sit under.
    fn home_jump_into(&mut self, path: PathBuf) {
        // A new browse: Esc comes back from here to the listing, so only the listing's
        // mark is still a way back.
        self.home.trail.retain(|mark| mark.place.is_none());
        if self.home.browsing.is_none() {
            self.home.leave_mark();
        }
        self.home.browse_start = Some(path.clone());
        self.home.browsing = Some(path);
        self.home.status = None;
        self.home.search.reset();
        self.home.filter.clear();
        self.home.sync_search_section();
        self.home.selected = 0;
        self.home_refresh();
    }

    /// Load a path from the home screen.
    ///
    /// The recent entry is recorded by the `Open` handler, which every open goes
    /// through, so this does not record one itself.
    fn home_open_path(&mut self, path: PathBuf, hive: bool) -> AppEvent {
        self.home_open_directory(path, hive, None)
    }

    /// As [`Self::home_open_path`], and carrying whether the directory being read is a
    /// lake table whose plain files this read is, so the dataset can say so.
    fn home_open_directory(
        &mut self,
        path: PathBuf,
        hive: bool,
        lake: Option<&'static str>,
    ) -> AppEvent {
        self.home_open_directory_as(path, hive, lake, None)
    }

    /// As [`Self::home_open_directory`], naming the reader to use.
    ///
    /// For a prefix in an object store, where nothing downstream reads the listing: the
    /// cloud branches scan before the directory-format dispatch is reached, so the format
    /// the listing counted has to travel with the open or the scan falls back to
    /// Parquet, which is what it always did.
    fn home_open_directory_as(
        &mut self,
        path: PathBuf,
        hive: bool,
        lake: Option<&'static str>,
        reader: Option<(FileFormat, Vec<(FileFormat, usize)>)>,
    ) -> AppEvent {
        let (format, left_out) = match reader {
            Some((format, left_out)) => (Some(format), left_out),
            None => (None, Vec::new()),
        };
        // A directory of partitions is only meaningful read as one hive dataset. Told
        // rather than stat'ed: the caller already knows what this is, and on a share that
        // has gone away a `stat` here would freeze the thread reading the keys — the same
        // reason the size below is left to the `Open` handler.
        let options = OpenOptions {
            hive,
            read_as_plain_files_of: lake,
            format,
            left_out,
            ..OpenOptions::default()
        };
        self.input_mode = InputMode::Normal;
        // Chosen here, so a failure is reported here.
        self.announce_open(true, "Scanning input".to_string(), 10);
        // A frame is drawn between this keypress and the `Open` that carries it out,
        // and it is the one the user is looking at when they press Enter — so it says
        // which file, not just that something is happening. `Open` fills in the size a
        // frame later; stat'ing here would put a possibly-dead mount on this thread.
        self.name_what_is_loading(path.clone());
        AppEvent::Open(vec![path], options)
    }

    /// Key handling for the home screen.
    fn home_key(&mut self, event: &KeyEvent) -> Option<AppEvent> {
        let ctrl = event.modifiers.contains(KeyModifiers::CONTROL);
        // The line beside the prompt answers the last key, and this one replaces it: a
        // key with something to say sets it again below. Left up, "Forgot laps.parquet"
        // stayed until the next time the listing changed.
        self.home.status = None;

        // The home screen puts every plain character into the filter — `q` has to
        // type a `q`, or you could never search for "quarterly". Quitting is Ctrl+C,
        // handled before this is reached, and Esc once there is no context left to
        // back out of.
        if self.home.path_input_active {
            match event.code {
                KeyCode::Esc => {
                    self.home.path_input_active = false;
                    self.home.path_input.clear();
                    self.home.path_listing = None;
                    self.home.path_pick = None;
                    self.home.status = None;
                }
                // The list under the prompt is the directory being typed: ↑↓ pick a
                // name in it, which Enter and Tab then take.
                KeyCode::Up | KeyCode::Down => {
                    let n = self.home.path_candidates().len();
                    self.home.path_pick = match (event.code, self.home.path_pick) {
                        _ if n == 0 => None,
                        (KeyCode::Down, None) => Some(0),
                        (KeyCode::Down, Some(i)) => Some((i + 1).min(n - 1)),
                        (KeyCode::Up, Some(0)) | (KeyCode::Up, None) => None,
                        (KeyCode::Up, Some(i)) => Some(i - 1),
                        (_, pick) => pick,
                    };
                }
                KeyCode::Enter => {
                    if let Some(picked) = self.home.picked_path() {
                        self.home.path_input = picked;
                        self.home.path_pick = None;
                    }
                    let raw = self.home.path_input.trim().to_string();
                    if raw.is_empty() {
                        self.home.path_input_active = false;
                        return None;
                    }
                    let path = home::expand_user_path(&raw);
                    // A URL is not stat'ed: `exists` asks the working directory about a
                    // file called `gs:`. Its name decides, as it does for a recent — a
                    // file opens, and anything else in a bucket is browsed, where the
                    // listing says what is there.
                    if home::is_object_store_url(&path) || home::is_cloud_place(&path) {
                        self.home.path_input.clear();
                        self.home.path_input_active = false;
                        let kind = if home::names_a_file(&path) {
                            discover::EntryKind::File
                        } else {
                            discover::EntryKind::Directory
                        };
                        return self.open_what_it_is(path, kind, true);
                    }
                    // And an HTTP URL is one file.
                    if !matches!(source::input_source(&path), source::InputSource::Local(_)) {
                        self.home.path_input.clear();
                        self.home.path_input_active = false;
                        return self.open_what_it_is(path, discover::EntryKind::File, true);
                    }
                    // Whether it is there, whether it is a directory and what kind of one
                    // are three filesystem calls, and a typed path is exactly where a
                    // dead mount gets named. All three go to a worker when the mount is
                    // one that might not answer.
                    if self.looking_could_block(&path) {
                        self.home.path_input.clear();
                        self.home.path_input_active = false;
                        return Some(AppEvent::ClassifyThenOpen { path, jump: true });
                    }
                    // Before the prompt closes: a typo is worth fixing where it was
                    // typed, rather than retyping the whole path.
                    if !path.exists()
                        && crate::members::split(&path).is_none()
                        && crate::members::split_variant(&path, &self.formats).is_none()
                    {
                        self.home.status = Some(format!("No such path: {}", path.display()));
                        return None;
                    }
                    self.home.path_input.clear();
                    self.home.path_input_active = false;
                    let kind = if path.is_dir() {
                        discover::classify_directory(&path)
                    } else {
                        discover::EntryKind::File
                    };
                    return self.open_what_it_is(path, kind, true);
                }
                KeyCode::Backspace => {
                    self.home.path_input.pop();
                    self.home.status = None;
                }
                KeyCode::Char('u') if ctrl => self.home.path_input.clear(),
                // The picked name, or what the names listed agree on. Before the
                // listing is in, completion reads the directory on a worker.
                KeyCode::Tab => {
                    let completed = self
                        .home
                        .picked_path()
                        .or_else(|| self.home.path_completion());
                    let listed = self
                        .home
                        .path_listing
                        .as_ref()
                        .is_some_and(|l| l.dir == home::typed_dir(&self.home.path_input));
                    match completed {
                        Some(completed) => self.home.path_input = completed,
                        None if !listed => self.request_path_completion(),
                        None => {}
                    }
                }
                KeyCode::Char(c) if !ctrl => {
                    self.home.path_input.push(c);
                    self.home.status = None;
                }
                _ => {}
            }
            // Whatever changed what is typed takes the pick away, and a new directory
            // is listed.
            if !matches!(event.code, KeyCode::Up | KeyCode::Down) {
                self.home.path_pick = None;
            }
            self.list_the_typed_directory();
            return None;
        }

        // Every plain character types into the filter, so no letter or bracket is
        // a key here: typing "json" must not move the cursor on the "j". Navigation
        // is the arrows and the Ctrl chords, which cannot be part of a name.
        match event.code {
            KeyCode::Esc => return self.home_escape(),
            KeyCode::Enter => return self.home_open_selected(),
            // Section to section, past however many rows the current one holds.
            KeyCode::Down if ctrl => self.home.jump_section(1),
            KeyCode::Up if ctrl => self.home.jump_section(-1),
            KeyCode::Up => self.home.move_selection(-1),
            KeyCode::Down => self.home.move_selection(1),
            KeyCode::Char('n') if ctrl => self.home.move_selection(1),
            KeyCode::Char('p') if ctrl => self.home.move_selection(-1),
            // Left/right fold the section the cursor is in, wherever in it the cursor
            // happens to be — so collapsing does not require first finding the header.
            // Tab cycles the sort. Every plain key goes into the filter, so an
            // ordinary letter is not available for this.
            KeyCode::Tab => {
                self.home.sort = self.home.sort.next();
                self.home.select_first_entry();
            }
            KeyCode::Left => self.home_collapse(true),
            KeyCode::Right => match self.selected_directory_to_enter() {
                // Into a directory that opens as one dataset rather than opening it, to
                // reach one partition or one file. This clears the filter, as browsing
                // anywhere does.
                Some(directory) => {
                    // The heading Enter leaves, for the same reason: this is the door
                    // the control bar advertises on a lake row, and arriving inside one
                    // with no explanation is the silent wrong answer #237 is about.
                    if let Some(format) = self
                        .home
                        .selected_entry()
                        .and_then(|entry| entry.kind.lake_name())
                    {
                        self.home.lake_here = Some((directory.clone(), format));
                    }
                    self.home_browse_into(directory);
                }
                None => self.home_collapse(false),
            },
            // A screenful, matching the table; the renderer keeps view_height current.
            KeyCode::PageUp => {
                let page = self.home.view_height.max(1) as isize;
                self.home.page_selection(-page);
            }
            KeyCode::PageDown => {
                let page = self.home.view_height.max(1) as isize;
                self.home.page_selection(page);
            }
            KeyCode::Home => self.home.page_selection(isize::MIN),
            KeyCode::End => self.home.page_selection(isize::MAX),
            KeyCode::Char('u') if ctrl => {
                self.home.filter.clear();
                self.home.sync_search_section();
                self.home.select_first_entry();
            }
            KeyCode::Char('r') if ctrl => self.home_reload(),
            // A browser's bookmark key: keep this place on the home screen, or stop.
            KeyCode::Char('d') if ctrl => self.home_toggle_remembered(),
            // Any local file's bytes, whatever datui would read it as.
            KeyCode::Char('x') if ctrl => {
                let local = |path: &Path| {
                    matches!(source::input_source(path), source::InputSource::Local(_))
                };
                match self.home.selected_entry() {
                    Some(entry)
                        if !self.home.selection_is_the_door()
                            && entry.table.is_none()
                            && matches!(
                                entry.kind,
                                discover::EntryKind::File
                                    | discover::EntryKind::Other
                                    | discover::EntryKind::Unknown
                            )
                            && local(&entry.path) =>
                    {
                        self.open_hex(entry.path, crate::hex_view::Origin::Home, false, None);
                    }
                    _ => self.flash_note("Ctrl+X shows a local file's bytes".to_string()),
                }
            }
            KeyCode::Char('a') if ctrl => {
                let on = self.home.selected_key();
                self.home.hide_unreadable = !self.home.hide_unreadable;
                // Inside a database, what is hidden is its own tables.
                let tables = self
                    .home
                    .sections
                    .iter()
                    .any(|section| section.rows.iter().any(|row| row.table.is_some()));
                self.flash_note(
                    match (self.home.hide_unreadable, tables) {
                        (true, true) => "Hiding internal tables",
                        (false, true) => "Showing internal tables",
                        (true, false) => "Hiding files with no reader",
                        (false, false) => "Showing files with no reader",
                    }
                    .to_string(),
                );
                // The same row where it is still there; the cursor stays put otherwise.
                self.home.reselect(on);
            }
            KeyCode::Backspace => {
                if self.home.filter.is_empty() {
                    self.home_ascend();
                } else {
                    self.home.filter.pop();
                    self.home.sync_search_section();
                    self.home.select_first_entry();
                }
            }
            // Forget the highlighted entry. Only meaningful in Recent — elsewhere the
            // row is a real directory listing, and datui does not delete files.
            // Shift+Delete forgets the lot. It sits next to the key that forgets
            // one, so it asks first — an accidental press should not silently throw
            // away every place the user has been.
            KeyCode::Delete if event.modifiers.contains(KeyModifiers::SHIFT) => {
                let count = self.cache.load_recents().len();
                if count == 0 {
                    self.home.status = Some("Nothing to forget".into());
                } else {
                    self.pending_clear_recents = true;
                    self.confirmation_modal
                        .show(format!("Forget all {count} recently opened datasets?"));
                }
            }
            KeyCode::Delete => self.home_forget_selected(),
            KeyCode::Char('~') if self.home.filter.is_empty() => {
                self.home.path_input_active = true;
                self.home.status = None;
                self.home.path_listing = None;
                self.home.path_pick = None;
                self.list_the_typed_directory();
            }
            // The one printable that is a key, and only before typing starts: a
            // filter beginning with a literal `?` matches nothing anyway, and this
            // is where a new user asks for the keys. F1 opens help mid-filter.
            KeyCode::Char('?') if self.home.filter.is_empty() && !ctrl => {
                self.open_help_overlay();
            }
            // Space before typing starts folds a header, as Enter does, and is otherwise
            // nothing: a filter of one space is invisible at the prompt and matched every
            // name with a space in it, below the working directory too.
            KeyCode::Char(' ') if self.home.filter.is_empty() && !ctrl => {
                if self.home.selection_is_header() {
                    self.home_toggle_fold();
                }
            }
            KeyCode::Char(c) if !ctrl => {
                self.home.filter.push(c);
                // Typing is what asks for the recursive search. Starting it here and
                // not on open means the walk is only ever paid for by someone who is
                // actually looking for something.
                self.spawn_home_search();
                self.home.sync_search_section();
                self.home.select_first_entry();
            }
            _ => {}
        }
        None
    }

    /// Get a color from the theme by name
    fn color(&self, name: &str) -> Color {
        self.theme.get(name)
    }

    /// The export format to offer by default for a dataset opened from `path`.
    ///
    /// The format the open read wins (`format`: what it sniffed, or `--format`), then
    /// the extension. A compressed CSV keeps its CSV identity: `sales.csv.gz` has
    /// extension `gz`, and the `.csv` that matters is in the stem, so reading the
    /// extension alone offered no default at all.
    fn export_format_for(path: &Path, format: Option<FileFormat>) -> Option<ExportFormat> {
        format
            .or_else(|| FileFormat::from_path(path))
            .or_else(|| {
                CompressionFormat::from_extension(path)
                    .and(path.file_stem())
                    .and_then(|stem| FileFormat::from_path(Path::new(stem)))
            })
            .and_then(crate::readers::export_default)
    }

    /// `options` for the compressed delimited file `file`, in the dialect of the
    /// delimited spec it matches, or as they are when it matches none. The loader sends
    /// such a file straight to be decompressed, past the scan that matches the others.
    fn with_delimited_spec(
        file: &Path,
        mut options: OpenOptions,
        formats: &crate::formats::Registry,
    ) -> Result<OpenOptions> {
        if options.delimited.is_some() {
            return Ok(options);
        }
        let asked = crate::formats::Asked {
            spec_file: options.spec_file.clone(),
            spec: options.spec_fetched.clone(),
            spec_name: options.spec_name.clone(),
            compression: options.compression,
            ..Default::default()
        };
        let crate::formats::Route::Delimited(choice) =
            crate::formats::route(file, &asked, formats).map_err(|e| color_eyre::eyre::eyre!(e))?
        else {
            return Ok(options);
        };
        let Some(delimited) = choice.spec.delimited.clone() else {
            return Ok(options);
        };
        delimited.apply(&mut options);
        let chosen =
            crate::delimited_spec::DelimitedRead::chosen(choice.spec, choice.by, choice.also);
        let read = crate::delimited_spec::read_facts(&chosen, &[file.to_path_buf()], &options)?;
        options.delimited = Some(Arc::new(read));
        Ok(options)
    }

    /// Read a compressed CSV, TSV or PSV into a table state, split on its format's
    /// separator.
    ///
    /// This is the one input datui cannot scan lazily: the file has to be
    /// decompressed and parsed before anything can be shown, which for a large export
    /// is minutes. It takes no `&self` so it can run on a background thread.
    fn decompressed_delimited_state(
        path: &Path,
        options: &OpenOptions,
        writer: &crate::unfinished::Writer,
    ) -> Result<DataTableState> {
        let separator = options
            .format
            .and_then(FileFormat::separator)
            .unwrap_or(b',');
        DataTableState::from_delimited_for_open(path, separator, options, writer)
    }

    /// Polars' view of one source's S3 settings, for `scan_parquet`.
    #[cfg(feature = "cloud")]
    fn build_s3_cloud_options(settings: &crate::cloud_sources::S3Settings) -> CloudOptions {
        let settings = settings.clone();
        let virtual_hosted = (settings.endpoint.is_some() || settings.virtual_hosted.is_some())
            .then(|| settings.virtual_hosted_style().to_string());
        let configs: Vec<(AmazonS3ConfigKey, String)> = [
            (AmazonS3ConfigKey::Endpoint, settings.endpoint),
            (AmazonS3ConfigKey::AccessKeyId, settings.access_key_id),
            (
                AmazonS3ConfigKey::SecretAccessKey,
                settings.secret_access_key,
            ),
            (AmazonS3ConfigKey::Token, settings.session_token),
            (AmazonS3ConfigKey::Region, settings.region),
            (AmazonS3ConfigKey::VirtualHostedStyleRequest, virtual_hosted),
            (
                AmazonS3ConfigKey::SkipSignature,
                settings.skip_signature.then(|| "true".to_string()),
            ),
        ]
        .into_iter()
        .filter_map(|(key, value)| value.map(|v| (key, v)))
        .collect();
        let opts = CloudOptions::default();
        if configs.is_empty() {
            opts
        } else {
            opts.with_aws(configs)
        }
    }

    /// The bucket and key of an `s3://bucket/key` or `gs://bucket/key` URL. The key
    /// is empty for a bucket root.
    #[cfg(feature = "cloud")]
    fn cloud_bucket_and_key(url: &str) -> Result<(String, String)> {
        if let Some((_, container, key)) = source::azure_parts(url) {
            return Ok((container, key.trim_matches('/').to_string()));
        }
        crate::cloud_browse::split_bucket_url(url)
            .map(|(_, bucket, key)| (bucket, key))
            .ok_or_else(|| {
                color_eyre::eyre::eyre!("URL must be s3://bucket/key or gs://bucket/key")
            })
    }

    /// The store Polars itself will scan `url` through, from its cache keyed on the
    /// bucket and `options`, so the footer read, the size probe and a download share
    /// one credential chain, TLS client and connection pool with the scan instead of
    /// each building a store of their own.
    #[cfg(feature = "cloud")]
    fn polars_object_store(
        url: &str,
        options: &CloudOptions,
        runtime: &tokio::runtime::Handle,
    ) -> Result<Arc<dyn object_store::ObjectStore>> {
        let url = url.to_string();
        let options = options.clone();
        wait_on_runtime(runtime, async move {
            let (_, store) = polars::io::cloud::build_object_store(
                PlRefPath::new(url.as_str()),
                Some(&options),
                false,
            )
            .await?;
            polars::prelude::PolarsResult::Ok(store.to_dyn_object_store().await.into_owned())
        })
        .ok_or_else(|| color_eyre::eyre::eyre!("cancelled"))?
        .map_err(|e| color_eyre::eyre::eyre!("Object store config failed: {}", e))
    }

    /// Human-readable byte size, for the download confirmation and the load's progress.
    fn format_bytes(n: u64) -> String {
        const KB: u64 = 1024;
        const MB: u64 = KB * 1024;
        const GB: u64 = MB * 1024;
        const TB: u64 = GB * 1024;
        if n >= TB {
            format!("{:.2} TB", n as f64 / TB as f64)
        } else if n >= GB {
            format!("{:.2} GB", n as f64 / GB as f64)
        } else if n >= MB {
            format!("{:.2} MB", n as f64 / MB as f64)
        } else if n >= KB {
            format!("{:.2} KB", n as f64 / KB as f64)
        } else {
            format!("{} bytes", n)
        }
    }

    /// Build an HTTP agent with a total time budget.
    ///
    /// ureq 3 moved timeouts off the request and onto agent configuration, so
    /// every request has to come from an agent to be bounded at all. Leaving a
    /// request unbounded would mean a remote that accepts a connection and then
    /// dribbles bytes forever hangs the whole TUI, and the user's only way out
    /// is to kill the process.
    ///
    /// `timeout_global` covers the entire exchange rather than individual
    /// socket operations, which is the property that matters here: a server
    /// that sends one byte every 29 seconds defeats a per-read timeout but not
    /// this one.
    #[cfg(feature = "http")]
    fn http_agent(total: std::time::Duration) -> ureq::Agent {
        ureq::Agent::config_builder()
            .timeout_global(Some(total))
            .build()
            .into()
    }

    #[cfg(feature = "http")]
    fn fetch_remote_size_http(url: &str) -> Result<Option<u64>> {
        let agent = Self::http_agent(std::time::Duration::from_secs(15));
        // ureq asks for gzip by default and strips Content-Length from a compressed
        // answer, so a server that compresses (GitHub Pages does) reports no size.
        // Identity asks for the file's own length, which is what lands on disk.
        match agent.head(url).header("Accept-Encoding", "identity").call() {
            Ok(r) => Ok(r
                .headers()
                .get("Content-Length")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok())),
            Err(_) => Ok(None),
        }
    }

    /// The size of one S3 or GCS object, from a HEAD through the shared store.
    #[cfg(feature = "cloud")]
    fn fetch_remote_size_cloud(
        url: &str,
        cloud: &crate::config::CloudConfig,
        runtime: &tokio::runtime::Handle,
    ) -> Result<Option<u64>> {
        use object_store::ObjectStoreExt;

        let (_bucket, key) = Self::cloud_bucket_and_key(url)?;
        if key.is_empty() {
            return Ok(None);
        }
        let (_, _, store) = Self::cloud_store_for(Path::new(url), cloud, runtime)?;
        let path = crate::cloud_browse::object_path(&key);
        let head = wait_on_runtime(runtime, async move { store.head(&path).await });
        Ok(head.and_then(|r| r.ok()).map(|meta| meta.size))
    }

    /// Download `url` to a temporary file. `stop` ends it early, while the server is
    /// sending or while it is silent, and any failure removes the file; see
    /// [`crate::download::read_to_temp`].
    #[cfg(feature = "http")]
    fn download_http_to_temp(
        url: &str,
        temp_dir: Option<&Path>,
        extension: Option<&str>,
        writer: &crate::unfinished::Writer,
    ) -> Result<crate::download::TempDownload> {
        use crate::download::StreamError;

        let url = url.to_string();
        let open = move || {
            let agent = Self::http_agent(std::time::Duration::from_secs(300));
            let response = agent
                .get(&url)
                .call()
                .map_err(|e| format!("Download failed. Check the URL and your connection: {e}"))?;
            let status = response.status();
            if status.is_client_error() || status.is_server_error() {
                return Err(format!(
                    "Server returned {} {}. Check the URL.",
                    status.as_u16(),
                    status.canonical_reason().unwrap_or("Unknown")
                ));
            }
            // No length: ureq hands back a compressed answer decompressed, and the
            // Content-Length it came with is the wire's, not the file's.
            Ok((response.into_body().into_reader(), None))
        };
        crate::download::read_to_temp(temp_dir, extension, open, writer).map_err(
            |error| match error {
                StreamError::Open(message) => color_eyre::eyre::eyre!(message),
                StreamError::Read(e) => {
                    color_eyre::eyre::eyre!("Download failed partway. Check your connection: {e}")
                }
                StreamError::Short { expected, got } => color_eyre::eyre::eyre!(
                    "Download failed partway: it ended after {got} of {expected} bytes."
                ),
                StreamError::Write(report) => report,
                StreamError::Cut => color_eyre::eyre::eyre!("Download was cancelled."),
            },
        )
    }

    /// Stream one S3, GCS or Azure object to a temporary file, named for the user by
    /// its scheme in any error. A few chunks are in memory at a time; see
    /// [`crate::download`]. `writer`'s open stopping ends it early, and any failure
    /// removes the file.
    #[cfg(feature = "cloud")]
    fn download_cloud_to_temp(
        url: &str,
        cloud: &crate::config::CloudConfig,
        options: &OpenOptions,
        runtime: &tokio::runtime::Handle,
        writer: &crate::unfinished::Writer,
    ) -> Result<crate::download::TempDownload> {
        use crate::download::StreamError;
        use object_store::ObjectStoreExt;

        let (label, example) = match source::input_source(Path::new(url)) {
            source::InputSource::Gcs(_) => ("GCS", "gs://bucket/path/file.csv"),
            source::InputSource::Azure(_) => (
                "Azure",
                "abfss://container@account.dfs.core.windows.net/path/file.csv",
            ),
            _ => ("S3", "s3://bucket/path/file.csv"),
        };
        let ext = source::download_suffix(url);
        let (_bucket, key) = Self::cloud_bucket_and_key(url)?;
        if key.is_empty() {
            return Err(color_eyre::eyre::eyre!(
                "{label} URL must point to an object (e.g. {example})"
            ));
        }
        let (_, _, store) = Self::cloud_store_for(Path::new(url), cloud, runtime)?;

        let path = crate::cloud_browse::object_path(&key);
        let open = async move {
            let got = store.get(&path).await.map_err(|e| e.to_string())?;
            let len = got.range.end - got.range.start;
            Ok((got.into_stream(), Some(len)))
        };
        crate::download::stream_to_temp(
            runtime,
            options.temp_dir.as_deref(),
            ext.as_deref(),
            open,
            writer,
        )
        .map_err(|error| match error {
            StreamError::Open(e) => color_eyre::eyre::eyre!(
                "Could not read from {label}. Check credentials and URL: {e}"
            ),
            StreamError::Read(e) => {
                color_eyre::eyre::eyre!("Could not read {label} object body: {e}")
            }
            StreamError::Short { expected, got } => color_eyre::eyre::eyre!(
                "Could not read {label} object body: it ended after {got} of {expected} bytes"
            ),
            StreamError::Write(report) => report,
            StreamError::Cut => color_eyre::eyre::eyre!("{label} download was cancelled."),
        })
    }

    /// Run the worker of an open's phase, as `load`'s job: its answer goes to the loader.
    ///
    /// Every phase runs off the event thread. The size probe is a HEAD request: fifteen
    /// seconds of timeout for HTTP, unbounded for S3 and GCS, and inline it froze the UI
    /// precisely where the user is most likely to want out. Scanning is where the
    /// wall-clock time goes — CSV schema inference, and hive directories with many files
    /// — and the schema read of a directory reads a footer from each file.
    /// The open's scan of `paths`, named `path`: what the frame is, or what has to
    /// happen before there is one. Run by the open's `Scan` phase, and by the home
    /// screen's preview, which hands what it builds to the open.
    pub(crate) fn scan_for_open(
        cloud: &crate::config::CloudConfig,
        formats: &crate::formats::Registry,
        paths: &[PathBuf],
        options: OpenOptions,
        path: Option<PathBuf>,
    ) -> std::result::Result<loading::LoadAnswer, String> {
        use loading::LoadAnswer;
        let bytes_of = |files: &[PathBuf]| -> u64 {
            files
                .iter()
                .filter_map(|f| std::fs::metadata(f).ok())
                .map(|m| m.len())
                .sum()
        };
        // What the read passed over rides back with the options it was asked
        // for, so the dataset can say what it left out. Seeded with what the
        // caller already knows and overwritten by what the read finds: a
        // directory on disk is the read's own answer, because it is the pass
        // that decides, while for a prefix in an object store Polars does the
        // listing and never sees the other formats — there the home screen's
        // listing is the only witness.
        let mut report = ReadReport {
            left_out: options.left_out.clone(),
            files_disagree: options.files_disagree,
            format: None,
            format_read: None,
            read_python: Vec::new(),
            sqlite: None,
            opened: None,
            splits: options.splits.clone(),
            delimited: None,
            table: None,
        };
        // A followed file reads every row it can and counts the rest: a row
        // that does not fit the schema never stops the follow.
        let options = OpenOptions {
            ignore_errors: options.ignore_errors || options.follow,
            ..options
        };
        let named = |e: color_eyre::Report| {
            crate::error_display::user_message_from_report(&e, path.as_deref())
        };
        // NDJSON followed is scanned rather than read whole.
        let followed_lines = options.follow
            && crate::follow::format_of(&paths[0], options.format)
                .descriptor()
                .lines
                == Some(crate::cli::Lines::Json);
        // An Arrow IPC stream followed is read by a scan of its own, not converted.
        let followed_stream = options.follow
            && crate::follow::followed_stream(
                &paths[0],
                Some(crate::follow::format_of(&paths[0], options.format)),
                &options,
            );
        let scan = if followed_lines {
            crate::follow::scan_lines(&paths[0], &options, &mut report.read_python).map(Scan::from)
        } else if followed_stream {
            crate::follow::stream::scan(&paths[0])
                .map(Scan::from)
                .map_err(|e| color_eyre::eyre::eyre!(e))
        } else {
            Self::build_lazyframe_from_paths_with(cloud, paths, &options, &mut report, formats)
        }
        // Named as the dataset is: a download by its URL, not its temp file.
        .map_err(named)?;
        let format = scan.format(report.format.or(options.format));
        // Bounded to the complete records, and counted for the watcher. A
        // recording that cannot be followed is read as it stands, and goes on.
        let recording = options
            .spool
            .as_ref()
            .is_some_and(|handle| handle.spool().tee().is_some());
        let (scan, tail) = match scan {
            Scan::Frame(lf) if options.follow => {
                let format = crate::follow::format_of(&paths[0], format);
                let refused = (!followed_stream)
                    .then(|| crate::follow::refusal(Some(format), &options))
                    .flatten();
                match refused {
                    Some(_) if recording => (Scan::Frame(lf), None),
                    Some(refusal) => return Err(refusal),
                    None => {
                        let (lf, tail) =
                            crate::follow::bound_to_complete(*lf, &paths[0], format, &options)
                                .map_err(named)?;
                        (Scan::Frame(Box::new(lf)), Some(Arc::new(tail)))
                    }
                }
            }
            _ if options.follow && !recording => {
                return Err(crate::follow::refusal(format, &options)
                    .unwrap_or_else(|| "This file cannot be followed as it grows.".to_string()));
            }
            scan => (scan, None),
        };
        let read_mode = scan.read_mode(format, report.format_read.is_some(), &options);
        let mut options = OpenOptions {
            left_out: report.left_out,
            files_disagree: report.files_disagree,
            format,
            format_read: report.format_read,
            sqlite: report.sqlite,
            opened: report.opened,
            splits: report.splits,
            read_python: report.read_python,
            read_mode,
            tail,
            table: report.table.or_else(|| options.table.clone()),
            ..options
        };
        // The spec's dialect stays with the dataset, so a read again (`H`,
        // a decompressed copy) reads as this one did.
        if let Some(read) = report.delimited {
            read.delimited().apply(&mut options);
            options.delimited = Some(read);
        }
        Ok(match scan {
            Scan::Frame(lf) => LoadAnswer::Scanned { lf, path, options },
            Scan::Decompress { file, .. } => LoadAnswer::Compressed {
                file,
                path,
                options,
            },
            Scan::Streams(files) => LoadAnswer::Convert {
                what: loading::Conversion::Streams,
                bytes: bytes_of(&files),
                files,
                path,
                options,
            },
            Scan::DecompressSpec { file, choice } => LoadAnswer::CompressedRecords {
                file,
                path,
                choice,
                options,
            },
            Scan::ReadInto { files, format } => LoadAnswer::Convert {
                what: loading::Conversion::Text(format),
                bytes: bytes_of(&files),
                files,
                path,
                options,
            },
            Scan::Tables { file, tables, .. } => LoadAnswer::Tables { file, tables, path },
            Scan::Unpack {
                file,
                member,
                format,
            } => LoadAnswer::Convert {
                what: loading::Conversion::Text(format),
                bytes: bytes_of(std::slice::from_ref(&file)),
                files: vec![file],
                path,
                options: OpenOptions {
                    table: Some(member),
                    ..options
                },
            },
            Scan::Hex { file, asked } => LoadAnswer::Hex {
                file,
                asked,
                record_size: options.record_size,
            },
        })
    }

    /// The open's schema read of the scan's frame: the dataset, built with everything
    /// the open `made`. Run by the open's `ReadSchema` phase, and by the home screen's
    /// preview.
    pub(crate) fn read_schema_for_open(
        lf: LazyFrame,
        path: Option<PathBuf>,
        options: OpenOptions,
        cloud: &crate::config::CloudConfig,
        runtime: &tokio::runtime::Handle,
        report: &crate::measurements::OpenReport,
        made: loading::Made,
    ) -> std::result::Result<loading::LoadAnswer, String> {
        use loading::LoadAnswer;
        let (state, facts, debug_label) =
            Self::build_schema_state(lf, path.as_deref(), &options, cloud, runtime, report)
                .map_err(|e| crate::error_display::user_message_from_report(&e, path.as_deref()))?;
        // Everything the open found, given to the dataset as it is built.
        let loading::Made {
            download,
            converted,
            notes,
            other_tables,
            detail,
        } = made;
        let mut open_notes = facts.open_notes;
        open_notes.extend(notes);
        let mut other_tables_found = facts.other_tables;
        other_tables_found.extend(other_tables);
        let state = state.with_open(OpenFacts {
            fetched: Self::fetched(download.as_ref(), path.as_deref()),
            download,
            converted,
            other_tables: other_tables_found,
            open_notes,
            detail: detail.or(facts.detail),
            ..facts
        });
        Ok(LoadAnswer::SchemaRead {
            state: Box::new(state),
            path,
            options,
            debug_label: Some(debug_label),
        })
    }

    fn spawn_load_phase(&mut self, load: loading::LoadId, step: loading::Step) {
        use loading::{LoadAnswer, Step};
        let job = Job::Load(load);
        match step {
            #[cfg(any(feature = "http", feature = "cloud"))]
            Step::ReadHeaders {
                url,
                format,
                options,
                writer,
            } => {
                let (cloud, runtime) = (self.app_config.cloud.clone(), self.runtime.clone());
                self.spawn_job(job, Some("Reading headers..."), move |_| {
                    let read = crate::remote_model::read(&url, format, &cloud, &runtime, &|| {
                        writer.stopped()
                    });
                    let crate::remote_model::Read { lf, summary, notes } = match read {
                        Ok(read) => read,
                        Err(crate::model_files::RangeError::NoRanges) => {
                            return Ok(Answer::Load(Box::new(LoadAnswer::NoRanges { options })));
                        }
                        // The URL in the message may carry a password or a signature.
                        Err(crate::model_files::RangeError::Failed(message)) => {
                            return Err(crate::logging::redact(&message, &[]));
                        }
                    };
                    let opened = Arc::new(crate::model_files::opened(&summary));
                    let options = OpenOptions {
                        format: Some(format),
                        opened: Some(opened.clone()),
                        ..options
                    };
                    // The table is the headers, in memory: nothing is left to scan.
                    let state = Self::schema_state_from_full_scan(
                        lf,
                        None,
                        &OpenOptions {
                            hive: false,
                            ..options.clone()
                        },
                    )
                    .map_err(|e| crate::error_display::user_message_from_report(&e, Some(&url)))?
                    .with_open(OpenFacts {
                        detail: opened.detail.clone(),
                        open_notes: notes,
                        read_as: Some(format),
                        ..Default::default()
                    });
                    Ok(Answer::Load(Box::new(LoadAnswer::SchemaRead {
                        state: Box::new(state),
                        path: Some(url),
                        options,
                        debug_label: Some("model headers (ranged)".to_string()),
                    })))
                });
            }
            #[cfg(any(feature = "http", feature = "cloud"))]
            Step::Probe(pending) => {
                #[cfg(feature = "cloud")]
                let (cloud, runtime) = (self.app_config.cloud.clone(), self.runtime.clone());
                self.spawn_job(job, Some("Checking size..."), move |_| {
                    // Arrow in a store: its listing says which objects, and which of
                    // them are streams to download.
                    #[cfg(feature = "cloud")]
                    if let loading::PendingDownload::Arrow { url, .. } = &pending {
                        let (_, _, options) = pending.parts();
                        let (objects, options) = crate::cloud_arrow::list(
                            url, options, &cloud, &runtime,
                        )
                        .map_err(|e| crate::error_display::user_message_from_report(&e, None))?;
                        let size = crate::cloud_arrow::stream_bytes(&objects);
                        return Ok(Answer::Load(Box::new(LoadAnswer::Sized(
                            loading::PendingDownload::Arrow {
                                url: url.clone(),
                                objects,
                                size: Some(size),
                                options,
                            },
                        ))));
                    }
                    let size = match &pending {
                        #[cfg(feature = "http")]
                        loading::PendingDownload::Http { url, .. } => {
                            Self::fetch_remote_size_http(url).unwrap_or(None)
                        }
                        #[cfg(feature = "cloud")]
                        loading::PendingDownload::S3 { url, .. }
                        | loading::PendingDownload::Gcs { url, .. }
                        | loading::PendingDownload::Azure { url, .. } => {
                            Self::fetch_remote_size_cloud(url, &cloud, &runtime).unwrap_or(None)
                        }
                        #[cfg(feature = "cloud")]
                        loading::PendingDownload::Arrow { size, .. } => *size,
                    };
                    Ok(Answer::Load(Box::new(LoadAnswer::Sized(
                        pending.with_size(size),
                    ))))
                });
            }
            #[cfg(any(feature = "http", feature = "cloud"))]
            Step::Download { pending, writer } => {
                // The load's stop flag is raised when it is abandoned or another open
                // replaces it, and when the app drops: the download stops at the next
                // chunk, or while the source is silent, and removes its file. Quitting
                // removes it even if the process ends first (`ExitSweep`).
                #[cfg(feature = "cloud")]
                let (cloud, runtime) = (self.app_config.cloud.clone(), self.runtime.clone());
                let status = match &pending {
                    #[cfg(feature = "http")]
                    loading::PendingDownload::Http { .. } => "Downloading...",
                    #[cfg(feature = "cloud")]
                    loading::PendingDownload::S3 { .. } => "Downloading from S3...",
                    #[cfg(feature = "cloud")]
                    loading::PendingDownload::Gcs { .. } => "Downloading from GCS...",
                    #[cfg(feature = "cloud")]
                    loading::PendingDownload::Azure { .. } => "Downloading from Azure...",
                    #[cfg(feature = "cloud")]
                    loading::PendingDownload::Arrow { url, .. } => {
                        match source::input_source(Path::new(url)) {
                            source::InputSource::Gcs(_) => "Downloading from GCS...",
                            source::InputSource::Azure(_) => "Downloading from Azure...",
                            _ => "Downloading from S3...",
                        }
                    }
                };
                // How much, when the server said: a download nobody was asked about
                // says what it is fetching.
                let sized = pending
                    .parts()
                    .1
                    .filter(|_| status == "Downloading...")
                    .map(|size| format!("Downloading {}...", discover::format_size(size)));
                let status = sized.as_deref().unwrap_or(status);
                self.spawn_job(job, Some(status), move |_| {
                    let (url, _, options) = pending.parts();
                    let (download, options) = match &pending {
                        #[cfg(feature = "http")]
                        loading::PendingDownload::Http { .. } => {
                            let ext = source::download_suffix(url);
                            Self::download_http_to_temp(
                                url,
                                options.temp_dir.as_deref(),
                                ext.as_deref(),
                                &writer,
                            )
                            .map(|file| (file, options.clone()))
                        }
                        #[cfg(feature = "cloud")]
                        loading::PendingDownload::S3 { .. }
                        | loading::PendingDownload::Gcs { .. }
                        | loading::PendingDownload::Azure { .. } => {
                            Self::download_cloud_to_temp(url, &cloud, options, &runtime, &writer)
                                .map(|file| (file, options.clone()))
                        }
                        // Its streams, converted as they arrive; its IPC files stay put.
                        #[cfg(feature = "cloud")]
                        loading::PendingDownload::Arrow { objects, .. } => {
                            crate::cloud_arrow::download(
                                objects, options, &cloud, &runtime, &writer,
                            )
                            .map(|(file, parts)| {
                                let options = OpenOptions {
                                    format: Some(FileFormat::Arrow),
                                    hive: false,
                                    arrow_parts: Some(Arc::new(parts)),
                                    ..options.clone()
                                };
                                (file, options)
                            })
                        }
                    }
                    .map_err(|e| crate::error_display::user_message_from_report(&e, None))?;
                    Ok(Answer::Load(Box::new(LoadAnswer::Downloaded {
                        download,
                        options,
                    })))
                });
            }
            Step::Spool {
                options,
                writer,
                read,
            } => {
                // The read is a thread of its own, so a producer gone quiet does not hold
                // up the stop: Ctrl+O and quitting remove the partial file at once.
                let piped = self.stdin_reader.take();
                let stdout = self.stdout_pass.take();
                self.spawn_job(job, Some("Reading stdin..."), move |_| {
                    let open = move || -> crate::download::Opened<Box<dyn std::io::Read + Send>> {
                        Ok((piped.unwrap_or_else(|| Box::new(std::io::stdin())), None))
                    };
                    // Followed, the copy goes on behind the first rows; recorded, it
                    // goes to the file the user named.
                    let (download, options) = if options.follow || options.tee.is_some() {
                        match crate::follow::spool(open, options, &writer, &read, stdout)? {
                            (crate::follow::Spooled::Temp(download), options) => {
                                (download, options)
                            }
                            (crate::follow::Spooled::Kept(file), options) => {
                                return Ok(Answer::Load(Box::new(LoadAnswer::Recorded {
                                    file,
                                    options,
                                })));
                            }
                        }
                    } else {
                        crate::stdin::spool(open, options, &writer, &read)?
                    };
                    Ok(Answer::Load(Box::new(LoadAnswer::Spooled {
                        download,
                        options,
                    })))
                });
            }
            Step::FetchSpec {
                url,
                options,
                writer,
            } => {
                #[cfg(any(feature = "http", feature = "cloud"))]
                let (cloud, runtime) = (self.app_config.cloud.clone(), self.runtime.clone());
                self.spawn_job(job, Some("Reading spec..."), move |_| {
                    #[cfg(any(feature = "http", feature = "cloud"))]
                    let fetched = crate::remote_model::fetch_small(
                        &url,
                        crate::formats::MAX_SPEC_BYTES,
                        &cloud,
                        &runtime,
                        &|| writer.stopped(),
                    );
                    #[cfg(not(any(feature = "http", feature = "cloud")))]
                    let fetched: std::result::Result<Option<Vec<u8>>, String> = {
                        let _ = &writer;
                        Err(format!(
                            "{} is a URL, and this build reads no URLs",
                            url.display()
                        ))
                    };
                    // The URL in the message may carry a password or a signature.
                    let bytes = fetched
                        .map_err(|message| crate::logging::redact(&message, &[]))?
                        .ok_or_else(|| {
                            format!(
                                "{}: a spec is at most {}",
                                crate::logging::redact(&url.display().to_string(), &[]),
                                crate::formats::MAX_SPEC_SAID
                            )
                        })?;
                    let spec = crate::formats::Spec::from_bytes(&bytes, &url)
                        .map_err(|e| crate::logging::redact(&e.to_string(), &[]))?;
                    Ok(Answer::Load(Box::new(LoadAnswer::SpecFetched {
                        spec: Arc::new(spec),
                        options,
                    })))
                });
            }
            Step::DecompressRecords {
                file,
                path,
                choice,
                options,
                writer,
            } => {
                self.spawn_job(job, Some("Decompressing..."), move |_| {
                    let failed = |e: color_eyre::Report| {
                        crate::error_display::user_message_from_report(&e, Some(path.as_path()))
                    };
                    let compression = options
                        .compression
                        .or_else(|| CompressionFormat::from_extension(&file))
                        .ok_or_else(|| format!("{} is not compressed", path.display()))?;
                    let temp_dir = options.temp_dir.clone().unwrap_or_else(std::env::temp_dir);
                    let copy =
                        DataTableState::decompress_to_copy(&file, compression, &temp_dir, &writer)
                            .map_err(failed)?;
                    Ok(Answer::Load(Box::new(LoadAnswer::DecompressedRecords {
                        copy,
                        path,
                        choice,
                        options,
                    })))
                });
            }
            Step::ReadRecords {
                copy,
                path,
                choice,
                options,
            } => {
                self.spawn_job(job, Some("Reading records..."), move |_| {
                    let named = path
                        .file_name()
                        .map(|n| n.to_string_lossy().into_owned())
                        .unwrap_or_default();
                    let read = crate::formats::read(&copy, &named, choice)?;
                    let lf = Arc::clone(&read.records).into_lazy().map_err(|e| {
                        crate::error_display::user_message_from_report(
                            &color_eyre::eyre::eyre!(e),
                            Some(path.as_path()),
                        )
                    })?;
                    Ok(Answer::Load(Box::new(LoadAnswer::Scanned {
                        lf: Box::new(lf),
                        path: Some(path),
                        options: OpenOptions {
                            format_read: Some(Arc::new(read)),
                            ..options
                        },
                    })))
                });
            }
            Step::Decompress {
                file,
                path,
                options,
                writer,
                download,
            } => {
                // Only delimited text comes this way, its format said by the loader;
                // CSV when not, so it can have its header turned off.
                let options = OpenOptions {
                    format: options.format.or(Some(FileFormat::TEXT)),
                    ..options
                };
                let formats = self.formats.clone();
                self.spawn_job(job, Some("Decompressing..."), move |_| {
                    let failed = |e: color_eyre::Report| {
                        crate::error_display::user_message_from_report(&e, Some(path.as_path()))
                    };
                    let options =
                        Self::with_delimited_spec(&file, options, &formats).map_err(failed)?;
                    let state = Self::decompressed_delimited_state(&file, &options, &writer)
                        .map_err(failed)?
                        .with_open(OpenFacts {
                            fetched: Self::fetched(download.as_ref(), Some(&path)),
                            download,
                            open_notes: options
                                .delimited
                                .as_ref()
                                .map(|read| read.notes())
                                .unwrap_or_default(),
                            delimited: options.delimited.clone(),
                            read_as: options.format,
                            // The loader sends a compressed file here without a scan.
                            read_mode: options.format.and_then(|f| {
                                f.read_mode(crate::Stored::Compressed {
                                    in_memory: options.decompress_in_memory,
                                })
                            }),
                            ..Default::default()
                        });
                    Ok(Answer::Load(Box::new(LoadAnswer::SchemaRead {
                        state: Box::new(state),
                        path: Some(path),
                        options,
                        debug_label: Some("decompressed delimited".to_string()),
                    })))
                });
            }
            Step::Convert {
                what,
                files,
                path,
                options,
                writer,
                read,
            } => {
                // The load's stop flag ends it at the next record batch or chunk,
                // removing its files; quitting removes them even if the process ends
                // first.
                let formats = self.formats.clone();
                self.spawn_job(job, Some(what.status()), move |_| {
                    let named = |e: color_eyre::Report| {
                        crate::error_display::user_message_from_report(&e, path.as_deref())
                    };
                    let converted = match what {
                        loading::Conversion::Streams => {
                            let converted = crate::ipc_stream::convert(
                                &files,
                                options.temp_dir.as_deref(),
                                &writer,
                                &read,
                            )
                            .map_err(named)?;
                            loading::Converted::Streams {
                                file: converted.file,
                                parts: converted.parts,
                            }
                        }
                        loading::Conversion::Text(format) => {
                            let display = path.clone().unwrap_or_else(|| files[0].clone());
                            let (converted, detail) =
                                crate::readers::convert(&crate::readers::ConvertIn {
                                    files: &files,
                                    display: &display,
                                    format,
                                    options: &options,
                                    formats: &formats,
                                    writer: &writer,
                                    read: &read,
                                })
                                .map_err(named)?;
                            loading::Converted::Frame {
                                files: converted.files,
                                lf: Box::new(converted.lf),
                                notes: converted.notes,
                                other_tables: converted.other_tables,
                                detail,
                            }
                        }
                    };
                    Ok(Answer::Load(Box::new(LoadAnswer::Converted {
                        converted,
                        path,
                        options,
                    })))
                });
            }
            Step::Scan {
                paths,
                options,
                display,
                status,
            } => {
                let cloud = self.app_config.cloud.clone();
                let formats = self.formats.clone();
                // A download is scanned from a temp path the user never typed and would not
                // recognise; the URL they did type is what names the dataset.
                let path = display.or_else(|| paths.first().cloned());
                self.reads.scans += 1;
                self.spawn_job(job, Some(status), move |_| {
                    Self::scan_for_open(&cloud, &formats, &paths, options, path)
                        .map(|answer| Answer::Load(Box::new(answer)))
                });
            }
            Step::ReadSchema {
                lf,
                path,
                options,
                progress,
                made,
            } => {
                self.debug.schema_load = None;
                let cloud = self.app_config.cloud.clone();
                let runtime = self.runtime.clone();
                let report = crate::measurements::OpenReport {
                    progress,
                    meter: Arc::new(crate::measurements::Meter::default()),
                    remembered: Some(self.cache.clone()),
                };
                self.spawn_job(job, Some("Caching schema..."), move |_| {
                    Self::read_schema_for_open(*lf, path, options, &cloud, &runtime, &report, made)
                        .map(|answer| Answer::Load(Box::new(answer)))
                });
            }
            Step::Nothing
            | Step::Crash(_)
            | Step::Install(_)
            | Step::Failed(_)
            | Step::Tables(_)
            | Step::Hex(_) => {
                unreachable!("not a phase with a worker")
            }
            #[cfg(any(feature = "http", feature = "cloud"))]
            Step::Ask(_) => unreachable!("not a phase with a worker"),
        }
    }

    /// What the user is being asked to agree to before a remote file is downloaded.
    #[cfg(any(feature = "http", feature = "cloud"))]
    fn download_confirmation_message(
        pending: &loading::PendingDownload,
        note: Option<&str>,
    ) -> String {
        let (url, size, options) = pending.parts();
        let size_str = size
            .map(Self::format_bytes)
            .unwrap_or_else(|| "unknown".to_string());
        let dest_dir = options
            .temp_dir
            .as_deref()
            .map(|p| p.display().to_string())
            .unwrap_or_else(|| std::env::temp_dir().display().to_string());
        let note = note.map(|note| format!("{note}\n\n")).unwrap_or_default();
        // Only a store's Arrow streams are downloaded, and converted as they arrive.
        let files = match pending.arrow_files() {
            Some((1, 0)) => "Arrow stream: converted as it downloads\n".to_string(),
            Some((streams, 0)) => {
                format!("Files: {streams} Arrow streams, converted as they download\n")
            }
            Some((streams, in_place)) => {
                let streams = match streams {
                    1 => "1 Arrow stream, converted as it downloads".to_string(),
                    n => format!("{n} Arrow streams, converted as they download"),
                };
                let in_place = match in_place {
                    1 => "1 IPC file read in place".to_string(),
                    n => format!("{n} IPC files read in place"),
                };
                format!("Files: {streams}; {in_place}\n")
            }
            None => String::new(),
        };
        format!(
            "{note}URL: {url}\n{files}File size: {size_str}\nDestination: {dest_dir} (temporary file)\n\nContinue with download?"
        )
    }

    /// Take the offer on the note the cursor is on: read its column as text.
    ///
    /// Only a note that carries the offer has one, and the offer is taken off a note
    /// datui could not act on, so the `Ok(false)` arms here are for a note that has
    /// gone stale under the cursor rather than for anything to tell the user about. A
    /// failure is the scan's, and is shown the way any other failed read is.
    fn read_the_selected_note_s_column_as_text(&mut self) {
        let Some(state) = self.data_table_state.as_mut() else {
            return;
        };
        let notes = state.notes();
        let Some(column) = notes
            .get(self.info_modal.notes_selected_index)
            .and_then(|note| note.read_as_text.clone())
        else {
            return;
        };
        // Rebuilt here, read off the UI thread. A failure is left showing on the state.
        if let Ok(true) = state.deferred(|s| s.read_column_as_text(&column)) {
            // The note that offered this is gone and the list is shorter, so the
            // cursor would otherwise sit past the end. Kept as near to where the
            // user left it as the shorter list allows, rather than thrown to the
            // top: one or two notes went, not all of them.
            let notes = state.notes().len();
            self.info_modal.notes_selected_index = self
                .info_modal
                .notes_selected_index
                .min(notes.saturating_sub(1));
            self.info_modal.notes_scroll_offset = 0;
            self.spawn_async_collect(Self::LOADING_BUFFER);
        }
    }

    fn hoist_partition_columns(
        lf: LazyFrame,
        schema: &Schema,
        partition_columns: &[String],
        drifts: bool,
    ) -> LazyFrame {
        hoist_partition_columns(lf, schema, partition_columns, drifts)
    }
}

/// Put hive partition columns first, ahead of the file's own columns. `drifts` keeps
/// the scan's hidden drift column, which the select would otherwise drop.
///
/// A free function rather than a method: rebuilding the scan to read a column as text
/// has to put the columns back the same way, and it happens on the table's state
/// rather than on the app.
pub(crate) fn hoist_partition_columns(
    lf: LazyFrame,
    schema: &Schema,
    partition_columns: &[String],
    drifts: bool,
) -> LazyFrame {
    if partition_columns.is_empty() {
        return lf;
    }
    let mut exprs: Vec<_> = partition_columns
        .iter()
        .map(|s| col(s.as_str()))
        .chain(
            schema
                .iter_names()
                .map(|s| s.to_string())
                .filter(|c| !partition_columns.contains(c))
                .map(|s| col(s.as_str())),
        )
        .collect();
    if drifts {
        exprs.push(col(crate::schema_union::DRIFT_COLUMN));
    }
    lf.select(exprs)
}

/// A local Hive directory as its listing found it: what every set of its footers is
/// read against.
struct LocalHive {
    dir: PathBuf,
    partition_columns: Vec<String>,
    /// The first file's partition values, which type the partition columns.
    values: Vec<(String, String)>,
    skipped: crate::schema_union::SkippedFiles,
}

/// A local dataset as some set of its footers describes it, and the scan that reads it.
struct LocalDataset {
    dataset: crate::schema_union::DatasetSchema,
    lf: LazyFrame,
    /// Each file's rows, or empty when they are not all known.
    file_rows: Vec<usize>,
    /// Every file, in scan order.
    paths: Vec<String>,
    /// Each readable file's rows, one group a file, or empty unless every footer was
    /// read: the count, without a pass of its own.
    row_groups: Vec<Vec<usize>>,
    /// The readable files and a scan of any of them, once every footer is known: a
    /// page then reads only the files holding its rows (#659).
    by_file: Option<crate::widgets::datatable::RemoteRead>,
}

impl LocalHive {
    /// What the footers at `read` say about the dataset of `files`. Shared by the open,
    /// which may have read only the two ends, and the pass that reads the rest: the two
    /// differ only in how much they know. `None` when nothing could be read.
    fn dataset(
        &self,
        files: &[PathBuf],
        read: &[usize],
        footers: &[Option<crate::schema_union::FileSchema>],
    ) -> Option<LocalDataset> {
        let mut dataset = crate::schema_union::union_sampled(files.len(), read, footers);
        if dataset.schema.is_empty() {
            return None;
        }
        dataset.schema = Arc::new(crate::schema_union::with_partition_columns(
            &dataset.schema,
            &self.partition_columns,
            &self.values,
        ));
        let paths: Vec<String> = files
            .iter()
            .map(|f| f.to_string_lossy().into_owned())
            .collect();
        let every_footer = read.len() == files.len();
        // Numbering rows needs every file's row count; a sampled dataset has not read
        // them all, so it forgoes the distinction rather than guessing at it.
        let file_rows: Vec<usize> = if every_footer {
            footers
                .iter()
                .map(|f| f.as_ref().map(|f| f.rows))
                .collect::<Option<Vec<_>>>()
                .unwrap_or_default()
        } else {
            Vec::new()
        };
        // Over the readable files only, as the scan is: a file mid-write is in neither.
        let row_groups: Vec<Vec<usize>> = if every_footer {
            footers.iter().flatten().map(|f| vec![f.rows]).collect()
        } else {
            Vec::new()
        };
        let drift = crate::schema_union::ScanDrift::new(&paths, &dataset, &file_rows);
        let schema = dataset.schema.clone();
        // Over the files that will open. `drift` is keyed by path, so a scan of fewer
        // of them still knows what each one holds.
        let readable = crate::schema_union::readable_paths(&paths, &dataset.unreadable);
        // Belt and braces: a dataset with nothing readable has an empty schema and has
        // already been handed back above.
        if readable.is_empty() {
            return None;
        }
        let scan: crate::widgets::datatable::FileScan = {
            let (schema, partition_columns, drift) = (
                schema.clone(),
                self.partition_columns.clone(),
                drift.map(Arc::new),
            );
            Arc::new(
                move |files: &[String], as_text: &[polars::prelude::PlSmallStr]| {
                    let lf = crate::schema_union::lenient_scan(
                        files,
                        schema.clone(),
                        None,
                        drift.as_deref(),
                        as_text,
                    )?;
                    Ok(hoist_partition_columns(
                        lf,
                        &schema,
                        &partition_columns,
                        drift.is_some(),
                    ))
                },
            )
        };
        let lf = scan(&readable, &[]).ok()?;
        // The rows are in the footers, so the counter answers without reading anything.
        let by_file = (!row_groups.is_empty()).then(|| {
            let counted = row_groups.clone();
            crate::widgets::datatable::RemoteRead {
                urls: readable.into_owned(),
                scan,
                count: Arc::new(move || Ok(counted.clone())),
            }
        });
        let dataset = dataset
            .with_partition_layouts(&self.dir.to_string_lossy(), &paths)
            .with_skipped(self.skipped);
        Some(LocalDataset {
            dataset,
            lf,
            file_rows,
            paths,
            row_groups,
            by_file,
        })
    }
}

/// Keep a local dataset's footers against its listing's fingerprint, if every one was
/// read and parsed — the same two conditions the cloud cache keeps, for the same
/// reasons (see `App::remember_dataset_shape`). No fingerprint, no keeping: a dataset
/// within one wave is not worth it, and one whose files moved under the listing has
/// none.
/// A number for each walk the home search starts, so scorings of one are never taken
/// for another's, even when the two walked the same place.
fn next_search_epoch() -> u64 {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

fn remember_local_shape(
    cache: Option<&crate::cache::CacheManager>,
    key: &str,
    fingerprint: Option<&str>,
    files: usize,
    read: &[usize],
    footers: &[Option<crate::schema_union::FileSchema>],
) {
    let (Some(cache), Some(fingerprint)) = (cache, fingerprint) else {
        return;
    };
    if read.len() != files || !footers.iter().all(Option::is_some) {
        return;
    }
    let (cached, schemas) = crate::schema_union::footers_to_cache(footers);
    cache.save_dataset_shape(
        key,
        crate::cache::DatasetShape {
            fingerprint: fingerprint.to_string(),
            files: cached,
            schemas,
            taken_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or_default(),
        },
    );
}

/// What a pass behind a staged open reported, and which dataset it was reading for.
/// `None` where the footers are: a pass that could not read them says so, so the
/// dataset stops waiting.
type FootersReported = Option<(u64, Option<crate::widgets::datatable::FootersFound>)>;

/// A cloud dataset as some set of its footers describes it.
///
/// The open builds one from the two ends of the listing and the pass behind it builds
/// another from every footer; what tells them apart is only how much they know.
#[cfg(feature = "cloud")]
struct CloudDataset {
    dataset: crate::schema_union::DatasetSchema,
    /// Each file's rows, or empty when they are not all known — the same condition
    /// under which the scan declines to number its rows.
    file_rows: Vec<usize>,
    urls: Vec<String>,
    /// Each file's row groups, or empty unless every footer was read and parsed.
    row_groups: Vec<Vec<usize>>,
    scan: crate::widgets::datatable::FileScan,
    partition_columns: Vec<String>,
}
impl App {
    /// Schema for a local directory of Parquet files: every column any of them has, from
    /// their footers, instead of `collect_schema()` over the whole set or one file's
    /// columns standing in for all.
    ///
    /// As a cloud prefix opens: past one wave of footers the two ends open the dataset
    /// and the rest are read behind it, joining when they land, and a directory whose
    /// listing has not changed since its footers were last all read opens from what
    /// they said then. On a network mount each footer is round trips, and a Hive tree
    /// is thousands of footers. `None` when the path is not that shape, or when nothing
    /// could be read — either way the caller falls back to the general scan, which
    /// reports the error properly if there is one.
    fn schema_state_from_local_hive(
        path: Option<&Path>,
        options: &OpenOptions,
        report: &crate::measurements::OpenReport,
    ) -> Option<(DataTableState, OpenFacts)> {
        if !options.single_spine_schema {
            return None;
        }
        let p = path.filter(|p| p.is_dir() && options.hive)?;
        let (files, skipped) = DataTableState::list_parquet_dir(p, &report.meter);
        let first = files.first()?;
        let hive = LocalHive {
            dir: p.to_path_buf(),
            partition_columns: DataTableState::discover_hive_partition_columns(p),
            values: DataTableState::hive_partition_values(p, first),
            skipped,
        };
        let files = Arc::new(files);
        let key = p.to_string_lossy().into_owned();
        // Up to a wave the footers cost one round of reads either way, so the dataset
        // opens whole and nothing is worth remembering.
        let wave = files.len() > crate::schema_union::FOOTERS_AT_ONCE;
        let stat_began = std::time::Instant::now();
        let stats = if wave {
            DataTableState::stat_files(&files)
        } else {
            Vec::new()
        };
        let stat_took = stat_began.elapsed();
        // None when a file went between the listing and its stat: that listing
        // describes nothing worth keeping.
        let sizes: Option<Vec<u64>> = stats.iter().map(|s| s.map(|(size, _)| size)).collect();
        let fingerprint = wave
            .then(|| {
                let stats: Vec<(u64, u64)> = stats.iter().copied().collect::<Option<_>>()?;
                let paths: Vec<String> = files
                    .iter()
                    .map(|f| f.to_string_lossy().into_owned())
                    .collect();
                Some(crate::cache::DatasetShape::fingerprint_of(
                    paths
                        .iter()
                        .zip(&stats)
                        .map(|(path, (size, modified))| (path.as_str(), *size, *modified, None)),
                ))
            })
            .flatten();
        let remembered = fingerprint
            .as_ref()
            .zip(report.remembered.as_ref())
            .and_then(|(fingerprint, cache)| cache.dataset_shape(&key, fingerprint))
            .zip(sizes.as_ref())
            .and_then(|(shape, sizes)| {
                crate::schema_union::footers_from_cache(&shape.files, &shape.schemas, sizes)
            });
        let from_cache = remembered.is_some();
        let staged = !from_cache && wave;
        let read = if from_cache {
            (0..files.len()).collect()
        } else if staged {
            crate::schema_union::ends_of(files.len())
        } else {
            crate::schema_union::footers_to_read(files.len())
        };
        let footers = match remembered {
            Some(footers) => footers,
            None => {
                DataTableState::read_local_footers(&files, &read, &report.progress, &report.meter)
            }
        };
        if !from_cache {
            remember_local_shape(
                report.remembered.as_ref(),
                &key,
                fingerprint.as_deref(),
                files.len(),
                &read,
                &footers,
            );
        }
        log::debug!(
            target: "datui",
            "local hive: {} files, stat in {stat_took:.1?}, {} footers {}",
            files.len(),
            read.len(),
            if from_cache {
                "from the shape cache"
            } else if staged {
                "read, the rest behind"
            } else {
                "read"
            }
        );
        let opened = hive.dataset(&files, &read, &footers)?;
        let state = DataTableState::from_schema_and_lazyframe(
            opened.dataset.schema.clone(),
            opened.lf,
            options,
            Some(hive.partition_columns.clone()),
        )
        .ok()?;
        let mut facts = OpenFacts {
            remote_files: opened.by_file.map(Into::into),
            // The footers just read say how wide each column is, as the cloud object's
            // do: a binary column's width is known nowhere else.
            column_bytes: crate::schema_union::column_bytes_per_row(&footers),
            // The count is in the footers just read, so no pass reads them again for it.
            row_groups: opened.row_groups,
            dataset: Some(DatasetAtOpen {
                schema: opened.dataset,
                file_rows: opened.file_rows,
                files: opened.paths,
            }),
            ..Default::default()
        };
        if staged {
            // The meter the open writes into: what the footers cost is both passes.
            let (meter, remembered) = (report.meter.clone(), report.remembered.clone());
            let ends = read;
            facts.footers_pending = Some(Arc::new(move |progress: &Arc<_>| {
                let read = crate::schema_union::footers_to_read(files.len());
                // The ends were read by the open; a footer is read once.
                let rest: Vec<usize> = read.iter().copied().filter(|i| !ends.contains(i)).collect();
                let mut fresh =
                    DataTableState::read_local_footers(&files, &rest, progress, &meter).into_iter();
                if progress.is_cancelled() {
                    return None;
                }
                let footers: Vec<Option<_>> = read
                    .iter()
                    .map(|i| match ends.iter().position(|e| e == i) {
                        Some(at) => footers[at].clone(),
                        None => fresh.next().flatten(),
                    })
                    .collect();
                remember_local_shape(
                    remembered.as_ref(),
                    &key,
                    fingerprint.as_deref(),
                    files.len(),
                    &read,
                    &footers,
                );
                let whole = hive.dataset(&files, &read, &footers)?;
                Some(crate::widgets::datatable::FootersFound {
                    dataset: whole.dataset,
                    lf: whole.lf,
                    file_rows: whole.file_rows,
                    files: whole.paths,
                    row_groups: whole.row_groups,
                    remote: whole.by_file,
                })
            }));
        }
        Some((state, facts))
    }

    /// The same one-file trick against an object store. This is the route that used to
    /// block the UI thread on a network round trip.
    #[cfg(feature = "cloud")]
    fn schema_state_from_cloud_hive(
        path: Option<&Path>,
        options: &OpenOptions,
        cloud: &crate::config::CloudConfig,
        runtime: &tokio::runtime::Handle,
        report: &crate::measurements::OpenReport,
    ) -> Option<(DataTableState, OpenFacts)> {
        // A prefix of Arrow files is read from its download (`cloud_arrow`).
        if !options.single_spine_schema || options.format == Some(FileFormat::Arrow) {
            return None;
        }
        // Unlike the local path this does not require --hive: a directory or glob URL
        // is already a hive scan by shape.
        let p = path.filter(|p| {
            let s = p.as_os_str().to_string_lossy();
            home::is_object_store_url(p) && (options.hive || source::is_prefix_or_glob(&s))
        })?;

        let (full, cloud_opts, store) = Self::cloud_store_for(p, cloud, runtime).ok()?;
        let (_bucket, key) = Self::cloud_bucket_and_key(&full).ok()?;
        Self::schema_state_from_cloud_hive_with(
            full, key, store, cloud_opts, options, runtime, report,
        )
    }

    /// The same, against a store already built.
    ///
    /// Split out so a test can hand it an in-memory store and cover the choice between
    /// the two routes below — including that each is given the counter it was called
    /// with, rather than one of its own.
    #[cfg(feature = "cloud")]
    fn schema_state_from_cloud_hive_with(
        full: String,
        key: String,
        store: Arc<dyn object_store::ObjectStore>,
        cloud_opts: CloudOptions,
        options: &OpenOptions,
        runtime: &tokio::runtime::Handle,
        report: &crate::measurements::OpenReport,
    ) -> Option<(DataTableState, OpenFacts)> {
        // Every file listed once, and the scan, the schema and the count all work from
        // that list — for a glob as much as for a prefix. datui expands the glob
        // itself: it lists the literal part of the key and matches the rest, so a glob
        // is an ordinary list of files by the time anything else sees it, and gets the
        // schema union, the row count, the notes and the measurements that a prefix
        // gets.
        //
        // The star cannot be handed to the object store. A listing prefix is a literal
        // string, so `data/*.parquet` matches nothing and the open falls through to a
        // whole-dataset scan with none of the above — which is what used to happen, for
        // every glob, silently (#228).
        let pattern = full.contains('*').then(|| {
            globset::GlobBuilder::new(&key)
                .literal_separator(true)
                .build()
                .map(|g| g.compile_matcher())
        });
        let pattern = match pattern {
            // A pattern datui cannot read is not one it should guess at.
            Some(Err(_)) => return None,
            Some(Ok(matcher)) => Some(matcher),
            None => None,
        };
        let listed = cloud_hive::prefix_of_glob(&key).to_string();
        Self::schema_state_from_cloud_files(
            CloudTarget {
                full: &full,
                key: listed,
                pattern: pattern.as_ref(),
            },
            store,
            cloud_opts,
            options,
            runtime,
            report,
        )
    }

    /// A cloud prefix of Parquet files as one dataset, from a single listing of it.
    ///
    /// The files are scanned by name, so Polars does not list the prefix again, and
    /// leniently (see `cloud_hive::lenient_scan`), since files written years apart
    /// differ. The state keeps the list, so the count reads footers rather than data
    /// and a buffer reads only the files holding its rows (see `RemoteFiles`).
    #[cfg(feature = "cloud")]
    fn schema_state_from_cloud_files(
        target: CloudTarget<'_>,
        store: Arc<dyn object_store::ObjectStore>,
        cloud_opts: CloudOptions,
        options: &OpenOptions,
        runtime: &tokio::runtime::Handle,
        report: &crate::measurements::OpenReport,
    ) -> Option<(DataTableState, OpenFacts)> {
        let CloudTarget { full, key, pattern } = target;
        // Kept before the listing takes ownership of it: this is the prefix that was
        // listed, and the notes measure every file's path against it.
        let root = cloud_hive::url_of_key(full, &key).unwrap_or_else(|| full.to_string());
        let (files, skipped) = {
            let store = store.clone();
            // The listing is one `list` whose pages object_store turns over itself, so
            // this brackets the whole of it: the first request to the last page. The
            // request count is not datui's to give — the paging happens inside the
            // store — so the listing reports a time and the data files it found, and
            // leaves requests and bytes to the footer pass, which does issue its own.
            // The count is after the filtering: what is reported is the dataset's
            // files, not every object under the prefix.
            let listing_began = std::time::Instant::now();
            let pattern = pattern.cloned();
            let (files, skipped) = wait_on_runtime(runtime, async move {
                cloud_hive::list_dataset_files(&store, &key, pattern.as_ref()).await
            })?
            .ok()?;
            report
                .meter
                .listed(listing_began.elapsed(), Some(files.len()), false);
            (Arc::new(files), skipped)
        };
        // Past one wave of concurrent reads the footers stop being free: the two ends
        // open the dataset and the rest are read behind it, joining when they land. Up
        // to a wave they cost one round trip either way, so the dataset opens whole —
        // rows numbered, absent cells marked, notes complete.
        // What the listing says this dataset is now. Taking it costs nothing — the
        // listing has already happened, and it is the only thing that has to — and it
        // is what decides whether the footers can be skipped entirely.
        let fingerprint = crate::cache::DatasetShape::fingerprint_of(
            files
                .iter()
                .map(|f| (f.key.as_str(), f.size, f.stamp, f.etag.as_deref())),
        );
        let remembered = report
            .remembered
            .as_ref()
            .and_then(|cache| cache.dataset_shape(full, &fingerprint))
            // No length check: the fingerprint leads with the file count, so a listing
            // of a different size cannot match one in the first place.
            .and_then(|shape| cloud_hive::footers_from_cache(&shape.files, &shape.schemas));

        let staged = remembered.is_none() && files.len() > cloud_hive::FOOTERS_AT_ONCE;
        let read = if remembered.is_some() {
            // Every file, because the cache holds every file: a remembered dataset
            // opens whole, with its rows numbered and its notes complete, however large
            // it is. That is the point of remembering it.
            (0..files.len()).collect()
        } else if staged {
            crate::schema_union::ends_of(files.len())
        } else {
            crate::schema_union::footers_to_read(files.len())
        };
        let footers = match remembered {
            Some(cached) => cached,
            None => Self::cloud_footers(
                store.clone(),
                files.clone(),
                read.clone(),
                runtime,
                report.progress.clone(),
                report.meter.clone(),
            )?,
        };
        Self::remember_dataset_shape(
            report.remembered.as_ref(),
            full,
            &fingerprint,
            &read,
            &files,
            &footers,
        );
        let opened =
            Self::cloud_dataset_from_footers(full, &root, &files, &read, &footers, &cloud_opts)?;
        let CloudDataset {
            dataset,
            file_rows,
            urls,
            row_groups,
            scan,
            partition_columns,
        } = opened;
        let schema = dataset.schema.clone();
        // The objects that will open. One whose footer would not read is one Polars
        // cannot read either, and left in the scan it takes the whole prefix down with
        // it on the first page.
        //
        // A staged open can only leave out what it has read: two footers, so an object
        // that will not parse anywhere but the two ends is in this scan and the first
        // page fails on it. That is a window, not a lost guarantee — the pass behind
        // the open finds it and the join swaps in a scan without it — but for a directory
        // with a file mid-write, a prefix over sixty-four objects shows an error where
        // a smaller one shows rows.
        let readable = crate::schema_union::readable_paths(&urls, &dataset.unreadable);
        // Everything downstream describes the same list or none of it. The counter
        // returns one entry per object it is given and `OpenFacts::row_groups` wants one
        // per url, so a counter over the full listing beside a shorter url list is not
        // a wrong count, it is no count at all: the lengths disagree, the answer is
        // dropped without a word, and the dataset spends the rest of the session
        // re-counting itself and never reaching an end to jump to.
        let counted: Vec<cloud_hive::DatasetFile> = files
            .iter()
            .enumerate()
            // Searched rather than scanned, for the same reason `readable_paths` does:
            // a prefix can be hundreds of thousands of objects.
            .filter(|(index, _)| dataset.unreadable.binary_search(index).is_err())
            .map(|(_, file)| file.clone())
            .collect();
        // Belt and braces, both of them: a prefix with nothing readable has no schema
        // and was handed back above, and the two lists are filtered from the same
        // indices so they cannot come out different lengths. Kept because the cost of
        // the invariant quietly breaking is a dataset that counts itself forever and
        // never finds its end, which is not a thing to leave to a comment.
        if readable.is_empty() || readable.len() != counted.len() {
            return None;
        }
        let count: crate::widgets::datatable::FileCounter = {
            let (runtime, counted, store) = (runtime.clone(), Arc::new(counted), store.clone());
            // The same meter again: this counts by re-reading every footer, so its
            // requests are footer requests and belong in the same tally.
            let meter = report.meter.clone();
            Arc::new(move || {
                let (store, counted, meter) = (store.clone(), counted.clone(), meter.clone());
                wait_on_runtime(&runtime, async move {
                    cloud_hive::row_groups_of_files(&store, &counted, &meter).await
                })
                .ok_or_else(|| "cancelled".to_string())?
                .map_err(|e| e.to_string())
            })
        };
        let lf = scan(&readable, &[]).ok()?;
        let state =
            DataTableState::from_schema_and_lazyframe(schema, lf, options, Some(partition_columns))
                .ok()?;
        let mut facts = OpenFacts {
            remote_files: Some(crate::widgets::datatable::RemoteFiles {
                urls: Arc::new(readable.into_owned()),
                scan,
                count,
                offsets: None,
            }),
            // The listing's sizes: what a full scan's local copy would fetch, known
            // before it fetches anything.
            remote_objects: files
                .iter()
                .filter_map(|file| {
                    Some(crate::local_copy::RemoteObject {
                        url: cloud_hive::url_of_key(full, &file.key)?,
                        size: file.size,
                        etag: file.etag.clone(),
                    })
                })
                .collect(),
            // The footers just read hold the count too, so the dataset opens counted —
            // but `cloud_dataset_from_footers` gives row groups only when every file was
            // read and every footer parsed. A footer sampled past or failed would count
            // as no rows, which both undercounts the dataset and puts that file's rows
            // out of reach of a windowed scan; leaving the count to `RemoteFiles::count`
            // means it is retried instead.
            row_groups,
            dataset: Some(DatasetAtOpen {
                schema: dataset.with_skipped(skipped),
                file_rows,
                files: urls,
            }),
            ..Default::default()
        };
        if staged {
            // Everything the pass behind the open needs, held as one closure the way
            // the scan and the counter are: the store and the listing it already has,
            // so it neither lists the prefix again nor has to be told what it is
            // reading.
            let (store, cloud_opts, runtime) = (store.clone(), cloud_opts.clone(), runtime.clone());
            let (files, full) = (files.clone(), full.to_string());
            // The same meter the open is writing into, not a new one: this pass reads
            // the dataset's footers over again — including the two ends the open
            // already read, since the whole dataset is built from one set of them, and
            // a sample of them past `MAX_FOOTER_READS` — and what the footers cost is
            // both passes added up, re-reads and all. It is held rather than handed
            // in because it belongs to this dataset: the next open builds its own state
            // and its own meter, and this closure goes with the state it was built for.
            let meter = report.meter.clone();
            // This is the pass that reads a large dataset's footers, so this is where a
            // large dataset gets remembered. The open above it has read two and has
            // nothing worth keeping; leaving the saving there meant the cache only ever
            // held datasets small enough to open in one wave — the ones that cost least
            // to read in the first place.
            let remembered = report.remembered.clone();
            let fingerprint = fingerprint.clone();
            facts.footers_pending = Some(Arc::new(move |progress: &Arc<_>| {
                let read = crate::schema_union::footers_to_read(files.len());
                let footers = Self::cloud_footers(
                    store.clone(),
                    files.clone(),
                    read.clone(),
                    &runtime,
                    progress.clone(),
                    meter.clone(),
                )?;
                Self::remember_dataset_shape(
                    remembered.as_ref(),
                    &full,
                    &fingerprint,
                    &read,
                    &files,
                    &footers,
                );
                let whole = Self::cloud_dataset_from_footers(
                    &full,
                    &root,
                    &files,
                    &read,
                    &footers,
                    &cloud_opts,
                )?;
                // The same exclusion the open makes: a footer that would not read on
                // this pass either is a file Polars cannot read, and scanning it takes
                // the prefix down. This pass can find one the open could not — it only
                // read two footers — so the exclusion belongs on both sides.
                let readable =
                    crate::schema_union::readable_paths(&whole.urls, &whole.dataset.unreadable)
                        .into_owned();
                let lf = (whole.scan)(&readable, &[]).ok()?;
                // Over the same files, so the count it answers with fits the list the
                // dataset is about to hold.
                let counted: Vec<cloud_hive::DatasetFile> = files
                    .iter()
                    .enumerate()
                    .filter(|(index, _)| whole.dataset.unreadable.binary_search(index).is_err())
                    .map(|(_, file)| file.clone())
                    .collect();
                if counted.len() != readable.len() {
                    return None;
                }
                let count: crate::widgets::datatable::FileCounter = {
                    let (runtime, counted, store) =
                        (runtime.clone(), Arc::new(counted), store.clone());
                    // As above: counting re-reads every footer, and those reads count.
                    let meter = meter.clone();
                    Arc::new(move || {
                        let (store, counted, meter) =
                            (store.clone(), counted.clone(), meter.clone());
                        wait_on_runtime(&runtime, async move {
                            cloud_hive::row_groups_of_files(&store, &counted, &meter).await
                        })
                        .ok_or_else(|| "cancelled".to_string())?
                        .map_err(|e| e.to_string())
                    })
                };
                Some(crate::widgets::datatable::FootersFound {
                    // What the listing passed over travels with the pass, or the note
                    // about it is on screen from the open and gone the moment the
                    // columns join — which on a prefix of more than a wave of files is
                    // every prefix there is.
                    dataset: whole.dataset.with_skipped(skipped),
                    lf,
                    file_rows: whole.file_rows,
                    // Every file listed: the dataset's per-file findings index this.
                    files: whole.urls,
                    row_groups: whole.row_groups,
                    remote: Some(crate::widgets::datatable::RemoteRead {
                        urls: readable,
                        scan: whole.scan,
                        count,
                    }),
                })
            }));
        }
        Some((state, facts))
    }

    /// The footers at `read`, fetched on the runtime. `None` if the open was abandoned.
    ///
    /// Everything is cloned into the future rather than borrowed: it outlives this
    /// frame, and the counter is shared with whoever is rendering anyway.
    #[cfg(feature = "cloud")]
    fn cloud_footers(
        store: Arc<dyn object_store::ObjectStore>,
        files: Arc<Vec<cloud_hive::DatasetFile>>,
        read: Vec<usize>,
        runtime: &tokio::runtime::Handle,
        progress: Arc<crate::schema_union::FooterProgress>,
        meter: Arc<crate::measurements::Meter>,
    ) -> Option<Vec<Option<cloud_hive::FileFooter>>> {
        wait_on_runtime(runtime, async move {
            cloud_hive::footers_of_files_reporting(&store, &files, &read, &progress, &meter).await
        })
    }

    /// What a set of a cloud dataset's footers says, and the scan that reads it.
    ///
    /// Shared by the open, which has read the two ends, and the pass behind it, which
    /// has read them all: the two differ only in how much they know, and a dataset
    /// built from a sample already says so — it forgoes numbering its rows and scopes
    /// its notes to the footers it saw.
    #[cfg(feature = "cloud")]
    /// Keep what this pass learned, if it learned the whole of it.
    ///
    /// Two conditions, and both matter.
    ///
    /// Every footer must have been read. A staged open has read two of them and a
    /// sampled one a spread, and either kept as though it were the whole dataset would
    /// hand the next open a smaller dataset than it asked for, with nothing to say that
    /// is what happened.
    ///
    /// Every footer must have *parsed*. A footer read can fail because the file is
    /// corrupt, and it can fail because the store throttled the request or a token
    /// expired — and nothing here can tell those apart. Remembering the failure turns a
    /// moment's trouble into a file that is missing from the dataset on every open from
    /// now until something else in the prefix changes, which is not a trade a cache is
    /// allowed to make. Read them again next time; the one that was really corrupt
    /// costs a read and says the same thing.
    fn remember_dataset_shape(
        cache: Option<&crate::cache::CacheManager>,
        full: &str,
        fingerprint: &str,
        read: &[usize],
        files: &[cloud_hive::DatasetFile],
        footers: &[Option<cloud_hive::FileFooter>],
    ) {
        let Some(cache) = cache else {
            return;
        };
        // The dataset index too, which is what the home screen reads. A dataset opened
        // straight from a bucket used to be recorded here, by the URL it was opened as,
        // and nowhere else — so its recent row showed no shape, no size and no label,
        // and looked broken beside the local rows. Written before the shape, because a
        // sampled read still says what the columns are, and the shape below wants
        // every footer. A sampled read does not replace a whole one, though: the shape
        // cache is the smaller of the two and forgets a dataset long before the index
        // does, and a reopen that finds its shape gone reads a sample first.
        if let Some((path, facts)) = Self::facts_from_cloud_footers(full, files, read, footers) {
            let existing = cache.load_dataset_facts();
            if Self::facts_worth_recording(existing.get(&path), &facts) {
                cache.record_dataset_facts(&[(path, facts)]);
            }
        }
        if read.len() != files.len() || !footers.iter().all(Option::is_some) {
            return;
        }
        let (cached, schemas) = cloud_hive::footers_to_cache(footers);
        cache.save_dataset_shape(
            full,
            crate::cache::DatasetShape {
                fingerprint: fingerprint.to_string(),
                files: cached,
                schemas,
                taken_at: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or_default(),
            },
        );
    }

    /// What the home screen can say about a cloud dataset from the footers an open
    /// read: its columns, its rows when every footer was read, its kind, and what it
    /// holds. Keyed by the URL as opened, which is what the recents store holds.
    ///
    /// A remote row has no fingerprint to check, so `mtime` is the newest object's
    /// stamp and `size` the total, for the record's own sake.
    #[cfg(feature = "cloud")]
    fn facts_from_cloud_footers(
        full: &str,
        files: &[cloud_hive::DatasetFile],
        read: &[usize],
        footers: &[Option<cloud_hive::FileFooter>],
    ) -> Option<(PathBuf, crate::cache::DatasetFacts)> {
        let (dataset, partition_columns) =
            cloud_hive::dataset_schema_from_footers(files, read, footers).ok()?;
        let columns: Vec<String> = dataset
            .schema
            .iter_names()
            .map(|name| name.to_string())
            .collect();
        let every_footer = read.len() == files.len() && footers.iter().all(Option::is_some);
        let rows = every_footer.then(|| {
            footers
                .iter()
                .flatten()
                .map(|f| f.row_group_rows.iter().sum::<usize>())
                .sum()
        });
        // A directory of files, unless the listing came back with the one object the URL
        // names — which is what `--hive` on a single object gets. The trailing slash is
        // not asked about: this route is entered for `--hive s3://bucket/sales` too.
        let directory = !(files.len() == 1 && full.trim_end_matches('/').ends_with(&files[0].key));
        let kind = if !directory {
            discover::EntryKind::File
        } else if !partition_columns.is_empty() {
            discover::EntryKind::Hive
        } else {
            discover::EntryKind::MultiFile
        };
        let holds = if directory {
            discover::Holds {
                formats: vec![("parquet".to_string(), files.len())],
                ..Default::default()
            }
        } else {
            Default::default()
        };
        Some((
            PathBuf::from(full),
            crate::cache::DatasetFacts {
                mtime: files.iter().map(|f| f.stamp).max().unwrap_or_default(),
                size: files.iter().map(|f| f.size).sum(),
                rows,
                cols: Some(columns.len()),
                cols_sampled: !every_footer,
                columns,
                kind: Some(kind),
                classified_by: discover::CLASSIFIER_VERSION,
                cost: Default::default(),
                holds,
            },
        ))
    }

    /// Whether a record learned from a cloud open should replace what the index has:
    /// anything replaces nothing, a whole read replaces anything, and a sampled read
    /// replaces only another sample.
    #[cfg(feature = "cloud")]
    fn facts_worth_recording(
        existing: Option<&crate::cache::DatasetFacts>,
        new: &crate::cache::DatasetFacts,
    ) -> bool {
        match existing {
            None => true,
            Some(_) if new.rows.is_some() => true,
            Some(old) => old.rows.is_none(),
        }
    }

    /// What the home screen can say about one object opened from a bucket: its rows
    /// and columns from the footer the open read, under the URL it resolved to. The
    /// object's size is not known here — the footer is read from the tail — so the
    /// record carries none, and the row shows none. Its `mtime` is the time of the
    /// open: a remote record is never fingerprinted by it, and the index evicts its
    /// oldest `mtime` first, so a zero would make these the first to go.
    #[cfg(feature = "cloud")]
    fn record_cloud_object_facts(
        cache: Option<&crate::cache::CacheManager>,
        full: &str,
        footer: &cloud_hive::ParquetFooter,
    ) {
        let Some(cache) = cache else {
            return;
        };
        let columns: Vec<String> = footer
            .schema
            .iter_names()
            .map(|name| name.to_string())
            .collect();
        cache.record_dataset_facts(&[(
            PathBuf::from(full),
            crate::cache::DatasetFacts {
                mtime: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or_default(),
                size: 0,
                rows: Some(footer.row_group_rows.iter().sum()),
                cols: Some(columns.len()),
                cols_sampled: false,
                columns,
                kind: Some(discover::EntryKind::File),
                classified_by: discover::CLASSIFIER_VERSION,
                cost: discover::Cost {
                    row_groups: Some(footer.row_group_rows.len()),
                    ..Default::default()
                },
                holds: Default::default(),
            },
        )]);
    }

    #[cfg(feature = "cloud")]
    fn cloud_dataset_from_footers(
        full: &str,
        // The literal part of `full`, which for a glob is everything before its star.
        // The layout and column-range notes work by taking each file's path relative to
        // the dataset's root, so a root with a star in it is a prefix of nothing and
        // every note goes quietly empty.
        root: &str,
        files: &[cloud_hive::DatasetFile],
        read: &[usize],
        footers: &[Option<cloud_hive::FileFooter>],
        cloud_opts: &CloudOptions,
    ) -> Option<CloudDataset> {
        let (dataset, partition_columns) =
            cloud_hive::dataset_schema_from_footers(files, read, footers).ok()?;
        let urls: Vec<String> = files
            .iter()
            .filter_map(|f| cloud_hive::url_of_key(full, &f.key))
            .collect();
        if urls.is_empty() || urls.len() != files.len() {
            return None;
        }
        // A file that stores a column in a type the dataset's column cannot hold is not
        // read for it; its rows are null there rather than failing the scan, and carry
        // their file's drift group so the null can be told from a real one.
        let file_rows: Vec<usize> = if read.len() == files.len() {
            footers
                .iter()
                .map(|f| f.as_ref().map(|f| f.row_group_rows.iter().sum()))
                .collect::<Option<Vec<_>>>()
                .unwrap_or_default()
        } else {
            Vec::new()
        };
        let row_groups: Vec<Vec<usize>> =
            if read.len() == files.len() && footers.iter().all(Option::is_some) {
                footers
                    .iter()
                    .flatten()
                    .map(|f| f.row_group_rows.clone())
                    .collect()
            } else {
                Vec::new()
            };
        let drift = crate::schema_union::ScanDrift::new(&urls, &dataset, &file_rows);
        let schema = dataset.schema.clone();
        let scan: crate::widgets::datatable::FileScan = {
            let (schema, partition_columns, drift, cloud_opts) = (
                schema.clone(),
                partition_columns.clone(),
                drift.map(Arc::new),
                cloud_opts.clone(),
            );
            Arc::new(
                move |urls: &[String], as_text: &[polars::prelude::PlSmallStr]| {
                    let drifts = drift.is_some();
                    cloud_hive::lenient_scan(
                        urls,
                        schema.clone(),
                        Some(cloud_opts.clone()),
                        drift.as_deref(),
                        as_text,
                    )
                    .map(|lf| {
                        Self::hoist_partition_columns(lf, &schema, &partition_columns, drifts)
                    })
                },
            )
        };
        Some(CloudDataset {
            dataset: dataset.with_partition_layouts(root, &urls),
            file_rows,
            urls,
            row_groups,
            scan,
            partition_columns,
        })
    }

    /// General schema route: ask the frame itself. Slow for a wide hive dataset, which
    /// is the reason this whole phase belongs on a background thread.
    fn schema_state_from_full_scan(
        mut lf: LazyFrame,
        path: Option<&Path>,
        options: &OpenOptions,
    ) -> Result<DataTableState> {
        let schema = lf
            .collect_schema()
            .map_err(color_eyre::eyre::Report::from)?;
        let partition_columns =
            match path.filter(|p| options.hive && (p.is_dir() || source::expands_as_glob(p))) {
                Some(p) => DataTableState::discover_hive_partition_columns(p)
                    .into_iter()
                    .filter(|c| schema.contains(c.as_str()))
                    .collect::<Vec<_>>(),
                None => Vec::new(),
            };
        let lf = Self::hoist_partition_columns(lf, &schema, &partition_columns, false);
        let part_cols = (!partition_columns.is_empty()).then_some(partition_columns);
        DataTableState::from_schema_and_lazyframe(schema, lf, options, part_cols)
    }

    /// Build the table state for a loaded frame, by the cheapest route that applies.
    ///
    /// Returns the state and a label naming the route it came from, for the debug
    /// overlay. Takes its config by value so all of it can run off the UI thread.
    fn build_schema_state(
        lf: LazyFrame,
        path: Option<&Path>,
        options: &OpenOptions,
        cloud: &crate::config::CloudConfig,
        runtime: &tokio::runtime::Handle,
        report: &crate::measurements::OpenReport,
    ) -> Result<(DataTableState, OpenFacts, String)> {
        // The facts carry the meter of the route that actually built the dataset, so it
        // is installed with the dataset and nothing else can reach it. An open that
        // fails never gets here, which is what keeps the dataset still on screen
        // showing its own figures.
        let (state, mut facts, label) =
            Self::schema_state_by_route(lf, path, options, cloud, runtime, report)?;
        // What the open did, as against what it found. The one place both are known:
        // the scan has reported what it passed over, the caller has said whether this
        // is a lake table's plain files, and the state that will carry the notes is in
        // hand. See `DataTableState::open_notes` for why they are not the other notes.
        // A delimited file read with a header whose names are all numbers: its first
        // row of data, most likely, which `H` reads as data instead.
        let names_look_like_data = options.format.and_then(FileFormat::separator).is_some()
            && options.has_header != Some(false)
            && !crate::schema_union::names_are_names(
                &state
                    .schema()
                    .iter_names()
                    .map(|n| n.to_string())
                    .collect::<Vec<_>>(),
            );
        facts.open_notes = crate::notes::from_the_open(
            &options.left_out,
            options.read_as_plain_files_of,
            options.files_disagree,
            names_look_like_data,
        );
        // And the half of it that cannot be missed: the row count on screen is a true
        // count of the files and a wrong one of the table.
        facts.not_the_table = options.read_as_plain_files_of;
        if let Some(splits) = &options.splits {
            facts.other_tables = splits.others.clone();
            facts
                .open_notes
                .extend(crate::notes::map_caches(splits.caches));
        }
        if let Some(read) = &options.format_read {
            facts.open_notes.extend(read.notes());
            facts.format_read = Some(read.clone());
        }
        if let Some(opened) = &options.opened {
            facts.records = opened.window.clone();
            facts.detail = opened.detail.clone();
            facts.other_tables = opened.other_tables.clone();
            facts.open_notes.extend(opened.notes.iter().cloned());
            facts.units = opened.units.clone();
        }
        if let Some(sqlite) = &options.sqlite {
            facts.pushdown = Some(sqlite.pushdown.clone());
            facts.hold = sqlite.hold.lock().ok().and_then(|mut hold| hold.take());
            facts.other_tables = sqlite.other_tables.clone();
        }
        if let Some(read) = &options.delimited {
            facts.open_notes.extend(read.notes());
            facts.delimited = Some(read.clone());
        }
        facts.read_mode = options.read_mode;
        facts.read_as = options.format;
        // The display path of a downloaded object is its URL too; only a scan that
        // really reads the object store in place buffers like one.
        // Arrow in a store reads its IPC files in place, and its streams from their
        // download (`cloud_arrow`).
        facts.remote_source = match &options.arrow_parts {
            Some(parts) => parts.iter().any(|part| {
                matches!(part, crate::ipc_stream::Part::InPlace(p) if source::is_remote_url(p))
            }),
            None => path.is_some_and(source::scans_in_place),
        };
        // The cheap footer-sum row count, for a local Parquet hive directory. Asked
        // here because a stat on a mount that has stopped answering hangs its thread.
        // A directory read as another format counts its rows by a scan: its footers
        // are not Parquet's.
        if options.hive
            && options.format.is_none_or(|f| f == FileFormat::Parquet)
            && let Some(dir) = path.filter(|p| !source::is_remote_url(p) && p.is_dir())
        {
            facts.parquet_count_dir = Some(dir.to_path_buf());
        }
        Ok((state, facts, label))
    }

    /// Scan a prefix in an object store with the reader its format calls for.
    ///
    /// Every cloud path went to `scan_parquet` whatever was under it, so a prefix of
    /// CSV came back "Could not read from S3. Check credentials and URL" — a false
    /// statement about the user's login, made about a directory datui could see the
    /// contents of. Polars' other scans take the same `CloudOptions` and do their own
    /// listing; nothing was passing them.
    ///
    /// Parquet keeps its own branch at each call site: it is the only one with hive
    /// partitioning, which is a Parquet-only capability in this reader, and it is the
    /// path every cloud dataset took before this existed.
    ///
    /// `None` when the format is not one of these, which sends the caller back to the
    /// Parquet scan it always made.
    #[cfg(feature = "cloud")]
    fn scan_cloud_prefix(
        url: &str,
        cloud_opts: CloudOptions,
        format: FileFormat,
        glob: bool,
        options: &OpenOptions,
    ) -> Option<Result<LazyFrame>> {
        // The formats the docs say a prefix reads in place. Parquet takes the caller's
        // own scan, and a prefix of model files is read by its headers before this.
        if !format.reads_bucket_prefix() {
            return None;
        }
        // A plain prefix is narrowed to the keys with an extension. A console's folder
        // marker comes back from the listing as `data` for `data/`, which Polars reads
        // as a file of a different kind from the rest and refuses the whole prefix.
        let pl_path = if url.ends_with('/') && !url.contains('*') {
            PlRefPath::new(format!("{url}**/*.*").as_str())
        } else {
            PlRefPath::new(url)
        };
        let scan = crate::readers::of(format).bucket_scan?;
        Some(scan(crate::readers::BucketIn {
            url,
            path: pl_path,
            cloud: cloud_opts,
            glob,
            options,
            format,
        }))
    }

    /// The format a prefix or glob in a store is read as, other than Parquet: what
    /// the listing said, else what a glob's names end in (`*.arrow`).
    #[cfg(feature = "cloud")]
    fn cloud_glob_format(url: &str, options: &OpenOptions) -> Option<FileFormat> {
        options
            .format
            .or_else(|| {
                url.contains('*')
                    .then(|| FileFormat::from_path(Path::new(url)))
                    .flatten()
            })
            .filter(|f| *f != FileFormat::Parquet)
    }

    /// The plain URL and Polars options for one object-store path, through the source
    /// it names or belongs to (`cloud_sources::resolve`).
    #[cfg(feature = "cloud")]
    fn resolve_cloud_url(
        path: &Path,
        cloud: &crate::config::CloudConfig,
    ) -> Result<(String, CloudOptions)> {
        let text = path.to_string_lossy();
        let resolved = crate::cloud_sources::resolve_for_open(&text, cloud)
            .map_err(|e| color_eyre::eyre::eyre!(e))?;
        let options = match resolved.kind {
            crate::cloud_browse::ProviderKind::S3 => Self::build_s3_cloud_options(&resolved.s3),
            crate::cloud_browse::ProviderKind::Gcs
                if resolved.signing == crate::cloud_sources::Signing::Unsigned =>
            {
                CloudOptions::default()
                    .with_gcp([(polars::io::cloud::GoogleConfigKey::SkipSignature, "true")])
            }
            crate::cloud_browse::ProviderKind::Gcs => match &resolved.gcloud {
                // The token comes from `gcloud` whenever Polars asks, so a long scan
                // outlives the one fetched here.
                Some((configuration, _)) => CloudOptions::default()
                    .with_credential_provider(Some(crate::gcloud::polars_provider(configuration))),
                None => match &resolved.google_credentials {
                    Some(file) => CloudOptions::default().with_gcp([(
                        polars::io::cloud::GoogleConfigKey::ApplicationCredentials,
                        file.to_string_lossy().into_owned(),
                    )]),
                    None => CloudOptions::default(),
                },
            },
            crate::cloud_browse::ProviderKind::Azure => {
                let (account, _, _) = source::azure_parts(&resolved.url)
                    .ok_or_else(|| color_eyre::eyre::eyre!("not an Azure URL"))?;
                CloudOptions::default()
                    .with_azure(crate::azure::polars_options(&account, &resolved.azure))
            }
        };
        Ok((resolved.url, options))
    }

    /// The URL, Polars options and store for one object-store path. `cloud` is the
    /// effective config the `App` keeps (see `OpenOptions::effective_cloud`).
    #[cfg(feature = "cloud")]
    fn cloud_store_for(
        path: &Path,
        cloud: &crate::config::CloudConfig,
        runtime: &tokio::runtime::Handle,
    ) -> Result<(String, CloudOptions, Arc<dyn object_store::ObjectStore>)> {
        let (full, cloud_opts) = Self::resolve_cloud_url(path, cloud)?;
        let store = Self::polars_object_store(&full, &cloud_opts, runtime)?;
        Ok((full, cloud_opts, store))
    }

    /// One Parquet object read in place: schema and row count from its footer, in one
    /// tail read through one store.
    ///
    /// Asking the frame for its schema fetched the footer through Polars, and Polars
    /// answers `len()` on a cloud scan by reading the first row group rather than the
    /// footer, so the background count that followed an open downloaded row group 0 a
    /// second time, alongside the buffer that was showing it. The footer has both
    /// answers for one 256 KiB range request; the schema is handed to the scan so
    /// Polars does not fetch it again, and the count is known before the first frame.
    /// A failure is returned, not swallowed: the caller falls back to asking the frame
    /// and puts the reason in the debug label.
    #[cfg(feature = "cloud")]
    fn schema_state_from_cloud_object(
        path: &Path,
        options: &OpenOptions,
        cloud: &crate::config::CloudConfig,
        runtime: &tokio::runtime::Handle,
        report: &crate::measurements::OpenReport,
    ) -> Result<(DataTableState, OpenFacts)> {
        let (full, cloud_opts, store) = Self::cloud_store_for(path, cloud, runtime)?;
        let (_bucket, key) = Self::cloud_bucket_and_key(&full)?;
        if key.is_empty() {
            return Err(color_eyre::eyre::eyre!("a bucket, not an object"));
        }
        let meter = report.meter.clone();
        let footer = wait_on_runtime(runtime, async move {
            cloud_hive::footer_of_cloud_parquet(store, &key, &meter).await
        })
        .ok_or_else(|| color_eyre::eyre::eyre!("cancelled"))??;
        let args = ScanArgsParquet {
            schema: Some(footer.schema.clone()),
            cloud_options: Some(cloud_opts),
            hive_options: polars::io::HiveOptions::default(),
            glob: false,
            ..Default::default()
        };
        let lf = LazyFrame::scan_parquet(PlRefPath::new(full.as_str()), args)?;
        let state =
            DataTableState::from_schema_and_lazyframe(footer.schema.clone(), lf, options, None)?;
        // The commonest cloud open, and the one the dataset index never heard about:
        // the prefix route records what it read, and this one read a footer too.
        Self::record_cloud_object_facts(report.remembered.as_ref(), &full, &footer);
        let facts = OpenFacts {
            row_groups: vec![footer.row_group_rows],
            remote_objects: footer
                .object_bytes
                .map(|size| crate::local_copy::RemoteObject {
                    url: full,
                    size,
                    etag: footer.object_etag,
                })
                .into_iter()
                .collect(),
            column_bytes: footer.column_bytes_per_row,
            ..Default::default()
        };
        Ok((state, facts))
    }

    /// The schema routes, cheapest first: one local footer, one cloud footer (a hive
    /// prefix or a single object), then asking the frame.
    fn schema_state_by_route(
        lf: LazyFrame,
        path: Option<&Path>,
        options: &OpenOptions,
        cloud: &crate::config::CloudConfig,
        runtime: &tokio::runtime::Handle,
        report: &crate::measurements::OpenReport,
    ) -> Result<(DataTableState, OpenFacts, String)> {
        #[cfg(not(feature = "cloud"))]
        let _ = (cloud, runtime);

        // A meter per attempt, and the winner's is the open's. The routes are tried in
        // order and the earlier ones measure before they discover they cannot finish —
        // the local hive route times its walk and its footer pass, then bails five
        // different ways. Sharing one meter would leave those figures on a dataset some
        // later route built, which is a row saying no files on a dataset that has them.
        //
        // Two guards, and the test holds them together rather than either alone: the
        // attempts take separate meters, and both full-scan arms hand back an empty
        // one. A directory whose only Parquet is a writer's own bookkeeping — a
        // `_delta_log` checkpoint — reaches the screen through the second of those, so
        // `test_a_route_that_gave_up_leaves_no_figures_on_the_dataset_that_opened`
        // fails when both are reverted and passes when either still stands. The
        // per-attempt meter alone is defensive: the route guards are mutually
        // exclusive enough that nothing reaches a later route through the first.
        let attempt = |report: &crate::measurements::OpenReport| crate::measurements::OpenReport {
            progress: report.progress.clone(),
            meter: Arc::new(crate::measurements::Meter::default()),
            remembered: report.remembered.clone(),
        };

        let local = attempt(report);
        if let Some((state, facts)) = Self::schema_state_from_local_hive(path, options, &local) {
            let facts = OpenFacts {
                measurements: local.meter,
                ..facts
            };
            return Ok((state, facts, "one-file (local)".to_string()));
        }
        #[cfg(feature = "cloud")]
        let cloud_hive_attempt = attempt(report);
        #[cfg(feature = "cloud")]
        if let Some((state, facts)) =
            Self::schema_state_from_cloud_hive(path, options, cloud, runtime, &cloud_hive_attempt)
        {
            let facts = OpenFacts {
                measurements: cloud_hive_attempt.meter,
                ..facts
            };
            return Ok((state, facts, "one-file (cloud)".to_string()));
        }
        #[cfg(feature = "cloud")]
        if let Some(p) = path.filter(|p| {
            source::scans_in_place(p)
                && !options.hive
                && !source::is_prefix_or_glob(&p.to_string_lossy())
        }) {
            let object = attempt(report);
            match Self::schema_state_from_cloud_object(p, options, cloud, runtime, &object) {
                Ok((state, facts)) => {
                    let facts = OpenFacts {
                        measurements: object.meter,
                        ..facts
                    };
                    return Ok((state, facts, "footer (cloud)".to_string()));
                }
                // Visible in the debug overlay, because the fallback costs a row group
                // for the count and that should not pass for the intended path.
                Err(e) => {
                    // A fresh meter, not the failed footer read's: the full scan
                    // measures nothing, and showing the attempt that did not work
                    // would describe a route the dataset did not come by.
                    return Self::schema_state_from_full_scan(lf, path, options).map(|state| {
                        (
                            state,
                            OpenFacts::default(),
                            format!("full scan (cloud footer: {e})"),
                        )
                    });
                }
            }
        }
        Self::schema_state_from_full_scan(lf, path, options)
            .map(|state| (state, OpenFacts::default(), "full scan".to_string()))
    }

    /// The files of one split, when `dir` is a Hugging Face `datasets` cache: its
    /// `dataset_info.json` or `state.json` beside Arrow files. What was chosen and left
    /// out goes in `report`. Any other directory reads every file.
    fn hugging_face_split(
        dir: &Path,
        format: FileFormat,
        files: Vec<PathBuf>,
        options: &OpenOptions,
        report: &mut ReadReport,
    ) -> Result<Vec<PathBuf>> {
        let metadata = || {
            ["dataset_info.json", "state.json"]
                .iter()
                .any(|name| dir.join(name).is_file())
        };
        if format != FileFormat::Arrow || !metadata() {
            return Ok(files);
        }
        let names: Vec<&str> = files
            .iter()
            .map(|f| f.file_name().and_then(|n| n.to_str()).unwrap_or_default())
            .collect();
        let (chosen, splits) = crate::hf_splits::choose(&names, options.table.as_deref())
            .map_err(|e| color_eyre::eyre::eyre!("{}: {e}", dir.display()))?;
        report.splits = Some(Arc::new(splits));
        Ok(chosen.into_iter().map(|i| files[i].clone()).collect())
    }

    /// Why `--table` was refused for a file of `format`, which holds one table.
    fn one_table(format: Option<FileFormat>) -> color_eyre::Report {
        color_eyre::eyre::eyre!(cli::one_table(format))
    }

    /// The inputs of an Arrow read as one table, in order: each IPC file scanned where
    /// it is, in a bucket or on disk, and each run of streams as its rows of
    /// `converted`, the IPC file they were converted to. Stacked as the files of a
    /// directory are ([`DataTableState::union_of_files`]).
    fn scan_arrow_parts(
        cloud: &crate::config::CloudConfig,
        converted: Option<&PathBuf>,
        parts: &[crate::ipc_stream::Part],
    ) -> Result<LazyFrame> {
        use crate::ipc_stream::Part;
        #[cfg(not(feature = "cloud"))]
        let _ = cloud;
        let scan = |path: &Path| -> Result<LazyFrame> {
            #[cfg(feature = "cloud")]
            if source::is_remote_url(path) {
                let (url, cloud_options) = Self::resolve_cloud_url(path, cloud)?;
                let args = polars::prelude::UnifiedScanArgs {
                    cloud_options: Some(cloud_options),
                    ..Default::default()
                };
                return Ok(LazyFrame::scan_ipc(
                    PlRefPath::new(url.as_str()),
                    Default::default(),
                    args,
                )?);
            }
            // A converted stream sits in a temp directory the user names, `[` and all,
            // and a file read in place may be called `d[1].arrow` (#632).
            let args = polars::prelude::UnifiedScanArgs {
                glob: source::expands_as_glob(path),
                ..Default::default()
            };
            Ok(LazyFrame::scan_ipc(
                polars::prelude::PlRefPath::try_from_path(path)?,
                Default::default(),
                args,
            )?)
        };
        let streams = |offset: u64, rows: u64| -> Result<LazyFrame> {
            let file = converted
                .ok_or_else(|| color_eyre::eyre::eyre!("No converted Arrow file to read."))?;
            let lf = scan(file)?;
            // The whole file needs no slice, which would hide its row count.
            let whole = offset == 0
                && parts
                    .iter()
                    .all(|part| matches!(part, Part::Converted { .. }));
            Ok(if whole {
                lf
            } else {
                lf.slice(offset as i64, rows as polars::prelude::IdxSize)
            })
        };
        let mut frames = Vec::new();
        let mut run: Option<(u64, u64)> = None;
        for part in parts {
            match part {
                Part::Converted { offset, rows, .. } => {
                    run = Some(match run {
                        Some((start, n)) if start + n == *offset => (start, n + rows),
                        Some((start, n)) => {
                            frames.push(streams(start, n)?);
                            (*offset, *rows)
                        }
                        None => (*offset, *rows),
                    });
                }
                Part::InPlace(path) => {
                    if let Some((start, n)) = run.take() {
                        frames.push(streams(start, n)?);
                    }
                    frames.push(scan(path)?);
                }
            }
        }
        if let Some((start, n)) = run {
            frames.push(streams(start, n)?);
        }
        match frames.len() {
            0 => Err(color_eyre::eyre::eyre!("No Arrow files to read.")),
            1 => Ok(frames.remove(0)),
            _ => Ok(polars::prelude::concat(
                frames.as_slice(),
                DataTableState::union_of_files(),
            )?),
        }
    }

    /// One split of a `save_to_disk` DatasetDict, `dir`, whose `dataset_dict.json` names
    /// `splits`: the subdirectory `--table` names, else the first offered, read as any
    /// directory is. The others are listed, as a cache directory's are.
    fn dataset_dict_split(
        dir: &Path,
        splits: &[String],
        options: &OpenOptions,
        report: &mut ReadReport,
        formats: &crate::formats::Registry,
    ) -> Result<Scan> {
        let listed: Vec<&str> = splits.iter().map(String::as_str).collect();
        let mut picked = crate::hf_splits::pick(&listed, options.table.as_deref())
            .map_err(|e| color_eyre::eyre::eyre!("{}: {e}", dir.display()))?;
        let split = dir.join(picked.split.as_deref().unwrap_or_default());
        let inner = OpenOptions {
            table: None,
            splits: None,
            ..options.clone()
        };
        let scan = Self::build_local_lazyframe(&[split], &inner, report, formats)?;
        // The split's own directory names no splits; its `map()` files are still counted.
        picked.caches = report.splits.as_ref().map_or(0, |inner| inner.caches);
        report.splits = Some(Arc::new(picked));
        Ok(scan)
    }

    /// Build the LazyFrame for `paths`.
    ///
    /// Takes the cloud config by reference rather than reading `self`, so the same
    /// code can run on a background thread — scanning is where the wall-clock time
    /// goes for CSV (schema inference) and for hive directories with many files.
    /// Whether the files about to be read as one table do not all carry the same
    /// columns, for the note that says so.
    ///
    /// Only for the formats with no footer. A Parquet dataset's footers are read anyway
    /// and produce the exact version of this — which columns, in how many files, and
    /// where — so a second, vaguer note above those would be noise.
    ///
    /// A spread of the files rather than all of them, the same three
    /// [`crate::schema_union::sample_files`] reads for the label, and for the same
    /// reason: this runs on the way into a read the user is waiting for.
    fn files_disagree(
        files: &[PathBuf],
        options: &OpenOptions,
        found: FileFormat,
    ) -> crate::schema_union::Disagreement {
        // The format the read will use, not the one the names suggested: an explicit
        // `--format` outranks both, and judging a directory with a reader the open will
        // not use is a note about a read that never happened.
        let format = options.format.unwrap_or(found);
        if format == FileFormat::Parquet {
            return Default::default();
        }
        // Null values are the one setting the sample cannot mirror: `--null-value`
        // takes `COL=VAL` forms the reader resolves against the file it is opening, and
        // a sample that guessed would report a widening the table never did. They are
        // unset unless the user names them, so this stands down where it must and runs
        // everywhere else.
        if options.null_values.is_some() {
            return Default::default();
        }
        crate::schema_union::sample_files(files, format, &Self::read_as(options)).disagreement()
    }

    /// The reader settings a sample has to copy to describe what the open will do.
    ///
    /// Taken from the options the open is actually being made with, not guessed at and
    /// then bailed out of: `from_args_and_config` fills in `infer_schema_length` and
    /// `parse_strings` on every run with no flags at all, so a predicate over "did the
    /// user set anything" is true every time. That shipped once, and the notes about
    /// how a directory had been stacked never appeared outside the tests.
    fn read_as(options: &OpenOptions) -> crate::schema_union::ReadAs {
        crate::schema_union::ReadAs {
            delimiter: options.delimiter,
            has_header: options.has_header,
            skip_rows: options.skip_rows,
            skip_lines: options.skip_lines,
            infer_schema_length: options.infer_schema_length,
            ignore_errors: options.ignore_errors,
            try_parse_dates: options.csv_try_parse_dates(),
            comment_char: options.comment_char.clone(),
            header_rows: options.header_rows.clone(),
            header_join: options.header_join.clone(),
        }
    }

    /// A format spec reads a local file (or a downloaded copy); an object store path
    /// that is scanned in place would otherwise open without it and say nothing.
    fn refuse_spec_in_place(path: &Path, options: &OpenOptions) -> Result<()> {
        if source::is_remote_url(path)
            && (options.spec_file.is_some() || options.spec_name.is_some())
        {
            return Err(crate::error_display::FileError::new(
                path,
                "format specs read local files; download it first",
            )
            .into());
        }
        Ok(())
    }

    /// `found` is what the read has to say about itself, for the caller to put in the
    /// dataset's notes: which data files it passed over, and whether the files it did
    /// read carry the same columns. Written here rather than worked out by the caller
    /// because this is the pass that decides, and a second opinion formed from a second
    /// directory read is a second answer waiting to disagree.
    fn build_lazyframe_from_paths_with(
        cloud: &crate::config::CloudConfig,
        paths: &[PathBuf],
        options: &OpenOptions,
        report: &mut ReadReport,
        formats: &crate::formats::Registry,
    ) -> Result<Scan> {
        // Arrow streams converted, or a bucket's Arrow listed: the load says where
        // each input's rows are.
        if let Some(parts) = &options.arrow_parts {
            if options.table.is_some() && options.splits.is_none() {
                return Err(Self::one_table(Some(FileFormat::Arrow)));
            }
            return Self::scan_arrow_parts(cloud, paths.first(), parts).map(Scan::from);
        }
        // Only the cloud readers below take the settings.
        #[cfg(not(feature = "cloud"))]
        let _ = cloud;
        let path = &paths[0];
        Self::refuse_spec_in_place(path, options)?;
        match source::input_source(path) {
            source::InputSource::Http(_url) => {
                #[cfg(feature = "http")]
                {
                    return Err(color_eyre::eyre::eyre!(
                        "HTTP/HTTPS load is handled in the event loop; this path should not be reached."
                    ));
                }
                #[cfg(not(feature = "http"))]
                {
                    return Err(color_eyre::eyre::eyre!(
                        "HTTP/HTTPS URLs are not supported in this build. Rebuild with default features."
                    ));
                }
            }
            source::InputSource::S3(url) => {
                #[cfg(feature = "cloud")]
                {
                    let (full, cloud_opts) =
                        Self::resolve_cloud_url(Path::new(&format!("s3://{url}")), cloud)?;
                    let is_glob = source::is_prefix_or_glob(&full);
                    // The reader the prefix's own format calls for, when the listing
                    // said what that is. Only Parquet falls through to the scan below.
                    if let Some(format) = Self::cloud_glob_format(&full, options)
                        && let Some(lf) = Self::scan_cloud_prefix(
                            &full,
                            cloud_opts.clone(),
                            format,
                            is_glob,
                            options,
                        )
                    {
                        return lf.map(Scan::from);
                    }
                    let pl_path = PlRefPath::new(full.as_str());
                    let hive_options = if is_glob {
                        polars::io::HiveOptions::new_enabled()
                    } else {
                        polars::io::HiveOptions::default()
                    };
                    let args = ScanArgsParquet {
                        cloud_options: Some(cloud_opts),
                        hive_options,
                        glob: is_glob,
                        ..Default::default()
                    };
                    let lf = LazyFrame::scan_parquet(pl_path, args).map_err(|e| {
                        color_eyre::eyre::eyre!(
                            "Could not read from S3. Check credentials and URL: {}",
                            e
                        )
                    })?;
                    // The frame alone. Building a state here would ask Polars for the
                    // schema, which lists every file under a prefix, and the schema
                    // phase that follows lists them once more for itself.
                    return Ok(lf.into());
                }
                #[cfg(not(feature = "cloud"))]
                {
                    let _ = url;
                    return Err(color_eyre::eyre::eyre!(
                        "S3 is not supported in this build. Rebuild with default features and set AWS credentials (e.g. AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION)."
                    ));
                }
            }
            source::InputSource::Gcs(url) => {
                #[cfg(feature = "cloud")]
                {
                    let (full, cloud_opts) =
                        Self::resolve_cloud_url(Path::new(&format!("gs://{url}")), cloud)?;
                    let is_glob = source::is_prefix_or_glob(&full);
                    // The reader the prefix's own format calls for, when the listing
                    // said what that is. Only Parquet falls through to the scan below.
                    if let Some(format) = Self::cloud_glob_format(&full, options)
                        && let Some(lf) = Self::scan_cloud_prefix(
                            &full,
                            cloud_opts.clone(),
                            format,
                            is_glob,
                            options,
                        )
                    {
                        return lf.map(Scan::from);
                    }
                    let pl_path = PlRefPath::new(full.as_str());
                    let hive_options = if is_glob {
                        polars::io::HiveOptions::new_enabled()
                    } else {
                        polars::io::HiveOptions::default()
                    };
                    let args = ScanArgsParquet {
                        cloud_options: Some(cloud_opts),
                        hive_options,
                        glob: is_glob,
                        ..Default::default()
                    };
                    let lf = LazyFrame::scan_parquet(pl_path, args).map_err(|e| {
                        color_eyre::eyre::eyre!(
                            "Could not read from GCS. Check credentials and URL: {}",
                            e
                        )
                    })?;
                    return Ok(lf.into());
                }
                #[cfg(not(feature = "cloud"))]
                {
                    let _ = url;
                    return Err(color_eyre::eyre::eyre!(
                        "GCS (gs://) is not supported in this build. Rebuild with default features."
                    ));
                }
            }
            source::InputSource::Azure(url) => {
                #[cfg(feature = "cloud")]
                {
                    let (full, cloud_opts) = Self::resolve_cloud_url(Path::new(&url), cloud)?;
                    let is_glob = source::is_prefix_or_glob(&full);
                    // The reader the prefix's own format calls for, when the listing
                    // said what that is. Only Parquet falls through to the scan below.
                    if let Some(format) = Self::cloud_glob_format(&full, options)
                        && let Some(lf) = Self::scan_cloud_prefix(
                            &full,
                            cloud_opts.clone(),
                            format,
                            is_glob,
                            options,
                        )
                    {
                        return lf.map(Scan::from);
                    }
                    let args = ScanArgsParquet {
                        cloud_options: Some(cloud_opts),
                        hive_options: if is_glob {
                            polars::io::HiveOptions::new_enabled()
                        } else {
                            polars::io::HiveOptions::default()
                        },
                        glob: is_glob,
                        ..Default::default()
                    };
                    let lf = LazyFrame::scan_parquet(PlRefPath::new(full.as_str()), args).map_err(
                        |e| {
                            color_eyre::eyre::eyre!(
                                "Could not read from Azure. Check credentials and URL: {}",
                                e
                            )
                        },
                    )?;
                    return Ok(lf.into());
                }
                #[cfg(not(feature = "cloud"))]
                {
                    let _ = url;
                    return Err(color_eyre::eyre::eyre!(
                        "Azure is not supported in this build. Rebuild with default features."
                    ));
                }
            }
            source::InputSource::Local(_) => {}
        }
        Self::build_local_lazyframe(paths, options, report, formats)
    }

    /// The files a directory holds, read as `found`: through the delimited spec the
    /// first of them matches, when one does, else as the format says.
    fn read_directory_files(
        files: &[PathBuf],
        options: &OpenOptions,
        found: FileFormat,
        report: &mut ReadReport,
        formats: &crate::formats::Registry,
    ) -> Result<Scan> {
        if options.delimited.is_none()
            && options.format.is_none()
            && found.separator().is_some()
            && let Some(first) = files.first()
            && let Some(choice) = Self::delimited_spec_of(first, options, formats)?
        {
            let nested = OpenOptions {
                hive: false,
                format: Some(found),
                splits: report.splits.clone(),
                ..options.clone()
            };
            return Self::read_with_delimited_spec(files, &nested, report, formats, choice);
        }
        let nested = OpenOptions {
            hive: false,
            format: Some(options.format.unwrap_or(found)),
            splits: report.splits.clone(),
            ..options.clone()
        };
        Self::build_local_lazyframe(files, &nested, report, formats)
    }

    /// The delimited spec whose glob or magic `file` matches, if one does.
    fn delimited_spec_of(
        file: &Path,
        options: &OpenOptions,
        formats: &crate::formats::Registry,
    ) -> Result<Option<crate::formats::Choice>> {
        let asked = crate::formats::Asked {
            compression: options.compression,
            text_only: true,
            ..Default::default()
        };
        match crate::formats::route(file, &asked, formats)
            .map_err(|e| color_eyre::eyre::eyre!(e))?
        {
            crate::formats::Route::Delimited(choice) => Ok(Some(choice)),
            _ => Ok(None),
        }
    }

    /// `paths` read with the CSV reader in the dialect of the delimited spec `choice`
    /// holds.
    fn read_with_delimited_spec(
        paths: &[PathBuf],
        options: &OpenOptions,
        report: &mut ReadReport,
        formats: &crate::formats::Registry,
        choice: crate::formats::Choice,
    ) -> Result<Scan> {
        let mut nested = options.clone();
        let Some(delimited) = choice.spec.delimited.clone() else {
            return Err(color_eyre::eyre::eyre!(
                "{} is not a delimited spec",
                choice.spec.name
            ));
        };
        delimited.apply(&mut nested);
        nested.delimited = Some(Arc::new(crate::delimited_spec::DelimitedRead::chosen(
            choice.spec,
            choice.by,
            choice.also,
        )));
        Self::build_local_lazyframe(paths, &nested, report, formats)
    }

    /// The local half of `build_lazyframe_from_paths_with`. A directory resolves to
    /// local files, so the recursion stays here and needs no cloud settings.
    fn build_local_lazyframe(
        paths: &[PathBuf],
        options: &OpenOptions,
        report: &mut ReadReport,
        formats: &crate::formats::Registry,
    ) -> Result<Scan> {
        let path = &paths[0];

        // `--hex`: the file's bytes, whatever it holds.
        if options.hex
            && let [one] = paths
            && one.is_file()
        {
            return Ok(Scan::Hex {
                file: one.clone(),
                asked: true,
            });
        }

        // A format spec: one asked for, or one whose glob or magic the path matches. A
        // path whose name or bytes already say what it is opens as it always has, but
        // for a delimited spec's text. Several files are matched by the first, and
        // only to a delimited spec.
        if !options.hive && options.delimited.is_none() {
            let asked = crate::formats::Asked {
                spec_file: options.spec_file.clone(),
                spec_name: options.spec_name.clone(),
                variant: options.spec_variant.clone(),
                spec: options.spec_fetched.clone(),
                builtin: options.format.is_some(),
                compression: options.compression,
                text_only: paths.len() > 1,
            };
            match crate::formats::route(path, &asked, formats)
                .map_err(|e| color_eyre::eyre::eyre!(e))?
            {
                crate::formats::Route::Elsewhere => {}
                crate::formats::Route::Delimited(choice) => {
                    return Self::read_with_delimited_spec(paths, options, report, formats, choice);
                }
                crate::formats::Route::Read(read) => {
                    let lf = Arc::clone(&read.records).into_lazy()?;
                    report.format_read = Some(Arc::new(*read));
                    return Ok(lf.into());
                }
                crate::formats::Route::Decompress(choice) => {
                    return Ok(Scan::DecompressSpec {
                        file: path.clone(),
                        choice,
                    });
                }
            }
        } else if options.hive && (options.spec_file.is_some() || options.spec_name.is_some()) {
            return Err(color_eyre::eyre::eyre!(
                "a format spec reads one file, or one directory of column files"
            ));
        }

        // The header lines of a delimited spec's first file: its units and metadata.
        if let Some(read) = &options.delimited
            && report.delimited.is_none()
            && path.is_file()
        {
            report.delimited = Some(Arc::new(crate::delimited_spec::read_facts(
                read, paths, options,
            )?));
        }

        // One path that is a directory, whether or not `--hive` said so: naming a
        // directory is the request to read it, and the dispatch below is what picks the
        // reader for what it holds. Behind `options.hive` alone, every route that
        // reached here with a directory and without the flag fell through to the
        // Parquet scan and answered `Unsupported file type`.
        if paths.len() == 1 && (options.hive || path.is_dir()) {
            // A file is a file whatever its name holds: `a*b.parquet` is not a glob.
            let is_single_file = path.is_file();
            if !is_single_file {
                // What the directory holds picks the reader. A directory used to go
                // straight to the Parquet scan whatever was in it, so a directory of
                // `.json.gz` was opened by seeking each file's last four bytes for a
                // `PAR1` that was never going to be there — the files were fine, the
                // reader was never asked to be the right one.
                if path.is_dir()
                    && let Some(splits) = crate::hf_splits::dataset_dict(path)
                {
                    return Self::dataset_dict_split(path, &splits, options, report, formats);
                }
                if path.is_dir() {
                    match crate::discover::directory_format(path) {
                        // Flat and Parquet: the scan below is already right for it.
                        crate::discover::DirectoryFormat::One(FileFormat::Parquet, _) => {}
                        // Partitions, or an empty directory. The files are a level down
                        // under `key=value` and only the hive scan walks a tree — but
                        // hive partitioning is a Parquet-only capability in the reader
                        // datui uses (`HiveOptions::new_disabled()` is hard-coded for
                        // CSV and NDJSON), so partitions of anything else cannot be
                        // read as one table here. Saying which files they are beats
                        // Parquet's complaint that they do not end with `PAR1`.
                        crate::discover::DirectoryFormat::Deeper => {
                            if let crate::discover::DirectoryFormat::One(found, files) =
                                crate::discover::hive_leaf_format(path)
                                && found != FileFormat::Parquet
                            {
                                // The extension rather than the format's own name: it
                                // is what is on the files the user can see.
                                let named = files
                                    .first()
                                    .and_then(|f| crate::discover::data_extension(f))
                                    .unwrap_or_else(|| format!("{found:?}").to_lowercase());
                                return Err(color_eyre::eyre::eyre!(
                                    "{} is partitioned into key=value directories of .{} \
                                     files. datui reads hive partitioning for Parquet \
                                     only — open one partition instead.",
                                    path.display(),
                                    named
                                ));
                            }
                        }
                        crate::discover::DirectoryFormat::One(found, files) => {
                            // Read as the files themselves, through the same readers a
                            // list of files typed on the command line goes through. An
                            // explicit `--format` is the user's own answer and outranks
                            // what the names say.
                            let format = options.format.unwrap_or(found);
                            let files =
                                Self::hugging_face_split(path, format, files, options, report)?;
                            report.files_disagree = Self::files_disagree(&files, options, found);
                            return Self::read_directory_files(
                                &files, options, found, report, formats,
                            );
                        }
                        crate::discover::DirectoryFormat::Mixed {
                            format: found,
                            files,
                            passed_over,
                        } => {
                            // The commonest format is the table. A directory of a
                            // thousand CSVs and one stray JSON is a directory of CSVs,
                            // and refusing the whole of it over the stray was datui
                            // deciding that a directory it could read was not worth
                            // reading.
                            let format = options.format.unwrap_or(found);
                            let files =
                                Self::hugging_face_split(path, format, files, options, report)?;
                            report.files_disagree = Self::files_disagree(&files, options, found);
                            let lf = Self::read_directory_files(
                                &files, options, found, report, formats,
                            )?;
                            // After the call, which reads a flat directory of one format
                            // and leaves nothing out of its own. A model's config and
                            // tokenizer JSON are not data the read passed over, and the
                            // weights are not the commonest format there, so neither is
                            // said.
                            if !matches!(found, FileFormat::Safetensors | FileFormat::Gguf) {
                                report.left_out = passed_over;
                            }
                            return Ok(lf);
                        }
                    }
                }
                let use_parquet_hive =
                    path.is_dir() || path.as_os_str().to_string_lossy().contains(".parquet");
                if use_parquet_hive {
                    // Only build the LazyFrame here; schema and partition discovery are the
                    // schema phase's ("Caching schema").
                    return DataTableState::scan_parquet_hive(path).map(Scan::from);
                }
                return Err(color_eyre::eyre::eyre!(
                    "With --hive use a directory or a glob pattern for Parquet (e.g. path/to/dir or path/**/*.parquet)"
                ));
            }
        }

        // A file with no extension may still be Parquet: a part file in a directory named
        // `.parquet`. A regular file is only read when nothing else settled it.
        let effective_format = options
            .format
            .or_else(|| FileFormat::from_path(path))
            .or_else(|| {
                (path.extension().is_none()
                    && crate::discover::is_parquet_key(&path.to_string_lossy()))
                .then_some(FileFormat::Parquet)
            })
            // Any other file whose name says no format, by its first bytes: each
            // format's signature says where it is believed (`crate::readers`).
            .or_else(|| crate::readers::sniff_open(path, options.compression));
        report.format = effective_format;

        // Refused rather than ignored: a file of one table opened with `--table` would
        // otherwise look like the table asked for.
        if options.table.is_some()
            && !effective_format.is_some_and(FileFormat::takes_table)
            && options.splits.is_none()
        {
            return Err(Self::one_table(effective_format));
        }

        // One compressed CSV, TSV or PSV, as a directory of one resolves to: the load
        // decompresses it (`Step::Decompress`) into a copy the dataset holds. Read here,
        // the copy went with the state dropped below and the frame scanned nothing.
        if let [file] = paths
            && options
                .compression
                .or_else(|| CompressionFormat::from_extension(file))
                .is_some()
            && let Some(format) = crate::loading::delimited_format(file, options)
        {
            return Ok(Scan::Decompress {
                file: file.clone(),
                format,
            });
        }

        let Some(format) = effective_format else {
            if !path.exists() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("File not found: {}", path.display()),
                )
                .into());
            }
            // A local file nothing reads is shown as its bytes (#588).
            if paths.len() == 1 && path.is_file() {
                return Ok(Scan::Hex {
                    file: path.clone(),
                    asked: false,
                });
            }
            return Err(color_eyre::eyre::eyre!(match paths.len() {
                1 => UNSUPPORTED.to_string(),
                _ => crate::readers::many_files_refused(),
            }));
        };
        // The home screen asks `reads_many_files` before it offers a directory as one
        // dataset, and this is the same question, so it cannot offer one this refuses.
        if paths.len() > 1 && !format.reads_many_files() {
            if !path.exists() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("File not found: {}", path.display()),
                )
                .into());
            }
            return Err(color_eyre::eyre::eyre!(crate::readers::many_files_refused()));
        }
        crate::readers::scan(crate::readers::ScanIn {
            format,
            paths,
            options,
            report,
            formats,
        })
    }

    /// Whether the plain help overlay is on screen.
    pub fn help_visible(&self) -> bool {
        self.show_help
    }

    /// Open the views list for the dataset on screen, scored against it.
    fn open_template_list(&mut self) {
        if self.data_table_state.is_none() || self.path.is_none() {
            return;
        }
        self.template_modal.table_state.select(Some(0));
        self.refresh_view_list();
        self.template_modal.active = true;
        self.template_modal.mode = TemplateModalMode::List;
    }

    /// Rebuild the list's rows from the store, scored and annotated against
    /// the open dataset; the selection stays near where it was.
    fn refresh_view_list(&mut self) {
        let (Some(state), Some(path)) = (&self.data_table_state, self.view_path()) else {
            return;
        };
        let rows: Vec<ViewRow> = self
            .template_manager
            .find_relevant_templates(path, state.source_schema())
            .into_iter()
            .map(|(template, score)| {
                let reason = template::match_reason(&template, path, state.source_schema());
                ViewRow {
                    template,
                    score,
                    reason,
                }
            })
            .collect();
        self.template_modal.broken_templates = self.template_manager.broken_templates.clone();
        let selected = self.template_modal.table_state.selected().unwrap_or(0);
        self.template_modal.table_state.select(if rows.is_empty() {
            None
        } else {
            Some(selected.min(rows.len() - 1))
        });
        self.template_modal.rows = rows;
    }

    /// Open the save-view form prefilled from the open dataset: a name the
    /// user will recognize, this file's paths and patterns as criteria, and
    /// schema match on — the criterion that carries the view to the next
    /// table shaped like this one.
    fn open_save_view_form(&mut self) {
        self.template_modal
            .enter_create_mode(self.history_limit, &self.theme);

        let query = self.data_table_state.as_ref().and_then(|state| {
            let (query, sql_query, fuzzy_query) = active_query_settings(
                state.get_active_query(),
                state.get_active_sql_query(),
                state.get_active_fuzzy_query(),
            );
            sql_query.or(fuzzy_query).or(query)
        });
        self.template_modal.name_input.suggest(
            self.template_manager
                .suggest_name(self.path.as_deref(), query.as_deref()),
        );

        // Data piped in has no file to pin; its columns are what match it.
        if let Some(path) = self.path.as_ref().filter(|_| !self.reads_stdin()) {
            // Pin this file: its absolute path or URL, its path relative to the
            // working directory when it is local and under it, and glob suggestions.
            let absolute_path = template::exact_location(path);
            self.template_modal
                .exact_path_input
                .suggest(absolute_path.to_string_lossy());
            if let Some(relative) = template::relative_location(path) {
                self.template_modal.relative_path_input.suggest(relative);
            }

            // Suggest a path pattern from the absolute path: the parent of a
            // bare relative name is "", and ""/*.parquet is a pattern that
            // matches every parquet file anywhere, forever. The separator is the
            // path's own, or a Windows path never fits its pattern.
            if let Some(parent) = absolute_path.parent()
                && let Some(parent_str) = parent.to_str()
                && !parent_str.is_empty()
                && let Some(ext) = absolute_path.extension()
            {
                let separator = if crate::source::is_remote_url(path) {
                    '/'
                } else {
                    std::path::MAIN_SEPARATOR
                };
                self.template_modal.path_pattern_input.suggest(format!(
                    "{}{separator}*.{}",
                    parent_str.trim_end_matches(separator),
                    ext.to_string_lossy()
                ));
            }

            // Suggest a filename pattern with digit runs wildcarded, so
            // sales_2024.csv offers itself to sales_2025.csv.
            if let Some(filename) = path.file_name()
                && let Some(filename_str) = filename.to_str()
            {
                use regex::Regex;
                let pattern = match Regex::new(r"\d+") {
                    Ok(re) => re.replace_all(filename_str, "*").to_string(),
                    Err(_) => filename_str.to_string(),
                };
                self.template_modal.filename_pattern_input.suggest(pattern);
            }
        }

        // Schema match starts on: "apply this to a similar table" is the
        // reason views exist, and the columns are the only criterion that
        // says similar.
        if let Some(ref state) = self.data_table_state
            && !state.source_schema().is_empty()
        {
            self.template_modal.schema_match_enabled = true;
        }
    }

    /// Validate and persist the form: a new view, or the edited one. The
    /// settings are rebuilt from the table's applied state either way. A
    /// failed save keeps the form open.
    fn save_view_form(&mut self) {
        self.template_modal.name_error = None;
        let name = self.template_modal.name_input.value().trim().to_string();
        if name.is_empty() {
            self.template_modal.name_error = Some("name is required".to_string());
            self.template_modal.form_focus = FormFocus::Name;
            return;
        }
        let renaming_to_taken = match &self.template_modal.editing_template_id {
            None => self.template_manager.template_exists(&name),
            Some(id) => self
                .template_manager
                .get_template_by_name(&name)
                .is_some_and(|other| other.id != *id),
        };
        if renaming_to_taken {
            self.template_modal.name_error = Some("name already exists".to_string());
            self.template_modal.form_focus = FormFocus::Name;
            return;
        }

        let non_empty = |input: &widgets::text_input::TextInput| {
            let value = input.value().trim();
            (!value.is_empty()).then(|| value.to_string())
        };
        let match_criteria = template::MatchCriteria {
            exact_path: non_empty(&self.template_modal.exact_path_input)
                .map(std::path::PathBuf::from),
            relative_path: non_empty(&self.template_modal.relative_path_input),
            path_pattern: non_empty(&self.template_modal.path_pattern_input),
            filename_pattern: non_empty(&self.template_modal.filename_pattern_input),
            // The columns the view's settings run on, not the query's output: the
            // next file is matched as loaded.
            schema_columns: if self.template_modal.schema_match_enabled {
                self.data_table_state.as_ref().map(|state| {
                    state
                        .source_schema()
                        .iter_names()
                        .map(|s| s.to_string())
                        .collect()
                })
            } else {
                None
            },
            schema_types: None,
        };
        let description = {
            let value = self.template_modal.description_input.value();
            (!value.is_empty()).then(|| value.to_string())
        };

        let saved = if let Some(editing_id) = self.template_modal.editing_template_id.clone() {
            let Some(mut template) = self
                .template_manager
                .get_template_by_id(&editing_id)
                .cloned()
            else {
                return;
            };
            template.name = name;
            template.description = description;
            let stored_schema = template.match_criteria.schema_columns.take();
            template.match_criteria = match_criteria;
            let editing_the_active_view =
                self.active_template_id.as_deref() == Some(editing_id.as_str());
            // The same principle as the settings below: editing an unapplied
            // view must not swap the columns it matches on for the columns of
            // whatever table happens to be open. The toggle still works — off
            // drops the criterion — and the active view follows its table.
            if !editing_the_active_view
                && self.template_modal.schema_match_enabled
                && stored_schema.is_some()
            {
                template.match_criteria.schema_columns = stored_schema;
            }
            // The settings follow the table only while this view is the one
            // dressing it. Editing an unapplied view changes its name,
            // description and matching alone — it must not overwrite what
            // the view carries with whatever the table happens to show.
            if editing_the_active_view && let Some(state) = &self.data_table_state {
                let (query, sql_query, fuzzy_query) = active_query_settings(
                    state.get_active_query(),
                    state.get_active_sql_query(),
                    state.get_active_fuzzy_query(),
                );
                template.settings = template::TemplateSettings {
                    query,
                    sql_query,
                    fuzzy_query,
                    filters: state.get_filters().to_vec(),
                    sort_columns: state.get_sort_columns().to_vec(),
                    sort_descending: state.get_sort_descending().to_vec(),
                    sort_ascending: state.get_sort_ascending(),
                    column_order: state.get_column_order().to_vec(),
                    locked_columns_count: state.locked_columns_count(),
                    pivot: state.last_pivot_spec().cloned(),
                    melt: state.last_melt_spec().cloned(),
                    reshape_source: state.reshape_source().cloned(),
                };
            }
            self.template_manager.update_template(&template).is_ok()
        } else {
            self.create_template_from_current_state(name, description, match_criteria)
                .is_ok()
        };
        if saved {
            self.refresh_view_list();
            self.template_modal.exit_form();
        }
    }

    /// The selected view's score breakdown, for the list's `i` popup.
    fn view_score_details(&self) -> Option<(String, String)> {
        let state = self.data_table_state.as_ref()?;
        let path = self.view_path()?;
        let idx = self.template_modal.table_state.selected()?;
        let row = self.template_modal.rows.get(idx)?;
        let template = &row.template;

        let exact_path_match = template::exact_path_matches(&template.match_criteria, path);
        let relative_path_match = template::relative_path_matches(&template.match_criteria, path);
        let file_cols: std::collections::HashSet<&str> = state
            .source_schema()
            .iter_names()
            .map(|s| s.as_str())
            .collect();
        let exact_schema_match =
            template
                .match_criteria
                .schema_columns
                .as_ref()
                .is_some_and(|required| {
                    let required: std::collections::HashSet<&str> =
                        required.iter().map(|s| s.as_str()).collect();
                    required.is_subset(&file_cols) && file_cols.len() == required.len()
                });

        let mut details = format!("Total score: {:.1}\n\n", row.score);
        if exact_path_match && exact_schema_match {
            details.push_str("Exact path + exact schema: 2000.0\n");
        } else if exact_path_match {
            details.push_str("Exact path: 1000.0\n");
        } else if relative_path_match && exact_schema_match {
            details.push_str("Relative path + exact schema: 1950.0\n");
        } else if relative_path_match {
            details.push_str("Relative path: 950.0\n");
        } else if exact_schema_match {
            details.push_str("Exact schema: 900.0\n");
        } else {
            if template::path_pattern_matches(&template.match_criteria, path) {
                details.push_str("Path pattern match: 50.0+\n");
            }
            if template::filename_pattern_matches(&template.match_criteria, path) {
                details.push_str("Filename pattern match: 30.0+\n");
            }
            if let Some(required_cols) = &template.match_criteria.schema_columns {
                let matching_count = required_cols
                    .iter()
                    .filter(|col| file_cols.contains(col.as_str()))
                    .count();
                if matching_count > 0 {
                    details.push_str(&format!(
                        "Partial schema match: {:.1} ({} columns)\n",
                        matching_count as f64 * 2.0,
                        matching_count
                    ));
                }
            }
        }
        if template.usage_count > 0 {
            details.push_str(&format!(
                "Usage count: {:.1}\n",
                (template.usage_count.min(10) as f64) * 1.0
            ));
        }
        if let Some(last_used) = template.last_used
            && let Ok(duration) = std::time::SystemTime::now().duration_since(last_used)
        {
            let days_since = duration.as_secs() / 86400;
            if days_since <= 7 {
                details.push_str("Recent usage: 5.0\n");
            } else if days_since <= 30 {
                details.push_str("Recent usage: 2.0\n");
            }
        }
        Some((format!("Score: {}", template.name), details))
    }

    /// Set the appropriate help overlay visible (main, template, or analysis). No-op if already visible.
    fn open_help_overlay(&mut self) {
        let already = self.show_help
            || (self.template_modal.active && self.template_modal.show_help)
            || (self.analysis_modal.active && self.analysis_modal.show_help);
        if already {
            return;
        }
        if self.analysis_modal.active {
            self.analysis_modal.show_help = true;
        } else if self.template_modal.active {
            self.template_modal.show_help = true;
        } else {
            self.show_help = true;
        }
    }

    /// True while the confirmation modal is asking whether to download a remote file.
    ///
    /// That is the one confirmation the user has to be able to walk away from: the
    /// size probe behind it can take fifteen seconds, and the answer to "actually,
    /// never mind" is the home screen, not the exit.
    pub fn awaiting_download_confirmation(&self) -> bool {
        self.confirmation_modal.active && self.loading.asking()
    }

    fn key(&mut self, event: &KeyEvent) -> Option<AppEvent> {
        self.debug.on_key(event);

        // A completion flash lives until the next key: whatever this key does,
        // the bar's line about the last action is stale now.
        self.flash = None;

        let ctrl = event.modifiers.contains(KeyModifiers::CONTROL);
        // Ctrl-Q quits from anywhere, before any mode gets a say — including a mode
        // with no CONTROL arm of its own (the chart view) that would otherwise swallow
        // it while busy.
        if ctrl && event.code == KeyCode::Char('q') {
            return Some(AppEvent::Exit);
        }
        // Ctrl-C too, a text field included: a terminal user's reflex for leaving, and
        // the field copies with Alt+W instead.
        if ctrl && event.code == KeyCode::Char('c') {
            return Some(AppEvent::Exit);
        }

        // Acts at once (see `hard_escape_while_busy`), ahead of the keys held behind
        // the view.
        if event.code == KeyCode::Esc && self.view_applying() {
            self.cancel_view();
            return None;
        }
        // The same for a find that is reading.
        if event.code == KeyCode::Esc && self.finding() {
            self.cancel_find();
            return None;
        }

        if event.code == KeyCode::Esc
            && self.input_mode == InputMode::Normal
            && !self.analysis_modal.active
            && !self.error_modal.active
            && !self.confirmation_modal.active
            && self.return_from_quality_evidence(true)
        {
            return None;
        }

        // F1 opens help first so no other branch (e.g. Editing) can consume it.
        if event.code == KeyCode::F(1) {
            self.open_help_overlay();
            return None;
        }

        // Home owns the whole screen and every key while it is up — except under a
        // modal or the help overlay. Both render over home unconditionally, so if
        // home also ate their keys they would be undismissable, and Esc would try
        // to leave home instead.
        if self.input_mode == InputMode::Home
            && !self.confirmation_modal.active
            && !self.error_modal.active
            && !self.show_help
        {
            return self.home_key(event);
        }

        // Ctrl+O goes home from anywhere, including mid-load. That is what makes
        // browsing cheap: opening the wrong 300 MB file costs one keystroke to leave,
        // not a wait for it to finish.
        if event.code == KeyCode::Char('o')
            && event.modifiers.contains(KeyModifiers::CONTROL)
            && (!self.confirmation_modal.active || self.awaiting_download_confirmation())
        {
            self.enter_home();
            return None;
        }

        // Handle modals first - they have highest priority
        // Confirmation modal (for overwrite)
        if self.confirmation_modal.active {
            match event.code {
                KeyCode::Left | KeyCode::Char('h') => {
                    self.confirmation_modal.focus_yes = true;
                }
                KeyCode::Right | KeyCode::Char('l') => {
                    self.confirmation_modal.focus_yes = false;
                }
                KeyCode::Tab => {
                    // Toggle between Yes and No
                    self.confirmation_modal.focus_yes = !self.confirmation_modal.focus_yes;
                }
                // ←→ carry the choice, so ↑↓ scroll a long question; the
                // render clamps the offset.
                KeyCode::Up => {
                    self.confirmation_modal.scroll =
                        self.confirmation_modal.scroll.saturating_sub(1);
                }
                KeyCode::Down => {
                    self.confirmation_modal.scroll =
                        self.confirmation_modal.scroll.saturating_add(1);
                }
                KeyCode::Enter if self.pending_leave.is_some() => {
                    let stop = self.confirmation_modal.focus_yes;
                    self.confirmation_modal.hide();
                    return self.leave_recording(stop);
                }
                KeyCode::Enter => {
                    if self.confirmation_modal.focus_yes {
                        // The confirmations that are not about overwriting a file come
                        // first: reading every row, and forgetting recents.
                        if std::mem::take(&mut self.pending_read_all) {
                            self.confirmation_modal.hide();
                            // Every row is a sample method like the others: it shows in
                            // the strip, and `s` changes it back.
                            let sample = sampling::Sample {
                                method: sampling::SampleMethod::EveryRow,
                                ..self.analysis_modal.sample.clone()
                            };
                            return self.apply_sample(sample);
                        }
                        if self.pending_clear_recents {
                            self.pending_clear_recents = false;
                            self.confirmation_modal.hide();
                            self.cache.clear_recents();
                            self.home_refresh();
                            self.home.status = None;
                            return None;
                        }
                        if let Some(place) = self.pending_forget_place.take() {
                            self.confirmation_modal.hide();
                            let paths = self.home.recents_in(&place);
                            self.cache.forget_recents(&paths);
                            self.home_refresh();
                            self.home.status = None;
                            return None;
                        }
                        // The overwrite was agreed to: each export may now replace the
                        // file it asked about, and only through that answer.
                        if let Some((path, format)) = self.pending_quality_export.take() {
                            self.confirmation_modal.hide();
                            self.analysis_modal.data_quality_export = None;
                            return Some(AppEvent::QualityReportExport(
                                path,
                                format,
                                Overwrite::Replace,
                            ));
                        }
                        if let Some(request) = self.pending_chart_export.take() {
                            self.confirmation_modal.hide();
                            return Some(AppEvent::ChartExport(ChartExportRequest {
                                overwrite: Overwrite::Replace,
                                ..request
                            }));
                        }
                        if let Some(request) = self.pending_export.take() {
                            self.confirmation_modal.hide();
                            return Some(AppEvent::Export(ExportRequest {
                                overwrite: Overwrite::Replace,
                                ..request
                            }));
                        }
                        if let Some((format, header)) = self.pending_copy.take() {
                            self.confirmation_modal.hide();
                            return Some(AppEvent::CopyTable { format, header });
                        }
                        #[cfg(any(feature = "http", feature = "cloud"))]
                        if self.loading.asking() {
                            self.confirmation_modal.hide();
                            // The loader lets go of its hold on the generation as the
                            // download starts, and the download's job takes it before
                            // anything else can look.
                            let step = self.loading.confirmed();
                            return self.run_load_step(step);
                        }
                    } else {
                        self.pending_clear_recents = false;
                        self.pending_read_all = false;
                        self.pending_forget_place = None;
                        // Declining an overwrite returns to the filled form:
                        // the typed path, format and options survive the No.
                        if self.pending_chart_export.take().is_some() {
                            self.chart_export_modal.resume();
                        }
                        // The report's dialog stays open behind the question.
                        self.pending_quality_export = None;
                        if self.pending_export.take().is_some() {
                            self.export_modal.resume();
                            self.input_mode = InputMode::Export;
                        }
                        self.pending_copy = None;
                        if self.loading.asking() {
                            self.enter_home();
                            return None;
                        }
                        self.confirmation_modal.hide();
                    }
                }
                KeyCode::Esc => {
                    // Disarmed on every exit from the modal, so a declined confirmation
                    // cannot fire against whatever the *next* one is asking about.
                    self.pending_clear_recents = false;
                    self.pending_read_all = false;
                    self.pending_forget_place = None;
                    // Staying: the recording goes on, and so does the view.
                    self.pending_leave = None;
                    // Declining an overwrite returns to the filled form: the
                    // typed path, format and options survive the Esc.
                    if self.pending_chart_export.take().is_some() {
                        self.chart_export_modal.resume();
                    }
                    self.pending_quality_export = None;
                    if self.pending_export.take().is_some() {
                        self.export_modal.resume();
                        self.input_mode = InputMode::Export;
                    }
                    self.pending_copy = None;
                    if self.loading.asking() {
                        // Declining a download used to quit datui outright, which made
                        // a remote open the one thing in the app you could not back out
                        // of. `enter_home` puts the open down and hides this.
                        self.enter_home();
                        return None;
                    }
                    self.confirmation_modal.hide();
                }
                _ => {}
            }
            return None;
        }
        // Error modal
        if self.error_modal.active {
            match event.code {
                // A long diagnostic scrolls; the render clamps the offset.
                KeyCode::Up | KeyCode::Char('k') => {
                    self.error_modal.scroll = self.error_modal.scroll.saturating_sub(1);
                    return None;
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    self.error_modal.scroll = self.error_modal.scroll.saturating_add(1);
                    return None;
                }
                KeyCode::PageUp => {
                    self.error_modal.scroll = self.error_modal.scroll.saturating_sub(8);
                    return None;
                }
                KeyCode::PageDown => {
                    self.error_modal.scroll = self.error_modal.scroll.saturating_add(8);
                    return None;
                }
                KeyCode::Esc | KeyCode::Enter => {
                    self.error_modal.hide();
                    // With nothing loaded, dismissing the error would otherwise leave
                    // an empty table and no indication of what to do. Go back to the
                    // list the dataset was chosen from, carrying the reason, so the
                    // next choice is one keystroke away.
                    if self.data_table_state.is_none() {
                        let reason = self.last_load_error.take();
                        self.enter_home();
                        self.home.status = reason;
                    }
                }
                _ => {}
            }
            return None;
        }

        // Main table: the column cursor keys (before help/mode blocks so they always work
        // in Normal). No is_press()/is_release() check: some terminals do not report key
        // kind correctly. Exclude template/analysis modals so they can handle Left/Right
        // themselves.
        let in_main_table = !(self.input_mode != InputMode::Normal
            || self.show_help
            || self.template_modal.active
            || self.analysis_modal.active);
        if in_main_table
            && let Some(mv) = Self::column_cursor_key(event)
            && let Some(state) = self.data_table_state.as_mut()
        {
            state.move_cursor(mv);
            if self.debug.enabled {
                self.debug.last_action = format!("move_cursor({mv:?})");
            }
            return None;
        }

        if self.show_help
            || (self.template_modal.active && self.template_modal.show_help)
            || (self.analysis_modal.active && self.analysis_modal.show_help)
        {
            match event.code {
                KeyCode::Esc => {
                    if self.analysis_modal.active && self.analysis_modal.show_help {
                        self.analysis_modal.show_help = false;
                    } else if self.template_modal.active && self.template_modal.show_help {
                        self.template_modal.show_help = false;
                    } else {
                        self.show_help = false;
                    }
                    self.help_scroll = 0;
                }
                KeyCode::Char('?') => {
                    if self.analysis_modal.active && self.analysis_modal.show_help {
                        self.analysis_modal.show_help = false;
                    } else if self.template_modal.active && self.template_modal.show_help {
                        self.template_modal.show_help = false;
                    } else {
                        self.show_help = false;
                    }
                    self.help_scroll = 0;
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    self.help_scroll = self.help_scroll.saturating_add(1);
                }
                KeyCode::Up | KeyCode::Char('k') => {
                    self.help_scroll = self.help_scroll.saturating_sub(1);
                }
                KeyCode::PageDown => {
                    self.help_scroll = self.help_scroll.saturating_add(10);
                }
                KeyCode::PageUp => {
                    self.help_scroll = self.help_scroll.saturating_sub(10);
                }
                KeyCode::Home => {
                    self.help_scroll = 0;
                }
                KeyCode::End => {
                    // The render clamps this to the last page and persists the result.
                    self.help_scroll = usize::MAX;
                }
                _ => {}
            }
            return None;
        }

        if event.code == KeyCode::Char('?') {
            let ctrl_help = event.modifiers.contains(KeyModifiers::CONTROL);
            // The home screen always accepts characters, into its filter or path input.
            let in_text_input = self.text_field_focused() || self.input_mode == InputMode::Home;
            // Ctrl-? always opens help; bare ? only when not in a text field
            if ctrl_help || !in_text_input {
                self.open_help_overlay();
                return None;
            }
        }

        if self.input_mode == InputMode::SortFilter {
            return self.sort_filter_key(event);
        }

        if self.input_mode == InputMode::Export {
            return self.export_key(event);
        }

        if self.input_mode == InputMode::Inspect {
            return self.inspector_key(event);
        }

        if self.input_mode == InputMode::ValueCounts {
            return self.value_counts_key(event);
        }

        if self.input_mode == InputMode::Hex {
            return self.hex_key(event);
        }

        if self.input_mode == InputMode::GoToColumn {
            self.go_to_column_key(event);
            return None;
        }

        if self.input_mode == InputMode::PickFormat {
            return self.format_picker_key(event);
        }

        if self.input_mode == InputMode::Copy {
            return self.copy_key(event);
        }

        if self.input_mode == InputMode::PivotMelt {
            return self.pivot_melt_key(event);
        }

        if self.input_mode == InputMode::Info {
            return self.info_key(event);
        }

        if self.input_mode == InputMode::Chart {
            return self.chart_key(event);
        }

        if self.analysis_modal.active {
            return self.analysis_key(event);
        }

        if self.template_modal.active {
            return self.template_key(event);
        }

        if self.input_mode == InputMode::Editing {
            return self.editing_key(event);
        }

        const RIGHT_KEYS: [KeyCode; 2] = [KeyCode::Right, KeyCode::Char('l')];

        const LEFT_KEYS: [KeyCode; 2] = [KeyCode::Left, KeyCode::Char('h')];

        const DOWN_KEYS: [KeyCode; 2] = [KeyCode::Down, KeyCode::Char('j')];

        const UP_KEYS: [KeyCode; 2] = [KeyCode::Up, KeyCode::Char('k')];

        // The letter arms below are unmodified keys. Without this guard the
        // bare-`Char` matches also fired with Ctrl or Alt held, so Ctrl+E
        // opened Export and Ctrl+R reversed — bindings nobody declared.
        // Paging (Ctrl+F/B/D/U) is the only modified set this match owns;
        // the global escapes were handled before reaching here.
        if event
            .modifiers
            .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT)
            && !matches!(event.code, KeyCode::Char('f' | 'b' | 'd' | 'u'))
        {
            return None;
        }

        match event.code {
            // q pops the context: opened from the home screen, it returns
            // there; launched straight onto a file, it quits as it always
            // has. Q and Ctrl+Q stay unconditional.
            KeyCode::Char('q') => {
                if self.opened_from_home {
                    self.enter_home();
                    None
                } else {
                    Some(AppEvent::Exit)
                }
            }
            KeyCode::Char('Q') => Some(AppEvent::Exit),
            KeyCode::Char('R') => Some(AppEvent::Reset),
            // Read the dataset again with its first row the other way: as column names,
            // or as data under `column_1`, `column_2`, …. Only delimited text has a
            // header to turn off; anything else carries its own names, and this does
            // nothing there.
            KeyCode::Char('H') => {
                let (paths, options) = self.opened.clone()?;
                options.format.and_then(FileFormat::separator)?;
                let options = OpenOptions {
                    has_header: Some(!options.has_header.unwrap_or(true)),
                    ..options
                };
                self.set_loading_phase("Scanning input", 10);
                self.name_what_is_loading(paths[0].clone());
                Some(AppEvent::Open(paths, options))
            }
            KeyCode::Char('#') => {
                if let Some(ref mut state) = self.data_table_state {
                    state.toggle_row_numbers();
                }
                None
            }
            // The column cursor's width, applied as typed so its effect shows (#647).
            KeyCode::Char('<' | '>' | '=' | 'w')
                if event.is_press() && !event.modifiers.contains(KeyModifiers::CONTROL) =>
            {
                if let Some(state) = self.data_table_state.as_mut()
                    && let Some(name) = state.current_column().map(str::to_string)
                {
                    let (choice, shown) = (state.width_choice(&name), state.shown_width(&name));
                    let width = match event.code {
                        KeyCode::Char('<') => choice.narrower(shown),
                        KeyCode::Char('>') => choice.wider(shown),
                        KeyCode::Char('=') => WidthChoice::Fit,
                        _ => WidthChoice::Auto,
                    };
                    state.set_width_choices([(name, width)]);
                }
                None
            }
            // Ctrl+F pages down, below.
            KeyCode::Char('f')
                if event.is_press() && !event.modifiers.contains(KeyModifiers::CONTROL) =>
            {
                self.open_find();
                None
            }
            KeyCode::Char('n') if event.is_press() => {
                self.find_again(find::Direction::Next);
                None
            }
            KeyCode::Char('N') if event.is_press() => {
                self.find_again(find::Direction::Previous);
                None
            }
            KeyCode::Char('D') => {
                // The type row is drawn from the schema the table already has, so
                // this is a render-time flip like `,`. Session-only.
                self.dtype_row = !self.dtype_row;
                if self.debug.enabled {
                    self.debug.last_action = format!(
                        "toggle_dtype_row({})",
                        if self.dtype_row { "on" } else { "off" }
                    );
                }
                None
            }
            KeyCode::Char('F') => {
                self.open_value_counts();
                None
            }
            KeyCode::Char(',') => {
                // Formatting is applied at render time, so this takes effect on
                // the next frame with no re-collect. Session-only: the config
                // file stays the source of truth at launch.
                self.number_format.enabled = !self.number_format.enabled;
                if self.debug.enabled {
                    self.debug.last_action = format!(
                        "toggle_number_format({})",
                        if self.number_format.enabled {
                            "on"
                        } else {
                            "off"
                        }
                    );
                }
                None
            }
            KeyCode::Esc => {
                // The find is the nearest layer: its mark goes first, then a drill.
                if self.find_shown() {
                    self.find.active = None;
                    return None;
                }
                let mut from_counts = false;
                let drilled_up = if let Some(ref mut state) = self.data_table_state {
                    if state.is_drilled_down() {
                        from_counts = state.drilled_into_value();
                        let _ = state.deferred(|s| s.drill_up());
                        true
                    } else {
                        false
                    }
                } else {
                    false
                };
                if drilled_up {
                    self.sync_sort_filter_modal();
                }
                // Out of a drill from Value Counts, back to the counts it came from:
                // the view is the one they were read of.
                if from_counts
                    && std::mem::take(&mut self.value_counts.drill_return)
                    && let Some(state) = self.data_table_state.as_ref()
                {
                    self.value_counts.rebase(state.len_generation());
                    self.input_mode = InputMode::ValueCounts;
                }
                if drilled_up {
                    self.spawn_async_collect(Self::LOADING_BUFFER);
                    return None;
                }
                // Out of a follow: the rows read so far stay.
                if let Some(state) = self.data_table_state.as_mut()
                    && state.follow().is_some()
                {
                    state.stop_following();
                    self.flash_note("Stopped following".to_string());
                }
                // Escape no longer exits - use 'q' or Ctrl-C to exit
                // (Info modal handles Esc in its own block)
                None
            }
            KeyCode::Char('t') if event.is_press() => self.toggle_follow(),
            code if RIGHT_KEYS.contains(&code) || LEFT_KEYS.contains(&code) => {
                if let Some(ref mut state) = self.data_table_state {
                    state.move_cursor(if RIGHT_KEYS.contains(&code) {
                        crate::widgets::column_paging::CursorMove::Right
                    } else {
                        crate::widgets::column_paging::CursorMove::Left
                    });
                }
                None
            }
            code if event.is_press() && DOWN_KEYS.contains(&code) => {
                let would_collect = self
                    .data_table_state
                    .as_ref()
                    .map(|s| s.scroll_would_trigger_collect(1))
                    .unwrap_or(false);
                if would_collect {
                    self.busy = true;
                    Some(AppEvent::DoScrollNext)
                } else {
                    if let Some(ref mut s) = self.data_table_state {
                        s.select_next();
                    }
                    None
                }
            }
            code if event.is_press() && UP_KEYS.contains(&code) => {
                let would_collect = self
                    .data_table_state
                    .as_ref()
                    .map(|s| s.scroll_would_trigger_collect(-1))
                    .unwrap_or(false);
                if would_collect {
                    self.busy = true;
                    Some(AppEvent::DoScrollPrev)
                } else {
                    if let Some(ref mut s) = self.data_table_state {
                        s.select_previous();
                    }
                    None
                }
            }
            KeyCode::PageDown if event.is_press() => {
                let would_collect = self
                    .data_table_state
                    .as_ref()
                    .map(|s| s.scroll_would_trigger_collect(s.visible_rows as i64))
                    .unwrap_or(false);
                if would_collect {
                    self.busy = true;
                    Some(AppEvent::DoScrollDown)
                } else {
                    if let Some(ref mut s) = self.data_table_state {
                        s.page_down();
                    }
                    None
                }
            }
            KeyCode::Home if event.is_press() => self.jump_key(AppEvent::DoScrollHome),
            KeyCode::End | KeyCode::Char('G') if event.is_press() => {
                self.jump_key(AppEvent::DoScrollEnd)
            }
            KeyCode::Char('f')
                if event.modifiers.contains(KeyModifiers::CONTROL) && event.is_press() =>
            {
                let would_collect = self
                    .data_table_state
                    .as_ref()
                    .map(|s| s.scroll_would_trigger_collect(s.visible_rows as i64))
                    .unwrap_or(false);
                if would_collect {
                    self.busy = true;
                    Some(AppEvent::DoScrollDown)
                } else {
                    if let Some(ref mut s) = self.data_table_state {
                        s.page_down();
                    }
                    None
                }
            }
            KeyCode::Char('b')
                if event.modifiers.contains(KeyModifiers::CONTROL) && event.is_press() =>
            {
                let would_collect = self
                    .data_table_state
                    .as_ref()
                    .map(|s| s.scroll_would_trigger_collect(-(s.visible_rows as i64)))
                    .unwrap_or(false);
                if would_collect {
                    self.busy = true;
                    Some(AppEvent::DoScrollUp)
                } else {
                    if let Some(ref mut s) = self.data_table_state {
                        s.page_up();
                    }
                    None
                }
            }
            KeyCode::Char('d')
                if event.modifiers.contains(KeyModifiers::CONTROL) && event.is_press() =>
            {
                let half = self
                    .data_table_state
                    .as_ref()
                    .map(|s| (s.visible_rows / 2).max(1) as i64)
                    .unwrap_or(1);
                let would_collect = self
                    .data_table_state
                    .as_ref()
                    .map(|s| s.scroll_would_trigger_collect(half))
                    .unwrap_or(false);
                if would_collect {
                    self.busy = true;
                    Some(AppEvent::DoScrollHalfDown)
                } else {
                    if let Some(ref mut s) = self.data_table_state {
                        s.half_page_down();
                    }
                    None
                }
            }
            KeyCode::Char('u')
                if event.modifiers.contains(KeyModifiers::CONTROL) && event.is_press() =>
            {
                let half = self
                    .data_table_state
                    .as_ref()
                    .map(|s| (s.visible_rows / 2).max(1) as i64)
                    .unwrap_or(1);
                let would_collect = self
                    .data_table_state
                    .as_ref()
                    .map(|s| s.scroll_would_trigger_collect(-half))
                    .unwrap_or(false);
                if would_collect {
                    self.busy = true;
                    Some(AppEvent::DoScrollHalfUp)
                } else {
                    if let Some(ref mut s) = self.data_table_state {
                        s.half_page_up();
                    }
                    None
                }
            }
            KeyCode::PageUp if event.is_press() => {
                let would_collect = self
                    .data_table_state
                    .as_ref()
                    .map(|s| s.scroll_would_trigger_collect(-(s.visible_rows as i64)))
                    .unwrap_or(false);
                if would_collect {
                    self.busy = true;
                    Some(AppEvent::DoScrollUp)
                } else {
                    if let Some(ref mut s) = self.data_table_state {
                        s.page_up();
                    }
                    None
                }
            }
            KeyCode::Enter if event.is_press() => {
                if self.input_mode != InputMode::Normal {
                    return None;
                }
                // With no group to drill into, Enter is Space: the row inspector.
                if self.enter_inspects() {
                    self.open_inspector();
                    return None;
                }
                self.drill_selected_row();
                None
            }
            KeyCode::Char('i') if event.is_press() => {
                if let Some(state) = self.data_table_state.as_mut() {
                    // Unread notes put the panel's Notes tab in front — that is
                    // what the accented `i` chip was promising. Read before the
                    // mark, which is what retires the accent.
                    let unseen = state.notes_unseen();
                    state.mark_notes_seen();
                    if unseen {
                        self.info_modal
                            .open_on(crate::widgets::info::InfoTab::Notes);
                    } else if state.format_detail().is_some_and(|d| d.first) {
                        // A table whose columns are the same for every file (a model's
                        // tensors, an audio file's frames, a VCD dump's changes): what
                        // is particular to it is its own tab.
                        self.info_modal
                            .open_on(crate::widgets::info::InfoTab::Format);
                    } else {
                        self.info_modal.open();
                    }
                    self.input_mode = InputMode::Info;
                    self.read_file_facts();
                }
                None
            }
            KeyCode::Char('/') => {
                self.input_mode = InputMode::Editing;
                self.input_type = Some(InputType::Search);
                self.query_mode = self.opening_query_mode();
                self.query_focus = QueryFocus::Input;
                self.query_run_error = None;
                self.sql_completion = None;
                self.sql_columns.clear();
                if let Some(state) = &mut self.data_table_state {
                    self.query_input.set_value(state.get_active_query());
                    self.sql_input.set_value(state.get_active_sql_query());
                    self.fuzzy_input.set_value(state.get_active_fuzzy_query());
                    // The restored query arrives selected: typing states a new
                    // question, arrows edit the old one. Unselected, typing
                    // appended to the tail of the last query.
                    self.query_input.select_all();
                    self.sql_input.select_all();
                    self.fuzzy_input.select_all();
                    state.suppress_error_display = true;
                    self.sql_columns = state.sql_table_columns();
                } else {
                    self.query_input.clear();
                    self.sql_input.clear();
                    self.fuzzy_input.clear();
                }
                self.sync_query_focus();
                None
            }
            KeyCode::Char(':') if event.is_press() => {
                if self.data_table_state.is_some() {
                    self.input_mode = InputMode::Editing;
                    self.input_type = Some(InputType::GoToLine);
                    self.query_input.clear();
                    self.query_input.set_focused(true);
                }
                None
            }
            KeyCode::Char('V') => {
                // Apply the best view whose criteria match this dataset. When none
                // does, the answer is not silence and not the best-scored stranger: the
                // list opens, so the user sees what exists and picks — or saves one.
                if let Some(ref state) = self.data_table_state
                    && let Some(path) = self.view_path()
                {
                    match self
                        .template_manager
                        .get_most_relevant(path, state.source_schema())
                    {
                        Some(template) => {
                            if let Err(e) = self.apply_template(&template) {
                                self.error_modal.show(format!("Error applying view: {}", e));
                            }
                        }
                        None => self.open_template_list(),
                    }
                }
                None
            }
            KeyCode::Char('v') => {
                if self.data_table_state.is_some() && self.path.is_some() {
                    self.open_template_list();
                }
                None
            }
            KeyCode::Char('s') => {
                if self.data_table_state.is_some() {
                    // Rebuilt from the table's applied state, never from what the modal
                    // held last time: an edit staged and then canceled must not arrive
                    // pre-staged, one Apply away from committing silently.
                    self.sync_sort_filter_modal();
                    let current = self
                        .data_table_state
                        .as_ref()
                        .and_then(|state| state.current_column())
                        .map(str::to_string);
                    self.sort_filter_modal.open(
                        self.history_limit,
                        &self.theme,
                        current.as_deref(),
                    );
                    self.input_mode = InputMode::SortFilter;
                }
                None
            }
            KeyCode::Char('r') => {
                if let Some(state) = &mut self.data_table_state {
                    state.deferred(DataTableState::reverse);
                    self.spawn_async_collect("Sorting...");
                }
                None
            }
            KeyCode::Char('a') => {
                // Open analysis modal; no computation until user selects a tool from the sidebar (Enter)
                if self.data_table_state.is_some()
                    && self.input_mode == InputMode::Normal
                    && self.quality_evidence_return.is_none()
                {
                    self.analysis_modal.open();
                    // The sample outlives a close, but its scope names this
                    // dataset's rows: another dataset starts from its current view.
                    if self.analysis_modal.sample_dataset != Some(self.dataset_generation) {
                        self.analysis_modal.sample.scope = data_quality::QualityScope::CurrentView;
                        self.analysis_modal.sample_dataset = Some(self.dataset_generation);
                    }
                }
                None
            }
            KeyCode::Char('c') => {
                if let Some(state) = &self.data_table_state
                    && self.input_mode == InputMode::Normal
                {
                    let numeric_columns: Vec<String> = state
                        .schema()
                        .iter()
                        .filter(|(_, dtype)| dtype.is_numeric())
                        .map(|(name, _)| name.to_string())
                        .collect();
                    let datetime_columns: Vec<String> = state
                        .schema()
                        .iter()
                        .filter(|(_, dtype)| {
                            matches!(
                                dtype,
                                DataType::Datetime(_, _) | DataType::Date | DataType::Time
                            )
                        })
                        .map(|(name, _)| name.to_string())
                        .collect();
                    let category_columns: Vec<String> = state
                        .schema()
                        .iter()
                        .filter(|(_, dtype)| chart_data::is_category_dtype(dtype))
                        .map(|(name, _)| name.to_string())
                        .collect();
                    self.chart_modal.open(
                        ChartColumns {
                            numeric: &numeric_columns,
                            datetime: &datetime_columns,
                            category: &category_columns,
                        },
                        self.app_config.chart.row_limit,
                        self.dataset_generation,
                    );
                    self.chart_cache.clear();
                    self.input_mode = InputMode::Chart;
                }
                None
            }
            KeyCode::Char('p') => {
                if let Some(state) = &self.data_table_state
                    && self.input_mode == InputMode::Normal
                {
                    self.pivot_melt_modal.available_columns =
                        state.schema().iter_names().map(|s| s.to_string()).collect();
                    self.pivot_melt_modal.column_dtypes = state
                        .schema()
                        .iter()
                        .map(|(n, d)| (n.to_string(), d.clone()))
                        .collect();
                    self.pivot_melt_modal.open(self.history_limit, &self.theme);
                    self.input_mode = InputMode::PivotMelt;
                }
                None
            }
            KeyCode::Char('e') => {
                if self.data_table_state.is_some() && self.input_mode == InputMode::Normal {
                    self.export_modal.open(
                        self.original_file_format,
                        self.history_limit,
                        &self.theme,
                        self.original_file_delimiter,
                    );
                    if let Some(state) = self.data_table_state.as_ref() {
                        self.export_modal.offer_source_file = state.can_name_source_files();
                        self.export_modal.nested_columns = state
                            .get_column_order()
                            .iter()
                            .filter_map(|name| state.schema().get(name))
                            .any(crate::nested_json::is_nested);
                        self.export_modal.avro_renames =
                            state.get_column_order().iter().any(|name| {
                                state
                                    .schema()
                                    .get(name)
                                    .is_some_and(|dtype| crate::avro_types::renames(name, dtype))
                            });
                    }
                    self.input_mode = InputMode::Export;
                }
                None
            }
            KeyCode::Char(' ') if event.is_press() => {
                if self.input_mode == InputMode::Normal {
                    self.open_inspector();
                }
                None
            }
            KeyCode::Char('g') if event.is_press() => {
                if self.input_mode == InputMode::Normal {
                    self.open_go_to_column();
                }
                None
            }
            KeyCode::Char('b') if event.is_press() => {
                if self.input_mode == InputMode::Normal {
                    self.open_format_picker();
                }
                None
            }
            KeyCode::Char('y') => {
                if self.input_mode == InputMode::Normal
                    && let Some(state) = self.data_table_state.as_ref()
                {
                    let columns = state.get_column_order().to_vec();
                    let context = copy_modal::CopyContext {
                        row_number: state.selected_display_row().unwrap_or(0),
                        view_rows: state.copy_view_df().map(|d| d.height()).unwrap_or(0),
                        view_cols: columns.len(),
                        total_rows: state.num_rows_if_valid(),
                    };
                    let current = state.current_column().map(str::to_string);
                    self.copy_modal.open(columns, current.as_deref(), context);
                    self.input_mode = InputMode::Copy;
                }
                None
            }
            _ => None,
        }
    }

    /// Handle one event. A key that arrives while the app is busy is not acted on and
    /// not dropped either: it comes back as `Err(key)` for the caller to hold until the
    /// app is idle. The main loop ([`event_pump::EventPump`]) does exactly that;
    /// [`App::event`] is the same call for callers that have nowhere to hold a key.
    pub fn handle(&mut self, event: &AppEvent) -> EventOutcome {
        if let AppEvent::Key(key) = event
            && self.is_busy()
            && !self.key_acts_while_busy(key)
        {
            return Err(*key);
        }
        let out = self.dispatch_event(event);
        // Not while this handler is returning a continuation. A follow-up is the rest of
        // the event just handled — the analysis sets `computing` and returns
        // `AnalysisChunk`, and the phase that chunk will spawn has not spawned — so
        // nothing holds the generation yet, and the errands would advance it out from
        // under the errand that is halfway through. They run after every event and are
        // built to wait; one more event is nothing to them.
        if out.is_none() {
            self.let_waiting_errands_in();
        }
        self.ensure_chart_data();
        self.home_score_search();
        Ok(out)
    }

    /// The errands that wait for the generation to be free, given their turn: after
    /// every event, and when a continuation's hold is let go.
    pub(crate) fn let_waiting_errands_in(&mut self) {
        // Columns a dataset's footers found while the user was inside a query are held
        // rather than dropped; this is where they get in, on the first event after the
        // view comes back to the data.
        if self.join_held_footers() {
            self.reread_after_the_footers_joined();
        }
        // And the same turn for a re-read owed to a dataset whose footers could not be
        // read: it waits on the same work, and gets in the same way.
        self.reread_when_the_work_allows();
        self.collect_when_the_work_allows();
    }

    pub fn event(&mut self, event: &AppEvent) -> Option<AppEvent> {
        self.handle(event).unwrap_or(None)
    }

    /// True while chart data for the current view is being prepared off-thread — either
    /// its worker is running, or it is waiting its turn behind an orphaned worker that
    /// cannot be cancelled (see `ChartInflight::stale`). Either way the user is waiting
    /// on a computation and the throbber should say so.
    pub fn chart_preparing(&self) -> bool {
        match self.chart_inflight.as_ref() {
            Some(inflight) if !inflight.stale => true,
            Some(_) => self.chart_request_pending(),
            None => false,
        }
    }

    /// Whether the chart view wants data it does not have and cannot be told it will
    /// never get.
    fn chart_request_pending(&self) -> bool {
        if self.input_mode != InputMode::Chart || !self.chart_modal.active {
            return false;
        }
        ChartRequest::from_modal(&self.chart_modal)
            .is_some_and(|request| self.chart_cache.get(&request).is_none())
    }

    /// Forget everything chart-related that belongs to the view or dataset on its way
    /// out: the cache, the handed-over slot, an export parked on data that is now never
    /// coming, and an export write still running (its file may still appear, but its
    /// result is ignored and `busy` is released). The preparation in flight is marked
    /// stale rather than forgotten: it cannot be cancelled, so it is waited for and its
    /// result discarded on arrival. Called when the chart view closes and whenever the
    /// dataset changes or is left for the home screen.
    fn reset_chart_state(&mut self) {
        self.chart_cache.clear();
        if let Some(inflight) = self.chart_inflight.as_mut() {
            inflight.stale = true;
            inflight
                .cancel
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }
        // A failed export reopens its modal; it must not follow the user to the next
        // dataset.
        self.chart_export_modal.close();
        *self
            .pending_chart_result
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = None;
        let writing = self
            .jobs
            .supersede(|job| matches!(job, Job::ChartExport { .. }));
        let waiting = self.chart_export_waiting.take().is_some();
        if writing || waiting {
            self.export_progress = None;
            self.status_message = None;
            self.busy = false;
        }
    }

    /// True when the chart cache holds the data for the modal's current selection.
    pub fn chart_data_ready(&self) -> bool {
        ChartRequest::from_modal(&self.chart_modal).is_some_and(|r| self.chart_cache.satisfies(&r))
    }

    /// Start preparing the chart the modal currently asks for, unless the cache already
    /// has it, it is known to fail, or another preparation is still running (the newest
    /// selection is picked up when that one lands). Runs after every event, so a change
    /// of column or option is noticed as soon as it is made and render only ever draws.
    fn ensure_chart_data(&mut self) {
        if self.input_mode != InputMode::Chart || !self.chart_modal.active {
            return;
        }
        let request = ChartRequest::from_modal(&self.chart_modal);
        if let Some(inflight) = self.chart_inflight.as_ref()
            && !request
                .as_ref()
                .is_some_and(|r| r.reads_as(&inflight.request))
        {
            // A count streaming a large view for a selection the cursor has moved
            // past would hold up the next chart for as long as it reads.
            inflight
                .cancel
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }
        let Some(request) = request else {
            return;
        };
        if self.chart_cache.get(&request).is_some() {
            self.chart_cache.touch(&request, self.chart_modal.log_scale);
            return;
        }
        if self.chart_inflight.is_some() {
            return;
        }
        let Some(state) = self.data_table_state.as_ref() else {
            return;
        };
        // Unsorted: the rows a chart draws do not depend on the table's order, a line
        // is drawn in X order anyway, and a sort would make a sampled read read it all.
        let lf = state.analysis_lf();
        let schema = state.schema().clone();
        let dataset = Some(state.len_generation());
        let sampling = chart_data::ChartSampling {
            limit: self.chart_modal.row_limit,
            known_total: state.num_rows_if_valid(),
            seed: self.analysis_modal.sample.seed,
            streaming: self.app_config.performance.polars_streaming,
            full_passes: !state.is_remote_source(),
            held: self.chart_cache.held_rows(dataset),
            cancel: Arc::default(),
        };
        self.chart_inflight = Some(ChartInflight {
            dataset,
            request: request.clone(),
            stale: false,
            cancel: Arc::clone(&sampling.cancel),
        });
        let slot = self.pending_chart_result.clone();
        let tx = self.events.clone();
        self.runtime.spawn_blocking(move || {
            // A panic in the preparation must still report back: without the event the
            // in-flight record would stand for the rest of the session and every later
            // selection would be refused.
            let result = logging::catch_panic(|| request.prepare(&lf, &schema, &sampling))
                .unwrap_or_else(|_| Err(color_eyre::eyre::eyre!("Chart preparation panicked")))
                .map_err(|e| crate::error_display::user_message_from_report(&e, None));
            *slot.lock().unwrap_or_else(|e| e.into_inner()) = Some(result);
            let _ = tx.send(AppEvent::BackgroundChartReady);
        });
    }

    fn dispatch_event(&mut self, event: &AppEvent) -> Option<AppEvent> {
        self.debug.num_events += 1;

        match event {
            AppEvent::Key(key) => {
                // Leaving while standard input is still being recorded asks first.
                if let Some(leaving) = self.leaves(key)
                    && !self.confirmation_modal.active
                    && self.recording().is_some_and(|spool| spool.live())
                {
                    self.ask_about_recording(leaving);
                    return None;
                }
                self.key(key)
            }
            AppEvent::Open(paths, options) => {
                if paths.is_empty() {
                    return Some(AppEvent::Crash("No paths provided".to_string()));
                }
                // Home is now in the stack, so q pops back to it. Never unset:
                // a reread from the table (H) is not a new place.
                if self.input_mode == InputMode::Home {
                    self.opened_from_home = true;
                }
                // `az://container/path` and its kin name no account; where they were
                // typed, or the config, does.
                #[cfg(feature = "cloud")]
                let expanded = match paths
                    .iter()
                    .map(|p| {
                        crate::cloud_sources::expand_azure_short_url(
                            p,
                            &self.app_config.cloud,
                            self.home.browsing.as_deref(),
                        )
                    })
                    .collect::<std::result::Result<Vec<_>, _>>()
                {
                    Ok(expanded) => expanded,
                    Err(message) => return Some(AppEvent::Crash(message)),
                };
                #[cfg(feature = "cloud")]
                if &expanded != paths {
                    return Some(AppEvent::Open(expanded, options.clone()));
                }
                // Asks the filesystem for the size the loading screen shows, and whether
                // the path is there to be a recent.
                let request =
                    loading::OpenRequest::named(paths.clone(), options.clone(), &self.formats);
                self.begin_new_dataset();
                let step = self.loading.open(request);
                self.run_load_step(step)
            }
            AppEvent::OpenLazyFrame(lf, options) => {
                self.begin_new_dataset();
                let step = self.loading.open_frame((**lf).clone(), options.clone());
                self.run_load_step(step)
            }
            AppEvent::HomeListingReady {
                generation,
                listing,
                known,
                visits,
                newest,
                folds,
            } => {
                // Only the current listing's answer clears the flag: a stale one landing
                // first said nothing was in flight while the listing for where the user
                // is still ran. Every refresh asks again, so the newest always answers.
                if *generation == self.home_generation {
                    self.home.listing_in_flight = false;
                }
                // Read fresh from the cache, so true whichever listing carried them:
                // the facts fill in rows the recursive search finds the same way, and
                // only the first listing after entering home carries the folds.
                self.home.known = known.clone();
                self.home.visits = visits.clone();
                self.home.newest_recent = newest.clone();
                if let Some(folds) = folds {
                    self.home.folds = folds.clone();
                }
                // A listing from a superseded request describes somewhere the user has
                // already left.
                if *generation != self.home_generation {
                    return None;
                }
                self.home.apply_listing((**listing).clone());
                // Probes are chosen from the sections, so they can only be started
                // once those exist — asking before the listing lands finds nothing.
                self.spawn_home_probes();
                #[cfg(feature = "cloud")]
                self.spawn_cloud_discovery();
                self.request_home_measurements();
                self.request_home_classifications();
                None
            }
            AppEvent::HomeListingFailed => {
                // The rows already listed stay. The panic is flashed as a raw worker's.
                self.home.listing_in_flight = false;
                None
            }
            AppEvent::HomeMeasured { measured, done } => {
                for (path, m) in measured {
                    self.home.enriched.insert(path.clone(), m.clone());
                }
                self.home.apply_measurements();
                if *done {
                    self.home.measure_in_flight = false;
                    self.request_home_measurements();
                }
                None
            }
            AppEvent::HomeClassified { measured, done } => {
                // Kept even when the listing has been rebuilt since it was asked for. A
                // probe or a cloud peek landing rebuilds it, and a Recent section full of
                // buckets lands several in a row: dropping the answer each time left a
                // share's rows unlabeled for as long as the cloud kept answering.
                for (path, m) in measured {
                    self.home.enriched.insert(path.clone(), m.clone());
                }
                // Nothing re-sorts. `apply_measurements` writes the kind into the row
                // where it already is, which is the whole reason a kind is allowed to
                // arrive after the row was drawn: a listing that reshuffled itself
                // under the cursor while it filled in would be worse than a late
                // label.
                self.home.apply_measurements();
                // The next batch is chosen from the viewport as it is now, so a page
                // that scrolled past four hundred rows while this one was out asks
                // about the forty it landed on, not the four hundred it left behind.
                if *done {
                    self.home.classify_in_flight = false;
                    self.request_home_classifications();
                }
                None
            }
            AppEvent::HomePathListed { listing } => {
                // Kept only for the directory still being typed: a listing for one the
                // user has typed past would offer names from somewhere else.
                if self.home.path_input_active
                    && home::typed_dir(&self.home.path_input) == listing.dir
                {
                    self.home.path_listing = Some((**listing).clone());
                }
                None
            }
            AppEvent::HomePathCompleted {
                generation,
                typed,
                completed,
                candidates,
            } => {
                // Discard if the user has typed since asking: completing onto a
                // different string would scramble what they are in the middle of.
                if *generation != self.home_generation || &self.home.path_input != typed {
                    return None;
                }
                if *candidates == 0 {
                    self.home.status = Some("No such path".to_string());
                } else {
                    self.home.status = None;
                    if *candidates > 1 {
                        self.flash_note(format!("{candidates} matches"));
                    }
                    self.home.path_input = completed.clone();
                }
                None
            }
            AppEvent::HomePreviewReady {
                path,
                stamp,
                read_at,
                rows,
                prepared,
            } => {
                let prepared = prepared.lock().ok().and_then(|mut p| p.take());
                // The columns came with the rows: the pane lists them, for a CSV too.
                if let Some(prepared) = &prepared {
                    let schema = prepared
                        .state
                        .schema()
                        .iter()
                        .map(|(name, dtype)| (name.to_string(), dtype.clone()))
                        .collect();
                    self.home_schema_cache.insert(path.clone(), Some(schema));
                }
                let prepared = prepared.filter(|_| read_at.is_some());
                self.home_previews.landed(
                    path.clone(),
                    *stamp,
                    read_at.unwrap_or(*stamp),
                    rows.clone(),
                    prepared,
                );
                None
            }
            AppEvent::HomeSchemaReady {
                generation,
                path,
                preview,
            } => {
                self.home_schema_inflight.retain(|p| p != path);
                // A preview's columns are not taken back by a metadata read that had none.
                let known = self
                    .home_schema_cache
                    .get(path)
                    .is_some_and(Option::is_some);
                if *generation == self.home_generation && (preview.is_some() || !known) {
                    self.home_schema_cache.insert(path.clone(), preview.clone());
                }
                None
            }
            AppEvent::HomeSearchBatch {
                generation,
                root,
                found,
                scanned,
            } => {
                // Results from a walk that a later navigation superseded describe a
                // place the user has left. The walk is abandoned, not cancelled, so
                // late batches are expected rather than exceptional.
                if *generation == self.home_generation {
                    self.home.search_batch(root, found.clone(), *scanned);
                }
                None
            }
            AppEvent::HomeSearchScored { epoch, matches } => {
                // A scoring that died is not asked again: the next would die the same
                // way, and the matches already listed stand.
                if let Some(matches) = matches {
                    self.home.search_scored(*epoch, (**matches).clone());
                }
                None
            }
            AppEvent::HomeSearchDone {
                generation,
                root,
                scanned,
                limited,
            } => {
                if *generation == self.home_generation {
                    self.home.search_finished(root, *scanned, limited.clone());
                }
                self.home_search_inflight = false;
                // A walk abandoned by a browse held up the one the filter now asks for.
                if !self.home.filter.is_empty() && self.home.search.root.is_none() {
                    self.spawn_home_search();
                }
                None
            }
            #[cfg(feature = "cloud")]
            AppEvent::HomeCloudSources { sources } => {
                self.home.cloud = sources.clone();
                self.home_refresh();
                None
            }
            #[cfg(feature = "cloud")]
            AppEvent::HomeCloudListed {
                id,
                buckets,
                details,
                failure,
                listed_at,
            } => {
                if let Some(source) = self.home.cloud.iter_mut().find(|s| &s.id == id) {
                    source.refreshing = false;
                    for (place, lines) in details {
                        source.place_details.insert(place.clone(), lines.clone());
                    }
                    match failure {
                        // A refresh that failed keeps what the last one found: stale
                        // buckets are more use than none, and the row says it failed.
                        Some((short, detail)) => {
                            for bucket in buckets {
                                if !source.buckets.contains(bucket) {
                                    source.buckets.push(bucket.clone());
                                }
                            }
                            source.status = home::CloudStatus::Failed {
                                short: short.clone(),
                                detail: detail.clone(),
                            };
                        }
                        None => {
                            source.buckets = buckets.clone();
                            source.status = home::CloudStatus::Listed;
                            source.listed_at = Some(*listed_at);
                        }
                    }
                }
                self.home_refresh();
                None
            }
            AppEvent::HomeProbeFailed { root, message } => {
                self.home_probes_inflight.retain(|p| p != root);
                self.home.probe_failed(root.clone());
                self.home.probe_errors.insert(root.clone(), message.clone());
                self.home_refresh();
                None
            }
            AppEvent::HomeProbeProgress { root, rows } => {
                // Only while that listing is still out: a late batch must not paint
                // over the whole answer.
                if self.home_probes_inflight.contains(root) && !self.home.probed.contains_key(root)
                {
                    self.home.listing_so_far.insert(root.clone(), rows.clone());
                    self.home_refresh();
                }
                None
            }
            AppEvent::HomeProbeReady {
                root,
                rows,
                cut_short,
            } => {
                // Give the slot back. The cap exists to bound threads wedged on a dead
                // `hard` mount, which never send this event and so keep their slot for
                // good — a probe that answered is not one of those. Without this the
                // list only grows, and after MAX_CONCURRENT_PROBES roots no further
                // root is ever probed for the rest of the session.
                self.home_probes_inflight.retain(|p| p != root);
                let landed = rows.is_some();
                match rows {
                    Some(rows) => self.home.probe_ready(root.clone(), rows.clone()),
                    None => self.home.probe_failed(root.clone()),
                }
                if *cut_short {
                    self.home.cut_short.insert(root.clone());
                }
                // An account read with its keys because the sign-in has no data role
                // says so beside the account.
                #[cfg(feature = "cloud")]
                if let Some((account, _, _)) = source::azure_parts(&root.to_string_lossy())
                    && crate::azure::remembered_key(&account).is_some()
                {
                    for source in &mut self.home.cloud {
                        let place = source
                            .buckets
                            .iter()
                            .find(|b| home::cloud_account(b).is_some_and(|(_, a)| a == account))
                            .cloned();
                        if let Some(place) = place {
                            let lines = source.place_details.entry(place).or_default();
                            if !lines.iter().any(|(k, _)| k == "access") {
                                lines.push(("access".to_string(), "access key".to_string()));
                            }
                        }
                    }
                }
                // Rebuild so the listing picks the result up; the probe is the only
                // thing that ever reads a remote root.
                self.home_refresh();
                // And then ask about the rows it brought. After the rebuild, never
                // before: the picker reads `visible()`, which is written by the
                // rebuild, so a peek asked between `probe_ready` and here looks at the
                // previous listing and finds nothing in it to ask about.
                //
                // Asked here at all because a listing that lands while the cursor is
                // already where it will stay may draw no further frame, and the frame
                // is what otherwise notices.
                #[cfg(feature = "cloud")]
                if landed {
                    self.peek_cloud_directories();
                }
                #[cfg(not(feature = "cloud"))]
                let _ = landed;
                None
            }
            AppEvent::HomeCloudKinds { kinds, failed } => {
                for directory in failed {
                    self.home.peeking.remove(directory);
                    self.home.peek_failed.insert(directory.clone());
                }
                let roots: Vec<PathBuf> = self.home.probed.keys().cloned().collect();
                for (directory, kind) in kinds {
                    // Answered: out of the in-flight set and into the one the rows are
                    // labelled from. Every directory asked about comes back, so nothing
                    // stays in `peeking` and nothing is asked twice.
                    self.home.peeking.remove(directory);
                    self.home
                        .cloud_kinds
                        .insert(directory.clone(), kind.clone());
                }
                for root in roots {
                    self.home.apply_cloud_kinds(&root);
                }
                self.home_refresh();
                None
            }
            AppEvent::Resize(_cols, _rows) => {
                // No work here: the next render sets visible_rows and flips needs_recollect,
                // which the main loop turns into an async collect against the correct size.
                None
            }
            AppEvent::Collect => {
                self.spawn_async_collect(Self::LOADING_BUFFER);
                None
            }
            AppEvent::DoScrollDown => self.handle_scroll(|s| s.page_down()),
            AppEvent::DoScrollUp => self.handle_scroll(|s| s.page_up()),
            AppEvent::DoScrollNext => self.handle_scroll(|s| s.select_next()),
            AppEvent::DoScrollPrev => self.handle_scroll(|s| s.select_previous()),
            AppEvent::DoScrollEnd => self.handle_scroll(|s| s.scroll_to_end()),
            AppEvent::DoScrollHome => self.handle_scroll(|s| s.scroll_to_start()),
            AppEvent::DoScrollHalfDown => self.handle_scroll(|s| s.half_page_down()),
            AppEvent::DoScrollHalfUp => self.handle_scroll(|s| s.half_page_up()),
            AppEvent::GoToLine(n) => {
                let n = *n;
                self.handle_scroll(|s| s.scroll_to_row_centered(n))
            }
            AppEvent::AnalysisChunk => {
                // Binary columns are stubbed by the source: their blobs are never read
                // for analysis (multi-GB blobs across partitions can exhaust memory).
                let (source, known_total) = match &self.data_table_state {
                    Some(state) => self.sample_source(state),
                    None => {
                        self.analysis_computation = None;
                        self.analysis_modal.computing = None;
                        self.busy = false;
                        return None;
                    }
                };
                let comp = self.analysis_computation.take()?;
                if comp.df.is_none() {
                    let sample = self.analysis_modal.sample.clone();
                    let streaming = self.app_config.performance.polars_streaming;
                    self.spawn_job(
                        Job::Analysis(jobs::AnalysisRun::default()),
                        Some("Computing statistics..."),
                        move |_| {
                            let results = source
                                .cut(&sample.scope)
                                .and_then(|lf| {
                                    crate::statistics::compute_describe_from_lazy(
                                        &lf,
                                        known_total,
                                        &sample,
                                        streaming,
                                    )
                                })
                                .map_err(|e| format!("{e}"))?;
                            Ok(Answer::Described(results))
                        },
                    );
                }
                None
            }
            AppEvent::AnalysisDistributionCompute => {
                if let Some(state) = &self.data_table_state {
                    let (source, known_total) = self.sample_source(state);
                    let sample = self.analysis_modal.sample.clone();
                    let streaming = self.app_config.performance.polars_streaming;
                    self.spawn_job(
                        Job::Analysis(jobs::AnalysisRun::default()),
                        Some("Analyzing distributions..."),
                        move |_| {
                            let options = crate::statistics::ComputeOptions {
                                include_distribution_info: true,
                                include_distribution_analyses: true,
                                include_correlation_matrix: false,
                                include_skewness_kurtosis_outliers: true,
                                polars_streaming: streaming,
                            };
                            let results = source
                                .cut(&sample.scope)
                                .and_then(|lf| {
                                    crate::statistics::compute_statistics_for_sample(
                                        &lf,
                                        &sample,
                                        known_total,
                                        options,
                                    )
                                })
                                .map_err(|e| format!("{e}"))?;
                            Ok(Answer::Distributions(results))
                        },
                    );
                } else {
                    self.analysis_modal.computing = None;
                    self.busy = false;
                }
                None
            }
            AppEvent::AnalysisCorrelationCompute => {
                if let Some(state) = &self.data_table_state {
                    let (source, known_total) = self.sample_source(state);
                    let streaming = state.polars_streaming();
                    let sample = self.analysis_modal.sample.clone();
                    let seed = sample.seed;
                    self.spawn_job(
                        Job::Analysis(jobs::AnalysisRun::default()),
                        Some("Computing correlation matrix..."),
                        move |_| {
                            // Only the numeric columns: nothing else is correlated, and on a
                            // wide table the rest is most of what a full read would hold.
                            let result = source
                                .cut(&sample.scope)
                                .and_then(|lf| {
                                    let schema = lf.clone().collect_schema()?;
                                    let numeric: Vec<polars::prelude::Expr> = schema
                                        .iter()
                                        .filter(|(_, dtype)| dtype.is_numeric())
                                        .map(|(name, _)| col(name.clone()))
                                        .collect();
                                    crate::sampling::read(
                                        &lf.select(numeric),
                                        &sample,
                                        known_total,
                                        streaming,
                                    )
                                })
                                .map(|rows| {
                                    let matrix =
                                        crate::statistics::compute_correlation_matrix(&rows.df)
                                            .ok();
                                    crate::statistics::AnalysisResults {
                                        column_statistics: vec![],
                                        total_rows: rows.total_rows,
                                        sample_size: rows.sample_size,
                                        per_value: rows.per_value.map(|per_value| per_value.kept),
                                        sample_seed: seed,
                                        correlation_matrix: matrix,
                                        distribution_analyses: vec![],
                                    }
                                });
                            let results = result.map_err(|e| format!("{e}"))?;
                            Ok(Answer::Correlations(results))
                        },
                    );
                } else {
                    self.analysis_modal.computing = None;
                    self.busy = false;
                }
                None
            }
            AppEvent::AnalysisDataQualityCompute => {
                // The plan Run committed; Setup's Run is the only way here.
                if let Some(state) = &self.data_table_state {
                    let plan = self.analysis_modal.data_quality_plan.clone();
                    let source_scope = plan.scope.uses_source();
                    let (lf, source, cached_rows) = if source_scope {
                        let (lf, source) = state.data_quality_source_scan();
                        (lf, source, None)
                    } else {
                        let ordered = matches!(
                            plan.scope,
                            data_quality::QualityScope::FirstRows(_)
                                | data_quality::QualityScope::ViewRows { .. }
                        );
                        let (lf, source) = state.data_quality_scan(ordered);
                        let rows = state.num_rows_if_valid().map(|rows| match &plan.scope {
                            data_quality::QualityScope::CurrentView => rows,
                            data_quality::QualityScope::FirstRows(limit) => rows.min(*limit),
                            data_quality::QualityScope::ViewRows { start, end } => {
                                rows.min(*end).saturating_sub(start.saturating_sub(1))
                            }
                            _ => unreachable!(),
                        });
                        (lf, source, rows)
                    };
                    let streaming = state.polars_streaming();
                    // An audio file's signal checks read its samples whole: a full run's.
                    let audio = (plan.compute == data_quality::QualityCompute::Full)
                        .then(|| state.window_for_quality(&plan.scope))
                        .flatten()
                        .and_then(crate::audio::recording);
                    let view_generation = state.len_generation();
                    let dataset_generation = self.dataset_generation;
                    let kept_entry = self.kept_quality_entry(&plan.sample());
                    let kept = kept_entry.map(|kept| kept.rows.clone());
                    // A sampled run on rows already read is labeled as their read was:
                    // the file may have changed since, and these rows did not.
                    let kept_source = kept_entry
                        .filter(|_| plan.compute == data_quality::QualityCompute::Sample)
                        .map(|kept| kept.source.clone());
                    let mut identity = self.quality_source_identity(state, &plan.scope);
                    let copy_job = match self.quality_copy_plan(&plan) {
                        data_quality::CopyPlan::Kept { .. } => self
                            .quality_copy_kept()
                            .cloned()
                            .map_or(QualityCopyJob::Source, QualityCopyJob::Kept),
                        data_quality::CopyPlan::Fetch { .. } => match state.remote_objects() {
                            Some(objects) => QualityCopyJob::Fetch {
                                objects,
                                root: self.quality_copies_root(),
                            },
                            None => QualityCopyJob::Source,
                        },
                        _ => QualityCopyJob::Source,
                    };
                    #[cfg(feature = "cloud")]
                    let (cloud, runtime) = (self.app_config.cloud.clone(), self.runtime.clone());
                    // Only a confirmed full scan pays to read the values a type
                    // conflict hides, and only its access plan promised the read.
                    let mut source = source;
                    if plan.compute == data_quality::QualityCompute::Full
                        && let Some(source) = source.as_mut()
                    {
                        source.conflict_scan = state.quality_conflict_scan();
                    }
                    // Each stage the worker enters comes back as the job's progress, so a
                    // cancelled run's stages are dropped. The watch is the job's: Esc
                    // stops the run through its record.
                    let started = self.start_job(
                        Job::Analysis(jobs::AnalysisRun::default()),
                        Some("Profiling data quality..."),
                    );
                    let ticket = started.ticket();
                    let phases = self.events.clone();
                    let watch = data_quality::QualityWatch::new(move |phase| {
                        let _ = phases.send(AppEvent::JobProgress {
                            ticket,
                            progress: Progress::QualityPhase(phase),
                        });
                    });
                    if let Some(progress) = self.analysis_modal.computing.as_mut() {
                        progress.read = Some(watch.read().clone());
                    }
                    if let Some(Job::Analysis(run)) = self.jobs.job_mut(ticket) {
                        run.watch = Some(watch.clone());
                    }
                    started.run(&self.runtime, move |worker| {
                        // A stat of a local file as the run begins, not a read.
                        match kept_source {
                            Some(source) => identity = source,
                            None => identity.stat(),
                        }
                        let lf = if source_scope {
                            data_quality::prepare_source_quality_scan(lf, source.as_ref())
                                .map_err(|error| format!("{error}"))?
                        } else {
                            lf
                        };
                        let lf =
                            data_quality::apply_quality_scope(lf, &plan.scope, source.as_ref())
                                .map_err(|error| format!("{error}"))?;
                        // Held to the end of the run: the copy stays on disk while its
                        // passes read it, released or not.
                        let fetch = |objects: &[crate::local_copy::RemoteObject], root: &Path| {
                            #[cfg(feature = "cloud")]
                            {
                                Self::fetch_quality_copy(
                                    objects,
                                    root,
                                    &cloud,
                                    &runtime,
                                    watch.read(),
                                )
                            }
                            #[cfg(not(feature = "cloud"))]
                            {
                                let _ = (objects, root);
                                Err(color_eyre::eyre::eyre!("Built without cloud support"))
                            }
                        };
                        let kept_copy = |copy: Option<Arc<crate::local_copy::LocalCopy>>| {
                            worker.send(AppEvent::BackgroundQualityCopyKept {
                                dataset_generation,
                                copy,
                            });
                        };
                        let (lf, held) =
                            Self::quality_scope_on_copy(lf, copy_job, &watch, fetch, kept_copy)
                                .map_err(|error| format!("{error}"))?;
                        let (results, rows) = crate::data_quality::compute_data_quality_watched(
                            &lf,
                            cached_rows,
                            &plan,
                            source.as_ref(),
                            streaming,
                            kept.as_deref(),
                            &watch,
                        );
                        let results = match (results, audio) {
                            (Ok(mut results), Some(audio)) => {
                                crate::data_quality::add_signal_observations(
                                    &mut results,
                                    &audio,
                                    &watch,
                                )
                                .map(|()| results)
                            }
                            (results, _) => results,
                        };
                        // Let go before the answer goes out: a `d` handled as soon
                        // as it lands must find the app's handle the last one.
                        drop(held);
                        let kept = rows.map(|rows| KeptQualitySample {
                            dataset_generation,
                            view_generation,
                            sample: plan.sample(),
                            rows: std::sync::Arc::new(rows),
                            source: identity.clone(),
                        });
                        match results {
                            Ok(mut results) => {
                                results.source = Some(Box::new(identity));
                                Ok(Answer::DataQuality {
                                    results: Box::new(results),
                                    kept,
                                    plan: Box::new(plan),
                                })
                            }
                            Err(error) => {
                                // Stopped after the sample was read: the read is kept.
                                if let Some(kept) = kept {
                                    worker.send(AppEvent::BackgroundQualitySampleKept { kept });
                                }
                                Err(format!("{error}"))
                            }
                        }
                    });
                } else {
                    self.analysis_modal.computing = None;
                    self.busy = false;
                }
                None
            }
            AppEvent::BackgroundLenReady {
                len_generation,
                num_rows,
                file_row_groups,
            } => {
                if self.len_count_inflight == Some(*len_generation) {
                    self.len_count_inflight = None;
                }
                if self.len_count_failed == Some(*len_generation) {
                    self.len_count_failed = None;
                }
                // A count of the view a running query replaced goes back with it.
                if let Some(run) = self.query_running.as_mut() {
                    run.rollback.count_landed(
                        *len_generation,
                        *num_rows,
                        file_row_groups.as_deref(),
                    );
                    if run.len_count_inflight == Some(*len_generation) {
                        run.len_count_inflight = None;
                    }
                }
                // Apply the exact total only if the data hasn't changed since the count
                // was spawned. This runs independently of the buffer paint (which has
                // usually already rendered), so it just corrects the scrollbar/total —
                // no busy state, no re-collect.
                if let Some(state) = self.data_table_state.as_mut()
                    && state.count_landed(*len_generation, *num_rows, file_row_groups.as_deref())
                {
                    // End was pressed before there was an end to go to.
                    if self.end_after_count == Some(*len_generation) {
                        self.end_after_count = None;
                        self.status_message = None;
                        return self.jump_key(AppEvent::DoScrollEnd);
                    }
                } else if self.end_after_count == Some(*len_generation) {
                    // This is the count End was waiting on, and it answers a frame that
                    // is gone — a join landed underneath it and took a fresh
                    // `len_generation` past it. Left here the flag is stranded on a
                    // generation nothing will ever match: the next count to fail for any
                    // reason would speak in its name. So it is retired, and the status
                    // it put up comes down with it.
                    //
                    // Retired, not asked again of the frame that is here. That frame can
                    // belong to a dataset the user opened since — `end_after_count` names
                    // a `len_generation`, which says nothing about which dataset — and
                    // re-asking made the *new* dataset scroll itself to the end on the
                    // strength of a key pressed in the old one. A jump the frame change
                    // swallowed is a jump the user can make again; a jump that arrives on
                    // its own, in a directory they did not press it in, is not.
                    self.retire_the_end_that_was_waiting();
                }
                self.remember_a_downloads_shape();
                None
            }
            AppEvent::FramePainted => {
                self.frame_painted();
                None
            }
            AppEvent::BackgroundLenFailed { len_generation } => {
                if self.len_count_inflight == Some(*len_generation) {
                    self.len_count_inflight = None;
                }
                if let Some(run) = self.query_running.as_mut()
                    && run.len_count_inflight == Some(*len_generation)
                {
                    run.len_count_inflight = None;
                    run.len_count_failed = Some(*len_generation);
                }
                // Mark this generation's count as failed so the row count renders as "?"
                // instead of a misleading provisional total. Before the End handling
                // below: this is about the count, not about who was waiting on it.
                //
                // Only for the frame on screen, because the slot holds one generation.
                // Counts for two frames run at once — a join, a query, a filter or a
                // sort takes a fresh `len_generation` without stopping the count already
                // running — so a failure arriving is not necessarily this frame's.
                // Written unconditionally, an orphan's failure overwrote a live frame's,
                // `count_unknown` went false, and the bar fell through from "?" to the
                // number the buffer happened to reach: a confident partial on a dataset
                // whose count failed. The orphan's own failure is worth nothing to
                // anybody — nothing will ever render against a generation that is gone.
                if self
                    .data_table_state
                    .as_ref()
                    .is_some_and(|state| state.len_generation() == *len_generation)
                {
                    self.len_count_failed = Some(*len_generation);
                }
                // Only for the count End was actually waiting on. Taken unconditionally,
                // a count that failed for one frame answered for an End pressed on
                // another — printing "Could not count the rows to find the end" about a
                // key the user pressed somewhere else entirely, and long since.
                if self.end_after_count == Some(*len_generation) {
                    self.end_after_count = None;
                    if self
                        .data_table_state
                        .as_ref()
                        .is_some_and(|state| state.len_generation() == *len_generation)
                    {
                        self.status_message =
                            Some("Could not count the rows to find the end".to_string());
                    } else {
                        // The frame it was counting is gone, so its failure says nothing
                        // about the one on screen, and the End it belonged to cannot be
                        // answered by it. Retired quietly, as above.
                        self.take_down_the_counting_status();
                    }
                }
                None
            }
            AppEvent::BackgroundFootersJoined { .. } => {
                // Taken whoever the event belongs to, and judged by what is *in* the
                // slot rather than by the event that woke us. Two passes can be running
                // at once, and the newer one may have overwritten the slot before the
                // older one's event is handled: judging by the event would throw the
                // newer answer away and leave the dataset on screen waiting for one
                // that has already been and gone. An entry is also worth draining
                // either way — it is a dataset's worth of schema and every file name.
                let taken = self
                    .pending_footers_result
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .take();
                // Whether this is still the dataset on screen. Not whether an open is in
                // flight: going home leaves the dataset up and puts any open down, and
                // coming straight back to it must not find it stranded on two footers
                // for the rest of the session.
                if let Some((slot_generation, found)) = taken
                    && slot_generation == self.dataset_generation
                {
                    let Some(found) = found else {
                        // The pass could not read them. The dataset stays as it opened
                        // and stops waiting, so it can go and count itself the ordinary
                        // way rather than never at all — which is what the collect
                        // below sets going, since it is the counting the dataset was
                        // declining while it waited.
                        if let Some(state) = self.data_table_state.as_mut() {
                            state.give_up_on_pending_footers();
                        }
                        // The pass is not bringing a count after all, so the jump goes
                        // back to waiting on the ordinary one the collect starts. Owed
                        // rather than run: the collect bumps `task_generation`, and an
                        // export or an analysis may be waiting on the one it would bump
                        // past. `reread_when_the_work_allows` runs it the moment that
                        // work is done.
                        self.reread_owed = Some(slot_generation);
                        self.reread_when_the_work_allows();
                        return None;
                    };
                    self.footers_held = Some((slot_generation, found));
                    if self.join_held_footers() {
                        self.reread_after_the_footers_joined();
                    }
                }
                None
            }
            AppEvent::BackgroundQualitySampleKept { kept } => {
                self.retain_quality_sample(kept);
                None
            }
            AppEvent::BackgroundQualityCopyKept {
                dataset_generation,
                copy,
            } => {
                self.retain_quality_copy(*dataset_generation, copy.clone());
                None
            }
            AppEvent::OpenNamed(paths, options) => {
                if let Some(event) = Self::route_named_without_looking(paths, options) {
                    return Some(event);
                }
                let (paths, options) = (paths.clone(), options.clone());
                let formats = self.formats.clone();
                // The open's first phase. Unleased, as the look is: an answer for an open
                // the user has left (Ctrl+O) is thrown away by the loader, not waited for.
                self.make_way_for_an_open();
                let load = self.loading.look_at_paths();
                self.spawn_job(Job::OpenNamed(load), Some("Scanning input..."), move |_| {
                    if let Some(missing) = Self::missing_named_path(&paths, &formats) {
                        return Ok(Answer::NamedPathMissing(missing));
                    }
                    let (paths, options, directory) =
                        match Self::route_named_paths_with(paths, options, &formats) {
                            AppEvent::LookThenOpenDirectory(dir, options) => {
                                (Vec::new(), options, Some(dir))
                            }
                            AppEvent::Open(paths, options) => (paths, options, None),
                            _ => unreachable!("a named path is opened or looked at"),
                        };
                    Ok(Answer::NamedPaths {
                        paths,
                        options: Box::new(options),
                        directory,
                    })
                });
                None
            }
            AppEvent::LookThenOpenDirectory(dir, options) => {
                // The name on the wait, so the first frame says which directory is being
                // looked at rather than sitting blank. `spawn_job` puts the throbber up
                // and the keys that survive it — Ctrl+C, Ctrl+O — keep working, which
                // is the whole of what doing this on the event thread cost.
                let looking = dir.clone();
                let options = options.clone();
                self.make_way_for_an_open();
                let load = self.loading.look_at_directory(looking.clone());
                // A newer look replaces an older one.
                self.jobs
                    .supersede(|job| matches!(job, Job::LookAtDirectory { .. }));
                // The same words the loading screen shows, so the control bar and the
                // screen above it do not name the wait two different ways.
                // Unleased. A lease exists to make a bump wait for an answer that
                // would otherwise be stranded — and this answer is *meant* to be
                // thrown away when the user moves on, which is the whole of the guard
                // below. Leased, it made everything else wait instead: Ctrl+O out of a
                // seventeen-second look and open a small CSV, and its buffer collect
                // was owed until the abandoned look finally returned.
                // Advertising Ctrl+O as the way out of the wait and then holding the
                // next dataset behind it is the wait again, wearing a different hat.
                #[cfg(feature = "cloud")]
                let (cloud, runtime) = (self.app_config.cloud.clone(), self.runtime.clone());
                let job = Job::LookAtDirectory {
                    load,
                    path: looking.clone(),
                };
                self.spawn_job(job, Some(Self::LOOKING_AT_A_DIRECTORY), move |_| {
                    #[cfg(feature = "cloud")]
                    if home::is_object_store_url(&looking) {
                        let url = looking.to_string_lossy().into_owned();
                        let peeked = wait_on_runtime(&runtime, async move {
                            crate::cloud_browse::peek_kind(&url, &cloud).await
                        })
                        .and_then(Result::ok);
                        let (kind, holds) = match peeked {
                            Some((kind, holds)) => (kind, Some(Box::new(holds))),
                            None => (discover::EntryKind::Unknown, None),
                        };
                        return Ok(Answer::LookedAt {
                            kind,
                            holds,
                            options: Box::new(options),
                        });
                    }
                    // A panic here used to unwind through `run()` and report a crash,
                    // because the look was made on the way to the first frame. On a
                    // worker it is swallowed with the dropped handle instead, and nothing
                    // would ever be sent: the spinner would stay up and the directory
                    // unopened for as long as the user waited. Caught, so the answer is
                    // "a directory" and the home screen opens on it. Read the way this
                    // open will read them, so the rule judges the directory the user is
                    // about to see rather than one nobody will open.
                    let as_read = Self::read_as(&options);
                    let looked = logging::catch_panic(|| {
                        let mut entry = discover::Entry::directory(&looking);
                        entry.kind = discover::EntryKind::Unknown;
                        home::look_into_as(&entry, &as_read)
                    });
                    let kind = match looked {
                        Ok(entry) => entry.kind,
                        Err(_) => discover::EntryKind::Directory,
                    };
                    Ok(Answer::LookedAt {
                        kind,
                        holds: None,
                        options: Box::new(options),
                    })
                });
                None
            }
            AppEvent::ClassifyThenOpen { path, jump } => {
                // A second Enter replaces the first rather than being refused. Every key
                // acts on the home screen even while `busy`, so a second one is
                // reachable, and the newer look is the one the user is waiting for — and
                // refusing meant a look at a share that never answers killed the feature
                // for the rest of the session, silently.
                let looking = path.clone();
                self.jobs.supersede(|job| matches!(job, Job::Classify(_)));
                let look = Job::Classify(jobs::Classify {
                    path: looking.clone(),
                    browsing: self.home.browsing.clone(),
                    jump: *jump,
                });
                let name = looking
                    .file_name()
                    .map(|n| n.to_string_lossy().into_owned())
                    .unwrap_or_else(|| looking.display().to_string());
                // The home screen's own line, because the control bar's is the table's.
                self.home.status = Some(format!("Looking at {name}..."));
                self.spawn_job(look, Some(Self::LOOKING), move |_| {
                    // Every one of these can sit forever on a share that has gone away,
                    // which is the whole reason they are here and not where keys are read.
                    let found = if !looking.exists() {
                        None
                    } else if looking.is_dir() {
                        Some(crate::discover::classify_directory(&looking))
                    } else {
                        Some(crate::discover::EntryKind::File)
                    };
                    Ok(Answer::Kind(found))
                });
                None
            }
            AppEvent::JobEnded(ticket) => self.job_ended(*ticket),
            AppEvent::JobProgress { ticket, progress } => {
                self.job_progress(*ticket, progress);
                None
            }
            AppEvent::Search(query) => {
                self.run_query(QueryMode::QStyle, query, "Applying query...");
                None
            }
            AppEvent::SqlSearch(sql) => {
                self.run_query(QueryMode::Sql, sql, "Applying SQL query...");
                None
            }
            AppEvent::FuzzySearch(query) => {
                self.run_query(QueryMode::Search, query, "Searching...");
                None
            }
            AppEvent::Filter(statements) => {
                if let Some(state) = &mut self.data_table_state {
                    state.deferred(|s| s.filter(statements.clone()));
                }
                self.spawn_async_collect("Filtering...");
                None
            }
            AppEvent::Sort(columns, descending) => {
                if let Some(state) = &mut self.data_table_state {
                    state.deferred(|s| s.sort_by(columns.clone(), descending.clone()));
                }
                self.spawn_async_collect("Sorting...");
                None
            }
            AppEvent::Reset => {
                if let Some(state) = &mut self.data_table_state {
                    state.deferred(|s| s.reset());
                }
                self.spawn_async_collect(Self::LOADING_BUFFER);
                // Clear active template when resetting
                self.active_template_id = None;
                None
            }
            AppEvent::ColumnOrder(order, locked_count) => {
                if let Some(state) = &mut self.data_table_state {
                    state.deferred(|s| {
                        s.set_column_order(order.clone());
                        s.set_locked_columns(*locked_count);
                    });
                    self.spawn_async_collect(Self::LOADING_BUFFER);
                }
                None
            }
            AppEvent::Pivot(spec) => {
                // The modal stays up until the result is in, so a pivot that fails
                // leaves the spec there to fix.
                let job = self.data_table_state.as_ref()?.plan_pivot(spec);
                let spec = spec.clone();
                self.spawn_job(Job::Pivot, Some(Self::COMPUTING_PIVOT), move |_| {
                    let pivoted = job
                        .run()
                        .map_err(|e| crate::error_display::user_message_from_report(&e, None))?;
                    Ok(Answer::Pivoted { spec, pivoted })
                });
                None
            }
            AppEvent::Melt(spec) => {
                self.busy = true;
                if let Some(state) = &mut self.data_table_state {
                    let result = state.deferred(|s| s.melt(spec));
                    match result {
                        Ok(()) => {
                            self.pivot_melt_modal.close();
                            self.input_mode = InputMode::Normal;
                            self.spawn_async_collect("Computing melt...");
                            None
                        }
                        Err(e) => {
                            self.busy = false;
                            self.error_modal
                                .show(crate::error_display::user_message_from_report(&e, None));
                            None
                        }
                    }
                } else {
                    self.busy = false;
                    None
                }
            }
            AppEvent::QualityReportExport(path, format, overwrite) => {
                // The report on screen and the plan it was measured with, cloned into
                // the writer: the file is built from memory and nothing is read.
                let results = self.analysis_modal.data_quality_results.clone()?;
                let plan = self.analysis_modal.quality_result_plan().clone();
                let (path, format, overwrite) = (path.clone(), *format, *overwrite);
                self.spawn_job(
                    Job::QualityReport,
                    Some("Writing the report..."),
                    move |_| {
                        crate::quality_export::write(&path, &results, &plan, format, overwrite)
                            .map_err(|error| Self::format_export_error(&error, &path))?;
                        Ok(Answer::QualityReportWritten(path))
                    },
                );
                None
            }
            AppEvent::ChartExport(request) => {
                self.busy = true;
                self.export_progress = Some(ExportProgress {
                    file_path: request.path.clone(),
                    current_phase: "Exporting chart".to_string(),
                    written: None,
                });
                Some(AppEvent::DoChartExport(request.clone()))
            }
            AppEvent::DoChartExport(request) => {
                // `ChartExport` arms `busy` and defers here so the phase can be drawn
                // first. A Ctrl-O in that window has already left the chart view, and
                // there is nothing to export any more: release the app rather than park
                // an export that no view would ever prepare.
                if self.input_mode != InputMode::Chart || !self.chart_modal.active {
                    self.export_progress = None;
                    self.status_message = None;
                    self.busy = false;
                    return None;
                }
                self.start_chart_export(request.clone());
                None
            }
            AppEvent::BackgroundChartReady => {
                // The result belongs to the one preparation in flight. It is installed
                // only while that record is current (a reset marks it stale when its
                // view or dataset goes) and only into the dataset it was computed from.
                // Taking the record is what lets the next request start; the slot is
                // emptied either way so a discarded series is not kept around.
                let inflight = self.chart_inflight.take()?;
                let outcome = self
                    .pending_chart_result
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .take()
                    .unwrap_or_else(|| Err("Chart preparation produced no result".to_string()));
                if inflight.stale {
                    return None;
                }
                // A count stopped part way is no answer, and must not be remembered as
                // a failure; the selection is prepared again when it comes back.
                if outcome.is_err() && inflight.cancel.load(std::sync::atomic::Ordering::Relaxed) {
                    return None;
                }
                let dataset = self.data_table_state.as_ref().map(|s| s.len_generation());
                if dataset != inflight.dataset {
                    return None;
                }
                self.chart_cache.insert(inflight.request, outcome);
                // An export parked on chart data resumes against the *current*
                // selection, whatever just landed: it is written if that selection is
                // now prepared, fails with the reason if that is the one that failed,
                // and otherwise waits for the next result (which `ensure_chart_data`
                // starts once this handler returns).
                if let Some(request) = self.chart_export_waiting.take() {
                    self.start_chart_export(request);
                }
                None
            }
            AppEvent::Export(request) => {
                if self.data_table_state.is_some() {
                    self.busy = true;
                    self.export_progress = Some(ExportProgress {
                        file_path: request.path.clone(),
                        current_phase: "Preparing export".to_string(),
                        written: None,
                    });
                    // Drawn before the export starts.
                    Some(AppEvent::DoExport(request.clone()))
                } else {
                    None
                }
            }
            AppEvent::Followed(news) => {
                self.followed(news);
                None
            }
            AppEvent::DoExport(request) => {
                let Some(state) = &self.data_table_state else {
                    self.export_progress = None;
                    self.busy = false;
                    return None;
                };
                let frame = match self.export_counts.take() {
                    Some(counts) => crate::widgets::datatable::ExportFrame::of(
                        polars::prelude::IntoLazy::lazy(counts),
                    ),
                    None => state.export_frame(request.options.source_file),
                };
                let streaming = state.polars_streaming();
                // One job from plan to commit: it holds the generation throughout,
                // and the rows it collects, if it collects, die with it.
                let phase = match request.route(streaming) {
                    crate::export::Route::Streamed => Self::export_write_phase(request),
                    crate::export::Route::Collected => "Collecting data",
                };
                self.export_progress = Some(ExportProgress {
                    file_path: request.path.clone(),
                    current_phase: phase.to_string(),
                    written: None,
                });
                let writing = Self::export_write_phase(request);
                let request = request.clone();
                self.spawn_job(Job::Export, Some("Exporting..."), move |worker| {
                    let report = worker.reporter();
                    let written = move |bytes| {
                        report(Progress::ExportWriting {
                            phase: writing,
                            bytes,
                        })
                    };
                    frame
                        .into_lazy()
                        .map_err(color_eyre::eyre::Report::from)
                        .and_then(|lf| crate::export::run(lf, &request, streaming, written))
                        .map_err(|e| Self::format_export_error(&e, &request.path))?;
                    // Success is reported only once the file is committed.
                    Ok(Answer::Exported(request.path))
                });
                None
            }
            AppEvent::CopyTable { format, header } => {
                let accepts = match self.copy_destination() {
                    Ok(destination) => destination.accepts(),
                    Err(e) => {
                        self.busy = false;
                        self.error_modal.show(e);
                        return None;
                    }
                };
                if let Some(state) = &self.data_table_state {
                    let lf = state.visible_lf();
                    let streaming = state.polars_streaming();
                    let (format, header) = (*format, *header);
                    self.spawn_job(Job::Copy, Some("Collecting data for copy..."), move |_| {
                        // A capped destination's copy is read in batches and given
                        // up at the cap; any other is collected and built whole.
                        let (payload, rows) = match accepts.base64_limit {
                            Some(limit) => crate::clipboard::bounded_table_text(
                                lf, format, header, limit,
                            )
                            .map(|(text, rows)| (crate::clipboard::Payload::text(text), rows)),
                            None => crate::statistics::collect_lazy(lf, streaming)
                                .map_err(|e| crate::error_display::user_message_from_polars(&e))
                                .and_then(|df| {
                                    crate::clipboard::tabular_payload(
                                        &df,
                                        format,
                                        header,
                                        accepts.html,
                                    )
                                    .map(|payload| (payload, df.height()))
                                }),
                        }
                        .map_err(|message| format!("Copy failed: {message}"))?;
                        // Handed on whole, never copied.
                        Ok(Answer::Copied {
                            payload,
                            message: format!(
                                "Copied {} rows as {}",
                                copy_modal::thousands(rows),
                                format.as_str()
                            ),
                        })
                    });
                } else {
                    self.busy = false;
                }
                None
            }
            _ => None,
        }
    }

    /// What `column` holds, for an export's axis ticks.
    fn axis_numbers(&self, column: &str) -> chart_data::AxisNumbers {
        let schema = self.data_table_state.as_ref().map(|s| s.schema().as_ref());
        chart_data::AxisNumbers::column(&self.number_format, schema, column)
    }

    /// What `columns` hold on one axis.
    fn axes_numbers(&self, columns: &[String]) -> chart_data::AxisNumbers {
        let schema = self.data_table_state.as_ref().map(|s| s.schema().as_ref());
        chart_data::AxisNumbers::columns(&self.number_format, schema, columns)
    }

    /// Build the export from the prepared chart for the current selection. `Ok(None)`
    /// means that chart is still being prepared and the caller should wait for it.
    /// Exports what is visible (effective x + y); a blank title means no title.
    fn build_chart_export_job(&self, title: &str) -> Result<Option<ChartExportJob>> {
        if self.data_table_state.is_none() {
            return Err(color_eyre::eyre::eyre!("No data loaded"));
        }
        let chart_title = Some(title.trim())
            .filter(|t| !t.is_empty())
            .map(str::to_string);

        let request = match (
            ChartRequest::from_modal(&self.chart_modal),
            self.chart_modal.chart_kind,
        ) {
            (Some(ChartRequest::XRange { .. }), _) => {
                return Err(color_eyre::eyre::eyre!("No Y axis columns selected"));
            }
            (None, ChartKind::XY) => {
                return Err(color_eyre::eyre::eyre!("No X axis column selected"));
            }
            (None, ChartKind::Histogram) => {
                return Err(color_eyre::eyre::eyre!("No histogram column selected"));
            }
            (None, ChartKind::BoxPlot) => {
                return Err(color_eyre::eyre::eyre!("No box plot column selected"));
            }
            (None, ChartKind::Kde) => {
                return Err(color_eyre::eyre::eyre!("No KDE column selected"));
            }
            (None, ChartKind::Heatmap) => {
                return Err(color_eyre::eyre::eyre!("No heatmap columns selected"));
            }
            (None, ChartKind::Bar) => {
                return Err(color_eyre::eyre::eyre!(
                    "No bar category and value selected"
                ));
            }
            (Some(request), _) => request,
        };
        let prepared = match self.chart_cache.get(&request) {
            Some(Ok(prepared)) => prepared,
            // A selection known not to chart is never retried, so waiting for its data
            // would wait forever: fail the export now with the reason.
            Some(Err(message)) => return Err(color_eyre::eyre::eyre!("{}", message)),
            None => return Ok(None),
        };
        let no_points = || color_eyre::eyre::eyre!("No valid data points to export");
        let notes = prepared.notes();

        let job = match prepared {
            ChartPrepared::XY(cache) => {
                let log_scale = self.chart_modal.log_scale;
                let points = if log_scale {
                    cache
                        .series_log
                        .clone()
                        .unwrap_or_else(|| log_series(&cache.series))
                } else {
                    cache.series.clone()
                };
                let series: Vec<ChartExportSeries> = points
                    .into_iter()
                    .zip(cache.y_columns.iter())
                    .zip(cache.breaks.iter())
                    .filter(|((points, _), _)| !points.is_empty())
                    .map(|((points, name), breaks)| ChartExportSeries {
                        name: name.clone(),
                        points,
                        breaks: breaks.clone(),
                    })
                    .collect();
                if series.is_empty() {
                    return Err(no_points());
                }

                let mut all_x_min = f64::INFINITY;
                let mut all_x_max = f64::NEG_INFINITY;
                let mut all_y_min = f64::INFINITY;
                let mut all_y_max = f64::NEG_INFINITY;
                for s in &series {
                    for &(x, y) in &s.points {
                        all_x_min = all_x_min.min(x);
                        all_x_max = all_x_max.max(x);
                        all_y_min = all_y_min.min(y);
                        all_y_max = all_y_max.max(y);
                    }
                }

                let chart_type = self.chart_modal.chart_type;
                let y_min_bounds = if chart_type == ChartType::Bar {
                    0.0_f64.min(all_y_min)
                } else if self.chart_modal.y_starts_at_zero {
                    0.0
                } else {
                    all_y_min
                };
                let y_max_bounds = if all_y_max > y_min_bounds {
                    all_y_max
                } else {
                    y_min_bounds + 1.0
                };
                let (x_min_bounds, x_max_bounds) = if all_x_max > all_x_min {
                    (all_x_min, all_x_max)
                } else {
                    (all_x_min - 0.5, all_x_min + 0.5)
                };

                let bounds = ChartExportBounds {
                    x_min: x_min_bounds,
                    x_max: x_max_bounds,
                    y_min: y_min_bounds,
                    y_max: y_max_bounds,
                    x_label: cache.x_column.clone(),
                    y_label: cache.y_columns.join(", "),
                    x_axis_kind: cache.x_axis_kind,
                    log_scale,
                    chart_title,
                    notes,
                    x_numbers: self.axis_numbers(&cache.x_column),
                    y_numbers: self.axes_numbers(&cache.y_columns),
                };
                ChartExportJob::Series {
                    series,
                    chart_type,
                    bounds,
                }
            }
            ChartPrepared::Histogram(data) => {
                if data.bins.is_empty() {
                    return Err(no_points());
                }
                let points: Vec<(f64, f64)> =
                    data.bins.iter().map(|b| (b.center, b.count)).collect();
                let series = vec![ChartExportSeries {
                    name: data.column.clone(),
                    points,
                    breaks: Vec::new(),
                }];
                let x_max = if data.x_max > data.x_min {
                    data.x_max
                } else {
                    data.x_min + 1.0
                };
                let y_max = if data.max_count > 0.0 {
                    data.max_count
                } else {
                    1.0
                };
                let bounds = ChartExportBounds {
                    x_min: data.x_min,
                    x_max,
                    y_min: 0.0,
                    y_max,
                    x_label: data.column.clone(),
                    y_label: "Count".to_string(),
                    x_axis_kind: chart_data::XAxisTemporalKind::Numeric,
                    log_scale: false,
                    chart_title,
                    notes,
                    x_numbers: self.axis_numbers(&data.column),
                    y_numbers: chart_data::AxisNumbers::count(&self.number_format),
                };
                ChartExportJob::Series {
                    series,
                    chart_type: ChartType::Bar,
                    bounds,
                }
            }
            ChartPrepared::BoxPlot(data) => {
                if data.stats.is_empty() {
                    return Err(no_points());
                }
                let bounds = BoxPlotExportBounds {
                    y_min: data.y_min,
                    y_max: data.y_max,
                    x_labels: data.stats.iter().map(|s| s.name.clone()).collect(),
                    x_label: "Columns".to_string(),
                    y_label: "Value".to_string(),
                    chart_title,
                    notes,
                    y_numbers: self.axes_numbers(
                        &data
                            .stats
                            .iter()
                            .map(|s| s.name.clone())
                            .collect::<Vec<_>>(),
                    ),
                };
                ChartExportJob::BoxPlot {
                    data: data.clone(),
                    bounds,
                }
            }
            ChartPrepared::Kde(data) => {
                if data.series.is_empty() {
                    return Err(no_points());
                }
                let series: Vec<ChartExportSeries> = data
                    .series
                    .iter()
                    .map(|s| ChartExportSeries {
                        name: s.name.clone(),
                        points: s.points.clone(),
                        breaks: Vec::new(),
                    })
                    .collect();
                let bounds = ChartExportBounds {
                    x_min: data.x_min,
                    x_max: data.x_max,
                    y_min: 0.0,
                    y_max: data.y_max,
                    x_label: series
                        .iter()
                        .map(|s| s.name.as_str())
                        .collect::<Vec<_>>()
                        .join(", "),
                    y_label: "Density".to_string(),
                    x_axis_kind: chart_data::XAxisTemporalKind::Numeric,
                    log_scale: false,
                    chart_title,
                    notes,
                    x_numbers: self
                        .axes_numbers(
                            &data
                                .series
                                .iter()
                                .map(|s| s.name.clone())
                                .collect::<Vec<_>>(),
                        )
                        .fractional(),
                    y_numbers: chart_data::AxisNumbers::measure(&self.number_format, "Density"),
                };
                ChartExportJob::Series {
                    series,
                    chart_type: ChartType::Line,
                    bounds,
                }
            }
            ChartPrepared::Heatmap(data) => {
                if data.counts.is_empty() || data.max_count <= 0.0 {
                    return Err(no_points());
                }
                let bounds = ChartExportBounds {
                    x_min: data.x_min,
                    x_max: data.x_max,
                    y_min: data.y_min,
                    y_max: data.y_max,
                    x_label: data.x_column.clone(),
                    y_label: data.y_column.clone(),
                    x_axis_kind: chart_data::XAxisTemporalKind::Numeric,
                    log_scale: false,
                    chart_title,
                    notes,
                    x_numbers: self.axis_numbers(&data.x_column),
                    y_numbers: self.axis_numbers(&data.y_column),
                };
                ChartExportJob::Heatmap {
                    data: data.clone(),
                    bounds,
                }
            }
            ChartPrepared::Bar(data) => {
                if data.bars.is_empty() {
                    return Err(no_points());
                }
                ChartExportJob::Bar {
                    format: data.value_format(&self.number_format),
                    data: data.clone(),
                    title: chart_title,
                    notes,
                }
            }
            // Rejected above, since a single X column has nothing to export; never
            // `Ok(None)`, which would park the export waiting for data that is here.
            ChartPrepared::XRange(_) => {
                return Err(color_eyre::eyre::eyre!("No Y axis columns selected"));
            }
        };
        Ok(Some(job))
    }

    /// Write the chart from the prepared data off-thread, or park the export until that
    /// data is ready. `busy` was set by `ChartExport` and stays set until the export ends.
    fn start_chart_export(&mut self, request: ChartExportRequest) {
        match self.build_chart_export_job(&request.title) {
            Ok(Some(job)) => {
                self.chart_export_waiting = None;
                let write = Job::ChartExport {
                    path: request.path.clone(),
                    format: request.format,
                };
                self.spawn_job(write, Some("Exporting chart..."), move |_| {
                    let ChartExportRequest {
                        path,
                        format,
                        width,
                        height,
                        overwrite,
                        ..
                    } = request;
                    job.write(&path, format, (width, height), overwrite)
                        .map_err(|e| Self::format_export_error(&e, &path))?;
                    Ok(Answer::ChartExported)
                });
            }
            // Still being prepared; `BackgroundChartReady` comes back here.
            Ok(None) => self.chart_export_waiting = Some(request),
            Err(e) => {
                let message = Self::format_export_error(&e, &request.path);
                self.finish_chart_export(&request.path, request.format, Err(message));
            }
        }
    }

    fn finish_chart_export(
        &mut self,
        path: &Path,
        format: ChartExportFormat,
        result: Result<(), String>,
    ) {
        self.chart_export_waiting = None;
        self.export_progress = None;
        self.status_message = None;
        self.busy = false;
        match result {
            Ok(()) => {
                self.flash_note(format!("Chart exported to {}", path.display()));
                self.chart_export_modal.close();
            }
            Err(message) => {
                self.error_modal.show(message);
                self.chart_export_modal.reopen_with_path(path, format);
            }
        }
    }

    /// Bring the Sort & Filter sidebar in line with the state actually applied to the
    /// frame on screen: the real column order and hidden set, the applied sort, the
    /// active filters. Called on open, so an edit staged in the modal and then
    /// canceled dies with it rather than arriving pre-staged next time — and after a
    /// drill-down swap, where a sidebar still showing the grouped view's filters
    /// would re-send one against a List column.
    fn sync_sort_filter_modal(&mut self) {
        let Some(state) = self.data_table_state.as_ref() else {
            return;
        };
        let filters = state.view_filters().to_vec();
        let sort_columns = state.view_sort_columns().to_vec();
        let sort_descending = state.view_sort_descending().to_vec();
        let headers: Vec<String> = state.schema().iter_names().map(|s| s.to_string()).collect();
        let order = state.headers();
        let locked = state.locked_columns_count();

        let modal = &mut self.sort_filter_modal;
        modal.filter.statements = filters;
        modal.filter.available_columns = order.clone();
        // The cursor starts on the add row; the editor never survives a resync.
        modal.filter.cursor = modal.filter.statements.len();
        modal.filter.editor = None;
        // A schema column the applied order leaves out is hidden; it is listed where
        // it stood when hidden, so showing it again puts it back there. The order the
        // sidebar last applied says where only while the table still shows it; once a
        // view, query or reshape has set the order, the schema places them.
        let shown: std::collections::HashSet<&str> = order.iter().map(String::as_str).collect();
        let applied = &modal.sort.applied_order;
        let current = applied
            .iter()
            .filter(|name| shown.contains(name.as_str()))
            .eq(order.iter());
        let reference: &[String] = if current { applied } else { &[] };
        let full = order_with_hidden(&order, &headers, reference);
        let places: HashMap<&str, usize> = full
            .iter()
            .enumerate()
            .map(|(i, name)| (name.as_str(), i))
            .collect();
        let place = |name: &String| places.get(name.as_str()).copied();
        // Everything up to the last frozen column stays frozen, hidden ones included.
        // A hidden column that ended the frozen span is known only to the applied order.
        let last_locked = applied
            .get(..modal.sort.applied_locked)
            .filter(|span| {
                current && span.iter().filter(|n| shown.contains(n.as_str())).count() == locked
            })
            .and_then(|span| span.iter().rev().find_map(place))
            .or_else(|| {
                locked
                    .checked_sub(1)
                    .and_then(|i| order.get(i))
                    .and_then(place)
            });
        modal.sort.columns = headers
            .iter()
            .map(|name| {
                let display_order = places[name.as_str()];
                SortColumn {
                    name: name.clone(),
                    // 1-based: what toggling a column in the modal assigns and what
                    // the sidebar prints.
                    sort_order: sort_columns.iter().position(|c| c == name).map(|o| o + 1),
                    sort_descending: sort_columns
                        .iter()
                        .position(|c| c == name)
                        .and_then(|i| sort_descending.get(i).copied())
                        .unwrap_or(false),
                    display_order,
                    is_locked: last_locked.is_some_and(|l| display_order <= l),
                    is_to_be_locked: false,
                    is_visible: shown.contains(name.as_str()),
                    width: state.width_choice(name),
                    shown_width: state.shown_width(name),
                }
            })
            .collect();
        modal.sort.has_unapplied_changes = false;
    }

    /// Apply everything the sidebar stages — column order and locks, the sort with
    /// its per-column directions, the filters — and close it. Enter and Ctrl+Enter,
    /// from anywhere in the sidebar.
    fn apply_sort_filter(&mut self) -> Option<AppEvent> {
        // A row still under edit is committed, never silently dropped.
        if self.sort_filter_modal.filter.editor.is_some() {
            self.sort_filter_modal.filter.commit_editor();
        }
        let (columns, descending) = self.sort_filter_modal.sort.sorted_columns_and_directions();
        let column_order = self.sort_filter_modal.sort.get_column_order();
        let locked_count = self.sort_filter_modal.sort.get_locked_columns_count();
        self.sort_filter_modal.sort.applied_order =
            self.sort_filter_modal.sort.get_full_column_order();
        self.sort_filter_modal.sort.applied_locked = self.sort_filter_modal.sort.get_locked_span();
        let statements = self.sort_filter_modal.filter.statements.clone();
        // Widths read nothing, so they apply here; a fit measures the rows on screen
        // when the table is next drawn. With nothing else changed the view stays
        // where it is, on the page the fit was asked for: applying the order, filters
        // and sort again would read the rows afresh from the top.
        let view_unchanged = self.data_table_state.as_mut().is_some_and(|state| {
            state.set_width_choices(self.sort_filter_modal.sort.width_choices());
            state.headers() == column_order
                && state.locked_columns_count() == locked_count
                && state.view_filters() == statements.as_slice()
                && state.view_sort_columns() == columns.as_slice()
                && state.view_sort_descending() == descending.as_slice()
        });
        for col in &mut self.sort_filter_modal.sort.columns {
            col.is_to_be_locked = false;
        }
        self.sort_filter_modal.sort.has_unapplied_changes = false;
        self.sort_filter_modal.close();
        self.input_mode = InputMode::Normal;
        if view_unchanged {
            return None;
        }
        let _ = self.send_event(AppEvent::ColumnOrder(column_order, locked_count));
        let _ = self.send_event(AppEvent::Filter(statements));
        Some(AppEvent::Sort(columns, descending))
    }

    /// Which of the Info panel's optional tabs the current dataset offers.
    fn info_tabs_on_offer(&self) -> crate::widgets::info::TabsOffered {
        self.data_table_state
            .as_ref()
            .map(crate::widgets::info::TabsOffered::of)
            .unwrap_or_default()
    }

    /// Start applying `template`. Its steps are planned here, which reads nothing; a
    /// step that cannot be planned fails here and changes nothing. The reads — a pivot,
    /// then the view's first rows — run in the background, and the view is installed
    /// when they are in. One that fails there puts the view before it back (#400).
    fn apply_template(&mut self, template: &Template) -> Result<()> {
        self.jobs.supersede(|job| matches!(job, Job::ViewPivot(_)));
        let Some(state) = self.data_table_state.as_mut() else {
            return Ok(());
        };
        match state.try_transition(|s| Self::replay_view(s, &template.settings, None))? {
            (Replayed::Planned, rollback) => {
                self.view_planned(template, rollback);
                Ok(())
            }
            (Replayed::Pivot(job), rollback) => {
                // The table stays as it is while the pivot is read.
                state.roll_back(rollback);
                // Past any load-ahead for the view on screen, whose rows must not land
                // in the one that replaces it.
                self.jobs.try_advance();
                let view = Job::ViewPivot(Box::new(template.clone()));
                self.spawn_job(view, Some(Self::APPLYING_VIEW), move |_| {
                    let pivoted = job
                        .run()
                        .map_err(|e| crate::error_display::user_message_from_report(&e, None))?;
                    Ok(Answer::ViewPivoted(pivoted))
                });
                Ok(())
            }
        }
    }

    /// The view's steps are planned over `rollback`, the view it replaces: mark it
    /// applied and read its first rows. Until they are in, a failure puts `rollback`
    /// back and the view marked applied before it.
    fn view_planned(
        &mut self,
        template: &Template,
        rollback: crate::widgets::datatable::ViewRollback,
    ) {
        if let Some(path) = &self.path {
            let mut used = template.clone();
            used.last_used = Some(std::time::SystemTime::now());
            used.usage_count += 1;
            used.last_matched_file = Some(path.clone());
            let _ = self.template_manager.save_template(&used);
        }
        let previous = self.active_template_id.replace(template.id.clone());
        let Some(state) = self.data_table_state.as_ref() else {
            return;
        };
        self.query_running = Some(QueryRun {
            origin: RunOrigin::View { previous },
            frame: state.len_generation(),
            rollback,
            len_count_inflight: self.len_count_inflight,
            count_after_paint: self.count_after_paint,
            len_count_failed: self.len_count_failed,
            rows: None,
        });
        if !self.spawn_async_collect(Self::APPLYING_VIEW) {
            // Nothing to read: the view has no rows. Applied on open, it was the
            // open's last step.
            self.query_running = None;
            self.busy = false;
            self.status_message = None;
            self.first_rows_settled();
        }
    }

    /// Whether a view is being applied at the table: its pivot or its first rows are
    /// being read.
    pub(crate) fn view_applying(&self) -> bool {
        if !self.is_busy() || !self.in_normal_table_view() {
            return false;
        }
        let pivot = self
            .jobs
            .current(|job| matches!(job, Job::ViewPivot(_)))
            .is_some();
        let rows = self.query_running.as_ref().is_some_and(|run| {
            matches!(run.origin, RunOrigin::View { .. })
                && self
                    .data_table_state
                    .as_ref()
                    .is_some_and(|state| state.len_generation() == run.frame)
        });
        pivot || rows
    }

    /// Stop applying a view and keep the one before it. As with a pivot, a worker runs
    /// to the end and the bump drops its answer.
    fn cancel_view(&mut self) {
        self.jobs.advance();
        self.screen_generation = self.screen_generation.wrapping_add(1);
        if let Some(run) = self.take_query_run() {
            self.roll_back_query_run(run);
        }
        // A collect for the view, queued behind a worker, would read it after all.
        self.forget_the_rows_read();
        self.read_after_view_rollback();
        self.flash_note("View cancelled".to_string());
    }

    /// A job's outcome is in: take it, and the job's record with it, from [`Jobs`], and
    /// act on it. The record goes in this step, so the job holds the generation and
    /// the keys until its answer is handled and not after: whatever the answer starts
    /// next holds them before anything else can look.
    ///
    /// What the job held is put down here, for every job alike: a job the user waited
    /// on gives the keys back, and its line on the control bar goes with it, unless the
    /// answer goes on to a continuation, which keeps the wait up across the gap.
    fn job_ended(&mut self, ticket: Ticket) -> Option<AppEvent> {
        let jobs::Ended {
            job,
            current,
            keys,
            outcome,
            ..
        } = self.jobs.end(ticket)?;
        let cancelled_analysis = !current && Self::reads_for_analysis(&job);
        let waited = keys.is_some();
        let out = match outcome {
            Outcome::Answered(answer) => self.answered(job, current, waited, *answer),
            Outcome::Failed { message, panicked } => {
                self.background_failed(&job, current, waited, &message, panicked);
                None
            }
        };
        if let Some(status) = keys {
            if out.is_some() {
                self.busy = true;
            } else {
                self.busy = false;
                // Unless a job that is still running says the same: the read of a
                // view's rows that its pivot's answer started.
                if self.status_message.as_deref() == Some(status.as_str())
                    && !self.jobs.shows(&status)
                {
                    self.status_message = None;
                }
            }
        }
        // A cancelled analysis's worker has exited: its read is over, and once no other
        // is still going, Run can run again and Setup no longer says it waits.
        if cancelled_analysis
            && self.cancelled_analysis().is_none()
            && self.analysis_modal.data_quality_setup_note.as_deref() == Some(QUALITY_RUN_WAITS)
        {
            self.analysis_modal.data_quality_setup_note = None;
        }
        out
    }

    /// A report from a job still running, taken while the job is current.
    fn job_progress(&mut self, ticket: Ticket, progress: &Progress) {
        if !self.jobs.is_current(ticket) {
            return;
        }
        match progress {
            Progress::ExportWriting { phase, bytes } => {
                if let Some(export) = self.export_progress.as_mut() {
                    export.current_phase = phase.to_string();
                    export.written = Some(*bytes);
                }
            }
            Progress::QualityPhase(phase) => {
                if let Some(progress) = self.analysis_modal.computing.as_mut() {
                    progress.phase = phase.stage.label().to_string();
                    progress.reads_source = Some(phase.reads_source);
                    progress.interruptible = Some(phase.interruptible);
                }
            }
            Progress::Finding { rows } => self.find_progress(*rows),
            Progress::HexFinding { read, total } => self.hex_find_progress(*read, *total),
        }
    }

    /// `job` answered. `current` says whether its answer is still the one waited for:
    /// a stale one changes nothing on screen, and whatever it carries is dropped here.
    /// `waited` says the user was waiting on it.
    fn answered(
        &mut self,
        job: Job,
        current: bool,
        waited: bool,
        answer: Answer,
    ) -> Option<AppEvent> {
        match answer {
            Answer::Load(answer) => {
                // The open's to judge, by its own identity rather than the generation: an
                // answer for an open given up or replaced, or for a phase it has left,
                // changes nothing on screen, and what it carries — a download's file, a
                // dataset — is dropped with it.
                let Job::Load(load) = job else {
                    return None;
                };
                let step = self.loading.answered(
                    load,
                    *answer,
                    #[cfg(any(feature = "http", feature = "cloud"))]
                    &self.jobs,
                );
                self.run_load_step(step)
            }
            Answer::NamedPaths {
                paths,
                options,
                directory,
            } => {
                // The user left the open while its paths were looked at, or another took
                // its place.
                let Job::OpenNamed(load) = job else {
                    return None;
                };
                if !self.loading.looking_at_paths(load) {
                    return None;
                }
                // Either carries the same open on: it is still starting.
                Some(match directory {
                    Some(dir) => AppEvent::LookThenOpenDirectory(dir, *options),
                    None => AppEvent::Open(paths, *options),
                })
            }
            Answer::NamedPathMissing(path) => {
                let Job::OpenNamed(load) = job else {
                    return None;
                };
                if !self.loading.looking_at_paths(load) {
                    return None;
                }
                // The session ends saying so; nothing is opened.
                if let Some(retired) = self.loading.retire() {
                    self.put_down_load(retired);
                }
                Some(AppEvent::NamedPathMissing(path))
            }
            Answer::LookedAt {
                kind,
                holds,
                options,
            } => {
                // The user pressed Ctrl+O and went to the home screen, a newer look
                // replaced this one, or another open took its place while this was
                // reading. Their choice is the one on screen, and this is the answer to a
                // question nobody is waiting for.
                let Job::LookAtDirectory { load, path } = job else {
                    return None;
                };
                if !self.loading.looking_at_directory(load) {
                    return None;
                }
                // An `Open` that follows carries the same open on.
                self.open_the_directory_looked_at(path, kind, holds.as_deref(), *options)
            }
            Answer::Kind(found) => {
                // Superseded: a newer look, a trip away from home, or something that took
                // the screen over owns the wait, so this one touches nothing.
                let Job::Classify(asked) = job else {
                    return None;
                };
                if !current {
                    return None;
                }
                self.home.status = None;

                // A key pressed on the home screen answers on the home screen. If they
                // went back to the data, opening now would arrive from nowhere; if the
                // browse has moved, the answer is about somewhere they navigated away
                // from, and acting on it would take them back into it.
                if self.input_mode != InputMode::Home || self.home.browsing != asked.browsing {
                    return None;
                }

                let path = asked.path;
                let Some(kind) = found else {
                    self.home.status = Some(format!("No such path: {}", path.display()));
                    if asked.jump {
                        // A typo typed at `~` is worth another go without retyping it.
                        self.home.path_input = path.display().to_string();
                        self.home.path_input_active = true;
                        self.list_the_typed_directory();
                    }
                    return None;
                };
                self.open_what_it_is(path, kind, asked.jump)
            }
            Answer::Rows(result) => {
                // A stale page is dropped; the wait belongs to whatever replaced it.
                let Job::Rows(inflight) = job else {
                    return None;
                };
                if !current {
                    return None;
                }
                // Timed to here rather than to the next paint: this is the moment the
                // rows exist to be drawn, and the frame that draws them costs the same
                // whatever the page cost to fetch.
                if let Some(state) = self.data_table_state.as_ref() {
                    let took = inflight.began.elapsed();
                    log::debug!(
                        target: "datui",
                        "rows {}..{} of {}: read in {took:.1?}",
                        inflight.start,
                        inflight.end,
                        inflight.dataset
                    );
                    state.measurements().read_page(took, inflight.files);
                }
                if let Some(state) = &mut self.data_table_state {
                    state.apply_async_collect(result);
                }
                self.retire_a_count_the_rows_answered();
                self.remember_a_downloads_shape();
                // Rows a follow counted while these were read are shown next.
                self.catch_up_follow();
                // The query's first rows are in: it stands.
                let ran = self.take_query_run();
                // A load-ahead's end is nobody's wait ending: whatever else is under
                // way meanwhile keeps its spinner and its message.
                if waited {
                    self.first_rows_settled();
                    if let Some(RunOrigin::Query(mode)) = ran.map(|run| run.origin)
                        && self.query_prompt_mode() == Some(mode)
                    {
                        self.leave_query_prompt_after_run();
                    }
                }
                None
            }
            Answer::RowsFailed {
                message,
                conversion,
            } => {
                self.rows_failed(current, waited, &message, conversion.as_deref());
                None
            }
            Answer::Described(results) => {
                if current {
                    self.analysis_modal.describe_results = Some(results);
                    self.analysis_modal.computing = None;
                }
                None
            }
            Answer::Distributions(results) => {
                if current {
                    self.analysis_modal.distribution_results = Some(results);
                    self.analysis_modal.computing = None;
                }
                None
            }
            Answer::Correlations(results) => {
                if current {
                    self.analysis_modal.correlation_results = Some(results);
                    self.analysis_modal.computing = None;
                }
                None
            }
            Answer::DataQuality {
                results,
                kept,
                plan,
            } => {
                // Kept whatever became of the run's results: the rows are the rows the
                // key names, and a read is not to be thrown away.
                if let Some(kept) = kept {
                    self.retain_quality_sample(&kept);
                }
                if current
                    && self.analysis_modal.active
                    && self.analysis_modal.selected_tool
                        == Some(analysis_modal::AnalysisTool::DataQuality)
                {
                    // Labeled with the plan it was dispatched with, whatever has been
                    // staged since.
                    self.cache_quality_result(&results, (*plan).clone());
                    self.analysis_modal.data_quality_last_plan = Some(*plan);
                    self.analysis_modal.data_quality_results = Some(*results);
                    self.analysis_modal.data_quality_from_cache = false;
                    self.analysis_modal
                        .set_quality_page(crate::data_quality::QualityPage::Overview);
                    self.analysis_modal.computing = None;
                }
                None
            }
            Answer::Sample { df, label } => {
                if current {
                    self.analysis_modal.computing = None;
                    self.show_sample_view(df, label);
                }
                None
            }
            Answer::Pivoted { spec, pivoted } => {
                // Superseded means something replaced the view, which owns the wait.
                if !current {
                    return None;
                }
                let installed = self.data_table_state.as_mut().map(|state| {
                    state
                        .deferred(|s| s.install_pivot(&spec, pivoted))
                        .map_err(|e| crate::error_display::user_message_from_report(&e, None))
                });
                match installed {
                    Some(Ok(())) => {
                        self.pivot_melt_modal.close();
                        // Only from the modal: a trip home meanwhile stays home.
                        if self.input_mode == InputMode::PivotMelt {
                            self.input_mode = InputMode::Normal;
                        }
                        // The wait passes to the read of its rows.
                        self.spawn_async_collect(Self::LOADING_BUFFER);
                    }
                    Some(Err(message)) => self.error_modal.show(message),
                    None => {}
                }
                None
            }
            Answer::ViewPivoted(pivoted) => {
                // Superseded means the view was cancelled or something replaced it, which
                // owns the wait.
                let Job::ViewPivot(template) = job else {
                    return None;
                };
                if !current {
                    return None;
                }
                let planned = self.data_table_state.as_mut().map(|state| {
                    // Nothing changed while the pivot was read, so the steps before
                    // it plan as they did; this time the pivot is in hand.
                    state
                        .try_transition(|s| Self::replay_view(s, &template.settings, Some(pivoted)))
                        .map(|(_, rollback)| rollback)
                        .map_err(|e| e.to_string())
                });
                match planned {
                    // The wait passes to the read of its rows.
                    Some(Ok(rollback)) => self.view_planned(&template, rollback),
                    Some(Err(message)) => self.view_pivot_failed(&message),
                    None => {}
                }
                None
            }
            Answer::DrillRow { group_index, row } => {
                // Superseded means something replaced the view, which owns the wait.
                if current {
                    self.drill_into(group_index, &row);
                }
                None
            }
            Answer::FieldsRead(values) => {
                // Superseded means something replaced the view, which owns the wait.
                let Job::InspectRow { frame, row } = job else {
                    return None;
                };
                if !current {
                    return None;
                }
                let asked = self
                    .inspector_modal
                    .read
                    .as_ref()
                    .is_some_and(|read| read.key() == (frame, row));
                if self.inspector_modal.active && asked {
                    self.inspector_modal.read =
                        Some(inspector_modal::FieldRead::Read { frame, row, values });
                }
                None
            }
            Answer::JsonParsed(root) => {
                // Superseded means something replaced the view, which owns the wait.
                let Job::InspectJson { token } = job else {
                    return None;
                };
                if !current || !self.inspector_modal.active {
                    return None;
                }
                let modal = &mut self.inspector_modal;
                if let Some(wait) = modal.json_wait.take_if(|w| w.token == token) {
                    let node = inspector_drill::Node::Json {
                        root,
                        path: Vec::new(),
                    };
                    modal.drill_in(wait.frame, wait.row, wait.label, node);
                }
                None
            }
            Answer::Indented(text) => {
                let Job::InspectPretty { token } = job else {
                    return None;
                };
                let modal = &mut self.inspector_modal;
                if current
                    && let Some(inspector_modal::Pretty::Pending { token: t, place }) =
                        modal.pretty.as_ref()
                    && *t == token
                {
                    modal.pretty = Some(inspector_modal::Pretty::Ready {
                        place: place.clone(),
                        text,
                    });
                }
                None
            }
            Answer::Unpacked(decoded) => {
                let Job::InspectUnpack { token } = job else {
                    return None;
                };
                let modal = &mut self.inspector_modal;
                if current
                    && let Some(inspector_modal::Unpack::Pending { token: t, place }) =
                        modal.unpack.as_ref()
                    && *t == token
                {
                    modal.unpack = Some(inspector_modal::Unpack::Ready {
                        place: place.clone(),
                        text: std::sync::Arc::new(decoded),
                    });
                }
                None
            }
            Answer::ValueWritten(open) => {
                if current && self.inspector_modal.active {
                    self.external_open = Some(open);
                }
                None
            }
            Answer::Exported(path) => {
                if current {
                    self.export_progress = None;
                    self.flash_note(format!("Exported to {}", path.display()));
                }
                None
            }
            Answer::Copied { payload, message } => {
                if current {
                    self.export_progress = None;
                    self.finish_copy(payload, message);
                }
                None
            }
            Answer::QualityReportWritten(path) => {
                if current {
                    self.flash_note(format!("Report written to {}", path.display()));
                }
                None
            }
            Answer::ChartExported => {
                // Leaving the chart's dataset supersedes the write: one that finishes
                // after Ctrl-O must not reopen its modal over the home screen.
                if let Job::ChartExport { path, format } = job
                    && current
                {
                    self.finish_chart_export(&path, format, Ok(()));
                }
                None
            }
            Answer::FileFacts(facts) => {
                if let Job::FileFacts { dataset } = job {
                    self.file_facts_landed(dataset, facts);
                }
                None
            }
            Answer::Found(found) => {
                if let Job::Find(run) = job {
                    self.find_answered(run, current, found);
                }
                None
            }
            Answer::HexOpened(source) => {
                self.hex_opened(job, current, *source);
                None
            }
            Answer::HexFound(hit) => {
                self.hex_found(job, current, hit);
                None
            }
            Answer::ValueCounts(counts) => {
                // Superseded means the screen moved on: another column, a cancel, a
                // trip away.
                if current {
                    self.value_counts.computing = None;
                    self.value_counts.hold(*counts);
                }
                None
            }
            // What a test's answer carries goes with it.
            #[cfg(test)]
            Answer::Probe(held) => {
                drop(held);
                None
            }
        }
    }

    /// Put down what a failed background operation started, and say why.
    ///
    /// The keys and the line it held are put down by [`Self::job_ended`]. Each arm
    /// clears only what the job itself started, and only when the job is current: a
    /// load-ahead that dies leaves the analysis beside it running, and an older look at
    /// a path leaves the newer one waiting. One that is not current is dropped.
    fn background_failed(
        &mut self,
        job: &Job,
        current: bool,
        waited: bool,
        message: &str,
        panicked: bool,
    ) {
        match job {
            // Judged by the open, as its answers are: one put down or replaced is not the
            // open the user is waiting on.
            Job::Load(load) | Job::OpenNamed(load) | Job::LookAtDirectory { load, .. } => {
                if let loading::Step::Failed(failed) = self.loading.failed(*load, message) {
                    self.load_failed(failed);
                }
            }
            Job::Classify(_) => {
                if current {
                    self.home.status = None;
                    self.error_modal.show(message.to_string());
                }
            }
            Job::Rows(_) | Job::OwedRows { .. } => self.rows_failed(current, waited, message, None),
            Job::Analysis(_) | Job::SampleRows => {
                if current {
                    self.analysis_modal.computing = None;
                    self.error_modal.show(message.to_string());
                }
            }
            // The form stays up with its spec, to be fixed.
            Job::Pivot | Job::Copy | Job::QualityReport => {
                if current {
                    self.error_modal.show(message.to_string());
                }
            }
            Job::ViewPivot(_) => {
                if current {
                    self.view_pivot_failed(message);
                }
            }
            Job::DrillRow => {
                // The grouped view stays as it was. A flash has one line, and a panic's
                // message is an internal error with the log's path under it: the log
                // has the details.
                if current {
                    self.flash_note(if panicked {
                        "Could not drill in; see the log".to_string()
                    } else {
                        format!("Could not drill in: {message}")
                    });
                }
            }
            Job::InspectJson { token } => {
                let modal = &mut self.inspector_modal;
                if current && let Some(wait) = modal.json_wait.take_if(|w| w.token == *token) {
                    modal.not_json = Some((wait.frame, wait.row, wait.path));
                    self.flash_note(if panicked {
                        "Could not read the JSON; see the log".to_string()
                    } else {
                        sentence(message)
                    });
                }
            }
            Job::InspectPretty { token } => {
                let modal = &mut self.inspector_modal;
                if let Some(inspector_modal::Pretty::Pending { token: t, place }) =
                    modal.pretty.as_ref()
                    && t == token
                {
                    modal.pretty = Some(inspector_modal::Pretty::Failed {
                        place: place.clone(),
                    });
                }
            }
            Job::InspectUnpack { token } => {
                let modal = &mut self.inspector_modal;
                if let Some(inspector_modal::Unpack::Pending { token: t, place }) =
                    modal.unpack.as_ref()
                    && t == token
                {
                    modal.unpack = Some(inspector_modal::Unpack::Failed {
                        place: place.clone(),
                    });
                }
            }
            Job::OpenValue => {
                if current {
                    self.flash_note(if panicked {
                        "Could not open the value; see the log".to_string()
                    } else {
                        format!("Could not open the value: {message}")
                    });
                }
            }
            Job::InspectRow { frame, row } => {
                if current {
                    let asked = self
                        .inspector_modal
                        .read
                        .as_ref()
                        .is_some_and(|read| read.key() == (*frame, *row));
                    if asked {
                        // The pane has room for the reason; a panic's is the log's.
                        let message = if panicked {
                            "Could not read the field; see the log".to_string()
                        } else {
                            format!("Could not read the field: {message}")
                        };
                        self.inspector_modal.read = Some(inspector_modal::FieldRead::Failed {
                            frame: *frame,
                            row: *row,
                            message,
                        });
                    }
                }
            }
            Job::Export => {
                if current {
                    self.export_progress = None;
                    self.error_modal.show(message.to_string());
                }
            }
            Job::ChartExport { path, format } => {
                if current {
                    self.finish_chart_export(path, *format, Err(message.to_string()));
                }
            }
            Job::Find(_) => self.find_failed(current, message),
            Job::HexOpen { .. } => {
                if current {
                    self.error_modal.show(message.to_string());
                }
            }
            Job::HexFind(_) => {
                if current {
                    self.status_message = None;
                    self.flash_note(message.to_string());
                }
            }
            Job::ValueCounts => {
                // Said on the screen, in place of the counts.
                if current && let Some(computing) = self.value_counts.computing.take() {
                    let why = if panicked {
                        "could not count; see the log".to_string()
                    } else {
                        message.to_string()
                    };
                    self.value_counts.failed = Some((computing.column, why));
                }
            }
            // Judged by the dataset, as its answer is.
            Job::FileFacts { dataset } => {
                // The panel has one line for it, and a panic's message is an internal
                // error with the log's path under it.
                let why = if panicked {
                    "could not read; see the log".to_string()
                } else {
                    message.to_string()
                };
                self.file_facts_landed(*dataset, FileFacts::Failed(why));
            }
        }
    }

    /// Ask a worker for the open file's size and footer, unless this dataset has
    /// already asked. A source with no file on this machine has none to ask for.
    ///
    /// No lease and no busy state. The answer is judged by `dataset_generation`, which
    /// a bump does not change, so a bump cannot strand it; leased, a slow stat would
    /// hold the next buffer collect behind it. And busy would hold the keys typed at
    /// the panel, Esc included, behind a read the panel already says it is waiting on.
    fn read_file_facts(&mut self) {
        let dataset = self.dataset_generation;
        if self.data_table_state.is_none()
            || self
                .file_facts
                .as_ref()
                .is_some_and(|(read, _)| *read == dataset)
            || self.file_facts_reading()
        {
            return;
        }
        // One file on this machine, or nothing: a glob is no file to stat, and several
        // files are not the first one's size.
        let several = self
            .opened
            .as_ref()
            .is_some_and(|(paths, _)| paths.len() > 1);
        let piped = self.reads_stdin();
        let Some(path) = self.path.clone().filter(|path| {
            !several
                && !piped
                && !source::is_remote_url(path)
                && !source::is_prefix_or_glob(&path.to_string_lossy())
        }) else {
            return;
        };
        let parquet = self.original_file_format == Some(ExportFormat::Parquet);
        #[cfg(test)]
        let read: FileFactsReader = self
            .file_facts_reader
            .clone()
            .unwrap_or_else(|| Arc::new(FileFacts::read));
        #[cfg(not(test))]
        let read = FileFacts::read;
        self.spawn_job(Job::FileFacts { dataset }, None, move |_| {
            Ok(Answer::FileFacts(read(&path, parquet)?))
        });
    }

    /// Whether the open dataset's file facts are being read.
    pub(crate) fn file_facts_reading(&self) -> bool {
        let dataset = self.dataset_generation;
        self.jobs
            .current(|job| matches!(job, Job::FileFacts { dataset: asked } if *asked == dataset))
            .is_some()
    }

    /// The file facts read for `dataset`, kept if that is still the dataset on screen.
    /// An answer for one replaced since is about a file no longer there.
    fn file_facts_landed(&mut self, dataset: u64, facts: FileFacts) {
        if dataset == self.dataset_generation {
            self.file_facts = Some((dataset, facts));
        }
    }

    /// What the Info panel knows about the open file: `None` until it is asked, and
    /// for a source with no file on this machine. Installing a dataset clears it, so
    /// what is here is the open dataset's.
    pub fn file_facts(&self) -> Option<&FileFacts> {
        Self::facts_shown(
            &self.file_facts,
            self.dataset_generation,
            self.file_facts_reading(),
        )
    }

    /// [`Self::file_facts`], from the fields it reads, for a caller holding the rest
    /// of the app.
    pub(crate) fn facts_shown(
        read: &Option<(u64, FileFacts)>,
        dataset: u64,
        reading: bool,
    ) -> Option<&FileFacts> {
        static READING: FileFacts = FileFacts::Reading;
        match read {
            Some((read_for, facts)) if *read_for == dataset => Some(facts),
            _ => reading.then_some(&READING),
        }
    }

    /// An open found a database of several tables: the home screen lists them, as it
    /// lists a directory of separate tables.
    fn land_on_tables(&mut self, tables: loading::Tables) {
        let loading::Tables {
            database,
            from_home,
        } = tables;
        self.status_message = None;
        self.busy = false;
        self.enter_home();
        if from_home {
            self.home_browse_into(database);
        } else {
            self.home_jump_into(database);
        }
    }

    /// An open failed before its first rows; the loader has put it down. The dataset
    /// already up is the current one again, or the home screen is, when that is where
    /// the open was chosen.
    fn load_failed(&mut self, failed: loading::Failed) {
        let loading::Failed { message, from_home } = failed;
        self.status_message = None;
        self.busy = false;
        // Kept so the home screen can say why, if dismissing the error lands the user
        // there from a command line that named the file. Chosen at home, the dialog
        // has said it, and the prompt's line saying it again was the same failure
        // reported twice (#547 D8).
        if from_home {
            self.last_load_error = None;
            self.enter_home();
        } else {
            self.last_load_error = Some(message.clone());
        }
        self.error_modal.show(message);
    }

    /// The table's rows could not be read. A query or view waiting on them is not
    /// applied (#400, #432); a page the table waited on ends the wait with the reason;
    /// a load-ahead's failure is left for the page that needs those rows.
    fn rows_failed(
        &mut self,
        current: bool,
        waited: bool,
        message: &str,
        conversion: Option<&crate::error_display::ConversionFailure>,
    ) {
        if !current {
            return;
        }
        let message = &self.named_by_source(message);
        if let Some(run) = self.take_query_run() {
            self.fail_query_run(run, message, conversion);
            return;
        }
        if !waited {
            return;
        }
        // A count waiting for this page to paint would read the frame that just
        // failed to: it fails with it, the way a count riding in the collect does.
        if let Some(generation) = self.count_after_paint.take() {
            if self.len_count_inflight == Some(generation) {
                self.len_count_inflight = None;
            }
            if self
                .data_table_state
                .as_ref()
                .is_some_and(|state| state.len_generation() == generation)
            {
                self.len_count_failed = Some(generation);
            }
        }
        self.first_rows_settled();
        self.error_modal.show(message.to_string());
    }

    /// `message` with the temporary files the dataset on screen reads (a download, a
    /// decompressed copy) called by what the user opened.
    fn named_by_source(&self, message: &str) -> String {
        let (Some(state), Some(source)) = (self.data_table_state.as_ref(), self.path.as_deref())
        else {
            return message.to_string();
        };
        state
            .temp_files()
            .into_iter()
            .fold(message.to_string(), |message, file| {
                crate::error_display::named_by_source(&message, file, source)
            })
    }

    /// A view's pivot could not be read or planned: the view before it stays.
    fn view_pivot_failed(&mut self, message: &str) {
        self.error_modal
            .show(format!("Error applying view: {message}"));
        self.read_after_view_rollback();
    }

    /// A query or view whose first rows could not be read is not applied: put back
    /// what it replaced and say why where its origin says to.
    fn fail_query_run(
        &mut self,
        run: QueryRun,
        message: &str,
        conversion: Option<&crate::error_display::ConversionFailure>,
    ) {
        let rows = run.rows;
        let origin = self.roll_back_query_run(run);
        self.first_rows_settled();
        self.status_message = None;
        self.busy = false;
        // Run from the prompt, the reason goes under the query, which stays open to
        // be fixed. Sent any other way — a view applied — there is nothing to edit,
        // and the error modal says why.
        let mode = match origin {
            RunOrigin::View { .. } => {
                self.error_modal
                    .show(format!("Error applying view: {message}"));
                self.read_after_view_rollback();
                return;
            }
            RunOrigin::Query(mode) if self.query_prompt_mode() == Some(mode) => mode,
            RunOrigin::Query(_) => {
                self.error_modal.show(message.to_string());
                return;
            }
        };
        let sql = mode == QueryMode::Sql;
        self.query_run_error = Some(match conversion {
            Some(failure) if sql => failure.sql_message(rows),
            _ => message.to_string(),
        });
        self.inline_failures = self.inline_failures.wrapping_add(1);
    }

    /// Put back the view a running query or view replaced, with its row count, and
    /// return where the query came from.
    fn roll_back_query_run(&mut self, run: QueryRun) -> RunOrigin {
        if let Some(state) = self.data_table_state.as_mut() {
            state.roll_back(run.rollback);
        }
        self.len_count_inflight = run.len_count_inflight;
        self.count_after_paint = run.count_after_paint;
        self.len_count_failed = run.len_count_failed;
        if let RunOrigin::View { previous } = &run.origin {
            self.active_template_id = previous.clone();
        }
        run.origin
    }

    /// The view before a failed or cancelled one is back: read its rows if it has none
    /// on hand, as when the view was applied on open, else stop being busy.
    fn read_after_view_rollback(&mut self) {
        self.busy = false;
        self.status_message = None;
        if !self.spawn_async_collect(Self::LOADING_BUFFER) {
            self.first_rows_settled();
        }
    }

    /// Run a view's steps on `state` in the order they were built. With a pivot or melt:
    /// the query, filters and sort it ran over, the reshape, then the query, filters and
    /// sort on its result. Without one: the query, filters and sort. Column order last.
    /// Stops at the first step that fails, and at a pivot unless `pivoted` holds it.
    fn replay_view(
        state: &mut DataTableState,
        settings: &template::TemplateSettings,
        pivoted: Option<DataFrame>,
    ) -> Result<Replayed> {
        if settings.pivot.is_some() || settings.melt.is_some() {
            if let Some(source) = &settings.reshape_source {
                Self::replay_query(
                    state,
                    source.sql_query.as_deref(),
                    source.query.as_deref(),
                    source.fuzzy_query.as_deref(),
                )?;
                Self::replay_filters_and_sort(
                    state,
                    &source.filters,
                    &source.sort_columns,
                    source.sort_directions(),
                )?;
            }
            let reshaped = match (&settings.pivot, &settings.melt, pivoted) {
                (Some(spec), _, Some(pivoted)) => state.install_pivot(spec, pivoted),
                (Some(spec), _, None) => {
                    Self::check_plan(state)?;
                    return Ok(Replayed::Pivot(Box::new(state.plan_pivot(spec))));
                }
                (None, Some(spec), _) => state.melt(spec),
                (None, None, _) => Ok(()),
            };
            reshaped.map_err(|e| {
                color_eyre::eyre::eyre!(
                    "{}",
                    crate::error_display::user_message_from_report(&e, None)
                )
            })?;
        }
        Self::replay_query(
            state,
            settings.sql_query.as_deref(),
            settings.query.as_deref(),
            settings.fuzzy_query.as_deref(),
        )?;
        Self::replay_filters_and_sort(
            state,
            &settings.filters,
            &settings.sort_columns,
            settings.sort_directions(),
        )?;
        if !settings.column_order.is_empty() {
            state.set_column_order(settings.column_order.clone());
            state.set_locked_columns(settings.locked_columns_count);
        }
        Self::check_plan(state)?;
        Ok(Replayed::Planned)
    }

    /// Whether the frame the steps so far built can be read, by its plan alone.
    fn check_plan(state: &DataTableState) -> Result<()> {
        state.check_plan().map_err(|e| {
            color_eyre::eyre::eyre!("{}", crate::error_display::user_message_from_polars(&e))
        })
    }

    /// A view's query: SQL or q-style (at most one is stored), then a search.
    fn replay_query(
        state: &mut DataTableState,
        sql: Option<&str>,
        dsl: Option<&str>,
        fuzzy: Option<&str>,
    ) -> Result<()> {
        let stated = |q: Option<&str>| q.filter(|q| !q.trim().is_empty()).map(str::to_string);
        if let Some(sql) = stated(sql) {
            state.sql_query(sql);
        } else if let Some(query) = stated(dsl) {
            state.query(query);
        }
        if state.error().is_none()
            && let Some(fuzzy) = stated(fuzzy)
        {
            state.fuzzy_search(fuzzy);
        }
        match state.error().cloned() {
            Some(error) => Err(color_eyre::eyre::eyre!(
                "{}",
                crate::error_display::user_message_from_polars(&error)
            )),
            None => Ok(()),
        }
    }

    /// A view's sidebar filters, then its sort.
    fn replay_filters_and_sort(
        state: &mut DataTableState,
        filters: &[FilterStatement],
        sort_columns: &[String],
        descending: Vec<bool>,
    ) -> Result<()> {
        if !filters.is_empty() {
            state.filter(filters.to_vec());
            if let Some(error) = state.error().cloned() {
                return Err(color_eyre::eyre::eyre!("{}", error));
            }
        }
        if !sort_columns.is_empty() {
            state.sort_by(sort_columns.to_vec(), descending);
            if let Some(error) = state.error().cloned() {
                return Err(color_eyre::eyre::eyre!("{}", error));
            }
        }
        Ok(())
    }

    /// What the status line says while an export writes its file.
    fn export_write_phase(request: &ExportRequest) -> &'static str {
        if request.options.compression(request.format).is_some() {
            "Writing and compressing file"
        } else {
            "Writing file"
        }
    }

    /// What the error modal says when writing an export, report or chart fails.
    fn format_export_error(error: &color_eyre::eyre::Report, path: &Path) -> String {
        use std::io::{self, ErrorKind};

        for cause in error.chain() {
            if let Some(io_err) = cause.downcast_ref::<io::Error>() {
                // Matched by type, not kind: an encoder's own errors share
                // kinds such as InvalidInput with the destination checks.
                let msg = match (crate::output_file::Refused::of(io_err), io_err.kind()) {
                    (Some(refused), _) => format!("{refused}."),
                    // A CSV open in a spreadsheet app, on Windows.
                    (None, _) if crate::error_display::held_by_another_program(io_err) => {
                        "it is open in another program; close it there and try again.".to_string()
                    }
                    (None, ErrorKind::PermissionDenied) => "permission denied.".to_string(),
                    (None, ErrorKind::IsADirectory) => "it is a directory.".to_string(),
                    (None, _) => crate::error_display::user_message_from_io(io_err, None),
                };
                return format!("Cannot write to {}: {}", path.display(), msg);
            }
            if let Some(pe) = cause.downcast_ref::<polars::prelude::PolarsError>() {
                let msg = crate::error_display::user_message_from_polars(pe);
                return format!("Export failed: {}", msg);
            }
        }
        let error_str = error.to_string();
        let first_line = error_str.lines().next().unwrap_or("Unknown error").trim();
        format!("Export failed: {}", first_line)
    }

    /// Above this estimated size a table copy asks first: most paste targets
    /// choke long before it, and the clipboard holds the whole thing at once.
    const COPY_CONFIRM_BYTES: usize = 10 * 1024 * 1024;
    /// Above this a table copy is refused outright; a file is the medium for
    /// data this size, and export writes one without holding it all in text.
    const COPY_REFUSE_BYTES: usize = 200 * 1024 * 1024;

    /// Enter in the copy dialog: the synchronous scopes copy from the buffer
    /// and flash; the table scope guards on size, then collects off-thread.
    fn perform_copy(&mut self) -> Option<AppEvent> {
        use copy_modal::{CopyScope, thousands};
        /// What Enter decided, worked out under the table borrow and acted on
        /// after it: writing to the clipboard needs the whole app back.
        enum Planned {
            Copy(clipboard::Payload, String),
            Collect,
            /// None: the size is not known (the row count is still coming, or a
            /// binary column's width is known to no footer).
            Confirm(Option<usize>),
        }
        let format = self.copy_modal.format;
        let header = self.copy_modal.header();
        let scope = self.copy_modal.scope;
        // What the destination takes decides what is built: no HTML flavor for one
        // that cannot offer it, and no copy past its cap.
        let accepts = match self.copy_destination() {
            Ok(destination) => destination.accepts(),
            Err(e) => {
                self.copy_modal.close();
                self.input_mode = InputMode::Normal;
                self.error_modal.show(e);
                return None;
            }
        };
        let planned: Result<Planned, String> = match self.data_table_state.as_ref() {
            None => Err("Nothing to copy: no table is open".to_string()),
            Some(state) => match scope {
                CopyScope::Cell => {
                    let column = self.copy_modal.column.clone().unwrap_or_default();
                    match state.copy_cell_value(&column) {
                        Some(value) => {
                            let row = state.selected_display_row().unwrap_or(0);
                            Ok(Planned::Copy(
                                clipboard::Payload::text(value),
                                format!("Copied cell {column} of row {}", thousands(row)),
                            ))
                        }
                        None => Err("Nothing to copy: the current row is not buffered".to_string()),
                    }
                }
                CopyScope::Row => match state.copy_row_df() {
                    Some(df) => clipboard::tabular_payload(&df, format, header, accepts.html).map(
                        |payload| {
                            let row = state.selected_display_row().unwrap_or(0);
                            Planned::Copy(
                                payload,
                                format!("Copied row {} as {}", thousands(row), format.as_str()),
                            )
                        },
                    ),
                    None => Err("Nothing to copy: the current row is not buffered".to_string()),
                },
                CopyScope::View => match state.copy_view_df() {
                    Some(df) => clipboard::tabular_payload(&df, format, header, accepts.html).map(
                        |payload| {
                            Planned::Copy(
                                payload,
                                format!(
                                    "Copied {} rows as {}",
                                    thousands(df.height()),
                                    format.as_str()
                                ),
                            )
                        },
                    ),
                    None => Err("Nothing to copy: no rows are on screen".to_string()),
                },
                CopyScope::Python => Ok(Planned::Copy(
                    clipboard::Payload::text(self.python_script(state)),
                    "Copied the view as Python".to_string(),
                )),
                CopyScope::Table => {
                    // A capped destination's copy is read only as far as its cap, so
                    // what could be held is the smaller of the two.
                    let cap = accepts
                        .base64_limit
                        .map_or(usize::MAX, |limit| limit / 4 * 3);
                    match state.estimated_copy_bytes() {
                        Some(bytes) if bytes > Self::COPY_REFUSE_BYTES => Err(format!(
                            "The table is about {} — too much to hold on a clipboard. \
                             Export it to a file instead (e).",
                            Self::format_bytes(bytes as u64)
                        )),
                        Some(bytes) if bytes.min(cap) > Self::COPY_CONFIRM_BYTES => {
                            Ok(Planned::Confirm(Some(bytes)))
                        }
                        Some(_) => Ok(Planned::Collect),
                        None if cap <= Self::COPY_CONFIRM_BYTES => Ok(Planned::Collect),
                        // The row count has not landed yet, or a binary column's width
                        // is unknown, so the size is anyone's guess: ask before
                        // collecting an unknown amount.
                        None => Ok(Planned::Confirm(None)),
                    }
                }
            },
        };
        self.copy_modal.close();
        self.input_mode = InputMode::Normal;
        match planned {
            Ok(Planned::Copy(payload, message)) => {
                self.finish_copy(payload, message);
                None
            }
            Ok(Planned::Collect) => Some(AppEvent::CopyTable { format, header }),
            Ok(Planned::Confirm(bytes)) => {
                self.pending_copy = Some((format, header));
                let counting = self
                    .data_table_state
                    .as_ref()
                    .is_some_and(|state| state.num_rows_if_valid().is_none());
                self.confirmation_modal.show(match bytes {
                    Some(bytes) => format!(
                        "This copies about {} to the clipboard.\n\nCopy the whole table?",
                        Self::format_bytes(bytes as u64)
                    ),
                    None if counting => "The table's size is not known yet — the row count \
                                         is still being read.\n\nCopy the whole table anyway?"
                        .to_string(),
                    None => "The size of the table's binary columns is not known.\n\n\
                             Copy the whole table anyway?"
                        .to_string(),
                });
                None
            }
            Err(message) => {
                self.error_modal.show(message);
                None
            }
        }
    }

    /// The view on screen as a Python Polars script: the open's reader, then every
    /// step that made the view. See [`python_script`].
    pub fn python_script(&self, state: &DataTableState) -> String {
        let (paths, options) = match &self.opened {
            Some((paths, options)) => (Some(paths.as_slice()), options.clone()),
            None => (None, OpenOptions::default()),
        };
        let cloud = options.effective_cloud(&self.app_config.cloud);
        let mut options = options;
        if options.read_python.is_empty() {
            // A decompressed file is read into its dataset directly, not through a scan.
            options.read_python = state.read_python().to_vec();
        }
        // The source an `s3://<id>@bucket` URL names has its own endpoint and region.
        let remote = paths
            .and_then(|paths| paths.first())
            .map(|p| p.to_string_lossy().into_owned())
            .filter(|p| source::is_remote_url(Path::new(p)));
        let source_of = remote
            .as_deref()
            .and_then(|url| source::split_source_id(url).0)
            .and_then(|id| cloud.connections.iter().find(|c| c.name == id));
        // What this session learned of the place: read unsigned, it is public.
        #[cfg(feature = "cloud")]
        let unsigned = remote
            .as_deref()
            .and_then(crate::cloud_sources::known_access)
            .unwrap_or(false);
        #[cfg(not(feature = "cloud"))]
        let unsigned = false;
        let record = python_script::OpenRecord {
            paths,
            options: &options,
            format: state.read_as().or(options.format),
            schema: state.source_schema(),
            remote_objects: state
                .remote_objects()
                .unwrap_or_default()
                .into_iter()
                .map(|object| object.url)
                .collect(),
            s3_endpoint: source_of
                .and_then(|c| c.endpoint_url.clone())
                .or(cloud.s3_endpoint_url.clone())
                .filter(|s| !s.trim().is_empty()),
            s3_region: source_of
                .and_then(|c| c.region.clone())
                .or(cloud.s3_region.clone())
                .filter(|s| !s.trim().is_empty()),
            unsigned,
            read_as_text: state.read_as_text().iter().map(|c| c.to_string()).collect(),
            spec: state.format_read().map(|read| read.spec.name.clone()),
        };
        python_script::Script {
            source: python_script::source(&record),
            steps: state.python_steps(),
        }
        .render()
    }

    /// Hand a payload to the clipboard destination, building the destination
    /// at the first copy, and flash or raise the error modal — a copy that
    /// silently did nothing would be worse than one that failed out loud.
    fn finish_copy(&mut self, payload: clipboard::Payload, message: String) {
        let written = self
            .copy_destination()
            .and_then(|destination| destination.write(payload));
        match written {
            Ok(()) => self.flash_note(message),
            Err(e) => self.error_modal.show(e),
        }
    }

    /// The clipboard destination, built at the first copy.
    fn copy_destination(&mut self) -> Result<&mut dyn clipboard::Destination, String> {
        if self.clipboard.is_none() {
            let choice = clipboard::BackendChoice::parse(&self.app_config.clipboard.backend)
                .unwrap_or_default();
            let limit = self.app_config.clipboard.osc52_limit_kb * 1024;
            self.clipboard = Some(clipboard::destination(choice, limit)?);
        }
        Ok(self
            .clipboard
            .as_deref_mut()
            .expect("destination just built"))
    }

    /// Whether Enter at the table opens the inspector, as Space does: there is a table,
    /// and it is not one whose rows drill into groups (a `by` view, a SQL GROUP BY).
    /// Inside a drill-down there is nothing further to drill into either.
    pub fn enter_inspects(&self) -> bool {
        self.input_mode == InputMode::Normal
            && self
                .data_table_state
                .as_ref()
                .is_some_and(|state| !state.can_drill_down())
    }

    /// Space at the table, and Enter where there is nothing to drill into: the
    /// inspector over the selected row.
    fn open_inspector(&mut self) {
        let Some(state) = self.data_table_state.as_ref() else {
            return;
        };
        if state.inspect_row().is_none() {
            self.flash_note("No row to inspect".to_string());
            return;
        }
        self.inspector_modal
            .open(state.inspect_fields(), state.current_column());
        self.input_mode = InputMode::Inspect;
    }

    /// `F` at the table: Value Counts for the column cursor's column.
    fn open_value_counts(&mut self) {
        let Some(state) = self.data_table_state.as_ref() else {
            return;
        };
        let names = state.get_column_order().to_vec();
        let Some(at) = state
            .current_column()
            .and_then(|current| names.iter().position(|n| n == current))
        else {
            return;
        };
        self.value_counts.open(names, at, state.len_generation());
        self.input_mode = InputMode::ValueCounts;
        self.count_values(false);
    }

    /// Whether the Value Counts screen is up: on its own, or under the export
    /// dialog writing its counts.
    pub(crate) fn value_counts_shown(&self) -> bool {
        self.input_mode == InputMode::ValueCounts
            || (self.input_mode == InputMode::Export && self.export_counts.is_some())
    }

    /// Whether a count for the Value Counts screen is being read while it is up.
    pub(crate) fn value_counts_computing(&self) -> bool {
        self.input_mode == InputMode::ValueCounts && self.value_counts.computing.is_some()
    }

    /// Where the export dialog goes back to: Value Counts when it is writing them.
    fn export_returns_to(&self) -> InputMode {
        if self.export_counts.is_some() {
            InputMode::ValueCounts
        } else {
            InputMode::Normal
        }
    }

    /// Count the column on the Value Counts screen, unless its counts are already
    /// held: quickly, or every row when `exact`. Off the UI thread, without holding
    /// the keys, so stepping to another column or Esc stops it.
    fn count_values(&mut self, exact: bool) {
        let Some(column) = self.value_counts.column().map(str::to_string) else {
            return;
        };
        let held_sample = self.value_counts.current().map(|c| c.is_sample());
        if held_sample == Some(false) || (held_sample == Some(true) && !exact) {
            return;
        }
        if self
            .value_counts
            .computing
            .as_ref()
            .is_some_and(|c| c.column == column && (c.exact || !exact))
        {
            return;
        }
        self.stop_value_count();
        let Some(state) = self.data_table_state.as_ref() else {
            return;
        };
        let read = if exact {
            value_counts::Read::Exact
        } else {
            value_counts::Read::Quick {
                sample_rows: self.app_config.performance.analysis_sample_rows,
                seed: self.analysis_modal.sample.seed,
                remote: state.is_remote_source(),
            }
        };
        let plan = value_counts::Plan {
            lf: state.analysis_lf(),
            column: column.clone(),
            read,
            known_total: state.num_rows_if_valid(),
            streaming: state.polars_streaming(),
        };
        let watch = crate::sampling::ReadWatch::default();
        self.value_counts.failed = None;
        self.value_counts.computing = Some(value_counts_modal::Computing {
            column,
            exact,
            watch: watch.clone(),
        });
        self.spawn_job(Job::ValueCounts, None, move |_| {
            plan.run(&watch)
                .map(|counts| Answer::ValueCounts(Box::new(counts)))
                .map_err(|e| crate::error_display::user_message_from_report(&e, None))
        });
    }

    /// Stop the count in flight, if one is: its read stops at its next batch and its
    /// answer is dropped.
    fn stop_value_count(&mut self) {
        if let Some(computing) = self.value_counts.computing.take() {
            computing.watch.stop();
            self.jobs.cancel(|job| matches!(job, Job::ValueCounts));
        }
    }

    /// Keys on the Value Counts screen.
    fn value_counts_key(&mut self, event: &KeyEvent) -> Option<AppEvent> {
        if !event.is_press() {
            return None;
        }
        let page = self.value_counts_page() as isize;
        match event.code {
            // A count still reading stops; with nothing to show for the column, Esc
            // goes on back to the table.
            KeyCode::Esc => {
                let counting = self.value_counts.counting();
                self.stop_value_count();
                if counting && self.value_counts.current().is_some() {
                    self.flash_note("Count stopped".to_string());
                } else {
                    self.input_mode = InputMode::Normal;
                }
            }
            KeyCode::Down | KeyCode::Char('j') => self.value_counts.move_by(1),
            KeyCode::Up | KeyCode::Char('k') => self.value_counts.move_by(-1),
            KeyCode::PageDown => self.value_counts.move_by(page),
            KeyCode::PageUp => self.value_counts.move_by(-page),
            KeyCode::Home => self.value_counts.move_to_start(),
            KeyCode::End | KeyCode::Char('G') => self.value_counts.move_to_end(),
            KeyCode::Left | KeyCode::Right | KeyCode::Char('h') | KeyCode::Char('l') => {
                let by = if matches!(event.code, KeyCode::Left | KeyCode::Char('h')) {
                    -1
                } else {
                    1
                };
                if self.value_counts.step(by) {
                    // The table follows, so Esc lands on the column last counted.
                    if let (Some(state), Some(column)) =
                        (self.data_table_state.as_mut(), self.value_counts.column())
                    {
                        state.set_current_column(column);
                    }
                    self.count_values(false);
                }
            }
            KeyCode::Char('s') => self.value_counts.toggle_order(),
            KeyCode::Char('a') => {
                if self.value_counts.current().is_some_and(|c| c.is_sample()) {
                    self.count_values(true);
                }
            }
            KeyCode::Enter => self.drill_into_counted_value(),
            KeyCode::Char('y') => self.copy_value_counts(),
            KeyCode::Char('e') => self.export_value_counts(),
            // The counts keep the rows they were read of; `t` counts the new ones too.
            KeyCode::Char('t') if self.follow_rows_waiting() => {
                self.stop_value_count();
                self.take_follow_rows(false);
                if let Some(state) = self.data_table_state.as_ref() {
                    let names = self.value_counts.columns.clone();
                    let at = self.value_counts.at;
                    self.value_counts.open(names, at, state.len_generation());
                }
                self.count_values(false);
            }
            _ => {}
        }
        None
    }

    /// Lines a page of the Value Counts listing moves: as many as the last frame drew.
    fn value_counts_page(&self) -> usize {
        self.value_counts.page.max(1)
    }

    /// Enter on Value Counts: the rows holding the value under the cursor, as a drill.
    fn drill_into_counted_value(&mut self) {
        let Some(kind) = self.value_counts.selected_kind() else {
            return;
        };
        let (Some(counts), Some(column)) = (
            self.value_counts.current().cloned(),
            self.value_counts.column().map(str::to_string),
        ) else {
            return;
        };
        let value = match kind {
            value_counts::LineKind::Value(at) => match counts.value(at) {
                Ok(value) => value,
                Err(_) => return,
            },
            value_counts::LineKind::Null => polars::prelude::AnyValue::Null,
            value_counts::LineKind::Other(_) => {
                self.flash_note("Other is many values: pick one to see its rows".to_string());
                return;
            }
        };
        let Some(state) = self.data_table_state.as_mut() else {
            return;
        };
        let nested = state.is_drilled_down();
        match state.deferred(|s| s.drill_into_value(&column, value)) {
            Ok(()) => {
                self.stop_value_count();
                // Inside a group already, Esc goes back past this view to the one
                // the group came from, so there are no counts to come back to.
                self.value_counts.drill_return = !nested;
                self.input_mode = InputMode::Normal;
                self.sync_sort_filter_modal();
                self.spawn_async_collect(Self::LOADING_BUFFER);
            }
            Err(e) => self.flash_note(format!(
                "Could not drill in: {}",
                crate::error_display::user_message_from_report(&e, None)
            )),
        }
    }

    /// `y` on Value Counts: every value with its count and percentages, as TSV.
    fn copy_value_counts(&mut self) {
        let Some(counts) = self.value_counts.current().cloned() else {
            return;
        };
        let order = self.value_counts.order;
        let html = match self.copy_destination() {
            Ok(destination) => destination.accepts().html,
            Err(e) => {
                self.error_modal.show(e);
                return;
            }
        };
        self.spawn_job(Job::Copy, Some("Copying..."), move |_| {
            let table = counts
                .table(order)
                .map_err(|e| format!("Copy failed: {e}"))?;
            let payload =
                crate::clipboard::tabular_payload(&table, clipboard::CopyFormat::Tsv, true, html)
                    .map_err(|e| format!("Copy failed: {e}"))?;
            Ok(Answer::Copied {
                payload,
                message: format!(
                    "Copied {} values as TSV",
                    copy_modal::thousands(table.height())
                ),
            })
        });
    }

    /// `e` on Value Counts: the export dialog, writing the counts.
    fn export_value_counts(&mut self) {
        let Some(counts) = self.value_counts.current() else {
            return;
        };
        let table = match counts.table(self.value_counts.order) {
            Ok(table) => table,
            Err(e) => {
                self.error_modal
                    .show(format!("Cannot export the counts: {e}"));
                return;
            }
        };
        self.export_modal.open(
            self.original_file_format,
            self.history_limit,
            &self.theme,
            self.original_file_delimiter,
        );
        self.export_modal.offer_source_file = false;
        self.export_modal.nested_columns = false;
        self.export_modal.avro_renames = table
            .columns()
            .iter()
            .any(|c| crate::avro_types::renames(c.name(), c.dtype()));
        self.export_counts = Some(table);
        self.input_mode = InputMode::Export;
    }

    /// `g` at the table: pick a shown column by name, bring it on screen and put the
    /// column cursor on it. Starts on the cursor's column, so ↑↓ move from there.
    fn open_go_to_column(&mut self) {
        let Some(state) = self.data_table_state.as_ref() else {
            return;
        };
        let names = state.get_column_order().to_vec();
        if names.is_empty() {
            return;
        }
        let at = state
            .current_column()
            .and_then(|current| names.iter().position(|n| n == current))
            .unwrap_or(0);
        self.go_to_column = crate::widgets::ui::PickerState::new(names);
        self.go_to_column.select_original(at);
        self.input_mode = InputMode::GoToColumn;
    }

    /// The column picker owns the keys: type to narrow, ↑↓ move, Enter goes, Esc
    /// closes without moving.
    fn go_to_column_key(&mut self, event: &KeyEvent) {
        match event.code {
            KeyCode::Esc => self.input_mode = InputMode::Normal,
            KeyCode::Enter => {
                let Some(index) = self.go_to_column.selected_original() else {
                    // Nothing matches; the picker says so and stays.
                    return;
                };
                // By name: the order may have changed under the picker since it opened.
                let name = self.go_to_column.items()[index].clone();
                if let Some(state) = self.data_table_state.as_mut() {
                    state.go_to_column(&name);
                }
                self.input_mode = InputMode::Normal;
            }
            KeyCode::Up => self.go_to_column.move_up(),
            KeyCode::Down => self.go_to_column.move_down(),
            KeyCode::Backspace => self.go_to_column.backspace(),
            KeyCode::Char(c) => self.go_to_column.filter_key(c, event.modifiers),
            _ => {}
        }
    }

    /// `b` at a table read through a format spec: the specs that could read it, the
    /// ones that matched first, to read it again with another.
    fn open_format_picker(&mut self) {
        let Some(read) = self
            .data_table_state
            .as_ref()
            .and_then(|s| s.format_read())
            .cloned()
        else {
            // Only a file read through a spec has a format to pick.
            return;
        };
        let mut names = vec![read.spec.name.clone()];
        names.extend(read.also.iter().cloned());
        for found in &self.formats.specs {
            if found.spec.layout == read.spec.layout
                && !found.spec.is_delimited()
                && !names.contains(&found.spec.name)
            {
                names.push(found.spec.name.clone());
            }
        }
        self.format_picker = crate::widgets::ui::PickerState::new(names);
        self.input_mode = InputMode::PickFormat;
    }

    /// The format picker owns the keys: type to narrow, ↑↓ move, Enter reads the file
    /// again with the spec chosen, Esc closes.
    fn format_picker_key(&mut self, event: &KeyEvent) -> Option<AppEvent> {
        match event.code {
            KeyCode::Esc => self.input_mode = InputMode::Normal,
            KeyCode::Enter => {
                let index = self.format_picker.selected_original()?;
                let name = self.format_picker.items()[index].clone();
                self.input_mode = InputMode::Normal;
                let current = self
                    .data_table_state
                    .as_ref()
                    .and_then(|s| s.format_read())
                    .map(|read| read.spec.name.clone());
                if current.as_deref() == Some(name.as_str()) {
                    return None;
                }
                let (paths, options) = self.opened.clone()?;
                let options = OpenOptions {
                    spec_name: Some(name),
                    spec_file: None,
                    spec_fetched: None,
                    spec_variant: None,
                    format_read: None,
                    sqlite: None,
                    format: None,
                    ..options
                };
                self.set_loading_phase("Scanning input", 10);
                self.name_what_is_loading(paths[0].clone());
                return Some(AppEvent::Open(paths, options));
            }
            KeyCode::Up => self.format_picker.move_up(),
            KeyCode::Down => self.format_picker.move_down(),
            KeyCode::Backspace => self.format_picker.backspace(),
            KeyCode::Char(c) => self.format_picker.filter_key(c, event.modifiers),
            _ => {}
        }
        None
    }

    fn close_inspector(&mut self) {
        self.inspector_modal.close();
        self.input_mode = InputMode::Normal;
    }

    /// Enter on a row of a grouped view: its group's rows, read off this thread
    /// when the buffer does not hold the row.
    fn drill_selected_row(&mut self) {
        let Some(state) = self.data_table_state.as_ref() else {
            return;
        };
        // An empty result has no row selected, and says so like any other.
        let drill = state
            .table_state
            .selected()
            .map(|selected| state.start_row() + selected)
            .and_then(|index| Some((index, state.drill_row(index)?)));
        match drill {
            None => self.flash_note("Nothing to drill into".to_string()),
            Some((group_index, DrillRow::Buffered(row))) => self.drill_into(group_index, &row),
            Some((group_index, DrillRow::Read(lf))) => {
                let streaming = state.polars_streaming();
                self.spawn_job(Job::DrillRow, Some(Self::READING_GROUP), move |_| {
                    let row = crate::statistics::collect_lazy(*lf, streaming)
                        .map_err(|e| crate::error_display::user_message_from_polars(&e))?;
                    Ok(Answer::DrillRow { group_index, row })
                });
            }
        }
    }

    /// The inspector's list as the row shown has it: Filled, Compare and the find
    /// text depend on the row's values, which a key may have moved.
    fn refresh_inspector_list(&mut self) {
        if let Some(state) = self.data_table_state.as_ref() {
            let visible = crate::widgets::inspector::visible_fields(&self.inspector_modal, state);
            self.inspector_modal.set_visible(visible);
        }
    }

    /// The pane for the focused value: as last drawn while that is still the
    /// focused value, else built for the key (without the table's preview, which
    /// only a frame knows).
    fn inspector_pane(&self) -> Option<crate::widgets::inspector::Pane> {
        let modal = &self.inspector_modal;
        if modal.drill.is_some() {
            return modal.pane.as_ref().map(|(_, pane)| pane.clone());
        }
        let state = self.data_table_state.as_ref()?;
        let row = state.inspect_row()?;
        let field = modal.focused()?;
        if let Some(pane) = modal.pane_for(row.frame, row.row, &field.name) {
            return Some(pane.clone());
        }
        let shown = crate::widgets::inspector::shown(field, &row, modal.read.as_ref(), state);
        Some(crate::widgets::inspector::pane(
            &field.dtype,
            &shown,
            &crate::widgets::inspector::PaneAsk {
                choice: modal.view,
                width: modal
                    .pane
                    .as_ref()
                    .map_or(80, |(key, _)| key.width as usize),
                table: None,
                indented: crate::widgets::inspector::Indented::None,
                not_json: modal.known_not_json(row.frame, row.row, &field.name),
                unpacked: modal.unpacked(&(row.frame, row.row, field.name.clone())),
                read_key: "Enter",
            },
        ))
    }

    /// The inspector's keys. Moving between rows moves the table's cursor, so the
    /// table is where the inspector left it on close.
    fn inspector_key(&mut self, event: &KeyEvent) -> Option<AppEvent> {
        if !event.is_press() {
            return None;
        }
        let modal = &mut self.inspector_modal;
        if modal.finding {
            match event.code {
                KeyCode::Esc => modal.clear_find(),
                KeyCode::Enter | KeyCode::Tab | KeyCode::Down => modal.finding = false,
                KeyCode::Up => {
                    modal.finding = false;
                    self.refresh_inspector_list();
                    self.inspector_modal.prev_field();
                    return None;
                }
                KeyCode::Backspace => modal.find_backspace(),
                KeyCode::Char(c) => modal.find_key(c, event.modifiers),
                _ => {}
            }
            // The focus follows the narrowing now, not at the next frame: a key
            // replayed before it acts on the field the find left focused.
            self.refresh_inspector_list();
            return None;
        }
        if modal.value_find.as_ref().is_some_and(|f| f.editing) {
            self.value_find_key(event);
            return None;
        }
        if event
            .modifiers
            .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT)
        {
            return None;
        }
        if modal.focus == inspector_modal::Focus::Value {
            return self.inspector_value_key(event);
        }
        if modal.drill.is_some() {
            return self.drill_key(event);
        }
        self.refresh_inspector_list();
        let modal = &mut self.inspector_modal;
        match event.code {
            KeyCode::Esc if !modal.filter.is_empty() => modal.clear_find(),
            KeyCode::Esc | KeyCode::Char(' ') => self.close_inspector(),
            KeyCode::Down | KeyCode::Char('j') => modal.next_field(),
            KeyCode::Up | KeyCode::Char('k') => modal.prev_field(),
            KeyCode::Home => modal.first_field(),
            KeyCode::End => modal.last_field(),
            KeyCode::PageDown => modal.page_fields(1),
            KeyCode::PageUp => modal.page_fields(-1),
            KeyCode::Tab => {
                if modal.focused().is_some() {
                    modal.focus = inspector_modal::Focus::Value;
                }
            }
            KeyCode::Char('/') => modal.finding = true,
            KeyCode::Char('e') => self.inspector_view(),
            KeyCode::Char('w') => self.inspector_wrap(),
            KeyCode::Char('f') => {
                modal.filled_only = !modal.filled_only;
                modal.list_offset = 0;
            }
            KeyCode::Char('s') => {
                modal.order = modal.order.next();
                modal.list_offset = 0;
            }
            KeyCode::Char('c') => {
                modal.compare = !modal.compare;
                if !modal.compare {
                    modal.filled_only = false;
                }
            }
            KeyCode::Char('m') => self.toggle_inspector_pin(),
            KeyCode::Right | KeyCode::Char('l') => return self.step_row(1),
            KeyCode::Left | KeyCode::Char('h') => return self.step_row(-1),
            KeyCode::Char('y') => self.copy_inspected_field(),
            KeyCode::Char('Y') => self.copy_inspected_row(),
            KeyCode::Char('o') => self.open_inspected_value(),
            KeyCode::Char('r') => self.read_focused_field(),
            KeyCode::Enter => return self.inspector_enter(),
            _ => {}
        }
        None
    }

    /// The keys with the focus in the value: scroll it, search it, change its view.
    fn inspector_value_key(&mut self, event: &KeyEvent) -> Option<AppEvent> {
        let Some(pane) = self.inspector_pane() else {
            self.inspector_modal.focus = inspector_modal::Focus::List;
            return None;
        };
        let modal = &mut self.inspector_modal;
        let h = modal.page.max(1);
        let content = &pane.content;
        let page = h.saturating_sub(1).max(1) as isize;
        match event.code {
            KeyCode::Esc
                if modal
                    .value_find
                    .as_ref()
                    .is_some_and(|f| !f.text.is_empty()) =>
            {
                modal.value_find = None;
            }
            KeyCode::Esc | KeyCode::Tab | KeyCode::BackTab => {
                modal.focus = inspector_modal::Focus::List;
            }
            KeyCode::Char(' ') => self.close_inspector(),
            KeyCode::Down | KeyCode::Char('j') => modal.reader.scroll(content, h, 1),
            KeyCode::Up | KeyCode::Char('k') => modal.reader.scroll(content, h, -1),
            KeyCode::PageDown => modal.reader.scroll(content, h, page),
            KeyCode::PageUp => modal.reader.scroll(content, h, -page),
            KeyCode::Home => modal.reader.home(),
            KeyCode::End => modal.reader.end(content, h),
            KeyCode::Char('/') => {
                modal.value_find = Some(inspector_modal::ValueFind {
                    editing: true,
                    pane: pane.id,
                    ..Default::default()
                });
            }
            KeyCode::Char('n') => self.next_value_hit(1),
            KeyCode::Char('N') => self.next_value_hit(-1),
            KeyCode::Char('e') => self.inspector_view(),
            KeyCode::Char('w') => self.inspector_wrap(),
            KeyCode::Char('y') => {
                if modal.drill.is_some() {
                    self.copy_drilled_item();
                } else {
                    self.copy_inspected_field();
                }
            }
            KeyCode::Char('o') if modal.drill.is_none() => self.open_inspected_value(),
            KeyCode::Right | KeyCode::Char('l') if modal.drill.is_none() => {
                return self.step_row(1);
            }
            KeyCode::Left | KeyCode::Char('h') if modal.drill.is_none() => {
                return self.step_row(-1);
            }
            _ => {}
        }
        None
    }

    /// A key typed into the value's find line. Enter finds every place and goes
    /// to the first at or after the pane's top.
    fn value_find_key(&mut self, event: &KeyEvent) {
        let pane = self.inspector_pane();
        let modal = &mut self.inspector_modal;
        let Some(find) = modal.value_find.as_mut() else {
            return;
        };
        match event.code {
            KeyCode::Esc => modal.value_find = None,
            KeyCode::Enter => {
                find.editing = false;
                let Some(pane) = pane else {
                    return;
                };
                find.hits = inspector_reader::find_hits(&pane.content, &find.text);
                find.pane = pane.id;
                let h = modal.page.max(1);
                let from = modal.reader.window(&pane.content, h).from;
                let at = find.hits.partition_point(|&p| p < from);
                find.current = (!find.hits.is_empty()).then(|| at % find.hits.len());
                if let Some(at) = find.current {
                    let pos = find.hits[at];
                    modal.reader.jump(&pane.content, h, pos);
                }
            }
            KeyCode::Backspace => {
                find.text.pop();
            }
            KeyCode::Char(c) => inspector_modal::edit_find(&mut find.text, c, event.modifiers),
            _ => {}
        }
    }

    /// `n` and `N` in the value: the next or the last place found, round the ends.
    fn next_value_hit(&mut self, step: isize) {
        let Some(pane) = self.inspector_pane() else {
            return;
        };
        let modal = &mut self.inspector_modal;
        let Some(find) = modal.value_find.as_mut() else {
            return;
        };
        if find.pane != pane.id && !find.text.is_empty() {
            find.hits = inspector_reader::find_hits(&pane.content, &find.text);
            find.pane = pane.id;
            find.current = None;
        }
        let n = find.hits.len();
        if n == 0 {
            return;
        }
        let at = match find.current {
            Some(at) => (at as isize + step).rem_euclid(n as isize) as usize,
            None if step > 0 => 0,
            None => n - 1,
        };
        find.current = Some(at);
        let pos = find.hits[at];
        let h = modal.page.max(1);
        modal.reader.jump(&pane.content, h, pos);
    }

    /// The inspector's keys inside a level drilled into: the same moves as at the
    /// row, but `→` and Enter open the focused item and `←` and Esc step back up.
    fn drill_key(&mut self, event: &KeyEvent) -> Option<AppEvent> {
        let modal = &mut self.inspector_modal;
        match event.code {
            KeyCode::Esc | KeyCode::Left | KeyCode::Char('h') => {
                modal.drill_out();
            }
            KeyCode::Char(' ') => self.close_inspector(),
            KeyCode::Down | KeyCode::Char('j') => modal.next_field(),
            KeyCode::Up | KeyCode::Char('k') => modal.prev_field(),
            KeyCode::Home => modal.first_field(),
            KeyCode::End => modal.last_field(),
            KeyCode::PageDown => modal.page_fields(1),
            KeyCode::PageUp => modal.page_fields(-1),
            KeyCode::Tab => modal.focus = inspector_modal::Focus::Value,
            KeyCode::Char('e') => self.inspector_view(),
            KeyCode::Char('w') => self.inspector_wrap(),
            KeyCode::Char('y') => self.copy_drilled_item(),
            KeyCode::Enter | KeyCode::Right | KeyCode::Char('l') => {
                let drill = modal.drill.as_ref()?;
                let (frame, row) = (drill.frame, drill.row);
                let (label, node) = drill.level().focused()?;
                let path = drill.item_key(&label);
                if node.opens() && !modal.known_not_json(frame, row, &path) {
                    self.inspector_open(frame, row, label, path, node);
                }
            }
            _ => {}
        }
        None
    }

    /// `e` in the inspector: the focused value's next view, where it has more than
    /// one. A number never has, so it never changes how a text field is then shown.
    fn inspector_view(&mut self) {
        if let Some(view) = self.inspector_pane().and_then(|pane| pane.next_view()) {
            self.inspector_modal.choose_view(view);
        }
    }

    /// `w`: word wrap or hard wrap, for every value until it is pressed again.
    fn inspector_wrap(&mut self) {
        let modal = &mut self.inspector_modal;
        modal.wrap = match modal.wrap {
            inspector_reader::Wrap::Word => inspector_reader::Wrap::Hard,
            inspector_reader::Wrap::Hard => inspector_reader::Wrap::Word,
        };
    }

    /// `m`: pin this row for Compare, or let the pin go when it is this row.
    fn toggle_inspector_pin(&mut self) {
        let Some(row) = self.data_table_state.as_ref().and_then(|s| s.inspect_row()) else {
            return;
        };
        let modal = &mut self.inspector_modal;
        let here = modal
            .pinned
            .as_ref()
            .is_some_and(|p| (p.frame, p.row) == (row.frame, row.row));
        if here {
            modal.pinned = None;
            self.flash_note("Unpinned".to_string());
        } else {
            let n = row.display_row;
            modal.pinned = Some(row);
            modal.compare = true;
            self.flash_note(format!(
                "Pinned row {}; Compare shows it",
                copy_modal::thousands(n)
            ));
        }
    }

    /// Enter in the inspector: on a group's row, its rows, as at the table; else
    /// open a nested value, or read the row's fields the buffer does not hold.
    fn inspector_enter(&mut self) -> Option<AppEvent> {
        let state = self.data_table_state.as_ref()?;
        if state.can_drill_down() {
            self.close_inspector();
            self.drill_selected_row();
            return None;
        }
        let row = state.inspect_row()?;
        let field = self.inspector_modal.focused().cloned()?;
        let shown = crate::widgets::inspector::shown(
            &field,
            &row,
            self.inspector_modal.read.as_ref(),
            state,
        );
        use crate::widgets::inspector::Shown;
        match shown {
            // A failed read is asked again: the pane said why, and Enter is the retry.
            Shown::Unread | Shown::Failed(_) => self.read_focused_field(),
            Shown::Value(ref v)
                if crate::widgets::inspector::value_opens(v)
                    && !self
                        .inspector_modal
                        .known_not_json(row.frame, row.row, &field.name) =>
            {
                let column = if field.buffered() {
                    row.values.column(&field.name).ok()
                } else {
                    self.inspector_modal
                        .read_values(row.frame, row.row)
                        .and_then(|values| values.column(&field.name).ok())
                };
                if let Some(column) = column {
                    let node =
                        inspector_drill::Node::Native(column.as_materialized_series().clone());
                    let path = inspector_drill::path_key([field.name.as_str()]);
                    self.inspector_open(row.frame, row.row, field.name.clone(), path, node);
                }
            }
            _ => {}
        }
        None
    }

    /// Read the focused row's hidden and binary fields, waited on; from then on the
    /// rows moved to are read too while the focus stays on this field.
    fn read_focused_field(&mut self) {
        let Some(state) = self.data_table_state.as_ref() else {
            return;
        };
        let Some(row) = state.inspect_row() else {
            return;
        };
        let Some(field) = self.inspector_modal.focused().cloned() else {
            return;
        };
        let shown = crate::widgets::inspector::shown(
            &field,
            &row,
            self.inspector_modal.read.as_ref(),
            state,
        );
        if matches!(
            shown,
            crate::widgets::inspector::Shown::Unread | crate::widgets::inspector::Shown::Failed(_)
        ) {
            self.inspector_modal.follow = Some(field.name.clone());
            self.read_inspected_fields(&row, true);
        }
    }

    /// What the inspector needs after a pass: the row moved to read while a read
    /// follows the rows, long JSON indented for its JSON view, and compressed
    /// bytes decompressed for their Text view. None holds the keys: moving on
    /// drops what is no longer wanted.
    fn inspector_needs(&mut self) {
        if self.input_mode != InputMode::Inspect || !self.inspector_modal.active {
            return;
        }
        let Some(state) = self.data_table_state.as_ref() else {
            return;
        };
        let Some(row) = state.inspect_row() else {
            return;
        };
        let modal = &self.inspector_modal;
        if modal.drill.is_some() {
            return;
        }
        let Some(field) = modal.focused().cloned() else {
            return;
        };
        let read_here = modal
            .read
            .as_ref()
            .is_some_and(|r| r.key() == (row.frame, row.row));
        if modal.follow.as_deref() == Some(field.name.as_str()) && !field.buffered() && !read_here {
            self.read_inspected_fields(&row, false);
            return;
        }
        // Long JSON text asked for the JSON view and not yet indented, or
        // compressed bytes asked for the Text view and not yet decompressed.
        let pane = modal.pane_for(row.frame, row.row, &field.name);
        let place = (row.frame, row.row, field.name.clone());
        let indent = pane.is_some_and(|pane| pane.indent)
            && !modal.pretty.as_ref().is_some_and(|p| *p.place() == place);
        let unpack = pane.is_some_and(|pane| pane.unpack)
            && !modal.unpack.as_ref().is_some_and(|u| *u.place() == place);
        if !indent && !unpack {
            return;
        }
        let column = if field.buffered() {
            row.values.column(&field.name).ok().cloned()
        } else {
            modal
                .read_values(row.frame, row.row)
                .and_then(|values| values.column(&field.name).ok().cloned())
        };
        let Some(column) = column else {
            return;
        };
        let modal = &mut self.inspector_modal;
        if unpack {
            modal.unpack_token += 1;
            let token = modal.unpack_token;
            modal.unpack = Some(inspector_modal::Unpack::Pending { token, place });
            self.spawn_job(Job::InspectUnpack { token }, None, move |_| {
                let value = column.get(0).map_err(|e| e.to_string())?;
                let bytes = match &value {
                    polars::prelude::AnyValue::Binary(b) => *b,
                    polars::prelude::AnyValue::BinaryOwned(b) => b.as_slice(),
                    _ => return Err("not bytes".to_string()),
                };
                inspector_bytes::decode_text(bytes, inspector_bytes::sniff(bytes))
                    .map(Answer::Unpacked)
                    .ok_or_else(|| "not text".to_string())
            });
            return;
        }
        modal.pretty_token += 1;
        let token = modal.pretty_token;
        modal.pretty = Some(inspector_modal::Pretty::Pending { token, place });
        self.spawn_job(Job::InspectPretty { token }, None, move |_| {
            let value = column.get(0).map_err(|e| e.to_string())?;
            let text = match &value {
                polars::prelude::AnyValue::String(s) => *s,
                polars::prelude::AnyValue::StringOwned(s) => s.as_str(),
                _ => return Err("not text".to_string()),
            };
            let json = inspector_drill::parse_json(text)?;
            let (pretty, _) = inspector_drill::json_text(&json, true, usize::MAX);
            Ok(Answer::Indented(std::sync::Arc::from(pretty)))
        });
    }

    /// The value the inspector wrote for another program, for the run loop.
    pub fn take_external_open(&mut self) -> Option<external_open::ExternalOpen> {
        self.external_open.take()
    }

    /// Whether the session reports the mouse, to take it again after a program
    /// had the terminal.
    pub fn mouse_enabled(&self) -> bool {
        self.app_config.display.mouse
    }

    /// The run loop opened `open`: a program that waited is done with its file;
    /// a failure is said on the bar.
    pub fn external_opened(&mut self, open: &external_open::ExternalOpen, failed: Option<String>) {
        let program = external_open::program_for(open.document, |name| std::env::var(name).ok());
        if matches!(program, external_open::Program::Wait(_)) {
            let _ = std::fs::remove_file(&open.path);
        }
        match failed {
            Some(e) => self.flash_note(format!("Could not open the value: {e}")),
            None if matches!(program, external_open::Program::Opener(_)) => {
                self.flash_note("Opened in the system viewer".to_string())
            }
            None => {}
        }
    }

    /// `y` in the inspector: the focused value as its view shows it — the stored
    /// value exact, indented JSON in the JSON view, the text bytes hold in their
    /// Text view, bytes otherwise as base64 — through the same clipboard path as
    /// the copy dialog. One over a capped destination's limit is refused
    /// unformatted; a large one is written off this thread.
    fn copy_inspected_field(&mut self) {
        use copy_modal::thousands;
        let Some(state) = self.data_table_state.as_ref() else {
            return;
        };
        let (Some(row), Some(field)) = (state.inspect_row(), self.inspector_modal.focused()) else {
            return;
        };
        let field = field.clone();
        let column = if field.buffered() {
            row.values.column(&field.name).ok().cloned()
        } else {
            self.inspector_modal
                .read_values(row.frame, row.row)
                .and_then(|values| values.column(&field.name).ok().cloned())
        };
        let Some(column) = column else {
            self.flash_note(format!("{} is not read yet; Enter reads it", field.name));
            return;
        };
        let message = format!(
            "Copied {} of row {}",
            field.name,
            thousands(row.display_row)
        );
        use crate::widgets::inspector::CopyAs;
        match self.inspector_pane().map(|pane| pane.copy) {
            Some(CopyAs::Text(text)) => self.copy_string(text.to_string(), message),
            Some(CopyAs::Escaped) => {
                let text = column
                    .get(0)
                    .map(|v| crate::exact::escaped(&crate::exact::value_text(&v)))
                    .unwrap_or_default();
                self.copy_string(text, message);
            }
            _ => self.copy_value(column, message),
        }
    }

    /// Copy `text`, refused when it is over a capped destination's limit.
    fn copy_string(&mut self, text: String, message: String) {
        let limit = match self.copy_destination() {
            Ok(destination) => destination.accepts().base64_limit,
            Err(e) => {
                self.error_modal.show(e);
                return;
            }
        };
        if let Some(limit) = limit
            && text.len() > limit / 4 * 3
        {
            self.error_modal
                .show(clipboard::over_osc52_limit(None, limit));
            return;
        }
        self.finish_copy(clipboard::Payload::text(text), message);
    }

    /// `Y` in the inspector: the whole row as one JSON object, exact, without
    /// leaving. Fields not read are left out, and the flash counts them.
    fn copy_inspected_row(&mut self) {
        use copy_modal::thousands;
        let Some(state) = self.data_table_state.as_ref() else {
            return;
        };
        let Some(row) = state.inspect_row() else {
            return;
        };
        let fields = self.inspector_modal.fields.clone();
        let read = self.inspector_modal.read.clone();
        let display = row.display_row;
        let size = row.values.estimated_size()
            + self
                .inspector_modal
                .read_values(row.frame, row.row)
                .map_or(0, |v| v.estimated_size());
        let build = move || {
            let (json, kept, unread) =
                crate::widgets::inspector::row_json(&fields, &row, read.as_ref());
            let message = if unread > 0 {
                format!(
                    "Copied row {}: {} fields, {} not read",
                    thousands(display),
                    thousands(kept),
                    thousands(unread)
                )
            } else {
                format!("Copied row {} as JSON", thousands(display))
            };
            (json, message)
        };
        if size <= Self::FIELD_COPY_INLINE_BYTES {
            let (json, message) = build();
            self.copy_string(json, message);
            return;
        }
        let limit = match self.copy_destination() {
            Ok(destination) => destination.accepts().base64_limit,
            Err(e) => {
                self.error_modal.show(e);
                return;
            }
        };
        self.spawn_job(Job::Copy, Some("Copying..."), move |_| {
            let (json, message) = build();
            if let Some(limit) = limit
                && json.len() > limit / 4 * 3
            {
                return Err(clipboard::over_osc52_limit(None, limit));
            }
            Ok(Answer::Copied {
                payload: clipboard::Payload::text(json),
                message,
            })
        });
    }

    /// `o` in the inspector: the value written to a file of its own, in the view
    /// it is shown in, for another program to open; see [`external_open`].
    fn open_inspected_value(&mut self) {
        let Some(state) = self.data_table_state.as_ref() else {
            return;
        };
        let Some(row) = state.inspect_row() else {
            return;
        };
        let Some(field) = self.inspector_modal.focused().cloned() else {
            return;
        };
        let column = if field.buffered() {
            row.values.column(&field.name).ok().cloned()
        } else {
            self.inspector_modal
                .read_values(row.frame, row.row)
                .and_then(|values| values.column(&field.name).ok().cloned())
        };
        let Some(column) = column else {
            self.flash_note(format!("{} is not read yet; Enter reads it", field.name));
            return;
        };
        if self.open_dir.is_none() {
            match tempfile::Builder::new().prefix("datui-values-").tempdir() {
                Ok(dir) => self.open_dir = Some(dir),
                Err(e) => {
                    self.flash_note(format!("Could not open the value: {e}"));
                    return;
                }
            }
        }
        let dir = self
            .open_dir
            .as_ref()
            .map(|d| d.path().to_path_buf())
            .unwrap_or_default();
        let shown_as = self.inspector_pane().map(|pane| pane.copy);
        let name = field.name.clone();
        let display = row.display_row;
        self.spawn_job(Job::OpenValue, Some("Writing the value..."), move |_| {
            use crate::widgets::inspector::CopyAs;
            let value = column.get(0).map_err(|e| e.to_string())?;
            let (bytes, extension, document): (Vec<u8>, &str, bool) = match (&shown_as, &value) {
                (Some(CopyAs::Text(text)), _) => {
                    let ext = if inspector_drill::looks_like_json(text) {
                        "json"
                    } else {
                        "txt"
                    };
                    (text.as_bytes().to_vec(), ext, false)
                }
                (_, polars::prelude::AnyValue::Binary(b)) => {
                    let kind = inspector_bytes::sniff(b);
                    (
                        b.to_vec(),
                        kind.map_or("bin", |k| k.extension()),
                        kind.is_some_and(|k| k.is_document()),
                    )
                }
                (_, polars::prelude::AnyValue::BinaryOwned(b)) => {
                    let kind = inspector_bytes::sniff(b);
                    (
                        b.clone(),
                        kind.map_or("bin", |k| k.extension()),
                        kind.is_some_and(|k| k.is_document()),
                    )
                }
                (_, v) if crate::exact::is_nested_value(v) => {
                    let text = crate::exact::copy_text(&column).map_err(|e| e.to_string())?;
                    (text.into_bytes(), "json", false)
                }
                (_, v) => {
                    let text = crate::exact::value_text(v);
                    let trimmed = text.trim_start();
                    let ext = if inspector_drill::looks_like_json(&text) {
                        "json"
                    } else if trimmed.starts_with('<') {
                        "xml"
                    } else {
                        "txt"
                    };
                    (text.into_bytes(), ext, false)
                }
            };
            let file = external_open::file_name(&name, display, extension);
            let path =
                external_open::write_value(&dir, &file, &bytes).map_err(|e| e.to_string())?;
            Ok(Answer::ValueWritten(external_open::ExternalOpen {
                path,
                document,
            }))
        });
    }

    /// Move the table's cursor `delta` rows, reading the next page in the
    /// background when the buffer does not hold the row: the table's own ↑↓.
    fn step_row(&mut self, delta: i64) -> Option<AppEvent> {
        let state = self.data_table_state.as_mut()?;
        if state.scroll_would_trigger_collect(delta) {
            self.busy = true;
            return Some(if delta > 0 {
                AppEvent::DoScrollNext
            } else {
                AppEvent::DoScrollPrev
            });
        }
        if delta > 0 {
            state.select_next();
        } else {
            state.select_previous();
        }
        None
    }

    /// The inspector's keys. Moving between rows moves the table's cursor, so the
    /// table is where the inspector left it on close.
    /// Open `node` as a level under the one shown: a list or struct at once, text as
    /// the JSON it holds, parsed here when short and on a worker when long. `path`
    /// is the text's place, remembered when it does not parse.
    fn inspector_open(
        &mut self,
        frame: u64,
        row: usize,
        label: String,
        path: String,
        node: inspector_drill::Node,
    ) {
        use inspector_drill::{JSON_INLINE_BYTES, Node, Shape};
        if node.shape() != Shape::Leaf {
            self.inspector_modal.drill_in(frame, row, label, node);
            return;
        }
        let Some(len) = node
            .with_text(|s| inspector_drill::opens_as_json(s).then_some(s.len()))
            .flatten()
        else {
            return;
        };
        // Short text is parsed on this key.
        if len <= JSON_INLINE_BYTES {
            match node.with_text(inspector_drill::parse_json) {
                Some(Ok(value)) => {
                    let node = Node::Json {
                        root: std::sync::Arc::new(value),
                        path: Vec::new(),
                    };
                    self.inspector_modal.drill_in(frame, row, label, node);
                }
                Some(Err(e)) => {
                    self.inspector_modal.not_json = Some((frame, row, path));
                    self.flash_note(sentence(&e));
                }
                None => {}
            }
            return;
        }
        let token = self.inspector_modal.wait_for_json(frame, row, label, path);
        // The worker reads the text where it is: the node is a one-row slice or a
        // shared document, so nothing up to the 4 MiB cap is copied to hand it over.
        self.spawn_job(
            Job::InspectJson { token },
            Some(Self::READING_JSON),
            move |_| match node.with_text(inspector_drill::parse_json) {
                Some(parsed) => parsed.map(|value| Answer::JsonParsed(std::sync::Arc::new(value))),
                None => Err("not JSON: not text".to_string()),
            },
        );
    }

    /// `y` inside a drill: the focused item's whole value, exact, as `y` copies a
    /// field; a JSON object or array as indented JSON.
    fn copy_drilled_item(&mut self) {
        use inspector_drill::Node;
        let Some(drill) = self.inspector_modal.drill.as_ref() else {
            return;
        };
        let Some((label, node)) = drill.level().focused() else {
            return;
        };
        let g = crate::glyphs::get();
        let mut path: Vec<&str> = drill.levels.iter().map(|l| l.label.as_str()).collect();
        path.push(&label);
        let display_row = self
            .data_table_state
            .as_ref()
            .and_then(|s| s.inspect_row())
            .map_or(drill.row + 1, |r| r.display_row);
        let message = format!(
            "Copied {} of row {}",
            path.join(&format!(" {} ", g.trail)),
            copy_modal::thousands(display_row)
        );
        match node {
            Node::Native(series) => self.copy_value(polars::prelude::Column::from(series), message),
            Node::Json { .. } => {
                let limit = match self.copy_destination() {
                    Ok(destination) => destination.accepts().base64_limit,
                    Err(e) => {
                        self.error_modal.show(e);
                        return;
                    }
                };
                // Formatted here up to a size that is quick; past it, on a worker.
                let cap = limit.map_or(Self::JSON_COPY_MAX_BYTES, |limit| limit / 4 * 3);
                let quick = cap.min(Self::FIELD_COPY_INLINE_BYTES);
                let null = serde_json::Value::Null;
                let value = node.json().unwrap_or(&null);
                if let Some(text) = inspector_drill::json_copy_text(value, quick) {
                    self.finish_copy(clipboard::Payload::text(text), message);
                    return;
                }
                if let Some(limit) = limit.filter(|_| cap <= Self::FIELD_COPY_INLINE_BYTES) {
                    self.error_modal
                        .show(clipboard::over_osc52_limit(None, limit));
                    return;
                }
                // The worker resolves the path itself: the document is shared, not copied.
                self.spawn_job(Job::Copy, Some("Copying..."), move |_| {
                    let value = node.json().unwrap_or(&serde_json::Value::Null);
                    let text =
                        inspector_drill::json_copy_text(value, cap).ok_or_else(|| match limit {
                            Some(limit) => clipboard::over_osc52_limit(None, limit),
                            None => "Copy failed: the value is too large to copy".to_string(),
                        })?;
                    Ok(Answer::Copied {
                        payload: clipboard::Payload::text(text),
                        message,
                    })
                });
            }
        }
    }

    /// Read, off this thread, the fields of `row` the buffer does not hold — the
    /// hidden columns and the binary ones — with the shown columns beside them, so a
    /// sort that orders ties differently on a second read cannot pass another row's
    /// fields off as this one's. `wait`: the user waits on it, as on Enter; a read
    /// that follows the rows does not hold the keys.
    fn read_inspected_fields(&mut self, row: &crate::widgets::datatable::InspectRow, wait: bool) {
        let Some(state) = self.data_table_state.as_ref() else {
            return;
        };
        let fields = state.inspect_fields();
        let wanted: Vec<String> = fields
            .iter()
            .filter(|f| !f.buffered())
            .map(|f| f.name.clone())
            .collect();
        let check: Vec<String> = fields
            .iter()
            .filter(|f| f.buffered())
            .map(|f| f.name.clone())
            .collect();
        let columns: Vec<String> = wanted.iter().chain(check.iter()).cloned().collect();
        let lf = match state.inspect_read_lf(row.row, &columns) {
            Ok(lf) => lf,
            Err(e) => {
                self.inspector_modal.read = Some(inspector_modal::FieldRead::Failed {
                    frame: row.frame,
                    row: row.row,
                    message: crate::error_display::user_message_from_polars(&e),
                });
                return;
            }
        };
        let expected = row.values.select(check.iter().map(String::as_str)).ok();
        let streaming = state.polars_streaming();
        let (frame, index) = (row.frame, row.row);
        self.inspector_modal.read = Some(inspector_modal::FieldRead::Reading { frame, row: index });
        self.spawn_job(
            Job::InspectRow { frame, row: index },
            wait.then_some(Self::READING_FIELDS),
            move |_| {
                let read = crate::statistics::collect_lazy(lf, streaming)
                    .map_err(|e| crate::error_display::user_message_from_polars(&e))?;
                if read.height() != 1 {
                    return Err("the row is no longer in the view".to_string());
                }
                let same = expected.is_some_and(|expected| {
                    read.select(check.iter().map(String::as_str))
                        .is_ok_and(|again| again.equals_missing(&expected))
                });
                if !same {
                    return Err("the view's order of equal rows changed on reading again; \
                         sort by a column that tells the rows apart"
                        .to_string());
                }
                let values = read
                    .select(wanted.iter().map(String::as_str))
                    .map_err(|e| e.to_string())?;
                Ok(Answer::FieldsRead(values))
            },
        );
    }

    /// Copy the one value of `column`, exact, and flash `message`.
    fn copy_value(&mut self, column: polars::prelude::Column, message: String) {
        // Destination first, as the copy dialog's: a value over the terminal's cap
        // is refused before it is formatted, here or on a worker.
        let limit = match self.copy_destination() {
            Ok(destination) => destination.accepts().base64_limit,
            Err(e) => {
                self.error_modal.show(e);
                return;
            }
        };
        if let Some(limit) = limit {
            let fits = limit / 4 * 3;
            let over = column
                .get(0)
                .is_ok_and(|value| crate::exact::copy_len_floor(&value, fits) > fits);
            if over {
                self.error_modal
                    .show(clipboard::over_osc52_limit(None, limit));
                return;
            }
        }
        if column.as_materialized_series().estimated_size() <= Self::FIELD_COPY_INLINE_BYTES {
            match crate::exact::copy_text(&column) {
                Ok(text) => self.finish_copy(clipboard::Payload::text(text), message),
                Err(e) => self.error_modal.show(format!("Copy failed: {e}")),
            }
            return;
        }
        self.spawn_job(Job::Copy, Some("Copying..."), move |_| {
            let text = crate::exact::copy_text(&column).map_err(|e| format!("Copy failed: {e}"))?;
            Ok(Answer::Copied {
                payload: clipboard::Payload::text(text),
                message,
            })
        });
    }

    /// Replace the clipboard destination, so tests can watch what a copy sends
    /// without a display server or a terminal in the loop.
    pub fn set_clipboard_destination(&mut self, destination: Box<dyn clipboard::Destination>) {
        self.clipboard = Some(destination);
    }

    pub fn create_template_from_current_state(
        &mut self,
        name: String,
        description: Option<String>,
        match_criteria: template::MatchCriteria,
    ) -> Result<template::Template> {
        let settings = if let Some(state) = &self.data_table_state {
            let (query, sql_query, fuzzy_query) = active_query_settings(
                state.get_active_query(),
                state.get_active_sql_query(),
                state.get_active_fuzzy_query(),
            );
            template::TemplateSettings {
                query,
                sql_query,
                fuzzy_query,
                filters: state.get_filters().to_vec(),
                sort_columns: state.get_sort_columns().to_vec(),
                sort_descending: state.get_sort_descending().to_vec(),
                sort_ascending: state.get_sort_ascending(),
                column_order: state.get_column_order().to_vec(),
                locked_columns_count: state.locked_columns_count(),
                pivot: state.last_pivot_spec().cloned(),
                melt: state.last_melt_spec().cloned(),
                reshape_source: state.reshape_source().cloned(),
            }
        } else {
            template::TemplateSettings {
                query: None,
                sql_query: None,
                fuzzy_query: None,
                filters: Vec::new(),
                sort_columns: Vec::new(),
                sort_descending: Vec::new(),
                sort_ascending: true,
                column_order: Vec::new(),
                locked_columns_count: 0,
                pivot: None,
                melt: None,
                reshape_source: None,
            }
        };

        self.template_manager
            .create_template(name, description, match_criteria, settings)
    }

    /// The query prompt's mode while it is open.
    pub fn query_prompt_mode(&self) -> Option<QueryMode> {
        (self.input_mode == InputMode::Editing && self.input_type == Some(InputType::Search))
            .then_some(self.query_mode)
    }

    /// The mode `/` opens on: the active query's own, so editing never
    /// reinterprets it in another language; otherwise the configured default.
    fn opening_query_mode(&self) -> QueryMode {
        let active = self.data_table_state.as_ref().and_then(|state| {
            if !state.get_active_sql_query().trim().is_empty() {
                Some(QueryMode::Sql)
            } else if !state.get_active_fuzzy_query().trim().is_empty() {
                Some(QueryMode::Search)
            } else if !state.get_active_query().trim().is_empty() {
                Some(QueryMode::QStyle)
            } else {
                None
            }
        });
        active
            .unwrap_or(self.app_config.query.default_mode)
            .resolve()
    }

    /// Switch the prompt's mode. Each mode keeps its own text; an error from the
    /// last run belongs to the mode that ran it.
    fn set_query_mode(&mut self, mode: QueryMode) {
        self.query_mode = mode.resolve();
        if let Some(state) = &mut self.data_table_state {
            state.dismiss_error();
        }
        self.query_run_error = None;
        self.sync_query_focus();
    }

    /// Tab in the SQL input: complete the column name or table name being typed,
    /// and on further presses step through the other names that match.
    fn complete_sql_name(&mut self) {
        let line = self
            .sql_input
            .line_at(self.sql_input.cursor_line())
            .unwrap_or_default()
            .to_string();
        let value = self.sql_input.value().to_string();
        let Some(step) = sql_assist::tab(
            &self.sql_columns,
            &line,
            self.sql_input.cursor_col(),
            &value,
            self.sql_input.cursor(),
            &mut self.sql_completion,
        ) else {
            return;
        };
        self.sql_input
            .replace_before_cursor(step.span, &step.insert);
        sql_assist::landed(
            &mut self.sql_completion,
            self.sql_input.value(),
            self.sql_input.cursor(),
        );
    }

    /// The columns of `df` the word at the SQL cursor could name, for the list
    /// under the input: every column while nothing is being typed.
    pub(crate) fn sql_column_matches(&self) -> Vec<&(String, DataType)> {
        let line = self
            .sql_input
            .line_at(self.sql_input.cursor_line())
            .unwrap_or_default();
        let word = sql_assist::word_before(line, self.sql_input.cursor_col())
            .map(|w| w.text)
            .unwrap_or_default();
        sql_assist::matching(&self.sql_columns, &word)
    }

    /// The text in the query prompt's current mode, while the prompt is open.
    pub fn query_prompt_text(&self) -> Option<&str> {
        Some(match self.query_prompt_mode()? {
            QueryMode::Sql => self.sql_input.value(),
            QueryMode::Search => self.fuzzy_input.value(),
            QueryMode::QStyle => self.query_input.value(),
        })
    }

    /// Why the last run failed, for the line under the input: a statement that
    /// failed while running, else one that could not be planned.
    pub fn query_prompt_error(&self) -> Option<String> {
        if let Some(error) = &self.query_run_error {
            return Some(error.clone());
        }
        let state = self.data_table_state.as_ref()?;
        let error = state.error()?;
        Some(if self.query_mode == QueryMode::Sql {
            crate::error_display::sql_error_message(error, state.sql_table_rows())
        } else {
            crate::error_display::user_message_from_polars(error)
        })
    }

    /// Bumped each time a running statement's failure is put in the prompt.
    pub fn inline_failures(&self) -> u64 {
        self.inline_failures
    }

    /// Plan a query in `mode` and read its first rows in the background. A query that
    /// cannot be planned leaves its error on the state, where the prompt shows it. One
    /// that plans stays pending — the prompt open, when it came from there — until its
    /// rows are in; if they fail, the view it replaced comes back.
    fn run_query(&mut self, mode: QueryMode, text: &str, status: &str) {
        self.query_run_error = None;
        let Some(state) = self.data_table_state.as_mut() else {
            return;
        };
        let rollback = state.rollback_point();
        let rows = state.sql_table_rows();
        // A query that cannot be planned changes nothing and leaves its error showing.
        state.deferred(|s| match mode {
            QueryMode::Sql => s.sql_query(text.to_string()),
            QueryMode::QStyle => s.query(text.to_string()),
            QueryMode::Search => s.fuzzy_search(text.to_string()),
        });
        if state.error().is_some() {
            return;
        }
        self.query_running = Some(QueryRun {
            origin: RunOrigin::Query(mode),
            frame: state.len_generation(),
            rollback,
            len_count_inflight: self.len_count_inflight,
            count_after_paint: self.count_after_paint,
            len_count_failed: self.len_count_failed,
            rows,
        });
        if !self.spawn_async_collect(status) {
            // Nothing to read: the rows on hand already show it.
            self.query_running = None;
            if self.query_prompt_mode() == Some(mode) {
                self.leave_query_prompt_after_run();
            }
        }
    }

    /// The query still running over the frame on screen, taken. One whose frame has
    /// since been replaced is dropped: its rollback would undo what replaced it.
    fn take_query_run(&mut self) -> Option<QueryRun> {
        let run = self.query_running.take()?;
        let frame = self.data_table_state.as_ref()?.len_generation();
        (run.frame == frame).then_some(run)
    }

    /// A query ran and its rows are in: the prompt closes on them.
    fn leave_query_prompt_after_run(&mut self) {
        self.sql_completion = None;
        self.input_mode = InputMode::Normal;
        self.input_type = None;
        self.sql_input.set_focused(false);
        self.query_input.set_focused(false);
        self.fuzzy_input.set_focused(false);
        if let Some(state) = &mut self.data_table_state {
            state.suppress_error_display = false;
        }
    }

    /// Only the current mode's input carries the cursor, and only while the
    /// input, not the tab bar, has focus.
    fn sync_query_focus(&mut self) {
        let input = self.query_focus == QueryFocus::Input;
        let mode = self.query_mode;
        self.sql_input.set_focused(input && mode == QueryMode::Sql);
        self.fuzzy_input
            .set_focused(input && mode == QueryMode::Search);
        self.query_input
            .set_focused(input && mode == QueryMode::QStyle);
    }

    /// Esc from anywhere in the prompt: nothing runs and nothing typed survives.
    fn close_query_prompt(&mut self) {
        self.query_run_error = None;
        self.sql_completion = None;
        self.query_input.clear();
        self.sql_input.clear();
        self.fuzzy_input.clear();
        self.query_input.set_focused(false);
        self.sql_input.set_focused(false);
        self.fuzzy_input.set_focused(false);
        self.input_mode = InputMode::Normal;
        self.input_type = None;
        if let Some(state) = &mut self.data_table_state {
            state.dismiss_error();
            state.suppress_error_display = false;
        }
    }

    /// Rows the active search matched, once the count is settled, while the
    /// Search input still holds the words that ran. A sidebar filter on top
    /// makes the row count something else, so then there is none to show.
    pub(crate) fn search_match_count(&self) -> Option<usize> {
        let state = self.data_table_state.as_ref()?;
        let ran = state.get_active_fuzzy_query();
        (!ran.trim().is_empty()
            && self.fuzzy_input.value() == ran
            && state.get_filters().is_empty()
            && !state.is_drilled_down()
            && state.is_num_rows_valid()
            && !self.row_count_pending())
        .then_some(state.num_rows())
    }

    fn get_help_info(&self) -> (String, String) {
        let (title, content) = match self.input_mode {
            InputMode::Normal => ("Table Help", help_strings::main_view()),
            InputMode::Editing => match self.input_type {
                Some(InputType::Search) => ("Query Help", help_strings::query()),
                Some(InputType::Find) => ("Find Help", help_strings::find()),
                _ => ("Go to Line", help_strings::go_to_line()),
            },
            InputMode::SortFilter => ("Sort & Filter Help", help_strings::sort_filter()),
            InputMode::PivotMelt => ("Pivot & Melt Help", help_strings::pivot_melt()),
            InputMode::Export => ("Export Help", help_strings::export()),
            InputMode::Copy => ("Copy Help", help_strings::copy()),
            InputMode::Inspect => ("Inspector Help", help_strings::inspector()),
            InputMode::GoToColumn => ("Go to Column", help_strings::go_to_column()),
            InputMode::PickFormat => ("Format Help", help_strings::format_picker()),
            InputMode::Info => ("Info Panel Help", help_strings::info_panel()),
            InputMode::Chart => ("Chart Help", help_strings::chart()),
            InputMode::Home => ("Home Help", help_strings::home()),
            InputMode::Hex => ("Hex View Help", help_strings::hex_view()),
            InputMode::ValueCounts => ("Value Counts Help", help_strings::value_counts()),
        };
        (title.to_string(), content.to_string())
    }
}

impl Widget for &mut App {
    fn render(self, area: Rect, buf: &mut Buffer) {
        self.begin_frame();
        self.debug.num_frames += 1;
        if self.debug.enabled {
            self.debug.show_help_at_render = self.show_help;
        }

        use crate::render::context::RenderContext;
        use crate::render::layout::app_layout;
        use crate::render::main_view::MainViewContent;

        let ctx = RenderContext::from_theme_and_config(
            &self.theme,
            self.table_cell_padding,
            self.column_colors,
            self.number_format.clone(),
        )
        .with_dtype_row(self.dtype_row);

        let main_view_content = MainViewContent::current(self);

        Clear.render(area, buf);
        let background_color = self.color("background");
        Block::default()
            .style(Style::default().bg(background_color))
            .render(area, buf);

        let app_layout = app_layout(area, self.debug.enabled);
        let main_area = app_layout.main_view;
        Clear.render(main_area, buf);

        crate::render::main_view_render::render_main_view(area, main_area, buf, self, &ctx);

        // Status messages are shown inline in the control bar (no overlay popups).

        if self.confirmation_modal.active {
            crate::render::overlays::render_confirmation_modal(
                area,
                buf,
                &mut self.confirmation_modal,
                &ctx,
            );
        }
        if self.error_modal.active {
            crate::render::overlays::render_error_modal(area, buf, &mut self.error_modal, &ctx);
        }
        if self.show_help
            || (self.template_modal.active && self.template_modal.show_help)
            || (self.analysis_modal.active && self.analysis_modal.show_help)
        {
            let (title, text): (String, String) =
                if self.analysis_modal.active && self.analysis_modal.show_help {
                    crate::render::analysis_view::help_title_and_text(&self.analysis_modal)
                } else if self.template_modal.active {
                    ("Views Help".to_string(), help_strings::views().to_string())
                } else {
                    let (t, txt) = self.get_help_info();
                    (t.to_string(), txt.to_string())
                };
            crate::render::overlays::render_help_overlay(
                area,
                buf,
                &title,
                &text,
                &mut self.help_scroll,
                &ctx,
            );
        }

        let row_count = self.data_table_state.as_ref().map(|s| s.num_rows());
        // The spinner follows the glyph set, so it cannot disagree with the rest of
        // the chrome about whether the terminal is doing UTF-8.
        let use_unicode_throbber = crate::glyphs::active_is_unicode();
        let mut controls = Controls::from_context(row_count.unwrap_or(0), &ctx)
            .with_unicode_throbber(use_unicode_throbber);

        // Derive the status message from the open in flight, an export, or the explicit
        // status message. An export started over an open's first rows is the one the
        // user is waiting on.
        let load = self
            .load_shown()
            .filter(|_| self.awaiting_dataset() || self.export_progress.is_none());
        let status_msg = match (load, &self.export_progress) {
            // The load is paused on the user; the bar names the modal's keys instead.
            (Some(_), _) if self.awaiting_download_confirmation() => None,
            (Some((current_phase, progress_percent, ..)), _) => {
                let current_phase = self.loading_phase(current_phase);
                // The percentage is a constant per phase, which was harmless beside a
                // phase name and is not beside a real fraction: 1,203 of 6,541 is 18%,
                // and "(40%)" next to it reads as that count's progress. The same
                // number the phase was built from, so a pass that ends mid-frame
                // cannot leave the count showing with the percentage back beside it.
                let counting = self.footers_this_frame.is_some();
                if progress_percent > 0 && !counting {
                    Some(format!("{}... ({}%)", current_phase, progress_percent))
                } else {
                    Some(format!("{}...", current_phase))
                }
            }
            (
                None,
                Some(ExportProgress {
                    current_phase,
                    written,
                    file_path,
                }),
            ) => {
                let filename = file_path.file_name().and_then(|f| f.to_str()).unwrap_or("");
                // The count last, so its changing width moves nothing.
                Some(match written {
                    Some(bytes) => format!(
                        "{}...  {}  {}",
                        current_phase,
                        filename,
                        crate::discover::format_size(*bytes)
                    ),
                    None => format!("{}...  {}", current_phase, filename),
                })
            }
            (None, None) => {
                if self.fetch_too_young_to_mention() {
                    None
                } else if self.is_busy() {
                    self.status_message.clone()
                } else if let Some((read, total)) = self
                    .footers_this_frame
                    .filter(|_| self.dataset_is_still_reading_its_footers())
                {
                    // The dataset opened from two footers and is still learning the
                    // rest. Said quietly, because nothing is wrong and nothing is
                    // blocked: the columns it finds will join what is already here.
                    Some(format!(
                        "Reading footers: {} of {}...",
                        crate::numfmt::group_chrome(read),
                        crate::numfmt::group_chrome(total)
                    ))
                } else if self.chart_preparing() {
                    Some("Preparing chart...".to_string())
                } else if main_view_content == MainViewContent::Datatable {
                    // Whatever is on the line, busy or not. An End waiting on a remote
                    // count parks without setting `busy` — keys go on working meanwhile,
                    // which is the point of parking — so both the message explaining the
                    // wait and the one saying the count failed were written here and
                    // painted by nothing.
                    //
                    // Only at the table, because that is what these messages are about.
                    // A parked End survives Ctrl+O, and the home screen has a caption and
                    // a row count of its own: shown there it would replace every key chip
                    // on the bar with a sentence about a dataset the user has left.
                    //
                    // Not every message needs this branch. The one `jump_key` puts up
                    // while a footer pass is running is superseded by the footers line
                    // above, which says the same thing with numbers.
                    self.status_message.clone()
                } else {
                    None
                }
            }
        };
        let status_msg = status_msg.map(|msg| {
            if self.input_dropped && self.is_busy() {
                format!("{msg}  input dropped while busy")
            } else {
                msg
            }
        });
        controls = controls.with_status_message(status_msg);
        controls = controls.with_flash(self.flash.as_ref().map(|f| f.message.clone()));
        controls = controls.with_reshaped(self.data_table_state.as_ref().and_then(|s| {
            if s.last_pivot_spec().is_some() {
                Some("pivoted")
            } else if s.last_melt_spec().is_some() {
                Some("melted")
            } else {
                None
            }
        }));
        controls = controls.with_not_the_table(
            self.data_table_state
                .as_ref()
                .and_then(|s| s.not_the_table()),
        );
        controls = controls.with_follow(self.follow_mark());
        let format_read = self.data_table_state.as_ref().and_then(|s| s.format_read());
        controls = controls.with_format(
            format_read.is_some(),
            format_read.map(|read| read.also.len()),
        );
        controls = controls.with_notes_pending(
            self.app_config.display.notes_accent
                && self
                    .data_table_state
                    .as_ref()
                    .is_some_and(|s| s.notes_unseen()),
        );

        match crate::render::main_view::control_bar_spec(self, main_view_content) {
            crate::render::main_view::ControlBarSpec::Datatable {
                dimmed,
                query_active,
                q_pops,
                enter_drills,
            } => {
                controls = controls
                    .with_dimmed(dimmed)
                    .with_query_active(query_active)
                    .with_q_pops(q_pops)
                    .with_enter_drills(enter_drills);
            }
            crate::render::main_view::ControlBarSpec::Custom(pairs) => {
                controls = controls.with_custom_controls(pairs);
            }
        }

        // Which columns are on screen, beside the rows, while the table is wider.
        if main_view_content == MainViewContent::Datatable {
            controls = controls.with_find(self.find_mark());
            controls = controls.with_columns(
                self.data_table_state
                    .as_ref()
                    .and_then(|s| s.columns_on_screen()),
            );
        }

        // The trailing figure belongs to whatever view is showing. On the home screen it
        // is the order the rows are in: each section's rule already counts its rows, and
        // a total across sections counted things no one listed together (#547 D11). It
        // yields to every chip at a narrow width.
        if main_view_content == MainViewContent::Home {
            // State, not actions. The Tab key that changes it lives with the other keys.
            let in_recents = self
                .home
                .selected_section()
                .and_then(|i| self.home.sections.get(i))
                .map(|s| s.grouped_by_place)
                .unwrap_or(false);
            let order = self.home.sort.label_in(in_recents);
            let waiting = self.home.listing_in_flight || self.home.awaiting_listing().is_some();
            let caption = if waiting && self.home.visible().is_empty() {
                "Looking...".to_string()
            } else {
                format!("by {order}")
            };
            controls = controls
                .with_caption(Some(caption))
                .with_caption_yielding(true);
        }

        // Chart preparation spins the throbber without setting `busy`, so the chart
        // sidebar keeps taking keys while the data is computed.
        controls = controls.with_busy(
            self.is_busy() || self.chart_preparing() || self.value_counts_computing(),
            self.throbber_frame,
        );
        // Reflect the row-count's determinacy in the control bar:
        //  - in flight   -> spinner (still being computed)
        //  - failed       -> "?" (computation gave up; don't show a misleading partial total)
        //  - otherwise    -> the number
        let count_pending = self.row_count_pending();
        let count_unknown = !count_pending
            && self.data_table_state.as_ref().is_some_and(|s| {
                !s.is_num_rows_valid() && self.len_count_failed == Some(s.len_generation())
            });
        // Nothing is counted while a load waits on the download confirmation, and a
        // spinning count there would read as progress.
        if self.awaiting_download_confirmation() {
            controls.row_count = None;
        }
        // The hex view has bytes, not rows: its status line says where the cursor is.
        if main_view_content == MainViewContent::Hex {
            controls.row_count = None;
        }
        controls = controls
            .with_row_count_pending(count_pending)
            .with_row_count_unknown(count_unknown)
            // "417 of 1,000" under a filter or query. Only a total something already
            // resolved: never a reason for the chrome to read data.
            .with_total_row_count(
                self.data_table_state
                    .as_ref()
                    .and_then(|s| s.total_rows_when_subset()),
            );
        controls.render(app_layout.control_bar, buf);
        self.pointer.chips_drawn(controls.drawn_chips());
        if let Some(debug_area) = app_layout.debug {
            self.debug.render(debug_area, buf);
        }

        // Last line of defence, and deliberately the last statement here.
        //
        // Everything above draws untrusted text: cell values, column names,
        // filenames, parser messages. ratatui strips control characters in
        // `Buffer::set_stringn` but not in `Span`/`Line` rendering, which is
        // what these widgets use, and the crossterm backend then prints each
        // cell symbol unfiltered. Without this sweep a cell containing
        // `\x1b]52;c;...\x07` writes to the user's clipboard.
        //
        // Doing it here rather than at each of the ~200 `Span` construction
        // sites means a new widget cannot forget to. See `crate::sanitize`.
        crate::sanitize::sanitize_buffer(buf);
    }
}

impl App {
    /// The view a caller that asked for one gets back when the app exits
    /// (`datui.view(..., capture=True)`): the active table's committed frame with
    /// datui's internal columns dropped. `None` when no dataset is open. Text still
    /// sitting in an editor was never applied, so it is not here either.
    ///
    /// Refused when the frame would scan a temporary file, because those are removed
    /// on exit and a plan over deleted paths fails later and worse: a remote download,
    /// a decompressed archive, or a converted stream or GPS log. The in-TUI export (`e`) writes
    /// real rows and is the way out for those datasets.
    pub fn capture_view(&self) -> Result<Option<LazyFrame>> {
        let Some(state) = &self.data_table_state else {
            return Ok(None);
        };
        if state.scans_a_download() {
            return Err(color_eyre::eyre::eyre!(
                "cannot return this view: the data was downloaded to a temporary file \
                 that is removed when datui exits. Export it from inside datui (press \
                 e) instead."
            ));
        }
        if state.scans_a_temp_file() {
            return Err(color_eyre::eyre::eyre!(
                "cannot return this view: the data was decompressed or converted into a \
                 temporary file that is removed when datui exits. Export it from \
                 inside datui (press e) instead."
            ));
        }
        Ok(Some(state.visible_lf()))
    }
}

impl Drop for App {
    fn drop(&mut self) {
        // The dataset's footer pass stops issuing reads. The open in flight is the
        // loader's, which stops it as it drops: a download still running stops at its
        // next chunk and removes its partial file, and a finished one is removed as
        // whatever holds it drops. Drop rather than the end of `run`, because it covers
        // every exit: a normal quit, an error return, an unwind from a panic, and the
        // Python binding calling `run` again in the same process.
        self.footer_progress.cancel();
    }
}

/// A source as a home-screen row, with the last run's buckets when they still apply.
#[cfg(feature = "cloud")]
fn home_cloud_source(
    source: &crate::cloud_sources::Source,
    cached: Option<&crate::cache::CloudListing>,
    listing: bool,
) -> home::CloudSource {
    let mut details: Vec<(String, String)> = vec![
        ("source".to_string(), source.id.clone()),
        (
            "api".to_string(),
            match source.kind {
                crate::cloud_browse::ProviderKind::S3 => "s3",
                crate::cloud_browse::ProviderKind::Gcs => "gcs",
                crate::cloud_browse::ProviderKind::Azure => "azure",
            }
            .to_string(),
        ),
    ];
    if let Some(endpoint) = &source.s3.endpoint {
        details.push(("endpoint".to_string(), endpoint.clone()));
    }
    if let Some(region) = &source.s3.region {
        details.push(("region".to_string(), region.clone()));
    }
    if let Some(project) = &source.project {
        details.push(("project".to_string(), project.clone()));
    }
    if let Some(profile) = &source.profile {
        details.push(("profile".to_string(), profile.clone()));
    }
    if let Some(configuration) = &source.gcloud {
        details.push(("configuration".to_string(), configuration.clone()));
    }
    if source.s3.virtual_hosted.is_some() {
        let style = if source.s3.virtual_hosted_style() {
            "virtual-hosted"
        } else {
            "path-style"
        };
        details.push(("addressing".to_string(), style.to_string()));
    }
    details.push(("login".to_string(), source.origin.clone()));

    let short = source.problem.as_deref().map(|problem| {
        if problem.starts_with("not signed in") {
            "not signed in"
        } else if problem.starts_with("unsupported login") {
            "unsupported login"
        } else {
            "not configured"
        }
    });
    // A source that cannot list says what to do about it where the row has room: the
    // count already carries the short problem, and the login it would use is moot
    // (#547 D5).
    let note = match (&source.problem, short) {
        (Some(problem), Some(short)) => problem
            .strip_prefix(short)
            .map(|rest| rest.trim_start_matches([':', ' ']))
            .filter(|rest| !rest.is_empty())
            .unwrap_or(problem)
            .to_string(),
        _ => [source.detail(), Some(source.origin.clone())]
            .into_iter()
            .flatten()
            .filter(|n| !n.is_empty())
            .collect::<Vec<_>>()
            .join(&format!(" {} ", crate::glyphs::get().middot)),
    };
    let mut names: Vec<String> = cached.map(|c| c.buckets.clone()).unwrap_or_default();
    for bucket in &source.buckets {
        if !names.contains(bucket) {
            names.push(bucket.clone());
        }
    }
    let status = match (&source.problem, short) {
        (Some(problem), Some(short)) => home::CloudStatus::Failed {
            short: short.to_string(),
            detail: problem.clone(),
        },
        _ if cached.is_some() => home::CloudStatus::Listed,
        _ if listing => home::CloudStatus::Listing,
        _ => home::CloudStatus::Unlisted,
    };
    home::CloudSource {
        id: source.id.clone(),
        label: source.label.clone(),
        api: match source.kind {
            crate::cloud_browse::ProviderKind::S3 => "s3",
            crate::cloud_browse::ProviderKind::Gcs => "gcs",
            crate::cloud_browse::ProviderKind::Azure => "azure",
        }
        .to_string(),
        note,
        buckets: names
            .iter()
            .map(|b| PathBuf::from(source.bucket_url(b)))
            .collect(),
        refreshing: listing && cached.is_some() && source.problem.is_none(),
        // A source that failed before any request has nothing to ask.
        asked: listing || source.problem.is_some(),
        listed_at: cached
            .map(|c| std::time::UNIX_EPOCH + std::time::Duration::from_secs(c.listed_at)),
        status,
        details,
        place_details: Default::default(),
    }
}

/// A listing error as a word for the row and the full message for the details pane.
#[cfg(feature = "cloud")]
fn summarize_cloud_failure(error: &str) -> (String, String) {
    let lower = error.to_lowercase();
    // A missing tool is already as short as it gets: `needs the AWS CLI`.
    if let Some(start) = lower.find("needs ") {
        return (error[start..].to_string(), error.to_string());
    }
    let short = if lower.contains("403")
        || lower.contains("forbidden")
        || lower.contains("accessdenied")
        || lower.contains("access denied")
    {
        "403"
    } else if lower.contains("401")
        || lower.contains("unauthorized")
        || lower.contains("credential")
        || lower.contains("invalidaccesskeyid")
        || lower.contains("expired")
        || lower.contains("sso")
        || lower.contains("az login")
    {
        "not logged in"
    } else if lower.contains("unsupported login") {
        "unsupported login"
    } else if lower.contains("no gcp project") {
        "no project"
    } else if lower.contains("is not set") {
        "not configured"
    } else if lower.contains("timed out")
        || lower.contains("timeout")
        || lower.contains("connection")
        || lower.contains("dns")
        || lower.contains("resolve")
    {
        "unavailable"
    } else {
        "error"
    };
    (short.to_string(), error.to_string())
}

/// Run a future on the app's runtime from a thread outside it, and wait for the answer.
///
/// Every background thread that needs the network goes through this rather than
/// `Handle::block_on`. That polls the future on the calling thread, and quitting shuts
/// the runtime down without waiting for those threads: the next timer or socket an
/// in-flight request touches then panics with "A Tokio 1.x context was found, but it is
/// being shutdown", across the terminal the user just got back. A task spawned onto the
/// runtime is dropped by the shutdown instead of polled, so the wait ends with `None`
/// and the abandoned request goes quietly.
#[cfg(feature = "cloud")]
fn wait_on_runtime<F>(runtime: &tokio::runtime::Handle, future: F) -> Option<F::Output>
where
    F: std::future::Future + Send + 'static,
    F::Output: Send + 'static,
{
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    runtime.spawn(async move {
        let _ = tx.send(future.await);
    });
    rx.recv().ok()
}

/// Restore the terminal, then turn how the loop ended into what `run_impl` returns.
/// The reader stops first, so nothing typed after the screen is handed back is read
/// here. The capture is taken after the screen is handed back, so a refused capture
/// still leaves the terminal usable.
fn conclude(
    end: event_pump::Ended,
    app: &App,
    capture: bool,
    reader: &mut terminal_input::TerminalInput,
    screen: &mut TakenTerminal,
) -> Result<Option<LazyFrame>> {
    reader.stop();
    screen.restore();
    match end {
        event_pump::Ended::Quit if capture => app.capture_view(),
        event_pump::Ended::Quit => Ok(None),
        event_pump::Ended::Crash(msg) => Err(color_eyre::eyre::eyre!(msg)),
        event_pump::Ended::NotFound(path) => Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("File not found: {}", path.display()),
        )
        .into()),
    }
}

/// The exit status the session's ending signal calls for, once one has ended it;
/// 0 until then. Set once.
static ENDED_BY_SIGNAL: std::sync::atomic::AtomicI32 = std::sync::atomic::AtomicI32::new(0);

/// The exit status for the binary when a signal ended the session [`run`] returned
/// from: `128 + n` for SIGTERM or SIGHUP, as if it had not been caught, and on
/// Windows the status a console process closed by its window ends with.
pub fn ended_by_signal() -> Option<i32> {
    let status = ENDED_BY_SIGNAL.load(std::sync::atomic::Ordering::SeqCst);
    (status != 0).then_some(status)
}

/// A signal that ends the session arrived: quit as `q` does, so the screen is handed
/// back and an open's temp files are removed (#510). A second one, or a session still
/// running a few seconds after the first, ends the process at once, as the signal
/// would have: a stuck event loop cannot make datui unkillable. Called on the runtime.
fn end_session(status: i32, tx: &std::sync::mpsc::Sender<AppEvent>) {
    use std::sync::atomic::Ordering;
    // Longer than the exit sweep's grace, which is part of a normal quit, and short
    // of the five seconds Windows allows a console process it is closing.
    const STRAGGLE: std::time::Duration = std::time::Duration::from_secs(3);
    fn end_now(status: i32) -> ! {
        restore_terminal();
        std::process::exit(status)
    }
    if ENDED_BY_SIGNAL
        .compare_exchange(0, status, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        end_now(ENDED_BY_SIGNAL.load(Ordering::SeqCst));
    }
    let _ = tx.send(AppEvent::Exit);
    tokio::spawn(async move {
        tokio::time::sleep(STRAGGLE).await;
        end_now(status);
    });
}

/// End the session on SIGTERM or SIGHUP (the terminal closing).
#[cfg(unix)]
fn quit_on_signals(runtime: &tokio::runtime::Handle, tx: &std::sync::mpsc::Sender<AppEvent>) {
    use tokio::signal::unix::{SignalKind, signal};
    // `signal` registers with the runtime it is called in.
    let _runtime = runtime.enter();
    for kind in [SignalKind::terminate(), SignalKind::hangup()] {
        let Ok(mut arrivals) = signal(kind) else {
            continue;
        };
        let tx = tx.clone();
        runtime.spawn(async move {
            while arrivals.recv().await.is_some() {
                end_session(128 + kind.as_raw_value(), &tx);
            }
        });
    }
}

/// End the session when its console window is closed, or the user logs off or the
/// machine shuts down. Tokio holds the control handler until the process exits, so
/// the quit runs before Windows ends it.
#[cfg(windows)]
fn quit_on_signals(runtime: &tokio::runtime::Handle, tx: &std::sync::mpsc::Sender<AppEvent>) {
    use tokio::signal::windows::{ctrl_close, ctrl_logoff, ctrl_shutdown};
    /// STATUS_CONTROL_C_EXIT, what a console process ends with when closed.
    const CLOSED: i32 = 0xC000_013A_u32 as i32;
    // Each registers with the runtime it is called in.
    let _runtime = runtime.enter();
    // Three listener types with one shape and no trait in common.
    macro_rules! quit_on {
        ($listen:expr) => {
            if let Ok(mut arrivals) = $listen {
                let tx = tx.clone();
                runtime.spawn(async move {
                    while arrivals.recv().await.is_some() {
                        end_session(CLOSED, &tx);
                    }
                });
            }
        };
    }
    quit_on!(ctrl_close());
    quit_on!(ctrl_logoff());
    quit_on!(ctrl_shutdown());
}

/// Run the TUI with either file paths or an existing LazyFrame. Single event loop
/// used by the CLI and the Python binding.
pub fn run(input: RunInput, config: Option<AppConfig>) -> Result<()> {
    run_impl(input, config, false).map(|_| ())
}

/// As `run`, but a normal quit hands back the active table's final view for the
/// caller to keep working with (the Python binding's `capture=True`). `None` when no
/// dataset was open at quit. See `App::capture_view` for what is refused and why.
pub fn run_captured(input: RunInput, config: Option<AppConfig>) -> Result<Option<LazyFrame>> {
    run_impl(input, config, true)
}

fn run_impl(
    input: RunInput,
    config: Option<AppConfig>,
    capture: bool,
) -> Result<Option<LazyFrame>> {
    use event_pump::EventPump;
    use std::io::Write;

    // First, so a missing file is named as the home directory has it.
    let input = startup::expand_home(input);
    use std::sync::{Mutex, Once, mpsc};

    // The saved views are read on a worker from here; the first thing that needs them
    // waits for the rest of the read, if any.
    let templates = Templates::read_in_background();

    // Install color_eyre at most once per process (e.g. first datui.view() in Python).
    // Subsequent run() calls skip install and reuse the result; no error-message detection.
    static COLOR_EYRE_INIT: Once = Once::new();
    static INSTALL_RESULT: Mutex<Option<Result<(), color_eyre::Report>>> = Mutex::new(None);
    COLOR_EYRE_INIT.call_once(|| {
        *INSTALL_RESULT.lock().unwrap_or_else(|e| e.into_inner()) = Some(color_eyre::install());
    });
    if let Some(Err(e)) = INSTALL_RESULT
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .as_ref()
    {
        return Err(color_eyre::eyre::eyre!(e.to_string()));
    }
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .map_err(|e| color_eyre::eyre::eyre!("Failed to create tokio runtime: {}", e))?;

    // Background work (e.g. the row-count `len()` over a huge or remote dataset) runs on
    // the runtime's blocking pool. Dropping the runtime normally *joins* those threads, so
    // quitting would hang until an in-flight count finished — minutes for a 474 GB hive
    // set. Shut the runtime down in the background instead: exit is immediate and the
    // abandoned read-only task dies with the process. This guard covers every return path
    // (Exit, Crash, `?`-propagated errors, channel disconnect).
    struct RtGuard(Option<tokio::runtime::Runtime>);
    impl Drop for RtGuard {
        fn drop(&mut self) {
            if let Some(rt) = self.0.take() {
                rt.shutdown_background();
            }
        }
    }
    let rt_guard = RtGuard(Some(rt));
    let rt_handle = rt_guard
        .0
        .as_ref()
        .expect("runtime present")
        .handle()
        .clone();

    // `--tee -` passes the stream on to standard output, so the screen is drawn on the
    // terminal itself; standard output as it was is kept for the copy.
    let passed = match &input {
        RunInput::Cli(args) if args.tee.as_deref().is_some_and(crate::stdin::is_stdin) => {
            Some(crate::tee::pass_stdout_on().map_err(|e| color_eyre::eyre::eyre!(e))?)
        }
        _ => None,
    };
    let mut terminal = match ratatui::try_init() {
        Ok(terminal) => QuietTerminal(Some(terminal)),
        Err(e) => {
            // No screen to keep up, so nothing to wait behind: a configuration that
            // cannot be used, or a named file that is not there, is the more useful
            // thing to say, as each always came first.
            if config.is_none() {
                startup::load_config(&input)?;
            }
            // Without a screen nothing is opened, so the specs are not loaded to look.
            if let Some(missing) =
                App::missing_named_path(startup::named_paths(&input), &Default::default())
            {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("File not found: {}", missing.display()),
                )
                .into());
            }
            return Err(color_eyre::eyre::eyre!(
                "datui requires an interactive terminal (TTY). No terminal detected: {}. \
                 There is no TTY inside a Jupyter notebook or when output is piped or \
                 redirected; run from a terminal with stdout connected to it.",
                e
            ));
        }
    };
    // Handed back on every way out of this function, after the reader below has let go.
    let mut screen = TakenTerminal { restored: false };
    // Anything written to stderr from here on would be drawn over the screen; it goes
    // to the log until this drops, on every way out of this function.
    let session = logging::TuiSession::begin(restore_terminal);
    push_keyboard_flags();
    let (tx, rx) = mpsc::channel::<AppEvent>();
    {
        let tx = tx.clone();
        session.wake_with(move || {
            let _ = tx.send(AppEvent::Wake);
        });
    }
    let mut reader = terminal_input::TerminalInput::start(tx.clone())?;
    // Only for the datui binary: the handlers stay for the life of the process, and a
    // host such as Python keeps its own.
    #[cfg(any(unix, windows))]
    if matches!(input, RunInput::Cli(_)) {
        quit_on_signals(&rt_handle, &tx);
    }

    // The settings are files, so they are read on a worker while the keys are already
    // being read: a slow mount shows a screen saying so, and Ctrl+C or Ctrl+Q leave it.
    let waiting_on = startup::named(&input);
    {
        let tx = tx.clone();
        std::thread::Builder::new()
            .name("datui-settings".into())
            .spawn(move || {
                let read = logging::catch_panic(|| startup::read(input, config))
                    .unwrap_or_else(|panic| Err(color_eyre::eyre::eyre!(panic)));
                let _ = tx.send(AppEvent::SettingsRead(Box::new(read)));
            })?;
    }
    let mut backlog = Vec::new();
    let grace_ends = std::time::Instant::now() + startup::GRACE;
    let mut waiting_shown = false;
    let settings = loop {
        let timeout = if waiting_shown {
            std::time::Duration::MAX
        } else {
            grace_ends.saturating_duration_since(std::time::Instant::now())
        };
        match rx.recv_timeout(timeout) {
            Ok(AppEvent::SettingsRead(read)) => break *read,
            Ok(AppEvent::Terminal(crossterm::event::Event::Key(key)))
                if key.modifiers.contains(KeyModifiers::CONTROL)
                    && matches!(key.code, KeyCode::Char('c') | KeyCode::Char('q')) =>
            {
                reader.stop();
                screen.restore();
                return Ok(None);
            }
            Ok(AppEvent::Crash(msg)) => {
                reader.stop();
                screen.restore();
                return Err(color_eyre::eyre::eyre!(msg));
            }
            // A signal, before there was an app to quit.
            Ok(AppEvent::Exit) => {
                reader.stop();
                screen.restore();
                return Ok(None);
            }
            Ok(event) => {
                if waiting_shown
                    && matches!(
                        event,
                        AppEvent::Terminal(crossterm::event::Event::Resize(..))
                    )
                {
                    terminal
                        .get()
                        .draw(|frame| startup::draw_waiting(frame, waiting_on.as_deref()))?;
                }
                // Typed before there was an app to take it: handled, in order, first.
                backlog.push(event);
            }
            Err(_) => {
                terminal
                    .get()
                    .draw(|frame| startup::draw_waiting(frame, waiting_on.as_deref()))?;
                let _ = std::io::stdout().flush();
                waiting_shown = true;
            }
        }
    };
    let startup::Settings {
        config,
        theme,
        input,
        opts,
        notes,
    } = match settings {
        Ok(settings) => settings,
        Err(e) => {
            reader.stop();
            screen.restore();
            return Err(e);
        }
    };

    // Choose the glyph alphabet before the first frame: on a terminal that is not
    // doing UTF-8, box-drawing characters render as replacement boxes and make the
    // UI harder to read rather than prettier.
    glyphs::init_with_overrides(config.display.unicode, &config.glyphs.overrides);

    // Taken once the settings say so; handed back with the screen.
    if config.display.mouse {
        let _ = crossterm::execute!(std::io::stdout(), pointer::EnableMouse);
    }

    let mut app = App::new_with_templates(tx.clone(), rt_handle, theme, config, templates);
    if let Some(out) = passed {
        app.pass_stdout_to(out);
    }
    app.startup_template = opts.template.clone();
    if opts.debug {
        app.enable_debug();
    }

    // Show the first frame immediately; the open it announces is handled right after.
    let open = match input {
        // No paths: open the home screen instead of loading anything.
        RunInput::Paths(paths, _) if paths.is_empty() => {
            app.enter_home();
            None
        }
        RunInput::Paths(paths, opts) => {
            // Whether each path is there, and whether a directory was named, is asked
            // after this frame, on a worker; the frame says what is being opened.
            app.set_loading_phase("Scanning input", 10);
            if let [path] = paths.as_slice() {
                app.name_what_is_loading(path.clone());
            }
            Some(AppEvent::OpenNamed(paths, opts))
        }
        RunInput::LazyFrame(lf, opts) => {
            app.set_loading_phase("Scanning input", 10);
            Some(AppEvent::OpenLazyFrame(lf, opts))
        }
        RunInput::Cli(_) => unreachable!("read_settings resolves the command line"),
    };
    // Declared before the pump, so it drops after it: the app's own files go with the
    // app, and this then removes what a worker was still writing.
    let _sweep = app.exit_sweep();
    let input_tx = tx.clone();
    let mut pump = EventPump::new(app, tx, rx);
    // The open goes out before the keys typed while the settings were read, so they
    // meet it as they would any open in flight: Ctrl+O puts it down, `q` quits. Sent
    // on the channel instead, it lost to a Ctrl+O offered ahead of the channel and
    // opened behind the home screen, or behind whatever was opened from there.
    pump.handle_first(backlog.into_iter().chain(open));
    let end = pump.run(|app| {
        if let Some(open) = app.take_external_open() {
            let mouse = app.mouse_enabled();
            let note = open_externally(&open, &mut reader, &input_tx, mouse, terminal.get());
            app.external_opened(&open, note);
        }
        terminal
            .get()
            .draw(|frame| frame.render_widget(app, frame.area()))?;
        let _ = std::io::stdout().flush();
        Ok(())
    })?;
    let result = conclude(end, &pump.app, capture, &mut reader, &mut screen);
    // stderr is the terminal again once the session is over.
    drop(session);
    // Not `eprintln!`, which panics when a hangup has taken the terminal away.
    for note in notes {
        let _ = writeln!(std::io::stderr(), "datui: {note}");
    }
    // Quit with the recording kept going: it goes on until its stream ends, with the
    // terminal handed back. A signal now ends the process as it always would.
    if let Some((tee, handle)) = pump.app.recording_after_exit() {
        let to = if tee.to_stdout() {
            format!("passing standard input on to {}", tee.name())
        } else {
            format!("recording standard input to {}", tee.path.display())
        };
        let _ = writeln!(
            std::io::stderr(),
            "datui: {to} until it ends (Ctrl+C stops it)"
        );
        handle.spool().wait();
        let done = if tee.to_stdout() {
            "datui: standard input ended".to_string()
        } else {
            format!("datui: saved {}", tee.path.display())
        };
        let _ = writeln!(std::io::stderr(), "{done}");
    }
    result
}

/// Open a value the inspector wrote: a program that takes the terminal gets it
/// (the key reader stopped, the screen and raw mode handed back) until it
/// returns; an opener is only started. Says what went wrong, if anything.
fn open_externally(
    open: &external_open::ExternalOpen,
    reader: &mut terminal_input::TerminalInput,
    tx: &std::sync::mpsc::Sender<AppEvent>,
    mouse: bool,
    terminal: &mut ratatui::DefaultTerminal,
) -> Option<String> {
    let program = external_open::program_for(open.document, |name| std::env::var(name).ok());
    let result = match &program {
        external_open::Program::Opener(_) => external_open::run(&program, &open.path),
        external_open::Program::Wait(_) => {
            reader.stop();
            restore_terminal();
            let result = external_open::run(&program, &open.path);
            let _ = crossterm::terminal::enable_raw_mode();
            let _ = crossterm::execute!(
                std::io::stdout(),
                crossterm::terminal::EnterAlternateScreen,
                crossterm::cursor::Hide
            );
            push_keyboard_flags();
            if mouse {
                let _ = crossterm::execute!(std::io::stdout(), pointer::EnableMouse);
            }
            let _ = terminal.clear();
            match terminal_input::TerminalInput::start(tx.clone()) {
                Ok(started) => *reader = started,
                Err(e) => {
                    let _ = tx.send(AppEvent::Crash(format!("Could not read keys again: {e}")));
                }
            }
            result
        }
    };
    result.err().map(|e| e.to_string())
}

/// `text` as a sentence for a flash: its first letter capitalized.
fn sentence(text: &str) -> String {
    let mut chars = text.chars();
    match chars.next() {
        Some(first) => first.to_uppercase().chain(chars).collect(),
        None => String::new(),
    }
}

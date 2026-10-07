use color_eyre::Result;
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use polars::datatypes::DataType;
#[cfg(feature = "cloud")]
use polars::io::cloud::{AmazonS3ConfigKey, CloudOptions};
use polars::prelude::{DataFrame, LazyFrame, Schema, col};
#[cfg(feature = "cloud")]
use polars::prelude::{PlRefPath, ScanArgsParquet};
use std::collections::HashMap;

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
pub mod catalog;
pub mod chart_data;
pub mod chart_export;
pub mod chart_export_modal;
mod chart_jobs;
mod chart_keys;
pub mod chart_modal;
mod chart_pdf;
mod chart_recipe;
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
pub mod codebook;
pub mod column_types;
pub mod columns;
pub mod commands;
pub mod config;
pub mod config_command;
pub mod context_menu;
mod copy_keys;
pub mod copy_modal;
pub mod csv_dialect;
pub mod data_quality;
pub mod dataflash;
mod dataset_files;
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
pub mod excel;
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
mod footer_state;
pub mod form;
pub mod formats;
pub mod framed_records;
pub mod fuzzy;
#[cfg(feature = "cloud")]
pub mod gcloud;
pub mod glyphs;
pub mod gps;
pub mod help;
mod hex_keys;
pub mod hex_view;
pub mod hf_splits;
pub mod home;
mod home_app;
mod home_keys;
pub mod home_preview;
pub mod indexed;
mod info_keys;
pub mod inspector_bytes;
pub mod inspector_drill;
mod inspector_keys;
pub mod inspector_modal;
pub mod inspector_reader;
pub mod intent_modal;
pub mod ipc_stream;
mod jobs;
pub mod journal;
pub mod limits;
pub mod lines;
pub mod link_open;
mod loading;
pub mod local_copy;
pub(crate) mod local_glob;
pub mod locality;
pub mod logging;
pub mod measurements;
pub mod members;
pub mod midi;
pub mod model_files;
pub mod nested_json;
pub mod notes;
pub mod nul_tail;
pub mod numfmt;
pub mod numpy;
mod open_options;
pub mod output_file;
pub mod parquet_footer;
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
mod quality_runs;
pub mod quality_trends;
#[cfg(any(feature = "http", feature = "cloud"))]
mod remote_model;
mod retype_keys;
pub mod retype_modal;
pub mod row_index;
mod run;
pub use run::{ended_by_signal, run, run_captured};
#[cfg(feature = "cloud")]
pub mod s3_tools;
mod sample_keys;
pub mod sample_modal;
pub mod sampling;
pub mod table_sample;
// Public so the fuzz targets in `fuzz/` can reach `parse_query`. The parser is
// hand-written and runs on whatever the user types, so it is fuzzed directly.
pub mod query;
pub mod readers;
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
pub(crate) mod spec_union;
mod sql_assist;
pub mod sqlite;
// Public so the fuzz target `sql_group_plan` can reach `plan`, which reads every SQL
// statement the prompt runs.
#[cfg(feature = "sql")]
pub mod sql_group;
#[cfg(feature = "sql")]
mod sql_plan;
pub mod startup;
pub mod statistics;
pub mod stdin;
pub mod table;
pub mod table_switch;
pub mod tee;
mod terminal;
mod terminal_color;
pub mod terminal_input;
pub mod text_formats;
pub mod themes;
pub mod typed_value;
pub mod ulog;
mod unfinished;
pub mod user_agent;
pub mod value_counts;
pub mod value_counts_modal;
pub mod vcd;
pub mod view;
mod view_keys;
pub mod widgets;

pub use cache::CacheManager;
pub use cli::Args;
pub use config::{
    AppConfig, ColorParser, ConfigManager, QueryMode, Theme, ThemeMode, rgb_to_256_color,
    rgb_to_basic_ansi,
};

use analysis_modal::{AnalysisModal, AnalysisProgress};
use background::{CacheWrites, InflightCollect, LenCount, OwedCount};
use chart_export::ChartExportRequest;
use chart_export_modal::ChartExportModal;
use chart_jobs::{ChartCache, ChartInflight, ChartPrepared, ChartRequest, ChartResultSlot};
use chart_modal::{ChartColumns, ChartModal};
pub use error_display::{ErrorKindForPython, error_for_python};
pub use export::{ExportOptions, ExportRequest};
use export_modal::{ExportFocus, ExportFormat, ExportModal};
pub use feedback::{ConfirmationModal, ErrorModal, Flash};
use filter_modal::{FilterOperator, FilterStatement, LogicalOperator};
use form::FormKey;
use jobs::{Answer, Job, Jobs, Outcome};
pub use jobs::{JobKind, Progress, Ticket};
use numfmt::NumberFormatSettings;
pub use open_options::{
    OpenOptions, ParseStringsTarget, ReadReport, SqliteOpen, TypedDialect, UnaskedDownload,
};
use output_file::Overwrite;
use pivot_melt_modal::{MeltSpec, PivotMeltModal, PivotSpec};
use quality_memory::QualityCacheEntry;
pub use quality_memory::{KeptQualitySample, QUALITY_MEMORY_BUDGET, RetainedCopy};
use scan::Scan;
use sort_filter_modal::SortFilterModal;
use sort_modal::{SortColumn, order_with_hidden};
use table::{DataTableState, DrillRow, OpenFacts};
pub use unfinished::ExitSweep;
pub use view::{SavedView, ViewManager, Views};
use widgets::column_widths::WidthChoice;
use widgets::debug::DebugState;
use widgets::text_input::TextInput;
use widgets::view_modal::{FormFocus, ViewModal, ViewModalMode, ViewRow};

/// Application name used for cache directory and other app-specific paths
pub const APP_NAME: &str = "datui";

/// What a file no reader takes, and no hex view can show, is told.
pub(crate) const UNSUPPORTED: &str =
    "Unsupported file type. --format names the format to read it as.";

/// Re-export compression format and file format from CLI module
pub use cli::{CompressionFormat, FileFormat, ReadMode, RemoteRead, Stored, Summary};

#[cfg(test)]
pub mod tests;

pub enum AppEvent {
    Key(KeyEvent),
    /// A key to take as if typed: what Enter on a help line presses. The event pump
    /// offers it as the next typed key, through `classify`, so it is held, converted or
    /// dropped as a typed key would be; outside the pump it is a `Key`.
    Press(KeyEvent),
    /// Read from the terminal by [`terminal_input::TerminalInput`]: a key press or a
    /// resize. [`event_pump::EventPump`] takes it off the channel and decides what a
    /// key does while the app is busy; the app itself only ever sees `Key`/`Resize`.
    Terminal(crossterm::event::Event),
    /// Something polled rather than sent changed (a background panic, a Polars
    /// warning): the loop should look. Handled as nothing.
    Wake,
    /// The terminal said what its background is (an OSC 11 reply, taken off the input
    /// stream by [`terminal_input`]). Under `theme.mode = "auto"` the palette follows.
    TerminalBackground(ThemeMode),
    /// The terminal window came back into focus: under `auto` the background is asked
    /// again, since the scheme may have changed while it was away.
    TerminalFocused,
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
    /// What a HEAD said an HTTP(S) file on home weighs: its row, measured.
    HomeSized {
        path: PathBuf,
        measured: crate::home::Measured,
    },
    /// What a HEAD settled about an HTTP(S) file on home: it cannot be had.
    HomeWebGone {
        path: PathBuf,
        gone: crate::error_display::HttpGone,
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
    /// Rows of a network directory read since its last batch, while its listing goes
    /// on.
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
    /// A cloud listing stopped because its place was left. Nothing is known about the
    /// place, so it is listed again when it is entered again.
    HomeProbeCancelled {
        root: PathBuf,
    },
    /// The names under `prefix` in a cloud directory cut short, asked for by a filter;
    /// `None` when the listing failed or was stopped.
    HomeNarrowed {
        dir: PathBuf,
        prefix: String,
        listed: Option<(Vec<crate::discover::Entry>, bool)>,
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
    /// The Info tab of a piped journal, read again once it ended, for the dataset of
    /// that generation.
    FollowedDetail {
        dataset_generation: u64,
        detail: Box<crate::text_formats::Detail>,
    },
    Exit,
    Crash(String),
    QQuery(String),
    SqlQuery(String),
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
    /// A documentation link the user confirmed, checked by `link_open::checked_url`:
    /// start the browser on it.
    OpenLink(String),
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
    /// Run the next chunk of analysis (describe/distribution).
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
    /// Every line of a text file opened from its first rows is indexed, `rows` of
    /// them, for the dataset of `generation`.
    LinesIndexed {
        generation: u64,
        rows: usize,
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
type FileFactsReader = Arc<
    dyn Fn(&Path, Option<crate::readers::Facts>) -> std::result::Result<FileFacts, String>
        + Send
        + Sync,
>;

/// What [`App::handle`] did with an event: `Ok` carries the follow-up event to send,
/// if any; `Err` returns a key that arrived while the app was busy. Nothing was done
/// with that key and it was not dropped: the caller keeps it and offers it again once
/// the app is idle.
pub type EventOutcome = Result<Option<AppEvent>, KeyEvent>;

/// What <kbd>Enter</kbd> will do on the highlighted row.
///
/// Written so the footer and the details pane can say it before it happens.
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
        let entry = match self.home.selected_row() {
            // A place row browses into the place, which is what `→` does on it too, so
            // it is labelled the same and offered once. An HTTP place has no listing to
            // browse and says so instead.
            Some(home::Row::Place { path, .. }) => {
                return if home::place_is_browsable(&path) {
                    WhatEnter::GoesInside
                } else {
                    WhatEnter::Explains
                };
            }
            Some(home::Row::Header { .. }) => return WhatEnter::FoldsSection,
            Some(home::Row::More { .. }) => return WhatEnter::ShowsMore,
            Some(home::Row::Hidden { .. }) => return WhatEnter::ShowsHidden,
            // "No match.": nothing to open and nothing to say about it.
            None => return WhatEnter::Nothing,
            // The door reads the directory it names whatever that directory is labelled —
            // the lake tables included, which is the one row that reads them at all.
            Some(home::Row::Door { .. }) => return WhatEnter::OpensDirectory,
            Some(home::Row::Entry { entry, .. }) => entry,
        };
        // A bookmark opens whole.
        if entry.kind != discover::EntryKind::File && self.home.bookmark(&entry.path).is_some() {
            return WhatEnter::OpensDirectory;
        }
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
    /// A host program's options, as the command line would give them, with a frame
    /// to show instead of its paths when there is one. Read as the command line is,
    /// `-c` included, but standard input is the host's, never data.
    Host(Box<Args>, Option<Box<LazyFrame>>),
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
    /// A column's type, picked over the table: from the Info panel's Schema tab or
    /// the cell menu.
    Retype,
    /// A datetime made from columns, as a spec's derived column.
    Combine,
    /// The table picker over a table of a file of several: open another.
    PickTable,
    Info,
    Chart,
    /// Value Counts: how often each value of one column occurs in the view.
    ValueCounts,
    /// The hex view: a file's bytes.
    Hex,
    /// The Sample form over the table (`S`).
    Sample,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputType {
    /// The command line (`:`): a row number, or a query in SQL or q.
    Query,
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
    rollback: crate::table::ViewRollback,
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
    /// it is marked again. Applied for a match rather than picked, `matched` says
    /// why once its rows are in.
    View {
        previous: Option<String>,
        matched: Option<(String, view::MatchReason)>,
    },
}

/// What the bar says of a recording (`--tee`): `rec` with its size and rate while it
/// goes on, `saved` with its size, length and file once it ended (`sent` and no file
/// for `--tee -`), or `stopped` and why, in the warning color, when it ended in an
/// error. The second value is that last.
fn recording_label(spool: &crate::follow::Spool) -> (String, bool) {
    let dot = crate::glyphs::get().middot;
    let size = crate::numfmt::bytes(spool.bytes());
    match spool.ended() {
        None => (
            format!(
                "rec {size} {dot} {}/s",
                crate::numfmt::bytes(spool.rate() as u64)
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

/// An export under way, for the footer: the file, its phase, and the bytes
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
/// active one set (SQL takes precedence over fuzzy over DSL query). Used when saving view settings.
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

/// The steps `state` shows, as a saved view keeps them: the query, filters, sort,
/// columns and reshape.
pub(crate) fn view_settings_of(state: &DataTableState) -> view::ViewSettings {
    let (query, sql_query, fuzzy_query) = active_query_settings(
        state.get_active_query(),
        state.get_active_sql_query(),
        state.get_active_fuzzy_query(),
    );
    view::ViewSettings {
        chart: None,
        sample: saved_sample_of(state),
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
        columns: state.column_changes().to_vec(),
    }
}

/// The sample `state` is, as a view keeps it: with the query and filters it was
/// drawn through, when it was drawn from the view's rows.
fn saved_sample_of(state: &DataTableState) -> Option<view::SavedSample> {
    let sampled = state.sampled()?;
    // The whole view it was drawn through: column types, a reshape and a sort pick
    // its rows as much as a query does.
    let through = sampled.through().then(|| view::ViewSettings {
        sample: None,
        chart: None,
        ..view_settings_of(sampled.source())
    });
    Some(view::SavedSample::of(
        sampled.sample(),
        sampled.path(),
        through,
    ))
}

/// How far planning a view's steps got.
pub(crate) enum Replayed {
    /// Every step is planned; the view's rows are still to be read.
    Planned,
    /// Stopped at the pivot, which has to be read before the steps after it can be
    /// planned.
    Pivot(Box<crate::table::PivotJob>),
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
    /// drawn. The loading body and the footer are painted a millisecond apart,
    /// and when each read the counter for itself they printed different numbers for
    /// one wait — and the bar could print a phase's flat percentage beside a count
    /// that had finished between the two reads.
    footers_this_frame: Option<(usize, usize)>,
    /// The objects a listing had found when this frame began, for the same reason.
    listed_this_frame: Option<usize>,
    /// Network roots currently being listed off-thread, so a probe is not started
    /// twice. Entries are never removed for a root that never answers — that thread
    /// is unreclaimable, and retrying it would only block another one.
    home_probes_inflight: Vec<PathBuf>,
    /// The stop flag of each cloud listing out, by place: leaving the place sets it, and
    /// the listing ends before its next page.
    home_listing_cancels: HashMap<PathBuf, Arc<std::sync::atomic::AtomicBool>>,
    /// The listing out for the names a filter asked of a cut-short cloud directory:
    /// where, the name prefix, and its stop flag.
    home_narrowing: Option<(PathBuf, String, Arc<std::sync::atomic::AtomicBool>)>,
    /// True once cloud discovery has been started. Enumeration costs a request per
    /// provider, so it happens once and its result is kept for the session.
    #[cfg(feature = "cloud")]
    cloud_discovery_started: bool,
    /// True while a recursive search below the working directory is out. One at a
    /// time: the walk is bounded, and a second one would only compete for the disk.
    home_search_inflight: bool,
    /// The home generation the walk out was started in. Its batches and its end are its
    /// own, whatever refreshes the listing meanwhile; the root decides whether they
    /// still describe where the user is.
    home_search_generation: u64,
    /// Set while the confirmation modal is asking about forgetting every recent.
    pending_clear_recents: bool,
    /// The checked link the confirmation modal is asking about opening.
    pending_link: Option<String>,
    /// Whether a browser opened here opens in front of the user: `o` on a
    /// documentation link is offered only then (`link_open::local_desktop`).
    pub local_desktop: bool,
    /// The place whose recents the confirmation modal is asking about forgetting.
    pending_forget_place: Option<PathBuf>,
    /// Why the last open failed, shown on the home screen when the error is dismissed
    /// and there is nothing to fall back to.
    last_load_error: Option<String>,
    /// Schema reads currently out, so the same one is not requested every frame.
    home_schema_inflight: Vec<PathBuf>,
    /// Invalidates listings and measurements from a request the user has moved past.
    home_generation: u64,
    /// Rows came in for a listing still being read; it is listed again before the
    /// next frame.
    home_refresh_owed: bool,
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
    original_file_format: Option<ExportFormat>,
    original_file_delimiter: Option<u8>,
    /// What `-` reads in place of standard input: a test's pipe.
    stdin_reader: Option<Box<dyn std::io::Read + Send>>,
    /// Where `--tee -` passes the stream on: standard output as the process got it.
    stdout_pass: Option<Box<dyn std::io::Write + Send>>,
    /// The follow mark as last drawn, so its clock redraws only when it changes.
    follow_drawn: Option<crate::render::footer::FollowMark>,
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
    /// What the dataset's columns mean, when a catalog that lists it says.
    pub codebook: Option<std::sync::Arc<codebook::Codebook>>,
    /// The catalog entry the open dataset is, or is inside, and its catalog's label:
    /// what Info's Documentation tab shows.
    pub catalog_entry: Option<(String, std::sync::Arc<catalog::Dataset>)>,
    /// The Documentation view, full screen over home (Ctrl+E).
    pub documentation: widgets::documentation::DocState,
    /// The same page for the open dataset, on Info's Documentation tab.
    pub info_documentation: widgets::documentation::DocState,
    /// The directories Ctrl+D kept in the cache before 0.4.0 have been moved into
    /// `catalog.toml`, or there were none.
    remembered_moved: bool,
    /// Send a HEAD for the HTTP(S) file under the cursor on home, to show its size.
    /// Off under `cargo test`, which never reaches the network unless a test asks.
    pub head_web_rows: bool,
    // One input per command line language, each with its own history. The history
    // ids ("query", "sql") name files already on disk; they stay as they are so no
    // history is lost or read as another language's.
    query_input: TextInput, // q, history id "query"
    sql_input: TextInput,   // SQL, history id "sql"
    /// The find prompt (`/`) and the find `n` and `N` repeat; history id "find".
    pub find: find::Find,
    /// The column cursor moved last: the footer offers the column's keys.
    column_hints: bool,
    pub input_mode: InputMode,
    input_type: Option<InputType>,
    query_mode: QueryMode,
    /// The language Ctrl+T last chose, which the command line opens on until a query
    /// in effect says otherwise.
    query_mode_chosen: Option<QueryMode>,
    /// The command line holds the query in effect, selected and untouched: Ctrl+T
    /// carries it selected, so typing still replaces it.
    query_text_restored: bool,
    /// The columns of `df`, for the command line's list and completion. Taken from
    /// the schema when it opens.
    sql_columns: Vec<(String, DataType)>,
    /// A Tab completion in progress in the command line.
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
    pub view_modal: ViewModal,
    /// Whether the open dataset was reached through the home screen. `q` pops
    /// the context: opened from home it returns there, launched straight onto
    /// a file it quits — the user's mental stack, not a mode.
    opened_from_home: bool,
    /// `--view NAME`, waiting for the dataset from the command line to land.
    /// Taken on the first install, so datasets opened later are not re-dressed.
    startup_view: Option<String>,
    pub analysis_modal: AnalysisModal,
    /// The Sample form over the table (`S`): the view's sample, the step under its
    /// query.
    pub sample_form: Option<sample_modal::SampleForm>,
    /// Where the memory available now is read from, which a sample is checked
    /// against. The system's, unless a test says otherwise.
    memory_probe: table_sample::MemoryProbe,
    /// How each random sample of a stream was drawn on this dataset, by what it was
    /// drawn from: drawn again, the same seed keeps the same rows whether or not the
    /// count has come in since.
    sample_paths: Vec<(String, table_sample::DrawPath)>,
    /// Reports, newest first, within [`QUALITY_MEMORY_BUDGET`].
    quality_cache: Vec<QualityCacheEntry>,
    /// See [`KeptQualitySample`]. Newest first, within [`QUALITY_MEMORY_BUDGET`].
    quality_samples: Vec<KeptQualitySample>,
    /// Acquisitions the budget released, newest first: (dataset, view, sample).
    quality_released: Vec<(u64, u64, sampling::Sample)>,
    /// [`QUALITY_MEMORY_BUDGET`], smaller in a test that fills it.
    quality_memory_budget: usize,
    /// Local copies Data Quality's full scans read instead of a remote source, newest
    /// first, within `analysis.quality_local_copy`. Removed from disk when
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
    /// The type picker, while it is open.
    pub retype: Option<retype_modal::RetypeModal>,
    /// The combine form, while it is open.
    pub combine: Option<retype_modal::CombineModal>,
    /// The type picker or the combine form go back to the Info panel, not the table.
    pub(crate) retype_from_info: bool,
    /// The tables `T` offers: the picker's lines, and what each opens.
    pub table_picker: crate::widgets::ui::PickerState,
    pub table_choices: Option<table_switch::Tables>,
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
    /// The selection the chart last asked for, and, when it stepped the aggregate of
    /// the one before, until when it waits for the next step before it is prepared.
    chart_asked: Option<(ChartRequest, Option<std::time::Instant>)>,
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
    /// The saved view `d` asked to delete, by id, while the confirmation is up.
    pending_delete_view: Option<String>,
    /// Delete on the Example datasets heading asked to hide them.
    pending_hide_examples: bool,
    pending_chart_export: Option<ChartExportRequest>,
    /// A Data Quality report export waiting on the overwrite confirmation.
    pending_quality_export: Option<(PathBuf, crate::quality_export::ReportFormat)>,
    /// The help overlay, over whatever screen it was opened at.
    help: help::Help,
    /// What the mouse can land on in the last frame, and the last click.
    pointer: pointer::Pointing,
    /// The menu a right click on a cell opened, while it is open.
    context_menu: Option<context_menu::ContextMenu>,
    cache: CacheManager,
    /// The recent and the shape an open writes, which the home listing waits on.
    cache_writes: CacheWrites,
    view_manager: Views,
    active_view_id: Option<String>, // ID of currently applied view
    /// An export under way, which the footer reports.
    export_progress: Option<ExportProgress>,
    theme: Theme, // Color theme for UI rendering
    /// `a` is waiting on the confirmation to read every row.
    pending_read_all: bool,
    history_limit: usize, // History limit for all text inputs (from config.query.history_limit)
    table_cell_padding: u16, // Spaces between columns (from config.display.cell_padding)
    column_colors: bool, // When true, colorize table cells by column type (from config.display.column_colors)
    /// Second header row of column types. Starts from `display.type_row`; `D` flips it.
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
    /// End was pressed while a text file's lines were still being indexed: jump when
    /// the last of them is, for that dataset alone.
    end_when_indexed: Option<u64>,
    /// Stops the indexing thread of the dataset on screen's lines.
    indexing_stop: Arc<std::sync::atomic::AtomicBool>,
    /// The lines being indexed, until they all are.
    indexing_lines: Option<Arc<crate::lines::Lines>>,
    /// The indexing waits while home is up.
    indexing_paused: bool,
    /// `:N` past the lines indexed so far, for that dataset: gone to once they all are.
    goto_when_indexed: Option<(u64, usize)>,
    /// The last count started: what it has read of the footers, and its stop (Esc).
    count_progress: Arc<crate::schema_union::FooterProgress>,
    /// The dataset (`dataset_generation`) an exact count was asked for (`c` in the
    /// Info panel), of more files than the count reads unasked.
    exact_count_asked: Option<u64>,
    /// `c` was pressed while a stopped count was still winding down: count again when
    /// its answer, for this `len_generation`, comes in.
    count_after_stop: Option<u64>,
    /// What a dataset's footers found while the user was looking at a query, a pivot or
    /// a drill-down rather than at the data. Held rather than applied, because widening
    /// the scan under a query takes the query's own columns away, and offered again the
    /// moment the view comes back to the dataset itself.
    footers_held: Option<(u64, crate::table::FootersFound)>,
    /// Fields a followed pipe's NDJSON brought after the open, held as footers are
    /// until the view is back on the data.
    followed_fields_held: Option<(u64, Vec<polars::prelude::Field>)>,
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
    /// The spinner's frame, counting up; each spinner takes it modulo its own frames.
    throbber_frame: u8,
    /// Status text for the footer, at the table view. Shown whether or not the app
    /// is busy: an End waiting on a remote row count parks without setting `busy`.
    status_message: Option<String>,
    analysis_computation: Option<AnalysisComputationState>,
    app_config: AppConfig,
    /// The terminal should be asked for its background before the next frame.
    background_query: bool,
    /// The format specs on the search path, read when the app was built.
    formats: Arc<crate::formats::Registry>,
}

impl App {
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
        match form::key(form, event) {
            FormKey::Cancel => modal.data_quality_intent_form = None,
            FormKey::Submit => match form.apply(&mut modal.data_quality_plan.intent) {
                Ok(()) => modal.data_quality_intent_form = None,
                Err(error) => form.error = Some(error),
            },
            FormKey::Act(_) => form.adjust(true),
            FormKey::Step(_, delta) => form.adjust(delta > 0),
            FormKey::Text(_) => {
                if let Some(input) = form.input_mut() {
                    let _ = input.handle_key(event, None);
                }
                form.error = None;
            }
            FormKey::Moved | FormKey::Other => {}
        }
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
                        // The dialog stays up while the report is written: a failed
                        // write says why on its status line, the path still there.
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
        match form::key(form, event) {
            FormKey::Cancel => {}
            FormKey::Submit => match form.expected() {
                Ok(expected) => self.analysis_modal.data_quality_plan.expected = expected,
                Err(problem) => {
                    form.error = Some(problem);
                    return;
                }
            },
            FormKey::Step(_, delta) => {
                form.cycle(&every, delta > 0);
                return;
            }
            FormKey::Text(_) => {
                if let Some(input) = form.input_mut() {
                    let _ = input.handle_key(event, None);
                    form.error = None;
                }
                return;
            }
            FormKey::Act(_) | FormKey::Moved | FormKey::Other => return,
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

    /// Read `catalog.toml` from `dir`, and write it there, from now on. For a test whose
    /// Ctrl+D must not write into the config directory every test in a process shares.
    pub fn use_catalog_dir(&mut self, dir: &Path) -> Result<()> {
        self.app_config.read_catalog_files(Some(dir))
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

    /// What views are matched against: the dataset's path and the table of its file it
    /// is. What was piped in, or a frame handed over (`datui.view(frame)`), is `-`,
    /// which no path criterion fits, so it matches by its columns alone.
    fn view_dataset(&self) -> Option<view::Dataset<'_>> {
        self.data_table_state.as_ref()?;
        let path = match self.path.as_deref() {
            Some(path) if !self.reads_stdin() => path,
            _ => Path::new(stdin::PATH),
        };
        Some(view::Dataset {
            path,
            table: self.view_table(),
        })
    }

    /// The table of a file of tables the dataset on screen is: the one named by
    /// `--table`, or by a path inside the file (`shop.db/orders`), which opens as the
    /// file with `--table`.
    fn view_table(&self) -> Option<&str> {
        let (_, options) = self.opened.as_ref()?;
        options.table.as_deref()
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
        // The view's sample: it is drawn again, and the tool runs on it once it is.
        if self.analysis_modal.sample_form.as_ref()?.view {
            let memory = self.memory_check();
            let form = self.analysis_modal.sample_form.as_mut()?;
            return match Self::submit_view_sample(form, memory) {
                sample_keys::Submitted::Stays => None,
                sample_keys::Submitted::Clear => {
                    self.analysis_modal.sample_form = None;
                    self.clear_table_sample();
                    if quality {
                        None
                    } else {
                        self.start_analysis_run()
                    }
                }
                sample_keys::Submitted::Draw { sample, anyway } => {
                    self.analysis_modal.sample_form = None;
                    self.apply_table_sample(sample, None, anyway, !quality);
                    if !quality {
                        self.analysis_modal.computing =
                            Some(AnalysisProgress::new("Drawing the sample"));
                    }
                    None
                }
            };
        }
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
        // A view with a sample: the form edits it, and the rows come from the view it
        // was drawn from.
        let (sample, view) = match state.sampled() {
            Some(sampled) => (sampled.sample().clone(), true),
            None => (sample.clone(), false),
        };
        let context = self.sample_context(state.unsampled());
        let mut form = sample_modal::SampleForm::new(&sample, context, &self.theme);
        form.inline = inline;
        form.view = view;
        form.bytes_per_row = Some(state.unsampled().sample_row_bytes(false));
        form.source_bytes_per_row = Some(state.unsampled().sample_row_bytes(true));
        self.analysis_modal.sample_form = Some(form);
        self.sync_sample_form_focus();
    }

    /// What the Sample form offers for `state`'s rows: its partitions, files, time
    /// columns and the columns an equal-per-value sample can split by.
    pub(crate) fn sample_context(&self, state: &DataTableState) -> sample_modal::SampleContext {
        let mut partition_columns = state.partition_columns().unwrap_or_default().to_vec();
        let mut partition_values = Vec::new();
        // A directory whose files agree opens as one scan and names no partition
        // columns; its directory names still do. One branch of the tree is walked for
        // the columns and one listing read for the first column's values: local,
        // and small next to opening the dataset.
        if let Some(dir) = self.path.as_ref().filter(|path| path.is_dir()) {
            if partition_columns.is_empty() {
                partition_columns = crate::readers::hive::discover_hive_partition_columns(dir)
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
        sample_modal::SampleContext {
            view_rows: state.num_rows_if_valid(),
            filtered: state.changes_rows(),
            files: state.quality_source_file_names().to_vec(),
            partition_columns,
            partition_values,
            time_columns: state.quality_temporal_columns(&data_quality::QualityScope::WholeSource),
            value_columns,
        }
    }

    fn sample_form_key(&mut self, event: &KeyEvent) -> Option<AppEvent> {
        let form = self.analysis_modal.sample_form.as_mut()?;
        let file_count = form.context.files.len();
        let key = form::key(form, event);
        match key {
            // In a tool's empty pane the form stays, as it was: Esc discards the
            // edit and hands the cursor back to the tool list.
            FormKey::Cancel if form.inline => {
                self.analysis_modal.focus = analysis_modal::AnalysisFocus::Sidebar;
                self.open_first_run_form();
            }
            FormKey::Cancel => self.analysis_modal.sample_form = None,
            FormKey::Submit => return self.run_sample_form(),
            FormKey::Step(_, delta) => {
                form.adjust(delta > 0);
                form.edited();
            }
            FormKey::Text(sample_modal::SampleField::Files)
                if matches!(event.code, KeyCode::PageDown | KeyCode::PageUp) =>
            {
                form.file_offset = if event.code == KeyCode::PageDown {
                    (form.file_offset + crate::widgets::sample_form::FILES_SHOWN)
                        .min(file_count.saturating_sub(1))
                } else {
                    form.file_offset
                        .saturating_sub(crate::widgets::sample_form::FILES_SHOWN)
                };
            }
            FormKey::Text(_) => {
                if let Some(input) = form.input_mut(form.field) {
                    let _ = input.handle_key(event, None);
                }
                form.edited();
            }
            FormKey::Act(_) | FormKey::Moved | FormKey::Other => {}
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

    /// Whether the Pivot & Melt builder is waiting on a pivot it started.
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
        let fields = follow.take_new_fields();
        if let Some(handle) = follow.take_held() {
            state.read_followed_through(&handle);
        }
        if let Some(message) = message {
            self.flash_note(message);
        }
        if !fields.is_empty() {
            self.followed_fields_held = Some((self.dataset_generation, fields));
        }
        self.catch_up_follow();
        self.join_followed_fields();
        self.describe_ended_journal();
    }

    /// Read a piped journal's Info tab again once it has ended, over every entry: the
    /// one the open read describes the entries that had arrived then. Not a job, which
    /// the user would wait on; the table works meanwhile.
    fn describe_ended_journal(&mut self) {
        let Some(lf) = self
            .data_table_state
            .as_mut()
            .and_then(|state| state.ended_journal_to_describe())
        else {
            return;
        };
        let generation = self.dataset_generation;
        let tx = self.events.clone();
        self.runtime.spawn_blocking(move || {
            let detail = logging::catch_panic(|| crate::journal::summary(&lf).ok())
                .ok()
                .flatten();
            if let Some(detail) = detail {
                let _ = tx.send(AppEvent::FollowedDetail {
                    dataset_generation: generation,
                    detail: Box::new(detail),
                });
            }
        });
    }

    /// Join the fields a followed pipe brought after the open, if the dataset can
    /// take them now, and read the rows on screen through the wider frame. Tried again
    /// after every event while they wait, as footers are.
    fn join_followed_fields(&mut self) {
        let Some((generation, _)) = self.followed_fields_held.as_ref() else {
            return;
        };
        if *generation != self.dataset_generation {
            self.followed_fields_held = None;
            return;
        }
        // Not under rows still being taken: the view reads the new rows first.
        if self.data_table_state.is_none()
            || self.work_the_join_would_cancel()
            || self.follow().is_some_and(|f| f.behind())
        {
            return;
        }
        let Some((generation, fields)) = self.followed_fields_held.take() else {
            return;
        };
        let Some(state) = self.data_table_state.as_mut() else {
            return;
        };
        match state.join_followed_fields(&fields) {
            Ok(true) => {
                self.spawn_async_collect(Self::LOADING_BUFFER);
            }
            Ok(false) => {}
            Err(()) => self.followed_fields_held = Some((generation, fields)),
        }
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

    /// Show a completion flash on the footer.
    fn flash_note(&mut self, message: String) {
        self.flash = Some(Flash::new(message));
    }

    /// A completion flash that ends in the path written: `Exported to …/out.csv`.
    fn flash_path(&mut self, prefix: &str, path: &std::path::Path) {
        self.flash = Some(Flash::path(prefix, path));
    }

    /// The completion flash on the footer, if one is showing.
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

    /// What the footer says about the follow of the dataset on screen.
    fn follow_mark(&self) -> Option<crate::render::footer::FollowMark> {
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
            return rec.is_some().then(|| crate::render::footer::FollowMark {
                rec,
                rec_stopped,
                ..Default::default()
            });
        };
        let waiting = follow.waiting();
        let (chip, note) = match follow.standing {
            // Standard input read as it arrives: how much has, until it ends.
            Standing::Following if follow.is_pipe() => {
                let read = follow.spool().map_or(0, |spool| spool.bytes());
                let chip = format!(
                    "reading stdin {} {}",
                    crate::glyphs::get().middot,
                    crate::numfmt::bytes(read)
                );
                let note = (follow.new_below > 0 && !state.on_last_row())
                    .then(|| rows(follow.new_below, "new below"));
                (chip, note)
            }
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
        Some(crate::render::footer::FollowMark {
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

    /// Show the next Polars user warning on the footer, once per session, when the
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
        let stop_sample =
            key.code == KeyCode::Esc && self.sample_drawing() && self.in_normal_table_view();
        // The help reads nothing, so it can always be closed, a load's screen included.
        let close_help = self.help.is_open()
            && matches!(key.code, KeyCode::Esc | KeyCode::F(1) | KeyCode::Char('?'));
        quit || home
            || close_help
            || cancel_analysis
            || cancel_pivot
            || cancel_view
            || cancel_find
            || stop_sample
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
        if self.hard_escape_while_busy(key) || self.menu_takes(key) {
            return true;
        }
        if self.key_acts_while_sampling(key) {
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
                    | KeyCode::Char('{')
                    | KeyCode::Char('}')
                    | KeyCode::F(1)
                    | KeyCode::Char('?')
        )
    }

    /// The plain table view: Normal mode with no help overlay, modal, or in-view modal
    /// (view, analysis) or context menu drawn over it.
    pub fn in_normal_table_view(&self) -> bool {
        self.input_mode == InputMode::Normal
            && !self.help.is_open()
            && !self.view_modal.active
            && !self.analysis_modal.active
            && !self.error_modal.active
            && !self.confirmation_modal.active
            && self.context_menu.is_none()
    }

    /// While a header is dragged over another column, a rule on the header where it
    /// would land: after that column when it moves right, before it when left.
    fn render_drop_mark(&self, buf: &mut Buffer, ctx: &crate::render::context::RenderContext) {
        let Some(pointer::Drag::Move { column, over }) = self.pointer.drag() else {
            return;
        };
        let Some(state) = self.data_table_state.as_ref() else {
            return;
        };
        let Some((header, columns)) = state.drawn_header() else {
            return;
        };
        let order = state.get_column_order();
        let (Some(from), Some(to)) = (
            order.iter().position(|c| c == column),
            order.iter().position(|c| c == over),
        ) else {
            return;
        };
        // Only where a drop would land: frozen among frozen, scrolling among scrolling.
        let locked = state.locked_columns_count().min(order.len());
        if from == to || (from < locked) != (to < locked) {
            return;
        }
        let Some((left, right, _)) = columns.iter().find(|(_, _, name)| name == over) else {
            return;
        };
        let x = if to > from {
            *right
        } else {
            left.saturating_sub(1)
        };
        if !(header.x..header.right()).contains(&x) {
            return;
        }
        let g = crate::glyphs::get();
        for y in header.y..header.bottom() {
            let cell = &mut buf[(x, y)];
            cell.set_symbol(g.rule);
            cell.set_style(Style::default().fg(ctx.accent));
        }
    }

    /// The context menu is open over the plain table view, nothing over it.
    pub(crate) fn menu_showing(&self) -> bool {
        self.context_menu.is_some()
            && self.input_mode == InputMode::Normal
            && self.data_table_state.is_some()
            && !self.help.is_open()
            && !self.view_modal.active
            && !self.analysis_modal.active
            && !self.error_modal.active
            && !self.confirmation_modal.active
    }

    /// Whether `key` is one the open menu answers itself (moving, choosing, closing),
    /// which reads nothing and so acts while busy.
    pub(crate) fn menu_takes(&self, key: &KeyEvent) -> bool {
        self.menu_showing()
            && key.modifiers.is_empty()
            && matches!(
                key.code,
                KeyCode::Up
                    | KeyCode::Down
                    | KeyCode::Char('j')
                    | KeyCode::Char('k')
                    | KeyCode::Enter
                    | KeyCode::Esc
            )
    }

    /// Open the context menu at `at`, over the cell the cursor was just put on.
    pub fn open_context_menu(&mut self, at: ratatui::layout::Position) {
        // A datetime is made from text, a date or a time.
        let combine = self
            .data_table_state
            .as_ref()
            .and_then(|state| {
                let column = state.current_column()?;
                state.schema().get(column).cloned()
            })
            .is_some_and(|dtype| {
                matches!(dtype, DataType::String | DataType::Date | DataType::Time)
            });
        self.context_menu = Some(context_menu::ContextMenu::with(
            at,
            context_menu::column_items(combine),
        ));
    }

    /// Close the context menu, if it is open.
    pub fn close_context_menu(&mut self) {
        self.context_menu = None;
    }

    /// The line `i` of the open menu, chosen: the menu closes and its key is
    /// pressed, offered as typed.
    pub fn choose_from_menu(&mut self, i: usize) -> Option<AppEvent> {
        let menu = self.context_menu.take()?;
        match menu.chosen(i)? {
            context_menu::MenuKey::Run(key) => Some(AppEvent::Press(key)),
            context_menu::MenuKey::Do(action) => {
                self.menu_action(action);
                None
            }
            _ => None,
        }
    }

    /// A header dropped on another column: the order with `column` moved to where
    /// `onto` is, as `H` / `L` would leave it pressed that many times. A frozen column
    /// moves among the frozen ones only, and a scrolling one among the scrolling.
    pub fn drop_column(&mut self, column: &str, onto: &str) -> Option<AppEvent> {
        let state = self.data_table_state.as_ref()?;
        let mut order = state.headers();
        let locked = state.locked_columns_count().min(order.len());
        let from = order.iter().position(|c| c == column)?;
        let to = order.iter().position(|c| c == onto)?;
        if from == to || (from < locked) != (to < locked) {
            return None;
        }
        self.flash = None;
        self.data_table_state.as_mut()?.set_current_column(column);
        let moving = order.remove(from);
        order.insert(to, moving);
        // The sidebar places hidden columns by the order it last applied; the column
        // moves there too, so the shown order agrees with the table (a hidden one may
        // sit otherwise than repeated H / L would leave it).
        let applied = &mut self.sort_filter_modal.sort.applied_order;
        if let (Some(i), Some(j)) = (
            applied.iter().position(|c| c == column),
            applied.iter().position(|c| c == onto),
        ) {
            let moving = applied.remove(i);
            applied.insert(j, moving);
        }
        Some(AppEvent::ColumnOrder(order, locked))
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
            InputMode::PickFormat | InputMode::Retype => true,
            InputMode::Combine => self
                .combine
                .as_ref()
                .is_some_and(|c| c.picker.is_some() || c.focus == retype_modal::CombineField::Name),
            InputMode::PickTable => true,
            InputMode::Sample => self
                .sample_form
                .as_ref()
                .is_some_and(|form| form.field.is_text()),
            // The whole inline editor types (pickers narrow, the value edits), as
            // do the add-sort Picker and the Columns tab's find.
            InputMode::SortFilter => self.sort_filter_modal.typing(),
            InputMode::PivotMelt => {
                // The Picker narrows by typing, so it types too.
                self.pivot_melt_modal.picker.is_some()
                    || self
                        .pivot_melt_modal
                        .is_text_row(self.pivot_melt_modal.focus)
            }
            InputMode::Chart => {
                if self.chart_export_modal.active {
                    self.chart_export_modal
                        .input(self.chart_export_modal.focus)
                        .is_some()
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
                    || (self.view_modal.active
                        && self.view_modal.mode != ViewModalMode::List
                        && matches!(
                            self.view_modal.form_focus,
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
    /// dataset's footer counting footers belonging to the directory you left.
    /// Whether the row count on the footer is on its way, so a spinner stands in
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
            || (self.row_count_pending() && !self.awaiting_open_confirmation())
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
        // Back from home to the table whose lines were being indexed.
        if self.indexing_paused && self.input_mode != InputMode::Home {
            self.index_lines();
        }
        self.footers_this_frame = self.footer_progress().reading();
        self.listed_this_frame = self.footer_progress().listed();
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

    /// What the loading screen and the footer say about the open in flight: its
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
            // A listing has no total to count towards, so it says how far it has got.
            None => match self.listed_this_frame {
                Some(listed) => std::borrow::Cow::Owned(format!(
                    "Listing files: {}",
                    crate::numfmt::group_chrome(listed)
                )),
                None => std::borrow::Cow::Borrowed(phase),
            },
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
    /// answers are for a screen nobody is on, their lines on the footer, and the
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
            Step::AskRead(read) => {
                // As for a download: nothing runs, and the generation is held.
                self.loading.hold_while_asking(self.jobs.hold());
                self.confirmation_modal
                    .show(Self::in_memory_confirmation_message(&read));
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
        self.cache_writes
            .spawn(move || cache.record_dataset_facts(&[(url, facts)]));
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
        if matches!(
            self.status_message.as_deref(),
            Some(Self::COUNTING_FOR_END | Self::INDEXING_FOR_ROW)
        ) {
            self.status_message = None;
        }
    }

    /// What the status line says while `:N` waits for the lines to be indexed.
    const INDEXING_FOR_ROW: &'static str = "Reading lines to find the row...";

    /// What the status line says while an End is waiting on a row count. Named so the
    /// paths that retire such an End can take the message back down without reaching
    /// for a literal, and without clearing a message that belongs to something else.
    const COUNTING_FOR_END: &'static str = "Counting rows to find the end...";

    /// What the footer says while a path is being looked at. Named so the answer can
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

    /// Index the rest of a text file's lines behind its first rows, and say when they
    /// are all in ([`AppEvent::LinesIndexed`]). Not a job, which the user would wait on:
    /// the table works meanwhile, and a read of every line waits for them on its own
    /// worker. The last dataset's indexing, if it is still going, stops.
    fn start_indexing(&mut self) {
        self.end_when_indexed = None;
        self.goto_when_indexed = None;
        self.index_lines();
    }

    /// Run the indexing of the dataset on screen's lines, if they still have lines to
    /// index: a new dataset's, or one paused while home was up. Lines of a dataset no
    /// longer on screen stop for good, and the reads waiting on them give up.
    fn index_lines(&mut self) {
        use std::sync::atomic::Ordering;
        self.indexing_stop.store(true, Ordering::Relaxed);
        self.indexing_paused = false;
        let lines = self
            .data_table_state
            .as_ref()
            .and_then(|state| state.lines_to_index().cloned());
        if let Some(old) = self.indexing_lines.take()
            && lines.as_ref().is_none_or(|lines| !Arc::ptr_eq(lines, &old))
        {
            old.stop_indexing();
        }
        let Some(lines) = lines.filter(|lines| lines.resume_indexing()) else {
            return;
        };
        self.indexing_lines = Some(lines.clone());
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        self.indexing_stop = stop.clone();
        let generation = self.dataset_generation;
        let tx = self.events.clone();
        let waiting = lines.clone();
        let spawned = std::thread::Builder::new()
            .name("datui-index".to_string())
            .spawn(move || {
                loop {
                    // Paused or replaced: whoever stopped it says what becomes of the
                    // reads waiting on the lines.
                    if stop.load(Ordering::Relaxed) {
                        return;
                    }
                    // A panic stops it where it is: the rows so far are what there is,
                    // rather than a count that never comes.
                    let done =
                        logging::catch_panic(|| lines.index_more(INDEX_STEP)).unwrap_or(true);
                    if done {
                        lines.stop_indexing();
                        let rows = lines.rows();
                        let _ = tx.send(AppEvent::LinesIndexed { generation, rows });
                        return;
                    }
                }
            });
        // No thread to index them: the lines so far are what there is, and nothing
        // waits for more.
        if spawned.is_err() {
            waiting.stop_indexing();
            self.indexing_lines = None;
            if let Some(state) = self.data_table_state.as_mut() {
                state.lines_indexed(waiting.rows());
            }
        }
    }

    /// Home is up: the indexing waits, the reads waiting on it with it, until the
    /// table is back ([`Self::begin_frame`]).
    fn pause_indexing(&mut self) {
        if self.indexing_lines.is_some() {
            self.indexing_stop
                .store(true, std::sync::atomic::Ordering::Relaxed);
            self.indexing_paused = true;
        }
    }

    /// More of the dataset's lines are indexed: its frames take them, and once all are,
    /// its count and an End that waited for it.
    fn lines_indexed(&mut self, generation: u64, rows: usize) {
        if generation != self.dataset_generation {
            return;
        }
        let Some(state) = self.data_table_state.as_mut() else {
            return;
        };
        self.indexing_lines = None;
        if !state.lines_indexed(rows) {
            // Set aside while the lines finished (the quality evidence view): they
            // land on the dataset that comes back.
            if let Some(held) = self.quality_evidence_return.as_mut() {
                held.lines_indexed(rows);
            }
            return;
        }
        if let Some((goto, row)) = self.goto_when_indexed.take()
            && goto == generation
        {
            self.take_down_the_counting_status();
            let _ = self.events.send(AppEvent::GoToLine(row));
        }
        if self.end_when_indexed.take() == Some(generation) {
            self.take_down_the_counting_status();
            if let Some(next) = self.jump_key(AppEvent::DoScrollEnd) {
                let _ = self.events.send(next);
                return;
            }
        }
        // The count the indexing held back starts now, and rows past the first ones
        // read are read.
        if self.in_normal_table_view() && !self.loading.awaiting_dataset() {
            self.spawn_collect(None);
        }
    }

    /// Whether the dataset's count waits to be asked for: it has more files than
    /// `[read] exact_count_files` and an estimate to show meanwhile.
    fn count_held_at_estimate(&self, state: &DataTableState) -> bool {
        let limit = self.app_config.read.exact_count_files;
        limit > 0
            && state.files_to_count().is_some_and(|files| files > limit)
            && self.exact_count_asked != Some(self.dataset_generation)
            && state.row_estimate(None).is_some()
    }

    /// The dataset's row count from a sample of its footers, while it is not counted:
    /// the dataset's own, or the one its footer pass has said so far.
    pub(crate) fn row_estimate(&self) -> Option<crate::schema_union::RowEstimate> {
        self.data_table_state
            .as_ref()?
            .row_estimate(self.footer_progress.estimate())
    }

    /// Whether the count running reads footers it can say it has read, and so can be
    /// stopped: `(read, of)`.
    pub(crate) fn footers_counted(&self) -> Option<(usize, usize)> {
        self.len_count_inflight?;
        self.count_progress
            .reading()
            .filter(|_| !self.count_progress.is_cancelled())
    }

    /// `c` in the Info panel: count every row exactly, though the dataset has more
    /// files than the count reads unasked.
    pub(crate) fn count_exactly(&mut self) {
        let Some(state) = self.data_table_state.as_ref() else {
            return;
        };
        if state.is_num_rows_valid() {
            return;
        }
        let generation = state.len_generation();
        self.exact_count_asked = Some(self.dataset_generation);
        // The footer pass is still bringing the count; the request holds for when it
        // lands.
        if state.counts_itself_later() {
            return;
        }
        // A count stopped before is asked again.
        if self.len_count_failed == Some(generation) {
            self.len_count_failed = None;
        }
        // One stopped and not yet wound down: again once it has.
        if self.len_count_inflight == Some(generation) && self.count_progress.is_cancelled() {
            self.count_after_stop = Some(generation);
            return;
        }
        if self.len_count_inflight != Some(generation) {
            self.len_count_inflight = Some(generation);
            let job = LenCount::for_state(state);
            self.spawn_count(job);
        }
    }

    /// Esc while a count reads footers: stop it. What it read is kept for the next.
    fn stop_count(&mut self) {
        self.count_progress.cancel();
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
        found: Option<crate::table::FootersFound>,
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
        // A sample being drawn was the last dataset's, and so were its paths.
        self.put_down_sample_draw();
        self.sample_paths.clear();
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
            // Off the opening path. It takes a lock several instances may be contending
            // for -- opening a dataset must not queue behind another instance's
            // bookkeeping. Only the next home listing waits on it, on its worker.
            let cache = self.cache.clone();
            self.cache_writes.spawn(move || {
                cache.push_recent(&path);
            });
        }
        self.forget_the_rows_read();
        self.file_facts = None;
        let shown = home::catalogs(&self.app_config);
        self.codebook = path.as_deref().and_then(|p| home::codebook_for(&shown, p));
        self.catalog_entry = path
            .as_deref()
            .and_then(|p| home::catalog_entry_for(&shown, p));
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
                    let follow = crate::follow::Follow::start(
                        tail.clone(),
                        self.app_config.read.follow_interval.duration(),
                        self.events.clone(),
                        options.spool.clone(),
                    );
                    state.start_following(if options.pipe {
                        follow.as_pipe()
                    } else {
                        follow
                    });
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
        // Named for this file, so after its path is set.
        self.open_info_documentation();
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
            self.count_unfit();
        }
        // The dataset is on screen now; whatever it still has to learn about itself is
        // read behind it.
        self.start_pending_footers();
        self.start_indexing();
        // `#` for text and logs, unless the flag or the config said.
        if options.row_numbers_auto
            && let Some(state) = self.data_table_state.as_mut()
            && state.numbered_by_default()
        {
            state.set_row_numbers(true);
        }
        self.sort_filter_modal = SortFilterModal::new();
        self.pivot_melt_modal = PivotMeltModal::new();
        self.status_message = Some(Self::LOADING_BUFFER.to_string());

        // The dataset is installed and its schema known, so this is where a view
        // meets it. `--view` names one and applies to this first open alone;
        // `[views] auto_apply` dresses every open that has a matching view.
        // A fresh dataset starts with no view applied: the previous file's view
        // must not wear the check mark here, nor count as applied when edited.
        self.active_view_id = None;
        let (view, reason) = match self.startup_view.take() {
            Some(name) => match self.view_manager.get_view_by_name(&name).cloned() {
                Some(view) => (Some(view), None),
                None => {
                    self.error_modal.show(format!("No view named \"{name}\""));
                    (None, None)
                }
            },
            None if self.app_config.views.auto_apply => self
                .view_dataset()
                .zip(self.data_table_state.as_ref())
                .and_then(|(dataset, state)| {
                    self.view_manager
                        .get_most_relevant(dataset, state.source_schema())
                })
                .map_or((None, None), |(view, reason)| (Some(view), Some(reason))),
            None => (None, None),
        };
        let Some(view) = view else {
            return false;
        };
        let applied = match reason {
            // Applied unasked, it says which view and why.
            Some(why) => self.apply_matched_view(&view, why),
            None => self.apply_view(&view),
        };
        match applied {
            // The view reads its own first rows, so the dataset's are never read.
            Ok(()) => true,
            Err(e) => {
                self.error_modal
                    .show(format!("Error applying view \"{}\": {e}", view.name));
                false
            }
        }
    }

    /// Say that the view `name` was applied because its criteria fit as `why` says.
    fn flash_view_applied(&mut self, name: &str, why: view::MatchReason) {
        self.flash_note(format!("View \"{name}\" applied: {}", why.as_str()));
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
        let held = self
            .data_table_state
            .as_ref()
            .is_some_and(|state| self.count_held_at_estimate(state));
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
            // A dataset of too many files to count unasked shows its estimate.
            && !held
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
    fn spawn_count(&mut self, job: LenCount) {
        #[cfg(test)]
        self.counts_spawned.set(self.counts_spawned.get() + 1);
        self.count_progress = job.progress.clone();
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
            warn_in_memory_above: None,
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
        // Lines still being indexed: the end is where the indexing ends.
        if matches!(jump, AppEvent::DoScrollEnd)
            && let Some(state) = self.data_table_state.as_ref()
            && state.indexing().is_some()
        {
            self.end_when_indexed = Some(self.dataset_generation);
            self.status_message = Some(Self::COUNTING_FOR_END.to_string());
            return None;
        }
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
        F: FnOnce(&mut crate::table::DataTableState) -> bool,
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
        self.export_modal
            .path_input
            .handle_key(event, Some(&self.cache));
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
        let views = ViewManager::load_or_empty().into();
        Self::new_with_views(events, runtime, theme, app_config, views)
    }

    /// An app whose saved views may still be on their way ([`Views`]).
    pub fn new_with_views(
        events: Sender<AppEvent>,
        runtime: tokio::runtime::Handle,
        theme: Theme,
        app_config: AppConfig,
        view_manager: Views,
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

        let theme_problem = app_config.theme.fallbacks.first().cloned();
        let chart_export_modal = ChartExportModal {
            recipe: app_config.chart.export_recipe,
            ..ChartExportModal::new()
        };
        let mut app = App {
            path: None,
            data_table_state: None,
            footer_progress: Arc::new(crate::schema_union::FooterProgress::default()),
            footers_this_frame: None,
            listed_this_frame: None,
            home: home::HomeState {
                hide_unreadable: !app_config.home.show_unreadable,
                formats: formats.clone(),
                ..Default::default()
            },
            home_probes_inflight: Vec::new(),
            home_listing_cancels: HashMap::new(),
            home_narrowing: None,
            #[cfg(feature = "cloud")]
            cloud_discovery_started: false,
            home_search_inflight: false,
            home_search_generation: 0,
            home_generation: 0,
            home_refresh_owed: false,
            home_schema_inflight: Vec::new(),
            last_load_error: None,
            pending_clear_recents: false,
            pending_link: None,
            local_desktop: link_open::local_desktop(link_open::Platform::current(), |name| {
                std::env::var(name).ok()
            }),
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
            codebook: None,
            catalog_entry: None,
            documentation: Default::default(),
            info_documentation: Default::default(),
            remembered_moved: false,
            head_web_rows: !cache::running_as_a_cargo_test(),
            query_input: TextInput::new()
                .with_history_limit(app_config.query.history_limit)
                .with_theme(&theme)
                .with_history("query".to_string()),
            sql_input: TextInput::statement()
                .with_history_limit(app_config.query.history_limit)
                .with_theme(&theme)
                .with_history("sql".to_string()),
            find: find::Find::new(
                TextInput::new()
                    .with_history_limit(app_config.query.history_limit)
                    .with_theme(&theme)
                    .with_history("find".to_string()),
            ),
            column_hints: false,
            input_mode: InputMode::Normal,
            input_type: None,
            query_mode: QueryMode::default().resolve(),
            query_mode_chosen: None,
            query_text_restored: false,
            sql_columns: Vec::new(),
            sql_completion: None,
            query_running: None,
            query_run_error: None,
            inline_failures: 0,
            sort_filter_modal: SortFilterModal::new(),
            pivot_melt_modal: PivotMeltModal::new(),
            view_modal: ViewModal::new(),
            opened_from_home: false,
            startup_view: None,
            analysis_modal: AnalysisModal::with_sample_rows(app_config.analysis.sample_rows),
            sample_form: None,
            memory_probe: std::sync::Arc::new(table_sample::available_memory),
            sample_paths: Vec::new(),
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
            chart_export_modal,
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
            retype: None,
            combine: None,
            retype_from_info: false,
            table_picker: crate::widgets::ui::PickerState::default(),
            table_choices: None,
            clipboard: None,
            pending_copy: None,
            chart_cache: ChartCache::default(),
            chart_inflight: None,
            chart_asked: None,
            pending_chart_result: Arc::new(Mutex::new(None)),
            chart_export_waiting: None,
            error_modal: ErrorModal::new(),
            flash: None,
            confirmation_modal: ConfirmationModal::new(),
            pending_export: None,
            pending_delete_view: None,
            pending_hide_examples: false,
            pending_chart_export: None,
            pending_quality_export: None,
            help: help::Help::default(),
            pointer: pointer::Pointing::default(),
            context_menu: None,
            cache,
            cache_writes: CacheWrites::default(),
            view_manager,
            active_view_id: None,
            export_progress: None,
            theme,
            pending_read_all: false,
            history_limit: app_config.query.history_limit,
            table_cell_padding: app_config.display.cell_padding.cells(),
            column_colors: app_config.display.column_colors,
            dtype_row: app_config.display.type_row,
            number_format: app_config
                .display
                .number_format
                .resolve(app_config.display.right_align_numbers)
                // AppConfig::load validates this, but App can be built from an
                // unvalidated config (e.g. the Python API): fall back to no
                // formatting while still honouring the alignment setting.
                .unwrap_or_else(|_| NumberFormatSettings {
                    align_numeric_right: app_config.display.right_align_numbers,
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
            followed_fields_held: None,
            reread_owed: None,
            #[cfg(test)]
            home_worker_dies: None,
            #[cfg(test)]
            file_facts_reader: None,
            end_when_the_footers_land: None,
            end_when_indexed: None,
            indexing_stop: Arc::default(),
            indexing_lines: None,
            indexing_paused: false,
            goto_when_indexed: None,
            count_progress: Arc::default(),
            exact_count_asked: None,
            count_after_stop: None,
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
            background_query: false,
            formats,
        };
        // A theme that could not be used: why is said on stderr after exit.
        if let Some(problem) = theme_problem {
            app.flash_note(problem);
        }
        app
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
        if self.home_refresh_owed {
            self.home_refresh();
        }
        #[cfg(feature = "http")]
        self.size_selected_web_file();
        // The rows on screen as the frame left them: each pass asks for those still
        // unknown, a batch at a time. Not under the path prompt, which hides the list.
        if !self.home.path_input_active {
            self.request_home_measurements();
            self.request_home_classifications();
            #[cfg(feature = "cloud")]
            self.peek_cloud_directories();
        }
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

    /// Info's Documentation tab for the open dataset: the catalog entry it is, or is
    /// inside, with what the format spec that read it says; closed when neither says.
    fn open_info_documentation(&mut self) {
        self.info_documentation.close();
        let spec = self.data_table_state.as_ref().and_then(|state| {
            state
                .format_read()
                .map(|read| read.spec.clone())
                .or_else(|| state.delimited_read().map(|read| read.spec.clone()))
        });
        let name = self
            .path
            .as_deref()
            .and_then(Path::file_name)
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        if let Some(doc) = widgets::documentation::Documented::new(
            self.catalog_entry.clone(),
            spec.and_then(|s| s.docs()).map(std::sync::Arc::new),
            name,
        ) {
            self.info_documentation.open(doc, None);
            self.info_documentation.links_open = self.local_desktop;
        }
    }

    /// A key while the Documentation view is open over home.
    fn documentation_key(&mut self, event: &KeyEvent) {
        let page = self.documentation.view_height.max(1) as isize;
        match event.code {
            KeyCode::Esc | KeyCode::Char('q') | KeyCode::Left => self.documentation.close(),
            KeyCode::Up | KeyCode::Char('k') => self.documentation.move_cursor(-1),
            KeyCode::Down | KeyCode::Char('j') => self.documentation.move_cursor(1),
            KeyCode::PageUp => self.documentation.move_cursor(-page),
            KeyCode::PageDown => self.documentation.move_cursor(page),
            KeyCode::Home | KeyCode::Char('g') => self.documentation.move_cursor(isize::MIN / 2),
            KeyCode::End | KeyCode::Char('G') => self.documentation.move_cursor(isize::MAX / 2),
            KeyCode::Enter | KeyCode::Char(' ') | KeyCode::Right => {
                self.documentation.toggle_legend();
            }
            KeyCode::Char('y') => self.copy_documentation_line(),
            KeyCode::Char('o') => {
                let link = self.documentation.link();
                self.ask_to_open_link(link);
            }
            // The view takes no text, so ? is help here, as at the table.
            KeyCode::Char('?') => self.open_help_overlay(),
            _ => {}
        }
    }

    /// `y` in the Documentation view: the line's link or value, whole.
    fn copy_documentation_line(&mut self) {
        match self.documentation.copy_text() {
            Some(text) => self.copy_documentation_text(text),
            None => self.flash_note("Nothing to copy on this line".to_string()),
        }
    }

    /// `o` on a Documentation page: ask, with the whole URL, before the browser
    /// opens it. Nothing on a line without a link; a status line where no local
    /// browser would show it, or the link is not http or https.
    pub(crate) fn ask_to_open_link(&mut self, link: Option<String>) {
        let Some(link) = link else {
            return;
        };
        if !self.local_desktop {
            self.flash_note("o opens links on a local desktop; y copies it".to_string());
            return;
        }
        match link_open::checked_url(&link) {
            Ok(url) => {
                self.confirmation_modal
                    .show_choice(format!("Open {url}?"), "Open", "Cancel");
                self.pending_link = Some(url);
            }
            Err(why) => self.flash_note(format!("Not opened: {why}; y copies it")),
        }
    }

    /// Put a line of a Documentation page on the clipboard, whole.
    pub(crate) fn copy_documentation_text(&mut self, text: String) {
        let shown: String = text.chars().take(60).collect();
        let said = if shown.len() < text.len() {
            format!("Copied {shown}{}", glyphs::get().ellipsis)
        } else {
            format!("Copied {shown}")
        };
        self.finish_copy(clipboard::Payload::text(text), said);
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
            && crate::hf_splits::split_place(&path).is_none()
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
            .catalogs
            .iter()
            .filter(|c| c.origin == catalog::Origin::Bundled)
            .flat_map(|c| c.datasets.iter())
            .find(|d| d.location == path)
            .filter(|_| {
                !jump && matches!(source::input_source(&path), source::InputSource::Http(_))
            })
            .map(|dataset| UnaskedDownload {
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

    /// What an open the home screen starts reads with: the config's read and CSV
    /// settings, as an open named on the command line has them under its flags.
    fn open_defaults(&self) -> OpenOptions {
        match crate::cli::parse_args(["datui"]) {
            Ok(args) => OpenOptions::from_args_and_config(&args, &self.app_config),
            Err(_) => OpenOptions::default(),
        }
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
        DataTableState::from_read(
            crate::readers::csv::read_delimited(path, separator, options, writer)?,
            options,
        )
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
        .chain([(
            AmazonS3ConfigKey::Client(crate::user_agent::CLIENT_KEY),
            crate::user_agent::get(),
        )])
        .collect();
        CloudOptions::default().with_aws(configs)
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
        crate::user_agent::ureq_config()
            .timeout_global(Some(total))
            .build()
            .into()
    }

    /// What a HEAD says an HTTP(S) file weighs: `None` when it does not say. An error
    /// only when the answer settles that the file cannot be had (a 404, no server); a
    /// server that refuses HEAD may still send the file.
    #[cfg(feature = "http")]
    fn fetch_remote_size_http(
        url: &str,
    ) -> std::result::Result<Option<u64>, crate::error_display::HttpGone> {
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
            Err(e) => crate::error_display::http_gone(url, &e).map_or(Ok(None), Err),
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
    ///
    /// Past `limit` bytes it stops with a [`crate::download::PastLimit`] error.
    #[cfg(feature = "http")]
    fn download_http_to_temp(
        url: &str,
        temp_dir: Option<&Path>,
        extension: Option<&str>,
        limit: Option<u64>,
        writer: &crate::unfinished::Writer,
    ) -> Result<crate::download::TempDownload> {
        use crate::download::StreamError;

        let url = url.to_string();
        let open = move || {
            let agent = Self::http_agent(std::time::Duration::from_secs(300));
            // ureq answers a 4xx or 5xx with an error, so every failure is said here.
            let response = agent
                .get(&url)
                .call()
                .map_err(|e| crate::error_display::http_message(&url, &e))?;
            // No length: ureq hands back a compressed answer decompressed, and the
            // Content-Length it came with is the wire's, not the file's.
            Ok((response.into_body().into_reader(), None))
        };
        crate::download::read_to_temp(temp_dir, extension, open, writer, limit).map_err(|error| {
            match error {
                StreamError::Open(message) => color_eyre::eyre::eyre!(message),
                StreamError::Read(e) => {
                    color_eyre::eyre::eyre!("Download failed partway. Check your connection: {e}")
                }
                StreamError::Short { expected, got } => color_eyre::eyre::eyre!(
                    "Download failed partway: it ended after {got} of {expected} bytes."
                ),
                StreamError::Write(report) => report,
                StreamError::Cut => color_eyre::eyre::eyre!("Download was cancelled."),
            }
        })
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
            return Err(crate::error_display::FileError::new(
                Path::new(url),
                format!("a {label} URL names an object here, such as {example}"),
            )
            .into());
        }
        let (_, _, store) = Self::cloud_store_for(Path::new(url), cloud, runtime)?;

        let path = crate::cloud_browse::object_path(&key);
        let open = async move {
            let got = store
                .get(&path)
                .await
                .map_err(|e| crate::error_display::store_message(&e))?;
            let len = got.range.end - got.range.start;
            Ok((got.into_stream(), Some(len)))
        };
        let failed = |what: String| -> color_eyre::Report {
            crate::error_display::FileError::new(Path::new(url), what).into()
        };
        crate::download::stream_to_temp(
            runtime,
            options.temp_dir.as_deref(),
            ext.as_deref(),
            open,
            writer,
        )
        .map_err(|error| match error {
            StreamError::Open(e) => failed(e),
            StreamError::Read(e) => failed(format!("the download stopped: {e}")),
            StreamError::Short { expected, got } => {
                failed(format!("it ended after {got} of {expected} bytes"))
            }
            StreamError::Write(report) => report,
            StreamError::Cut => failed("the download was cancelled".to_string()),
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
            guessed: false,
            read_notes: Vec::new(),
            typing: Default::default(),
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
        // An Arrow IPC stream followed is read by a scan of its own, not converted;
        // NDJSON followed is scanned rather than read whole, by its reader.
        let followed_stream = options.follow
            && crate::follow::followed_stream(
                &paths[0],
                Some(crate::follow::format_of(&paths[0], options.format)),
                &options,
            );
        let scan = if followed_stream {
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
            format_guessed: options.format_guessed || report.guessed,
            read_notes: report.read_notes,
            typing: report.typing,
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
                            // A file that is not there, or a host that does not
                            // answer, ends the open here, not after a question
                            // about downloading it.
                            Self::fetch_remote_size_http(url).map_err(|gone| gone.message)?
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
                    .map(|size| format!("Downloading {}...", crate::numfmt::bytes(size)));
                let status = sized.as_deref().unwrap_or(status);
                self.spawn_job(job, Some(status), move |_| {
                    let (url, _, options) = pending.parts();
                    let fetched = match &pending {
                        #[cfg(feature = "http")]
                        loading::PendingDownload::Http { .. } => {
                            let ext = source::download_suffix(url);
                            // A download nobody was asked about stops at its limit, when
                            // the server did not say its size: a size it said bounds the
                            // transfer, and the bytes counted here are decompressed.
                            let limit = options
                                .download_unasked
                                .filter(|_| pending.parts().1.is_none())
                                .map(|unasked| unasked.limit);
                            Self::download_http_to_temp(
                                url,
                                options.temp_dir.as_deref(),
                                ext.as_deref(),
                                limit,
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
                    };
                    let (download, options) = match fetched {
                        Err(e) if e.downcast_ref::<crate::download::PastLimit>().is_some() => {
                            return Ok(Answer::Load(Box::new(LoadAnswer::PastLimit(pending))));
                        }
                        fetched => fetched.map_err(|e| {
                            crate::error_display::user_message_from_report(&e, None)
                        })?,
                    };
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
                    // And read as it arrives when what it holds can be.
                    let (download, options) = if options.follow
                        || options.tee.is_some()
                        || crate::stdin::may_read_as_it_arrives(&options)
                    {
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
                        Err(crate::error_display::file_message(
                            &url,
                            "this build reads no URLs",
                        ))
                    };
                    // The URL in the message may carry a password or a signature.
                    let bytes = fetched
                        .map_err(|message| crate::logging::redact(&message, &[]))?
                        .ok_or_else(|| {
                            crate::logging::redact(
                                &crate::error_display::file_message(
                                    &url,
                                    &format!(
                                        "a format spec is at most {}",
                                        crate::formats::MAX_SPEC_SAID
                                    ),
                                ),
                                &[],
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
                    let copy = crate::readers::csv::decompress_to_copy(
                        &file,
                        compression,
                        &temp_dir,
                        &writer,
                    )
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
                // Only delimited text and lines come this way, the format said by the
                // loader or the scan.
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
                    let lines = options.delimited.is_none()
                        && options.format.is_some_and(FileFormat::is_lines);
                    let (state, opened) = if lines {
                        let (read, opened) =
                            crate::readers::csv::from_lines_decompressed(&file, &options, &writer)
                                .map_err(failed)?;
                        let state = DataTableState::from_read(read, &options).map_err(failed)?;
                        (state, Some(opened))
                    } else {
                        let state = Self::decompressed_delimited_state(&file, &options, &writer)
                            .map_err(failed)?;
                        (state, None)
                    };
                    let mut open_notes = options
                        .delimited
                        .as_ref()
                        .map(|read| read.notes())
                        .unwrap_or_default();
                    open_notes.extend(opened.iter().flat_map(|o| o.notes.iter().cloned()));
                    let state = state.with_open(OpenFacts {
                        fetched: Self::fetched(download.as_ref(), Some(&path)),
                        download,
                        open_notes,
                        records: opened.and_then(|o| o.window),
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
                    writes: self.cache_writes.clone(),
                };
                self.spawn_job(job, Some("Reading schema..."), move |_| {
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
            Step::AskRead(_) => unreachable!("not a phase with a worker"),
        }
    }

    /// What the user is asked before files past `[read] memory_warning` are
    /// read whole into memory: `big.json: JSON reads 2.1 GB into memory`.
    fn in_memory_confirmation_message(read: &loading::InMemory) -> String {
        let what = match read.files {
            1 => format!(
                "{}: {} reads",
                read.name
                    .file_name()
                    .map(|n| n.to_string_lossy().into_owned())
                    .unwrap_or_else(|| read.name.display().to_string()),
                read.format.title()
            ),
            n => format!("{n} {} files read", read.format.title()),
        };
        format!(
            "{what} {} into memory before the table appears.\n\nRead it?",
            crate::numfmt::bytes(read.bytes)
        )
    }

    /// What the user is being asked to agree to before a remote file is downloaded.
    #[cfg(any(feature = "http", feature = "cloud"))]
    fn download_confirmation_message(
        pending: &loading::PendingDownload,
        note: Option<&str>,
    ) -> String {
        let (url, size, options) = pending.parts();
        let size_str = size
            .map(crate::numfmt::bytes)
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

/// Bytes of a text file indexed per step behind its first rows, between which the
/// indexing looks whether it is still wanted.
const INDEX_STEP: usize = 16 << 20;

/// What a pass behind a staged open reported, and which dataset it was reading for.
/// `None` where the footers are: a pass that could not read them says so, so the
/// dataset stops waiting.
type FootersReported = Option<(u64, Option<crate::table::FootersFound>)>;

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
        dataset_files::open(Arc::new(dataset_files::LocalFiles::new(p)), options, report)
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
        dataset_files::open(
            Arc::new(dataset_files::StoreFiles::new(
                full,
                key,
                pattern.cloned(),
                store,
                cloud_opts,
                runtime,
            )),
            options,
            report,
        )
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
        footer: &cloud_hive::FileFooter,
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
                rows: Some(footer.rows()),
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
                Some(p) => crate::readers::hive::discover_hive_partition_columns(p)
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
            facts.indexing = opened.indexing.clone();
            facts.numbering = opened.numbering.clone();
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
        facts.open_notes.extend(options.read_notes.iter().cloned());
        facts.typing = options.typing.clone();
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
        // Not for one read by file: its counter reads only the footers the open did not.
        if options.hive
            && facts.remote_files.is_none()
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
        use object_store::azure::AzureConfigKey;
        use polars::io::cloud::GoogleConfigKey;
        let gcs_agent = (
            GoogleConfigKey::Client(crate::user_agent::CLIENT_KEY),
            crate::user_agent::get(),
        );
        let options = match resolved.kind {
            crate::source::ProviderKind::S3 => Self::build_s3_cloud_options(&resolved.s3),
            crate::source::ProviderKind::Gcs
                if resolved.signing == crate::cloud_sources::Signing::Unsigned =>
            {
                CloudOptions::default()
                    .with_gcp([(GoogleConfigKey::SkipSignature, "true".into()), gcs_agent])
            }
            crate::source::ProviderKind::Gcs => match &resolved.gcloud {
                // The token comes from `gcloud` whenever Polars asks, so a long scan
                // outlives the one fetched here.
                Some((configuration, _)) => CloudOptions::default()
                    .with_gcp([gcs_agent])
                    .with_credential_provider(Some(crate::gcloud::polars_provider(configuration))),
                None => match &resolved.google_credentials {
                    Some(file) => CloudOptions::default().with_gcp([
                        (
                            GoogleConfigKey::ApplicationCredentials,
                            file.to_string_lossy().into_owned(),
                        ),
                        gcs_agent,
                    ]),
                    None => CloudOptions::default().with_gcp([gcs_agent]),
                },
            },
            crate::source::ProviderKind::Azure => {
                let (account, _, _) = source::azure_parts(&resolved.url)
                    .ok_or_else(|| color_eyre::eyre::eyre!("not an Azure URL"))?;
                let mut azure = crate::azure::polars_options(&account, &resolved.azure);
                azure.push((
                    AzureConfigKey::Client(crate::user_agent::CLIENT_KEY),
                    crate::user_agent::get(),
                ));
                CloudOptions::default().with_azure(azure)
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
        let (footer, etag) = wait_on_runtime(runtime, async move {
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
        let column_bytes = crate::schema_union::column_bytes_per_row(&[Some(footer.clone())]);
        let facts = OpenFacts {
            remote_objects: vec![crate::local_copy::RemoteObject {
                url: full,
                size: footer.file_bytes as u64,
                etag,
            }],
            row_groups: vec![footer.row_group_rows],
            column_bytes,
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
            writes: report.writes.clone(),
        };

        let local = attempt(report);
        if let Some((state, facts)) = Self::schema_state_from_local_hive(path, options, &local) {
            let facts = OpenFacts {
                measurements: local.meter,
                ..facts
            };
            return Ok((state, facts, "one-file (local)".to_string()));
        }
        // An open abandoned mid-listing is not one for the routes below to scan whole.
        if report.progress.is_cancelled() {
            return Err(color_eyre::eyre::eyre!("cancelled"));
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

    /// Why `--table` was refused for `path`, a file of `format`, which holds one table.
    fn one_table(path: Option<&Path>, format: Option<FileFormat>) -> color_eyre::Report {
        match path {
            Some(path) => crate::error_display::FileError::new(path, cli::one_table(format)).into(),
            None => color_eyre::eyre::eyre!(cli::one_table(format)),
        }
    }

    /// A scan of `url` in an object store that Polars refused, with what to check.
    #[cfg(feature = "cloud")]
    fn cloud_scan_failed(url: &str, e: &polars::prelude::PolarsError) -> color_eyre::Report {
        let said = crate::error_display::user_message_from_polars(e);
        let (first, rest) = said.split_once('\n').unwrap_or((&said, ""));
        let first = first.trim_end().trim_end_matches('.');
        let what = format!("could not read it: {first}. Check the credentials and the URL.");
        let what = match rest {
            "" => what,
            rest => format!("{what}\n{rest}"),
        };
        crate::error_display::FileError::new(Path::new(url), what).into()
    }

    /// The inputs of an Arrow read as one table, in order: each IPC file scanned where
    /// it is, in a bucket or on disk, and each run of streams as its rows of
    /// `converted`, the IPC file they were converted to. Stacked as the files of a
    /// directory are ([`crate::readers::polars::union_of_files`]).
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
                crate::readers::polars::union_of_files(),
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
        // Null values are the one setting the sample cannot mirror: `--null`
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

    /// A format spec reads a local file, or the downloaded copy of one remote object
    /// (`loading::remote_download`). What reaches here remote is a prefix or a glob,
    /// which would otherwise be scanned in place without the spec and say nothing.
    fn refuse_spec_in_place(path: &Path, options: &OpenOptions) -> Result<()> {
        if source::is_remote_url(path)
            && (options.spec_file.is_some() || options.spec_name.is_some())
        {
            return Err(crate::error_display::FileError::new(
                path,
                "a format spec reads one remote object at a time, not a prefix or a glob; name the object",
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
                // Named by an input the user knows, never the converted copy.
                let named = parts.first().map(|part| match part {
                    crate::ipc_stream::Part::InPlace(path) => path.as_path(),
                    crate::ipc_stream::Part::Converted { source, .. } => source.as_path(),
                });
                return Err(Self::one_table(named, Some(FileFormat::Arrow)));
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
                    let lf = LazyFrame::scan_parquet(pl_path, args)
                        .map_err(|e| Self::cloud_scan_failed(&full, &e))?;
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
                    let lf = LazyFrame::scan_parquet(pl_path, args)
                        .map_err(|e| Self::cloud_scan_failed(&full, &e))?;
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
                    let lf = LazyFrame::scan_parquet(PlRefPath::new(full.as_str()), args)
                        .map_err(|e| Self::cloud_scan_failed(&full, &e))?;
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
            && let Some(first) = files.iter().find(|f| !crate::nul_tail::holds_nothing(f))
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
        // A spec's read says how its files differ itself, from their own header lines.
        report.files_disagree = Self::files_disagree(files, options, found);
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

        // A glob of local files the first of which a delimited spec reads: read through
        // the spec, file by file, as a directory of them is. Polars' own scan of the
        // glob would read the spec's header lines as data.
        if let [pattern] = paths
            && !options.hive
            && options.delimited.is_none()
            && options.format.is_none()
            && source::expands_as_glob(pattern)
        {
            let files = crate::local_glob::expand(pattern);
            if let Some(first) = files.iter().find(|f| !crate::nul_tail::holds_nothing(f))
                && let Some(choice) = Self::delimited_spec_of(first, options, formats)?
            {
                let format = FileFormat::from_path(first).filter(|f| f.separator().is_some());
                let nested = OpenOptions {
                    format: format.or(Some(FileFormat::Csv)),
                    ..options.clone()
                };
                return Self::read_with_delimited_spec(&files, &nested, report, formats, choice);
            }
        }

        // A format spec: one asked for, or one whose glob or magic the path matches. A
        // path whose name or bytes already say what it is opens as it always has, but
        // for a delimited spec's text. Several files are matched by the first, and
        // only to a delimited spec.
        if !options.hive && options.delimited.is_none() {
            let asked = crate::formats::Asked {
                spec_file: options.spec_file.clone(),
                spec_name: options.spec_name.clone(),
                variant: options.table.clone(),
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
                    // schema phase's ("Reading schema").
                    return crate::readers::hive::scan_parquet_hive(path).map(Scan::from);
                }
                return Err(color_eyre::eyre::eyre!(
                    "With --hive use a directory or a glob pattern for Parquet (e.g. path/to/dir or path/**/*.parquet)"
                ));
            }
        }

        // A file with no extension may still be Parquet: a part file in a directory named
        // `.parquet`. A regular file is only read when nothing else settled it. A name
        // that says text (`.log`, `.txt`) is read as lines unless its bytes say a format:
        // candump writes `.log`.
        let compressed = options
            .compression
            .or_else(|| CompressionFormat::from_extension(path))
            .is_some();
        // Under a compression suffix, the name before it says delimited text or lines
        // (`x.tsv.gz`, `app.log.gz`).
        let named = FileFormat::from_path(path).or_else(|| {
            compressed
                .then(|| FileFormat::from_path(Path::new(path.file_stem()?)))
                .flatten()
                .filter(|f| f.decompressed_once())
        });
        let mut effective_format = options
            .format
            // A name that says a format another refines is asked its bytes for it:
            // journal JSON in a `.json` file.
            .or_else(|| {
                named.filter(|f| !f.is_lines()).map(|f| {
                    (!compressed)
                        .then(|| crate::readers::refined(path, f))
                        .flatten()
                        .unwrap_or(f)
                })
            })
            .or_else(|| {
                (path.extension().is_none()
                    && crate::discover::is_parquet_key(&path.to_string_lossy()))
                .then_some(FileFormat::Parquet)
            })
            // Any other file whose name says no format, by its first bytes: each
            // format's signature says where it is believed (`crate::readers`).
            .or_else(|| crate::readers::sniff_open(path, options.compression))
            .or(named);
        // Text no signature claims: JSON, CSV or TSV on evidence, lines otherwise. Bytes
        // that are not text are shown as they are.
        if effective_format.is_none()
            && let [file] = paths
            && file.is_file()
        {
            effective_format = crate::lines::guess_file(file, options.compression)
                .map(|f| crate::lines::as_asked(f, options));
            report.guessed = effective_format.is_some();
        }
        report.format = effective_format;

        // Refused rather than ignored: a file of one table opened with `--table` would
        // otherwise look like the table asked for.
        if options.table.is_some()
            && !effective_format.is_some_and(FileFormat::takes_table)
            && options.splits.is_none()
        {
            return Err(Self::one_table(Some(path), effective_format));
        }

        // One compressed CSV, TSV, PSV or text file, as a directory of one resolves to:
        // the load decompresses it (`Step::Decompress`) into a copy the dataset holds.
        // Read here, the copy went with the state dropped below and the frame scanned
        // nothing.
        if let [file] = paths
            && compressed
            && let Some(format) = effective_format.filter(|f| f.decompressed_once())
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
        let guessed;
        let options = if report.guessed {
            guessed = OpenOptions {
                format_guessed: true,
                ..options.clone()
            };
            &guessed
        } else {
            options
        };
        crate::readers::scan(crate::readers::ScanIn {
            format,
            paths,
            options,
            report,
            formats,
        })
    }

    /// Whether the help overlay is on screen.
    pub fn help_visible(&self) -> bool {
        self.help.is_open()
    }

    /// The screen the help overlay shows the keys of, while it is up.
    pub fn help_context(&self) -> Option<datui_cli::keys::Context> {
        self.help.context()
    }

    /// Open the views list for the dataset on screen, scored against it.
    fn open_view_list(&mut self) {
        if self.view_dataset().is_none() {
            return;
        }
        self.view_modal.table_state.select(Some(0));
        self.refresh_view_list();
        self.view_modal.active = true;
        self.view_modal.mode = ViewModalMode::List;
    }

    /// Rebuild the list's rows from the store, scored and annotated against
    /// the open dataset; the selection stays near where it was.
    fn refresh_view_list(&mut self) {
        let (Some(state), Some(dataset)) = (&self.data_table_state, self.view_dataset()) else {
            return;
        };
        let rows: Vec<ViewRow> = self
            .view_manager
            .find_relevant_views(dataset, state.source_schema())
            .into_iter()
            .map(|(view, score)| {
                let reason = view::match_reason(&view, dataset, state.source_schema());
                ViewRow {
                    view,
                    score,
                    reason,
                }
            })
            .collect();
        self.view_modal.broken_views = self.view_manager.broken_views.clone();
        let selected = self.view_modal.table_state.selected().unwrap_or(0);
        self.view_modal.table_state.select(if rows.is_empty() {
            None
        } else {
            Some(selected.min(rows.len() - 1))
        });
        self.view_modal.rows = rows;
    }

    /// Open the save-view form prefilled from the open dataset: a name the
    /// user will recognize, this file's paths and patterns as criteria, and
    /// schema match on — the criterion that carries the view to the next
    /// table shaped like this one.
    fn open_save_view_form(&mut self) {
        self.view_modal
            .enter_create_mode(self.history_limit, &self.theme);

        let query = self.data_table_state.as_ref().and_then(|state| {
            let (query, sql_query, fuzzy_query) = active_query_settings(
                state.get_active_query(),
                state.get_active_sql_query(),
                state.get_active_fuzzy_query(),
            );
            sql_query.or(fuzzy_query).or(query)
        });
        self.view_modal.name_input.suggest(
            self.view_manager
                .suggest_name(self.path.as_deref(), query.as_deref()),
        );

        // Data piped in has no file to pin; its columns are what match it.
        if let Some(path) = self.path.as_ref().filter(|_| !self.reads_stdin()) {
            // Pin this file: its absolute path or URL, its path relative to the
            // working directory when it is local and under it, and glob suggestions.
            let absolute_path = view::exact_location(path);
            self.view_modal
                .exact_path_input
                .suggest(absolute_path.to_string_lossy());
            if let Some(relative) = view::relative_location(path) {
                self.view_modal.relative_path_input.suggest(relative);
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
                self.view_modal.path_pattern_input.suggest(format!(
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
                self.view_modal.filename_pattern_input.suggest(pattern);
            }
        }

        self.view_modal.table = self.view_table().map(str::to_string);

        // Schema match starts on: "apply this to a similar table" is the
        // reason views exist, and the columns are the only criterion that
        // says similar.
        if let Some(ref state) = self.data_table_state
            && !state.source_schema().is_empty()
        {
            self.view_modal.schema_match_enabled = true;
        }
    }

    /// Validate and persist the form: a new view, or the edited one. The
    /// settings are rebuilt from the table's applied state either way. A
    /// failed save keeps the form open.
    fn save_view_form(&mut self) {
        self.view_modal.name_error = None;
        let name = self.view_modal.name_input.value().trim().to_string();
        if name.is_empty() {
            self.view_modal.name_error = Some("name is required".to_string());
            self.view_modal.form_focus = FormFocus::Name;
            return;
        }
        let renaming_to_taken = match &self.view_modal.editing_view_id {
            None => self.view_manager.view_exists(&name),
            Some(id) => self
                .view_manager
                .get_view_by_name(&name)
                .is_some_and(|other| other.id != *id),
        };
        if renaming_to_taken {
            self.view_modal.name_error = Some("name already exists".to_string());
            self.view_modal.form_focus = FormFocus::Name;
            return;
        }

        let non_empty = |input: &widgets::text_input::TextInput| {
            let value = input.value().trim();
            (!value.is_empty()).then(|| value.to_string())
        };
        let match_criteria = view::MatchCriteria {
            exact_path: non_empty(&self.view_modal.exact_path_input).map(std::path::PathBuf::from),
            relative_path: non_empty(&self.view_modal.relative_path_input),
            path_pattern: non_empty(&self.view_modal.path_pattern_input),
            filename_pattern: non_empty(&self.view_modal.filename_pattern_input),
            // The columns the view's settings run on, not the query's output: the
            // next file is matched as loaded.
            schema_columns: if self.view_modal.schema_match_enabled {
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
            table: self.view_modal.table.clone(),
        };
        let description = {
            let value = self.view_modal.description_input.value();
            (!value.is_empty()).then(|| value.to_string())
        };

        let saved = if let Some(editing_id) = self.view_modal.editing_view_id.clone() {
            let Some(mut view) = self.view_manager.get_view_by_id(&editing_id).cloned() else {
                return;
            };
            view.name = name;
            view.description = description;
            let stored_schema = view.match_criteria.schema_columns.take();
            view.match_criteria = match_criteria;
            let editing_the_active_view =
                self.active_view_id.as_deref() == Some(editing_id.as_str());
            // The same principle as the settings below: editing an unapplied
            // view must not swap the columns it matches on for the columns of
            // whatever table happens to be open. The toggle still works — off
            // drops the criterion — and the active view follows its table.
            if !editing_the_active_view
                && self.view_modal.schema_match_enabled
                && stored_schema.is_some()
            {
                view.match_criteria.schema_columns = stored_schema;
            }
            // The settings follow the table only while this view is the one
            // dressing it. Editing an unapplied view changes its name,
            // description and matching alone — it must not overwrite what
            // the view carries with whatever the table happens to show.
            if editing_the_active_view && let Some(state) = &self.data_table_state {
                view.settings = view_settings_of(state);
                view.settings.chart = self.saved_chart();
            }
            match self.view_manager.update_view(&view) {
                Ok(()) => true,
                Err(e) => {
                    // Deleted elsewhere, it has left the list too; otherwise the form
                    // stays, edits and all, to try again.
                    if self.view_manager.get_view_by_id(&editing_id).is_none() {
                        self.refresh_view_list();
                        self.view_modal.exit_form();
                    }
                    self.error_modal.show(format!("Error saving view: {e}"));
                    return;
                }
            }
        } else {
            self.create_view_from_current_state(name, description, match_criteria)
                .is_ok()
        };
        if saved {
            self.refresh_view_list();
            self.view_modal.exit_form();
        }
    }

    /// The selected view's score breakdown, for the list's `i` popup.
    fn view_score_details(&self) -> Option<(String, String)> {
        let state = self.data_table_state.as_ref()?;
        let path = self.view_dataset()?;
        let idx = self.view_modal.table_state.selected()?;
        let row = self.view_modal.rows.get(idx)?;
        let view = &row.view;

        let exact_path_match = view::exact_path_matches(&view.match_criteria, path);
        let relative_path_match = view::relative_path_matches(&view.match_criteria, path);
        let file_cols: std::collections::HashSet<&str> = state
            .source_schema()
            .iter_names()
            .map(|s| s.as_str())
            .collect();
        let exact_schema_match =
            view.match_criteria
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
            if view::path_pattern_matches(&view.match_criteria, path) {
                details.push_str("Path pattern match: 50.0+\n");
            }
            if view::filename_pattern_matches(&view.match_criteria, path) {
                details.push_str("Filename pattern match: 30.0+\n");
            }
            if let Some(required_cols) = &view.match_criteria.schema_columns {
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
        if view.usage_count > 0 {
            details.push_str(&format!(
                "Usage count: {:.1}\n",
                (view.usage_count.min(10) as f64) * 1.0
            ));
        }
        if let Some(last_used) = view.last_used
            && let Ok(duration) = std::time::SystemTime::now().duration_since(last_used)
        {
            let days_since = duration.as_secs() / 86400;
            if days_since <= 7 {
                details.push_str("Recent usage: 5.0\n");
            } else if days_since <= 30 {
                details.push_str("Recent usage: 2.0\n");
            }
        }
        Some((format!("Score: {}", view.name), details))
    }

    /// Open the help overlay on the keys of the screen it is opened at. No-op if it is
    /// already up.
    pub(crate) fn open_help_overlay(&mut self) {
        // A question or an error under the help would take its keys unseen.
        if self.help.is_open() || self.confirmation_modal.active || self.error_modal.active {
            return;
        }
        let context = self.keys_context();
        // The home filter types too once something is typed into it.
        let typing = self.text_field_focused()
            || (self.input_mode == InputMode::Home
                && !self.documentation.is_open()
                && (!self.home.filter.is_empty() || self.home.path_input_active));
        self.help.open(context, typing);
    }

    /// Close the help when the screen under it changed on its own (a query that
    /// finished, a load that failed): its keys are for a screen that is gone, and
    /// Enter would press one of them on another. A question or an error that arrived
    /// under it takes the keys, so it closes for those too.
    fn close_help_left_behind(&mut self) {
        let left = self
            .help
            .context()
            .is_some_and(|shown| shown != self.keys_context());
        if left || self.confirmation_modal.active || self.error_modal.active {
            self.help.close();
        }
    }

    /// The screen the keys typed now go to, as the key registry names it.
    pub fn keys_context(&self) -> datui_cli::keys::Context {
        use crate::analysis_modal::{AnalysisTool, AnalysisView};
        use datui_cli::keys::Context;
        if self.analysis_modal.active {
            return match self.analysis_modal.view {
                AnalysisView::DistributionDetail => Context::DistributionDetail,
                AnalysisView::CorrelationDetail => Context::CorrelationDetail,
                AnalysisView::Main => match self.analysis_modal.selected_tool {
                    Some(AnalysisTool::DistributionAnalysis) => Context::Distribution,
                    Some(AnalysisTool::CorrelationMatrix) => Context::Correlation,
                    Some(AnalysisTool::DataQuality) => Context::DataQuality,
                    Some(AnalysisTool::Describe) | None => Context::Describe,
                },
            };
        }
        if self.view_modal.active {
            return Context::Views;
        }
        match self.input_mode {
            InputMode::Normal => Context::Table,
            InputMode::Editing => match self.input_type {
                Some(InputType::Find) => Context::Find,
                _ => Context::Query,
            },
            InputMode::SortFilter => Context::SortFilter,
            InputMode::PivotMelt => Context::PivotMelt,
            InputMode::Export => Context::Export,
            InputMode::Copy => Context::Copy,
            InputMode::Inspect => Context::Inspector,
            InputMode::GoToColumn => Context::GoToColumn,
            InputMode::PickFormat => Context::FormatPicker,
            InputMode::Retype => Context::Retype,
            InputMode::Combine => Context::Combine,
            InputMode::PickTable => Context::TablePicker,
            InputMode::Sample => Context::Sample,
            InputMode::Info => Context::Info,
            InputMode::Chart => Context::Chart,
            InputMode::Home if self.documentation.is_open() => Context::Documentation,
            InputMode::Home => Context::Home,
            InputMode::Hex => Context::Hex,
            InputMode::ValueCounts => Context::ValueCounts,
        }
    }

    /// True while the confirmation modal is asking whether to download a remote file,
    /// or to read a large one whole into memory.
    ///
    /// Those are the confirmations the user has to be able to walk away from: the
    /// size probe behind a download can take fifteen seconds, and the answer to
    /// "actually, never mind" is the home screen, not the exit.
    pub fn awaiting_open_confirmation(&self) -> bool {
        self.confirmation_modal.active && self.loading.asking()
    }

    fn key(&mut self, event: &KeyEvent) -> Option<AppEvent> {
        self.debug.on_key(event);

        // A completion flash lives until the next key: whatever this key does,
        // the bar's line about the last action is stale now.
        self.flash = None;
        // A key puts back a header being carried, so a release later moves nothing.
        self.cancel_drag();

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

        // The context menu takes its keys first. A line chosen closes it and presses
        // its key, offered as typed (as Enter on a help line is); any other key closes
        // it and then acts as it would have. A menu something else has covered since
        // (an error, a load's screen) is gone.
        if !self.menu_showing() {
            self.context_menu = None;
        }
        if let Some(menu) = self.context_menu.as_mut() {
            match menu.key(event) {
                context_menu::MenuKey::Moved => return None,
                context_menu::MenuKey::Close => {
                    self.context_menu = None;
                    return None;
                }
                context_menu::MenuKey::Run(key) => {
                    self.context_menu = None;
                    return Some(AppEvent::Press(key));
                }
                context_menu::MenuKey::Do(action) => {
                    self.context_menu = None;
                    self.menu_action(action);
                    return None;
                }
                context_menu::MenuKey::Other => self.context_menu = None,
            }
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
        // And for a count of footers, at the table its progress line is on.
        if event.code == KeyCode::Esc
            && self.input_mode == InputMode::Normal
            && self.in_normal_table_view()
            && self.footers_counted().is_some()
        {
            self.stop_count();
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

        // F1 opens help first so no other branch (e.g. Editing) can consume it; again,
        // it closes it.
        if event.code == KeyCode::F(1) {
            if self.help.is_open() {
                self.help.close();
            } else {
                self.open_help_overlay();
            }
            return None;
        }

        // Home owns the whole screen and every key while it is up — except under a
        // modal or the help overlay. Both render over home unconditionally, so if
        // home also ate their keys they would be undismissable, and Esc would try
        // to leave home instead.
        if self.input_mode == InputMode::Home
            && !self.confirmation_modal.active
            && !self.error_modal.active
            && !self.help.is_open()
        {
            return self.home_key(event);
        }

        // Ctrl+O goes home from anywhere, including mid-load. That is what makes
        // browsing cheap: opening the wrong 300 MB file costs one keystroke to leave,
        // not a wait for it to finish.
        if event.code == KeyCode::Char('o')
            && event.modifiers.contains(KeyModifiers::CONTROL)
            && (!self.confirmation_modal.active || self.awaiting_open_confirmation())
        {
            self.help.close();
            self.enter_home();
            return None;
        }

        // The confirmation modal (for an overwrite).
        if self.confirmation_modal.active {
            match event.code {
                KeyCode::Left | KeyCode::Char('h') => {
                    self.confirmation_modal.focus_yes = true;
                }
                KeyCode::Right | KeyCode::Char('l') => {
                    self.confirmation_modal.focus_yes = false;
                }
                KeyCode::Tab => {
                    self.confirmation_modal.focus_yes = !self.confirmation_modal.focus_yes;
                }
                // ←→ carry the choice, so ↑↓ (k/j) scroll a long question; the
                // render clamps the offset.
                KeyCode::Up | KeyCode::Char('k') => {
                    self.confirmation_modal.scroll =
                        self.confirmation_modal.scroll.saturating_sub(1);
                }
                KeyCode::Down | KeyCode::Char('j') => {
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
                        if let Some(url) = self.pending_link.take() {
                            self.confirmation_modal.hide();
                            return Some(AppEvent::OpenLink(url));
                        }
                        if self.pending_clear_recents {
                            self.pending_clear_recents = false;
                            self.confirmation_modal.hide();
                            self.cache.clear_recents();
                            self.home_refresh();
                            self.home.status = None;
                            return None;
                        }
                        // A full scan agreed to: Setup runs, past the question.
                        if self.analysis_modal.data_quality_confirm_run {
                            self.confirmation_modal.hide();
                            let event = self.run_quality_setup();
                            // Asked once: a run that waits or is refused asks again.
                            self.analysis_modal.data_quality_confirm_run = false;
                            return event;
                        }
                        if std::mem::take(&mut self.pending_hide_examples) {
                            self.confirmation_modal.hide();
                            self.cache.hide_examples();
                            self.home_refresh();
                            self.home.select_first_entry();
                            return None;
                        }
                        if let Some(id) = self.pending_delete_view.take() {
                            self.confirmation_modal.hide();
                            if self.view_manager.delete_view(&id).is_ok() {
                                self.refresh_view_list();
                            }
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
                        if self.loading.asking() {
                            self.confirmation_modal.hide();
                            // The loader lets go of its hold on the generation as the
                            // download or the read starts, and its job takes it before
                            // anything else can look.
                            let step = self.loading.confirmed();
                            return self.run_load_step(step);
                        }
                    } else {
                        self.pending_clear_recents = false;
                        self.pending_link = None;
                        self.pending_read_all = false;
                        self.pending_forget_place = None;
                        self.pending_delete_view = None;
                        self.pending_hide_examples = false;
                        // Declining the full read leaves the draft staged, and the
                        // sample and report as they were.
                        self.analysis_modal.data_quality_confirm_run = false;
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
                    self.pending_link = None;
                    self.pending_read_all = false;
                    self.pending_forget_place = None;
                    self.pending_delete_view = None;
                    self.pending_hide_examples = false;
                    self.analysis_modal.data_quality_confirm_run = false;
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
        // kind correctly. Exclude view/analysis modals so they can handle Left/Right
        // themselves.
        let in_main_table = !(self.input_mode != InputMode::Normal
            || self.help.is_open()
            || self.view_modal.active
            || self.analysis_modal.active);
        // The footer offers the column's keys once the column cursor moves, until a
        // key that is not about the column.
        if in_main_table && event.is_press() {
            self.column_hints = Self::column_cursor_key(event).is_some()
                || (self.column_hints
                    && matches!(
                        event.code,
                        KeyCode::Char(
                            '+' | '-' | 'F' | '[' | ']' | 'H' | 'L' | '<' | '>' | '=' | 'w'
                        )
                    ));
        }
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

        // The help owns the keys while it is up. Enter on a line closes it and presses
        // that line's key: handed back as this key's follow-up, it reaches the screen
        // under the help the way a typed key does, held while the app is busy.
        self.close_help_left_behind();
        if self.help.is_open() {
            return match self.help.key(event) {
                help::HelpKey::Press(key) => Some(AppEvent::Press(key)),
                help::HelpKey::Stay | help::HelpKey::Closed => None,
            };
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

        if self.input_mode == InputMode::Sample {
            return self.table_sample_form_key(event);
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

        if self.input_mode == InputMode::Retype {
            return self.retype_key(event);
        }

        if self.input_mode == InputMode::Combine {
            return self.combine_key(event);
        }

        if self.input_mode == InputMode::PickTable {
            return self.table_picker_key(event);
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

        if self.view_modal.active {
            return self.view_key(event);
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
            KeyCode::Char('H' | 'L') if event.is_press() => {
                self.move_cursor_column(event.code == KeyCode::Char('L'))
            }
            KeyCode::Char('+' | '-') if event.is_press() => {
                self.quick_filter(event.code == KeyCode::Char('+'))
            }
            KeyCode::Char('#') => {
                let renumbered = self
                    .data_table_state
                    .as_mut()
                    .is_some_and(|state| state.deferred(|s| s.toggle_row_numbers()));
                if renumbered {
                    self.spawn_async_collect(Self::LOADING_BUFFER);
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
                    // From the width on screen: `>` on a column filling the right edge
                    // widens what is seen.
                    let (choice, shown) = (state.width_choice(&name), state.on_screen_width(&name));
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
            // `/` finds, as in less and vim; `f` too. Ctrl+F pages down, below.
            KeyCode::Char('/' | 'f')
                if event.is_press() && !event.modifiers.contains(KeyModifiers::CONTROL) =>
            {
                self.open_find();
                None
            }
            KeyCode::Char('[' | ']') if event.is_press() => {
                self.sort_by_cursor_column(event.code == KeyCode::Char(']'))
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
                // A sample being drawn stops, keeping the rows so far.
                if self.sample_drawing() {
                    self.stop_sample_draw();
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
                // The Info panel handles Esc in its own block.
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
                    // A list of the file's tables starts its cursor on the one open.
                    if let Some(detail) = state.format_detail()
                        && let Some(at) = detail.list.iter().position(|(key, _)| {
                            detail.table.as_ref() == Some(key) && detail.tables.contains(key)
                        })
                    {
                        self.info_modal.detail_selected = at;
                    }
                    self.input_mode = InputMode::Info;
                    self.read_file_facts();
                    self.count_unfit();
                }
                None
            }
            KeyCode::Char(':') if event.is_press() => {
                self.open_command_line();
                None
            }
            KeyCode::Char('V') => {
                // Apply the best view whose criteria match this dataset. When none
                // does, the answer is not silence and not the best-scored stranger: the
                // list opens, so the user sees what exists and picks — or saves one.
                if let Some(ref state) = self.data_table_state
                    && let Some(dataset) = self.view_dataset()
                {
                    match self
                        .view_manager
                        .get_most_relevant(dataset, state.source_schema())
                    {
                        Some((view, why)) => {
                            if let Err(e) = self.apply_matched_view(&view, why) {
                                self.error_modal.show(format!("Error applying view: {}", e));
                            }
                        }
                        None => self.open_view_list(),
                    }
                }
                None
            }
            KeyCode::Char('v') => {
                self.open_view_list();
                None
            }
            KeyCode::Char('S') => {
                if self.input_mode == InputMode::Normal {
                    self.open_table_sample_form();
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
                    // The results a close put down come back on the view they are of.
                    let view = self.data_table_state.as_ref().map(|s| s.len_generation());
                    self.analysis_modal.open(view);
                    // The sample outlives a close, but its scope names this
                    // dataset's rows: another dataset starts from its current view.
                    if self.analysis_modal.sample_dataset != Some(self.dataset_generation) {
                        self.analysis_modal.sample.scope = data_quality::QualityScope::CurrentView;
                        self.analysis_modal.sample_dataset = Some(self.dataset_generation);
                    }
                    // A view with a sample: every tool reads it, whole.
                    let sampled = self
                        .data_table_state
                        .as_ref()
                        .is_some_and(|state| state.sampled().is_some());
                    self.analysis_modal.follow_view_sample(sampled);
                    self.sync_quality_plan();
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
                    // Show Me: the chart starts from the cursor column's type.
                    let cursor = state.current_column().and_then(|name| {
                        let dtype = state.schema().get(name)?.clone();
                        Some((name.to_string(), dtype))
                    });
                    // Dates and datetimes take a time bucket; a time of day does not.
                    let bucketable_columns: Vec<String> = state
                        .schema()
                        .iter()
                        .filter(|(_, dtype)| {
                            matches!(dtype, DataType::Datetime(_, _) | DataType::Date)
                        })
                        .map(|(name, _)| name.to_string())
                        .collect();
                    self.chart_modal.series_cap = Some(self.theme.series_colors().len());
                    self.chart_modal.row_order = self.view_state().sort;
                    let sampled = state.sampled().is_some();
                    self.chart_modal.open(
                        ChartColumns {
                            numeric: &numeric_columns,
                            datetime: &datetime_columns,
                            bucketable: &bucketable_columns,
                            category: &category_columns,
                        },
                        cursor.as_ref().map(|(name, dtype)| (name.as_str(), dtype)),
                        Some(self.app_config.analysis.chart_rows),
                        self.app_config.analysis.chart_grid,
                        self.dataset_generation,
                    );
                    // A view's sample is read whole: the chart has no sample of its own.
                    if sampled {
                        self.chart_modal.row_limit = None;
                    } else if self.chart_modal.view_sampled {
                        self.chart_modal.row_limit = Some(self.chart_modal.sample_rows);
                    }
                    self.chart_modal.view_sampled = sampled;
                    self.chart_cache.clear();
                    self.input_mode = InputMode::Chart;
                }
                None
            }
            KeyCode::Char('p') => {
                if self.data_table_state.is_some() && self.input_mode == InputMode::Normal {
                    self.open_pivot_builder();
                }
                None
            }
            KeyCode::Char('e') => {
                if self.data_table_state.is_some() && self.input_mode == InputMode::Normal {
                    self.export_counts = None;
                    self.export_modal.open(
                        self.original_file_format,
                        self.history_limit,
                        &self.theme,
                        self.original_file_delimiter,
                    );
                    // A name to start from, beside the source's rather than on it.
                    let stem = self.dataset_stem();
                    self.export_modal.suggest_path(&format!("{stem}-export"));
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
            KeyCode::Char('T') if event.is_press() => {
                if self.input_mode == InputMode::Normal {
                    self.open_table_picker();
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
        let started = std::time::Instant::now();
        let outcome = self.handle_event(event);
        self.debug.times.handler(started.elapsed());
        outcome
    }

    fn handle_event(&mut self, event: &AppEvent) -> EventOutcome {
        // Without the pump to offer it as typed, a pressed key is a key.
        if let AppEvent::Press(key) = event {
            return self.handle_event(&AppEvent::Key(*key));
        }
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
        // New rows on hand under an open find prompt: light up their matches.
        self.refresh_stale_live_matches();
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
        self.join_followed_fields();
        self.describe_ended_journal();
        // And the same turn for a re-read owed to a dataset whose footers could not be
        // read: it waits on the same work, and gets in the same way.
        self.reread_when_the_work_allows();
        self.collect_when_the_work_allows();
    }

    pub fn event(&mut self, event: &AppEvent) -> Option<AppEvent> {
        self.handle(event).unwrap_or(None)
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
                let mut request =
                    loading::OpenRequest::named(paths.clone(), options.clone(), &self.formats);
                request.warn_in_memory_above = self.app_config.read.memory_warning();
                self.begin_new_dataset();
                let step = self.loading.open(request);
                self.run_load_step(step)
            }
            AppEvent::OpenLazyFrame(lf, options) => {
                self.begin_new_dataset();
                let step = self.loading.open_frame((**lf).clone(), options.clone());
                self.run_load_step(step)
            }
            AppEvent::HomeListingReady { .. }
            | AppEvent::HomeListingFailed
            | AppEvent::HomeMeasured { .. }
            | AppEvent::HomeSized { .. }
            | AppEvent::HomeWebGone { .. }
            | AppEvent::HomeClassified { .. }
            | AppEvent::HomePathListed { .. }
            | AppEvent::HomePathCompleted { .. }
            | AppEvent::HomePreviewReady { .. }
            | AppEvent::HomeSchemaReady { .. }
            | AppEvent::HomeSearchBatch { .. }
            | AppEvent::HomeSearchScored { .. }
            | AppEvent::HomeSearchDone { .. }
            | AppEvent::HomeNarrowed { .. }
            | AppEvent::HomeProbeCancelled { .. }
            | AppEvent::HomeProbeFailed { .. }
            | AppEvent::HomeProbeProgress { .. }
            | AppEvent::HomeProbeReady { .. }
            | AppEvent::HomeCloudKinds { .. } => self.home_event(event),
            #[cfg(feature = "cloud")]
            AppEvent::HomeCloudSources { .. } | AppEvent::HomeCloudListed { .. } => {
                self.home_event(event)
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
                // Past the lines indexed so far: gone to once they all are.
                if let Some(state) = self.data_table_state.as_ref()
                    && state.indexing().is_some()
                    && (n >= state.num_rows()
                        || state.changes_rows()
                        || !state.view_sort_columns().is_empty()
                        || !state.view_sort_ascending())
                {
                    self.goto_when_indexed = Some((self.dataset_generation, n));
                    self.status_message = Some(Self::INDEXING_FOR_ROW.to_string());
                    self.busy = false;
                    return None;
                }
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
                    let streaming = self.app_config.performance.streaming;
                    self.spawn_job(
                        Job::Analysis(jobs::AnalysisRun::default()),
                        Some("Running analysis..."),
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
                    let streaming = self.app_config.performance.streaming;
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
            AppEvent::AnalysisDataQualityCompute => self.run_quality_compute(),
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
                if self.count_after_stop.take() == Some(*len_generation) {
                    self.count_exactly();
                    return None;
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
            AppEvent::LinesIndexed { generation, rows } => {
                self.lines_indexed(*generation, *rows);
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
                // The same words the loading screen shows, so the footer and the
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
                // The home screen's own line, because the footer's is the table's.
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
            AppEvent::QQuery(query) => {
                self.run_query(QueryMode::Q, query, "Applying query...");
                None
            }
            AppEvent::SqlQuery(sql) => {
                self.run_query(QueryMode::Sql, sql, "Applying SQL query...");
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
                // The sample is a step of the view: a reset takes it away too.
                if self
                    .data_table_state
                    .as_ref()
                    .is_some_and(|state| state.sampled().is_some())
                {
                    self.put_down_sample_draw();
                    if let Some(state) = self.data_table_state.take() {
                        self.data_table_state = Some(state.into_unsampled());
                    }
                    self.sample_changed();
                }
                if let Some(state) = &mut self.data_table_state {
                    state.deferred(|s| s.reset());
                }
                self.spawn_async_collect(Self::LOADING_BUFFER);
                self.active_view_id = None;
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
                            .map_err(|error| Self::format_export_error(&error))?;
                        Ok(Answer::QualityReportWritten(path))
                    },
                );
                None
            }
            AppEvent::ChartExport(..)
            | AppEvent::DoChartExport(..)
            | AppEvent::BackgroundChartReady => self.chart_event(event),
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
            AppEvent::FollowedDetail {
                dataset_generation,
                detail,
            } => {
                if *dataset_generation == self.dataset_generation
                    && let Some(state) = self.data_table_state.as_mut()
                {
                    state.set_format_detail((**detail).clone());
                }
                None
            }
            AppEvent::DoExport(request) => {
                let Some(state) = &self.data_table_state else {
                    self.export_progress = None;
                    self.busy = false;
                    return None;
                };
                // Cloned, not taken: a failed write reopens the dialog on the same counts.
                let frame = match self.export_counts.clone() {
                    Some(counts) => {
                        crate::table::ExportFrame::of(polars::prelude::IntoLazy::lazy(counts))
                    }
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
                        .map_err(|e| Self::format_export_error(&e))?;
                    // Success is reported only once the file is committed.
                    Ok(Answer::Exported(request.path))
                });
                None
            }
            AppEvent::OpenLink(url) => {
                // Started, not waited on; a browser that will not start is a line,
                // not an error to acknowledge.
                if link_open::open(url).is_err() {
                    self.flash_note("Couldn't open the link; y copies it".to_string());
                }
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
            AppEvent::TerminalBackground(mode) => {
                self.terminal_answered(*mode);
                None
            }
            AppEvent::TerminalFocused => {
                self.background_query |= self.app_config.theme.follow;
                None
            }
            _ => None,
        }
    }

    /// Why one of the sidebar's filters cannot apply: its value does not read as its
    /// column's type. The first such, said for the user.
    fn filter_problem(&self) -> Option<String> {
        let schema = self.data_table_state.as_ref()?.schema();
        self.sort_filter_modal
            .filter
            .statements
            .iter()
            .find_map(|f| crate::python_script::SidebarFilter::problem(f, schema.get(&f.column)))
    }

    /// Whether the dataset on screen is delimited text, whose first row `H` on the
    /// Info panel's Schema tab reads the other way.
    pub fn header_toggle_offered(&self) -> bool {
        self.opened
            .as_ref()
            .and_then(|(_, options)| options.format)
            .and_then(FileFormat::separator)
            .is_some()
    }

    /// Read the dataset again with its first row the other way: as column names, or
    /// as data under `column_1`, `column_2`, …. Only delimited text has a header to
    /// turn off; anything else carries its own names, and this does nothing there.
    pub(crate) fn toggle_header(&mut self) -> Option<AppEvent> {
        if !self.header_toggle_offered() {
            return None;
        }
        let (paths, options) = self.opened.clone()?;
        let options = OpenOptions {
            has_header: Some(!options.has_header.unwrap_or(true)),
            ..options
        };
        self.set_loading_phase("Scanning input", 10);
        self.name_what_is_loading(paths[0].clone());
        Some(AppEvent::Open(paths, options))
    }

    /// `H` / `L`: the column cursor's column one place left or right in the column
    /// order the sidebar's `+` / `-` set, the cursor with it. A frozen column moves
    /// among the frozen ones and a scrolling one among the scrolling ones; at an end,
    /// nothing moves.
    fn move_cursor_column(&mut self, right: bool) -> Option<AppEvent> {
        let state = self.data_table_state.as_ref()?;
        let at = state.current_column_index()?;
        let mut order = state.headers();
        let locked = state.locked_columns_count().min(order.len());
        let to = if right { at + 1 } else { at.checked_sub(1)? };
        if to >= order.len() || (at < locked) != (to < locked) {
            return None;
        }
        // Held by name, so it lands on the column where the move puts it.
        let moving = order[at].clone();
        self.data_table_state.as_mut()?.set_current_column(&moving);
        order.swap(at, to);
        // The sidebar places hidden columns by the order it last applied; the two
        // trade places there too, so that order still agrees with the table.
        let applied = &mut self.sort_filter_modal.sort.applied_order;
        if let (Some(i), Some(j)) = (
            applied.iter().position(|c| *c == order[at]),
            applied.iter().position(|c| *c == order[to]),
        ) {
            applied.swap(i, j);
        }
        Some(AppEvent::ColumnOrder(order, locked))
    }

    /// `+` / `-`: a filter on the cursor's cell, added to the sidebar's Filters list
    /// and applied, so it shows there, joins the others with "and", and `R` clears
    /// it. `+` keeps the rows with the cell's value and `-` drops them; a null cell
    /// is "is null" or "not null". The value is the cell's exactly as stored.
    /// `[` / `]` at the table: sort by the cursor's column, ascending or descending,
    /// in place of the sort in effect. The same key again on a view sorted that way
    /// by that column alone takes the sort away.
    fn sort_by_cursor_column(&mut self, descending: bool) -> Option<AppEvent> {
        let state = self.data_table_state.as_ref()?;
        let column = state.current_column()?.to_string();
        let already = state.view_sort_columns() == std::slice::from_ref(&column)
            && state.view_sort_descending() == [descending];
        if already {
            // Back to the natural order: `sort` with no columns resets the direction
            // `]` left behind, which would otherwise read as a reversal.
            if let Some(state) = self.data_table_state.as_mut() {
                state.deferred(|s| s.sort(Vec::new(), true));
            }
            self.spawn_async_collect("Sorting...");
            return None;
        }
        Some(AppEvent::Sort(vec![column], vec![descending]))
    }

    fn quick_filter(&mut self, keep: bool) -> Option<AppEvent> {
        let state = self.data_table_state.as_ref()?;
        let column = state.current_column()?.to_string();
        let row = state.copy_row_df()?;
        let series = row.column(&column).ok()?.as_materialized_series().clone();
        let value = series.get(0).ok()?;
        // The schema's type, not the buffer's: binary is buffered as a stub.
        let dtype = state.schema().get(&column)?.clone();
        let (operator, text) = if value.is_null() {
            let operator = if keep {
                FilterOperator::IsNull
            } else {
                FilterOperator::IsNotNull
            };
            (operator, String::new())
        } else {
            let operator = if keep {
                FilterOperator::Eq
            } else {
                FilterOperator::NotEq
            };
            // Text that reads back to this very value: a float exactly as stored,
            // a date and time to its last digit, in its zone.
            let text = crate::typed_value::text_of(&value, &dtype);
            let Some(text) = text else {
                let kind = match dtype {
                    DataType::List(_) => "lists",
                    DataType::Array(..) => "arrays",
                    DataType::Struct(_) => "structs",
                    DataType::Binary | DataType::BinaryOffset => "binary",
                    _ => "this type",
                };
                self.flash_note(format!("+ and - filter on plain values, not {kind}"));
                return None;
            };
            (operator, text)
        };
        let statement = FilterStatement {
            columns: Vec::new(),
            column,
            operator,
            value: text,
            logical_op: LogicalOperator::And,
        };
        let mut statements = state.view_filters().to_vec();
        if statements.contains(&statement) {
            return None;
        }
        statements.push(statement);
        Some(AppEvent::Filter(statements))
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
        let schema = state.schema().clone();
        let order = state.headers();
        let locked = state.locked_columns_count();

        let modal = &mut self.sort_filter_modal;
        modal.filter.applied = filters.clone();
        modal.filter.statements = filters;
        modal.filter.operands = order
            .iter()
            .map(|name| {
                schema
                    .get(name)
                    .map(crate::filter_modal::Operand::of)
                    .unwrap_or_default()
            })
            .collect();
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
                    shown_width: state.on_screen_width(name),
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
        // A value its column cannot compare with stays in the sidebar, which says why.
        if let Some(why) = self.filter_problem() {
            self.sort_filter_modal.sort.status = Some(why);
            return None;
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
        let facts_tab = self.info_facts_tab();
        self.data_table_state
            .as_ref()
            .map(|state| crate::widgets::info::TabsOffered {
                documentation: self.info_documentation.is_open(),
                ..crate::widgets::info::TabsOffered::of(state, facts_tab)
            })
            .unwrap_or_default()
    }

    /// The format of the dataset on screen, as the open read it.
    pub(crate) fn opened_format(&self) -> Option<FileFormat> {
        self.opened
            .as_ref()
            .and_then(|(_, options)| options.format)
            .or_else(|| self.path.as_deref().and_then(FileFormat::from_path))
    }

    /// The format's tab of the Info panel that the file facts fill: for one local file,
    /// not a hive directory, whose reader has a facts read. See
    /// [`crate::widgets::info::InfoContext::facts_tab`].
    pub(crate) fn info_facts_tab(&self) -> Option<&'static str> {
        self.info_facts()
            .and_then(|(format, _)| format.summary_tab())
    }

    /// The format whose facts read the Info panel's worker makes for the dataset on
    /// screen, and that read, once the panel has asked for the file's facts.
    pub(crate) fn info_facts(&self) -> Option<(FileFormat, crate::readers::Facts)> {
        match self.file_facts()? {
            // A directory, which has no footer of its own.
            FileFacts::Read {
                size: None,
                detail: None,
                ..
            } => None,
            _ => self.facts_of_open(),
        }
    }

    /// The facts read for the dataset on screen, if its file has one: one file, stored
    /// as its format says (a stream or a compressed copy has no footer).
    fn facts_of_open(&self) -> Option<(FileFormat, crate::readers::Facts)> {
        let hive = self
            .opened
            .as_ref()
            .is_some_and(|(_, options)| options.hive);
        let format = self.opened_format()?;
        let facts = crate::readers::of(format).facts?;
        let state = self.data_table_state.as_ref()?;
        let plain = state
            .read_mode()
            .is_none_or(|mode| Some(mode) == format.read_mode(crate::Stored::Plain));
        // Several files, whose footers the Notes and Schema tabs already sum up.
        let one_file = state.dataset_schema().is_none();
        (!hive && plain && one_file).then_some((format, facts))
    }

    /// Start applying `view`. Its steps are planned here, which reads nothing; a
    /// step that cannot be planned fails here and changes nothing. The reads — a pivot,
    /// then the view's first rows — run in the background, and the view is installed
    /// when they are in. One that fails there puts the view before it back (#400).
    fn apply_view(&mut self, view: &SavedView) -> Result<()> {
        self.apply_view_with(view, None)
    }

    /// [`Self::apply_view`], for a view applied because its criteria fit as `why`
    /// says: once its rows are in, a flash names it and the reason.
    fn apply_matched_view(&mut self, view: &SavedView, why: view::MatchReason) -> Result<()> {
        self.apply_view_with(view, Some(why))
    }

    fn apply_view_with(&mut self, view: &SavedView, why: Option<view::MatchReason>) -> Result<()> {
        self.jobs.supersede(|job| matches!(job, Job::ViewPivot(_)));
        if let Some(saved) = &view.settings.sample {
            return self.apply_sampled_view(view, saved, why);
        }
        let Some(state) = self.data_table_state.as_mut() else {
            return Ok(());
        };
        match state.try_transition(|s| Self::replay_view(s, &view.settings, None))? {
            (Replayed::Planned, rollback) => {
                self.view_planned(view, rollback, why);
                Ok(())
            }
            (Replayed::Pivot(job), rollback) => {
                // The table stays as it is while the pivot is read.
                state.roll_back(rollback);
                // Past any load-ahead for the view on screen, whose rows must not land
                // in the one that replaces it.
                self.jobs.try_advance();
                let pivot_view = Job::ViewPivot(Box::new((view.clone(), why)));
                self.spawn_job(pivot_view, Some(Self::APPLYING_VIEW), move |_| {
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
        view: &SavedView,
        rollback: crate::table::ViewRollback,
        why: Option<view::MatchReason>,
    ) {
        if let Some(path) = &self.path {
            use crate::logging::LogFailure;
            self.view_manager
                .record_use(&view.id, path)
                .or_log("record a view's use");
        }
        let previous = self.active_view_id.replace(view.id.clone());
        self.restore_view_chart(view.settings.chart.as_ref());
        let Some(state) = self.data_table_state.as_ref() else {
            return;
        };
        self.query_running = Some(QueryRun {
            origin: RunOrigin::View {
                previous,
                matched: why.map(|why| (view.name.clone(), why)),
            },
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
            if let Some(why) = why {
                self.flash_view_applied(&view.name, why);
            }
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

    /// What the footer says while a query's first rows are read over the frame on
    /// screen, from the read's job record. The rows drawn meanwhile are the view it
    /// replaces, under columns it may have changed.
    pub(crate) fn query_reading(&self) -> Option<&str> {
        let run = self.query_running.as_ref()?;
        let frame = self.data_table_state.as_ref()?.len_generation();
        if !matches!(run.origin, RunOrigin::Query(_)) || run.frame != frame {
            return None;
        }
        self.jobs
            .waiting_status(|job| Self::reading_rows(job) || Self::owed_rows(job))
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
    /// on gives the keys back, and its line on the footer goes with it, unless the
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
            Progress::SampleBegun(schema) => self.sample_begun(schema),
            Progress::SampleGrew => self.sample_grew(),
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
                    match ran.map(|run| run.origin) {
                        Some(RunOrigin::Query(mode)) if self.query_prompt_mode() == Some(mode) => {
                            self.leave_query_prompt_after_run();
                        }
                        // Shown once the wait is over, or the spinner's message hides it.
                        Some(RunOrigin::View {
                            matched: Some((name, why)),
                            ..
                        }) => self.flash_view_applied(&name, why),
                        _ => {}
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
                    self.analysis_modal.install_correlations(results);
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
            Answer::SampleDrawn(drawn) => self.sample_drawn(job, current, drawn),
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
            Answer::ReshapePreviewed { input, result } => {
                if let Job::ReshapePreview { epoch, token } = job {
                    self.reshape_preview_ended(epoch, token, input, result);
                }
                None
            }
            Answer::ViewPivoted(pivoted) => {
                // Superseded means the view was cancelled or something replaced it, which
                // owns the wait.
                let Job::ViewPivot(pivot) = job else {
                    return None;
                };
                let (view, why) = *pivot;
                if !current {
                    return None;
                }
                let planned = self.data_table_state.as_mut().map(|state| {
                    // Nothing changed while the pivot was read, so the steps before
                    // it plan as they did; this time the pivot is in hand.
                    state
                        .try_transition(|s| Self::replay_view(s, &view.settings, Some(pivoted)))
                        .map(|(_, rollback)| rollback)
                        .map_err(|e| e.to_string())
                });
                match planned {
                    // The wait passes to the read of its rows.
                    Some(Ok(rollback)) => self.view_planned(&view, rollback, why),
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
                // Written: the dialog held for a failure is done with.
                self.export_modal.close();
                self.export_counts = None;
                if current {
                    self.export_progress = None;
                    self.flash_path("Exported to ", &path);
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
                self.analysis_modal.data_quality_export = None;
                if current {
                    self.flash_path("Report written to ", &path);
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
            Answer::UnfitCounted(unfit) => {
                // Every value fitting says nothing in the Notes; the log says it ran.
                let columns: Vec<&str> = unfit.iter().map(|u| u.column.as_str()).collect();
                let said = if columns.is_empty() {
                    "none".to_string()
                } else {
                    columns.join(", ")
                };
                log::debug!(target: "datui", "values column types made null, by column: {said}");
                if let Job::UnfitCount { dataset, version } = job
                    && dataset == self.dataset_generation
                    && let Some(state) = self.data_table_state.as_mut()
                {
                    match version {
                        None => state.unfit_counted(&unfit),
                        Some(version) => state.changes_unfit_counted(version, &unfit),
                    }
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
            Job::SampleDraw(_) => self.sample_draw_failed(job, current, message),
            // The form stays up with its spec, to be fixed.
            Job::Pivot | Job::Copy => {
                if current {
                    self.error_modal.show(message.to_string());
                }
            }
            // The dialog is still up, the reason on its status line under the path.
            Job::QualityReport => {
                if current {
                    match self.analysis_modal.data_quality_export.as_mut() {
                        Some(form) => form.error = Some(message.to_string()),
                        None => self.error_modal.show(message.to_string()),
                    }
                }
            }
            Job::ViewPivot(_) => {
                if current {
                    self.view_pivot_failed(message);
                }
            }
            // The preview says why in its own pane; the log has a panic's details.
            Job::ReshapePreview { epoch, token } => {
                let message = if panicked {
                    "Could not preview; see the log".to_string()
                } else {
                    message.to_string()
                };
                self.reshape_preview_ended(*epoch, *token, None, Err(message));
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
            // The form comes back as it was, the reason on its status line, to fix
            // the path and press Enter again.
            Job::Export => {
                if current {
                    self.export_progress = None;
                    self.export_modal.resume();
                    self.export_modal.path_error = Some(message.to_string());
                    self.input_mode = InputMode::Export;
                } else {
                    self.export_modal.close();
                    self.export_counts = None;
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
            // The note is left unsaid; the log has why.
            Job::UnfitCount { .. } => {
                log::warn!(target: "datui", "counting values that did not fit their type failed: {message}");
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
        let facts = self.facts_of_open().map(|(_, facts)| facts);
        #[cfg(test)]
        let read: FileFactsReader = self
            .file_facts_reader
            .clone()
            .unwrap_or_else(|| Arc::new(FileFacts::read));
        #[cfg(not(test))]
        let read = FileFacts::read;
        self.spawn_job(Job::FileFacts { dataset }, None, move |_| {
            Ok(Answer::FileFacts(read(&path, facts)?))
        });
    }

    /// Count, behind the Info panel, the values the read's column types made null, for
    /// the Notes: one pass over the frame before the types, the first time the panel
    /// opens on a dataset with typed columns.
    fn count_unfit(&mut self) {
        let dataset = self.dataset_generation;
        let Some(state) = self.data_table_state.as_ref() else {
            return;
        };
        let read = state
            .unfit_to_count()
            .map(|(source, typed)| (source, typed, None));
        let view = state
            .changes_unfit_to_count()
            .map(|(source, typed, version)| (source, typed, Some(version)));
        let streaming = self.app_config.performance.streaming;
        for (source, typed, version) in [read, view].into_iter().flatten() {
            let running = self
                .jobs
                .current(|job| {
                    matches!(job, Job::UnfitCount { dataset: d, version: v }
                        if *d == dataset && *v == version)
                })
                .is_some();
            if running {
                continue;
            }
            self.spawn_job(Job::UnfitCount { dataset, version }, None, move |_| {
                let counted = crate::statistics::collect_lazy(
                    crate::column_types::unfit_frame(source, &typed),
                    streaming,
                )
                .map_err(|e| crate::error_display::user_message_from_polars(&e))?;
                Ok(Answer::UnfitCounted(crate::column_types::unfit_counts(
                    &counted, &typed,
                )))
            });
        }
    }

    /// Whether the values the read's column types made null are being counted.
    pub fn unfit_count_pending(&self) -> bool {
        self.jobs
            .current(|job| matches!(job, Job::UnfitCount { .. }))
            .is_some()
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
        if let RunOrigin::View { previous, .. } = &run.origin {
            self.active_view_id = previous.clone();
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
        settings: &view::ViewSettings,
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
        // Before the filters, which may compare in the types it gives.
        if !settings.columns.is_empty() {
            state.set_column_changes(&settings.columns);
        }
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

    /// A view's query: SQL or q (at most one is stored), then a Text query.
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
    /// Why an export did not write, for the dialog's status line, which sits under
    /// the path it is about.
    fn format_export_error(error: &color_eyre::eyre::Report) -> String {
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
                return format!("Cannot write: {msg}");
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
                            crate::numfmt::bytes(bytes as u64)
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
                        crate::numfmt::bytes(bytes as u64)
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
            read_mode: state.read_mode(),
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
            let limit = usize::try_from(self.app_config.clipboard.osc52_limit.bytes())
                .unwrap_or(usize::MAX);
            self.clipboard = Some(clipboard::destination(choice, limit)?);
        }
        Ok(self
            .clipboard
            .as_deref_mut()
            .expect("destination just built"))
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
                sample_rows: self.app_config.analysis.sample_rows,
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
            file_starts: state.file_row_starts().map(Arc::new),
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
        // The histogram has no lines to move through or drill into.
        let listing = !self.value_counts.shows_histogram();
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
            KeyCode::Down | KeyCode::Char('j') if listing => self.value_counts.move_by(1),
            KeyCode::Up | KeyCode::Char('k') if listing => self.value_counts.move_by(-1),
            KeyCode::PageDown if listing => self.value_counts.move_by(page),
            KeyCode::PageUp if listing => self.value_counts.move_by(-page),
            KeyCode::Home if listing => self.value_counts.move_to_start(),
            KeyCode::End | KeyCode::Char('G') if listing => self.value_counts.move_to_end(),
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
            KeyCode::Char('s') if listing => self.value_counts.toggle_order(),
            KeyCode::Char('c') => self.value_counts.toggle_view(),
            KeyCode::Char('a') => {
                if self.value_counts.current().is_some_and(|c| c.is_sample()) {
                    self.count_values(true);
                }
            }
            KeyCode::Enter if listing => self.drill_into_counted_value(),
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
        let stem = self.dataset_stem();
        self.export_modal.suggest_path(&format!("{stem}-counts"));
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
                    table: None,
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

    /// The tables of the source on screen, from what its open holds; `None` for a
    /// source of one.
    pub fn sibling_tables(&self) -> Option<table_switch::Tables> {
        let state = self.data_table_state.as_ref()?;
        let (paths, options) = self.opened.as_ref()?;
        table_switch::of(state, paths, options)
    }

    /// Whether the source on screen has another table for `T` to open.
    pub fn offers_other_tables(&self) -> bool {
        match (self.data_table_state.as_ref(), self.opened.as_ref()) {
            (Some(state), Some((paths, options))) => table_switch::several(state, paths, options),
            _ => false,
        }
    }

    /// `T` at the table: the source's tables, the one on screen marked, to open
    /// another. A source of one says so.
    fn open_table_picker(&mut self) {
        let Some(tables) = self.sibling_tables().filter(table_switch::Tables::several) else {
            self.flash_note("Only one table here".to_string());
            return;
        };
        let labels = tables.tables.iter().map(|t| t.label.clone()).collect();
        self.table_picker = crate::widgets::ui::PickerState::new(labels);
        if let Some(at) = tables.current {
            self.table_picker.select_original(at);
        }
        self.table_choices = Some(tables);
        self.input_mode = InputMode::PickTable;
    }

    /// The table picker owns the keys: type to narrow, ↑↓ move, Enter opens the table
    /// chosen, Esc closes.
    fn table_picker_key(&mut self, event: &KeyEvent) -> Option<AppEvent> {
        match event.code {
            KeyCode::Esc => {
                self.input_mode = InputMode::Normal;
                self.table_choices = None;
            }
            KeyCode::Enter => {
                let index = self.table_picker.selected_original()?;
                let tables = self.table_choices.take()?;
                self.input_mode = InputMode::Normal;
                if tables.current == Some(index) {
                    return None;
                }
                let table = tables.tables.get(index)?.table.clone();
                return self.switch_table(table);
            }
            KeyCode::Up => self.table_picker.move_up(),
            KeyCode::Down => self.table_picker.move_down(),
            KeyCode::Backspace => self.table_picker.backspace(),
            KeyCode::Char(c) => self.table_picker.filter_key(c, event.modifiers),
            _ => {}
        }
        None
    }

    /// Open `table` of the file on screen in its place (`None`: the whole file), as
    /// `--table` or home's row for it would: the query, filters and sort go with the
    /// table they were on, recents record it, and a view for it applies.
    pub(crate) fn switch_table(&mut self, table: Option<String>) -> Option<AppEvent> {
        let (paths, options) = self.opened.clone()?;
        let shown = match &table {
            Some(name) => crate::members::place(&paths[0], name),
            None => paths[0].clone(),
        };
        let options = OpenOptions {
            table,
            // `--view` was for the first open; a view for this table applies as on
            // any open.
            view: None,
            prepared: None,
            ..options
        };
        self.set_loading_phase("Scanning input", 10);
        self.name_what_is_loading(shown);
        Some(AppEvent::Open(paths, options))
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
            None => self.flash_note("No group to drill down into".to_string()),
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

    /// Under `theme.mode = "auto"`, switch to the theme for the terminal's background
    /// (`theme.dark` or `theme.light`), keeping the configured `theme.colors` over it as at startup. An
    /// explicit mode ignores the terminal.
    pub fn follow_terminal_background(&mut self, mode: ThemeMode) {
        let theme = &self.app_config.theme;
        if !theme.follow || theme.mode == Some(mode) {
            return;
        }
        let mut next = theme.clone();
        let built = theme.palette_for(mode).and_then(|colors| {
            next.colors = colors;
            next.mode = Some(mode);
            Theme::from_config(&next)
        });
        match built {
            Ok(built) => {
                self.theme = built;
                self.chart_modal.series_cap = Some(self.theme.series_colors().len());
                self.app_config.theme = next;
                // The prompts live as long as the app and keep the colors they were
                // given; a dialog's fields take the theme each time it opens.
                for input in [
                    &mut self.query_input,
                    &mut self.sql_input,
                    &mut self.find.input,
                ] {
                    *input = std::mem::take(input).with_theme(&self.theme);
                }
            }
            // The configured colors parsed at startup, so this is not expected; the
            // palette in use stays.
            Err(e) => log::warn!("cannot switch to the {mode:?} palette: {e}"),
        }
    }

    /// The terminal said what its background is: follow it under `auto`, and remember
    /// it for the next start's first frame.
    fn terminal_answered(&mut self, mode: ThemeMode) {
        if self.app_config.theme.follow {
            self.cache
                .remember_terminal_mode(&terminal_color::terminal_key(), mode);
        }
        self.follow_terminal_background(mode);
    }

    /// Settle the palette of the first frame under `auto`, without waiting for the
    /// terminal: its answer when `answered` has it, else what this terminal answered
    /// last time. An answer that comes later switches palettes if it differs.
    pub fn settle_first_palette(&mut self, answered: Option<ThemeMode>) {
        if let Some(mode) = answered {
            self.terminal_answered(mode);
        } else if self.app_config.theme.follow
            && let Some(mode) = self.cache.terminal_mode(&terminal_color::terminal_key())
        {
            self.follow_terminal_background(mode);
        }
    }

    /// Whether the run loop should ask the terminal for its background, once. Asked by
    /// [`AppEvent::TerminalFocused`] under `auto`.
    pub fn take_background_query(&mut self) -> bool {
        std::mem::take(&mut self.background_query)
    }

    /// The colors the next frame is drawn with.
    pub fn theme(&self) -> &Theme {
        &self.theme
    }

    /// Whether the palette follows the terminal (`theme.mode = "auto"`).
    pub fn follows_terminal(&self) -> bool {
        self.app_config.theme.follow
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

    pub fn create_view_from_current_state(
        &mut self,
        name: String,
        description: Option<String>,
        match_criteria: view::MatchCriteria,
    ) -> Result<view::SavedView> {
        let settings = match &self.data_table_state {
            Some(state) => view::ViewSettings {
                chart: self.saved_chart(),
                ..view_settings_of(state)
            },
            None => view::ViewSettings {
                chart: None,
                sample: None,
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
                columns: Vec::new(),
            },
        };

        self.view_manager
            .create_view(name, description, match_criteria, settings)
    }

    /// The command line's language while it is open.
    pub fn query_prompt_mode(&self) -> Option<QueryMode> {
        (self.input_mode == InputMode::Editing && self.input_type == Some(InputType::Query))
            .then_some(self.query_mode)
    }

    /// `:` at the table: the command line, holding the query in effect, selected,
    /// so typing states a new one and the arrows edit it.
    pub(crate) fn open_command_line(&mut self) {
        let Some(state) = self.data_table_state.as_mut() else {
            return;
        };
        self.input_mode = InputMode::Editing;
        self.input_type = Some(InputType::Query);
        self.query_run_error = None;
        self.sql_completion = None;
        self.query_input.set_value(state.get_active_query());
        self.sql_input.set_value(state.get_active_sql_query());
        self.query_input.select_all();
        self.sql_input.select_all();
        state.suppress_error_display = true;
        self.sql_columns = state.sql_table_columns();
        self.query_mode = self.opening_query_mode();
        self.query_text_restored = !self.query_input_shown().is_empty();
        self.sync_query_focus();
    }

    /// The language `:` opens in: the query in effect's own, so editing never
    /// reinterprets it; else the one Ctrl+T last chose; else the configured default.
    fn opening_query_mode(&self) -> QueryMode {
        let active = self.data_table_state.as_ref().and_then(|state| {
            if !state.get_active_sql_query().trim().is_empty() {
                Some(QueryMode::Sql)
            } else if !state.get_active_query().trim().is_empty() {
                Some(QueryMode::Q)
            } else {
                None
            }
        });
        active
            .or(self.query_mode_chosen)
            .unwrap_or(self.app_config.query.default_mode)
            .resolve()
    }

    /// The command line's input for its current language.
    fn query_input_mut(&mut self) -> &mut TextInput {
        match self.query_mode {
            QueryMode::Sql => &mut self.sql_input,
            QueryMode::Q => &mut self.query_input,
        }
    }

    /// The command line's input for its current language.
    pub(crate) fn query_input_shown(&self) -> &TextInput {
        match self.query_mode {
            QueryMode::Sql => &self.sql_input,
            QueryMode::Q => &self.query_input,
        }
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

    /// Tab in the command line: complete the column name (or, in SQL, the table
    /// name) being typed, and on further presses step through the other names that
    /// match.
    fn complete_column_name(&mut self) {
        let sql = self.query_mode == QueryMode::Sql;
        let columns = std::mem::take(&mut self.sql_columns);
        let mut cycle = self.sql_completion.take();
        let input = self.query_input_mut();
        let line = input
            .line_at(input.cursor_line())
            .unwrap_or_default()
            .to_string();
        let value = input.value().to_string();
        let complete = if sql {
            sql_assist::tab
        } else {
            sql_assist::q_tab
        };
        if let Some(step) = complete(
            &columns,
            &line,
            input.cursor_col(),
            &value,
            input.cursor(),
            &mut cycle,
        ) {
            input.replace_before_cursor(step.span, &step.insert);
            sql_assist::landed(&mut cycle, input.value(), input.cursor());
        }
        self.sql_completion = cycle;
        self.sql_columns = columns;
    }

    /// The columns of `df` the word at the command line's cursor could name, for
    /// the list under the input: every column while nothing is being typed.
    pub(crate) fn sql_column_matches(&self) -> Vec<&(String, DataType)> {
        let input = self.query_input_shown();
        let line = input.line_at(input.cursor_line()).unwrap_or_default();
        let word = match self.query_mode {
            QueryMode::Sql => sql_assist::word_before(line, input.cursor_col()),
            QueryMode::Q => sql_assist::q_word_before(line, input.cursor_col()),
        }
        .map(|w| w.text)
        .unwrap_or_default();
        sql_assist::matching(&self.sql_columns, &word)
    }

    /// The command line's text, while it is open.
    pub fn query_prompt_text(&self) -> Option<&str> {
        self.query_prompt_mode()?;
        Some(self.query_input_shown().value())
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
            QueryMode::Q => s.query(text.to_string()),
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
        if let Some(state) = &mut self.data_table_state {
            state.suppress_error_display = false;
        }
    }

    /// Only the current language's input carries the cursor.
    fn sync_query_focus(&mut self) {
        let mode = self.query_mode;
        self.sql_input.set_focused(mode == QueryMode::Sql);
        self.query_input.set_focused(mode == QueryMode::Q);
    }

    /// Esc from anywhere in the prompt: nothing runs and nothing typed survives.
    fn close_query_prompt(&mut self) {
        self.query_run_error = None;
        self.sql_completion = None;
        self.query_input.clear();
        self.sql_input.clear();
        self.query_input.set_focused(false);
        self.sql_input.set_focused(false);
        self.input_mode = InputMode::Normal;
        self.input_type = None;
        if let Some(state) = &mut self.data_table_state {
            state.dismiss_error();
            state.suppress_error_display = false;
        }
    }
}

impl Widget for &mut App {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let started = std::time::Instant::now();
        self.draw_frame(area, buf);
        self.debug.times.frame(started.elapsed());
    }
}

impl App {
    fn draw_frame(&mut self, area: Rect, buf: &mut Buffer) {
        self.begin_frame();
        self.debug.num_frames += 1;
        if self.debug.enabled {
            self.debug.show_help_at_render = self.help.is_open();
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

        // The footer grows, taking rows from the bottom of the view, only for a prompt
        // being typed or a job with progress.
        let progress = self.footer_progress_line(main_view_content);
        let prompt_room = crate::render::footer::MAX_LINES - 1;
        let prompt_rows = if main_view_content == MainViewContent::Datatable {
            crate::render::input_strip::rows(self, area.width, prompt_room)
        } else {
            0
        };
        let progress_rows = u16::from(progress.is_some() && prompt_rows < prompt_room);
        let footer_lines = 1 + prompt_rows + progress_rows;
        // The inspector is framed; its border sets it off from the footer.
        let rule = self.input_mode != InputMode::Inspect;
        let app_layout = app_layout(area, self.debug.enabled, footer_lines, rule);
        // A terminal too short for all of it keeps the status line first, then the
        // prompt, then the progress.
        let room = app_layout.footer.height.saturating_sub(1);
        let prompt_rows = prompt_rows.min(room);
        let progress_rows = progress_rows.min(room - prompt_rows);
        let main_area = app_layout.main_view;
        Clear.render(main_area, buf);

        crate::render::main_view_render::render_main_view(area, main_area, buf, self, &ctx);
        if self.in_normal_table_view() {
            self.render_drop_mark(buf, &ctx);
        }
        if self.menu_showing()
            && let Some(menu) = self.context_menu.clone()
        {
            menu.render(main_area, buf, &ctx);
        }

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
        self.close_help_left_behind();
        if self.help.is_open() {
            // Over the view, never the footer: its rule, and the lines it grows by
            // for a prompt or progress, are drawn after and would cut the frame.
            crate::render::help::render_help(app_layout.main_view, buf, &mut self.help, &ctx);
        }

        let footer = self.footer(main_view_content, progress_rows > 0);
        crate::render::footer::render_rule(app_layout.rule, buf, &ctx);
        let line = Rect {
            height: 1,
            ..app_layout.footer
        };
        let drawn = footer.render_line(line, buf, &ctx);
        if prompt_rows > 0 {
            crate::render::input_strip::render(
                Rect {
                    y: line.y + 1,
                    height: prompt_rows,
                    ..line
                },
                buf,
                self,
                &ctx,
            );
        }
        if let Some(progress) = progress.filter(|_| progress_rows > 0) {
            crate::render::footer::render_progress(
                &progress,
                Rect {
                    y: line.y + 1 + prompt_rows,
                    height: 1,
                    ..line
                },
                buf,
                &ctx,
            );
        }
        self.pointer.chips_drawn(
            drawn
                .iter()
                .map(|(rect, key)| (*rect, key.as_str()))
                .collect(),
        );
        if let Some(debug_area) = app_layout.debug {
            self.debug.render(debug_area, buf);
        }
        self.pointer.drawn();

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
        // The indexing stops, and the reads waiting on it give up, so nothing holds
        // the file once the app is gone (the Python binding runs on in the process).
        self.indexing_stop
            .store(true, std::sync::atomic::Ordering::Relaxed);
        if let Some(lines) = self.indexing_lines.take() {
            lines.stop_indexing();
        }
    }
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
pub(crate) fn wait_on_runtime<F>(runtime: &tokio::runtime::Handle, future: F) -> Option<F::Output>
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

/// `text` as a sentence for a flash: its first letter capitalized.
fn sentence(text: &str) -> String {
    let mut chars = text.chars();
    match chars.next() {
        Some(first) => first.to_uppercase().chain(chars).collect(),
        None => String::new(),
    }
}

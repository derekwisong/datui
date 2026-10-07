use color_eyre::Result;
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use polars::datatypes::DataType;
use polars::prelude::{DataFrame, LazyFrame, col};
use std::collections::HashMap;

use std::path::{Path, PathBuf};
use std::sync::{Arc, mpsc::Sender};
use widgets::info::{FileFacts, InfoModal};

use ratatui::style::Style;
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
pub mod chart_plot;
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
mod counting;
pub mod csv_dialect;
pub mod data_quality;
pub mod dataflash;
mod dataset_files;
pub mod dbc;
pub mod delimited_spec;
pub mod discover;
pub mod distribution_fit;
mod documentation_keys;
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
mod open_scan;
pub mod output_file;
mod overlay;
pub mod parquet_footer;
pub mod past_calendar;
mod picker_keys;
mod pivot_melt_keys;
pub mod pivot_melt_modal;
pub mod pointer;
pub mod pushdown;
pub mod python_script;
pub mod quality_export;
mod quality_form_keys;
pub mod quality_intent;
mod quality_keys;
mod quality_memory;
pub mod quality_report;
mod quality_runs;
pub mod quality_trends;
mod query_prompt;
#[cfg(any(feature = "http", feature = "cloud"))]
mod remote_model;
mod retype_keys;
pub mod retype_modal;
pub mod row_index;
mod run;
pub use run::{ended_by_signal, run, run_captured};
#[cfg(feature = "cloud")]
pub mod s3_tools;
mod sample_draw;
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
mod value_counts_keys;
pub mod value_counts_modal;
pub mod vcd;
pub mod view;
mod view_apply;
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
use chart_jobs::{ChartCache, ChartRequest};
use chart_modal::{ChartColumns, ChartModal};

pub use error_display::{ErrorKindForPython, error_for_python};
pub use export::{ExportOptions, ExportRequest};
use export_modal::{ExportFocus, ExportModal};
use feedback::Confirm;
pub use feedback::{ConfirmationModal, ErrorModal, Flash};
use filter_modal::{FilterOperator, FilterStatement, LogicalOperator};
use jobs::{Answer, Job, Jobs, Outcome};
pub use jobs::{JobKind, Progress, Ticket};
use numfmt::NumberFormatSettings;
pub use open_options::{
    OpenOptions, ParseStringsTarget, ReadReport, SqliteOpen, TypedDialect, UnaskedDownload,
};
use output_file::Overwrite;
use pivot_melt_modal::{MeltSpec, PivotMeltModal, PivotSpec};
pub use quality_memory::{KeptQualitySample, QUALITY_MEMORY_BUDGET, RetainedCopy};
use sort_filter_modal::SortFilterModal;
use sort_modal::{SortColumn, order_with_hidden};
use table::{DataTableState, DrillRow};
pub use unfinished::ExitSweep;
pub use view::{SavedView, ViewManager, Views};
use widgets::column_widths::WidthChoice;
use widgets::debug::DebugState;
use widgets::text_input::TextInput;
use widgets::view_modal::{FormFocus, ViewModal, ViewModalMode};

/// Application name used for cache directory and other app-specific paths
pub const APP_NAME: &str = "datui";

/// What a file no reader takes, and no hex view can show, is told.
pub(crate) const UNSUPPORTED: &str =
    "Unsupported file type. --format names the format to read it as.";

/// Re-export compression format and file format from CLI module
pub use cli::{CompressionFormat, FileFormat, ReadMode, RemoteRead, Stored, Summary};

#[cfg(test)]
pub mod tests;

/// A move through the table's rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scroll {
    Next,
    Prev,
    PageDown,
    PageUp,
    HalfDown,
    HalfUp,
    Start,
    End,
}

impl Scroll {
    /// Rows the move goes, to ask whether the buffer holds where it lands.
    fn delta(self, state: &DataTableState) -> i64 {
        let page = state.visible_rows as i64;
        let half = (state.visible_rows / 2).max(1) as i64;
        match self {
            Scroll::Next => 1,
            Scroll::Prev => -1,
            Scroll::PageDown => page,
            Scroll::PageUp => -page,
            Scroll::HalfDown => half,
            Scroll::HalfUp => -half,
            Scroll::Start | Scroll::End => 0,
        }
    }

    /// Make the move; true when the rows it lands on have to be read.
    fn run(self, state: &mut DataTableState) -> bool {
        match self {
            Scroll::Next => state.select_next(),
            Scroll::Prev => state.select_previous(),
            Scroll::PageDown => state.page_down(),
            Scroll::PageUp => state.page_up(),
            Scroll::HalfDown => state.half_page_down(),
            Scroll::HalfUp => state.half_page_up(),
            Scroll::Start => state.scroll_to_start(),
            Scroll::End => state.scroll_to_end(),
        }
    }
}

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
    Exit,
    Crash(String),
    QQuery(String),
    SqlQuery(String),
    Filter(Vec<FilterStatement>),
    Sort(Vec<String>, Vec<bool>), // Columns, and per column whether it runs descending
    ColumnOrder(Vec<String>, usize), // Column order, locked columns count
    /// The sidebar's Apply as one change: column order, locked count, filters, and the
    /// sort's columns with whether each runs descending.
    ApplyView(
        Vec<String>,
        usize,
        Vec<FilterStatement>,
        Vec<String>,
        Vec<bool>,
    ),
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
    /// A scroll deferred one frame, so the spinner shows while its rows are read.
    Scroll(Scroll),
    GoToLine(usize), // Deferred: jump to line number (when collect needed)
    /// Run an analysis tool off the UI thread; deferred so its progress shows first.
    AnalysisCompute(analysis_modal::AnalysisTool),
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
    /// Every line of a text file opened from its first rows is indexed, `rows` of
    /// them, for the dataset of `generation`.
    LinesIndexed {
        generation: u64,
        rows: usize,
    },
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
    /// Show the rest of `RECENT`, or of a directory cut short.
    ShowsMore,
    /// Go up a level: the `..` row.
    GoesUp,
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
            Some(home::Row::Up { .. }) => return WhatEnter::GoesUp,
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
    /// The count markers as they were, for the frame the rollback restores. A count
    /// of that frame still running when the query began lands while the query's frame
    /// is installed; its answer goes into `rollback`.
    counts: counting::CountMarkers,
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
pub(crate) enum Leaving {
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

impl ExportProgress {
    /// An export to `file_path` starting `phase`, nothing written yet.
    pub fn new(file_path: &Path, phase: &str) -> Self {
        Self {
            file_path: file_path.to_path_buf(),
            current_phase: phase.to_string(),
            written: None,
        }
    }
}

/// The correlation matrix of the sample's numeric columns. Only those are read: nothing
/// else is correlated, and on a wide table the rest is most of what a full read holds.
fn correlations_of_sample(
    lf: &LazyFrame,
    sample: &sampling::Sample,
    known_total: Option<usize>,
    streaming: bool,
) -> Result<crate::statistics::AnalysisResults> {
    let schema = lf.clone().collect_schema()?;
    let numeric: Vec<polars::prelude::Expr> = schema
        .iter()
        .filter(|(_, dtype)| dtype.is_numeric())
        .map(|(name, _)| col(name.clone()))
        .collect();
    let rows = crate::sampling::read(&lf.clone().select(numeric), sample, known_total, streaming)?;
    Ok(crate::statistics::AnalysisResults {
        column_statistics: vec![],
        total_rows: rows.total_rows,
        sample_size: rows.sample_size,
        per_value: rows.per_value.map(|per_value| per_value.kept),
        sample_seed: sample.seed,
        correlation_matrix: crate::statistics::compute_correlation_matrix(&rows.df).ok(),
        distribution_analyses: vec![],
    })
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
    /// The dataset's row count, footer pass and line indexing, and what waits on them.
    counting: counting::Counting,
    /// The home screen's work in flight, and what it keeps for the session.
    pub home_app: home_app::HomeApp,
    /// Home screen state. Rebuilt from the filesystem whenever home is entered;
    /// nothing here is persisted beyond the recents list.
    pub home: home::HomeState,
    /// Where the dataset on screen came from, and how it was opened.
    source: open_scan::OpenedSource,
    path: Option<PathBuf>,
    /// Standard input and output when datui sits in a pipe.
    pipes: run::Pipes,
    events: Sender<AppEvent>,
    debug: DebugState,
    pub info_modal: InfoModal,
    /// What the Info panel shows of the dataset beyond its schema.
    pub info: info_keys::InfoState,
    /// The command line: its inputs per mode, completion, and the query it is running.
    pub prompt: query_prompt::QueryPrompt,
    pub input_mode: InputMode,
    pub sort_filter_modal: SortFilterModal,
    pub pivot_melt_modal: PivotMeltModal,
    pub view_modal: ViewModal,
    pub analysis_modal: AnalysisModal,
    /// The sample form, and what the draws learned of memory and of the paths they took.
    pub sample: sample_draw::SampleState,
    /// What Data Quality runs keep within the memory budget.
    quality: quality_runs::QualityRuns,
    /// The chart view, its export form, and the preparations it keeps or waits on.
    pub chart: chart_jobs::Charts,
    pub export_modal: ExportModal,
    pub copy_modal: copy_modal::CopyModal,
    pub inspector_modal: inspector_modal::InspectorModal,
    /// What datui hands to other programs, and the clipboard.
    external: run::External,
    /// The go-to-column, format and table pickers.
    pub pickers: picker_keys::Pickers,
    /// The Value Counts screen (`F`).
    pub value_counts: value_counts_modal::ValueCountsModal,
    /// The counts the export dialog writes, when it was opened from Value Counts.
    export_counts: Option<polars::prelude::DataFrame>,
    /// The retype and combine forms.
    pub column_forms: retype_keys::ColumnForms,
    /// The hex view, and the number its next read is tagged with.
    pub hex_view: hex_keys::HexState,
    error_modal: ErrorModal,
    flash: Option<Flash>,
    pub confirmation_modal: ConfirmationModal,
    /// The help overlay, over whatever screen it was opened at.
    help: help::Help,
    /// What the mouse can land on in the last frame, and the last click.
    pointer: pointer::Pointing,
    /// The menu a right click on a cell opened, while it is open.
    context_menu: Option<context_menu::ContextMenu>,
    cache: CacheManager,
    /// The recent and the shape an open writes, which the home listing waits on.
    cache_writes: CacheWrites,
    /// Saved views, and the one applied to the dataset on screen.
    views: view_apply::SavedViews,
    /// An export under way, which the footer reports.
    export_progress: Option<ExportProgress>,
    theme: Theme, // Color theme for UI rendering
    /// How the table is drawn this session: from the config, with the session's own toggles.
    display: render::context::DisplaySettings,
    runtime: tokio::runtime::Handle, // Tokio runtime handle for background tasks
    /// Every general background operation, and the generation their answers are judged
    /// by. See [`jobs`].
    jobs: Jobs,
    /// The open in flight, from the request to its first rows: its phase, what the
    /// loading screen says, and what it holds. See [`loading`]. Going home abandons it;
    /// an answer from an open it no longer holds is dropped.
    loading: loading::Loader,
    /// Bumped once per dataset put on screen, which the jobs' generation is not: a collect
    /// bumps that, and the pass reading the rest of a dataset's footers outlives
    /// several. It is what says whether the columns arriving belong to the dataset the
    /// user is looking at.
    dataset_generation: u64,
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
    app_config: AppConfig,
    /// The format specs on the search path, read when the app was built.
    formats: Arc<crate::formats::Registry>,
}

impl App {
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
        self.source
            .opened
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
        let (_, options) = self.source.opened.as_ref()?;
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

    /// A cancelled analysis is still reading: say so, and start no read beside it.
    fn read_waits_for_cancelled(&mut self) -> bool {
        if self.cancelled_analysis_running().is_none() {
            return false;
        }
        self.flash_note(ANALYSIS_READ_WAITS.to_string());
        true
    }

    /// Run Describe, Distributions or Correlations on the sample, off the UI thread.
    /// Data Quality runs its own plan.
    fn spawn_analysis(&mut self, tool: analysis_modal::AnalysisTool) -> Option<AppEvent> {
        use analysis_modal::AnalysisTool;
        type Compute = fn(
            &LazyFrame,
            &sampling::Sample,
            Option<usize>,
            bool,
        ) -> Result<crate::statistics::AnalysisResults>;
        type Install = fn(&mut analysis_modal::AnalysisModal, crate::statistics::AnalysisResults);
        let (status, compute, install): (&str, Compute, Install) = match tool {
            AnalysisTool::DataQuality => return self.run_quality_compute(),
            AnalysisTool::Describe => (
                "Running analysis...",
                |lf, sample, known, streaming| {
                    crate::statistics::compute_describe_from_lazy(lf, known, sample, streaming)
                },
                |modal, results| modal.describe_results = Some(results),
            ),
            AnalysisTool::DistributionAnalysis => (
                "Analyzing distributions...",
                |lf, sample, known, streaming| {
                    let options = crate::statistics::ComputeOptions {
                        include_distribution_info: true,
                        include_distribution_analyses: true,
                        include_correlation_matrix: false,
                        include_skewness_kurtosis_outliers: true,
                        polars_streaming: streaming,
                    };
                    crate::statistics::compute_statistics_for_sample(lf, sample, known, options)
                },
                |modal, results| modal.distribution_results = Some(results),
            ),
            AnalysisTool::CorrelationMatrix => (
                "Computing correlation matrix...",
                correlations_of_sample,
                analysis_modal::AnalysisModal::install_correlations,
            ),
        };
        let Some(state) = &self.data_table_state else {
            self.analysis_modal.computing = None;
            self.busy = false;
            return None;
        };
        // Binary columns are stubbed by the source: their blobs are never read for
        // analysis (multi-GB blobs across partitions can exhaust memory).
        let (source, known_total) = self.sample_source(state);
        let streaming = match tool {
            AnalysisTool::CorrelationMatrix => state.polars_streaming(),
            _ => self.app_config.performance.streaming,
        };
        let sample = self.analysis_modal.sample.clone();
        self.spawn_job(
            Job::Analysis(jobs::AnalysisRun::default()),
            Some(status),
            move |_| {
                let results = source
                    .cut(&sample.scope)
                    .and_then(|lf| compute(&lf, &sample, known_total, streaming))
                    .map_err(|e| format!("{e}"))?;
                Ok(Answer::Analysis(install, results))
            },
        );
        None
    }

    /// Run the selected tool again from scratch, as `r` and `a` do.
    fn start_analysis_run(&mut self) -> Option<AppEvent> {
        let tool = self.analysis_modal.selected_tool?;
        if tool != analysis_modal::AnalysisTool::DataQuality && self.read_waits_for_cancelled() {
            return None;
        }
        let phase = match tool {
            analysis_modal::AnalysisTool::Describe => {
                self.analysis_modal.describe_results = None;
                "Describing data"
            }
            analysis_modal::AnalysisTool::DistributionAnalysis => {
                self.analysis_modal.distribution_results = None;
                "Analyzing distributions"
            }
            analysis_modal::AnalysisTool::CorrelationMatrix => {
                self.analysis_modal.correlation_results = None;
                "Computing correlations"
            }
            analysis_modal::AnalysisTool::DataQuality => return None,
        };
        self.analysis_modal.computing = Some(AnalysisProgress::new(phase));
        self.busy = true;
        Some(AppEvent::AnalysisCompute(tool))
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
        self.pipes.stdin_reader = Some(Box::new(reader));
    }

    /// Pass the stream on to `out` for `--tee -`: standard output as the process got
    /// it, or a test's pipe.
    #[doc(hidden)]
    pub fn pass_stdout_to(&mut self, out: impl std::io::Write + Send + 'static) {
        self.pipes.stdout_pass = Some(Box::new(out));
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
            self.counting.followed_fields_held = Some((self.dataset_generation, fields));
        }
        self.catch_up_follow();
        self.join_followed_fields();
        self.describe_ended_journal();
    }

    /// Read a piped journal's Info tab again once it has ended, over every entry: the
    /// one the open read describes the entries that had arrived then. Nobody waits on
    /// it; the table works meanwhile.
    fn describe_ended_journal(&mut self) {
        let Some(lf) = self
            .data_table_state
            .as_mut()
            .and_then(|state| state.ended_journal_to_describe())
        else {
            return;
        };
        let dataset = self.dataset_generation;
        self.spawn_job(Job::JournalDetail { dataset }, None, move |_| {
            Ok(Answer::JournalDescribed(
                crate::journal::summary(&lf).ok().map(Box::new),
            ))
        });
    }

    /// Join the fields a followed pipe brought after the open, if the dataset can
    /// take them now, and read the rows on screen through the wider frame. Tried again
    /// after every event while they wait, as footers are.
    fn join_followed_fields(&mut self) {
        let Some((generation, _)) = self.counting.followed_fields_held.as_ref() else {
            return;
        };
        if *generation != self.dataset_generation {
            self.counting.followed_fields_held = None;
            return;
        }
        // Not under rows still being taken: the view reads the new rows first.
        if self.data_table_state.is_none()
            || self.work_the_join_would_cancel()
            || self.follow().is_some_and(|f| f.behind())
        {
            return;
        }
        let Some((generation, fields)) = self.counting.followed_fields_held.take() else {
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
            Err(()) => self.counting.followed_fields_held = Some((generation, fields)),
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
                self.counting.end_after_count = Some(state.len_generation());
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
        self.source
            .opened
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
                Some(if self.source.opened_from_home {
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
        self.confirmation_modal.show_choice(
            message,
            "Stop recording",
            "Keep recording",
            Confirm::Leave(leaving),
        );
    }

    /// Leave as asked: the recording stopped and its file finished, or kept going
    /// until its stream ends, while datui goes home or quits.
    fn leave_recording(&mut self, leaving: Leaving, stop: bool) -> Option<AppEvent> {
        let handle = self
            .source
            .opened
            .as_ref()
            .and_then(|(_, options)| options.spool.clone());
        if stop {
            if let Some(handle) = &handle {
                handle.spool().stop();
            }
        } else {
            // Held past the dataset, so letting it go does not stop the copy.
            self.pipes.recording_on = handle;
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
        let handle = self.pipes.recording_on.take()?;
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
            self.pipes.recording_end_said = false;
            return false;
        };
        if std::mem::replace(&mut self.pipes.recording_end_said, true) {
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
        if now == self.pipes.follow_drawn {
            return ended;
        }
        self.pipes.follow_drawn = now;
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
                && self.chart.modal.picker.is_none()
                && !self.chart.export_modal.active)
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
        let (paths, options) = self.source.opened.clone()?;
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
        let leave_quality_evidence = self.quality.evidence_return.is_some()
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
            InputMode::Combine => {
                self.column_forms.combine.as_ref().is_some_and(|c| {
                    c.picker.is_some() || c.focus == retype_modal::CombineField::Name
                })
            }
            InputMode::PickTable => true,
            InputMode::Sample => self
                .sample
                .form
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
                if self.chart.export_modal.active {
                    // Every row is a choice or a text field.
                    self.chart
                        .export_modal
                        .choice(self.chart.export_modal.focus)
                        .is_none()
                } else {
                    // The open column Picker narrows by typing, so it types.
                    self.chart.modal.picker.is_some()
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
                .hex_view
                .view
                .as_ref()
                .is_some_and(|view| view.prompt.is_some() || view.picker.is_some()),
        }
    }

    pub fn send_event(&mut self, event: AppEvent) -> Result<()> {
        self.events.send(event)?;
        Ok(())
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

    /// Take the numbers the whole frame will be drawn from.
    ///
    /// Only one so far: the footer count. It is read here rather than where it is
    /// shown because two parts of the screen show it, they are painted at different
    /// moments, and a background thread is moving it between them.
    fn begin_frame(&mut self) {
        // Back from home to the table whose lines were being indexed.
        if self.counting.indexing_paused && self.input_mode != InputMode::Home {
            self.index_lines();
        }
        self.counting.footers_this_frame = self.footer_progress().reading();
        self.counting.listed_this_frame = self.footer_progress().listed();
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
        self.loading
            .progress()
            .unwrap_or(&self.counting.footer_progress)
    }

    /// Hold past the app: dropped after it, it removes the temp files the app's opens
    /// were still writing, giving their workers up to a second to stop first. Without
    /// it, a quit mid-download or mid-decompression can end the process before the
    /// worker removes its partial file.
    pub fn exit_sweep(&self) -> ExitSweep {
        ExitSweep(self.loading.unfinished().clone())
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
        if !state.wants_to_load_ahead() || self.counting.loaded_ahead_from == Some(position) {
            return;
        }
        self.counting.loaded_ahead_from = Some(position);
        self.spawn_collect(None);
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
            && self.counting.len_count_inflight != Some(generation)
            // Marked as running only once it is going to run. A dataset still reading
            // its own footers declines this count, because that pass is bringing it —
            // and the marker is cleared by a count coming back, so setting it for one
            // that was never started leaves it set for the rest of the session: a
            // spinner where the row count goes, a redraw on its account every frame,
            // and `End` waiting on nothing.
            && !state.counts_itself_later()
            // A count that failed is not tried again on every scroll. End asks again.
            && self.counting.len_count_failed != Some(generation)
            // A dataset of too many files to count unasked shows its estimate.
            && !held
        {
            self.counting.len_count_inflight = Some(generation);
            count = Some(LenCount::for_state(state));
        }
        let footers = count.take_if(|job| job.reads_footers());
        if count.take_if(|_| !state.is_remote_source()).is_some() {
            self.counting.count_after_paint = Some(generation);
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
                self.counting.len_count_inflight = None;
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
        self.home_app.reads.pages += 1;
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

    /// A move through the rows: made now when the buffer holds where it lands, or
    /// deferred a frame, busy, while the rows are read.
    fn scroll_key(&mut self, scroll: Scroll) -> Option<AppEvent> {
        let state = self.data_table_state.as_mut()?;
        if state.scroll_would_trigger_collect(scroll.delta(state)) {
            self.busy = true;
            return Some(AppEvent::Scroll(scroll));
        }
        scroll.run(state);
        None
    }

    /// Home, End and G. A jump may need a fill, so it is deferred behind a frame that
    /// shows the throbber — setting `start_row` alone used to leave the old buffer on
    /// screen, drawn from its first row — unless the view is already there, in which
    /// case only the selection settles and no frame or key is spent.
    fn jump_key(&mut self, jump: Scroll) -> Option<AppEvent> {
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
        if jump == Scroll::End
            && let Some(state) = self.data_table_state.as_ref()
            && state.indexing().is_some()
        {
            self.counting.end_when_indexed = Some(self.dataset_generation);
            self.status_message = Some(Self::COUNTING_FOR_END.to_string());
            return None;
        }
        if jump == Scroll::End
            && let Some(state) = self.data_table_state.as_ref()
            && state.footers_pending().is_some()
            && state.scan_is_the_root()
            && !state.is_num_rows_valid()
        {
            self.counting.end_when_the_footers_land = Some(self.dataset_generation);
            self.status_message = Some(Self::COUNTING_FOR_END.to_string());
            return None;
        }
        // Any other frame whose end is not known yet waits for its count too, rather than
        // jumping to the end of the rows read so far. A count waiting on a paint starts
        // now; one already running or riding in a collect is waited on.
        if jump == Scroll::End
            && let Some(state) = self.data_table_state.as_ref()
            && !state.is_num_rows_valid()
        {
            let generation = state.len_generation();
            self.counting.end_after_count = Some(generation);
            self.status_message = Some(Self::COUNTING_FOR_END.to_string());
            let held = self.counting.count_after_paint == Some(generation);
            if held {
                self.counting.count_after_paint = None;
            }
            if held || self.counting.len_count_inflight != Some(generation) {
                self.counting.len_count_inflight = Some(generation);
                self.spawn_count(LenCount::for_state(state));
            }
            return None;
        }
        let state = self.data_table_state.as_mut()?;
        let already_there = match jump {
            Scroll::Start => state.start_row() == 0,
            _ => state.at_end(),
        };
        if already_there {
            jump.run(state);
            return None;
        }
        self.busy = true;
        Some(AppEvent::Scroll(jump))
    }

    /// Run a scroll on `data_table_state` and resolve the busy/spawn cycle.
    /// `scroll` returns true when its movement leaves the buffered window (caller must collect).
    /// We clear `busy` ourselves when no collect is needed or the spawn no-ops, otherwise
    /// the busy flag set by the key handler would gate further input forever.
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
            counting: counting::Counting {
                footer_progress: Arc::new(crate::schema_union::FooterProgress::default()),
                footers_this_frame: None,
                loaded_ahead_from: None,
                len_count_inflight: None,
                count_after_paint: None,
                #[cfg(test)]
                counts_spawned: std::cell::Cell::new(0),
                #[cfg(test)]
                first_rows_asked: 0,
                len_count_failed: None,
                end_after_count: None,
                end_when_the_footers_land: None,
                end_when_indexed: None,
                indexing_stop: Arc::default(),
                indexing_lines: None,
                indexing_paused: false,
                goto_when_indexed: None,
                count_progress: Arc::default(),
                exact_count_asked: None,
                count_after_stop: None,
                footers_held: None,
                followed_fields_held: None,
                reread_owed: None,
                listed_this_frame: None,
            },
            home: home::HomeState {
                hide_unreadable: !app_config.home.show_unreadable,
                formats: formats.clone(),
                ..Default::default()
            },
            home_app: home_app::HomeApp {
                probes_inflight: Vec::new(),
                listing_cancels: HashMap::new(),
                narrowing: None,
                #[cfg(feature = "cloud")]
                cloud_discovery_started: false,
                search_inflight: false,
                search_generation: 0,
                last_load_error: None,
                schema_inflight: Vec::new(),
                generation: 0,
                refresh_owed: false,
                schema_cache: HashMap::new(),
                previews: crate::home_preview::Previews::default(),
                remembered_moved: false,
                #[cfg(test)]
                worker_dies: None,
                local_desktop: link_open::local_desktop(link_open::Platform::current(), |name| {
                    std::env::var(name).ok()
                }),
                reads: crate::home_preview::ReadCounts::default(),
            },
            source: open_scan::OpenedSource {
                original_file_format: None,
                original_file_delimiter: None,
                opened: None,
                opened_from_home: false,
                startup_view: None,
                shape_remembered: None,
            },
            pipes: run::Pipes {
                stdin_reader: None,
                stdout_pass: None,
                follow_drawn: None,
                recording_on: None,
                recording_end_said: false,
            },
            events,
            debug: DebugState::default(),
            info_modal: InfoModal::new(),
            info: info_keys::InfoState {
                file_facts: None,
                codebook: None,
                catalog_entry: None,
                documentation: Default::default(),
                info_documentation: Default::default(),
                head_web_rows: !cache::running_as_a_cargo_test(),
            },
            prompt: query_prompt::QueryPrompt {
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
                input_type: None,
                query_mode: QueryMode::default().resolve(),
                query_mode_chosen: None,
                query_text_restored: false,
                sql_columns: Vec::new(),
                sql_completion: None,
                query_running: None,
                query_run_error: None,
                inline_failures: 0,
            },
            input_mode: InputMode::Normal,
            sort_filter_modal: SortFilterModal::new(),
            pivot_melt_modal: PivotMeltModal::new(),
            view_modal: ViewModal::new(),
            analysis_modal: AnalysisModal::with_sample_rows(app_config.analysis.sample_rows),
            sample: sample_draw::SampleState {
                form: None,
                memory_probe: std::sync::Arc::new(table_sample::available_memory),
                paths: Vec::new(),
            },
            quality: quality_runs::QualityRuns {
                cache: Vec::new(),
                samples: Vec::new(),
                released: Vec::new(),
                memory_budget: QUALITY_MEMORY_BUDGET,
                copies: Vec::new(),
                copy_released: None,
                copy_unusable: None,
                copy_free: std::sync::Mutex::new(None),
                evidence_return: None,
                evidence_label: None,
            },
            chart: chart_jobs::Charts {
                modal: ChartModal::new(),
                export_modal: chart_export_modal,
                cache: ChartCache::default(),
                asked: None,
                export_waiting: None,
            },
            export_modal: ExportModal::new(),
            copy_modal: copy_modal::CopyModal::new(),
            inspector_modal: inspector_modal::InspectorModal::new(),
            external: run::External {
                open: None,
                open_dir: None,
                clipboard: None,
            },
            pickers: picker_keys::Pickers {
                go_to_column: crate::widgets::ui::PickerState::default(),
                format_picker: crate::widgets::ui::PickerState::default(),
                table_picker: crate::widgets::ui::PickerState::default(),
                table_choices: None,
            },
            value_counts: value_counts_modal::ValueCountsModal::default(),
            hex_view: hex_keys::HexState {
                view: None,
                serial: 0,
            },
            export_counts: None,
            column_forms: retype_keys::ColumnForms {
                retype: None,
                combine: None,
                retype_from_info: false,
            },
            error_modal: ErrorModal::new(),
            flash: None,
            confirmation_modal: ConfirmationModal::new(),
            help: help::Help::default(),
            pointer: pointer::Pointing::default(),
            context_menu: None,
            cache,
            cache_writes: CacheWrites::default(),
            views: view_apply::SavedViews {
                manager: view_manager,
                active_id: None,
            },
            export_progress: None,
            theme,
            display: render::context::DisplaySettings {
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
                background_query: false,
            },
            jobs,
            runtime,
            loading: loading::Loader::default(),
            dataset_generation: 0,
            #[cfg(test)]
            file_facts_reader: None,
            busy: false,
            throbber_frame: 0,
            screen_generation: 0,
            input_dropped: false,
            status_message: None,
            app_config,
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
        if self.home_app.refresh_owed {
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
}

impl App {
    /// Whether the help overlay is on screen.
    pub fn help_visible(&self) -> bool {
        self.help.is_open()
    }

    /// The screen the help overlay shows the keys of, while it is up.
    pub fn help_context(&self) -> Option<datui_cli::keys::Context> {
        self.help.context()
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
                && !self.info.documentation.is_open()
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
            InputMode::Editing => match self.prompt.input_type {
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
            InputMode::Home if self.info.documentation.is_open() => Context::Documentation,
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
        self.confirmation_modal.active
            && matches!(self.confirmation_modal.asking, Some(Confirm::Download))
    }

    /// Enter on the confirmation's Yes, or on either choice of one whose No acts too.
    fn confirmed(&mut self) -> Option<AppEvent> {
        let stop = self.confirmation_modal.focus_yes;
        match self.confirmation_modal.take()? {
            Confirm::Leave(leaving) => self.leave_recording(leaving, stop),
            Confirm::ReadAll => {
                // Every row is a sample method like the others: it shows in the strip,
                // and `s` changes it back.
                let sample = sampling::Sample {
                    method: sampling::SampleMethod::EveryRow,
                    ..self.analysis_modal.sample.clone()
                };
                self.apply_sample(sample)
            }
            Confirm::OpenLink(url) => Some(AppEvent::OpenLink(url)),
            Confirm::ClearRecents => {
                self.cache.clear_recents();
                self.home_refresh();
                self.home.status = None;
                None
            }
            Confirm::QualityFullScan => self.run_quality_setup(true),
            Confirm::HideExamples => {
                self.cache.hide_examples();
                self.home_refresh();
                self.home.select_first_entry();
                None
            }
            Confirm::DeleteView(id) => {
                if self.views.manager.delete_view(&id).is_ok() {
                    self.refresh_view_list();
                }
                None
            }
            Confirm::ForgetPlace(place) => {
                let paths = self.home.recents_in(&place);
                self.cache.forget_recents(&paths);
                self.home_refresh();
                self.home.status = None;
                None
            }
            // The overwrite was agreed to: each export may now replace the file it
            // asked about, and only through that answer.
            Confirm::QualityExport(path, format) => Some(AppEvent::QualityReportExport(
                path,
                format,
                Overwrite::Replace,
            )),
            Confirm::ChartExport(request) => Some(AppEvent::ChartExport(ChartExportRequest {
                overwrite: Overwrite::Replace,
                ..*request
            })),
            Confirm::Export(request) => Some(AppEvent::Export(ExportRequest {
                overwrite: Overwrite::Replace,
                ..*request
            })),
            Confirm::Copy(format, header) => Some(AppEvent::CopyTable { format, header }),
            Confirm::Download => {
                // The loader lets go of its hold on the generation as the download or
                // the read starts, and its job takes it before anything else can look.
                let step = self.loading.confirmed();
                self.run_load_step(step)
            }
        }
    }

    /// No or Esc on the confirmation: nothing it asked about happens. A declined
    /// overwrite returns to the filled form, so the typed path, format and options
    /// survive; the report's dialog and a declined full scan's draft stay where they
    /// were.
    fn declined(&mut self) -> Option<AppEvent> {
        match self.confirmation_modal.take() {
            Some(Confirm::ChartExport(_)) => self.chart.export_modal.resume(),
            Some(Confirm::Export(_)) => {
                self.export_modal.resume();
                self.input_mode = InputMode::Export;
            }
            // Backing out of a download, or a large read, goes home: `enter_home` puts
            // the open down.
            Some(Confirm::Download) => self.enter_home(),
            _ => {}
        }
        None
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
                KeyCode::Enter if self.confirmation_modal.focus_yes => {
                    return self.confirmed();
                }
                KeyCode::Enter => {
                    // The recording question's No is a choice too: keep recording.
                    if matches!(self.confirmation_modal.asking, Some(Confirm::Leave(_))) {
                        return self.confirmed();
                    }
                    return self.declined();
                }
                KeyCode::Esc => {
                    // Staying: the recording goes on, and so does the view.
                    if matches!(self.confirmation_modal.asking, Some(Confirm::Leave(_))) {
                        self.confirmation_modal.hide();
                        return None;
                    }
                    return self.declined();
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
                        let reason = self.home_app.last_load_error.take();
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
            self.prompt.column_hints = Self::column_cursor_key(event).is_some()
                || (self.prompt.column_hints
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
            self.debug.action(|| format!("move_cursor({mv:?})"));
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
                if self.source.opened_from_home {
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
                self.display.dtype_row = !self.display.dtype_row;
                let on = if self.display.dtype_row { "on" } else { "off" };
                self.debug.action(|| format!("toggle_dtype_row({on})"));
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
                self.display.number_format.enabled = !self.display.number_format.enabled;
                let on = if self.display.number_format.enabled {
                    "on"
                } else {
                    "off"
                };
                self.debug.action(|| format!("toggle_number_format({on})"));
                None
            }
            KeyCode::Esc => {
                // The find is the nearest layer: its mark goes first, then a drill.
                if self.find_shown() {
                    self.prompt.find.active = None;
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
            code if event.is_press() && DOWN_KEYS.contains(&code) => self.scroll_key(Scroll::Next),
            code if event.is_press() && UP_KEYS.contains(&code) => self.scroll_key(Scroll::Prev),
            KeyCode::PageDown if event.is_press() => self.scroll_key(Scroll::PageDown),
            KeyCode::Home if event.is_press() => self.jump_key(Scroll::Start),
            KeyCode::End | KeyCode::Char('G') if event.is_press() => self.jump_key(Scroll::End),
            KeyCode::Char('f')
                if event.modifiers.contains(KeyModifiers::CONTROL) && event.is_press() =>
            {
                self.scroll_key(Scroll::PageDown)
            }
            KeyCode::Char('b')
                if event.modifiers.contains(KeyModifiers::CONTROL) && event.is_press() =>
            {
                self.scroll_key(Scroll::PageUp)
            }
            KeyCode::Char('d')
                if event.modifiers.contains(KeyModifiers::CONTROL) && event.is_press() =>
            {
                self.scroll_key(Scroll::HalfDown)
            }
            KeyCode::Char('u')
                if event.modifiers.contains(KeyModifiers::CONTROL) && event.is_press() =>
            {
                self.scroll_key(Scroll::HalfUp)
            }
            KeyCode::PageUp if event.is_press() => self.scroll_key(Scroll::PageUp),
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
                        .views
                        .manager
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
                        self.display.history_limit,
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
                    && self.quality.evidence_return.is_none()
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
                    self.chart.modal.series_cap = Some(self.theme.series_colors().len());
                    self.chart.modal.row_order = self.view_state().sort;
                    let sampled = state.sampled().is_some();
                    self.chart.modal.open(
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
                        self.chart.modal.row_limit = None;
                    } else if self.chart.modal.view_sampled {
                        self.chart.modal.row_limit = Some(self.chart.modal.sample_rows);
                    }
                    self.chart.modal.view_sampled = sampled;
                    self.chart.cache.clear();
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
                        self.source.original_file_format,
                        self.display.history_limit,
                        &self.theme,
                        self.source.original_file_delimiter,
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
    pub fn handle(&mut self, event: AppEvent) -> EventOutcome {
        let started = std::time::Instant::now();
        let outcome = self.handle_event(event);
        self.debug.times.handler(started.elapsed());
        outcome
    }

    fn handle_event(&mut self, event: AppEvent) -> EventOutcome {
        // Without the pump to offer it as typed, a pressed key is a key.
        if let AppEvent::Press(key) = event {
            return self.handle_event(AppEvent::Key(key));
        }
        if let AppEvent::Key(key) = event
            && self.is_busy()
            && !self.key_acts_while_busy(&key)
        {
            return Err(key);
        }
        let out = self.dispatch_event(event);
        // Not while this handler is returning a continuation. A follow-up is the rest of
        // the event just handled — the analysis sets `computing` and returns
        // `AnalysisCompute`, and the job that will run has not spawned — so
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

    pub fn event(&mut self, event: AppEvent) -> Option<AppEvent> {
        self.handle(event).unwrap_or(None)
    }

    fn dispatch_event(&mut self, event: AppEvent) -> Option<AppEvent> {
        self.debug.num_events += 1;

        match event {
            AppEvent::Key(key) => {
                // Leaving while standard input is still being recorded asks first.
                if let Some(leaving) = self.leaves(&key)
                    && !self.confirmation_modal.active
                    && self.recording().is_some_and(|spool| spool.live())
                {
                    self.ask_about_recording(leaving);
                    return None;
                }
                self.key(&key)
            }
            AppEvent::Open(paths, options) => {
                if paths.is_empty() {
                    return Some(AppEvent::Crash("No paths provided".to_string()));
                }
                // Home is now in the stack, so q pops back to it. Never unset:
                // a reread from the table (H) is not a new place.
                if self.input_mode == InputMode::Home {
                    self.source.opened_from_home = true;
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
                if expanded != paths {
                    return Some(AppEvent::Open(expanded, options));
                }
                // Asks the filesystem for the size the loading screen shows, and whether
                // the path is there to be a recent.
                let mut request = loading::OpenRequest::named(paths, options, &self.formats);
                request.warn_in_memory_above = self.app_config.read.memory_warning();
                self.begin_new_dataset();
                let step = self.loading.open(request);
                self.run_load_step(step)
            }
            AppEvent::OpenLazyFrame(lf, options) => {
                self.begin_new_dataset();
                let step = self.loading.open_frame(*lf, options);
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
            AppEvent::Scroll(scroll) => self.handle_scroll(|s| scroll.run(s)),
            AppEvent::GoToLine(n) => {
                // Past the lines indexed so far: gone to once they all are.
                if let Some(state) = self.data_table_state.as_ref()
                    && state.indexing().is_some()
                    && (n >= state.num_rows()
                        || state.changes_rows()
                        || !state.view_sort_columns().is_empty()
                        || !state.view_sort_ascending())
                {
                    self.counting.goto_when_indexed = Some((self.dataset_generation, n));
                    self.status_message = Some(Self::INDEXING_FOR_ROW.to_string());
                    self.busy = false;
                    return None;
                }
                self.handle_scroll(|s| s.scroll_to_row_centered(n))
            }
            AppEvent::AnalysisCompute(tool) => self.spawn_analysis(tool),
            AppEvent::BackgroundLenReady { .. }
            | AppEvent::FramePainted
            | AppEvent::BackgroundLenFailed { .. }
            | AppEvent::LinesIndexed { .. } => self.counting_event(event),
            AppEvent::BackgroundQualitySampleKept { kept } => {
                self.retain_quality_sample(&kept);
                None
            }
            AppEvent::BackgroundQualityCopyKept {
                dataset_generation,
                copy,
            } => {
                self.retain_quality_copy(dataset_generation, copy);
                None
            }
            AppEvent::OpenNamed(paths, options) => {
                if let Some(event) = Self::route_named_without_looking(&paths, &options) {
                    return Some(event);
                }
                let (paths, options) = (paths, options);
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
                let looking = dir;
                let options = options;
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
                let looking = path;
                self.jobs.supersede(|job| matches!(job, Job::Classify(_)));
                let look = Job::Classify(jobs::Classify {
                    path: looking.clone(),
                    browsing: self.home.browsing.clone(),
                    jump,
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
            AppEvent::JobEnded(ticket) => self.job_ended(ticket),
            AppEvent::JobProgress { ticket, progress } => {
                self.job_progress(ticket, &progress);
                None
            }
            AppEvent::QQuery(query) => {
                self.run_query(QueryMode::Q, &query, "Applying query...");
                None
            }
            AppEvent::SqlQuery(sql) => {
                self.run_query(QueryMode::Sql, &sql, "Applying SQL query...");
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
                self.views.active_id = None;
                None
            }
            AppEvent::ApplyView(order, locked, filters, columns, descending) => {
                if let Some(state) = &mut self.data_table_state {
                    state.deferred(|s| {
                        s.apply_view(
                            order.clone(),
                            locked,
                            filters.clone(),
                            columns.clone(),
                            descending.clone(),
                        )
                    });
                    self.spawn_async_collect("Sorting...");
                }
                None
            }
            AppEvent::ColumnOrder(order, locked_count) => {
                if let Some(state) = &mut self.data_table_state {
                    state.deferred(|s| {
                        s.set_column_order(order.clone());
                        s.set_locked_columns(locked_count);
                    });
                    self.spawn_async_collect(Self::LOADING_BUFFER);
                }
                None
            }
            AppEvent::Pivot(spec) => {
                // The modal stays up until the result is in, so a pivot that fails
                // leaves the spec there to fix.
                let job = self.data_table_state.as_ref()?.plan_pivot(&spec);
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
                    let result = state.deferred(|s| s.melt(&spec));
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
                let results = self.analysis_modal.quality.results.clone()?;
                let plan = self.analysis_modal.quality_result_plan().clone();
                let (path, format, overwrite) = (path, format, overwrite);
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
            AppEvent::ChartExport(..) | AppEvent::DoChartExport(..) => self.chart_event(event),
            AppEvent::Export(request) => {
                if self.data_table_state.is_some() {
                    self.busy = true;
                    self.export_progress =
                        Some(ExportProgress::new(&request.path, "Preparing export"));
                    // Drawn before the export starts.
                    Some(AppEvent::DoExport(request))
                } else {
                    None
                }
            }
            AppEvent::Followed(news) => {
                self.followed(&news);
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
                    crate::export::Route::Streamed => Self::export_write_phase(&request),
                    crate::export::Route::Collected => "Collecting data",
                };
                self.export_progress = Some(ExportProgress::new(&request.path, phase));
                let writing = Self::export_write_phase(&request);
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
                if link_open::open(&url).is_err() {
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
                    let (format, header) = (format, header);
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
                self.terminal_answered(mode);
                None
            }
            AppEvent::TerminalFocused => {
                self.display.background_query |= self.app_config.theme.follow;
                None
            }
            // Taken before they reach here: a press becomes a key in `handle_event`;
            // the terminal, a wake, an exit, a crash and a missing named path in the event
            // pump; the settings in `run`. An update asks for a frame and nothing else.
            AppEvent::Press(_)
            | AppEvent::Terminal(_)
            | AppEvent::Wake
            | AppEvent::SettingsRead(_)
            | AppEvent::NamedPathMissing(_)
            | AppEvent::Exit
            | AppEvent::Crash(_)
            | AppEvent::Update => None,
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
        self.source
            .opened
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
        let (paths, options) = self.source.opened.clone()?;
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
}

impl App {
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
        self.close_overlay();
        if view_unchanged {
            return None;
        }
        Some(AppEvent::ApplyView(
            column_order,
            locked_count,
            statements,
            columns,
            descending,
        ))
    }

    /// The facts read for the dataset on screen, if its file has one: one file, stored
    /// as its format says (a stream or a compressed copy has no footer).
    fn facts_of_open(&self) -> Option<(FileFormat, crate::readers::Facts)> {
        let hive = self
            .source
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

    /// What the footer says while a query's first rows are read over the frame on
    /// screen, from the read's job record. The rows drawn meanwhile are the view it
    /// replaces, under columns it may have changed.
    pub(crate) fn query_reading(&self) -> Option<&str> {
        let run = self.prompt.query_running.as_ref()?;
        let frame = self.data_table_state.as_ref()?.len_generation();
        if !matches!(run.origin, RunOrigin::Query(_)) || run.frame != frame {
            return None;
        }
        self.jobs
            .waiting_status(|job| Self::reading_rows(job) || Self::owed_rows(job))
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
            && self.analysis_modal.quality.setup_note.as_deref() == Some(QUALITY_RUN_WAITS)
        {
            self.analysis_modal.quality.setup_note = None;
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
        match (job, answer) {
            (Job::JournalDetail { dataset }, Answer::JournalDescribed(Some(detail))) => {
                if dataset == self.dataset_generation
                    && let Some(state) = self.data_table_state.as_mut()
                {
                    state.set_format_detail(*detail);
                }
                None
            }
            (Job::FootersJoin { dataset }, Answer::FootersJoined(found)) => {
                self.footers_joined(dataset, found.map(|found| *found))
            }
            (Job::Load(load), Answer::Load(answer)) => {
                // The open's to judge, by its own identity rather than the generation: an
                // answer for an open given up or replaced, or for a phase it has left,
                // changes nothing on screen, and what it carries — a download's file, a
                // dataset — is dropped with it.
                let step = self.loading.answered(
                    load,
                    *answer,
                    #[cfg(any(feature = "http", feature = "cloud"))]
                    &self.jobs,
                );
                self.run_load_step(step)
            }
            (
                Job::OpenNamed(load),
                Answer::NamedPaths {
                    paths,
                    options,
                    directory,
                },
            ) => {
                // The user left the open while its paths were looked at, or another took
                // its place.
                if !self.loading.looking_at_paths(load) {
                    return None;
                }
                // Either carries the same open on: it is still starting.
                Some(match directory {
                    Some(dir) => AppEvent::LookThenOpenDirectory(dir, *options),
                    None => AppEvent::Open(paths, *options),
                })
            }
            (Job::OpenNamed(load), Answer::NamedPathMissing(path)) => {
                if !self.loading.looking_at_paths(load) {
                    return None;
                }
                // The session ends saying so; nothing is opened.
                if let Some(retired) = self.loading.retire() {
                    self.put_down_load(retired);
                }
                Some(AppEvent::NamedPathMissing(path))
            }
            (
                Job::LookAtDirectory { load, path },
                Answer::LookedAt {
                    kind,
                    holds,
                    options,
                },
            ) => {
                // The user pressed Ctrl+O and went to the home screen, a newer look
                // replaced this one, or another open took its place while this was
                // reading. Their choice is the one on screen, and this is the answer to a
                // question nobody is waiting for.
                if !self.loading.looking_at_directory(load) {
                    return None;
                }
                // An `Open` that follows carries the same open on.
                self.open_the_directory_looked_at(path, kind, holds.as_deref(), *options)
            }
            (Job::Classify(asked), Answer::Kind(found)) => {
                // Superseded: a newer look, a trip away from home, or something that took
                // the screen over owns the wait, so this one touches nothing.
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
            (Job::Rows(inflight), Answer::Rows(result)) => {
                // A stale page is dropped; the wait belongs to whatever replaced it.
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
            (
                _,
                Answer::RowsFailed {
                    message,
                    conversion,
                },
            ) => {
                self.rows_failed(current, waited, &message, conversion.as_deref());
                None
            }
            (_, Answer::Analysis(install, results)) => {
                if current {
                    install(&mut self.analysis_modal, results);
                    self.analysis_modal.computing = None;
                }
                None
            }
            (
                _,
                Answer::DataQuality {
                    results,
                    kept,
                    plan,
                },
            ) => {
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
                    self.analysis_modal.quality.last_plan = Some(*plan);
                    self.analysis_modal.quality.results = Some(*results);
                    self.analysis_modal.quality.from_cache = false;
                    self.analysis_modal
                        .set_quality_page(crate::data_quality::QualityPage::Overview);
                    self.analysis_modal.computing = None;
                }
                None
            }
            (job, Answer::SampleDrawn(drawn)) => self.sample_drawn(job, current, drawn),
            (_, Answer::Sample { df, label }) => {
                if current {
                    self.analysis_modal.computing = None;
                    self.show_sample_view(df, label);
                }
                None
            }
            (_, Answer::Pivoted { spec, pivoted }) => {
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
            (Job::ReshapePreview { epoch, token }, Answer::ReshapePreviewed { input, result }) => {
                self.reshape_preview_ended(epoch, token, input, result);
                None
            }
            (Job::ViewPivot(pivot), Answer::ViewPivoted(pivoted)) => {
                // Superseded means the view was cancelled or something replaced it, which
                // owns the wait.
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
            (_, Answer::DrillRow { group_index, row }) => {
                // Superseded means something replaced the view, which owns the wait.
                if current {
                    self.drill_into(group_index, &row);
                }
                None
            }
            (Job::InspectRow { frame, row }, Answer::FieldsRead(values)) => {
                // Superseded means something replaced the view, which owns the wait.
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
            (Job::InspectJson { token }, Answer::JsonParsed(root)) => {
                // Superseded means something replaced the view, which owns the wait.
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
            (Job::InspectPretty { token }, Answer::Indented(text)) => {
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
            (Job::InspectUnpack { token }, Answer::Unpacked(decoded)) => {
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
            (_, Answer::ValueWritten(open)) => {
                if current && self.inspector_modal.active {
                    self.external.open = Some(open);
                }
                None
            }
            (_, Answer::Exported(path)) => {
                // Written: the dialog held for a failure is done with.
                self.export_modal.close();
                self.export_counts = None;
                if current {
                    self.export_progress = None;
                    self.flash_path("Exported to ", &path);
                }
                None
            }
            (_, Answer::Copied { payload, message }) => {
                if current {
                    self.export_progress = None;
                    self.finish_copy(payload, message);
                }
                None
            }
            (_, Answer::QualityReportWritten(path)) => {
                self.analysis_modal.quality.export = None;
                if current {
                    self.flash_path("Report written to ", &path);
                }
                None
            }
            (Job::ChartPrepare(prep), Answer::ChartPrepared(prepared)) => {
                self.chart_prepared(*prep, current, Ok(*prepared));
                None
            }
            (Job::ChartExport { path, format }, Answer::ChartExported) => {
                // Leaving the chart's dataset supersedes the write: one that finishes
                // after Ctrl-O must not reopen its modal over the home screen.
                if current {
                    self.finish_chart_export(&path, format, Ok(()));
                }
                None
            }
            (Job::FileFacts { dataset }, Answer::FileFacts(facts)) => {
                self.file_facts_landed(dataset, facts);
                None
            }
            (Job::UnfitCount { dataset, version }, Answer::UnfitCounted(unfit)) => {
                // Every value fitting says nothing in the Notes; the log says it ran.
                let columns: Vec<&str> = unfit.iter().map(|u| u.column.as_str()).collect();
                let said = if columns.is_empty() {
                    "none".to_string()
                } else {
                    columns.join(", ")
                };
                log::debug!(target: "datui", "values column types made null, by column: {said}");
                if dataset == self.dataset_generation
                    && let Some(state) = self.data_table_state.as_mut()
                {
                    match version {
                        None => state.unfit_counted(&unfit),
                        Some(version) => state.changes_unfit_counted(version, &unfit),
                    }
                }
                None
            }
            (Job::Find(run), Answer::Found(found)) => {
                self.find_answered(run, current, found);
                None
            }
            (job, Answer::HexOpened(source)) => {
                self.hex_opened(job, current, *source);
                None
            }
            (job, Answer::HexFound(hit)) => {
                self.hex_found(job, current, hit);
                None
            }
            (_, Answer::ValueCounts(counts)) => {
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
            (_, Answer::Probe(held)) => {
                drop(held);
                None
            }
            // Each answer is the one its job asks for; another is dropped.
            _ => None,
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
            // A pass that failed could not read them: the dataset stops waiting.
            Job::FootersJoin { dataset } => {
                self.footers_joined(*dataset, None);
            }
            Job::ChartPrepare(prep) => {
                let message = if panicked {
                    "Chart preparation panicked".to_string()
                } else {
                    message.to_string()
                };
                self.chart_prepared(*prep.clone(), current, Err(message));
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
                    match self.analysis_modal.quality.export.as_mut() {
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
            // The Info tab keeps what the open read.
            Job::JournalDetail { .. } => {}
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
                .info
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
            .source
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
            self.info.file_facts = Some((dataset, facts));
        }
    }

    /// What the Info panel knows about the open file: `None` until it is asked, and
    /// for a source with no file on this machine. Installing a dataset clears it, so
    /// what is here is the open dataset's.
    pub fn file_facts(&self) -> Option<&FileFacts> {
        Self::facts_shown(
            &self.info.file_facts,
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
            self.home_app.last_load_error = None;
            self.enter_home();
        } else {
            self.home_app.last_load_error = Some(message.clone());
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
        if let Some(generation) = self.counting.count_after_paint.take() {
            if self.counting.len_count_inflight == Some(generation) {
                self.counting.len_count_inflight = None;
            }
            if self
                .data_table_state
                .as_ref()
                .is_some_and(|state| state.len_generation() == generation)
            {
                self.counting.len_count_failed = Some(generation);
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

    /// Above this estimated size a table copy asks first: most paste targets
    /// choke long before it, and the clipboard holds the whole thing at once.
    const COPY_CONFIRM_BYTES: usize = 10 * 1024 * 1024;
    /// Above this a table copy is refused outright; a file is the medium for
    /// data this size, and export writes one without holding it all in text.
    const COPY_REFUSE_BYTES: usize = 200 * 1024 * 1024;

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
                self.chart.modal.series_cap = Some(self.theme.series_colors().len());
                self.app_config.theme = next;
                // The prompts live as long as the app and keep the colors they were
                // given; a dialog's fields take the theme each time it opens.
                for input in [
                    &mut self.prompt.query_input,
                    &mut self.prompt.sql_input,
                    &mut self.prompt.find.input,
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
        std::mem::take(&mut self.display.background_query)
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
        self.external.open.take()
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
            self.display.table_cell_padding,
            self.display.column_colors,
            self.display.number_format.clone(),
        )
        .with_dtype_row(self.display.dtype_row);

        let main_view_content = MainViewContent::current(self);

        Clear.render(area, buf);
        let background_color = self.theme.background();
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
        self.counting.footer_progress.cancel();
        // The indexing stops, and the reads waiting on it give up, so nothing holds
        // the file once the app is gone (the Python binding runs on in the process).
        self.counting
            .indexing_stop
            .store(true, std::sync::atomic::Ordering::Relaxed);
        if let Some(lines) = self.counting.indexing_lines.take() {
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

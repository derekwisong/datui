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

pub mod analysis;
pub mod app;
pub mod cache;
pub mod canonical;
pub mod chart;
pub mod cli;
pub mod clipboard;
pub mod cloud;
pub mod commands;
pub mod config;
pub mod error_display;
pub mod exact;
pub mod export;
pub mod find;
pub mod formats;
pub mod glyphs;
pub mod home;
pub mod inspector;
pub mod limits;
pub mod loading;
pub mod logging;
pub mod notes;
pub mod numfmt;
pub use app::applied::Applied;
pub use app::overlay::Overlay;
pub mod past_calendar;
pub use app::run::{ended_by_signal, run, run_captured};
// Public for the fuzz targets in `fuzz/`.
pub mod query;
mod render;
pub mod sanitize;
pub mod table;
pub mod typed_value;
pub mod view;
pub mod widgets;

pub use cache::CacheManager;
pub use cli::Args;
pub use config::{
    AppConfig, ColorParser, ConfigManager, QueryMode, Theme, ThemeMode, rgb_to_256_color,
    rgb_to_basic_ansi,
};

use analysis::analysis_modal::{AnalysisModal, AnalysisProgress};
use app::background::{CacheWrites, InflightCollect, LenCount, OwedCount};
use chart::chart_export::ChartExportRequest;
use chart::chart_export_modal::ChartExportModal;
use chart::chart_jobs::ChartRequest;
use chart::chart_modal::ChartColumns;

pub use analysis::quality_memory::{KeptQualitySample, QUALITY_MEMORY_BUDGET, RetainedCopy};
use app::feedback::Confirm;
pub use app::feedback::{ConfirmationModal, ErrorModal, Flash};
use app::jobs::{Answer, Job, Jobs, Outcome};
pub use app::jobs::{JobKind, Progress, Ticket};
use app::modals::filter_modal::{FilterOperator, FilterStatement, LogicalOperator};
use app::modals::pivot_melt_modal::PivotMeltModal;
use app::modals::sort_filter_modal::SortFilterModal;
use app::modals::sort_modal::{SortColumn, order_with_hidden};
pub use error_display::{ErrorKindForPython, error_for_python};
use export::export_modal::{ExportFocus, ExportModal};
use export::output_file::Overwrite;
pub use export::{ExportOptions, ExportRequest};
pub use loading::open_options::{
    OpenOptions, ParseStringsTarget, ReadReport, SqliteOpen, TypedDialect, UnaskedDownload,
};
pub use loading::unfinished::ExitSweep;
use numfmt::NumberFormatSettings;
use table::{DataTableState, DrillRow};
pub use view::{SavedView, ViewManager, Views};
use widgets::column_widths::WidthChoice;
use widgets::debug::DebugState;
use widgets::text_input::TextInput;
use widgets::view_modal::{FormFocus, ViewModal, ViewModalMode};

/// Application name, used for cache and config paths.
pub const APP_NAME: &str = "datui";

/// What a file no reader takes, and no hex view can show, is told.
pub(crate) const UNSUPPORTED: &str =
    "Unsupported file type. --format names the format to read it as.";

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
    /// A key taken as if typed (Enter on a help line). The pump runs it through
    /// `classify` like a typed key; outside the pump it is a `Key`.
    Press(KeyEvent),
    /// Text the terminal pasted (bracketed paste), taken as one edit by the field that
    /// takes typed text (`App::paste`).
    Paste(String),
    /// Read from the terminal by [`app::terminal_input::TerminalInput`]. The
    /// [`app::event_pump::EventPump`] turns it into `Key`/`Resize`.
    Terminal(crossterm::event::Event),
    /// Something polled rather than sent changed (a background panic, a Polars
    /// warning): wakes the loop. Handled as nothing.
    Wake,
    /// The terminal's background (an OSC 11 reply); `theme.mode = "auto"` follows it.
    TerminalBackground(ThemeMode),
    /// The terminal regained focus: under `auto` the background is asked again.
    TerminalFocused,
    /// Settings `run` reads on a worker before building the app; never reaches the app.
    SettingsRead(Box<Result<app::startup::Settings>>),
    /// Paths from the command line or the Python binding, checked on a worker
    /// ([`JobKind::OpenNamed`]): a local-looking path may be a slow mount.
    OpenNamed(Vec<PathBuf>, OpenOptions),
    /// A path named on the command line is not there: the session ends.
    NamedPathMissing(PathBuf),
    /// Open these paths, through the loading controller's phases.
    Open(Vec<PathBuf>, OpenOptions),
    /// Open with an existing LazyFrame (e.g. from Python binding); no file load.
    OpenLazyFrame(Box<LazyFrame>, OpenOptions),
    /// A home listing built off-thread is ready.
    HomeListingReady {
        generation: u64,
        listing: Box<crate::home::Listing>,
        /// What earlier runs measured, when this listing read the cache's index: once a
        /// session.
        known: Option<crate::home::Known>,
        /// Records read since for the dataset just left, which its open wrote.
        learned: Vec<(PathBuf, crate::cache::DatasetFacts)>,
        /// How often and how lately each recent was opened.
        visits: std::collections::HashMap<PathBuf, crate::cache::Visits>,
        /// The recent opened last, where the cursor lands.
        newest: Option<PathBuf>,
        /// The saved folds, when entering the home screen asked for them.
        folds: Option<std::collections::HashMap<String, bool>>,
    },
    /// The home listing worker panicked; the panic is flashed.
    HomeListingFailed,
    /// The directory the `~` prompt is typing, read off-thread.
    HomePathListed {
        listing: Box<crate::home::PathListing>,
    },
    /// A completed path, worked out off-thread.
    HomePathCompleted {
        generation: u64,
        /// What was typed when completion was asked; a later keystroke makes it stale.
        typed: String,
        completed: String,
        candidates: usize,
    },
    /// The highlighted file's first rows, read as its open reads them, and the
    /// dataset that read built, for the open to install.
    HomePreviewReady {
        path: PathBuf,
        /// The row's stamp when it was asked for: what the rows are kept under.
        stamp: crate::home::home_preview::Stamp,
        /// The file's stamp when it was read: what the dataset is installed under.
        read_at: Option<crate::home::home_preview::Stamp>,
        rows: Option<Arc<crate::home::home_preview::PreviewRows>>,
        prepared: crate::home::home_preview::Handoff,
    },
    /// A schema read off-thread for the highlighted dataset.
    HomeSchemaReady {
        path: PathBuf,
        preview: Option<crate::home::discover::SchemaPreview>,
    },
    /// Measurements for home rows, a batch at a time (more often on a slow share).
    /// `done` ends the batch. No generation: a measurement is keyed by path and
    /// stays true whichever listing asked.
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
    /// What the rows on screen turned out to be; folded like
    /// [`AppEvent::HomeMeasured`]. `pass` is the filesystem (its mount point) the
    /// pass looked at.
    HomeClassified {
        pass: PathBuf,
        measured: Vec<(PathBuf, crate::home::Measured)>,
        done: bool,
    },
    /// A batch of datasets from the background search, sent repeatedly while it walks.
    HomeSearchBatch {
        generation: u64,
        root: PathBuf,
        found: Vec<crate::home::discover::Entry>,
        scanned: usize,
    },
    /// The filter scored against the search's files, for the walk `epoch` names.
    HomeSearchScored {
        epoch: u64,
        /// `None` from a worker that died.
        matches: Option<Box<crate::home::search::Matches>>,
    },
    /// The background search stopped; `limited` says why if it stopped short.
    HomeSearchDone {
        generation: u64,
        root: PathBuf,
        scanned: usize,
        limited: Option<String>,
    },
    /// The cloud sources on this machine with what an earlier run listed, sent before
    /// anything is fetched.
    #[cfg(feature = "cloud")]
    HomeCloudSources {
        sources: Vec<crate::home::CloudSource>,
    },
    /// One source's buckets listed, or not. Each source reports on its own.
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
        rows: Option<Vec<crate::home::discover::Entry>>,
        /// The listing stopped at [`crate::home::discover::MAX_ENTRIES_PER_DIR`].
        cut_short: bool,
    },
    /// Rows of a network directory read since its last batch.
    HomeProbeProgress {
        root: PathBuf,
        rows: Vec<crate::home::discover::Entry>,
    },
    /// Cloud directories that peeking found to be partitioned or Parquet datasets.
    HomeCloudKinds {
        kinds: Vec<(
            PathBuf,
            (
                crate::home::discover::EntryKind,
                crate::home::discover::Holds,
            ),
        )>,
        /// Directories whose peek failed or was lost, so left unlabeled.
        failed: Vec<PathBuf>,
    },
    /// A cloud listing stopped because its place was left; it is listed again on
    /// return.
    HomeProbeCancelled {
        root: PathBuf,
    },
    /// Names under `prefix` in a cut-short cloud directory, asked for by a filter;
    /// `None` when the listing failed or was stopped.
    HomeNarrowed {
        dir: PathBuf,
        prefix: String,
        listed: Option<(Vec<crate::home::discover::Entry>, bool)>,
    },
    /// A cloud listing was refused, with the service's reason.
    HomeProbeFailed {
        root: PathBuf,
        message: String,
    },
    /// A followed file's watcher found more rows, or that the file went.
    Followed(crate::loading::follow::News),
    Exit,
    Crash(String),
    /// A dialog or prompt answered: what it asks of the app. See [`Applied`].
    Applied(Applied),
    Collect,
    Update,
    Reset,
    Resize(u16, u16), // resized (width, height)
    /// A scroll deferred one frame, so the spinner shows while its rows are read.
    Scroll(Scroll),
    /// Run an analysis tool off the UI thread; deferred so its progress shows first.
    AnalysisCompute(analysis::analysis_modal::AnalysisTool),
    /// The sample a stopped Data Quality run had read, kept for the next run.
    BackgroundQualitySampleKept {
        kept: KeptQualitySample,
    },
    /// A full scan's local copy of a remote dataset, kept for that dataset. `None`
    /// when the copy did not read as the source.
    BackgroundQualityCopyKept {
        dataset_generation: u64,
        copy: Option<Arc<crate::cloud::local_copy::LocalCopy>>,
    },
    /// The exact row count for the current LazyFrame, applied only if
    /// `len_generation` still matches. Runs alongside the first paint, never
    /// blocking it.
    BackgroundLenReady {
        len_generation: u64,
        num_rows: usize,
        /// For a remote multi-file dataset, each file's row-group sizes from its footer.
        file_row_groups: Option<Vec<Vec<usize>>>,
    },
    /// The row count failed: the total stays unknown. Scrolling does not count
    /// again; End does.
    BackgroundLenFailed {
        len_generation: u64,
    },
    /// A frame was painted. The run loop calls [`App::frame_painted`]; a harness
    /// sends this when [`App::count_waits_for_a_frame`].
    FramePainted,
    /// A directory named on the command line: look at it on a worker, then do what
    /// `Enter` on its row would. An event so the first frame, with a spinner and a
    /// way out, is drawn before a look that can take seconds.
    LookThenOpenDirectory(PathBuf, OpenOptions),
    /// Look at a path off the UI thread, then browse into it, report a lake table, or
    /// open it. The filesystem calls can hang on a slow mount.
    ClassifyThenOpen {
        path: PathBuf,
        /// A path typed at `~` rather than a listed row: Esc returns to the listing.
        jump: bool,
    },
    /// A background job's outcome is in its record: `app::jobs::Jobs::end` takes it.
    JobEnded(Ticket),
    /// A report from a background job still running.
    JobProgress {
        ticket: Ticket,
        progress: Progress,
    },
}

impl AppEvent {
    /// A report sent many times while work runs: the loop may fold several into one
    /// frame. A measuring batch's end too: batches of tiny files end a few a
    /// millisecond, and a frame each cost more than the measuring.
    pub fn is_progress(&self) -> bool {
        matches!(
            self,
            AppEvent::HomeMeasured { .. }
                | AppEvent::HomeClassified { .. }
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
    dyn Fn(&Path, Option<crate::formats::readers::Facts>) -> std::result::Result<FileFacts, String>
        + Send
        + Sync,
>;

/// What [`App::handle`] did: `Ok` carries a follow-up event; `Err` returns a key
/// that arrived while busy, untouched, for the caller to offer again when idle.
pub type EventOutcome = Result<Option<AppEvent>, KeyEvent>;

/// What <kbd>Enter</kbd> will do on the highlighted row, for the footer and the
/// details pane. A prediction of `App::home_open_selected`;
/// `test_the_bar_says_what_enter_will_really_do` keeps the two in agreement.
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
            // A place browses as `→` does; an HTTP place has no listing and says so.
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
            None => return WhatEnter::Nothing,
            // The door reads its directory whatever its label, lake tables included.
            Some(home::Row::Door { .. }) => return WhatEnter::OpensDirectory,
            Some(home::Row::Entry { entry, .. }) => entry,
        };
        // A bookmark opens whole.
        if entry.kind != home::discover::EntryKind::File
            && self.home.bookmark(&entry.path).is_some()
        {
            return WhatEnter::OpensDirectory;
        }
        match entry.kind {
            home::discover::EntryKind::Unknown => WhatEnter::LooksFirst,
            // A database of several tables lists them.
            home::discover::EntryKind::File if entry.enter_lists_tables() => WhatEnter::GoesInside,
            home::discover::EntryKind::File => WhatEnter::OpensFile,
            home::discover::EntryKind::Other
                if matches!(
                    cloud::source::input_source(&entry.path),
                    cloud::source::InputSource::Local(_)
                ) =>
            {
                WhatEnter::OpensHex
            }
            home::discover::EntryKind::Other => WhatEnter::Nothing,
            home::discover::EntryKind::Hive | home::discover::EntryKind::MultiFile => {
                WhatEnter::OpensDirectory
            }
            // A plain directory, and a lake table, whose files are not its rows.
            home::discover::EntryKind::Directory
            | home::discover::EntryKind::Delta
            | home::discover::EntryKind::Iceberg
            | home::discover::EntryKind::Hudi => WhatEnter::GoesInside,
        }
    }
}

impl App {
    /// Whether `download` is held because the dataset came from a remote `path`;
    /// local stream conversions and stdin spools are held the same way.
    fn was_fetched(
        download: Option<&crate::cloud::download::TempDownload>,
        path: Option<&Path>,
    ) -> bool {
        download.is_some() && path.is_some_and(cloud::source::is_remote_url)
    }
}

/// Input for the shared run loop.
#[derive(Clone)]
pub enum RunInput {
    /// The command line as parsed; config is read behind the first frame
    /// ([`app::startup`]).
    Cli(Box<Args>),
    /// A host program's options, read as the command line's (`-c` included), with a
    /// frame to show instead of its paths. Stdin is the host's, never data.
    Host(Box<Args>, Option<Box<LazyFrame>>),
    Paths(Vec<PathBuf>, OpenOptions),
    LazyFrame(Box<LazyFrame>, OpenOptions),
}

/// The screen keys go to when no overlay is open (see [`Overlay`]).
#[derive(Debug, Default, PartialEq, Eq)]
pub enum InputMode {
    #[default]
    Normal,
    /// The home screen: pick a dataset to open, at startup or from a session.
    Home,
    /// The command line or the find line.
    Editing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputType {
    /// The command line (`:`): a row number, or a query in SQL or q.
    Query,
    Find,
}

/// A query whose first rows are being read. It can still fail on the data (a
/// value that will not cast); until the rows are in, the replaced view is kept.
struct QueryRun {
    origin: RunOrigin,
    /// The `len_generation` of the query's frame; once that frame is gone the
    /// rollback no longer applies.
    frame: u64,
    rollback: crate::table::ViewRollback,
    /// The count markers for the frame the rollback restores. A count of it still
    /// running lands into `rollback`.
    counts: loading::counting::CountMarkers,
    /// Rows `df` holds, when known, so a failure can say "of N".
    rows: Option<usize>,
}

/// Where a running query came from, which decides where its failure is said.
enum RunOrigin {
    /// The query prompt: inline under it while open in this mode, else a dialog.
    Query(QueryMode),
    /// A view applied: a dialog on failure, and the previous view marked applied
    /// again. `matched` says why it was applied for a match.
    View {
        previous: Option<String>,
        matched: Option<(String, view::MatchReason)>,
    },
}

/// The bar's recording label (`--tee`): `rec` with size and rate, `saved` with
/// size, length and file (`sent` for `--tee -`), or `stopped` and why. The bool
/// is true for that last (warning color).
fn recording_label(spool: &crate::loading::follow::Spool) -> (String, bool) {
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

/// An export under way, for the footer.
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

/// The correlation matrix of the sample's numeric columns, reading only those.
fn correlations_of_sample(
    lf: &LazyFrame,
    sample: &analysis::sampling::Sample,
    known_total: Option<usize>,
    streaming: bool,
) -> Result<crate::analysis::statistics::AnalysisResults> {
    let schema = lf.clone().collect_schema()?;
    let numeric: Vec<polars::prelude::Expr> = schema
        .iter()
        .filter(|(_, dtype)| dtype.is_numeric())
        .map(|(name, _)| col(name.clone()))
        .collect();
    let rows = crate::analysis::sampling::read(
        &lf.clone().select(numeric),
        sample,
        known_total,
        streaming,
    )?;
    Ok(crate::analysis::statistics::AnalysisResults {
        column_statistics: vec![],
        total_rows: rows.total_rows,
        sample_size: rows.sample_size,
        per_value: rows.per_value.map(|per_value| per_value.kept),
        correlation_matrix: crate::analysis::statistics::compute_correlation_matrix(&rows.df).ok(),
        distribution_analyses: vec![],
    })
}

/// Returns (query, sql_query, fuzzy_query) with only the active one set: SQL over
/// fuzzy over DSL. A saved view keeps one.
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

/// The steps `state` shows, as a saved view keeps them.
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

/// The sample `state` is, as a view keeps it, with the query and filters it was
/// drawn through when drawn from the view's rows.
fn saved_sample_of(state: &DataTableState) -> Option<view::SavedSample> {
    let sampled = state.sampled()?;
    // Column types, a reshape and a sort pick its rows as much as a query does.
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
    /// Stopped at the pivot, which must be read before later steps can be planned.
    Pivot(Box<crate::table::PivotJob>),
}

/// Why Data Quality's Run did not start: a cancelled run has not exited yet.
const QUALITY_RUN_WAITS: &str = "Run waits: the cancelled run is still stopping";

/// Why another source read did not start: a second read beside a cancelled one
/// can run memory out.
const ANALYSIS_READ_WAITS: &str = "A cancelled run is still finishing; try again shortly";

/// How long a cancelled run may take to finish its batch before the screen says
/// it is still going.
const CANCEL_GRACE: std::time::Duration = std::time::Duration::from_secs(1);

pub struct App {
    pub data_table_state: Option<DataTableState>,
    /// The dataset's row count, footer pass and line indexing, and what waits on them.
    counting: loading::counting::Counting,
    /// The home screen's work in flight, and what it keeps for the session.
    pub home_app: home::home_app::HomeApp,
    /// Home screen state, rebuilt whenever home is entered.
    pub home: home::HomeState,
    /// Where the dataset on screen came from, and how it was opened.
    source: loading::open_scan::OpenedSource,
    path: Option<PathBuf>,
    /// Standard input and output when datui sits in a pipe.
    pipes: app::run::Pipes,
    events: Sender<AppEvent>,
    debug: DebugState,
    pub info_modal: InfoModal,
    /// What the Info panel shows of the dataset beyond its schema.
    pub info: app::keys::info_keys::InfoState,
    /// The command line: its inputs per mode, completion, and the query it is running.
    pub prompt: query::query_prompt::QueryPrompt,
    pub input_mode: InputMode,
    /// What is open over the table. See [`Overlay`].
    pub overlay: Overlay,
    pub sort_filter_modal: SortFilterModal,
    pub pivot_melt_modal: PivotMeltModal,
    pub view_modal: ViewModal,
    pub analysis_modal: AnalysisModal,
    /// The sample form, and what the draws learned of memory and of the paths they took.
    pub sample: analysis::sample_draw::SampleState,
    /// What Data Quality runs keep within the memory budget.
    quality: analysis::quality_runs::QualityRuns,
    /// The chart view, its export form, and the preparations it keeps or waits on.
    pub chart: chart::chart_jobs::Charts,
    pub export_modal: ExportModal,
    pub copy_modal: app::modals::copy_modal::CopyModal,
    pub inspector_modal: inspector::inspector_modal::InspectorModal,
    /// What datui hands to other programs, and the clipboard.
    external: app::run::External,
    /// The go-to-column, format and table pickers.
    pub pickers: app::keys::picker_keys::Pickers,
    /// The Value Counts screen (`F`).
    pub value_counts: analysis::value_counts_modal::ValueCountsModal,
    /// The retype and combine forms.
    pub column_forms: app::keys::retype_keys::ColumnForms,
    /// The hex view, and the number its next read is tagged with.
    pub hex: app::keys::hex_keys::HexState,
    error_modal: ErrorModal,
    flash: Option<Flash>,
    pub confirmation_modal: ConfirmationModal,
    /// The help overlay, over whatever screen it was opened at.
    help: app::help::Help,
    /// What the mouse can land on in the last frame, and the last click.
    pointer: app::pointer::Pointing,
    /// The menu a right click on a cell opened, while it is open.
    context_menu: Option<app::context_menu::ContextMenu>,
    cache: CacheManager,
    /// The recent and the shape an open writes, which the home listing waits on.
    cache_writes: CacheWrites,
    /// Saved views, and the one applied to the dataset on screen.
    views: view::view_apply::SavedViews,
    /// An export under way, which the footer reports.
    export_progress: Option<ExportProgress>,
    theme: Theme, // Color theme for UI rendering
    /// How the table is drawn this session: from the config, with the session's own toggles.
    display: render::context::DisplaySettings,
    runtime: tokio::runtime::Handle, // Tokio runtime handle for background tasks
    /// Background jobs and the generation their answers are judged by. See [`app::jobs`].
    jobs: Jobs,
    /// The open in flight, from request to first rows. See [`loading`].
    loading: loading::Loader,
    /// Bumped once per dataset put on screen; the jobs' generation also moves per
    /// collect, and a footer pass outlives several. Says whether arriving columns
    /// belong to the dataset on screen.
    dataset_generation: u64,
    /// Replaces [`FileFacts::read`] in tests of a slow or failing read.
    #[cfg(test)]
    file_facts_reader: Option<FileFactsReader>,
    /// Show the throbber and hold keys (see [`App::handle`]).
    busy: bool,
    /// Bumped when the screen is replaced without the user asking (going home, an
    /// abandoned load). Held keys carry the value they were typed under and are
    /// dropped once it moves.
    screen_generation: u64,
    /// The main loop dropped a key typed while busy; cleared once held keys replay.
    input_dropped: bool,
    /// The spinner's frame, counting up; each spinner takes it modulo its own frames.
    throbber_frame: u8,
    /// Footer status at the table view, shown even when not busy: an End waiting on a
    /// remote count parks without setting `busy`.
    status_message: Option<String>,
    app_config: AppConfig,
    /// The format specs on the search path, read when the app was built.
    formats: Arc<crate::formats::Registry>,
}

impl App {
    /// Whether keys wait: a job the user waits on runs or is owed, an errand is
    /// between phases, or an open is on its way.
    pub fn is_busy(&self) -> bool {
        self.busy || self.jobs.holds_keys() || self.loading.waits()
    }

    /// The generation background answers are judged by.
    pub fn task_generation(&self) -> u64 {
        self.jobs.generation()
    }

    /// Whether the job `ticket` names is still running and its answer still wanted.
    pub fn job_is_current(&self, ticket: Ticket) -> bool {
        self.jobs.is_current(ticket)
    }

    /// Use `cache` from now on. For tests that read their store back: a shared one
    /// loses entries to other tests' opens.
    pub fn use_cache(&mut self, cache: CacheManager) {
        self.cache = cache;
    }

    /// Read and write `catalog.toml` in `dir`, so a test's Ctrl+D stays out of the
    /// shared config directory.
    pub fn use_catalog_dir(&mut self, dir: &Path) -> Result<()> {
        self.app_config.read_catalog_files(Some(dir))
    }

    /// Path of the installed dataset, if any.
    pub fn open_path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    /// Whether the dataset on screen was piped in.
    fn reads_stdin(&self) -> bool {
        self.source.opened.as_ref().is_some_and(
            |(paths, _)| matches!(paths.as_slice(), [path] if loading::stdin::is_stdin(path)),
        )
    }

    /// What views are matched against: the path and table. Piped input or a handed
    /// frame is `-`, matching by columns alone.
    fn view_dataset(&self) -> Option<view::Dataset<'_>> {
        self.data_table_state.as_ref()?;
        let path = match self.path.as_deref() {
            Some(path) if !self.reads_stdin() => path,
            _ => Path::new(loading::stdin::PATH),
        };
        Some(view::Dataset {
            path,
            table: self.view_table(),
        })
    }

    /// The table within a file of tables: from `--table` or a path inside the file
    /// (`shop.db/orders`).
    fn view_table(&self) -> Option<&str> {
        let (_, options) = self.source.opened.as_ref()?;
        options.table.as_deref()
    }

    /// Wait for the cache writes in flight (recents, dataset facts) to land, as the
    /// home listing does. For tests.
    #[doc(hidden)]
    pub fn settle_cache_writes(&self) {
        self.cache_writes.settle();
    }

    /// Whether any leased background work, current or abandoned, has yet to report.
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
        self.analysis_modal.view == analysis::analysis_modal::AnalysisView::Main
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
    /// cancelled during an unstoppable read.
    fn cancelled_analysis(&self) -> Option<(std::time::Instant, bool)> {
        let (since, job) = self.jobs.cancelled_running(Self::is_analysis_read)?;
        let runs_out = match job {
            Job::Analysis(run) => run.runs_out,
            _ => true,
        };
        Some((since, runs_out))
    }

    /// A read for the Analysis tools: a run, or the sample read to show as a table.
    fn is_analysis_read(job: &Job) -> bool {
        matches!(job, Job::Analysis(_) | Job::SampleRows)
    }

    /// A cancelled run the screen should show as still going: at once when cancelled
    /// during an unstoppable read, else once it outlasts its batch.
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
    fn spawn_analysis(&mut self, tool: analysis::analysis_modal::AnalysisTool) -> Option<AppEvent> {
        use analysis::analysis_modal::AnalysisTool;
        type Compute = fn(
            &LazyFrame,
            &analysis::sampling::Sample,
            Option<usize>,
            bool,
        ) -> Result<crate::analysis::statistics::AnalysisResults>;
        type Install = fn(
            &mut analysis::analysis_modal::AnalysisModal,
            crate::analysis::statistics::AnalysisResults,
        );
        let (status, compute, install): (&str, Compute, Install) = match tool {
            AnalysisTool::DataQuality => return self.run_quality_compute(),
            AnalysisTool::Describe => (
                "Running analysis...",
                |lf, sample, known, streaming| {
                    crate::analysis::statistics::compute_describe_from_lazy(
                        lf, known, sample, streaming,
                    )
                },
                |modal, results| modal.describe_results = Some(results),
            ),
            AnalysisTool::DistributionAnalysis => (
                "Analyzing distributions...",
                |lf, sample, known, streaming| {
                    let options = crate::analysis::statistics::ComputeOptions {
                        include_distribution_info: true,
                        include_distribution_analyses: true,
                        include_correlation_matrix: false,
                        include_skewness_kurtosis_outliers: true,
                        polars_streaming: streaming,
                    };
                    crate::analysis::statistics::compute_statistics_for_sample(
                        lf, sample, known, options,
                    )
                },
                |modal, results| modal.distribution_results = Some(results),
            ),
            AnalysisTool::CorrelationMatrix => (
                "Computing correlation matrix...",
                correlations_of_sample,
                analysis::analysis_modal::AnalysisModal::install_correlations,
            ),
        };
        let Some(state) = &self.data_table_state else {
            self.analysis_modal.computing = None;
            self.busy = false;
            return None;
        };
        // Binary columns are stubbed by the source: multi-GB blobs could exhaust memory.
        let (source, known_total) = self.sample_source(state);
        let streaming = match tool {
            AnalysisTool::CorrelationMatrix => state.polars_streaming(),
            _ => self.app_config.performance.streaming,
        };
        let sample = self.analysis_modal.sample.clone();
        self.spawn_job(
            Job::Analysis(app::jobs::AnalysisRun::default()),
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
        if tool != analysis::analysis_modal::AnalysisTool::DataQuality
            && self.read_waits_for_cancelled()
        {
            return None;
        }
        let phase = match tool {
            analysis::analysis_modal::AnalysisTool::Describe => {
                self.analysis_modal.describe_results = None;
                "Describing data"
            }
            analysis::analysis_modal::AnalysisTool::DistributionAnalysis => {
                self.analysis_modal.distribution_results = None;
                "Analyzing distributions"
            }
            analysis::analysis_modal::AnalysisTool::CorrelationMatrix => {
                self.analysis_modal.correlation_results = None;
                "Computing correlations"
            }
            analysis::analysis_modal::AnalysisTool::DataQuality => return None,
        };
        self.analysis_modal.computing = Some(AnalysisProgress::new(phase));
        self.busy = true;
        Some(AppEvent::AnalysisCompute(tool))
    }

    /// Stop waiting for the analysis in flight. The bump makes its answer stale; a
    /// Data Quality run stops at its next batch or stage, a plain collect runs out.
    /// The tool is put back unchosen so its view does not wait on a spinner.
    fn cancel_analysis(&mut self) {
        // The record stays running until the worker ends. Only Data Quality has a watch
        // to stop it and stages that say whether they stop partway.
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
        self.jobs.cancel(Self::is_analysis_read);
        self.jobs.advance();
        // Keys typed at the run are stale: a replayed second Enter would restart it.
        self.screen_generation = self.screen_generation.wrapping_add(1);
        self.analysis_modal.computing = None;
        self.busy = false;
        self.status_message = None;
        // Reading the sample to look at changed nothing on screen; the tool stays.
        if reading_sample {
            self.flash_note("Row view cancelled".to_string());
            return;
        }
        // Data Quality keeps its last report and returns to Setup. A read that runs to
        // its end is shown by the header and Setup until the worker exits; one that
        // stops at its next batch is done, and a flash says so.
        if self.analysis_modal.selected_tool
            == Some(analysis::analysis_modal::AnalysisTool::DataQuality)
        {
            self.open_quality_setup();
            if !read_runs_out {
                self.flash_note("Run cancelled".to_string());
            }
            return;
        }
        self.analysis_modal.selected_tool = None;
        self.analysis_modal.focus = analysis::analysis_modal::AnalysisFocus::Sidebar;
        self.flash_note("Analysis cancelled".to_string());
    }

    /// Whether the Pivot & Melt builder is waiting on a pivot it started.
    pub(crate) fn pivot_computing(&self) -> bool {
        self.overlay == Overlay::PivotMelt
            && self.jobs.current(|job| matches!(job, Job::Pivot)).is_some()
    }

    /// Stop waiting for the pivot in flight: the bump drops its answer; the form keeps
    /// its spec.
    fn cancel_pivot(&mut self) {
        self.jobs.advance();
        self.screen_generation = self.screen_generation.wrapping_add(1);
        self.busy = false;
        self.status_message = None;
        self.flash_note("Pivot cancelled".to_string());
    }

    /// Drill into the group on row `group_index` (values `row`), fetching its rows off
    /// the UI thread. A failure is said on the bar and leaves the grouped view.
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

    /// Pass the stream on to `out` for `--tee -` (stdout, or a test's pipe).
    #[doc(hidden)]
    pub fn pass_stdout_to(&mut self, out: impl std::io::Write + Send + 'static) {
        self.pipes.stdout_pass = Some(Box::new(out));
    }

    /// The follow of the dataset on screen, while it is followed.
    pub fn follow(&self) -> Option<&crate::loading::follow::Follow> {
        self.data_table_state.as_ref()?.follow()
    }

    /// Whether the follow's rows are on hand: no background read out and the view has
    /// taken what was counted. Refreshes hold no keys, so tests wait on this.
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

    /// A watcher report: counted rows wait for the view; news for the user is flashed.
    fn followed(&mut self, news: &crate::loading::follow::News) {
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

    /// Re-describe a piped journal's Info tab over every entry once it has ended.
    /// Nobody waits on it.
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
            crate::formats::journal::summary(&lf)
                .map(|detail| Answer::JournalDescribed(Box::new(detail)))
                .map_err(|e| e.to_string())
        });
    }

    /// Join fields a followed pipe brought after the open, if the dataset can take them
    /// now, and re-read the rows on screen. Retried after every event while waiting.
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

    /// Show the rows a follow counted, when the table is on screen with nothing running.
    /// A cursor on the last row stays there; elsewhere it stays put and the rows
    /// below are counted for the bar.
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

    /// Give the view the rows the follow counted, reading those on screen with `read`.
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
        // Move to the last row only once its rows are on hand; earlier, a frame drawn
        // meanwhile is shorter and the table clamps the cursor back.
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

    /// `t` over a surface that keeps its opening rows (Value Counts, Analysis, a chart):
    /// whether the follow has rows for it.
    fn follow_rows_waiting(&self) -> bool {
        self.follow().is_some_and(|f| f.behind()) && !self.loading.awaiting_dataset()
    }

    /// Standard input being recorded to the `--tee` file, for the dataset on screen.
    pub fn recording(&self) -> Option<&Arc<crate::loading::follow::Spool>> {
        self.data_table_state.as_ref()?;
        self.source
            .opened
            .as_ref()
            .and_then(|(_, options)| options.spool.as_ref())
            .map(|handle| handle.spool())
            .filter(|spool| spool.tee().is_some())
    }

    /// Where `key` takes the user out of the dataset: quitting, or home.
    fn leaving_by(&self, key: &KeyEvent) -> Option<Leaving> {
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

    /// Leave as asked, stopping the recording or keeping it until its stream ends.
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

    /// The recording to wait for after the terminal is handed back.
    pub fn recording_after_exit(
        &mut self,
    ) -> Option<(
        crate::loading::follow::Tee,
        Arc<crate::loading::follow::SpoolHandle>,
    )> {
        let handle = self.pipes.recording_on.take()?;
        let spool = handle.spool();
        let tee = spool.tee()?.clone();
        spool.live().then_some((tee, handle))
    }

    /// Say once that the recording ended, saved or stopped by an error. True when the
    /// frame must redraw.
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

    /// When the screen next changes with no event (a flash expiring): the loop sleeps
    /// until then at most.
    pub fn next_deadline(&self) -> Option<std::time::Instant> {
        let flash = self.flash.as_ref().map(|f| f.expires);
        let clock = self
            .follow()
            .filter(|f| f.standing == crate::loading::follow::Standing::Following)
            .and_then(|f| f.last_append)
            .map(crate::loading::follow::next_tick);
        // A recording's size and rate change every second until it ends.
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
        use crate::loading::follow::Standing;
        // The hex view shows a file's bytes, not the table the follow moves.
        if self.overlay == Overlay::Hex {
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
                        crate::loading::follow::age(at.elapsed())
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
        // What `t` does here: pause or resume at the table; over a surface that keeps its
        // rows, read the new ones.
        let refreshes = self.overlay == Overlay::ValueCounts
            || (self.overlay == Overlay::Chart && self.chart.modal.picker.is_none())
            || (self.overlay == Overlay::Analysis
                && self.analysis_modal.current_results().is_some());
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

    /// `t` at the table: pause or resume the follow, or start following (re-reading as
    /// `H` does).
    fn toggle_follow(&mut self) -> Option<AppEvent> {
        use crate::loading::follow::Standing;
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
        if paths
            .iter()
            .any(|path| crate::loading::stdin::is_stdin(path))
        {
            self.flash_note("Standard input is followed from the start: datui -f -".to_string());
            return None;
        }
        if let Some(refusal) = crate::loading::follow::refuse_paths(&paths, &options) {
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

    /// Flash the next Polars user warning once per session when the bar is free. True
    /// when the frame must redraw.
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

    /// Flash a background thread panic nothing else reported (a job's panic ends the
    /// job). True when the frame must redraw.
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

    /// Whether a flash would be seen: not under a busy message or a modal.
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

    /// Escapes that act at once while busy and jump the queue: Ctrl-Q and Ctrl-C quit,
    /// Ctrl-O goes home, confirmation modals keep their keys, and the home screen
    /// (never busy on its own account) keeps every key.
    pub fn hard_escape_while_busy(&self, key: &KeyEvent) -> bool {
        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        let quit = ctrl && matches!(key.code, KeyCode::Char('q' | 'c'));
        let home = ctrl && key.code == KeyCode::Char('o');
        let cancel_analysis = self.overlay == Overlay::Analysis
            && self.analysis_modal.computing.is_some()
            && key.code == KeyCode::Esc;
        let cancel_pivot = self.pivot_computing() && key.code == KeyCode::Esc;
        let leave_quality_evidence =
            self.quality.evidence_return.is_some() && self.at_table() && key.code == KeyCode::Esc;
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

    /// The column cursor keys: `h` `l` (←→), `[` `]` (Shift+←→) a page, `{` `}` first
    /// and last. Never with Ctrl or Alt: Ctrl+[ is Esc on a terminal.
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

    /// Whether a key may act while busy; the main loop adds "nothing queued" for the
    /// second group. The hard escapes always qualify. At the plain table view, keys
    /// that read nothing act too: quit, help, and the column cursor (which re-slices
    /// the held buffer, never collects; a count here would read cloud metadata on this
    /// thread, see `a_key_that_acts_while_busy_reads_nothing`). Everything else is
    /// type-ahead and waits. Never by keycode alone: the `h` in `/hello` never scrolls.
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
        // One row up or down inside the held rows while only more rows are awaited: the
        // table on screen is the one they are for.
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
                    // Drawn from what the table holds.
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

    /// The plain table view: Normal mode with nothing drawn over it.
    pub fn in_normal_table_view(&self) -> bool {
        self.at_table()
            && !self.help.is_open()
            && !self.error_modal.active
            && !self.confirmation_modal.active
            && self.context_menu.is_none()
    }

    /// While a header is dragged over another column, a rule where it would land:
    /// after that column moving right, before it moving left.
    fn render_drop_mark(&self, buf: &mut Buffer, ctx: &crate::render::context::RenderContext) {
        let Some(app::pointer::Drag::Move { column, over }) = self.pointer.drag() else {
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
            && self.at_table()
            && self.data_table_state.is_some()
            && !self.help.is_open()
            && !self.error_modal.active
            && !self.confirmation_modal.active
    }

    /// Whether the open menu answers `key` itself; that reads nothing, so it acts
    /// while busy.
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
        self.context_menu = Some(app::context_menu::ContextMenu::with(
            at,
            app::context_menu::column_items(combine),
        ));
    }

    /// Close the context menu, if it is open.
    pub fn close_context_menu(&mut self) {
        self.context_menu = None;
    }

    /// Choose line `i` of the open menu: it closes and its key is offered as typed.
    pub fn choose_from_menu(&mut self, i: usize) -> Option<AppEvent> {
        let menu = self.context_menu.take()?;
        match menu.chosen(i)? {
            app::context_menu::MenuKey::Run(key) => Some(AppEvent::Press(key)),
            app::context_menu::MenuKey::Do(action) => {
                self.menu_action(action);
                None
            }
            _ => None,
        }
    }

    /// A header dropped on another column: `column` moves to `onto`, as repeated `H` /
    /// `L` would. Frozen columns move among the frozen, scrolling among the scrolling.
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
        // The sidebar orders hidden columns by its last applied order; move the column
        // there too so the shown order agrees with the table.
        let applied = &mut self.sort_filter_modal.sort.applied_order;
        if let (Some(i), Some(j)) = (
            applied.iter().position(|c| c == column),
            applied.iter().position(|c| c == onto),
        ) {
            let moving = applied.remove(i);
            applied.insert(j, moving);
        }
        Some(AppEvent::Applied(Applied::ColumnOrder(order, locked)))
    }

    /// Whether a text field owns typed characters, so the wheel and `?` leave it alone.
    /// The home filter is excluded on purpose.
    pub fn text_field_focused(&self) -> bool {
        match self.overlay {
            Overlay::None => match self.input_mode {
                InputMode::Editing => true,
                InputMode::Home => false,
                InputMode::Normal => false,
            },
            Overlay::Analysis => {
                self.analysis_modal.sample_scope_typing()
                    || self.analysis_modal.quality_expected_typing()
                    || self.analysis_modal.intent_typing()
                    || self.analysis_modal.export_typing()
            }
            Overlay::View => {
                self.view_modal.mode != ViewModalMode::List
                    && matches!(
                        self.view_modal.form_focus,
                        FormFocus::Name
                            | FormFocus::Description
                            | FormFocus::ExactPath
                            | FormFocus::RelativePath
                            | FormFocus::PathPattern
                            | FormFocus::FilenamePattern
                    )
            }
            Overlay::Export { .. } => matches!(
                self.export_modal.focus,
                ExportFocus::PathInput | ExportFocus::CsvDelimiter
            ),
            Overlay::Copy => self.copy_modal.picker.is_some(),
            Overlay::Inspect => self.inspector_modal.finding,
            Overlay::GoToColumn => true,
            Overlay::PickFormat | Overlay::Retype { .. } => true,
            Overlay::Combine { .. } => self.column_forms.combine.as_ref().is_some_and(|c| {
                c.picker.is_some() || c.focus == app::modals::retype_modal::CombineField::Name
            }),
            Overlay::PickTable => true,
            Overlay::Sample => self
                .sample
                .form
                .as_ref()
                .is_some_and(|form| form.field.is_text()),
            // The inline editor, the add-sort Picker and the Columns tab's find type.
            Overlay::SortFilter => self.sort_filter_modal.typing(),
            Overlay::PivotMelt => {
                self.pivot_melt_modal.picker.is_some()
                    || self
                        .pivot_melt_modal
                        .is_text_row(self.pivot_melt_modal.focus)
            }
            Overlay::ChartExport => self.chart.export_modal.focus.is_text(),
            Overlay::Chart => self.chart.modal.picker.is_some(),
            Overlay::Info | Overlay::ValueCounts => false,
            // The prompt and the spec picker's filter type.
            Overlay::Hex => self
                .hex
                .view
                .as_ref()
                .is_some_and(|view| view.prompt.is_some() || view.picker.is_some()),
        }
    }

    /// Whether a spinner is on screen, so the run loop turns it and redraws.
    pub fn something_is_spinning(&self) -> bool {
        self.is_busy()
            || (self.row_count_pending() && !self.awaiting_open_confirmation())
            // The clock beside "source read finishing" keeps time until it has.
            || (self.overlay == Overlay::Analysis && self.cancelled_analysis_running().is_some())
            || self.chart_preparing()
            || self.value_counts_computing()
            || (self.input_mode == InputMode::Home
                && (self.home.awaiting_listing().is_some()
                    || self.home.sections_waiting()
                    || !self.home.peeking.is_empty()))
    }

    /// Take the numbers the whole frame is drawn from: the footer count, read once
    /// because two parts of the screen show it while a thread moves it.
    fn begin_frame(&mut self) {
        self.take_in_what_arrived();
        self.counting.footers_this_frame = self.footer_progress().reading();
        self.counting.listed_this_frame = self.footer_progress().listed();
        // Whatever this frame does not draw cannot be clicked.
        self.pointer.forget_drawn();
        if let Some(state) = self.data_table_state.as_mut() {
            state.forget_drawn();
        }
    }

    /// The state half of [`Self::begin_frame`]: what a frame takes in whether or not it
    /// is painted.
    fn take_in_what_arrived(&mut self) {
        // Measurements land a file at a time over listings of thousands: folded into the
        // rows once a frame, not once an answer.
        self.home.apply_new_measurements();
        // Back from home to the table whose lines were being indexed.
        if self.counting.indexing_paused && self.input_mode != InputMode::Home {
            self.index_lines();
        }
    }

    /// A frame's work without painting it, in the order [`EventPump::run`] does it
    /// around a frame: ask for what the frame needs, take in what arrived (as drawing
    /// does), and ask again for what that changed. For a harness with no terminal, at
    /// the point the run loop would draw: once the events on hand are handled.
    ///
    /// [`EventPump::run`]: crate::app::event_pump::EventPump::run
    pub fn frame_work(&mut self) {
        self.request_what_the_frame_needs();
        self.take_in_what_arrived();
        self.request_what_the_frame_needs();
    }

    /// The footer counter: the open's while one is on its way, else the dataset's. Each
    /// open counts on its own, so a replaced one cannot count under another's name.
    pub fn footer_progress(&self) -> &Arc<crate::formats::schema_union::FooterProgress> {
        self.loading
            .progress()
            .unwrap_or(&self.counting.footer_progress)
    }

    /// Held past the app: when dropped it removes temp files the app's opens were still
    /// writing, giving workers up to a second to stop.
    pub fn exit_sweep(&self) -> ExitSweep {
        ExitSweep(self.loading.unfinished().clone())
    }

    /// What the status line says while `:N` waits for the lines to be indexed.
    const INDEXING_FOR_ROW: &'static str = "Reading lines to find the row...";

    /// What the status line says while an End waits on a row count; named so only
    /// this message is taken down when the End is retired.
    const COUNTING_FOR_END: &'static str = "Counting rows to find the end...";

    /// What the footer says while a path is looked at; named so the answer takes down
    /// only its own line.
    const LOOKING: &'static str = "Looking...";

    /// The wait while a directory named on the command line is looked at (seconds for
    /// large Parquet).
    pub const LOOKING_AT_A_DIRECTORY: &'static str = "Looking at the directory";

    /// The wait while the rows for the view are fetched.
    pub const LOADING_BUFFER: &'static str = "Loading buffer...";

    /// The wait while a view's pivot or first rows are read.
    const APPLYING_VIEW: &'static str = "Applying view...";

    /// The wait while a grouped row the buffer does not hold is read to drill into.
    const READING_GROUP: &'static str = "Reading the group...";

    /// The wait while the inspector reads a row's hidden and binary fields.
    const READING_FIELDS: &'static str = "Reading fields...";

    /// Above this a field is copied off the UI thread: a long list's JSON is slow.
    const FIELD_COPY_INLINE_BYTES: usize = 1024 * 1024;
    /// The most JSON a copy of a JSON value writes where the clipboard sets no cap.
    const JSON_COPY_MAX_BYTES: usize = 64 * 1024 * 1024;
    /// The wait while the inspector parses long text as JSON.
    const READING_JSON: &'static str = "Reading JSON...";

    /// The wait while a pivot reads the view.
    const COMPUTING_PIVOT: &'static str = "Computing pivot...";

    /// How long a fetch goes unmentioned, so local paging does not blink a message.
    const A_FETCH_WORTH_SAYING: std::time::Duration = std::time::Duration::from_millis(300);

    /// Grow the buffer before the view reaches its end. Nothing waits on it: no `busy`,
    /// no message. One at a time; a scroll that outruns it waits on it or supersedes it
    /// by generation. Never when a bump would strand other work.
    fn load_ahead(&mut self) {
        // Check the generation before marking the position asked: otherwise a frame while
        // the generation is held spends the position on the refusal and never asks again.
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
        // Ask once per position: planning can return the buffer on hand (a row group too
        // large for the caps), and replanning every frame would be wasted.
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

    /// Spawn a buffer collect if needed; true if a job was spawned. Advances the
    /// generation, invalidating any prior collect. An unknown row count is computed
    /// in the background (`BackgroundLenReady`) and never gates the paint: the plan
    /// is then a top-of-data window.
    pub fn spawn_async_collect(&mut self, status: &str) -> bool {
        self.spawn_collect(Some(status))
    }

    /// As [`Self::spawn_async_collect`]; with no `status`, a load-ahead whose job holds
    /// no keys. See [`InflightCollect`].
    fn spawn_collect(&mut self, status: Option<&str>) -> bool {
        let held = self
            .data_table_state
            .as_ref()
            .is_some_and(|state| self.count_held_at_estimate(state));
        let Some(state) = self.data_table_state.as_mut() else {
            return false;
        };

        // The exact row count, if unknown and none is coming for this data version.
        // Independent of the generation (a scroll must not restart it); not busy. A
        // footer-only count runs now. On an object store a data count rides in the
        // collect below (answered outright by a short read). On a local frame it waits
        // for the paint (`count_after_paint`), and a short page makes it unnecessary.
        let mut count = None;
        let generation = state.len_generation();
        if !state.is_num_rows_valid()
            && self.counting.len_count_inflight != Some(generation)
            // Mark running only if it will run: a dataset still reading its footers counts
            // itself, and a marker set for a count never started stays set for the session.
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

        // With the count unknown this plans `slice(0, N)`, touching only the first files of
        // a partitioned set.
        let Some(state) = self.data_table_state.as_mut() else {
            return false;
        };
        let covered = inflight.is_some_and(|inflight| inflight.covers(state));
        // The rows are already coming in a load-ahead: wait on it (it takes the keys).
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
        // Everything past here advances the generation. If that would strand work (an
        // open's phase, say), queue the collect: the throbber keeps turning and it is
        // retried after every event. A count landing minutes later jumps to the end
        // through here with no key pressed (#238).
        if a_bump_would_strand {
            // Drop the count that would ride this collect rather than run it alone: alone it
            // is a full remote `len()`. Clearing the marker lets the retry ask again.
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
        // Owed from here, so a worker that dies first still answers the count.
        let count = count.map(|job| OwedCount::new(job, self.events.clone()));
        self.spawn_job(Job::Rows(inflight), status, move |_| {
            let plan = request.plan;
            // The count is answered after the page goes out: it may need its own pass.
            Ok(
                match crate::analysis::statistics::collect_lazy(
                    request.lf,
                    request.polars_streaming,
                ) {
                    Ok(df) => {
                        let returned = df.height();
                        let requested = request.buffer_end - request.buffer_start;
                        let start = request.buffer_start;
                        // Stitched and cut on the worker: a cut may copy up to the byte budget.
                        Answer::Rows(plan.fit(df)).then(move || {
                            if let Some(count) = count {
                                count.answer(|job| job.after_collect(start, returned, requested));
                            }
                        })
                    }
                    // A pass over a frame that just failed would fail too: the count goes unanswered,
                    // reports itself failed, and waits for a later interaction.
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

    /// Start `job` on a worker. With a `status` the app is busy with it: the bar says
    /// so and keys wait. Its answer, `Err`, or panic arrives via
    /// [`AppEvent::JobEnded`] at [`App::job_ended`]. Does not advance the generation:
    /// a caller replacing work in flight advances it first.
    fn spawn_job<F, R>(&mut self, job: Job, status: Option<&str>, work: F) -> Ticket
    where
        F: FnOnce(&app::jobs::Worker) -> std::result::Result<R, String> + Send + 'static,
        R: Into<app::jobs::Answered>,
    {
        let started = self.start_job(job, status);
        let ticket = started.ticket();
        started.run(&self.runtime, work);
        ticket
    }

    /// As [`Self::spawn_job`], with the ticket before the work: run it with
    /// [`app::jobs::Started::run`].
    fn start_job(&mut self, job: Job, status: Option<&str>) -> app::jobs::Started {
        if let Some(status) = status {
            // Any errand that led here passes its keys to this job's record.
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

    /// The wanted read of the table's rows in flight, if any.
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

    /// The rows read or owed are no longer for the table on screen: drop them on
    /// arrival.
    fn forget_the_rows_read(&mut self) {
        self.jobs
            .supersede(|job| Self::reading_rows(job) || Self::owed_rows(job));
    }

    /// Hold the generation: a continuation waiting to run, or an errand waiting on the
    /// user. See [`app::jobs::Hold`].
    pub(crate) fn hold_the_generation(&self) -> app::jobs::Hold {
        self.jobs.hold()
    }

    /// A job with no worker, started as `spawn_job` does, for tests that end it.
    #[cfg(test)]
    pub(crate) fn job_for_tests(&mut self, job: Job, status: Option<&str>) -> app::jobs::Started {
        self.start_job(job, status)
    }

    /// Whether the bar has no open and no export to report.
    #[cfg(test)]
    pub(crate) fn nothing_loading(&self) -> bool {
        self.loading.current().is_none() && self.export_progress.is_none()
    }

    /// An open on the loading screen saying `phase` about `path` of `size` bytes, with
    /// nothing running.
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

    /// An open of `path` begun and scanning with no worker. Its jobs are [`Job::Load`]
    /// with the id returned.
    #[cfg(test)]
    pub(crate) fn open_for_tests(&mut self, path: &str) -> loading::LoadId {
        self.put_down_load_in_flight();
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

    /// Install `state` through the loader as an open of a frame does, without reading
    /// its first rows.
    #[cfg(test)]
    pub(crate) fn install_for_tests(
        &mut self,
        state: DataTableState,
        path: Option<PathBuf>,
        options: &OpenOptions,
        debug_label: Option<String>,
    ) -> bool {
        self.put_down_load_in_flight();
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

    /// A page owed to the dataset on screen, as when the generation was held.
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

    /// A current `job` answers `answer` at once and the app handles it.
    #[cfg(test)]
    pub(crate) fn answer_for_tests(&mut self, job: Job, answer: Answer) -> Option<AppEvent> {
        let started = self.jobs.start(job, None);
        let ticket = started.ticket();
        started.end(Outcome::answered(answer));
        self.job_ended(ticket)
    }

    /// Whether advancing the generation would throw away an answer nothing will ask for
    /// again. See [`Jobs::would_strand`].
    fn work_a_bump_would_strand(&self) -> bool {
        self.jobs.would_strand()
    }

    /// A move through the rows: now if the buffer holds its landing, else deferred a
    /// frame while the rows are read.
    fn scroll_key(&mut self, scroll: Scroll) -> Option<AppEvent> {
        let state = self.data_table_state.as_mut()?;
        if state.scroll_would_trigger_collect(scroll.delta(state)) {
            self.busy = true;
            return Some(AppEvent::Scroll(scroll));
        }
        scroll.run(state);
        None
    }

    /// Home, End and G. A jump needing a fill is deferred behind a throbber frame;
    /// if the view is already there only the selection settles.
    fn jump_key(&mut self, jump: Scroll) -> Option<AppEvent> {
        // End on a remote dataset waits for the row count rather than reading every file
        // up to a guess. A dataset still joining its footers gets its count from that
        // pass; a second count would answer a `len_generation` the join replaces, and
        // the jump would never happen (`scan_is_the_root`: filters and sorts are rebuilt
        // over the joined scan). Lines still being indexed end where the indexing ends.
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
        // Any other frame of unknown length waits for its count too. A count waiting on a
        // paint starts now; one running or riding a collect is waited on.
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

    /// Run a scroll; `scroll` returns true when it leaves the buffer. Clears `busy`
    /// when no collect is spawned, else the key handler's flag would hold input.
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
        let theme = Theme::from_config(&AppConfig::default().theme).unwrap_or_else(|_| Theme {
            colors: std::collections::HashMap::new(),
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
        let cache = CacheManager::new(APP_NAME)
            .unwrap_or_else(|_| CacheManager::with_dir(std::env::temp_dir().join(APP_NAME)));
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
            counting: loading::counting::Counting::default(),
            home: home::HomeState {
                hide_unreadable: !app_config.home.show_unreadable,
                formats: formats.clone(),
                ..Default::default()
            },
            home_app: home::home_app::HomeApp {
                local_desktop: app::link_open::local_desktop(
                    app::link_open::Platform::current(),
                    |name| std::env::var(name).ok(),
                ),
                ..Default::default()
            },
            source: loading::open_scan::OpenedSource::default(),
            pipes: app::run::Pipes::default(),
            events,
            debug: DebugState::default(),
            info_modal: InfoModal::new(),
            info: app::keys::info_keys::InfoState {
                head_web_rows: !cache::running_as_a_cargo_test(),
                ..Default::default()
            },
            prompt: query::query_prompt::QueryPrompt {
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
            overlay: Overlay::None,
            sort_filter_modal: SortFilterModal::new(),
            pivot_melt_modal: PivotMeltModal::new(),
            view_modal: ViewModal::new(),
            analysis_modal: AnalysisModal::with_sample_rows(app_config.analysis.sample_rows),
            sample: analysis::sample_draw::SampleState {
                form: None,
                memory_probe: std::sync::Arc::new(analysis::table_sample::available_memory),
                paths: Vec::new(),
            },
            quality: analysis::quality_runs::QualityRuns {
                memory_budget: QUALITY_MEMORY_BUDGET,
                ..Default::default()
            },
            chart: chart::chart_jobs::Charts {
                export_modal: chart_export_modal,
                ..Default::default()
            },
            export_modal: ExportModal::new(),
            copy_modal: app::modals::copy_modal::CopyModal::new(),
            inspector_modal: inspector::inspector_modal::InspectorModal::new(),
            external: app::run::External::default(),
            pickers: app::keys::picker_keys::Pickers::default(),
            value_counts: analysis::value_counts_modal::ValueCountsModal::default(),
            hex: app::keys::hex_keys::HexState::default(),
            column_forms: app::keys::retype_keys::ColumnForms::default(),
            error_modal: ErrorModal::new(),
            flash: None,
            confirmation_modal: ConfirmationModal::new(),
            help: app::help::Help::default(),
            pointer: app::pointer::Pointing::default(),
            context_menu: None,
            cache,
            cache_writes: CacheWrites::default(),
            views: view::view_apply::SavedViews {
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
                    // App can be built from an unvalidated config (the Python API): fall back to no
                    // formatting, keeping the alignment setting.
                    .unwrap_or_else(|_| NumberFormatSettings {
                        align_numeric_right: app_config.display.right_align_numbers,
                        ..Default::default()
                    }),
                background_query: false,
                repaint: None,
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

    /// Use `registry` as the format specs on the search path.
    pub fn set_formats(&mut self, registry: crate::formats::Registry) {
        let registry = Arc::new(registry);
        self.home.formats = registry.clone();
        self.formats = registry;
    }

    pub fn enable_debug(&mut self) {
        self.debug.enabled = true;
    }

    // ---- Home screen -----------------------------------------------------

    /// After a frame, ask for what it lacked: counts for the rows on screen with none,
    /// and kinds for rows nothing has looked into. Workers read; this thread decides.
    pub fn request_what_the_frame_needs(&mut self) {
        if self.at_table() {
            self.load_ahead();
            self.catch_up_follow();
        }
        self.inspector_needs();
        if self.input_mode != InputMode::Home {
            return;
        }
        self.take_listing_news();
        #[cfg(feature = "http")]
        self.size_selected_web_file();
        // Each pass asks for the rows still unknown, a batch at a time; not under the
        // path prompt, which hides the list.
        if !self.home.path_input_active {
            self.request_selected_preview();
            self.request_home_schema();
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

    /// Open the help overlay on the keys of the current screen, unless already up.
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

    /// Close the help when the screen under it changed on its own (a query finished, a
    /// load failed): its keys are for a screen that is gone. A question or error
    /// that arrived under it closes it too.
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
        use crate::analysis::analysis_modal::{AnalysisTool, AnalysisView};
        use datui_cli::keys::Context;
        match self.overlay {
            Overlay::Analysis => match self.analysis_modal.view {
                AnalysisView::DistributionDetail => Context::DistributionDetail,
                AnalysisView::CorrelationDetail => Context::CorrelationDetail,
                AnalysisView::Main => match self.analysis_modal.selected_tool {
                    Some(AnalysisTool::DistributionAnalysis) => Context::Distribution,
                    Some(AnalysisTool::CorrelationMatrix) => Context::Correlation,
                    Some(AnalysisTool::DataQuality) => Context::DataQuality,
                    Some(AnalysisTool::Describe) | None => Context::Describe,
                },
            },
            Overlay::View => Context::Views,
            Overlay::None => match self.input_mode {
                InputMode::Normal => Context::Table,
                InputMode::Editing => match self.prompt.input_type {
                    Some(InputType::Find) => Context::Find,
                    _ => Context::Query,
                },
                InputMode::Home if self.info.documentation.is_open() => Context::Documentation,
                InputMode::Home => Context::Home,
            },
            Overlay::SortFilter => Context::SortFilter,
            Overlay::PivotMelt => Context::PivotMelt,
            Overlay::Export { .. } => Context::Export,
            Overlay::Copy => Context::Copy,
            Overlay::Inspect => Context::Inspector,
            Overlay::GoToColumn => Context::GoToColumn,
            Overlay::PickFormat => Context::FormatPicker,
            Overlay::Retype { .. } => Context::Retype,
            Overlay::Combine { .. } => Context::Combine,
            Overlay::PickTable => Context::TablePicker,
            Overlay::Sample => Context::Sample,
            Overlay::Info => Context::Info,
            Overlay::Chart | Overlay::ChartExport => Context::Chart,
            Overlay::Hex => Context::Hex,
            Overlay::ValueCounts => Context::ValueCounts,
        }
    }

    /// True while the confirmation modal asks whether to download a remote file or read
    /// a large one whole. These can be walked away from (to home, not exit): the
    /// size probe behind them can take seconds.
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
                // Every row is a sample method like the others: shown in the strip, `s` changes it.
                let sample = analysis::sampling::Sample {
                    method: analysis::sampling::SampleMethod::EveryRow,
                    ..self.analysis_modal.sample.clone()
                };
                self.apply_sample(sample)
            }
            Confirm::OpenLink(url) => Some(AppEvent::Applied(Applied::OpenLink(url))),
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
            // The overwrite was agreed to, for this export only.
            Confirm::QualityExport(path, format) => Some(AppEvent::Applied(
                Applied::QualityReportExport(path, format, Overwrite::Replace),
            )),
            Confirm::ChartExport(request) => Some(AppEvent::Applied(Applied::ChartExport(
                ChartExportRequest {
                    overwrite: Overwrite::Replace,
                    ..*request
                },
            ))),
            Confirm::Export(request) => Some(AppEvent::Applied(Applied::Export(ExportRequest {
                overwrite: Overwrite::Replace,
                ..*request
            }))),
            Confirm::Copy(format, header) => {
                Some(AppEvent::Applied(Applied::CopyTable { format, header }))
            }
            Confirm::Download => {
                // The loader releases the generation as the download or read starts, and its job
                // takes it at once.
                let step = self.loading.confirmed();
                self.run_load_step(step)
            }
        }
    }

    /// No or Esc on the confirmation: nothing asked about happens. A declined overwrite
    /// returns to the filled form; other dialogs stay as they were.
    fn declined(&mut self) -> Option<AppEvent> {
        match self.confirmation_modal.take() {
            Some(Confirm::ChartExport(_)) => self.open_overlay(Overlay::ChartExport),
            Some(Confirm::Export(_)) => self.open_over(|returns_to| Overlay::Export { returns_to }),
            // Backing out of a download or a large read goes home, which puts the open down.
            Some(Confirm::Download) => self.enter_home(),
            _ => {}
        }
        None
    }

    fn key(&mut self, event: &KeyEvent) -> Option<AppEvent> {
        self.debug.on_key(event);

        // A completion flash lasts until the next key.
        self.flash = None;
        // A key puts back a header being carried, so a release later moves nothing.
        self.cancel_drag();

        let ctrl = event.modifiers.contains(KeyModifiers::CONTROL);
        // Ctrl-Q quits from anywhere, before any mode (the chart view has no CONTROL arm
        // and would swallow it while busy).
        if ctrl && event.code == KeyCode::Char('q') {
            return Some(AppEvent::Exit);
        }
        // Ctrl-C too, even in a text field (which copies with Alt+W instead).
        if ctrl && event.code == KeyCode::Char('c') {
            return Some(AppEvent::Exit);
        }

        // The context menu takes keys first. A chosen line closes it and presses its key,
        // offered as typed; any other key closes it and then acts. A menu covered since
        // (an error, a load's screen) is gone.
        if !self.menu_showing() {
            self.context_menu = None;
        }
        if let Some(menu) = self.context_menu.as_mut() {
            match menu.key(event) {
                app::context_menu::MenuKey::Moved => return None,
                app::context_menu::MenuKey::Close => {
                    self.context_menu = None;
                    return None;
                }
                app::context_menu::MenuKey::Run(key) => {
                    self.context_menu = None;
                    return Some(AppEvent::Press(key));
                }
                app::context_menu::MenuKey::Do(action) => {
                    self.context_menu = None;
                    self.menu_action(action);
                    return None;
                }
                app::context_menu::MenuKey::Other => self.context_menu = None,
            }
        }

        // Acts at once (see `hard_escape_while_busy`), ahead of keys held behind the view.
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
            && self.at_table()
            && self.in_normal_table_view()
            && self.footers_counted().is_some()
        {
            self.stop_count();
            return None;
        }

        if event.code == KeyCode::Esc
            && self.at_table()
            && !self.error_modal.active
            && !self.confirmation_modal.active
            && self.return_from_quality_evidence(true)
        {
            return None;
        }

        // F1 toggles help before any other branch (e.g. Editing) can consume it.
        if event.code == KeyCode::F(1) {
            if self.help.is_open() {
                self.help.close();
            } else {
                self.open_help_overlay();
            }
            return None;
        }

        // Home owns every key, except under a modal or the help overlay: both render over
        // home, and if home ate their keys they could not be dismissed.
        if self.input_mode == InputMode::Home
            && !self.confirmation_modal.active
            && !self.error_modal.active
            && !self.help.is_open()
        {
            return self.home_key(event);
        }

        // Ctrl+O goes home from anywhere, mid-load included.
        if event.code == KeyCode::Char('o')
            && event.modifiers.contains(KeyModifiers::CONTROL)
            && (!self.confirmation_modal.active || self.awaiting_open_confirmation())
        {
            self.help.close();
            self.enter_home();
            return None;
        }

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
                // ←→ carry the choice, so ↑↓ (k/j) scroll a long question; the render clamps.
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
                    // With nothing loaded, go back to the list the dataset was chosen from, with the
                    // reason, rather than leave an empty table.
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

        // The column cursor keys at the main table, before the help and mode blocks. No
        // is_press() check: some terminals misreport key kind.
        let in_main_table = self.at_table() && !self.help.is_open();
        // The footer offers the column's keys once the cursor moves, until another key.
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

        // The help owns the keys while up. Enter on a line closes it and returns that key
        // as the follow-up, reaching the screen under it as a typed key would.
        self.close_help_left_behind();
        if self.help.is_open() {
            return match self.help.key(event) {
                app::help::HelpKey::Press(key) => Some(AppEvent::Press(key)),
                app::help::HelpKey::Stay | app::help::HelpKey::Closed => None,
            };
        }

        if event.code == KeyCode::Char('?') {
            let ctrl_help = event.modifiers.contains(KeyModifiers::CONTROL);
            // The home screen always accepts characters, into its filter or path input.
            let in_text_input = self.text_field_focused() || self.input_mode == InputMode::Home;
            // Ctrl-? always opens help; bare ? only outside a text field.
            if ctrl_help || !in_text_input {
                self.open_help_overlay();
                return None;
            }
        }

        match self.overlay {
            Overlay::None => {}
            Overlay::SortFilter => return self.sort_filter_key(event),
            Overlay::Export { .. } => return self.export_key(event),
            Overlay::Sample => return self.table_sample_form_key(event),
            Overlay::Inspect => return self.inspector_key(event),
            Overlay::ValueCounts => return self.value_counts_key(event),
            Overlay::Hex => return self.hex_key(event),
            Overlay::GoToColumn => {
                self.go_to_column_key(event);
                return None;
            }
            Overlay::PickFormat => return self.format_picker_key(event),
            Overlay::Retype { .. } => return self.retype_key(event),
            Overlay::Combine { .. } => return self.combine_key(event),
            Overlay::PickTable => return self.table_picker_key(event),
            Overlay::Copy => return self.copy_key(event),
            Overlay::PivotMelt => return self.pivot_melt_key(event),
            Overlay::Info => return self.info_key(event),
            Overlay::Chart | Overlay::ChartExport => return self.chart_key(event),
            Overlay::Analysis => return self.analysis_key(event),
            Overlay::View => return self.view_key(event),
        }

        if self.input_mode == InputMode::Editing {
            return self.editing_key(event);
        }

        const RIGHT_KEYS: [KeyCode; 2] = [KeyCode::Right, KeyCode::Char('l')];

        const LEFT_KEYS: [KeyCode; 2] = [KeyCode::Left, KeyCode::Char('h')];

        const DOWN_KEYS: [KeyCode; 2] = [KeyCode::Down, KeyCode::Char('j')];

        const UP_KEYS: [KeyCode; 2] = [KeyCode::Up, KeyCode::Char('k')];

        // The letter arms below are unmodified keys only (else Ctrl+E would open Export).
        // Paging (Ctrl+F/B/D/U) is the only modified set here; global escapes came first.
        if event
            .modifiers
            .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT)
            && !matches!(event.code, KeyCode::Char('f' | 'b' | 'd' | 'u'))
        {
            return None;
        }

        match event.code {
            // q pops the context: back home if opened from there, else quit. Q and Ctrl+Q
            // always quit.
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
            // The column's width, applied as typed.
            KeyCode::Char('<' | '>' | '=' | 'w')
                if event.is_press() && !event.modifiers.contains(KeyModifiers::CONTROL) =>
            {
                if let Some(state) = self.data_table_state.as_mut()
                    && let Some(name) = state.current_column().map(str::to_string)
                {
                    // From the width on screen: `>` on a column clipped at the right edge widens what
                    // is seen.
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
                // Drawn from the schema at render time, like `,`. Session-only.
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
                // Applied at render time, so no re-collect. Session-only.
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
                // Out of a drill from Value Counts, back to the counts of this view.
                if from_counts
                    && std::mem::take(&mut self.value_counts.drill_return)
                    && let Some(state) = self.data_table_state.as_ref()
                {
                    self.value_counts.rebase(state.len_generation());
                    self.open_overlay(Overlay::ValueCounts);
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
                if !self.at_table() {
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
                    // Unread notes open the Notes tab (the accented `i` chip's promise). Read before
                    // the mark, which retires the accent.
                    let unseen = state.notes_unseen();
                    state.mark_notes_seen();
                    if unseen {
                        self.info_modal
                            .open_on(crate::widgets::info::InfoTab::Notes);
                    } else if state.format_detail().is_some_and(|d| d.first) {
                        // A table whose columns are the same for every file (model tensors, audio frames,
                        // VCD changes) opens on its format's own tab.
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
                    self.open_overlay(Overlay::Info);
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
                // Apply the best view whose criteria match. When none does, open the list so the
                // user picks or saves one.
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
                if self.at_table() {
                    self.open_table_sample_form();
                }
                None
            }
            KeyCode::Char('s') => {
                if self.data_table_state.is_some() {
                    // Rebuilt from the table's applied state, never the modal's last contents: a
                    // canceled edit must not come back staged.
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
                    self.open_overlay(Overlay::SortFilter);
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
                // Nothing computes until a tool is chosen.
                if self.data_table_state.is_some()
                    && self.at_table()
                    && self.quality.evidence_return.is_none()
                {
                    // The results a close put down come back on the view they are of.
                    let view = self.data_table_state.as_ref().map(|s| s.len_generation());
                    self.analysis_modal.open(view);
                    self.open_overlay(Overlay::Analysis);
                    // The sample outlives a close, but its scope names this dataset's rows: another
                    // dataset starts from its current view.
                    if self.analysis_modal.sample_dataset != Some(self.dataset_generation) {
                        self.analysis_modal.sample.scope =
                            analysis::data_quality::QualityScope::CurrentView;
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
                    && self.at_table()
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
                        .filter(|(_, dtype)| chart::chart_data::is_category_dtype(dtype))
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
                    self.open_overlay(Overlay::Chart);
                }
                None
            }
            KeyCode::Char('p') => {
                if self.data_table_state.is_some() && self.at_table() {
                    self.open_pivot_builder();
                }
                None
            }
            KeyCode::Char('e') => {
                if self.data_table_state.is_some() && self.at_table() {
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
                            .any(crate::export::nested_json::is_nested);
                        self.export_modal.avro_renames =
                            state.get_column_order().iter().any(|name| {
                                state.schema().get(name).is_some_and(|dtype| {
                                    crate::export::avro_types::renames(name, dtype)
                                })
                            });
                    }
                    self.open_over(|returns_to| Overlay::Export { returns_to });
                }
                None
            }
            KeyCode::Char(' ') if event.is_press() => {
                if self.at_table() {
                    self.open_inspector();
                }
                None
            }
            KeyCode::Char('g') if event.is_press() => {
                if self.at_table() {
                    self.open_go_to_column();
                }
                None
            }
            KeyCode::Char('b') if event.is_press() => {
                if self.at_table() {
                    self.open_format_picker();
                }
                None
            }
            KeyCode::Char('T') if event.is_press() => {
                if self.at_table() {
                    self.open_table_picker();
                }
                None
            }
            KeyCode::Char('y') => {
                if self.at_table()
                    && let Some(state) = self.data_table_state.as_ref()
                {
                    let columns = state.get_column_order().to_vec();
                    let context = app::modals::copy_modal::CopyContext {
                        row_number: state.selected_display_row().unwrap_or(0),
                        view_rows: state.copy_view_df().map(|d| d.height()).unwrap_or(0),
                        view_cols: columns.len(),
                        total_rows: state.num_rows_if_valid(),
                    };
                    let current = state.current_column().map(str::to_string);
                    self.copy_modal.open(columns, current.as_deref(), context);
                    self.open_overlay(Overlay::Copy);
                }
                None
            }
            _ => None,
        }
    }

    /// Handle one event. A key arriving while busy is neither acted on nor dropped: it
    /// returns as `Err(key)` for the caller to hold until idle, as
    /// [`app::event_pump::EventPump`] does. [`App::event`] is for callers with nowhere to
    /// hold a key.
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
        // Not while returning a continuation: the follow-up is the rest of this event
        // (e.g. `AnalysisCompute` before its job spawns), nothing holds the generation
        // yet, and an errand would advance it underneath. Errands wait for the next event.
        if out.is_none() {
            self.let_waiting_errands_in();
        }
        self.ensure_chart_data();
        self.home_score_search();
        // New rows on hand under an open find prompt: light up their matches.
        self.refresh_stale_live_matches();
        Ok(out)
    }

    /// Give the errands waiting for a free generation their turn: after every event,
    /// and when a continuation's hold is released.
    pub(crate) fn let_waiting_errands_in(&mut self) {
        // Columns footers found while the user was inside a query are held; they join on
        // the first event after the view returns to the data.
        if self.join_held_footers() {
            self.reread_after_the_footers_joined();
        }
        self.join_followed_fields();
        self.describe_ended_journal();
        // Likewise a re-read owed to a dataset whose footers could not be read.
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
                if let Some(leaving) = self.leaving_by(&key)
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
                // Home is now in the stack, so q pops back to it. Never unset: a reread (H) is not
                // a new place.
                if self.input_mode == InputMode::Home {
                    self.source.opened_from_home = true;
                }
                // `az://container/path` names no account; where it was typed, or the config, does.
                #[cfg(feature = "cloud")]
                let expanded = match paths
                    .iter()
                    .map(|p| {
                        crate::cloud::cloud_sources::expand_azure_short_url(
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
                // Asks the filesystem for the loading screen's size and whether the path exists to
                // be a recent.
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
                // The next render sets visible_rows and needs_recollect; the main loop collects.
                self.display.repaint = Some(render::context::Repaint::Whole);
                None
            }
            AppEvent::Collect => {
                self.spawn_async_collect(Self::LOADING_BUFFER);
                None
            }
            AppEvent::Scroll(scroll) => self.handle_scroll(|s| scroll.run(s)),
            AppEvent::AnalysisCompute(tool) => self.spawn_analysis(tool),
            AppEvent::BackgroundLenReady { .. }
            | AppEvent::FramePainted
            | AppEvent::BackgroundLenFailed { .. } => self.counting_event(event),
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
                let formats = self.formats.clone();
                // The open's first phase. Unleased: an answer for an open the user left is thrown
                // away by the loader, not waited for.
                self.put_down_load_in_flight();
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
            AppEvent::LookThenOpenDirectory(looking, options) => {
                // Named on the wait so the first frame says which directory is being looked at;
                // Ctrl+C and Ctrl+O keep working during it.
                self.put_down_load_in_flight();
                let load = self.loading.look_at_directory(looking.clone());
                // A newer look replaces an older one.
                self.jobs
                    .supersede(|job| matches!(job, Job::LookAtDirectory { .. }));
                // The loading screen's words, so the footer agrees with it. Unleased: the answer
                // is meant to be dropped when the user moves on, and a lease would make the
                // next open's collect wait for the abandoned look.
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
                            crate::cloud::cloud_browse::peek_kind(&url, &cloud).await
                        })
                        .and_then(Result::ok);
                        let (kind, holds) = match peeked {
                            Some((kind, holds)) => (kind, Some(Box::new(holds))),
                            None => (home::discover::EntryKind::Unknown, None),
                        };
                        return Ok(Answer::LookedAt {
                            kind,
                            holds,
                            options: Box::new(options),
                        });
                    }
                    // Caught: on a worker a panic would be swallowed and the spinner stay up forever,
                    // so the answer becomes "a directory" and home opens on it. Read as this open
                    // will read, so the rule judges the directory the user is about to see.
                    let as_read = Self::read_as(&options);
                    let looked = logging::catch_panic(|| {
                        let mut entry = home::discover::Entry::directory(&looking);
                        entry.kind = home::discover::EntryKind::Unknown;
                        home::look_into_as(&entry, &as_read)
                    });
                    let kind = match looked {
                        Ok(entry) => entry.kind,
                        Err(_) => home::discover::EntryKind::Directory,
                    };
                    Ok(Answer::LookedAt {
                        kind,
                        holds: None,
                        options: Box::new(options),
                    })
                });
                None
            }
            AppEvent::ClassifyThenOpen {
                path: looking,
                jump,
            } => {
                // A second Enter supersedes the first (home keys act while busy): the newer look
                // is the one waited for, and refusing would let a dead share block every look.
                self.jobs.supersede(|job| matches!(job, Job::Classify(_)));
                let look = Job::Classify(app::jobs::Classify {
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
                let formats = self.formats.clone();
                self.spawn_job(look, Some(Self::LOOKING), move |_| {
                    // Each of these can hang on a share that went away; hence off the key thread.
                    let found = if looking.is_dir() {
                        Some(crate::home::discover::classify_directory(&looking))
                    } else if looking.exists()
                        // A member of an archive, or a variant a spec reads from a file.
                        || crate::formats::members::split(&looking).is_some()
                        || crate::formats::members::split_variant(&looking, &formats).is_some()
                    {
                        Some(crate::home::discover::EntryKind::File)
                    } else {
                        None
                    };
                    Ok(Answer::Kind(found))
                });
                None
            }
            AppEvent::JobEnded(ticket) => self.job_ended(ticket),
            AppEvent::Applied(applied) => self.apply(applied),
            AppEvent::JobProgress { ticket, progress } => {
                self.job_progress(ticket, &progress);
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
            AppEvent::Followed(news) => {
                self.followed(&news);
                None
            }
            AppEvent::Paste(text) => self.paste(&text),
            AppEvent::TerminalBackground(mode) => {
                self.terminal_answered(mode);
                None
            }
            AppEvent::TerminalFocused => {
                self.display.background_query |= self.app_config.theme.follow;
                self.display.repaint = self
                    .display
                    .repaint
                    .max(Some(render::context::Repaint::BeforeMoving));
                None
            }
            // Taken before here: a press becomes a key in `handle_event`; terminal, wake, exit,
            // crash and missing-path events in the pump; settings in `run`.
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

    /// The first sidebar filter whose value does not read as its column's type, said
    /// for the user.
    fn filter_problem(&self) -> Option<String> {
        let schema = self.data_table_state.as_ref()?.schema();
        self.sort_filter_modal
            .filter
            .statements
            .iter()
            .find_map(|f| {
                crate::export::python_script::SidebarFilter::problem(f, schema.get(&f.column))
            })
    }

    /// Whether the dataset is delimited text, whose header `H` on Info's Schema tab
    /// toggles.
    pub fn header_toggle_offered(&self) -> bool {
        self.source
            .opened
            .as_ref()
            .and_then(|(_, options)| options.format)
            .and_then(FileFormat::separator)
            .is_some()
    }

    /// Read the dataset again with its first row the other way: as names, or as data
    /// under `column_1`, …. Only delimited text; elsewhere a no-op.
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

    /// `H` / `L`: move the cursor's column one place in the sidebar's order, the
    /// cursor with it. Frozen among frozen, scrolling among scrolling; nothing at an
    /// end.
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
        // The sidebar orders hidden columns by its last applied order; swap them there
        // too so it agrees with the table.
        let applied = &mut self.sort_filter_modal.sort.applied_order;
        if let (Some(i), Some(j)) = (
            applied.iter().position(|c| *c == order[at]),
            applied.iter().position(|c| *c == order[to]),
        ) {
            applied.swap(i, j);
        }
        Some(AppEvent::Applied(Applied::ColumnOrder(order, locked)))
    }

    /// `[` / `]` at the table: sort by the cursor's column, replacing the sort in
    /// effect. The same key again on that sort alone removes it.
    fn sort_by_cursor_column(&mut self, descending: bool) -> Option<AppEvent> {
        let state = self.data_table_state.as_ref()?;
        let column = state.current_column()?.to_string();
        let already = state.view_sort_columns() == std::slice::from_ref(&column)
            && state.view_sort_descending() == [descending];
        if already {
            // Back to natural order: `sort` with no columns also resets the direction `]` left,
            // which would read as a reversal.
            if let Some(state) = self.data_table_state.as_mut() {
                state.deferred(|s| s.sort(Vec::new(), true));
            }
            self.spawn_async_collect("Sorting...");
            return None;
        }
        Some(AppEvent::Applied(Applied::Sort(
            vec![column],
            vec![descending],
        )))
    }
}

impl App {
    /// `+` / `-`: add a sidebar filter keeping (or dropping) rows with the cursor's
    /// cell value exactly as stored, and apply it. A null cell is "is null" / "not
    /// null"; `R` clears it.
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
            // Text that reads back to exactly this value: a float as stored, a datetime to its
            // last digit, in its zone.
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
        Some(AppEvent::Applied(Applied::Filter(statements)))
    }

    /// Bring the sidebar in line with what is applied to the frame on screen (column
    /// order, hidden set, sort, filters). Called on open, so a canceled edit never
    /// returns staged, and after a drill-down, where stale filters would hit a List
    /// column.
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
                    .map(crate::app::modals::filter_modal::Operand::of)
                    .unwrap_or_default()
            })
            .collect();
        modal.filter.available_columns = order.clone();
        // The cursor starts on the add row; the editor never survives a resync.
        modal.filter.cursor = modal.filter.statements.len();
        modal.filter.editor = None;
        // A schema column the applied order leaves out is hidden, and listed where it stood
        // so showing it restores its place. The sidebar's last applied order decides that
        // only while the table still shows it; after a view, query or reshape set the
        // order, the schema does.
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
        // Everything up to the last frozen column stays frozen, hidden ones included; only
        // the applied order knows a hidden column that ended the frozen span.
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
                    // 1-based, as the modal assigns and the sidebar prints.
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

    /// Apply everything the sidebar stages and close it (Enter or Ctrl+Enter).
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
        // Widths read nothing, so they apply here. With nothing else changed the view
        // stays on its page: re-applying order, filters and sort would read from the top.
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
        Some(AppEvent::Applied(Applied::ApplyView(
            column_order,
            locked_count,
            statements,
            columns,
            descending,
        )))
    }

    /// Facts read for the dataset's single file stored as its format says (a stream or
    /// compressed copy has no footer).
    fn facts_of_open(&self) -> Option<(FileFormat, crate::formats::readers::Facts)> {
        let hive = self
            .source
            .opened
            .as_ref()
            .is_some_and(|(_, options)| options.hive);
        let format = self.opened_format()?;
        let facts = crate::formats::readers::of(format).facts?;
        let state = self.data_table_state.as_ref()?;
        let plain = state
            .read_mode()
            .is_none_or(|mode| Some(mode) == format.read_mode(crate::Stored::Plain));
        // Several files, whose footers the Notes and Schema tabs already sum up.
        let one_file = state.dataset_schema().is_none();
        (!hive && plain && one_file).then_some((format, facts))
    }

    /// The footer's line while a query's first rows are read; the rows drawn meanwhile
    /// are the replaced view's.
    pub(crate) fn query_reading(&self) -> Option<&str> {
        let run = self.prompt.query_running.as_ref()?;
        let frame = self.data_table_state.as_ref()?.len_generation();
        if !matches!(run.origin, RunOrigin::Query(_)) || run.frame != frame {
            return None;
        }
        self.jobs
            .waiting_status(|job| Self::reading_rows(job) || Self::owed_rows(job))
    }

    /// A job's outcome is in: take it and its record from [`Jobs`] and act on it. The
    /// job holds the generation and keys until its answer is handled, so whatever the
    /// answer starts next holds them first. A waited-on job gives back the keys and
    /// its footer line here, unless the answer continues, keeping the wait up.
    fn job_ended(&mut self, ticket: Ticket) -> Option<AppEvent> {
        let app::jobs::Ended {
            job,
            current,
            keys,
            outcome,
            ..
        } = self.jobs.end(ticket)?;
        let cancelled_analysis = !current && Self::is_analysis_read(&job);
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
                // Unless a job still running shows the same line (a view's rows read after its
                // pivot).
                if self.status_message.as_deref() == Some(status.as_str())
                    && !self.jobs.shows(&status)
                {
                    self.status_message = None;
                }
            }
        }
        // A cancelled analysis's worker exited: once no other is going, Run is free again.
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

    /// `job` answered. `current`: still the answer waited for; a stale one changes
    /// nothing and its payload is dropped here. `waited`: the user waited on it.
    fn answered(
        &mut self,
        job: Job,
        current: bool,
        waited: bool,
        answer: Answer,
    ) -> Option<AppEvent> {
        match (job, answer) {
            (Job::JournalDetail { dataset }, Answer::JournalDescribed(detail)) => {
                if dataset == self.dataset_generation
                    && let Some(state) = self.data_table_state.as_mut()
                {
                    state.set_format_detail(*detail);
                }
                None
            }
            (Job::IndexLines { dataset }, Answer::LinesIndexed(rows)) => {
                self.lines_indexed(dataset, rows);
                None
            }
            (Job::FootersJoin { dataset }, Answer::FootersJoined(found)) => {
                self.footers_joined(dataset, found.map(|found| *found))
            }
            (Job::Load(load), Answer::Load(answer)) => {
                // The loader judges by load identity, not generation: an answer for a replaced or
                // abandoned open, or a phase it left, is dropped with what it carries.
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
                // The user left the open while its paths were looked at, or another replaced it.
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
                // Ctrl+O, a newer look or another open replaced this one: nobody waits for it.
                if !self.loading.looking_at_directory(load) {
                    return None;
                }
                // An `Open` that follows carries the same open on.
                self.open_the_directory_looked_at(path, kind, holds.as_deref(), *options)
            }
            (Job::Classify(asked), Answer::Kind(found)) => {
                // Superseded: whatever replaced it owns the wait.
                if !current {
                    return None;
                }
                self.home.status = None;

                // A home key answers on home: if the user went back to the data, or browsed
                // elsewhere, acting now would pull them back.
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
                // Timed here, when the rows exist to be drawn; the paint costs the same whatever
                // the fetch did.
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
                Job::Rows(_),
                Answer::RowsFailed {
                    message,
                    conversion,
                },
            ) => {
                self.rows_failed(current, waited, &message, conversion.as_deref());
                None
            }
            (Job::Analysis(_), Answer::Analysis(install, results)) => {
                if current {
                    install(&mut self.analysis_modal, results);
                    self.analysis_modal.computing = None;
                }
                None
            }
            (
                Job::Analysis(_),
                Answer::DataQuality {
                    results,
                    kept,
                    plan,
                },
            ) => {
                // Kept whatever became of the results: a read is not to be thrown away.
                if let Some(kept) = kept {
                    self.retain_quality_sample(&kept);
                }
                if current
                    && self.overlay == Overlay::Analysis
                    && self.analysis_modal.selected_tool
                        == Some(analysis::analysis_modal::AnalysisTool::DataQuality)
                {
                    // Labeled with the plan it ran with, whatever has been staged since.
                    self.cache_quality_result(&results, (*plan).clone());
                    self.analysis_modal.quality.last_plan = Some(*plan);
                    self.analysis_modal.quality.results = Some(*results);
                    self.analysis_modal.quality.from_cache = false;
                    self.analysis_modal
                        .set_quality_page(crate::analysis::data_quality::QualityPage::Overview);
                    self.analysis_modal.computing = None;
                }
                None
            }
            (Job::SampleDraw(draw), Answer::SampleDrawn(drawn)) => {
                self.sample_drawn(*draw, current, drawn)
            }
            (Job::SampleRows, Answer::Sample { df, label }) => {
                if current {
                    self.analysis_modal.computing = None;
                    self.show_sample_view(df, label);
                }
                None
            }
            (Job::Pivot, Answer::Pivoted { spec, pivoted }) => {
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
                        // Only from the modal: a trip home meanwhile stays home.
                        if self.overlay == Overlay::PivotMelt {
                            self.close_overlay();
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
                // Superseded: cancelled or replaced, and the replacement owns the wait.
                let (view, why) = *pivot;
                if !current {
                    return None;
                }
                let planned = self.data_table_state.as_mut().map(|state| {
                    // Nothing changed while the pivot was read, so earlier steps plan as before, now
                    // with the pivot in hand.
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
            (Job::DrillRow, Answer::DrillRow { group_index, row }) => {
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
                if self.overlay == Overlay::Inspect && asked {
                    self.inspector_modal.read =
                        Some(inspector::inspector_modal::FieldRead::Read { frame, row, values });
                }
                None
            }
            (Job::InspectJson { token }, Answer::JsonParsed(root)) => {
                // Superseded means something replaced the view, which owns the wait.
                if !current || self.overlay != Overlay::Inspect {
                    return None;
                }
                let modal = &mut self.inspector_modal;
                if let Some(wait) = modal.json_wait.take_if(|w| w.token == token) {
                    let node = inspector::inspector_drill::Node::Json {
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
                    && let Some(inspector::inspector_modal::Pretty::Pending { token: t, place }) =
                        modal.pretty.as_ref()
                    && *t == token
                {
                    modal.pretty = Some(inspector::inspector_modal::Pretty::Ready {
                        place: place.clone(),
                        text,
                    });
                }
                None
            }
            (Job::InspectUnpack { token }, Answer::Unpacked(decoded)) => {
                let modal = &mut self.inspector_modal;
                if current
                    && let Some(inspector::inspector_modal::Unpack::Pending { token: t, place }) =
                        modal.unpack.as_ref()
                    && *t == token
                {
                    modal.unpack = Some(inspector::inspector_modal::Unpack::Ready {
                        place: place.clone(),
                        text: std::sync::Arc::new(decoded),
                    });
                }
                None
            }
            (Job::OpenValue, Answer::ValueWritten(open)) => {
                if current && self.overlay == Overlay::Inspect {
                    self.external.open = Some(open);
                }
                None
            }
            (Job::Export, Answer::Exported(path)) => {
                // Written: the dialog held for a failure is done with.
                self.export_modal.close();
                if current {
                    self.export_progress = None;
                    self.flash_path("Exported to ", &path);
                }
                None
            }
            (Job::Copy, Answer::Copied { payload, message }) => {
                if current {
                    self.export_progress = None;
                    self.finish_copy(payload, message);
                }
                None
            }
            (Job::QualityReport, Answer::QualityReportWritten(path)) => {
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
                // Leaving the chart's dataset supersedes the write, so a late finish does not
                // reopen its modal over home.
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
            (
                Job::HexOpen {
                    origin,
                    fallback,
                    record_size,
                },
                Answer::HexOpened(source),
            ) => {
                // Superseded: another file, or the view was left.
                if current {
                    self.hex_opened(origin, fallback, record_size, *source);
                }
                None
            }
            (Job::HexFind(run), Answer::HexFound(hit)) => {
                if current {
                    self.hex_found(run, hit);
                }
                None
            }
            (Job::ValueCounts, Answer::ValueCounts(counts)) => {
                // Superseded: another column, a cancel, or a trip away.
                if current {
                    self.value_counts.computing = None;
                    self.value_counts.hold(*counts);
                }
                None
            }
            // What a test's answer carries goes with it; it rides any job.
            #[cfg(test)]
            (_, Answer::Probe(held)) => {
                drop(held);
                None
            }
            // An answer under another job than its own is a bug: dropped with what it
            // carries. Every answer is named, so a new one needs its own arm above.
            (
                job,
                Answer::Load(_)
                | Answer::NamedPaths { .. }
                | Answer::NamedPathMissing(_)
                | Answer::LookedAt { .. }
                | Answer::Kind(_)
                | Answer::Rows(_)
                | Answer::ReshapePreviewed { .. }
                | Answer::ViewPivoted(_)
                | Answer::FieldsRead(_)
                | Answer::JsonParsed(_)
                | Answer::Indented(_)
                | Answer::Unpacked(_)
                | Answer::ChartPrepared(_)
                | Answer::ChartExported
                | Answer::FileFacts(_)
                | Answer::UnfitCounted(_)
                | Answer::Found(_)
                | Answer::HexOpened(_)
                | Answer::HexFound(_)
                | Answer::FootersJoined(_)
                | Answer::JournalDescribed(_)
                | Answer::LinesIndexed(_)
                | Answer::RowsFailed { .. }
                | Answer::Analysis(..)
                | Answer::DataQuality { .. }
                | Answer::SampleDrawn(_)
                | Answer::Sample { .. }
                | Answer::Pivoted { .. }
                | Answer::DrillRow { .. }
                | Answer::ValueWritten(_)
                | Answer::Exported(_)
                | Answer::Copied { .. }
                | Answer::QualityReportWritten(_)
                | Answer::ValueCounts(_),
            ) => {
                log::error!(target: "datui", "a {:?} job got another job's answer; dropped", job.kind());
                None
            }
        }
    }

    /// Put down what a failed job started, and say why. [`Self::job_ended`] already
    /// released its keys and line. Each arm clears only what that job started; most act
    /// only when it is current, and the rest are judged by something of their own.
    fn background_failed(
        &mut self,
        job: &Job,
        current: bool,
        waited: bool,
        message: &str,
        panicked: bool,
    ) {
        // With one line for the reason, a panic's message gives way to the log.
        let could_not = |what: &str| {
            if panicked {
                format!("Could not {what}; see the log")
            } else {
                format!("Could not {what}: {message}")
            }
        };
        match job {
            // Judged by the open: one put down or replaced is not the one waited on.
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
            Job::Rows(_) | Job::OwedRows { .. } => {
                self.rows_failed(current, waited, message, None);
            }
            Job::SampleDraw(draw) => self.sample_draw_failed(draw, current, message),
            // The preview says why in its own pane; the log has a panic's details.
            Job::ReshapePreview { epoch, token } => {
                let message = if panicked {
                    "Could not preview; see the log".to_string()
                } else {
                    message.to_string()
                };
                self.reshape_preview_ended(*epoch, *token, None, Err(message));
            }
            Job::InspectPretty { token } => {
                let modal = &mut self.inspector_modal;
                if let Some(inspector::inspector_modal::Pretty::Pending { token: t, place }) =
                    modal.pretty.as_ref()
                    && t == token
                {
                    modal.pretty = Some(inspector::inspector_modal::Pretty::Failed {
                        place: place.clone(),
                    });
                }
            }
            Job::InspectUnpack { token } => {
                let modal = &mut self.inspector_modal;
                if let Some(inspector::inspector_modal::Unpack::Pending { token: t, place }) =
                    modal.unpack.as_ref()
                    && t == token
                {
                    modal.unpack = Some(inspector::inspector_modal::Unpack::Failed {
                        place: place.clone(),
                    });
                }
            }
            Job::Find(_) => self.find_failed(current, message),
            // Judged by the dataset, as its answer is; the panel has one line for it.
            Job::FileFacts { dataset } => {
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
            // The Info tab keeps what the open read; the log says why it has no more.
            Job::JournalDetail { .. } => {
                log::warn!(target: "datui", "reading the journal's detail failed: {message}");
            }
            // Stopped: whoever stopped it says what becomes of the reads waiting on the
            // lines.
            Job::IndexLines { .. } => {}
            // The rest act only for the job still current; a stale failure is dropped.
            _ if !current => {
                if matches!(job, Job::Export) {
                    // The dialog held for a failure is done with.
                    self.export_modal.close();
                }
            }
            Job::Classify(_) => {
                self.home.status = None;
                self.error_modal.show(message.to_string());
            }
            Job::Analysis(_) | Job::SampleRows => {
                self.analysis_modal.computing = None;
                self.error_modal.show(message.to_string());
            }
            // The form stays up with its spec, to be fixed.
            Job::Pivot | Job::Copy | Job::HexOpen { .. } => {
                self.error_modal.show(message.to_string());
            }
            // The dialog is still up, the reason on its status line under the path.
            Job::QualityReport => match self.analysis_modal.quality.export.as_mut() {
                Some(form) => form.error = Some(message.to_string()),
                None => self.error_modal.show(message.to_string()),
            },
            Job::ViewPivot(_) => self.view_pivot_failed(message),
            // The grouped view stays as it was.
            Job::DrillRow => self.flash_note(could_not("drill in")),
            Job::InspectJson { token } => {
                let modal = &mut self.inspector_modal;
                if let Some(wait) = modal.json_wait.take_if(|w| w.token == *token) {
                    modal.not_json = Some((wait.frame, wait.row, wait.path));
                    self.flash_note(if panicked {
                        "Could not read the JSON; see the log".to_string()
                    } else {
                        sentence(message)
                    });
                }
            }
            Job::OpenValue => self.flash_note(could_not("open the value")),
            Job::InspectRow { frame, row } => {
                let asked = self
                    .inspector_modal
                    .read
                    .as_ref()
                    .is_some_and(|read| read.key() == (*frame, *row));
                if asked {
                    self.inspector_modal.read =
                        Some(inspector::inspector_modal::FieldRead::Failed {
                            frame: *frame,
                            row: *row,
                            message: could_not("read the field"),
                        });
                }
            }
            // The form comes back with the reason on its status line.
            Job::Export => {
                self.export_progress = None;
                self.export_modal.path_error = Some(message.to_string());
                self.open_over(|returns_to| Overlay::Export { returns_to });
            }
            Job::ChartExport { path, format } => {
                self.finish_chart_export(path, *format, Err(message.to_string()));
            }
            Job::HexFind(_) => {
                self.status_message = None;
                self.flash_note(message.to_string());
            }
            // Said on the screen, in place of the counts.
            Job::ValueCounts => {
                if let Some(computing) = self.value_counts.computing.take() {
                    let why = if panicked {
                        "could not count; see the log".to_string()
                    } else {
                        message.to_string()
                    };
                    self.value_counts.failed = Some((computing.column, why));
                }
            }
        }
    }

    /// Ask a worker for the open file's size and footer, once per dataset; a source
    /// with no local file has none. Unleased and not busy: judged by
    /// `dataset_generation`, which bumps leave alone, so it neither strands nor holds
    /// collects or the panel's keys.
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
        // One local file only: a glob has nothing to stat, and several files are not the
        // first one's size.
        let several = self
            .source
            .opened
            .as_ref()
            .is_some_and(|(paths, _)| paths.len() > 1);
        let piped = self.reads_stdin();
        let Some(path) = self.path.clone().filter(|path| {
            !several
                && !piped
                && !cloud::source::is_remote_url(path)
                && !cloud::source::is_prefix_or_glob(&path.to_string_lossy())
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

    /// Keep the facts read for `dataset` if it is still on screen.
    fn file_facts_landed(&mut self, dataset: u64, facts: FileFacts) {
        if dataset == self.dataset_generation {
            self.info.file_facts = Some((dataset, facts));
        }
    }

    /// What the Info panel knows about the open file: `None` until asked, and for a
    /// source with no local file. Cleared on install.
    pub fn file_facts(&self) -> Option<&FileFacts> {
        Self::facts_shown(
            &self.info.file_facts,
            self.dataset_generation,
            self.file_facts_reading(),
        )
    }

    /// [`Self::file_facts`] from its fields, for a caller borrowing the rest of the app.
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

    /// An open found a database of several tables: the home screen lists them.
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

    /// An open failed before its first rows; the loader has put it down. The previous
    /// dataset, or home if the open came from there, is current again.
    fn load_failed(&mut self, failed: loading::Failed) {
        let loading::Failed { message, from_home } = failed;
        self.status_message = None;
        self.busy = false;
        // Kept so home can say why when dismissing the error lands there from a command
        // line. Chosen at home, the dialog already said it.
        if from_home {
            self.home_app.last_load_error = None;
            self.enter_home();
        } else {
            self.home_app.last_load_error = Some(message.clone());
        }
        self.error_modal.show(message);
    }

    /// The table's rows could not be read. A waiting query or view is not applied; a
    /// waited-on page ends its wait with the reason; a load-ahead's failure is left for
    /// the page that needs those rows.
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
        // A count waiting on this paint would read the frame that just failed: it fails
        // too, as a count riding the collect does.
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

    /// `message` with the dataset's temp files (a download, a decompressed copy) named
    /// by what the user opened.
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

    /// Above this estimated size a table copy asks first: most paste targets choke
    /// sooner, and the clipboard holds it all.
    const COPY_CONFIRM_BYTES: usize = 10 * 1024 * 1024;
    /// Above this a table copy is refused; export writes files this size without
    /// holding them as text.
    const COPY_REFUSE_BYTES: usize = 200 * 1024 * 1024;

    /// Enter on a grouped row: its group's rows, read off-thread when the buffer lacks
    /// the row.
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
                    let row = crate::analysis::statistics::collect_lazy(*lf, streaming)
                        .map_err(|e| crate::error_display::user_message_from_polars(&e))?;
                    Ok(Answer::DrillRow { group_index, row })
                });
            }
        }
    }

    /// Under `theme.mode = "auto"`, switch to `theme.dark` or `theme.light` for the
    /// terminal's background, with `theme.colors` over it. An explicit mode ignores
    /// the terminal.
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
                // The prompts live as long as the app and keep their colors; dialogs take the
                // theme each time they open.
                for input in [
                    &mut self.prompt.query_input,
                    &mut self.prompt.sql_input,
                    &mut self.prompt.find.input,
                ] {
                    *input = std::mem::take(input).with_theme(&self.theme);
                }
            }
            // The colors parsed at startup, so unexpected; keep the current palette.
            Err(e) => log::warn!("cannot switch to the {mode:?} palette: {e}"),
        }
    }

    /// The terminal's background: follow it under `auto`, and remember it for the next
    /// start's first frame.
    fn terminal_answered(&mut self, mode: ThemeMode) {
        if self.app_config.theme.follow {
            self.cache
                .remember_terminal_mode(&app::terminal_color::terminal_key(), mode);
        }
        self.follow_terminal_background(mode);
    }

    /// Settle the first frame's palette under `auto` without waiting: `answered` if
    /// given, else this terminal's last answer. A later answer switches if it differs.
    pub fn settle_first_palette(&mut self, answered: Option<ThemeMode>) {
        if let Some(mode) = answered {
            self.terminal_answered(mode);
        } else if self.app_config.theme.follow
            && let Some(mode) = self
                .cache
                .terminal_mode(&app::terminal_color::terminal_key())
        {
            self.follow_terminal_background(mode);
        }
    }

    /// Whether the run loop should ask the terminal's background, once (set by
    /// [`AppEvent::TerminalFocused`] under `auto`).
    pub fn take_background_query(&mut self) -> bool {
        std::mem::take(&mut self.display.background_query)
    }

    /// Whether the next frame repaints every cell (after a resize, or when the
    /// terminal regains focus), for the run loop.
    pub fn take_repaint(&mut self) -> Option<render::context::Repaint> {
        std::mem::take(&mut self.display.repaint)
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
    pub fn take_external_open(&mut self) -> Option<inspector::external_open::ExternalOpen> {
        self.external.open.take()
    }

    /// Whether the session reports the mouse, to retake it after a program had the
    /// terminal.
    pub fn mouse_enabled(&self) -> bool {
        self.app_config.display.mouse
    }

    /// The run loop opened `open`: a waiting program is done with its file; a failure
    /// goes on the bar.
    pub fn external_opened(
        &mut self,
        open: &inspector::external_open::ExternalOpen,
        failed: Option<String>,
    ) {
        let program =
            inspector::external_open::program_for(open.document, |name| std::env::var(name).ok());
        if matches!(program, inspector::external_open::Program::Wait(_)) {
            let _ = std::fs::remove_file(&open.path);
        }
        match failed {
            Some(e) => self.flash_note(format!("Could not open the value: {e}")),
            None if matches!(program, inspector::external_open::Program::Opener(_)) => {
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
        .with_dtype_row(self.display.dtype_row)
        .with_stripes_follow_rows(self.app_config.display.scroll_region);

        let main_view_content = MainViewContent::current(self);

        Clear.render(area, buf);
        let background_color = self.theme.background();
        Block::default()
            .style(Style::default().bg(background_color))
            .render(area, buf);

        // The footer grows into the view only for a prompt being typed or a job with
        // progress.
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
        let rule = self.overlay != Overlay::Inspect;
        let app_layout = app_layout(area, self.debug.enabled, footer_lines, rule);
        // A short terminal keeps the status line first, then the prompt, then progress.
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
            // Over the view, never the footer, whose rule and extra lines are drawn after.
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

        // Last, deliberately. Widgets draw untrusted text (cells, names, filenames,
        // parser messages); ratatui strips control characters in `set_stringn` but not in
        // `Span`/`Line`, and crossterm prints cells unfiltered, so a cell holding
        // `\x1b]52;c;...\x07` would write the clipboard. One sweep here covers every
        // widget. See `crate::sanitize`.
        crate::sanitize::sanitize_buffer(buf);
    }
}

impl App {
    /// The view returned on exit to a caller that asked (`datui.view(...,
    /// capture=True)`): the committed frame without internal columns; `None` with no
    /// dataset. Refused when the frame scans a temp file (download, decompressed or
    /// converted copy), which is removed on exit; export (`e`) is the way out.
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
        // Stop the footer pass. The loader stops the open in flight as it drops (a running
        // download removes its partial file). In Drop to cover every exit: quit, error,
        // panic unwind, and the Python binding running again in-process.
        self.counting.stop_footer_pass();
        // Stop indexing so nothing holds the file once the app is gone (the Python
        // binding runs on).
        self.counting.stop_indexing();
    }
}

/// Run a future on the app's runtime from an outside thread and wait for it. Not
/// `Handle::block_on`: that polls on the calling thread, and after quit's runtime
/// shutdown the next timer or socket panics ("Tokio 1.x context ... being
/// shutdown"). A spawned task is dropped instead, and the wait ends with `None`.
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

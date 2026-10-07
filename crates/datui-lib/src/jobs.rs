//! Background operations, and their one owner.
//!
//! [`Jobs`] starts every general background operation and keeps one record per job
//! until its outcome is handled: the [`Job`], its [`Ticket`], whether a generation
//! bump would strand it, whether the user waits on it, and whether its answer is
//! still wanted. The app keeps no job markers of its own.
//!
//! - **One outcome.** A worker returns its [`Answer`] or an error; a panic is
//!   caught. The outcome goes into the record and [`AppEvent::JobEnded`] says so. A
//!   [`Started`] job dropped without running ends as failed.
//! - **Acceptance and release in one step.** [`Jobs::end`] hands over the outcome
//!   and removes the record in one call, so whatever the app starts while handling
//!   the answer takes the generation before anything else looks: it never reads
//!   free between two phases of one errand, and no finished job still holds it.
//! - **Supersession.** Advancing the generation supersedes the jobs it scopes;
//!   [`Jobs::supersede`] cancels others. A superseded job stops holding the
//!   generation at once; its worker runs on and its stale outcome is dropped.
//! - **Keys.** A waited-on job holds the keys and its footer line until it ends or
//!   is superseded; [`Jobs::quiet`] releases them and [`Jobs::wait_on`] takes them
//!   for a running job. A page asked for while the generation is held is owed
//!   ([`Jobs::owe`]): it holds the keys, not the generation, until run.
//! - **Holds.** A continuation the pump has not dispatched, or a download waiting
//!   on the user, also holds the generation ([`Hold`]).
//!
//! Not owned here: the row count (`OwedCount`) and the home screen's workers, each
//! keyed by its own marker. `reread_after_the_footers_joined` sends its jump straight
//! to the channel, safe only because its callers checked nothing would be stranded.
//!
//! A worker that never returns (a `hard` NFS mount, a wedged object-store read)
//! keeps its keys and lease until superseded.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::mpsc::Sender;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use polars::prelude::DataFrame;

use crate::loading::{LoadAnswer, LoadId};
use crate::{AppEvent, OpenOptions, logging};

/// Which kind of operation a [`Ticket`] names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JobKind {
    Load,
    OpenNamed,
    LookAtDirectory,
    Classify,
    Rows,
    Analysis,
    SampleRows,
    SampleDraw,
    Pivot,
    ViewPivot,
    ReshapePreview,
    DrillRow,
    InspectRow,
    InspectJson,
    InspectPretty,
    InspectUnpack,
    OpenValue,
    Export,
    Copy,
    QualityReport,
    FileFacts,
    ChartExport,
    ChartPrepare,
    Find,
    ValueCounts,
    HexOpen,
    HexFind,
    UnfitCount,
    FootersJoin,
    JournalDetail,
    IndexLines,
}

/// One started operation. Issued when it starts, carried by its worker, and handed
/// back with its outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Ticket {
    id: u64,
    generation: u64,
    kind: JobKind,
}

impl Ticket {
    pub fn kind(self) -> JobKind {
        self.kind
    }
}

/// A background operation. Its fields are what the app needs of it; the record
/// holding it is its only marker.
#[derive(Debug, Clone)]
pub(crate) enum Job {
    /// A phase of the open it names, before its first rows. Judged by the open
    /// ([`crate::loading::Loader`]), not the generation.
    Load(LoadId),
    /// The first phase of an open: whether the named paths exist and which is a
    /// directory.
    OpenNamed(LoadId),
    /// The look at a directory named on the command line, before `load` opens it.
    LookAtDirectory { load: LoadId, path: PathBuf },
    /// A look at a path chosen on the home screen.
    Classify(Classify),
    /// The table's rows: a page the table waits on, or a load-ahead.
    Rows(crate::InflightCollect),
    /// A page asked for while the generation was held: no worker yet; read once
    /// nothing would be stranded.
    OwedRows { dataset: u64, status: String },
    /// An Analysis tool's computation.
    Analysis(AnalysisRun),
    /// The sample, or the rows behind a finding, read to show as a table.
    SampleRows,
    /// The view's sample, drawn into memory as the table shows it.
    SampleDraw(Box<SampleDraw>),
    /// A pivot from the Pivot & Melt builder.
    Pivot,
    /// A view's pivot, read before its rows: the view, and why it was applied for a
    /// match.
    ViewPivot(Box<(crate::view::SavedView, Option<crate::view::MatchReason>)>),
    /// The Pivot & Melt preview, request `token` of opening `epoch`; judged by those,
    /// not the generation. Nobody waits on it.
    ReshapePreview { epoch: u64, token: u64 },
    /// The group row Enter drills into, when the buffer did not hold it.
    DrillRow,
    /// The inspector's fields of row `row` of frame `frame`, not in the buffer.
    InspectRow { frame: u64, row: usize },
    /// The inspector's text parsed as JSON to drill into; answers
    /// [`crate::inspector_drill::JsonWait`].
    InspectJson { token: u64 },
    /// The inspector's long JSON indented for its JSON view; answers
    /// [`crate::inspector_modal::Pretty`].
    InspectPretty { token: u64 },
    /// The inspector's gzip or zstd bytes decompressed for its Text view; answers
    /// [`crate::inspector_modal::Unpack`].
    InspectUnpack { token: u64 },
    /// The inspector's value written to a file for another program to open.
    OpenValue,
    /// An export, from plan to committed file.
    Export,
    /// Collecting and formatting the view for a copy.
    Copy,
    /// Writing the Data Quality report.
    QualityReport,
    /// The open file's size and footer for the Info panel, for `dataset_generation`
    /// `dataset` (the generation does not tell datasets apart).
    FileFacts { dataset: u64 },
    /// Writing a chart. The path and format reopen the form on a failure.
    ChartExport {
        path: PathBuf,
        format: crate::chart::chart_export::ChartExportFormat,
    },
    /// A chart's data for the selection on screen; judged by itself (see
    /// [`ChartPrep`]).
    ChartPrepare(Box<ChartPrep>),
    /// A find reading the view for its next match.
    Find(crate::find::FindRun),
    /// Counting a column's values for the Value Counts screen.
    ValueCounts,
    /// Mapping a file for the hex view, opened from `origin`.
    HexOpen {
        origin: crate::hex_view::Origin,
        fallback: bool,
        record_size: Option<usize>,
    },
    /// A find reading the hex view's file.
    HexFind(crate::hex_view::HexFindRun),
    /// Counting values the read's column types made null, for the Notes; judged by
    /// `dataset`. `version` is the view's column changes counted; `None` for the read's
    /// types.
    UnfitCount { dataset: u64, version: Option<u64> },
    /// The pass reading every footer behind a staged open; judged by `dataset`, since
    /// it outlives several collects.
    FootersJoin { dataset: u64 },
    /// A piped journal's Info tab re-read once it ended, for `dataset`.
    JournalDetail { dataset: u64 },
    /// Indexing the rest of a text file's lines behind its first rows, for the
    /// `dataset_generation` it was started for. Stopped by its flag, it fails.
    IndexLines { dataset: u64 },
}

/// A look at a path chosen on the home screen. Home keys act while busy, so a
/// second Enter supersedes the first.
#[derive(Debug, Clone)]
pub(crate) struct Classify {
    pub(crate) path: PathBuf,
    /// Where home was browsing when the look was asked; an answer for a place left
    /// opens nothing. Not `home_generation`: listings rebuild for unrelated reasons
    /// (another root's probe answering).
    pub(crate) browsing: Option<PathBuf>,
    /// A path typed at `~` rather than a row already listed.
    pub(crate) jump: bool,
}

/// A chart's data being prepared. One at a time, so a burst of selection changes
/// does not fan out into a collect per column; the newest selection is prepared
/// next. Its answer is kept only while current and only for its own dataset.
#[derive(Debug, Clone)]
pub(crate) struct ChartPrep {
    pub(crate) request: crate::ChartRequest,
    /// `len_generation` of the dataset read, so an answer is never installed into
    /// another with the same column names.
    pub(crate) dataset: Option<u64>,
    /// Set when the selection or its view moves on. A streamed count or group-by stops
    /// at its next batch; a sampled read runs out.
    pub(crate) cancel: Arc<std::sync::atomic::AtomicBool>,
}

/// A view's sample being drawn. Judged by its rows, not the generation: paging,
/// finds and inspection move the generation meanwhile. It lands in the view whose
/// sample holds `rows`.
#[derive(Clone)]
pub(crate) struct SampleDraw {
    pub(crate) sample: crate::analysis::sampling::Sample,
    pub(crate) rows: Arc<crate::analysis::table_sample::SampleRows>,
    /// Stops the draw; the rows so far stay.
    pub(crate) watch: crate::analysis::sampling::ReadWatch,
    /// Drawn from the view's query or filters rather than the source under them.
    pub(crate) through: bool,
    /// The steps laid on the built sample: query, filters, sort and columns of the view
    /// drawn from, or being applied.
    pub(crate) replay: Option<crate::view::ViewSettings>,
    /// Analysis asked for it, and runs its tool once it is drawn.
    pub(crate) then_analyze: bool,
    /// How a random sample of a stream is drawn, decided before it starts.
    pub(crate) path: Option<crate::analysis::table_sample::DrawPath>,
    /// What the draw is remembered by, for drawing it the same way again.
    pub(crate) path_key: String,
    /// The drawn rows' columns, once cut to scope. The view becomes the sample's when
    /// its first rows land; until then, and if none come, the old view stays.
    pub(crate) schema: Option<polars::prelude::SchemaRef>,
}

impl std::fmt::Debug for SampleDraw {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SampleDraw")
            .field("sample", &self.sample)
            .field("through", &self.through)
            .field("then_analyze", &self.then_analyze)
            .finish_non_exhaustive()
    }
}

/// An Analysis tool's run.
#[derive(Debug, Clone, Default)]
pub(crate) struct AnalysisRun {
    /// A Data Quality run's watch: how it is told to stop, and what it has read.
    pub(crate) watch: Option<crate::analysis::data_quality::QualityWatch>,
    /// Cancelled during a read that cannot stop part way: it runs to its end.
    pub(crate) runs_out: bool,
}

impl Job {
    pub(crate) fn kind(&self) -> JobKind {
        match self {
            Job::Load(_) => JobKind::Load,
            Job::OpenNamed(_) => JobKind::OpenNamed,
            Job::LookAtDirectory { .. } => JobKind::LookAtDirectory,
            Job::Classify(_) => JobKind::Classify,
            Job::Rows(_) | Job::OwedRows { .. } => JobKind::Rows,
            Job::Analysis(_) => JobKind::Analysis,
            Job::SampleRows => JobKind::SampleRows,
            Job::SampleDraw(_) => JobKind::SampleDraw,
            Job::Pivot => JobKind::Pivot,
            Job::ViewPivot(_) => JobKind::ViewPivot,
            Job::ReshapePreview { .. } => JobKind::ReshapePreview,
            Job::DrillRow => JobKind::DrillRow,
            Job::InspectRow { .. } => JobKind::InspectRow,
            Job::InspectJson { .. } => JobKind::InspectJson,
            Job::InspectPretty { .. } => JobKind::InspectPretty,
            Job::InspectUnpack { .. } => JobKind::InspectUnpack,
            Job::OpenValue => JobKind::OpenValue,
            Job::Export => JobKind::Export,
            Job::Copy => JobKind::Copy,
            Job::QualityReport => JobKind::QualityReport,
            Job::FileFacts { .. } => JobKind::FileFacts,
            Job::ChartExport { .. } => JobKind::ChartExport,
            Job::ChartPrepare(_) => JobKind::ChartPrepare,
            Job::Find(_) => JobKind::Find,
            Job::ValueCounts => JobKind::ValueCounts,
            Job::HexOpen { .. } => JobKind::HexOpen,
            Job::HexFind(_) => JobKind::HexFind,
            Job::UnfitCount { .. } => JobKind::UnfitCount,
            Job::FootersJoin { .. } => JobKind::FootersJoin,
            Job::JournalDetail { .. } => JobKind::JournalDetail,
            Job::IndexLines { .. } => JobKind::IndexLines,
        }
    }

    /// The open this job is a phase of, if it is one.
    pub(crate) fn load(&self) -> Option<LoadId> {
        match self {
            Job::Load(load) | Job::OpenNamed(load) | Job::LookAtDirectory { load, .. } => {
                Some(*load)
            }
            _ => None,
        }
    }

    /// Whether a generation bump waits for this job's answer: stale answers are never
    /// asked for again, so most jobs are leased. Not leased: buffer rows (re-asked, and
    /// leasing would chain pages); looks at named paths (meant to be dropped on
    /// Ctrl+O); file facts, journal detail and the footer pass (judged by dataset); the
    /// Pivot & Melt preview (by request); chart data (by dataset, re-asked when
    /// missing). An owed page has nothing running.
    fn leased(&self) -> bool {
        !matches!(
            self,
            Job::Rows(_)
                | Job::SampleDraw(_)
                | Job::OwedRows { .. }
                | Job::OpenNamed(_)
                | Job::LookAtDirectory { .. }
                | Job::FileFacts { .. }
                | Job::UnfitCount { .. }
                | Job::FootersJoin { .. }
                | Job::JournalDetail { .. }
                | Job::IndexLines { .. }
                | Job::ReshapePreview { .. }
                | Job::ChartPrepare(_)
        )
    }

    /// Whether advancing the generation makes this answer stale. Excluded jobs are put
    /// down by their own owner: file facts (dataset), chart export and data (chart
    /// view), an owed page (dataset), a preview (builder request).
    fn follows_the_generation(&self) -> bool {
        !matches!(
            self,
            Job::FileFacts { .. }
                | Job::SampleDraw(_)
                | Job::UnfitCount { .. }
                | Job::FootersJoin { .. }
                | Job::JournalDetail { .. }
                | Job::IndexLines { .. }
                | Job::ChartExport { .. }
                | Job::ChartPrepare(_)
                | Job::OwedRows { .. }
                | Job::ReshapePreview { .. }
        )
    }
}

/// What a job's worker sends back when it succeeds. Each belongs to one [`Job`].
pub(crate) enum Answer {
    /// [`Job::Load`]: what the phase found. A download dropped unhandled removes its
    /// file.
    Load(Box<LoadAnswer>),
    /// [`Job::OpenNamed`]: the paths are there; `directory` is one to look at first.
    NamedPaths {
        paths: Vec<PathBuf>,
        options: Box<OpenOptions>,
        directory: Option<PathBuf>,
    },
    /// [`Job::OpenNamed`]: a path named is not there.
    NamedPathMissing(PathBuf),
    /// [`Job::LookAtDirectory`]: what the look found; `holds` (a cloud listing) picks
    /// the reader.
    LookedAt {
        kind: crate::home::discover::EntryKind,
        holds: Option<Box<crate::home::discover::Holds>>,
        options: Box<OpenOptions>,
    },
    /// [`Job::Classify`]: what the path turned out to be; `None` is a path that is not
    /// there.
    Kind(Option<crate::home::discover::EntryKind>),
    /// [`Job::Rows`]: the rows read.
    Rows(crate::table::CollectResult),
    /// [`Job::Rows`]: the read failed. `conversion` is a value that would not convert,
    /// for the SQL prompt to word itself.
    RowsFailed {
        message: String,
        conversion: Option<Box<crate::error_display::ConversionFailure>>,
    },
    /// [`Job::Analysis`]: a Describe, Distributions or Correlations result, and
    /// where on the modal it goes.
    Analysis(
        fn(
            &mut crate::analysis::analysis_modal::AnalysisModal,
            crate::analysis::statistics::AnalysisResults,
        ),
        crate::analysis::statistics::AnalysisResults,
    ),
    /// [`Job::Analysis`]: a Data Quality report, the rows a sampled run read, and the
    /// plan it ran with.
    DataQuality {
        results: Box<crate::analysis::data_quality::DataQualityResults>,
        kept: Option<crate::KeptQualitySample>,
        plan: Box<crate::analysis::data_quality::DataQualityPlan>,
    },
    /// [`Job::SampleRows`]: rows to show as a table.
    Sample { df: DataFrame, label: String },
    /// [`Job::SampleDraw`]: the draw ended; its rows are in the job's chunks.
    SampleDrawn(crate::analysis::table_sample::Drawn),
    /// [`Job::Pivot`]: the pivot.
    Pivoted {
        spec: crate::pivot_melt_modal::PivotSpec,
        pivoted: DataFrame,
    },
    /// [`Job::ViewPivot`]: the view's pivot.
    ViewPivoted(DataFrame),
    /// [`Job::ReshapePreview`]: the head read, if any, and the preview or the reshape's
    /// error.
    ReshapePreviewed {
        input: Option<crate::pivot_melt_modal::PreviewInput>,
        result: Result<crate::pivot_melt_modal::PreviewFrame, String>,
    },
    /// [`Job::DrillRow`]: the group row.
    DrillRow { group_index: usize, row: DataFrame },
    /// [`Job::InspectRow`]: the fields read.
    FieldsRead(DataFrame),
    /// [`Job::InspectJson`]: the document.
    JsonParsed(std::sync::Arc<serde_json::Value>),
    /// [`Job::InspectPretty`]: the text, indented.
    Indented(std::sync::Arc<str>),
    /// [`Job::InspectUnpack`]: the text, as far as it was decompressed.
    Unpacked(crate::inspector_bytes::Decoded),
    /// [`Job::OpenValue`]: the file, written.
    ValueWritten(crate::external_open::ExternalOpen),
    /// [`Job::Export`]: the file, committed.
    Exported(PathBuf),
    /// [`Job::Copy`]: the formatted view or field and its flash. The clipboard is
    /// written on the event thread, which owns its handle.
    Copied {
        payload: crate::clipboard::Payload,
        message: String,
    },
    /// [`Job::QualityReport`]: the report, written.
    QualityReportWritten(PathBuf),
    /// [`Job::ChartExport`]: the chart, written.
    ChartExported,
    /// [`Job::ChartPrepare`]: the chart's data, and the Color column's values if
    /// counted.
    ChartPrepared(
        Box<(
            crate::chart::chart_plot::PlotData,
            Option<crate::chart::chart_modal::ColorCounts>,
        )>,
    ),
    /// [`Job::FileFacts`]: what the file is.
    FileFacts(crate::widgets::info::FileFacts),
    /// [`Job::Find`]: the cell found, or `None` when nothing in the view matches.
    Found(Option<crate::find::Found>),
    /// [`Job::ValueCounts`]: the column's values, counted.
    ValueCounts(Box<crate::analysis::value_counts::ValueCounts>),
    /// [`Job::HexOpen`]: the file, mapped.
    HexOpened(Box<crate::hex_view::HexSource>),
    /// [`Job::HexFind`]: where the pattern is, if anywhere.
    HexFound(crate::hex_view::HexHit),
    /// [`Job::UnfitCount`]: the columns whose types made values null.
    UnfitCounted(Vec<crate::formats::column_types::Unfit>),
    /// A test's answer, which says when it is dropped.
    #[cfg(test)]
    Probe(Arc<()>),
    /// [`Job::FootersJoin`]: what the footers say; `None` when unreadable, so the
    /// dataset stops waiting.
    FootersJoined(Option<Box<crate::table::FootersFound>>),
    /// [`Job::JournalDetail`]: the journal's Info tab, when it could be read.
    JournalDescribed(Option<Box<crate::formats::text_formats::Detail>>),
    /// [`Job::IndexLines`]: every line is indexed, this many rows of them.
    LinesIndexed(usize),
}

impl Answer {
    /// This answer, then `after` on the worker's thread once sent: for work riding in a
    /// job, like the row count a page read may answer, which must not delay the page.
    pub(crate) fn then(self, after: impl FnOnce() + Send + 'static) -> Answered {
        Answered {
            answer: self,
            then: Some(Box::new(after)),
        }
    }
}

/// An answer, and what the worker does once it has been sent.
pub(crate) struct Answered {
    answer: Answer,
    then: Option<Box<dyn FnOnce() + Send>>,
}

impl From<Answer> for Answered {
    fn from(answer: Answer) -> Self {
        Self { answer, then: None }
    }
}

/// How a job ended.
pub(crate) enum Outcome {
    Answered(Box<Answer>),
    /// The worker returned an error or panicked; `panicked` means `message` is an
    /// internal error naming the log.
    Failed {
        message: String,
        panicked: bool,
    },
}

impl Outcome {
    /// A job's answer, for tests that end a job by hand.
    #[cfg(test)]
    pub(crate) fn answered(answer: Answer) -> Self {
        Self::Answered(Box::new(answer))
    }
}

/// A report from a job still running.
#[derive(Debug, Clone)]
pub enum Progress {
    /// An export has written `bytes` of its file.
    ExportWriting { phase: &'static str, bytes: u64 },
    /// A Data Quality run entered a stage.
    QualityPhase(crate::analysis::data_quality::QualityPhase),
    /// A find has read `rows` of the view.
    Finding { rows: usize },
    /// A find in the hex view has read `read` of the file's `total` bytes.
    HexFinding { read: u64, total: u64 },
    /// A sample's rows are cut to its scope, with these columns: its view can be built.
    SampleBegun(polars::prelude::SchemaRef),
    /// A sample kept another chunk.
    SampleGrew,
}

/// A job whose outcome has been taken: what it was, whether its answer is still
/// wanted, and the outcome.
pub(crate) struct Ended {
    pub(crate) job: Job,
    /// Not superseded: the answer is the one the app is waiting for.
    pub(crate) current: bool,
    /// The footer's line while the user waited, if they still do.
    pub(crate) keys: Option<String>,
    pub(crate) outcome: Outcome,
}

/// Picks the jobs that panic before their work starts; see [`Jobs::worker_dies`].
#[cfg(test)]
pub(crate) type WorkerDies = Box<dyn FnMut(&Job) -> bool + Send>;

/// Picks the jobs whose worker waits before its work starts; see [`Jobs::worker_waits`].
#[cfg(test)]
pub(crate) type WorkerWaits = Box<dyn FnMut(&Job) -> Option<std::sync::mpsc::Receiver<()>> + Send>;

type Slot = Arc<Mutex<Option<Outcome>>>;

/// The record of one job: the only marker the app keeps for it.
struct Record {
    ticket: Ticket,
    job: Job,
    /// Where the worker puts the outcome; `None` for an owed job (no worker yet).
    slot: Option<Slot>,
    /// The footer's line while the user waits on it; set, keys wait.
    keys: Option<String>,
    /// When it was superseded: its answer is stale, and it holds neither generation
    /// nor keys.
    superseded: Option<Instant>,
    /// Superseded by the user's cancel, not replacement: a read still going that a new
    /// one should not start beside.
    cancelled: bool,
    /// Set when it is superseded, for its worker to see ([`superseded`]).
    stale: Arc<std::sync::atomic::AtomicBool>,
}

std::thread_local! {
    /// The stale flag of the job running on this thread, if a job runs on it.
    static RUNNING: std::cell::RefCell<Option<Arc<std::sync::atomic::AtomicBool>>> =
        const { std::cell::RefCell::new(None) };
}

/// Whether this thread's job was superseded: its answer will be dropped, so a wait
/// inside may give up. False off a job's thread.
pub(crate) fn superseded() -> bool {
    RUNNING.with(|running| {
        running
            .borrow()
            .as_ref()
            .is_some_and(|stale| stale.load(std::sync::atomic::Ordering::Relaxed))
    })
}

impl Record {
    fn running(&self) -> bool {
        self.slot.is_some()
    }

    fn current(&self) -> bool {
        self.running() && self.superseded.is_none()
    }

    /// Whether a bump of the generation would strand its answer.
    fn holds(&self, generation: u64) -> bool {
        self.current() && self.job.leased() && self.ticket.generation == generation
    }

    fn supersede(&mut self, now: Instant) {
        self.superseded = Some(now);
        self.keys = None;
        self.stale.store(true, std::sync::atomic::Ordering::Relaxed);
    }
}

/// A started job until its outcome is in. [`Started::run`] hands it to a worker;
/// dropped without ending, it ends as failed, so no record waits forever.
pub(crate) struct Started {
    ticket: Ticket,
    slot: Slot,
    events: Sender<AppEvent>,
    ended: bool,
    stale: Arc<std::sync::atomic::AtomicBool>,
    #[cfg(test)]
    dies: bool,
    #[cfg(test)]
    waits: Option<std::sync::mpsc::Receiver<()>>,
}

impl Started {
    pub(crate) fn ticket(&self) -> Ticket {
        self.ticket
    }

    /// Run `work` on a blocking thread of `runtime`; its answer, error or panic is the
    /// outcome. What `work` holds is dropped before the outcome is sent.
    pub(crate) fn run<F, R>(self, runtime: &tokio::runtime::Handle, work: F)
    where
        F: FnOnce(&Worker) -> Result<R, String> + Send + 'static,
        R: Into<Answered>,
    {
        let mut started = self;
        runtime.spawn_blocking(move || {
            let worker = Worker {
                ticket: started.ticket,
                events: started.events.clone(),
            };
            #[cfg(test)]
            let dies = started.dies;
            // A send or a dropped sender both let it go, so a failing test frees it.
            #[cfg(test)]
            if let Some(gate) = started.waits.take() {
                let _ = gate.recv();
            }
            RUNNING.with(|running| *running.borrow_mut() = Some(started.stale.clone()));
            let ran = logging::catch_panic(|| {
                #[cfg(test)]
                if dies {
                    panic!("worker died");
                }
                work(&worker)
            });
            RUNNING.with(|running| *running.borrow_mut() = None);
            match ran {
                Ok(Ok(answered)) => {
                    let Answered { answer, then } = answered.into();
                    started.finish(Outcome::Answered(Box::new(answer)));
                    if let Some(then) = then {
                        then();
                    }
                }
                Ok(Err(message)) => started.finish(Outcome::Failed {
                    message,
                    panicked: false,
                }),
                Err(message) => started.finish(Outcome::Failed {
                    message,
                    panicked: true,
                }),
            }
        });
    }

    /// End the job with `outcome` here, with no worker.
    #[cfg(test)]
    pub(crate) fn end(mut self, outcome: Outcome) {
        self.finish(outcome);
    }

    fn finish(&mut self, outcome: Outcome) {
        if std::mem::replace(&mut self.ended, true) {
            return;
        }
        *self.slot.lock().unwrap_or_else(|e| e.into_inner()) = Some(outcome);
        // Nobody to tell: the app is going, so drop what the outcome holds (a downloaded
        // file) now.
        if self.events.send(AppEvent::JobEnded(self.ticket)).is_err() {
            drop(self.slot.lock().unwrap_or_else(|e| e.into_inner()).take());
        }
    }
}

impl Drop for Started {
    fn drop(&mut self) {
        self.finish(Outcome::Failed {
            message: "The background task stopped before it answered".to_string(),
            panicked: true,
        });
    }
}

/// What a running job's worker has: its ticket, and a way to report progress.
pub(crate) struct Worker {
    ticket: Ticket,
    events: Sender<AppEvent>,
}

impl Worker {
    /// A progress reporter; the app takes reports only while the job is current.
    pub(crate) fn reporter(&self) -> impl Fn(Progress) + Send + Sync + 'static {
        let (ticket, events) = (self.ticket, Mutex::new(self.events.clone()));
        move |progress| {
            let events = events.lock().unwrap_or_else(|e| e.into_inner());
            let _ = events.send(AppEvent::JobProgress { ticket, progress });
        }
    }

    /// Send an event not this job's: something read that is worth keeping whatever
    /// becomes of the job.
    pub(crate) fn send(&self, event: AppEvent) {
        let _ = self.events.send(event);
    }
}

type HoldCounts = Arc<Mutex<HashMap<u64, usize>>>;

/// A hold on the generation by work that is not a running job: a continuation
/// [`crate::event_pump::EventPump`] has not dispatched, the gap between an errand's
/// phases, a download waiting on the user. Released on drop.
#[must_use]
pub(crate) struct Hold {
    generation: u64,
    counts: HoldCounts,
}

impl Drop for Hold {
    fn drop(&mut self) {
        let mut counts = self.counts.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(n) = counts.get_mut(&self.generation) {
            *n = n.saturating_sub(1);
            if *n == 0 {
                counts.remove(&self.generation);
            }
        }
    }
}

/// The owner of every general background operation: the generation, one record per
/// job until handled, and the non-job holds.
pub(crate) struct Jobs {
    events: Sender<AppEvent>,
    /// The generation answers are judged by; advancing it makes following jobs' answers
    /// stale.
    generation: u64,
    next_id: u64,
    records: Vec<Record>,
    holds: HoldCounts,
    /// Which jobs panic before starting, for tests.
    #[cfg(test)]
    pub(crate) worker_dies: Option<WorkerDies>,
    /// Which jobs' workers wait on the returned receiver before starting, so tests can
    /// catch a job mid-step without a race.
    #[cfg(test)]
    pub(crate) worker_waits: Option<WorkerWaits>,
}

impl Jobs {
    pub(crate) fn new(events: Sender<AppEvent>) -> Self {
        Self {
            events,
            generation: 0,
            next_id: 0,
            records: Vec::new(),
            holds: Arc::default(),
            #[cfg(test)]
            worker_dies: None,
            #[cfg(test)]
            worker_waits: None,
        }
    }

    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }

    fn ticket(&mut self, job: &Job) -> Ticket {
        self.next_id = self.next_id.wrapping_add(1);
        Ticket {
            id: self.next_id,
            generation: self.generation,
            kind: job.kind(),
        }
    }

    /// Record `job` as started on the current generation. With `keys` the user waits:
    /// keys are held until it ends or is superseded, the footer showing `keys`.
    pub(crate) fn start(&mut self, job: Job, keys: Option<&str>) -> Started {
        let ticket = self.ticket(&job);
        #[cfg(test)]
        let dies = self.worker_dies.as_mut().is_some_and(|dies| dies(&job));
        #[cfg(test)]
        let waits = self.worker_waits.as_mut().and_then(|waits| waits(&job));
        let slot = Slot::default();
        let stale = Arc::<std::sync::atomic::AtomicBool>::default();
        self.records.push(Record {
            ticket,
            job,
            slot: Some(slot.clone()),
            keys: keys.map(str::to_string),
            superseded: None,
            cancelled: false,
            stale: stale.clone(),
        });
        Started {
            ticket,
            slot,
            events: self.events.clone(),
            ended: false,
            stale,
            #[cfg(test)]
            dies,
            #[cfg(test)]
            waits,
        }
    }

    /// Record `job` as owed until the generation is free (waited on with `keys`).
    /// Nothing runs until the app takes it back with [`Self::take_owed`].
    pub(crate) fn owe(&mut self, job: Job, keys: Option<&str>) {
        let ticket = self.ticket(&job);
        self.records.push(Record {
            ticket,
            job,
            slot: None,
            keys: keys.map(str::to_string),
            superseded: None,
            cancelled: false,
            stale: Arc::default(),
        });
    }

    /// The owed job `which` picks, if there is one.
    pub(crate) fn owed(&self, which: impl Fn(&Job) -> bool) -> Option<&Job> {
        self.records
            .iter()
            .find(|r| !r.running() && which(&r.job))
            .map(|r| &r.job)
    }

    /// Take back the owed job `which` picks, to run or to put down.
    pub(crate) fn take_owed(&mut self, which: impl Fn(&Job) -> bool) -> Option<Job> {
        let at = self
            .records
            .iter()
            .position(|r| !r.running() && which(&r.job))?;
        Some(self.records.remove(at).job)
    }

    /// Take the outcome of `ticket`'s job and remove its record; `None` if no record or
    /// no outcome yet. Removing it here means the job holds generation and keys until
    /// the app has its answer, and no longer.
    pub(crate) fn end(&mut self, ticket: Ticket) -> Option<Ended> {
        let at = self.records.iter().position(|r| r.ticket == ticket)?;
        let outcome = self.records[at]
            .slot
            .as_ref()?
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()?;
        let record = self.records.remove(at);
        Some(Ended {
            job: record.job,
            current: record.superseded.is_none(),
            keys: record.keys,
            outcome,
        })
    }

    /// Whether the job `ticket` names is running and its answer still wanted.
    pub(crate) fn is_current(&self, ticket: Ticket) -> bool {
        self.records
            .iter()
            .any(|r| r.ticket == ticket && r.current())
    }

    /// The newest running job `which` picks whose answer is still wanted.
    pub(crate) fn current(&self, which: impl Fn(&Job) -> bool) -> Option<(Ticket, &Job)> {
        self.records
            .iter()
            .rev()
            .find(|r| r.current() && which(&r.job))
            .map(|r| (r.ticket, &r.job))
    }

    /// As [`Self::current`], to change what the record says about the job.
    pub(crate) fn current_mut(&mut self, which: impl Fn(&Job) -> bool) -> Option<&mut Job> {
        self.records
            .iter_mut()
            .rev()
            .find(|r| r.current() && which(&r.job))
            .map(|r| &mut r.job)
    }

    /// What the record of the job `ticket` names says about it, to change it.
    pub(crate) fn job_mut(&mut self, ticket: Ticket) -> Option<&mut Job> {
        self.records
            .iter_mut()
            .find(|r| r.ticket == ticket)
            .map(|r| &mut r.job)
    }

    /// The newest job `which` picks that the user cancelled and whose worker still runs,
    /// with when it was cancelled.
    pub(crate) fn cancelled_running(
        &self,
        which: impl Fn(&Job) -> bool,
    ) -> Option<(Instant, &Job)> {
        self.records
            .iter()
            .filter(|r| r.running() && r.cancelled && which(&r.job))
            .filter_map(|r| r.superseded.map(|since| (since, &r.job)))
            .max_by_key(|(since, _)| *since)
    }

    /// Whether a job `which` picks is running, its answer wanted or not.
    pub(crate) fn running(&self, which: impl Fn(&Job) -> bool) -> bool {
        self.records.iter().any(|r| r.running() && which(&r.job))
    }

    /// Whether a job still holding the keys shows `status` on the footer.
    pub(crate) fn shows(&self, status: &str) -> bool {
        self.records
            .iter()
            .any(|r| r.keys.as_deref() == Some(status))
    }

    /// Whether advancing the generation now would throw away an answer nothing will ask
    /// for again: a stranded job or a hold. Asked of the records, not of a list of
    /// kinds of work.
    pub(crate) fn would_strand(&self) -> bool {
        self.records.iter().any(|r| r.holds(self.generation)) || self.held(self.generation)
    }

    fn held(&self, generation: u64) -> bool {
        self.holds
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&generation)
            .is_some_and(|n| *n > 0)
    }

    /// Whether the user is waiting on a job: one running or owed that holds the keys.
    pub(crate) fn holds_keys(&self) -> bool {
        self.records.iter().any(|r| r.keys.is_some())
    }

    /// Whether the user waits on some job, and on none but those `which` picks.
    pub(crate) fn keys_held_only_by(&self, which: impl Fn(&Job) -> bool) -> bool {
        self.holds_keys()
            && self
                .records
                .iter()
                .filter(|r| r.keys.is_some())
                .all(|r| which(&r.job))
    }

    /// Whether the user waits on the newest current job `which` picks.
    pub(crate) fn waited_on(&self, which: impl Fn(&Job) -> bool) -> bool {
        self.records
            .iter()
            .rev()
            .find(|r| r.current() && which(&r.job))
            .is_some_and(|r| r.keys.is_some())
    }

    /// The footer line of the newest waited-on job `which` picks, running or owed.
    pub(crate) fn waiting_status(&self, which: impl Fn(&Job) -> bool) -> Option<&str> {
        self.records
            .iter()
            .rev()
            .filter(|r| r.superseded.is_none() && which(&r.job))
            .find_map(|r| r.keys.as_deref())
    }

    /// The user waits on the running job `which` picks from now on, with `status` on
    /// the footer (a load-ahead a scroll caught up with). Whether there was one.
    pub(crate) fn wait_on(&mut self, which: impl Fn(&Job) -> bool, status: &str) -> bool {
        let Some(record) = self
            .records
            .iter_mut()
            .rev()
            .find(|r| r.current() && which(&r.job))
        else {
            return false;
        };
        record.keys = Some(status.to_string());
        true
    }

    /// Nobody waits on the jobs `which` picks any more, though their answers are still
    /// wanted: keys return to the user. Returns their footer lines.
    pub(crate) fn quiet(&mut self, which: impl Fn(&Job) -> bool) -> Vec<String> {
        self.records
            .iter_mut()
            .filter(|r| which(&r.job))
            .filter_map(|r| r.keys.take())
            .collect()
    }

    /// Advance the generation if that strands nothing. Returns whether it did.
    pub(crate) fn try_advance(&mut self) -> bool {
        if self.would_strand() {
            return false;
        }
        self.advance();
        true
    }

    /// Advance the generation whatever runs (a cancel, a new dataset): jobs that follow
    /// it are superseded.
    pub(crate) fn advance(&mut self) {
        self.generation = self.generation.wrapping_add(1);
        let now = Instant::now();
        for record in &mut self.records {
            if record.current() && record.job.follows_the_generation() {
                record.supersede(now);
            }
        }
    }

    /// Supersede the running jobs `which` picks as the user's cancel: as
    /// [`Self::supersede`], and [`Self::cancelled_running`] says so while they run on.
    pub(crate) fn cancel(&mut self, which: impl Fn(&Job) -> bool) -> bool {
        for record in &mut self.records {
            if record.current() && which(&record.job) {
                record.cancelled = true;
            }
        }
        self.supersede(which)
    }

    /// Supersede the running jobs `which` picks and drop the owed ones: stale, holding
    /// neither generation nor keys. Whether there were any.
    pub(crate) fn supersede(&mut self, which: impl Fn(&Job) -> bool) -> bool {
        let now = Instant::now();
        let before = self.records.len();
        self.records.retain(|r| r.running() || !which(&r.job));
        let mut any = self.records.len() != before;
        for record in &mut self.records {
            if record.current() && which(&record.job) {
                record.supersede(now);
                any = true;
            }
        }
        any
    }

    /// Hold the current generation until the hold is dropped.
    pub(crate) fn hold(&self) -> Hold {
        *self
            .holds
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry(self.generation)
            .or_default() += 1;
        Hold {
            generation: self.generation,
            counts: self.holds.clone(),
        }
    }

    /// Whether any leased job is still running, superseded or not, or anything holds a
    /// generation.
    pub(crate) fn in_flight(&self) -> bool {
        self.records.iter().any(|r| r.running() && r.job.leased())
            || self
                .holds
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .values()
                .any(|n| *n > 0)
    }

    /// Whether leased work a cancel passed still runs (started on a generation since
    /// left).
    pub(crate) fn running_behind(&self) -> bool {
        self.records
            .iter()
            .any(|r| r.running() && r.job.leased() && r.ticket.generation != self.generation)
            || self
                .holds
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .iter()
                .any(|(generation, n)| *generation != self.generation && *n > 0)
    }

    /// Move every supersession back by `by`, for tests of cancel timing.
    #[cfg(test)]
    pub(crate) fn backdate_supersessions(&mut self, by: std::time::Duration) {
        for record in &mut self.records {
            if let Some(since) = record.superseded.as_mut() {
                *since -= by;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc::{Receiver, channel};
    use std::time::Duration;

    fn jobs() -> (Jobs, Receiver<AppEvent>) {
        let (tx, rx) = channel();
        (Jobs::new(tx), rx)
    }

    fn spawn<F, R>(jobs: &mut Jobs, job: Job, work: F) -> Ticket
    where
        F: FnOnce(&Worker) -> Result<R, String> + Send + 'static,
        R: Into<Answered>,
    {
        let started = jobs.start(job, None);
        let ticket = started.ticket();
        started.run(&crate::tests::test_runtime(), work);
        ticket
    }

    /// The ticket of the next `JobEnded`, waiting for it.
    fn ended(rx: &Receiver<AppEvent>) -> Ticket {
        match rx.recv_timeout(Duration::from_secs(30)) {
            Ok(AppEvent::JobEnded(ticket)) => ticket,
            Ok(_) => panic!("another event"),
            Err(e) => panic!("the job never ended: {e}"),
        }
    }

    fn quiet(rx: &Receiver<AppEvent>) {
        assert!(
            rx.recv_timeout(Duration::from_millis(50)).is_err(),
            "nothing more"
        );
    }

    /// A job that answers ends once, current, with its answer; it holds the
    /// generation until then and not after.
    #[test]
    fn an_answer_ends_its_job_once() {
        let (mut jobs, rx) = jobs();
        let (go, wait) = channel::<()>();
        let ticket = spawn(&mut jobs, Job::Export, move |_| {
            wait.recv().ok();
            Ok(Answer::Exported(PathBuf::from("out.csv")))
        });
        assert_eq!(ticket.kind(), JobKind::Export);
        assert!(jobs.would_strand(), "a bump would strand it");
        assert!(!jobs.try_advance(), "so the generation stays");
        assert!(jobs.is_current(ticket));

        go.send(()).unwrap();
        assert_eq!(ended(&rx), ticket);
        assert!(
            jobs.would_strand(),
            "its record stands until the app has the answer"
        );
        let ended = jobs.end(ticket).expect("the outcome is in");
        assert!(ended.current);
        assert!(matches!(
            ended.outcome,
            Outcome::Answered(ref answer) if matches!(**answer, Answer::Exported(_))
        ));
        assert!(!jobs.would_strand(), "and goes with it");
        assert!(jobs.end(ticket).is_none(), "once");
        quiet(&rx);
        assert!(jobs.try_advance());
    }

    /// An error and a panic end the job the same way: one failure, then nothing.
    #[test]
    fn an_error_and_a_panic_each_end_a_job_once() {
        let (mut jobs, rx) = jobs();
        for panics in [false, true] {
            let ticket = spawn(&mut jobs, Job::QualityReport, move |_| {
                assert!(!panics, "worker died");
                Err::<Answer, _>("disk full".to_string())
            });
            assert_eq!(ended(&rx), ticket);
            let ended = jobs.end(ticket).expect("the outcome is in");
            match ended.outcome {
                Outcome::Failed { message, panicked } => {
                    assert_eq!(panicked, panics);
                    assert!(message.contains(if panics { "worker died" } else { "disk full" }));
                }
                Outcome::Answered(_) => panic!("it failed"),
            }
            quiet(&rx);
            assert!(!jobs.would_strand());
        }
    }

    /// A job started and never run still ends: dropped, it fails.
    #[test]
    fn a_job_dropped_unrun_ends_failed() {
        let (mut jobs, rx) = jobs();
        let started = jobs.start(Job::Pivot, None);
        let ticket = started.ticket();
        assert!(jobs.would_strand());
        drop(started);
        assert_eq!(ended(&rx), ticket);
        assert!(matches!(
            jobs.end(ticket).map(|e| e.outcome),
            Some(Outcome::Failed { panicked: true, .. })
        ));
        assert!(!jobs.would_strand());
    }

    /// Advancing the generation supersedes the jobs that follow it: they hold nothing
    /// up at once, their outcome still arrives, as stale, and whatever it carries is
    /// dropped with it. A job that follows its own owner is left alone.
    #[test]
    fn a_superseded_job_ends_stale_and_lets_go() {
        let (mut jobs, rx) = jobs();
        let held = Arc::new(());
        let analysis = || Job::Analysis(AnalysisRun::default());
        let is_analysis = |job: &Job| matches!(job, Job::Analysis(_));
        let stale = jobs.start(analysis(), Some("Running analysis..."));
        let own = jobs.start(
            Job::ChartExport {
                path: PathBuf::from("chart.png"),
                format: crate::chart::chart_export::ChartExportFormat::Png,
            },
            None,
        );
        let (stale_ticket, own_ticket) = (stale.ticket(), own.ticket());
        assert!(jobs.would_strand());
        assert!(jobs.holds_keys());

        jobs.advance();
        assert!(!jobs.is_current(stale_ticket), "the analysis is superseded");
        assert!(jobs.is_current(own_ticket), "the chart export is not");
        assert!(!jobs.would_strand(), "and neither holds the new generation");
        assert!(!jobs.holds_keys(), "nor the keys");
        assert!(jobs.running_behind(), "though it runs on");
        assert!(
            jobs.cancelled_running(is_analysis).is_none(),
            "superseded, not cancelled by the user"
        );

        stale.end(Outcome::answered(Answer::Probe(held.clone())));
        assert_eq!(ended(&rx), stale_ticket);
        let stale_end = jobs.end(stale_ticket).expect("its outcome still arrives");
        assert!(!stale_end.current, "as stale");
        drop(stale_end);
        assert_eq!(Arc::strong_count(&held), 1, "and what it carried is let go");
        drop(own);
        assert_eq!(ended(&rx), own_ticket);
        assert!(jobs.end(own_ticket).is_some_and(|e| e.current));
        assert!(!jobs.running_behind());
    }

    /// A cancel supersedes only what it picks: the job's keys and lease go at once,
    /// its stale outcome still arrives, and the job beside it is untouched.
    #[test]
    fn a_cancel_supersedes_only_what_it_picks() {
        let (mut jobs, rx) = jobs();
        let look = |path: &str| {
            Job::Classify(Classify {
                path: PathBuf::from(path),
                browsing: None,
                jump: false,
            })
        };
        let older = jobs.start(look("/a"), Some("Looking..."));
        let pivot = jobs.start(Job::Pivot, Some("Computing pivot..."));
        assert!(jobs.supersede(|job| matches!(job, Job::Classify(_))));
        let newer = jobs.start(look("/b"), Some("Looking..."));
        assert!(
            jobs.is_current(pivot.ticket()),
            "the cancel picked looks only"
        );
        assert!(
            jobs.current(|job| matches!(job, Job::Classify(c) if c.path.ends_with("b")))
                .is_some(),
            "the newer look is the one waited on"
        );

        let ticket = older.ticket();
        older.end(Outcome::Failed {
            message: "gone".to_string(),
            panicked: false,
        });
        assert_eq!(ended(&rx), ticket);
        let ended_older = jobs.end(ticket).expect("its outcome arrives");
        assert!(!ended_older.current);
        assert!(ended_older.keys.is_none(), "it holds no keys to give back");
        assert!(jobs.is_current(newer.ticket()));
        assert!(jobs.holds_keys());
        drop((newer, pivot));
    }

    /// A job the user waits on holds the keys until its answer is handled, and says
    /// so; a quiet one does not, a scroll can start waiting on it, and a job put back
    /// to quiet lets the keys go without losing its answer.
    #[test]
    fn keys_belong_to_the_job_the_user_waits_on() {
        let (mut jobs, rx) = jobs();
        let rows = || Job::Rows(crate::InflightCollect::for_tests(0, 100));
        let is_rows = |job: &Job| matches!(job, Job::Rows(_));
        let ahead = jobs.start(rows(), None);
        assert!(!jobs.holds_keys(), "a load-ahead holds no keys");
        assert!(jobs.wait_on(is_rows, "Loading buffer..."));
        assert!(
            jobs.holds_keys(),
            "a scroll that caught up with it waits on it"
        );
        jobs.quiet(is_rows);
        assert!(!jobs.holds_keys());
        assert!(jobs.wait_on(is_rows, "Loading buffer..."));

        let ticket = ahead.ticket();
        ahead.end(Outcome::Failed {
            message: "no rows".to_string(),
            panicked: false,
        });
        assert_eq!(ended(&rx), ticket);
        assert!(jobs.holds_keys(), "until the app has the answer");
        let ended = jobs.end(ticket).expect("the outcome is in");
        assert_eq!(ended.keys.as_deref(), Some("Loading buffer..."));
        assert!(!jobs.holds_keys());
    }

    /// An owed page holds the keys but not the generation, outlives an advance, and is
    /// taken back once to run, or dropped by a cancel.
    #[test]
    fn an_owed_page_waits_for_the_generation() {
        let (mut jobs, _rx) = jobs();
        let hold = jobs.hold();
        let owed = |job: &Job| matches!(job, Job::OwedRows { .. });
        jobs.owe(
            Job::OwedRows {
                dataset: 3,
                status: "Loading buffer...".to_string(),
            },
            Some("Loading buffer..."),
        );
        assert!(jobs.holds_keys(), "the user waits on it");
        drop(hold);
        assert!(!jobs.would_strand(), "it holds no generation of its own");
        assert!(!jobs.in_flight(), "and nothing runs for it");
        jobs.advance();
        assert!(
            matches!(jobs.owed(owed), Some(Job::OwedRows { dataset: 3, .. })),
            "an advance does not put it down: it belongs to its dataset"
        );
        assert!(jobs.take_owed(owed).is_some());
        assert!(jobs.take_owed(owed).is_none(), "taken once");
        assert!(!jobs.holds_keys());

        jobs.owe(
            Job::OwedRows {
                dataset: 4,
                status: String::new(),
            },
            Some("Loading buffer..."),
        );
        assert!(jobs.supersede(owed));
        assert!(jobs.owed(owed).is_none(), "a cancel drops it");
        assert!(!jobs.holds_keys());
    }

    /// How long a cancelled job has been going is the record's to say.
    #[test]
    fn a_cancelled_job_says_since_when() {
        let (mut jobs, _rx) = jobs();
        let run = jobs.start(
            Job::Analysis(AnalysisRun {
                watch: None,
                runs_out: true,
            }),
            None,
        );
        assert!(jobs.cancel(|job| matches!(job, Job::Analysis(_))));
        assert!(!jobs.is_current(run.ticket()));
        let (since, job) = jobs
            .cancelled_running(|job| matches!(job, Job::Analysis(_)))
            .expect("still running");
        assert!(matches!(
            job,
            Job::Analysis(AnalysisRun { runs_out: true, .. })
        ));
        let before = since;
        jobs.backdate_supersessions(std::time::Duration::from_secs(5));
        let (since, _) = jobs
            .cancelled_running(|job| matches!(job, Job::Analysis(_)))
            .expect("still running");
        assert!(since < before);
        drop(run);
    }

    /// The stale end of a passed job leaves the newer one of its kind alone.
    #[test]
    fn a_stale_end_leaves_the_newer_job_alone() {
        let (mut jobs, rx) = jobs();
        let older = jobs.start(Job::Pivot, None);
        jobs.advance();
        let newer = jobs.start(Job::Pivot, None);
        let (older_ticket, newer_ticket) = (older.ticket(), newer.ticket());

        older.end(Outcome::Failed {
            message: "gone".to_string(),
            panicked: false,
        });
        assert_eq!(ended(&rx), older_ticket);
        assert!(jobs.end(older_ticket).is_some_and(|e| !e.current));
        assert!(
            jobs.is_current(newer_ticket),
            "the newer pivot is still waited on"
        );
        assert!(jobs.would_strand());
        drop(newer);
    }

    /// Records are let go with the owner: an outcome nobody took is dropped, and with
    /// it whatever it carried.
    #[test]
    fn an_outcome_nobody_takes_is_dropped_with_the_owner() {
        let (mut jobs, rx) = jobs();
        let held = Arc::new(());
        let started = jobs.start(Job::Copy, None);
        started.end(Outcome::answered(Answer::Probe(held.clone())));
        assert!(matches!(rx.try_recv(), Ok(AppEvent::JobEnded(_))));
        assert_eq!(Arc::strong_count(&held), 2, "held by the record");
        drop(jobs);
        assert_eq!(Arc::strong_count(&held), 1);
    }

    /// A hold keeps the generation across the gap between two phases of one errand
    /// and lets go when dropped; a hold on a passed generation keeps nothing.
    #[test]
    fn a_hold_keeps_the_generation_until_dropped() {
        let (mut jobs, _rx) = jobs();
        let hold = jobs.hold();
        assert!(jobs.would_strand());
        assert!(jobs.in_flight());
        assert!(!jobs.try_advance());
        drop(hold);
        assert!(!jobs.would_strand());
        assert!(!jobs.in_flight());

        let passed = jobs.hold();
        jobs.advance();
        assert!(
            !jobs.would_strand(),
            "a passed generation's hold holds nothing up"
        );
        assert!(jobs.running_behind());
        drop(passed);
        assert!(!jobs.running_behind());
    }

    /// The rows and the looks at named paths hold no lease: a bump does not wait for
    /// them, and supersedes them.
    #[test]
    fn replaceable_jobs_hold_no_lease() {
        let (mut jobs, _rx) = jobs();
        let rows = jobs.start(Job::Rows(crate::InflightCollect::for_tests(0, 100)), None);
        let load = crate::loading::LoadId::for_tests(1);
        let path = PathBuf::from("/data");
        let look = jobs.start(Job::LookAtDirectory { load, path }, None);
        let named = jobs.start(Job::OpenNamed(load), None);
        let facts = jobs.start(Job::FileFacts { dataset: 1 }, None);
        let footers = jobs.start(Job::FootersJoin { dataset: 1 }, None);
        let journal = jobs.start(Job::JournalDetail { dataset: 1 }, None);
        let index = jobs.start(Job::IndexLines { dataset: 1 }, None);
        let mut modal = crate::chart::chart_modal::ChartModal::new();
        modal.spec.encoding.x.field = Some("a".into());
        let chart = jobs.start(
            Job::ChartPrepare(Box::new(ChartPrep {
                request: crate::ChartRequest::from_modal(&modal).expect("a request"),
                dataset: Some(1),
                cancel: Arc::default(),
            })),
            None,
        );
        assert!(!jobs.would_strand());
        assert!(jobs.try_advance());
        assert!(!jobs.is_current(rows.ticket()));
        assert!(!jobs.is_current(look.ticket()));
        assert!(!jobs.is_current(named.ticket()));
        assert!(
            jobs.is_current(facts.ticket()),
            "file facts follow the dataset"
        );
        // The footer pass outlives every collect of its dataset, which bumps the
        // generation; its answer is judged by the dataset alone. So is a journal's.
        assert!(jobs.is_current(footers.ticket()));
        assert!(jobs.is_current(journal.ticket()));
        assert!(jobs.is_current(index.ticket()));
        // A chart's data is judged by its dataset and put down with its view.
        assert!(jobs.is_current(chart.ticket()));
    }

    /// Progress is sent with the job's ticket; what runs after the answer runs after
    /// it is sent.
    #[test]
    fn a_worker_reports_and_runs_on_after_its_answer() {
        let (mut jobs, rx) = jobs();
        let ticket = spawn(&mut jobs, Job::Export, |worker| {
            let report = worker.reporter();
            report(Progress::ExportWriting {
                phase: "Writing",
                bytes: 7,
            });
            let events = worker.events.clone();
            Ok(Answer::Exported(PathBuf::from("x")).then(move || {
                let _ = events.send(AppEvent::Wake);
            }))
        });
        let mut seen = Vec::new();
        for _ in 0..3 {
            seen.push(rx.recv_timeout(Duration::from_secs(30)).expect("an event"));
        }
        assert!(matches!(
            seen[0],
            AppEvent::JobProgress {
                ticket: t,
                progress: Progress::ExportWriting { bytes: 7, .. }
            } if t == ticket
        ));
        assert!(matches!(seen[1], AppEvent::JobEnded(t) if t == ticket));
        assert!(matches!(seen[2], AppEvent::Wake), "after the answer");
    }

    /// A worker that panics before its work starts, as `worker_dies` makes it.
    #[test]
    fn a_worker_picked_to_die_fails_its_job() {
        let (mut jobs, rx) = jobs();
        jobs.worker_dies = Some(Box::new(|job| matches!(job, Job::DrillRow)));
        let ticket = spawn(&mut jobs, Job::DrillRow, |_| {
            Ok(Answer::Exported(PathBuf::from("never")))
        });
        assert_eq!(ended(&rx), ticket);
        assert!(matches!(
            jobs.end(ticket).map(|e| e.outcome),
            Some(Outcome::Failed { panicked: true, .. })
        ));
    }

    /// A worker picked to wait does no work until it is let go.
    #[test]
    fn a_worker_picked_to_wait_runs_once_let_go() {
        let (mut jobs, rx) = jobs();
        let (waits, release) = crate::tests::worker_waits_once(|job| matches!(job, Job::Pivot));
        jobs.worker_waits = waits;
        let ticket = spawn(&mut jobs, Job::Pivot, |_| {
            Ok(Answer::Exported(PathBuf::from("done")))
        });
        assert!(rx.try_recv().is_err(), "it waits");
        assert!(jobs.is_current(ticket));
        release.send(()).unwrap();
        assert_eq!(ended(&rx), ticket);
        assert!(matches!(
            jobs.end(ticket).map(|e| e.outcome),
            Some(Outcome::Answered(_))
        ));
    }
}

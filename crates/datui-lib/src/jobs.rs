//! Background operations, and their one owner.
//!
//! [`Jobs`] starts every general background operation the app runs and keeps one
//! record for each until its outcome has been handled: which operation it is
//! ([`Job`]), the [`Ticket`] that names it, whether a bump of the generation would
//! strand it, and whether its answer is still wanted.
//!
//! - **One outcome.** A worker returns its [`Answer`] or an error; a panic is caught.
//!   The outcome goes into the record, then [`AppEvent::JobEnded`] says so. A worker
//!   cannot answer twice or forget to, and a [`Started`] job dropped without running
//!   ends as failed.
//! - **Acceptance and release in one step.** [`Jobs::end`] hands the outcome over
//!   with the record, saying whether the job is still current, and the record goes in
//!   the same call. Whatever the app starts while handling the answer holds the
//!   generation before anything else can look at it, so the generation never reads
//!   free between two phases of one errand (#221). Nor does a job hold it after its
//!   answer is handled: there is no release left to arrive behind the answer, and no
//!   frame in which an idle app finds a finished job still holding it (#490).
//! - **Supersession.** Advancing the generation supersedes the jobs it scopes, and
//!   [`Jobs::supersede`] cancels others. A superseded job stops holding the
//!   generation at once; its worker runs on, and its outcome still arrives, stale, so
//!   whatever it carries is dropped then.
//! - **Holds.** Work that is not a running job also holds the generation: a
//!   continuation the event pump has not dispatched, and a download waiting on the
//!   user ([`Hold`]).
//!
//! Not owned here: the row count (`OwedCount`), the footer pass, chart preparation
//! and the home screen's workers. Each is keyed by something other than the
//! generation and answers what its own marker waits for.
//!
//! One handoff goes around the holds: `reread_after_the_footers_joined` sends its
//! jump straight to the channel, which is safe only because both its callers have
//! already checked that nothing would be stranded. Another handoff added that way
//! would not be.
//!
//! A worker that never returns at all (a `hard` NFS mount, a wedged object-store
//! read) never ends its job. Its keys and its lease stay with it until it is
//! superseded; cancelling a stalled syscall is out of reach.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::mpsc::Sender;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use polars::prelude::{DataFrame, LazyFrame};

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
    Pivot,
    ViewPivot,
    DrillRow,
    InspectRow,
    Export,
    Copy,
    QualityReport,
    FileFacts,
    ChartExport,
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

    /// The generation it started on.
    pub fn generation(self) -> u64 {
        self.generation
    }
}

/// A background operation, named where it starts.
///
/// The fields are what tells this operation from a newer one of the same kind where
/// the generation cannot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Job {
    /// A phase of an open before its first rows: the size probe, a download, the scan,
    /// a decompression, the schema.
    Load,
    /// Whether the paths named on the command line are there, and which is a
    /// directory.
    OpenNamed,
    /// The look at a directory named on the command line, before it is opened.
    LookAtDirectory(PathBuf),
    /// A look at a path chosen on the home screen, by request.
    Classify(u64),
    /// The table's rows: a page the table waits on, or a load-ahead.
    Rows,
    /// An Analysis tool's computation.
    Analysis,
    /// The sample, or the rows behind a finding, read to show as a table.
    SampleRows,
    /// A pivot from the Pivot & Melt form.
    Pivot,
    /// A view's pivot, read before the view's rows.
    ViewPivot,
    /// The group row Enter drills into, when the buffer did not hold it.
    DrillRow,
    /// The inspector's fields of one row that the buffer does not hold: row `row` of
    /// frame `frame`.
    InspectRow { frame: u64, row: usize },
    /// An export, from plan to committed file.
    Export,
    /// Collecting and formatting the view for a copy.
    Copy,
    /// Writing the Data Quality report.
    QualityReport,
    /// Reading the open file's size and footer for the Info panel. `dataset` is the
    /// `dataset_generation` it was read for: the generation does not tell one dataset's
    /// read from the next.
    FileFacts { dataset: u64 },
    /// Writing a chart. `generation` is `chart_export_generation`'s; the path and
    /// format reopen the form on a failure.
    ChartExport {
        generation: u64,
        path: PathBuf,
        format: crate::chart_export::ChartExportFormat,
    },
}

impl Job {
    pub(crate) fn kind(&self) -> JobKind {
        match self {
            Job::Load => JobKind::Load,
            Job::OpenNamed => JobKind::OpenNamed,
            Job::LookAtDirectory(_) => JobKind::LookAtDirectory,
            Job::Classify(_) => JobKind::Classify,
            Job::Rows => JobKind::Rows,
            Job::Analysis => JobKind::Analysis,
            Job::SampleRows => JobKind::SampleRows,
            Job::Pivot => JobKind::Pivot,
            Job::ViewPivot => JobKind::ViewPivot,
            Job::DrillRow => JobKind::DrillRow,
            Job::InspectRow { .. } => JobKind::InspectRow,
            Job::Export => JobKind::Export,
            Job::Copy => JobKind::Copy,
            Job::QualityReport => JobKind::QualityReport,
            Job::FileFacts { .. } => JobKind::FileFacts,
            Job::ChartExport { .. } => JobKind::ChartExport,
        }
    }

    /// Whether a bump of the generation waits for this job's answer.
    ///
    /// A bump makes every answer on the generation it leaves stale, and nothing asks
    /// for a stale answer again: an export that never writes its file, an analysis left
    /// on its spinner, a dataset that never opens. So the bump waits, except for these:
    ///
    /// - the buffer's rows, whose answer, thrown away, is simply asked for again, and
    ///   which, waited on, would make every page wait on the last one;
    /// - the looks at a path named on the command line, whose answers are meant to be
    ///   thrown away when the user moves on (Ctrl+O out of a long look must not hold
    ///   the next dataset's rows behind it);
    /// - the Info panel's file facts, judged by the dataset rather than the generation.
    fn leased(&self) -> bool {
        !matches!(
            self,
            Job::Rows | Job::OpenNamed | Job::LookAtDirectory(_) | Job::FileFacts { .. }
        )
    }

    /// Whether advancing the generation makes this job's answer stale. The Info
    /// panel's facts belong to a dataset, and a chart export to the chart view, each
    /// put down by its own owner.
    fn follows_the_generation(&self) -> bool {
        !matches!(self, Job::FileFacts { .. } | Job::ChartExport { .. })
    }
}

/// What a job's worker sends back when it succeeds. Each belongs to one [`Job`].
pub(crate) enum Answer {
    /// [`Job::Load`]: the scan's frame.
    Scanned {
        lf: LazyFrame,
        path: Option<PathBuf>,
        options: Box<OpenOptions>,
    },
    /// [`Job::Load`]: the dataset, its schema read.
    SchemaRead {
        state: Box<crate::widgets::datatable::DataTableState>,
        path: Option<PathBuf>,
        options: Box<OpenOptions>,
        debug_label: Option<String>,
    },
    /// [`Job::Load`]: the remote file's size, to put the download to the user.
    #[cfg(any(feature = "http", feature = "cloud"))]
    RemoteSize(Box<crate::PendingDownload>),
    /// [`Job::Load`]: the remote file, downloaded. Dropped unhandled, it removes the
    /// file.
    #[cfg(any(feature = "http", feature = "cloud"))]
    Downloaded {
        download: crate::download::TempDownload,
        options: Box<OpenOptions>,
    },
    /// [`Job::OpenNamed`]: the paths are there; `directory` is one to look at first.
    NamedPaths {
        paths: Vec<PathBuf>,
        options: Box<OpenOptions>,
        directory: Option<PathBuf>,
    },
    /// [`Job::OpenNamed`]: a path named is not there.
    NamedPathMissing(PathBuf),
    /// [`Job::LookAtDirectory`]: what the look found. `holds` is what a cloud
    /// directory's listing found, which picks its reader.
    LookedAt {
        path: PathBuf,
        kind: crate::discover::EntryKind,
        holds: Option<Box<crate::discover::Holds>>,
        options: Box<OpenOptions>,
    },
    /// [`Job::Classify`]: what the path turned out to be; `None` is a path that is not
    /// there.
    Kind {
        request: u64,
        path: PathBuf,
        found: Option<crate::discover::EntryKind>,
        jump: bool,
    },
    /// [`Job::Rows`]: the rows read.
    Rows(crate::widgets::datatable::CollectResult),
    /// [`Job::Rows`]: the read failed. `conversion` is a value that would not
    /// convert, for the SQL prompt to say in its own words.
    RowsFailed {
        message: String,
        conversion: Option<Box<crate::error_display::ConversionFailure>>,
    },
    /// [`Job::Analysis`]: Describe's statistics.
    Described(crate::statistics::AnalysisResults),
    /// [`Job::Analysis`]: the distributions.
    Distributions(crate::statistics::AnalysisResults),
    /// [`Job::Analysis`]: the correlation matrix.
    Correlations(crate::statistics::AnalysisResults),
    /// [`Job::Analysis`]: a Data Quality report, the rows a sampled run read, and the
    /// plan it ran with.
    DataQuality {
        results: Box<crate::data_quality::DataQualityResults>,
        kept: Option<crate::KeptQualitySample>,
        plan: Box<crate::data_quality::DataQualityPlan>,
    },
    /// [`Job::SampleRows`]: rows to show as a table.
    Sample { df: DataFrame, label: String },
    /// [`Job::Pivot`]: the pivot.
    Pivoted {
        spec: crate::pivot_melt_modal::PivotSpec,
        pivoted: DataFrame,
    },
    /// [`Job::ViewPivot`]: the view's pivot.
    ViewPivoted(DataFrame),
    /// [`Job::DrillRow`]: the group row.
    DrillRow { group_index: usize, row: DataFrame },
    /// [`Job::InspectRow`]: the fields of row `row` of frame `frame`.
    FieldsRead {
        frame: u64,
        row: usize,
        values: DataFrame,
    },
    /// [`Job::Export`]: the file, committed.
    Exported(PathBuf),
    /// [`Job::Copy`]: the view or a field, formatted, and the flash that says what was
    /// copied. The clipboard is written on the event thread, which owns its handle.
    Copied {
        payload: crate::clipboard::Payload,
        message: String,
    },
    /// [`Job::QualityReport`]: the report, written.
    QualityReportWritten(PathBuf),
    /// [`Job::ChartExport`]: the chart, written.
    ChartExported {
        generation: u64,
        path: PathBuf,
        format: crate::chart_export::ChartExportFormat,
    },
    /// [`Job::FileFacts`]: what the file is.
    FileFacts {
        dataset: u64,
        facts: crate::widgets::info::FileFacts,
    },
    /// A test's answer, which says when it is dropped.
    #[cfg(test)]
    Probe(Arc<()>),
}

impl Answer {
    /// This answer, then `after` on the worker's thread once it has been sent. For
    /// work that rides in a job without being part of it: the row count a page read
    /// may answer, which must not hold the page back.
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
    /// Its worker returned an error, or panicked. `panicked` says `message` is an
    /// internal error naming the log rather than a reason the user can act on.
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
    QualityPhase(crate::data_quality::QualityPhase),
}

/// A job whose outcome has been taken: what it was, whether its answer is still
/// wanted, and the outcome.
pub(crate) struct Ended {
    pub(crate) ticket: Ticket,
    pub(crate) job: Job,
    /// Not superseded: the answer is the one the app is waiting for.
    pub(crate) current: bool,
    pub(crate) outcome: Outcome,
}

/// Picks the jobs that panic before their work starts; see [`Jobs::worker_dies`].
#[cfg(test)]
pub(crate) type WorkerDies = Box<dyn FnMut(&Job) -> bool + Send>;

type Slot = Arc<Mutex<Option<Outcome>>>;

/// The record of one job.
struct Record {
    ticket: Ticket,
    job: Job,
    /// Where its worker puts the outcome.
    slot: Slot,
    /// When it was superseded. Its answer is stale and it holds nothing up.
    superseded: Option<Instant>,
}

impl Record {
    /// Whether a bump of the generation would strand its answer.
    fn holds(&self, generation: u64) -> bool {
        self.superseded.is_none() && self.job.leased() && self.ticket.generation == generation
    }
}

/// A job that has been started, until its outcome is in. Whoever holds it ends it:
/// [`Started::run`] hands it to a worker, which ends it with the worker's outcome.
/// Dropped without ending, it ends as failed, so no record waits for an outcome that
/// is never coming.
pub(crate) struct Started {
    ticket: Ticket,
    slot: Slot,
    events: Sender<AppEvent>,
    ended: bool,
    #[cfg(test)]
    dies: bool,
}

impl Started {
    pub(crate) fn ticket(&self) -> Ticket {
        self.ticket
    }

    /// Run `work` on a blocking thread of `runtime`. Its answer, its error or its
    /// panic is the job's outcome. Whatever `work` holds is dropped before the outcome
    /// is sent, so nothing the job used outlives its answer unless the answer carries
    /// it.
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
            let ran = logging::catch_panic(|| {
                #[cfg(test)]
                if dies {
                    panic!("worker died");
                }
                work(&worker)
            });
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

    /// End the job with `outcome` here, with no worker: for tests that need a job in
    /// flight and decide how it ends.
    #[cfg(test)]
    pub(crate) fn end(mut self, outcome: Outcome) {
        self.finish(outcome);
    }

    fn finish(&mut self, outcome: Outcome) {
        if std::mem::replace(&mut self.ended, true) {
            return;
        }
        *self.slot.lock().unwrap_or_else(|e| e.into_inner()) = Some(outcome);
        // Nobody to tell means the app is gone or going: what the outcome holds, a
        // downloaded file say, goes now rather than with the last of the app.
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
    /// A way to report progress, from wherever the work needs it. The app takes a
    /// report only while the job is current.
    pub(crate) fn reporter(&self) -> impl Fn(Progress) + Send + Sync + 'static {
        let (ticket, events) = (self.ticket, Mutex::new(self.events.clone()));
        move |progress| {
            let events = events.lock().unwrap_or_else(|e| e.into_inner());
            let _ = events.send(AppEvent::JobProgress { ticket, progress });
        }
    }

    /// Send an event that is not this job's: something it read that is worth keeping
    /// whatever becomes of the job.
    pub(crate) fn send(&self, event: AppEvent) {
        let _ = self.events.send(event);
    }
}

type HoldCounts = Arc<Mutex<HashMap<u64, usize>>>;

/// A hold on the generation by work that is not a running job: a continuation
/// [`crate::event_pump::EventPump`] has not dispatched, the gap between two phases of
/// one errand, and a download waiting on the user. Released when dropped.
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

/// The owner of every general background operation: the generation they are judged
/// by, one record per job until its outcome is handled, and the holds on the
/// generation that are not jobs.
pub(crate) struct Jobs {
    events: Sender<AppEvent>,
    /// The generation answers are judged by. Advancing it makes the answers of the
    /// jobs that follow it stale.
    generation: u64,
    next_id: u64,
    records: Vec<Record>,
    holds: HoldCounts,
    /// Which jobs panic before their work starts, for tests of what a dying worker
    /// leaves behind.
    #[cfg(test)]
    pub(crate) worker_dies: Option<WorkerDies>,
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
        }
    }

    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }

    /// Record `job` as started on the current generation. The caller runs it, or, in
    /// a test, ends it.
    pub(crate) fn start(&mut self, job: Job) -> Started {
        self.next_id = self.next_id.wrapping_add(1);
        let ticket = Ticket {
            id: self.next_id,
            generation: self.generation,
            kind: job.kind(),
        };
        #[cfg(test)]
        let dies = self.worker_dies.as_mut().is_some_and(|dies| dies(&job));
        let slot = Slot::default();
        self.records.push(Record {
            ticket,
            job,
            slot: slot.clone(),
            superseded: None,
        });
        Started {
            ticket,
            slot,
            events: self.events.clone(),
            ended: false,
            #[cfg(test)]
            dies,
        }
    }

    /// Take the outcome of the job `ticket` names, and its record with it. `None` for
    /// a ticket with no record, or one whose outcome is not in yet.
    ///
    /// The record goes here, in the call that hands its answer over, so a job holds
    /// the generation until the app has its answer and not a moment after.
    pub(crate) fn end(&mut self, ticket: Ticket) -> Option<Ended> {
        let at = self.records.iter().position(|r| r.ticket == ticket)?;
        let outcome = self.records[at]
            .slot
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()?;
        let record = self.records.remove(at);
        Some(Ended {
            ticket,
            job: record.job,
            current: record.superseded.is_none(),
            outcome,
        })
    }

    /// Whether the job `ticket` names is running and its answer still wanted.
    pub(crate) fn is_current(&self, ticket: Ticket) -> bool {
        self.records
            .iter()
            .any(|r| r.ticket == ticket && r.superseded.is_none())
    }

    /// Whether advancing the generation now would throw away an answer nothing will
    /// ask for again: a job it would strand, or a hold on it.
    ///
    /// A count rather than a list of the kinds of work that might be running, which
    /// was found short by one in three consecutive reviews (#221).
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

    /// Advance the generation if that strands nothing. Returns whether it did.
    pub(crate) fn try_advance(&mut self) -> bool {
        if self.would_strand() {
            return false;
        }
        self.advance();
        true
    }

    /// Advance the generation whatever is running: a cancel, or a new dataset taking
    /// the screen. The jobs that follow the generation are superseded; nothing is
    /// left waiting on an answer that will be dropped.
    pub(crate) fn advance(&mut self) {
        self.generation = self.generation.wrapping_add(1);
        let now = Instant::now();
        for record in &mut self.records {
            if record.job.follows_the_generation() && record.superseded.is_none() {
                record.superseded = Some(now);
            }
        }
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
        self.records.iter().any(|r| r.job.leased())
            || self
                .holds
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .values()
                .any(|n| *n > 0)
    }

    /// Whether leased work started on `generation` is still running, superseded or
    /// not.
    pub(crate) fn running_on(&self, generation: u64) -> bool {
        self.records
            .iter()
            .any(|r| r.job.leased() && r.ticket.generation == generation)
            || self.held(generation)
    }

    /// Whether leased work a cancel passed is still running: started on a generation
    /// since left.
    pub(crate) fn running_behind(&self) -> bool {
        self.records
            .iter()
            .any(|r| r.job.leased() && r.ticket.generation != self.generation)
            || self
                .holds
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .iter()
                .any(|(generation, n)| *generation != self.generation && *n > 0)
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
        let started = jobs.start(job);
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
        let started = jobs.start(Job::Pivot);
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
        let stale = jobs.start(Job::Analysis);
        let own = jobs.start(Job::ChartExport {
            generation: 1,
            path: PathBuf::from("chart.png"),
            format: crate::chart_export::ChartExportFormat::Png,
        });
        let (stale_ticket, own_ticket) = (stale.ticket(), own.ticket());
        assert!(jobs.would_strand());

        jobs.advance();
        assert!(!jobs.is_current(stale_ticket), "the analysis is superseded");
        assert!(jobs.is_current(own_ticket), "the chart export is not");
        assert!(!jobs.would_strand(), "and neither holds the new generation");
        assert!(
            jobs.running_on(stale_ticket.generation()),
            "though it runs on"
        );
        assert!(jobs.running_behind());

        stale.end(Outcome::answered(Answer::Probe(held.clone())));
        assert_eq!(ended(&rx), stale_ticket);
        let stale_end = jobs.end(stale_ticket).expect("its outcome still arrives");
        assert!(!stale_end.current, "as stale");
        drop(stale_end);
        assert_eq!(Arc::strong_count(&held), 1, "and what it carried is let go");
        drop(own);
        assert_eq!(ended(&rx), own_ticket);
        assert!(jobs.end(own_ticket).is_some_and(|e| e.current));
        assert!(!jobs.running_on(stale_ticket.generation()));
    }

    /// The stale end of a passed job leaves the newer one of its kind alone.
    #[test]
    fn a_stale_end_leaves_the_newer_job_alone() {
        let (mut jobs, rx) = jobs();
        let older = jobs.start(Job::Pivot);
        jobs.advance();
        let newer = jobs.start(Job::Pivot);
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
        let started = jobs.start(Job::Copy);
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
        let rows = jobs.start(Job::Rows);
        let look = jobs.start(Job::LookAtDirectory(PathBuf::from("/data")));
        let named = jobs.start(Job::OpenNamed);
        let facts = jobs.start(Job::FileFacts { dataset: 1 });
        assert!(!jobs.would_strand());
        assert!(jobs.try_advance());
        assert!(!jobs.is_current(rows.ticket()));
        assert!(!jobs.is_current(look.ticket()));
        assert!(!jobs.is_current(named.ticket()));
        assert!(
            jobs.is_current(facts.ticket()),
            "file facts follow the dataset"
        );
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
}

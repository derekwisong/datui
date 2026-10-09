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
            typed: None,
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

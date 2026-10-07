use crate::quality_memory::QualityCopyJob;
use crate::*;
use std::sync::mpsc;

/// Choosing equal rows per value of a column sets the grain to that column, so
/// Segments and Trends have what the sample was drawn for; a grain chosen
/// afterwards is not taken back.
#[test]
fn the_grain_follows_an_equal_per_value_sample_once() {
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    let per_date = sampling::SampleMethod::PerPartition {
        column: "date".into(),
    };
    app.analysis_modal.sample.method = per_date.clone();
    app.sync_quality_plan();
    assert_eq!(
        app.analysis_modal.quality.plan.grain,
        data_quality::QualityGrain::Partition("date".into())
    );

    app.analysis_modal.quality.plan.grain = data_quality::QualityGrain::Dataset;
    app.sync_quality_plan();
    assert_eq!(
        app.analysis_modal.quality.plan.grain,
        data_quality::QualityGrain::Dataset,
        "the same sample again leaves the chosen grain alone"
    );
}

/// A copy that cannot stand in for the scan is neither used nor kept: the passes
/// read the source, and the dataset's later full scans do too.
#[test]
fn a_copy_that_does_not_read_as_the_source_is_let_go() {
    let root = tempfile::tempdir().unwrap();
    let objects = vec![crate::local_copy::RemoteObject {
        url: "s3://lake/a.parquet".into(),
        size: 3,
        etag: None,
    }];
    // The scan reads an object the copy does not hold.
    let lf = LazyFrame::scan_parquet(
        polars::prelude::PlRefPath::new("s3://lake/b.parquet"),
        Default::default(),
    )
    .unwrap();
    let mut heard = None;
    let (read, held) = App::quality_scope_on_copy(
        lf,
        QualityCopyJob::Fetch {
            objects,
            root: root.path().to_path_buf(),
        },
        &data_quality::QualityWatch::default(),
        |objects, root| {
            crate::local_copy::LocalCopy::fetch(
                root,
                objects,
                &sampling::ReadWatch::default(),
                |_, write| write(b"abc"),
            )
        },
        |copy| heard = Some(copy.is_some()),
    )
    .unwrap();
    assert_eq!(heard, Some(false), "nothing to keep");
    assert!(held.is_none());
    assert_eq!(
        crate::local_copy::scan_paths(&read),
        ["s3://lake/b.parquet"]
    );
    let left = std::fs::read_dir(root.path())
        .unwrap()
        .flatten()
        .filter(|entry| entry.file_name().to_string_lossy().starts_with("copy-"))
        .count();
    assert_eq!(left, 0, "its files went with it");

    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    let generation = app.dataset_generation;
    app.event(AppEvent::BackgroundQualityCopyKept {
        dataset_generation: generation,
        copy: None,
    });
    assert_eq!(app.quality.copy_unusable, Some(generation));
}

fn key(app: &mut App, code: KeyCode) -> Option<AppEvent> {
    app.event(AppEvent::Key(KeyEvent::new(code, KeyModifiers::NONE)))
}

fn screen(app: &mut App) -> String {
    let area = ratatui::layout::Rect::new(0, 0, 100, 24);
    let mut buffer = ratatui::buffer::Buffer::empty(area);
    app.render(area, &mut buffer);
    buffer.content().iter().map(|cell| cell.symbol()).collect()
}

/// A Data Quality report on screen and a run under way in `stage`: its job, whose
/// test decides how it ends.
fn quality_run_under_way(
    stage: data_quality::QualityPhase,
) -> (App, mpsc::Receiver<AppEvent>, jobs::Started) {
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    let df = polars::prelude::df!("id" => [1i64, 2, 3]).unwrap();
    app.data_table_state = Some(
        DataTableState::from_schema_and_lazyframe(
            df.schema().clone(),
            polars::prelude::IntoLazy::lazy(df.clone()),
            &OpenOptions::default(),
            None,
        )
        .unwrap(),
    );
    let modal = &mut app.analysis_modal;
    modal.active = true;
    modal.selected_tool = Some(analysis_modal::AnalysisTool::DataQuality);
    modal.focus = analysis_modal::AnalysisFocus::Main;
    let plan = data_quality::DataQualityPlan::default();
    modal.quality.results = Some(data_quality::DataQualityResults::empty(
        Some(3),
        &plan,
        df.schema(),
    ));
    modal.quality.last_plan = Some(plan.clone());
    modal.set_quality_page(data_quality::QualityPage::Overview);
    modal.quality.plan.sample_seed = 7;
    let mut progress = AnalysisProgress::new(stage.stage.label());
    progress.reads_source = Some(stage.reads_source);
    progress.interruptible = Some(stage.interruptible);
    modal.computing = Some(progress);
    let run = Job::Analysis(crate::jobs::AnalysisRun {
        watch: Some(data_quality::QualityWatch::default()),
        runs_out: false,
    });
    let worker = app.job_for_tests(run, Some("Profiling data quality..."));
    (app, rx, worker)
}

fn quality_stage(stage: data_quality::QualityStage) -> Progress {
    Progress::QualityPhase(data_quality::QualityPhase {
        stage,
        reads_source: false,
        interruptible: false,
    })
}

/// Esc on a Data Quality run whose read cannot stop at once: the last report is
/// kept, Setup comes back, and until the worker exits the header and Setup say
/// the read is finishing and Run does not start another beside it. The stages
/// the stopped worker still sends are dropped.
#[test]
fn a_cancelled_run_says_so_until_its_worker_exits() {
    let (mut app, rx, worker) = quality_run_under_way(data_quality::QualityPhase {
        stage: data_quality::QualityStage::CountingRows,
        reads_source: true,
        interruptible: false,
    });
    let stopped = worker.ticket();

    assert!(app.hard_escape_while_busy(&KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE)));
    assert!(key(&mut app, KeyCode::Esc).is_none());
    assert!(app.analysis_modal.computing.is_none());
    assert!(!app.is_busy());
    assert!(
        app.analysis_modal.quality.results.is_some(),
        "the last report is kept"
    );
    assert_eq!(
        app.analysis_modal.quality.page,
        data_quality::QualityPage::Setup
    );
    assert!(app.flash_message().is_none(), "state, not a flash");
    let text = screen(&mut app);
    assert!(text.contains("Cancelling: source read finishing"), "{text}");

    // A stage the stopped worker still sends changes nothing.
    app.event(AppEvent::JobProgress {
        ticket: stopped,
        progress: quality_stage(data_quality::QualityStage::ProfilingColumns),
    });
    assert!(app.analysis_modal.computing.is_none());

    // Run waits, with the reason, rather than read beside it.
    assert!(key(&mut app, KeyCode::Enter).is_none());
    assert!(!app.is_busy());
    assert!(screen(&mut app).contains("Run waits"));
    // So does r on the report.
    key(&mut app, KeyCode::Esc);
    assert_eq!(
        app.analysis_modal.quality.page,
        data_quality::QualityPage::Overview
    );
    assert!(key(&mut app, KeyCode::Char('r')).is_none());
    assert!(!app.is_busy());
    key(&mut app, KeyCode::Char('e'));
    assert!(key(&mut app, KeyCode::Enter).is_none());

    // The worker exits; the state goes, the reason with it, and Run runs.
    drop(worker);
    while let Ok(event) = rx.try_recv() {
        app.event(event);
    }
    assert!(app.cancelled_analysis_running().is_none());
    let text = screen(&mut app);
    assert!(!text.contains("Cancelling"), "{text}");
    assert!(!text.contains("Run waits"), "{text}");
    key(&mut app, KeyCode::Char('e'));
    assert!(matches!(
        key(&mut app, KeyCode::Enter),
        Some(AppEvent::AnalysisCompute(
            crate::analysis_modal::AnalysisTool::DataQuality
        ))
    ));

    // The run in flight hears its own stages, and shows them.
    let running = app.job_for_tests(
        Job::Analysis(crate::jobs::AnalysisRun::default()),
        Some("Profiling data quality..."),
    );
    app.event(AppEvent::JobProgress {
        ticket: running.ticket(),
        progress: quality_stage(data_quality::QualityStage::ProfilingColumns),
    });
    let progress = app.analysis_modal.computing.as_ref().unwrap();
    assert_eq!(progress.phase, "Profiling columns");
    assert_eq!(progress.reads_source, Some(false));
}

/// Esc on a stage that stops within a batch: the watch is told, a flash says the
/// run is cancelled, and nothing claims a read is finishing. Run still waits for
/// the worker to exit; should it outlast its batch, the screen says it is
/// stopping rather than leave Run refused without a reason.
#[test]
fn a_run_that_stops_at_its_next_batch_is_not_called_a_finishing_read() {
    let (mut app, rx, worker) = quality_run_under_way(data_quality::QualityPhase {
        stage: data_quality::QualityStage::ProfilingColumns,
        reads_source: true,
        interruptible: true,
    });
    let watch = match app.jobs.current(|job| matches!(job, Job::Analysis(_))) {
        Some((_, Job::Analysis(run))) => run.watch.clone().unwrap(),
        _ => panic!("the run is under way"),
    };

    assert!(app.hard_escape_while_busy(&KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE)));
    assert!(key(&mut app, KeyCode::Esc).is_none());
    assert!(watch.cancelled(), "the worker's reads were told to stop");
    assert_eq!(app.flash_message(), Some("Run cancelled"));
    assert_eq!(
        app.analysis_modal.quality.page,
        data_quality::QualityPage::Setup
    );
    assert!(app.cancelled_run_shown().is_none());
    let text = screen(&mut app);
    assert!(!text.contains("Cancelling"), "{text}");

    // Run waits for the worker all the same, and says why.
    assert!(key(&mut app, KeyCode::Enter).is_none());
    assert!(!app.is_busy());
    assert!(
        screen(&mut app).contains(QUALITY_RUN_WAITS),
        "{}",
        screen(&mut app)
    );

    // Past its batch and still going: now the screen says so.
    app.jobs.backdate_supersessions(CANCEL_GRACE);
    let text = screen(&mut app);
    assert!(text.contains("Cancelling: run stopping"), "{text}");
    assert!(!text.contains("source read finishing"), "{text}");

    drop(worker);
    while let Ok(event) = rx.try_recv() {
        app.event(event);
    }
    assert!(app.cancelled_analysis_running().is_none());
    assert!(!screen(&mut app).contains("Run waits"));
}

/// A report, an error or a stage from a run that is no longer current changes
/// nothing on screen and nothing in the cache: the run in flight goes on, and its
/// own report is the one installed. The rows the stale run read are still kept,
/// keyed by what chose them, since a finished read is not thrown away.
#[test]
fn a_stale_report_never_replaces_the_current_one() {
    let (mut app, rx, worker) = quality_run_under_way(data_quality::QualityPhase {
        stage: data_quality::QualityStage::ProfilingColumns,
        reads_source: false,
        interruptible: false,
    });
    // Two runs on a generation since left, and the run in flight.
    drop(worker);
    let stale = app.job_for_tests(Job::Analysis(crate::jobs::AnalysisRun::default()), None);
    let failing = app.job_for_tests(Job::Analysis(crate::jobs::AnalysisRun::default()), None);
    app.jobs.advance();
    let worker = app.job_for_tests(
        Job::Analysis(crate::jobs::AnalysisRun::default()),
        Some("Profiling data quality..."),
    );
    while let Ok(event) = rx.try_recv() {
        app.event(event);
    }
    let on_screen = app.analysis_modal.quality.results.clone().unwrap();
    let df = polars::prelude::df!("id" => [1i64, 2, 3]).unwrap();
    let state = app.data_table_state.as_ref().unwrap();
    let (dataset_generation, view_generation) = (app.dataset_generation, state.len_generation());
    let measured = |seed: u64| {
        let plan = data_quality::DataQualityPlan {
            dataset_rows: 2,
            sample_seed: seed,
            ..data_quality::DataQualityPlan::default()
        };
        let (results, rows) = data_quality::compute_data_quality_kept(
            &polars::prelude::IntoLazy::lazy(df.clone()),
            None,
            &plan,
            None,
            false,
            None,
        )
        .unwrap();
        let kept = KeptQualitySample {
            dataset_generation,
            view_generation,
            sample: plan.sample(),
            rows: std::sync::Arc::new(rows.unwrap()),
            source: crate::quality_export::SourceIdentity::default(),
        };
        (plan, results, kept)
    };

    let (old_plan, old_results, old_rows) = measured(1);
    app.event(AppEvent::JobProgress {
        ticket: stale.ticket(),
        progress: quality_stage(data_quality::QualityStage::Assembling),
    });
    let ended = stale.ticket();
    stale.end(Outcome::answered(Answer::DataQuality {
        results: Box::new(old_results),
        kept: Some(old_rows),
        plan: Box::new(old_plan.clone()),
    }));
    app.event(AppEvent::JobEnded(ended));
    let ended = failing.ticket();
    failing.end(Outcome::Failed {
        message: "the stale run failed".to_string(),
        panicked: false,
    });
    app.event(AppEvent::JobEnded(ended));
    assert_eq!(
        format!("{:?}", app.analysis_modal.quality.results),
        format!("{:?}", Some(&on_screen)),
        "the report on screen stays"
    );
    assert_eq!(
        app.analysis_modal
            .computing
            .as_ref()
            .map(|p| p.phase.as_str()),
        Some("Profiling columns"),
        "the run in flight goes on"
    );
    assert!(app.is_busy() && !app.error_modal.active);
    assert!(!app.quality_cached(&old_plan), "nothing cached from it");
    assert!(app.quality_kept_serves(&old_plan), "its rows are kept");

    let (new_plan, new_results, new_rows) = measured(2);
    let ended = worker.ticket();
    worker.end(Outcome::answered(Answer::DataQuality {
        results: Box::new(new_results.clone()),
        kept: Some(new_rows),
        plan: Box::new(new_plan.clone()),
    }));
    app.event(AppEvent::JobEnded(ended));
    assert_eq!(
        format!("{:?}", app.analysis_modal.quality.results),
        format!("{:?}", Some(&new_results))
    );
    assert_eq!(
        app.analysis_modal.quality.last_plan.as_ref(),
        Some(&new_plan)
    );
    assert!(app.analysis_modal.computing.is_none() && !app.is_busy());
    assert!(app.quality_cached(&new_plan));
    while let Ok(event) = rx.try_recv() {
        app.event(event);
    }
    assert!(!app.background_work_in_flight());
}

/// The progress view holds still while a run goes through its stages: a new
/// stage, a spinner frame, the clock and a row count change the words on two
/// lines, and no cell outside them moves, at 80x24 and 60x20.
#[test]
fn stages_and_spinner_frames_do_not_move_the_layout() {
    use data_quality::QualityStage;
    let (mut app, _rx, _worker) = quality_run_under_way(data_quality::QualityPhase {
        stage: QualityStage::Preparing,
        reads_source: false,
        interruptible: false,
    });
    let frames = [
        (QualityStage::Preparing, None, 0, 0),
        (QualityStage::ReadingSample, Some(true), 1, 0),
        (QualityStage::ReadingSample, Some(true), 2, 9),
        (QualityStage::CountingSegments, Some(true), 3, 75),
        (QualityStage::ProfilingColumns, Some(false), 4, 3_700),
        (QualityStage::CheckingSharedNulls, Some(false), 5, 3_700),
        (QualityStage::Assembling, Some(false), 6, 3_700),
    ];
    for (width, height) in [(80, 24), (60, 20)] {
        let area = ratatui::layout::Rect::new(0, 0, width, height);
        let mut drawn = Vec::new();
        for (stage, reads, frame, seconds) in frames {
            // The monotonic clock starts at boot: a runner up for less than an
            // hour cannot hold an instant an hour back, so that frame waits.
            let Some(started) =
                std::time::Instant::now().checked_sub(std::time::Duration::from_secs(seconds))
            else {
                continue;
            };
            let watch = crate::sampling::ReadWatch::default();
            if reads == Some(true) {
                watch.saw(1_234_567 * frame as usize);
            }
            let progress = app.analysis_modal.computing.as_mut().unwrap();
            progress.phase = stage.label().to_string();
            progress.reads_source = reads;
            progress.read = Some(watch);
            progress.started = started;
            app.throbber_frame = frame;
            let mut buffer = ratatui::buffer::Buffer::empty(area);
            app.render(area, &mut buffer);
            let rows = (0..height)
                .map(|y| {
                    (0..width)
                        .map(|x| buffer[(x, y)].symbol().to_string())
                        .collect::<String>()
                })
                .collect::<Vec<_>>();
            let at = rows
                .iter()
                .enumerate()
                .find_map(|(y, row)| row.find(stage.label()).map(|x| (y, x)))
                .unwrap_or_else(|| panic!("{} on screen: {rows:#?}", stage.label()));
            let clock = crate::numfmt::clock(std::time::Duration::from_secs(seconds));
            assert!(rows[at.0].contains(&clock), "the clock beside the stage");
            drawn.push((rows, at));
        }
        let (first, at) = &drawn[0];
        for (rows, stage_at) in &drawn[1..] {
            assert_eq!(stage_at, at, "the stage starts in one place");
            for (y, (row, before)) in rows.iter().zip(first).enumerate() {
                // The stage's line and the one two below it, which says what
                // it reads, are the words that change.
                if y != at.0 && y != at.0 + 2 && row != before {
                    // The footer's spinner turns in its one cell.
                    let moved = row
                        .chars()
                        .zip(before.chars())
                        .filter(|(now, then)| now != then)
                        .count();
                    assert!(
                        y + 1 == height as usize && moved <= 1,
                        "row {y} moved at {width}x{height}:\n{before}\n{row}"
                    );
                }
            }
        }
    }
}

/// No other way in starts a read beside a cancelled run still reading: not the
/// rows a finding stages, not `v` on a sample no longer kept, not another tool.
/// Each waits, says why, and stays as it was; once the worker exits, each reads.
#[test]
fn nothing_reads_beside_a_cancelled_run_through_another_way_in() {
    let (mut app, rx, worker) = quality_run_under_way(data_quality::QualityPhase {
        stage: data_quality::QualityStage::CountingRows,
        reads_source: true,
        interruptible: false,
    });
    assert!(key(&mut app, KeyCode::Esc).is_none());
    assert!(app.cancelled_analysis_running().is_some());
    key(&mut app, KeyCode::Esc);
    assert_eq!(
        app.analysis_modal.quality.page,
        data_quality::QualityPage::Overview
    );
    let refused = |app: &mut App, what: &str| {
        assert!(!app.is_busy(), "{what} started a read");
        assert!(app.analysis_modal.computing.is_none(), "{what}");
        assert_eq!(app.flash_message(), Some(ANALYSIS_READ_WAITS), "{what}");
        app.flash = None;
    };
    let duplicates = || analysis_modal::EvidenceRead {
        rows: quality_report::EvidenceRows::Duplicates,
        label: "Data Quality / Duplicate rows".to_string(),
        sample: None,
        scope: data_quality::QualityScope::CurrentView,
        summary: Vec::new(),
    };

    // A finding's staged read: Enter waits, and the read stays staged.
    app.analysis_modal.quality.evidence_read = Some(duplicates());
    assert!(key(&mut app, KeyCode::Enter).is_none());
    refused(&mut app, "a finding's rows");
    assert!(app.analysis_modal.quality.evidence_read.is_some());
    key(&mut app, KeyCode::Esc);

    // The sample's rows, which the cancelled run never kept.
    assert!(key(&mut app, KeyCode::Char('v')).is_none());
    refused(&mut app, "v");

    // Another tool.
    app.analysis_modal.focus = analysis_modal::AnalysisFocus::Sidebar;
    app.analysis_modal.sidebar_state.select(Some(0));
    app.analysis_modal.sample_run_for = Some(app.dataset_generation);
    assert!(key(&mut app, KeyCode::Enter).is_none());
    refused(&mut app, "Describe");
    // Its Sample form stays open, as filled.
    app.analysis_modal.sample_run_for = None;
    app.analysis_modal.focus = analysis_modal::AnalysisFocus::Sidebar;
    assert!(key(&mut app, KeyCode::Enter).is_none());
    assert!(
        app.analysis_modal.sample_form.is_some(),
        "the form is the pane"
    );
    assert!(key(&mut app, KeyCode::Enter).is_none());
    refused(&mut app, "Describe's Sample form");
    assert!(app.analysis_modal.sample_form.is_some());
    app.analysis_modal.sample_run_for = Some(app.dataset_generation);
    app.analysis_modal.sample_form = None;
    app.analysis_modal.focus = analysis_modal::AnalysisFocus::Sidebar;

    // The worker exits: each way in reads again.
    drop(worker);
    while let Ok(event) = rx.try_recv() {
        app.event(event);
    }
    assert!(app.cancelled_analysis_running().is_none());
    assert!(matches!(
        key(&mut app, KeyCode::Enter),
        Some(AppEvent::AnalysisCompute(
            crate::analysis_modal::AnalysisTool::Describe
        ))
    ));
}

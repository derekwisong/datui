//! Data Quality over a remote multi-file dataset, counted at the wire.
//!
//! An in-process S3 stand-in (`common/fake_s3.rs`) serves a prefix of Parquet files
//! and counts every request and byte, so these tests say what each run costs the
//! source rather than what datui believes it read. Nothing leaves the loopback.

use crate::common::next_event;
use crate::fake_s3::{FakeS3, WireCount};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use datui::data_quality::{
    QualityComparison, QualityCompute, QualityGrain, QualityStage, TemporalRole,
    TemporalRoleAssignment,
};
use datui::{App, AppConfig, AppEvent, OpenOptions, Progress};
use polars::prelude::*;
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::widgets::Widget;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::mpsc;

const FILES: usize = 4;
const ROWS: usize = 5_000;

/// `FILES` Parquet objects under `events/`, `ROWS` each: an id, an event time every
/// 37 minutes, a received time 40 seconds later, a region and an amount with gaps.
fn remote_events() -> BTreeMap<String, Vec<u8>> {
    let minute = 60_000_000i64;
    let start = 1_704_067_200_000_000i64;
    let datetime = DataType::Datetime(TimeUnit::Microseconds, None);
    (0..FILES)
        .map(|file| {
            let ids = (0..ROWS)
                .map(|row| (file * ROWS + row) as i64)
                .collect::<Vec<_>>();
            let at = ids
                .iter()
                .map(|id| start + id * 37 * minute)
                .collect::<Vec<_>>();
            let sent = at.iter().map(|at| at + 40_000_000).collect::<Vec<_>>();
            let regions = ids
                .iter()
                .map(|id| ["North", "South", "East"][*id as usize % 3])
                .collect::<Vec<_>>();
            let amounts = ids
                .iter()
                .map(|id| (id % 13 != 0).then_some(*id as f64 / 4.0))
                .collect::<Vec<_>>();
            let mut df = df!(
                "id" => &ids,
                "at" => at,
                "sent" => sent,
                "region" => regions,
                "amount" => amounts,
            )
            .unwrap()
            .lazy()
            .with_columns([
                col("at").cast(datetime.clone()),
                col("sent").cast(datetime.clone()),
            ])
            .collect()
            .unwrap();
            let mut bytes = Vec::new();
            ParquetWriter::new(&mut bytes)
                .with_row_group_size(Some(1_000))
                .finish(&mut df)
                .unwrap();
            (format!("events/part-{file}.parquet"), bytes)
        })
        .collect()
}

/// Handle `first` and every event it leads to, until nothing more is owed. The
/// stages of the run in flight that read the source, in order.
fn settle(
    app: &mut App,
    rx: &mpsc::Receiver<AppEvent>,
    first: Option<AppEvent>,
) -> Vec<QualityStage> {
    let mut reads = Vec::new();
    let mut next = first;
    loop {
        match next.take() {
            Some(event) => {
                if let AppEvent::JobProgress {
                    ticket,
                    progress: Progress::QualityPhase(phase),
                } = &event
                    && app.job_is_current(*ticket)
                    && phase.reads_source
                {
                    reads.push(phase.stage);
                }
                next = app.event(&event);
            }
            None => match next_event(app, rx) {
                Some(event) => next = Some(event),
                None => return reads,
            },
        }
    }
}

fn press(app: &mut App, code: KeyCode) -> Option<AppEvent> {
    app.event(&AppEvent::Key(KeyEvent::new(code, KeyModifiers::NONE)))
}

fn screen(app: &mut App) -> String {
    let area = Rect::new(0, 0, 120, 40);
    let mut buffer = Buffer::empty(area);
    app.render(area, &mut buffer);
    buffer.content().iter().map(|cell| cell.symbol()).collect()
}

/// The bucket's prefix, opened, with Data Quality's Setup on screen.
fn open_remote(s3: &FakeS3) -> (App, mpsc::Receiver<AppEvent>) {
    open_remote_with(s3, AppConfig::default(), None)
}

/// [`open_remote`] with `config` (its cloud settings pointed at the bucket) and,
/// when given, `cache` as the cache directory local copies are written under.
fn open_remote_with(
    s3: &FakeS3,
    config: AppConfig,
    cache: Option<&Path>,
) -> (App, mpsc::Receiver<AppEvent>) {
    let config = AppConfig {
        cloud: s3.cloud_config(),
        ..config
    };
    let theme = datui::Theme::from_config(&config.theme).unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new_with_config(tx, crate::common::test_runtime(), theme, config);
    if let Some(cache) = cache {
        app.use_cache(datui::CacheManager::with_dir(cache.to_path_buf()));
    }
    settle(
        &mut app,
        &rx,
        Some(AppEvent::Open(
            vec![PathBuf::from("s3://lake/events/")],
            OpenOptions::default(),
        )),
    );
    assert_eq!(
        app.data_table_state.as_ref().map(|state| state.num_rows()),
        Some(FILES * ROWS)
    );
    press(&mut app, KeyCode::Char('a'));
    app.analysis_modal.sidebar_state.select(Some(3));
    assert!(
        press(&mut app, KeyCode::Enter).is_none(),
        "Setup reads nothing"
    );
    (app, rx)
}

/// Stage `change` in Setup, Run, and settle: the stages that read the source and
/// what the bucket was asked for meanwhile, Setup's own reads included.
fn edit_and_run(
    app: &mut App,
    rx: &mpsc::Receiver<AppEvent>,
    s3: &FakeS3,
    change: impl FnOnce(&mut datui::data_quality::DataQualityPlan),
) -> (Vec<QualityStage>, WireCount) {
    let before = s3.wire.count();
    if !app.analysis_modal.data_quality_page.is_setup() {
        press(app, KeyCode::Char('e'));
    }
    change(&mut app.analysis_modal.data_quality_plan);
    screen(app);
    let mut first = press(app, KeyCode::Enter);
    if app.confirmation_modal.asks_full_scan() {
        // A full scan asks first; Enter there runs it.
        first = press(app, KeyCode::Enter);
    }
    let reads = settle(app, rx, first);
    assert!(
        app.analysis_modal.data_quality_results.is_some(),
        "a report is on screen"
    );
    (reads, s3.wire.count().since(&before))
}

fn roles() -> Vec<TemporalRoleAssignment> {
    vec![
        TemporalRoleAssignment {
            role: TemporalRole::Event,
            column: "at".into(),
            timezone: None,
        },
        TemporalRoleAssignment {
            role: TemporalRole::Received,
            column: "sent".into(),
            timezone: None,
        },
    ]
}

fn window(every: &str) -> QualityGrain {
    QualityGrain::TimeWindows {
        column: "at".into(),
        every: every.into(),
    }
}

/// #415's "What edits should cost" on a remote prefix of several files, counted in
/// requests and bytes at the bucket. The first daily run samples and counts its days
/// in one pass. Roles, a coarser window, the files, row chunks and a comparison ask
/// the bucket for nothing; a partition is one count; a new seed is a new sample.
#[test]
fn a_remote_prefix_is_sampled_once_and_its_edits_ask_the_bucket_for_nothing() {
    let s3 = FakeS3::serve("lake", remote_events());
    let (mut app, rx) = open_remote(&s3);

    let (reads, wire) = edit_and_run(&mut app, &rx, &s3, |plan| {
        plan.dataset_rows = 2_000;
        plan.sample_seed = 7;
        plan.grain = window("1d");
    });
    assert_eq!(reads, [QualityStage::ReadingSample], "one pass");
    assert!(wire.gets > 0 && wire.bytes > 0, "{wire:?}");
    let first = app.analysis_modal.data_quality_results.clone().unwrap();
    assert_eq!(first.total_rows, Some(FILES * ROWS));
    // Days the sample drew from, and days it missed, hold every row between them.
    let days: usize = first
        .segments
        .iter()
        .filter_map(|s| s.total_rows)
        .sum::<usize>()
        + first
            .unsampled_segments
            .iter()
            .map(|s| s.total_rows)
            .sum::<usize>();
    assert_eq!(days, FILES * ROWS, "every row counted into a day");

    let free = |reads: Vec<QualityStage>, wire: WireCount, what: &str| {
        assert!(reads.is_empty(), "{what}: {reads:?}");
        assert_eq!(wire, WireCount::default(), "{what}");
    };
    let (reads, wire) = edit_and_run(&mut app, &rx, &s3, |plan| plan.temporal_roles = roles());
    free(reads, wire, "a role edit");
    let results = app.analysis_modal.data_quality_results.as_ref().unwrap();
    assert!(!results.temporal.is_empty(), "the interval is measured");

    let (reads, wire) = edit_and_run(&mut app, &rx, &s3, |plan| plan.grain = window("1w"));
    free(reads, wire, "weeks from days");

    let (reads, wire) = edit_and_run(&mut app, &rx, &s3, |plan| {
        plan.grain = QualityGrain::File;
    });
    free(reads, wire, "files from their footers");
    let results = app.analysis_modal.data_quality_results.as_ref().unwrap();
    assert_eq!(results.segments.len(), FILES);
    assert!(
        results
            .segments
            .iter()
            .all(|segment| segment.total_rows == Some(ROWS) && segment.evaluated_rows > 0),
        "{:?}",
        results.segments
    );

    let (reads, wire) = edit_and_run(&mut app, &rx, &s3, |plan| {
        plan.comparison = QualityComparison::Previous;
    });
    free(reads, wire, "a comparison");
    let results = app.analysis_modal.data_quality_results.as_ref().unwrap();
    assert!(results.segments[1].compared_with.is_some());

    let (reads, wire) = edit_and_run(&mut app, &rx, &s3, |plan| {
        plan.grain = QualityGrain::RowChunks(5_000);
    });
    free(reads, wire, "row chunks where the sampled rows sat");

    let (reads, wire) = edit_and_run(&mut app, &rx, &s3, |plan| {
        plan.grain = QualityGrain::Partition("region".into());
        plan.comparison = QualityComparison::None;
    });
    assert_eq!(reads, [QualityStage::CountingSegments], "one count");
    assert!(wire.requests() > 0, "{wire:?}");

    let (reads, wire) = edit_and_run(&mut app, &rx, &s3, |plan| plan.sample_seed = 8);
    assert_eq!(reads, [QualityStage::ReadingSample], "a new sample");
    assert!(wire.requests() > 0, "{wire:?}");
}

/// A full scan keeps no rows, so a full report's comparison is all it can change for
/// free: Compare and a baseline chosen in Setup are worked out from the segments the
/// report holds, with no confirmation and no request. A role is a new measurement:
/// it asks, and reads.
#[test]
fn a_full_scan_is_compared_again_without_a_request() {
    let s3 = FakeS3::serve("lake", remote_events());
    // Each pass over the bucket, as a dataset past the local copy's budget reads.
    let (mut app, rx) = open_remote_with(&s3, copies_off(), None);

    let (reads, wire) = edit_and_run(&mut app, &rx, &s3, |plan| {
        plan.method = datui::sampling::SampleMethod::EveryRow;
        plan.compute = QualityCompute::Full;
        plan.grain = QualityGrain::File;
    });
    assert!(reads.contains(&QualityStage::ProfilingColumns), "{reads:?}");
    assert!(wire.gets > 0 && wire.bytes > 0, "{wire:?}");
    let full = app.analysis_modal.data_quality_results.clone().unwrap();
    assert_eq!(full.evaluated_rows, FILES * ROWS);
    assert!(full.segments.iter().all(|s| s.compared_with.is_none()));

    let before = s3.wire.count();
    press(&mut app, KeyCode::Char('e'));
    app.analysis_modal.data_quality_plan.comparison = QualityComparison::Previous;
    let text = screen(&mut app);
    assert!(text.contains("Changed: Compare"), "{text}");
    assert!(press(&mut app, KeyCode::Enter).is_none(), "nothing to run");
    assert!(!app.confirmation_modal.asks_full_scan(), "nothing to ask");
    assert!(!app.is_busy());
    let compared = app.analysis_modal.data_quality_results.clone().unwrap();
    assert_eq!(compared.evaluated_rows, full.evaluated_rows);
    assert!(compared.segments[0].compared_with.is_none());
    assert_eq!(
        compared.segments[1].compared_with.as_deref(),
        Some(compared.segments[0].label.as_str())
    );
    assert_eq!(
        app.analysis_modal
            .data_quality_last_plan
            .as_ref()
            .unwrap()
            .comparison,
        QualityComparison::Previous,
        "the report is labeled with the comparison it shows"
    );

    let baseline = compared.segments[2].label.clone();
    press(&mut app, KeyCode::Char('e'));
    {
        let plan = &mut app.analysis_modal.data_quality_plan;
        plan.comparison = QualityComparison::Baseline;
        plan.baseline_segment = Some(baseline.clone());
    }
    assert!(press(&mut app, KeyCode::Enter).is_none());
    let against = app.analysis_modal.data_quality_results.clone().unwrap();
    assert_eq!(
        against.segments[0].compared_with.as_deref(),
        Some(baseline.as_str())
    );
    assert_eq!(s3.wire.count().since(&before), WireCount::default());

    // Back to the comparison the cache holds a report for: still no read.
    press(&mut app, KeyCode::Char('e'));
    app.analysis_modal.data_quality_plan.comparison = QualityComparison::None;
    app.analysis_modal.data_quality_plan.baseline_segment = None;
    assert!(press(&mut app, KeyCode::Enter).is_none());
    let none = app.analysis_modal.data_quality_results.clone().unwrap();
    assert!(none.segments.iter().all(|s| s.compared_with.is_none()));
    assert_eq!(s3.wire.count().since(&before), WireCount::default());

    let (reads, wire) = edit_and_run(&mut app, &rx, &s3, |plan| plan.temporal_roles = roles());
    assert!(!reads.is_empty(), "roles on a full scan read again");
    assert!(wire.requests() > 0, "{wire:?}");
}

fn copies_off() -> AppConfig {
    let mut config = AppConfig::default();
    config.analysis.quality_local_copy = datui::config::ByteSize(0);
    config
}

fn full_scan(plan: &mut datui::data_quality::DataQualityPlan) {
    plan.method = datui::sampling::SampleMethod::EveryRow;
    plan.compute = QualityCompute::Full;
    plan.grain = window("1d");
}

/// The local copies under `cache`.
fn copies(cache: &Path) -> Vec<PathBuf> {
    std::fs::read_dir(cache.join("quality-copies"))
        .map(|entries| {
            entries
                .flatten()
                .map(|entry| entry.path())
                .filter(|path| {
                    path.file_name()
                        .and_then(|name| name.to_str())
                        .is_some_and(|name| name.starts_with("copy-"))
                })
                .collect()
        })
        .unwrap_or_default()
}

fn bytes_under(dir: &Path) -> u64 {
    std::fs::read_dir(dir)
        .map(|entries| {
            entries
                .flatten()
                .map(|entry| {
                    let path = entry.path();
                    if path.is_dir() {
                        bytes_under(&path)
                    } else {
                        entry.metadata().map(|meta| meta.len()).unwrap_or(0)
                    }
                })
                .sum()
        })
        .unwrap_or(0)
}

/// The report, without what says how it was read: the copy and the source must
/// measure the same thing.
fn measured(app: &App) -> String {
    let mut results = app.analysis_modal.data_quality_results.clone().unwrap();
    results.reads = None;
    results.source = None;
    // Each app draws its own seed; a full scan samples nothing with it.
    results.sample_seed = 0;
    format!("{results:?}")
}

/// Run a full scan staged in Setup, confirming it, and settle.
fn run_staged(app: &mut App, rx: &mpsc::Receiver<AppEvent>) -> Vec<QualityStage> {
    let mut first = press(app, KeyCode::Enter);
    if app.confirmation_modal.asks_full_scan() {
        first = press(app, KeyCode::Enter);
    }
    settle(app, rx, first)
}

/// A full scan within the copy budget fetches each object once, with one request
/// each, and every pass reads the copy. Setup says so before Run, the report says
/// so after, and the copy measures what the source measures. A second full scan
/// with roles reads the copy again and asks the bucket for nothing.
#[test]
fn a_full_scan_fetches_each_object_once_and_reuses_the_copy() {
    let objects = remote_events();
    let total: u64 = objects.values().map(|bytes| bytes.len() as u64).sum();
    let s3 = FakeS3::serve("lake", objects);
    let cache = tempfile::tempdir().unwrap();
    let (mut app, rx) = open_remote_with(&s3, AppConfig::default(), Some(cache.path()));

    full_scan(&mut app.analysis_modal.data_quality_plan);
    let text = screen(&mut app);
    assert!(
        text.contains("1 fetch of 4 objects") && text.contains("to a local copy"),
        "{text}"
    );
    let (reads, wire) = edit_and_run(&mut app, &rx, &s3, full_scan);
    assert_eq!(reads, [QualityStage::CopyingSource], "one read: the fetch");
    assert_eq!(
        wire,
        WireCount {
            lists: 0,
            heads: 0,
            gets: FILES as u64,
            bytes: total,
        },
        "each object once, whole"
    );
    let results = app.analysis_modal.data_quality_results.clone().unwrap();
    assert_eq!(results.evaluated_rows, FILES * ROWS);
    let copy = results
        .reads
        .unwrap()
        .copy
        .expect("the passes read the copy");
    assert!(copy.fetched && copy.bytes == total && copy.objects == FILES);
    let held = copies(cache.path());
    assert_eq!(held.len(), 1);
    assert!(bytes_under(&held[0]) >= total);
    assert_eq!(app.quality_copy_bytes(), total);
    let fetched = measured(&app);

    // The same study read from the bucket in its passes measures the same.
    let source = FakeS3::serve("lake", remote_events());
    let (mut direct, direct_rx) = open_remote_with(&source, copies_off(), None);
    edit_and_run(&mut direct, &direct_rx, &source, full_scan);
    assert!(source.wire.count().gets > FILES as u64, "several passes");
    assert_eq!(measured(&direct), fetched, "the copy reads as the source");

    // A role is a new measurement: it reads the copy, not the bucket.
    press(&mut app, KeyCode::Char('e'));
    app.analysis_modal.data_quality_plan.temporal_roles = roles();
    let text = screen(&mut app);
    assert!(text.contains("passes over the local copy"), "{text}");
    assert!(
        text.contains("local copy"),
        "the Read rule names it: {text}"
    );
    let (reads, wire) = edit_and_run(&mut app, &rx, &s3, |plan| plan.temporal_roles = roles());
    assert!(reads.is_empty(), "{reads:?}");
    assert_eq!(wire, WireCount::default());
    let results = app.analysis_modal.data_quality_results.as_ref().unwrap();
    assert!(!results.temporal.is_empty(), "the interval is measured");
    assert!(!results.reads.unwrap().copy.unwrap().fetched);
}

/// `d` in Setup releases the copy and its files with the kept rows; Setup says the
/// next run fetches again, and it does. Opening the dataset again removes the copy
/// too.
#[test]
fn a_copy_is_released_by_d_and_by_opening_again() {
    let s3 = FakeS3::serve("lake", remote_events());
    let cache = tempfile::tempdir().unwrap();
    let (mut app, rx) = open_remote_with(&s3, AppConfig::default(), Some(cache.path()));
    edit_and_run(&mut app, &rx, &s3, full_scan);
    assert_eq!(copies(cache.path()).len(), 1);

    press(&mut app, KeyCode::Char('e'));
    let text = screen(&mut app);
    assert!(
        text.contains("local copy"),
        "the Read rule names it: {text}"
    );
    press(&mut app, KeyCode::Char('d'));
    assert!(copies(cache.path()).is_empty(), "released, the files go");
    assert_eq!(app.quality_copy_bytes(), 0);
    assert!(
        app.flash_message()
            .is_some_and(|flash| flash.starts_with("Released the local copy")),
        "{:?}",
        app.flash_message()
    );
    app.analysis_modal.data_quality_plan.temporal_roles = roles();
    let text = screen(&mut app);
    assert!(text.contains("Released since last copy"), "{text}");
    let before = s3.wire.count();
    let reads = run_staged(&mut app, &rx);
    assert_eq!(reads, [QualityStage::CopyingSource]);
    assert_eq!(
        s3.wire.count().since(&before).gets,
        FILES as u64,
        "fetched again"
    );
    assert_eq!(copies(cache.path()).len(), 1);

    settle(
        &mut app,
        &rx,
        Some(AppEvent::Open(
            vec![PathBuf::from("s3://lake/events/")],
            OpenOptions::default(),
        )),
    );
    assert!(app.data_table_state.is_some());
    assert!(
        copies(cache.path()).is_empty(),
        "a reopened dataset fetches anew"
    );
    assert_eq!(app.quality_copy_bytes(), 0);
}

/// Two objects of about 0.8 MB of noise each: past a 1 MiB budget.
fn large_events() -> BTreeMap<String, Vec<u8>> {
    let mut state = 0x2545_f491_4f6c_dd1du64;
    (0..2)
        .map(|file| {
            let noise = (0..100_000)
                .map(|_| {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    state
                })
                .collect::<Vec<u64>>();
            let mut df = df!(
                "id" => (0..100_000i64).collect::<Vec<_>>(),
                "noise" => noise,
            )
            .unwrap();
            let mut bytes = Vec::new();
            ParquetWriter::new(&mut bytes).finish(&mut df).unwrap();
            (format!("events/part-{file}.parquet"), bytes)
        })
        .collect()
}

/// Past `analysis.quality_local_copy` a full scan reads the bucket in its
/// passes as before, and Setup says why before Run. Nothing is written locally.
#[test]
fn above_the_budget_a_full_scan_reads_the_source_in_passes() {
    let objects = large_events();
    let total: u64 = objects.values().map(|bytes| bytes.len() as u64).sum();
    assert!(total > 1024 * 1024, "{total}");
    let s3 = FakeS3::serve("lake", objects);
    let cache = tempfile::tempdir().unwrap();
    let mut config = AppConfig::default();
    config.analysis.quality_local_copy = datui::config::ByteSize::mib(1);
    let (mut app, rx) = {
        let config = AppConfig {
            cloud: s3.cloud_config(),
            ..config
        };
        let theme = datui::Theme::from_config(&config.theme).unwrap();
        let (tx, rx) = mpsc::channel();
        let mut app = App::new_with_config(tx, crate::common::test_runtime(), theme, config);
        app.use_cache(datui::CacheManager::with_dir(cache.path().to_path_buf()));
        settle(
            &mut app,
            &rx,
            Some(AppEvent::Open(
                vec![PathBuf::from("s3://lake/events/")],
                OpenOptions::default(),
            )),
        );
        press(&mut app, KeyCode::Char('a'));
        app.analysis_modal.sidebar_state.select(Some(3));
        press(&mut app, KeyCode::Enter);
        (app, rx)
    };
    let full = |plan: &mut datui::data_quality::DataQualityPlan| {
        plan.method = datui::sampling::SampleMethod::EveryRow;
        plan.compute = QualityCompute::Full;
    };
    full(&mut app.analysis_modal.data_quality_plan);
    let text = screen(&mut app);
    assert!(text.contains("passes over the source"), "{text}");
    assert!(text.contains("No local copy:"), "{text}");
    let (reads, wire) = edit_and_run(&mut app, &rx, &s3, full);
    assert!(!reads.contains(&QualityStage::CopyingSource), "{reads:?}");
    assert!(reads.contains(&QualityStage::ProfilingColumns), "{reads:?}");
    assert!(wire.gets > 2, "a pass per check: {wire:?}");
    assert!(copies(cache.path()).is_empty());
    let results = app.analysis_modal.data_quality_results.as_ref().unwrap();
    assert!(results.reads.unwrap().copy.is_none());
}

/// Handle events until the run's worker has exited.
fn until_the_worker_exits(app: &mut App, rx: &mpsc::Receiver<AppEvent>) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
    while app.background_work_in_flight() {
        assert!(
            std::time::Instant::now() < deadline,
            "the worker never exited"
        );
        if let Ok(event) = rx.recv_timeout(std::time::Duration::from_millis(20)) {
            let mut next = Some(event);
            while let Some(event) = next.take() {
                next = app.event(&event);
            }
        }
    }
}

/// Esc while the copy is being fetched stops it at its next chunk, and the partial
/// copy is removed; nothing is kept.
#[test]
fn a_cancel_mid_fetch_leaves_no_copy() {
    let s3 = FakeS3::serve("lake", remote_events());
    let cache = tempfile::tempdir().unwrap();
    let (mut app, rx) = open_remote_with(&s3, AppConfig::default(), Some(cache.path()));
    s3.slow_gets(300);
    full_scan(&mut app.analysis_modal.data_quality_plan);
    let before = s3.wire.count();
    let mut next = press(&mut app, KeyCode::Enter);
    if app.confirmation_modal.asks_full_scan() {
        next = press(&mut app, KeyCode::Enter);
    }
    loop {
        let event = match next.take() {
            Some(event) => event,
            None => rx
                .recv_timeout(std::time::Duration::from_secs(60))
                .expect("the run reports its stages"),
        };
        let fetching = matches!(
            &event,
            AppEvent::JobProgress { ticket, progress: Progress::QualityPhase(phase) }
                if app.job_is_current(*ticket) && phase.stage == QualityStage::CopyingSource
        );
        next = app.event(&event);
        if fetching {
            break;
        }
    }
    // The first object's answer is still on its way.
    press(&mut app, KeyCode::Esc);
    assert!(app.analysis_modal.computing.is_none(), "cancelled");
    until_the_worker_exits(&mut app, &rx);
    let wire = s3.wire.count().since(&before);
    assert!(
        wire.gets < FILES as u64,
        "stopped before the last object: {wire:?}"
    );
    assert!(copies(cache.path()).is_empty(), "the partial copy is gone");
    assert_eq!(app.quality_copy_bytes(), 0);
}

/// An object gone from the bucket fails the fetch partway: the run says so, and the
/// objects already copied are removed with the rest.
#[test]
fn a_failed_fetch_leaves_no_copy() {
    let s3 = FakeS3::serve("lake", remote_events());
    let cache = tempfile::tempdir().unwrap();
    let (mut app, rx) = open_remote_with(&s3, AppConfig::default(), Some(cache.path()));
    s3.remove(&format!("events/part-{}.parquet", FILES - 1));
    full_scan(&mut app.analysis_modal.data_quality_plan);
    let before = s3.wire.count();
    let reads = run_staged(&mut app, &rx);
    assert_eq!(reads, [QualityStage::CopyingSource]);
    until_the_worker_exits(&mut app, &rx);
    assert!(app.modal_showing(), "the failure is shown");
    assert!(app.analysis_modal.data_quality_results.is_none());
    assert_eq!(
        s3.wire.count().since(&before).gets,
        FILES as u64 - 1,
        "three copied; the stand-in counts no GET of a missing key"
    );
    assert!(
        copies(cache.path()).is_empty(),
        "the objects copied went too"
    );
    assert_eq!(app.quality_copy_bytes(), 0);
}

/// An object rewritten at the same size after the open answers with another ETag
/// than the listing's: the fetch fails rather than copy a dataset not on screen,
/// and nothing is left.
#[test]
fn an_object_rewritten_since_open_fails_the_fetch() {
    let s3 = FakeS3::serve("lake", remote_events());
    let cache = tempfile::tempdir().unwrap();
    let (mut app, rx) = open_remote_with(&s3, AppConfig::default(), Some(cache.path()));
    let key = "events/part-1.parquet";
    let mut rewritten = remote_events().remove(key).unwrap();
    let middle = rewritten.len() / 2;
    rewritten[middle] ^= 0xff;
    s3.put(key, rewritten);
    full_scan(&mut app.analysis_modal.data_quality_plan);
    let reads = run_staged(&mut app, &rx);
    assert_eq!(reads, [QualityStage::CopyingSource]);
    until_the_worker_exits(&mut app, &rx);
    assert!(app.modal_showing(), "the failure is shown");
    let text = screen(&mut app);
    assert!(text.contains("part-1.parquet: it changed"), "{text}");
    assert!(app.analysis_modal.data_quality_results.is_none());
    assert!(copies(cache.path()).is_empty());
    assert_eq!(app.quality_copy_bytes(), 0);
}

/// The objects of `remote_events` under hive directories, `day=1/` and so on: the
/// scan reads `day` from the path.
fn hive_events() -> BTreeMap<String, Vec<u8>> {
    remote_events()
        .into_iter()
        .enumerate()
        .map(|(day, (key, bytes))| {
            let name = key.trim_start_matches("events/");
            (format!("events/day={}/{name}", day + 1), bytes)
        })
        .collect()
}

/// A hive dataset's copy keeps its `key=value` directories, so the passes over it
/// read the partition column and measure what the bucket does.
#[test]
fn a_hive_dataset_is_copied_with_its_partitions() {
    let s3 = FakeS3::serve("lake", hive_events());
    let cache = tempfile::tempdir().unwrap();
    let (mut app, rx) = open_remote_with(&s3, AppConfig::default(), Some(cache.path()));
    assert!(
        app.data_table_state
            .as_ref()
            .is_some_and(|state| state.schema().contains("day")),
        "the partition column"
    );
    let (reads, wire) = edit_and_run(&mut app, &rx, &s3, full_scan);
    assert_eq!(reads, [QualityStage::CopyingSource], "the copy stands in");
    assert_eq!(wire.gets, FILES as u64);
    let results = app.analysis_modal.data_quality_results.as_ref().unwrap();
    assert!(results.reads.unwrap().copy.is_some());
    assert!(results.columns.iter().any(|column| column.name == "day"));

    let source = FakeS3::serve("lake", hive_events());
    let (mut direct, direct_rx) = open_remote_with(&source, copies_off(), None);
    edit_and_run(&mut direct, &direct_rx, &source, full_scan);
    assert_eq!(
        measured(&direct),
        measured(&app),
        "the copy reads as the source"
    );
}

/// One object opened from a bucket is copied too, its size from the footer read
/// that opened it: one GET for the copy, then every pass reads it.
#[test]
fn one_remote_object_is_copied_once() {
    let object = remote_events()
        .remove("events/part-0.parquet")
        .expect("the first object");
    let size = object.len() as u64;
    let s3 = FakeS3::serve(
        "lake",
        BTreeMap::from([("one.parquet".to_string(), object)]),
    );
    let cache = tempfile::tempdir().unwrap();
    let config = AppConfig {
        cloud: s3.cloud_config(),
        ..AppConfig::default()
    };
    let theme = datui::Theme::from_config(&config.theme).unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new_with_config(tx, crate::common::test_runtime(), theme, config);
    app.use_cache(datui::CacheManager::with_dir(cache.path().to_path_buf()));
    settle(
        &mut app,
        &rx,
        Some(AppEvent::Open(
            vec![PathBuf::from("s3://lake/one.parquet")],
            OpenOptions::default(),
        )),
    );
    assert_eq!(
        app.data_table_state.as_ref().map(|state| state.num_rows()),
        Some(ROWS)
    );
    press(&mut app, KeyCode::Char('a'));
    app.analysis_modal.sidebar_state.select(Some(3));
    press(&mut app, KeyCode::Enter);
    let (reads, wire) = edit_and_run(&mut app, &rx, &s3, |plan| {
        plan.method = datui::sampling::SampleMethod::EveryRow;
        plan.compute = QualityCompute::Full;
    });
    assert_eq!(reads, [QualityStage::CopyingSource]);
    assert_eq!(
        wire,
        WireCount {
            lists: 0,
            heads: 0,
            gets: 1,
            bytes: size,
        }
    );
    let results = app.analysis_modal.data_quality_results.as_ref().unwrap();
    assert_eq!(results.evaluated_rows, ROWS);
    assert_eq!(copies(cache.path()).len(), 1);
}

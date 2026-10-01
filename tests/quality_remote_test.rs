//! Data Quality over a remote multi-file dataset, counted at the wire.
//!
//! An in-process S3 stand-in (`common/fake_s3.rs`) serves a prefix of Parquet files
//! and counts every request and byte, so these tests say what each run costs the
//! source rather than what datui believes it read. Nothing leaves the loopback.

#![cfg(feature = "cloud")]

mod common;
#[path = "common/fake_s3.rs"]
mod fake_s3;

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use datui::data_quality::{
    QualityComparison, QualityGrain, QualityStage, TemporalRole, TemporalRoleAssignment,
};
use datui::{App, AppConfig, AppEvent, OpenOptions};
use fake_s3::{FakeS3, WireCount};
use polars::prelude::*;
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::widgets::Widget;
use std::collections::BTreeMap;
use std::path::PathBuf;
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

fn work_pending(app: &App) -> bool {
    app.is_busy() || app.row_count_pending()
}

/// The next event: one already sent, or one background work still owes.
fn next_event(app: &App, rx: &mpsc::Receiver<AppEvent>) -> Option<AppEvent> {
    // Only a hang guard; nothing here is timed.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(300);
    loop {
        if let Ok(event) = rx.try_recv() {
            return Some(event);
        }
        if !work_pending(app) {
            return None;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "background work never reported back"
        );
        if let Ok(event) = rx.recv_timeout(std::time::Duration::from_millis(50)) {
            return Some(event);
        }
    }
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
                if let AppEvent::BackgroundQualityPhase { generation, phase } = &event
                    && *generation == app.task_generation()
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
    let config = AppConfig {
        cloud: s3.cloud_config(),
        ..AppConfig::default()
    };
    let theme = datui::Theme::from_config(&config.theme).unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new_with_config(tx, common::test_runtime(), theme, config);
    settle(
        &mut app,
        &rx,
        Some(AppEvent::Open(
            vec![PathBuf::from("s3://lake/events/")],
            OpenOptions::default(),
        )),
    );
    assert_eq!(
        app.data_table_state.as_ref().map(|state| state.num_rows),
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
    if app.analysis_modal.data_quality_confirm_run {
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

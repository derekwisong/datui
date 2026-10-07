//! What Data Quality runs cost, measured: wall time, requests and bytes at the
//! source, peak memory and spill, for a first run and for role and grain edits.
//!
//! Ignored: these read millions of rows. `scripts/dev/quality_bench.py` runs them,
//! one scenario per process so peak memory is that scenario's, against this tree
//! and an earlier commit, and prints the table. To run one by hand:
//!
//! ```bash
//! cargo test --release --test quality_bench_test -- --ignored --exact remote_prefix --nocapture
//! ```
//!
//! Each step prints one line: `BENCH <tab> scenario <tab> step <tab> key=value...`.
//! Requests and bytes are counted by the in-process S3 stand-in, so only the remote
//! scenario has them; a local read goes through Polars' own file access, which
//! nothing here counts, so those are reported as unknown. Peak memory is the
//! process's resident high-water mark over the step (`VmHWM`, reset through
//! `/proc/self/clear_refs`). Spill is the most the Polars spill directory held,
//! and disk the most Data Quality's local copies held in the cache directory,
//! each sampled every few milliseconds.
//!
//! The steps press Run as a user would, on each version's own Setup page; the
//! fixtures are written once, deterministically, under `DATUI_BENCH_DIR`.

mod common;
#[path = "common/fake_s3.rs"]
mod fake_s3;

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use datui::analysis_modal::{AnalysisFocus, AnalysisTool};
use datui::data_quality::{
    QualityComparison, QualityCompute, QualityGrain, QualityPage, QualityScope, TemporalRole,
    TemporalRoleAssignment,
};
use datui::sampling::{Sample, SampleMethod};
use datui::{App, AppConfig, AppEvent, OpenOptions};
use polars::prelude::*;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

const ROWS: usize = 2_000_000;
const FILES: usize = 8;
const SAMPLE_ROWS: usize = 100_000;
const SEED: u64 = 42;

/// Where the fixtures live, and the spill directory.
fn bench_dir() -> PathBuf {
    std::env::var_os("DATUI_BENCH_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join("datui-quality-bench"))
}

/// Rows `from..to` of the benchmark table: an id; an event time every 37 seconds
/// from 2024-01-01 (about 2.3 years over the table); a received time up to ten
/// minutes later, missing every 997th row; four regions; an amount missing every
/// 13th row; and a note with 50,000 distinct values.
fn table(from: usize, to: usize) -> DataFrame {
    let start = 1_704_067_200_000_000i64;
    let ids = (from as i64..to as i64).collect::<Vec<_>>();
    let at = ids
        .iter()
        .map(|id| start + id * 37_000_000)
        .collect::<Vec<_>>();
    let sent = ids
        .iter()
        .zip(&at)
        .map(|(id, at)| (id % 997 != 0).then_some(at + (id % 600) * 1_000_000))
        .collect::<Vec<_>>();
    let datetime = DataType::Datetime(TimeUnit::Microseconds, None);
    df!(
        "id" => &ids,
        "at" => at,
        "sent" => sent,
        "region" => ids.iter().map(|id| ["North", "South", "East", "West"][*id as usize % 4]).collect::<Vec<_>>(),
        "amount" => ids.iter().map(|id| (id % 13 != 0).then_some(*id as f64 * 0.25)).collect::<Vec<_>>(),
        "note" => ids.iter().map(|id| format!("note {}", id % 50_000)).collect::<Vec<_>>(),
    )
    .unwrap()
    .lazy()
    .with_columns([
        col("at").cast(datetime.clone()),
        col("sent").cast(datetime),
    ])
    .collect()
    .unwrap()
}

/// The fixtures, written once: `FILES` Parquet parts, one Parquet file and one CSV
/// of the same `ROWS` rows.
fn fixtures() -> PathBuf {
    let dir = bench_dir().join("fixtures");
    let done = dir.join(".written");
    if done.exists() {
        return dir;
    }
    std::fs::create_dir_all(dir.join("parts")).unwrap();
    let per_file = ROWS / FILES;
    for file in 0..FILES {
        let mut part = table(file * per_file, (file + 1) * per_file);
        ParquetWriter::new(
            std::fs::File::create(dir.join("parts").join(format!("part-{file}.parquet"))).unwrap(),
        )
        .with_row_group_size(Some(50_000))
        .finish(&mut part)
        .unwrap();
    }
    let mut whole = table(0, ROWS);
    ParquetWriter::new(std::fs::File::create(dir.join("events.parquet")).unwrap())
        .with_row_group_size(Some(50_000))
        .finish(&mut whole)
        .unwrap();
    CsvWriter::new(std::fs::File::create(dir.join("events.csv")).unwrap())
        .finish(&mut whole)
        .unwrap();
    std::fs::write(&done, b"").unwrap();
    dir
}

/// The resident high-water mark starts again from what is resident now.
fn reset_peak_memory() {
    let _ = std::fs::write("/proc/self/clear_refs", "5");
}

/// `VmHWM` in KiB, or `None` where `/proc` does not have it.
fn peak_memory_kib() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    status
        .lines()
        .find_map(|line| line.strip_prefix("VmHWM:"))
        .and_then(|value| value.split_whitespace().next()?.parse().ok())
}

fn bytes_under(dir: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
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
}

/// Samples the spill directory while a step runs; the most it held.
struct SpillWatch {
    peak: Arc<AtomicU64>,
    running: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl SpillWatch {
    fn start(dir: PathBuf) -> SpillWatch {
        let peak = Arc::new(AtomicU64::new(0));
        let running = Arc::new(AtomicBool::new(true));
        let thread = {
            let (peak, running) = (peak.clone(), running.clone());
            std::thread::spawn(move || {
                while running.load(Ordering::Relaxed) {
                    peak.fetch_max(bytes_under(&dir), Ordering::Relaxed);
                    std::thread::sleep(Duration::from_millis(5));
                }
            })
        };
        SpillWatch {
            peak,
            running,
            thread: Some(thread),
        }
    }

    fn stop(mut self) -> u64 {
        self.running.store(false, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
        self.peak.load(Ordering::Relaxed)
    }
}

fn press(app: &mut App, code: KeyCode) -> Option<AppEvent> {
    app.event(&AppEvent::Key(KeyEvent::new(code, KeyModifiers::NONE)))
}

/// Handle `first` and every event after it until the app owes nothing.
fn settle(app: &mut App, rx: &mpsc::Receiver<AppEvent>, first: Option<AppEvent>) {
    let deadline = Instant::now() + Duration::from_secs(3_600);
    let mut next = first;
    loop {
        if let Some(event) = next.take() {
            next = app.event(&event);
            continue;
        }
        match rx.recv_timeout(Duration::from_millis(10)) {
            Ok(event) => next = Some(event),
            Err(_)
                if !app.is_busy()
                    && !app.row_count_pending()
                    && !app.background_work_in_flight() =>
            {
                return;
            }
            Err(_) => assert!(Instant::now() < deadline, "the run never finished"),
        }
    }
}

/// One setup to run: the shared sample and the study.
#[derive(Clone)]
struct Study {
    method: SampleMethod,
    grain: QualityGrain,
    roles: bool,
    compare: bool,
}

fn daily() -> QualityGrain {
    QualityGrain::TimeWindows {
        column: "at".into(),
        every: "1d".into(),
    }
}

fn weekly() -> QualityGrain {
    QualityGrain::TimeWindows {
        column: "at".into(),
        every: "1w".into(),
    }
}

/// The steps every scenario takes, in order: a first sampled daily run, then a
/// role edit and two grain edits on it; a first full scan, then the same edits and
/// a comparison.
fn steps() -> Vec<(&'static str, Study)> {
    let sampled = SampleMethod::Spread;
    let every = SampleMethod::EveryRow;
    let study = |method: &SampleMethod, grain, roles| Study {
        compare: false,
        method: method.clone(),
        grain,
        roles,
    };
    vec![
        ("sample: first run, daily", study(&sampled, daily(), false)),
        ("sample: + roles", study(&sampled, daily(), true)),
        ("sample: grain to weekly", study(&sampled, weekly(), true)),
        (
            "sample: grain to region",
            study(&sampled, QualityGrain::Partition("region".into()), true),
        ),
        ("full: first run, daily", study(&every, daily(), false)),
        ("full: + roles", study(&every, daily(), true)),
        ("full: grain to weekly", study(&every, weekly(), true)),
        (
            "full: compare each week with the one before",
            Study {
                compare: true,
                ..study(&every, weekly(), true)
            },
        ),
    ]
}

/// Stage `study` and press Run on the Data Quality setup page, as a user would.
fn run_study(app: &mut App, rx: &mpsc::Receiver<AppEvent>, study: &Study) {
    let sample = Sample {
        scope: QualityScope::CurrentView,
        method: study.method.clone(),
        rows: SAMPLE_ROWS,
        seed: SEED,
    };
    let modal = &mut app.analysis_modal;
    modal.active = true;
    modal.selected_tool = Some(AnalysisTool::DataQuality);
    modal.focus = AnalysisFocus::Main;
    modal.sample = sample.clone();
    modal.set_quality_page(QualityPage::Setup);
    let plan = &mut modal.quality.plan;
    plan.scope = sample.scope.clone();
    plan.method = sample.method.clone();
    plan.dataset_rows = sample.rows;
    plan.sample_seed = sample.seed;
    plan.compute = if study.method == SampleMethod::EveryRow {
        QualityCompute::Full
    } else {
        QualityCompute::Sample
    };
    plan.grain = study.grain.clone();
    plan.comparison = if study.compare {
        QualityComparison::Previous
    } else {
        QualityComparison::None
    };
    plan.temporal_roles = if study.roles {
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
    } else {
        Vec::new()
    };
    let mut first = press(app, KeyCode::Enter);
    if app.confirmation_modal.asks_full_scan() {
        // A full scan asks first; Enter there runs it.
        first = press(app, KeyCode::Enter);
    }
    settle(app, rx, first);
}

/// Open `path` and take every step, printing a line for each.
fn bench(scenario: &str, path: &str, config: AppConfig, wire: Option<&fake_s3::Wire>) {
    let spill = bench_dir().join("spill");
    std::fs::create_dir_all(&spill).unwrap();
    let theme = datui::Theme::from_config(&config.theme).unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new_with_config(tx, common::test_runtime(), theme, config);
    let opened = Instant::now();
    settle(
        &mut app,
        &rx,
        Some(AppEvent::Open(
            vec![PathBuf::from(path)],
            OpenOptions::default(),
        )),
    );
    assert!(
        app.data_table_state.is_some(),
        "{scenario}: {path} did not open"
    );
    eprintln!("{scenario}: opened in {:?}", opened.elapsed());
    // Where a full scan's local copy of a remote source goes, under the cache
    // directory the test harness isolates.
    let copies = std::env::var_os("DATUI_CACHE_DIR")
        .map(PathBuf::from)
        .unwrap_or_default()
        .join("quality-copies");
    for (step, study) in steps() {
        let before = wire.map(fake_s3::Wire::count);
        reset_peak_memory();
        let watch = SpillWatch::start(spill.clone());
        let disk = SpillWatch::start(copies.clone());
        let started = Instant::now();
        run_study(&mut app, &rx, &study);
        let took = started.elapsed();
        let spilled = watch.stop();
        let disk = disk.stop();
        let peak = peak_memory_kib()
            .map(|kib| kib.to_string())
            .unwrap_or_else(|| "unknown".into());
        let (gets, bytes) = match (wire, before) {
            (Some(wire), Some(before)) => {
                let delta = wire.count().since(&before);
                (delta.requests().to_string(), delta.bytes.to_string())
            }
            _ => ("unknown".into(), "unknown".into()),
        };
        let outcome = match app.analysis_modal.quality.results.as_ref() {
            _ if app.modal_showing() => "error".to_string(),
            Some(results) => format!(
                "evaluated={} segments={} intervals={} compared={}",
                results.evaluated_rows,
                results.segments.len(),
                results.temporal.len(),
                results
                    .segments
                    .iter()
                    .filter(|segment| segment.compared_with.is_some())
                    .count()
            ),
            None => "no report".to_string(),
        };
        println!(
            "BENCH\t{scenario}\t{step}\twall_ms={}\trequests={gets}\tbytes={bytes}\tpeak_rss_kib={peak}\tspill_bytes={spilled}\tdisk_bytes={disk}\t{outcome}",
            took.as_millis()
        );
        assert!(!app.modal_showing(), "{scenario}: {step} failed");
    }
}

/// The env every scenario runs with: Polars spills, if it ever does, where it is
/// measured.
fn prepare() -> PathBuf {
    let dir = fixtures();
    // SAFETY: set before Polars reads its configuration; one scenario per process.
    unsafe {
        std::env::set_var("POLARS_OOC_SPILL_DIR", bench_dir().join("spill"));
    }
    dir
}

#[test]
#[ignore = "a benchmark: run through scripts/dev/quality_bench.py"]
fn remote_prefix() {
    let dir = prepare();
    let objects = (0..FILES)
        .map(|file| {
            let name = format!("part-{file}.parquet");
            let bytes = std::fs::read(dir.join("parts").join(&name)).unwrap();
            (format!("events/{name}"), bytes)
        })
        .collect::<BTreeMap<_, _>>();
    let s3 = fake_s3::FakeS3::serve("lake", objects);
    let config = AppConfig {
        cloud: s3.cloud_config(),
        ..AppConfig::default()
    };
    bench(
        "remote: 8 Parquet files over S3",
        "s3://lake/events/",
        config,
        Some(&s3.wire),
    );
}

#[test]
#[ignore = "a benchmark: run through scripts/dev/quality_bench.py"]
fn local_csv() {
    let dir = prepare();
    let path = dir.join("events.csv");
    bench(
        "local: one CSV",
        path.to_str().unwrap(),
        AppConfig::default(),
        None,
    );
}

#[test]
#[ignore = "a benchmark: run through scripts/dev/quality_bench.py"]
fn local_parquet() {
    let dir = prepare();
    let path = dir.join("events.parquet");
    bench(
        "local: one Parquet file",
        path.to_str().unwrap(),
        AppConfig::default(),
        None,
    );
}

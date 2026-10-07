//! The App end to end, one test executable. Shared helpers live here; each module
//! holds one area's tests, and the `formats`, `cloud` and `quality` directories hold
//! modules of their own. Filter by module, as in
//! `scripts/dev/test.sh integration app loading::` or `app formats_follow::`.

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use datui::analysis::analysis_modal::AnalysisTool;
use datui::app::event_pump::EventPump;
use datui::{App, AppEvent, InputMode, JobKind, OpenOptions, Overlay, QueryMode};
use polars::prelude::*;
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::widgets::Widget;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::mpsc;

#[path = "formats/audio.rs"]
mod audio;
#[path = "formats/can.rs"]
mod can;
#[cfg(feature = "cloud")]
#[path = "cloud/download.rs"]
mod cloud_download;
#[cfg(feature = "cloud")]
#[path = "cloud/parity.rs"]
mod cloud_parity;
#[path = "../common/mod.rs"]
mod common;
#[path = "formats/elf.rs"]
mod elf;
#[cfg(feature = "cloud")]
#[path = "../common/fake_s3.rs"]
mod fake_s3;
#[path = "formats/flight_logs.rs"]
mod flight_logs;
#[path = "formats/delimited.rs"]
mod formats_delimited;
#[path = "formats/follow.rs"]
mod formats_follow;
#[path = "formats/lines.rs"]
mod formats_lines;
#[path = "formats/open.rs"]
mod formats_open;
#[path = "formats/gps.rs"]
mod gps;
#[path = "formats/hex.rs"]
mod hex;
#[path = "formats/journal.rs"]
mod journal;
#[path = "formats/midi.rs"]
mod midi;
#[path = "formats/model_files.rs"]
mod model_files;
#[path = "formats/numpy.rs"]
mod numpy;
#[cfg(feature = "cloud")]
#[path = "quality/remote.rs"]
mod remote_quality;
#[cfg(feature = "sqlite")]
#[path = "formats/sqlite.rs"]
mod sqlite;
#[path = "table_sample.rs"]
mod table_sample;
#[path = "formats/tables.rs"]
mod tables;
#[path = "formats/text_formats.rs"]
mod text_formats;
use common::{drain_events, next_event, pump_open_until_loaded, work_pending};

/// Enter on a tool in the Analysis sidebar. A tool with no result yet shows its
/// Sample form in the pane rather than running; the next Enter runs it.
fn show_sample_form(app: &mut App) {
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
}

/// Ticks for a loop that polls the way `run()` does until what it waits for is true.
/// Bounded by a hang guard rather than a count: on a loaded machine a count of ticks
/// runs out before the work does. The guard fails the test instead of ending the loop,
/// so a wait that never came true cannot fall through to asserts that pass anyway.
#[track_caller]
fn ticks() -> impl Iterator<Item = usize> {
    let caller = std::panic::Location::caller();
    let deadline = std::time::Instant::now() + common::HANG_GUARD;
    (0..).inspect(move |_| {
        assert!(
            std::time::Instant::now() < deadline,
            "the wait at {caller} never finished"
        );
    })
}

/// Hand results and their follow-ups back through the channel, as `run()` does, until
/// `done`. Waits on the channel between checks, so a slow machine costs time and
/// never the answer.
#[track_caller]
fn pump_until(
    app: &mut App,
    rx: &mpsc::Receiver<AppEvent>,
    tx: &mpsc::Sender<AppEvent>,
    done: impl Fn(&App) -> bool,
) {
    for _ in ticks() {
        while let Ok(ev) = rx.try_recv() {
            if let Some(next) = app.event(ev) {
                let _ = tx.send(next);
            }
        }
        // As `run()` paints after every update.
        app.frame_painted();
        if done(app) {
            return;
        }
        if let Ok(ev) = rx.recv_timeout(std::time::Duration::from_millis(50))
            && let Some(next) = app.event(ev)
        {
            let _ = tx.send(next);
        }
    }
}

/// As `pump_open_until_loaded`, but hands back the message a failed open ended with.
///
/// A load that fails ends its job with the reason, which the app shows — the reading
/// happens off the event thread — and only the paths that never get that far crash
/// outright.
fn pump_open_until_error(
    app: &mut App,
    rx: &std::sync::mpsc::Receiver<AppEvent>,
    paths: Vec<PathBuf>,
    options: OpenOptions,
) -> Option<String> {
    let mut next: Option<AppEvent> = Some(AppEvent::Open(paths, options));
    loop {
        match next.take() {
            Some(AppEvent::Crash(message)) => return Some(message),
            Some(ev) => {
                next = app.event(ev);
                if let Some(message) = app.error_message() {
                    return Some(message.to_string());
                }
            }
            None => next = Some(next_event(app, rx)?),
        }
    }
}

/// A line of y over x, on the chart view `open_chart_view` opened.
fn select_line(app: &mut App) {
    use datui::chart::chart_modal::Mark;
    app.chart.modal.set_mark(Mark::Line);
    app.chart.modal.spec.encoding.x.field = Some("x".to_string());
    app.chart.modal.spec.encoding.y.field = vec!["y".to_string()];
}

/// Opens a small x/y dataset in the chart view, from the first column: `c` on a
/// number suggests its histogram.
fn open_chart_view(name: &str) -> (App, mpsc::Receiver<AppEvent>, mpsc::Sender<AppEvent>) {
    let test_data_dir = common::fixture_dir();
    let csv_path = test_data_dir.join(name);
    let mut df = df!(
        "x" => (0..5).collect::<Vec<i32>>(),
        "y" => (0..5).map(|i| i * 3).collect::<Vec<i32>>()
    )
    .unwrap();
    let mut file = File::create(&csv_path).unwrap();
    CsvWriter::new(&mut file).finish(&mut df).unwrap();

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![csv_path], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);
    assert!(app.data_table_state.is_some());

    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Char('c'),
        KeyModifiers::NONE,
    )));
    assert_eq!(app.overlay, Overlay::Chart);
    (app, rx, tx)
}

/// Feed background results back until the chart for the current selection is prepared.
#[track_caller]
fn pump_until_chart_ready(
    app: &mut App,
    rx: &mpsc::Receiver<AppEvent>,
    tx: &mpsc::Sender<AppEvent>,
) {
    pump_until(app, rx, tx, App::chart_data_ready);
}

/// A small chart export to `path`, where nothing is yet.
fn chart_export_request(
    path: &Path,
    format: datui::chart::chart_export::ChartExportFormat,
) -> datui::chart::chart_export::ChartExportRequest {
    datui::chart::chart_export::ChartExportRequest {
        path: path.to_path_buf(),
        format,
        options: datui::chart::chart_export::ExportOptions {
            width: 400,
            height: 300,
            dpi: 96.0,
            ..Default::default()
        },
        overwrite: datui::export::output_file::Overwrite::Forbid,
        recipe: false,
    }
}

/// Flights by carrier and origin, with a date and a delay: a category, a second
/// category, a date and a number.
fn open_flights(name: &str) -> (App, mpsc::Receiver<AppEvent>, mpsc::Sender<AppEvent>) {
    let path = common::fixture_dir().join(name);
    let carriers = ["UA", "B6", "EV", "DL", "AA", "MQ", "US", "WN", "F9"];
    let origins = ["EWR", "JFK", "LGA"];
    let n = 900;
    let mut df = df!(
        "carrier" => (0..n).map(|i| carriers[i % carriers.len()]).collect::<Vec<_>>(),
        "origin" => (0..n).map(|i| origins[i % origins.len()]).collect::<Vec<_>>(),
        "day" => (0..n).map(|i| 19723 + (i as i32 / 10)).collect::<Vec<i32>>(),
        "delay" => (0..n).map(|i| (i % carriers.len()) as f64 + (i % 2) as f64).collect::<Vec<f64>>()
    )
    .unwrap();
    df.apply("day", |c| c.cast(&DataType::Date).unwrap())
        .unwrap();
    ParquetWriter::new(File::create(&path).unwrap())
        .finish(&mut df)
        .unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);
    (app, rx, tx)
}

/// Wait for the outcome of a background scan.
///
/// Scanning runs off the event thread so a slow one cannot freeze the interface, so
/// its result arrives over the channel rather than as a return value.
fn await_scan_outcome(rx: &mpsc::Receiver<AppEvent>) -> AppEvent {
    rx.recv_timeout(std::time::Duration::from_secs(30))
        .expect("background scan should report an outcome")
}

/// A table with whole rows copied (but for their bytes), text that parses but for a
/// few values, codes that all parse, and two columns missing at different rates.
fn open_findings_fixture(
    name: &str,
) -> (
    App,
    mpsc::Receiver<AppEvent>,
    mpsc::Sender<AppEvent>,
    PathBuf,
) {
    let dir = common::fixture_dir();
    let path = dir.join(name);
    // A thousand distinct rows, then two more copies of the first three hundred.
    let rows = (0..1_000i64)
        .chain(0..300)
        .chain(0..300)
        .collect::<Vec<_>>();
    let mut df = df!(
        "id" => &rows,
        "code" => rows.iter().map(|row| if row % 50 == 0 { "n/a".to_string() } else { (1_000 + row).to_string() }).collect::<Vec<_>>(),
        "zip" => rows.iter().map(|row| format!("{:05}", row % 97)).collect::<Vec<_>>(),
        "a" => rows.iter().map(|row| (row % 9 != 0).then_some(*row as f64)).collect::<Vec<_>>(),
        "b" => rows.iter().map(|row| (row % 4 != 1).then_some(*row as f64)).collect::<Vec<_>>(),
    )
    .unwrap();
    // Bytes that differ on every row, copies too: the checks read binary as one stub,
    // so they do not tell copies apart, and neither may the rows a finding opens.
    let blob = (0..rows.len() as u32)
        .map(|position| position.to_le_bytes().to_vec())
        .collect::<Vec<_>>();
    df.with_column(Column::new("blob".into(), blob)).unwrap();
    ParquetWriter::new(File::create(&path).unwrap())
        .finish(&mut df)
        .unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path.clone()], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);
    (app, rx, tx, path)
}

/// Nothing was started: the app is idle, holds nothing, and nothing is on the channel.
#[track_caller]
fn assert_nothing_started(app: &mut App, rx: &mpsc::Receiver<AppEvent>) {
    assert!(!app.is_busy(), "nothing is running");
    assert!(
        !app.background_work_in_flight(),
        "no background work was started"
    );
    assert!(rx.try_recv().is_err(), "and nothing has answered");
}

/// Every character on screen that is not ASCII is a glyph slot, which has an ASCII
/// twin under `LANG=C`.
fn assert_glyph_slots(screen: &str) {
    let g = datui::glyphs::get();
    let slots = [
        g.rail,
        g.rule_h,
        g.middot,
        g.ellipsis,
        g.warning,
        g.check,
        g.dash,
        g.times,
        g.updown,
        g.updown_lr,
        g.null,
        g.scroll_thumb,
        g.scroll_track,
        g.binary_stub,
    ]
    .concat();
    for c in screen.chars().filter(|c| !c.is_ascii()) {
        assert!(
            slots.contains(c) || "╭╮╰╯│─".contains(c),
            "{c:?} is not a glyph slot:\n{screen}"
        );
    }
}

/// A table whose times are text in a US format, the way many CSV exports write
/// them: nothing reads them as time until Setup is told how.
fn open_text_times_fixture(
    name: &str,
) -> (
    App,
    mpsc::Receiver<AppEvent>,
    mpsc::Sender<AppEvent>,
    PathBuf,
) {
    let dir = common::fixture_dir();
    let path = dir.join(name);
    let rows = 600i64;
    let created = (0..rows)
        .map(|row| {
            (row % 150 != 7).then(|| {
                format!(
                    "01/{:02}/2024 {:02}:{:02}:00",
                    1 + row % 5,
                    8 + row % 10,
                    row % 60
                )
            })
        })
        .collect::<Vec<_>>();
    let mut created = created;
    created[11] = Some("not a time".to_string());
    let sent = (0..rows)
        .map(|row| {
            Some(format!(
                "01/{:02}/2024 {:02}:{:02}:00",
                1 + row % 5,
                9 + row % 10,
                row % 60
            ))
        })
        .collect::<Vec<_>>();
    let mut df = df!(
        "id" => (0..rows).collect::<Vec<_>>(),
        "created" => created,
        "sent" => sent,
        "region" => (0..rows).map(|row| ["West", "East"][row as usize % 2]).collect::<Vec<_>>(),
    )
    .unwrap();
    ParquetWriter::new(File::create(&path).unwrap())
        .finish(&mut df)
        .unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path.clone()], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);
    (app, rx, tx, path)
}

/// Handle events until the work is done, counting the Data Quality runs that
/// finished and the stages that were reported on the way.
fn drain_quality(
    app: &mut App,
    rx: &mpsc::Receiver<AppEvent>,
    first: Option<AppEvent>,
) -> (usize, usize) {
    let (mut finished, mut stages) = (0, 0);
    let mut handle = |app: &mut App, event: AppEvent| {
        let mut next = Some(event);
        while let Some(event) = next {
            let run = matches!(&event, AppEvent::JobEnded(t) if t.kind() == JobKind::Analysis);
            if let AppEvent::JobProgress { ticket, .. } = &event
                && app.job_is_current(*ticket)
            {
                stages += 1
            }
            next = app.event(event);
            // A run that failed says so; one that finished does not.
            if run && app.error_message().is_none() {
                finished += 1;
            }
        }
    };
    if let Some(event) = first {
        handle(app, event);
    }
    while let Some(event) = next_event(app, rx) {
        handle(app, event);
    }
    (finished, stages)
}

/// Run from Setup, handle events until the work is done, and return the stages
/// that read the source, in order. Empty when the run read nothing.
fn run_quality_reads(
    app: &mut App,
    rx: &mpsc::Receiver<AppEvent>,
) -> Vec<datui::analysis::data_quality::QualityStage> {
    let first = press(app, KeyCode::Enter);
    assert!(
        matches!(
            first,
            Some(AppEvent::AnalysisCompute(AnalysisTool::DataQuality))
        ),
        "Enter runs"
    );
    let mut reads = Vec::new();
    let mut handle = |app: &mut App, event: AppEvent| {
        let mut next = Some(event);
        while let Some(event) = next {
            if let AppEvent::JobProgress {
                ticket,
                progress: datui::Progress::QualityPhase(phase),
            } = &event
                && app.job_is_current(*ticket)
                && phase.reads_source
            {
                reads.push(phase.stage);
            }
            next = app.event(event);
        }
    };
    handle(app, first.unwrap());
    while let Some(event) = next_event(app, rx) {
        handle(app, event);
    }
    assert!(app.analysis_modal.quality.results.is_some());
    reads
}

/// Weekday rows over eight weeks, forty a day, the second week missing: what a
/// business feed looks like. Written as CSV, which the sampler streams.
fn open_weekday_feed(name: &str) -> (App, mpsc::Receiver<AppEvent>, mpsc::Sender<AppEvent>) {
    let dir = common::fixture_dir();
    let path = dir.join(name);
    // 2024-01-01 is a Monday.
    let days = (0..56)
        .filter(|day| day % 7 < 5 && !(7..14).contains(day))
        .collect::<Vec<i32>>();
    let day = days
        .iter()
        .flat_map(|day| std::iter::repeat_n(19_723 + day, 40))
        .collect::<Vec<_>>();
    let rows = day.len();
    let mut df = df!(
        "day" => day,
        "amount" => (0..rows).map(|row| (row % 3 != 0).then_some(row as f64)).collect::<Vec<_>>(),
    )
    .unwrap()
    .lazy()
    .with_column(col("day").cast(DataType::Date))
    .collect()
    .unwrap();
    CsvWriter::new(File::create(&path).unwrap())
        .finish(&mut df)
        .unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);
    (app, rx, tx)
}

/// `rows` rows written to `<name>` in a fresh fixture directory, as CSV or Parquet by its
/// extension: an id, a region, and an amount missing on every `gap`th row (never
/// with a gap of 0). Opened, with Data Quality's Setup on screen.
fn open_quality_fixture(
    name: &str,
    rows: usize,
    gap: usize,
) -> (
    App,
    mpsc::Receiver<AppEvent>,
    mpsc::Sender<AppEvent>,
    PathBuf,
) {
    let path = common::fixture_dir().join(name);
    write_quality_fixture(&path, rows, gap);
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path.clone()], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);
    open_quality_setup(&mut app);
    (app, rx, tx, path)
}

fn write_quality_fixture(path: &Path, rows: usize, gap: usize) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let amounts = (0..rows)
        .map(|row| (gap == 0 || row % gap != 0).then_some(row as f64 * 1.5))
        .collect::<Vec<_>>();
    let mut df = df!(
        "id" => (0..rows as i64).collect::<Vec<_>>(),
        "region" => (0..rows).map(|row| ["North", "South"][row % 2]).collect::<Vec<_>>(),
        "amount" => amounts,
    )
    .unwrap();
    let file = File::create(path).unwrap();
    if path.extension().is_some_and(|ext| ext == "csv") {
        CsvWriter::new(file).finish(&mut df).unwrap();
    } else {
        ParquetWriter::new(file).finish(&mut df).unwrap();
    }
}

/// `a`, Data Quality, Enter: its Setup, which reads nothing.
fn open_quality_setup(app: &mut App) {
    press(app, KeyCode::Char('a'));
    app.analysis_modal.focus = datui::analysis::analysis_modal::AnalysisFocus::Sidebar;
    app.analysis_modal.sidebar_state.select(Some(3));
    assert!(press(app, KeyCode::Enter).is_none());
    assert!(!app.is_busy());
}

fn amount_nulls(app: &App) -> usize {
    app.analysis_modal
        .quality
        .results
        .as_ref()
        .unwrap()
        .columns
        .iter()
        .find(|column| column.name == "amount")
        .unwrap()
        .null_count
}

/// A Hive directory one file past a wave of footers: day `i` holds `i + 1` rows of
/// `v`, and day 30 alone has a `late` column, which only a full footer pass finds.
/// Returns the file paths in scan order and the total rows.
fn write_past_one_wave(dir: &Path) -> (Vec<PathBuf>, usize) {
    let days = datui::formats::schema_union::FOOTERS_AT_ONCE + 6;
    let mut files = Vec::new();
    let mut total = 0;
    for i in 0..days {
        let rows = i + 1;
        let v: Vec<i64> = (0..rows as i64).collect();
        let df = if i == 30 {
            df!("v" => &v, "late" => vec!["x"; rows]).unwrap()
        } else {
            df!("v" => &v).unwrap()
        };
        let sub = format!("day={i:03}");
        write_parquet(dir, &sub, df);
        files.push(dir.join(sub).join("data.parquet"));
        total += rows;
    }
    (files, total)
}

/// How many times each local footer under `dir` was read, for as long as the guard
/// lives.
fn count_footer_reads(
    dir: &Path,
) -> (
    std::sync::Arc<std::sync::Mutex<std::collections::HashMap<PathBuf, usize>>>,
    datui::formats::schema_union::FooterHookGuard,
) {
    let reads = std::sync::Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));
    let counted = reads.clone();
    let guard = datui::formats::schema_union::on_local_footer_read(dir, move |path| {
        *counted
            .lock()
            .unwrap()
            .entry(path.to_path_buf())
            .or_insert(0) += 1;
    });
    (reads, guard)
}

/// Which of `files` are opened, by anyone, from when it is made: the kernel's count
/// (inotify), since Polars opens a file without telling datui.
#[cfg(target_os = "linux")]
struct OpenWatch {
    fd: i32,
    watches: std::collections::HashMap<i32, PathBuf>,
}

#[cfg(target_os = "linux")]
impl OpenWatch {
    fn new(files: &[PathBuf]) -> Self {
        use std::os::unix::ffi::OsStrExt;
        // SAFETY: plain syscalls on a descriptor this struct owns and closes on drop.
        let fd = unsafe { libc::inotify_init1(libc::IN_NONBLOCK | libc::IN_CLOEXEC) };
        assert!(fd >= 0, "inotify_init1");
        let watches = files
            .iter()
            .map(|file| {
                let path = std::ffi::CString::new(file.as_os_str().as_bytes()).unwrap();
                let wd = unsafe { libc::inotify_add_watch(fd, path.as_ptr(), libc::IN_OPEN) };
                assert!(wd >= 0, "inotify_add_watch {}", file.display());
                (wd, file.clone())
            })
            .collect();
        OpenWatch { fd, watches }
    }

    /// The files opened since the last call.
    fn opened(&self) -> std::collections::BTreeSet<PathBuf> {
        let mut opened = std::collections::BTreeSet::new();
        let mut buf = vec![0u8; 64 * 1024];
        loop {
            let n = unsafe { libc::read(self.fd, buf.as_mut_ptr().cast(), buf.len()) };
            if n <= 0 {
                return opened;
            }
            let mut at = 0;
            while at < n as usize {
                // SAFETY: the kernel writes whole events; read_unaligned for the buffer's
                // alignment.
                let event: libc::inotify_event =
                    unsafe { std::ptr::read_unaligned(buf[at..].as_ptr().cast()) };
                if let Some(file) = self.watches.get(&event.wd) {
                    opened.insert(file.clone());
                }
                at += std::mem::size_of::<libc::inotify_event>() + event.len as usize;
            }
        }
    }
}

#[cfg(target_os = "linux")]
impl Drop for OpenWatch {
    fn drop(&mut self) {
        unsafe { libc::close(self.fd) };
    }
}

/// Open a local directory of Parquet files and return the loaded app, or `None` if the
/// open never finished.
fn open_local_dataset(dir: &std::path::Path) -> App {
    open_local_dataset_with_channel(dir).0
}

/// The same, keeping the app's own event channel so a test can drive background work.
fn open_local_dataset_with_channel(
    dir: &std::path::Path,
) -> (App, mpsc::Receiver<AppEvent>, mpsc::Sender<AppEvent>) {
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    let opts = OpenOptions {
        hive: true,
        ..OpenOptions::default()
    };
    pump_open_until_loaded(&mut app, &rx, vec![dir.to_path_buf()], opts);
    (app, rx, tx)
}

/// Write one Parquet file at `sub/data.parquet` under `dir`.
fn write_parquet(dir: &std::path::Path, sub: &str, mut df: polars::prelude::DataFrame) {
    let d = dir.join(sub);
    std::fs::create_dir_all(&d).unwrap();
    let f = File::create(d.join("data.parquet")).unwrap();
    ParquetWriter::new(f).finish(&mut df).unwrap();
}

/// Render a loaded app until no collect is owed and no background work is still to
/// report, and return what the table area shows. Idle alone is not enough: a row
/// count landing later can widen the buffer after the app first goes quiet. Drains the
/// app's events each pass: a buffer fill lands as one, so without it the screen is
/// whatever the first synchronous collect managed.
#[track_caller]
fn painted(
    app: &mut App,
    rx: &mpsc::Receiver<AppEvent>,
    tx: &mpsc::Sender<AppEvent>,
    area: Rect,
) -> String {
    let mut buf = Buffer::empty(area);
    for _ in ticks() {
        app.render(area, &mut buf);
        app.frame_painted();
        let mut handled = false;
        while let Ok(ev) = rx.try_recv() {
            handled = true;
            if let Some(next) = app.event(ev) {
                let _ = tx.send(next);
            }
        }
        let needs = app
            .data_table_state
            .as_mut()
            .map(|s| {
                let n = s.needs_recollect;
                s.needs_recollect = false;
                n
            })
            .unwrap_or(false);
        // Something that landed since the frame was drawn needs a frame of its own
        // before the view can be called finished.
        if !handled && !needs && !work_pending(app) {
            break;
        }
        if needs {
            app.spawn_async_collect("Loading buffer...");
        }
        common::wait_for_event(tx, rx);
    }
    app.render(area, &mut buf);
    buf.content().iter().map(|cell| cell.symbol()).collect()
}

/// Run a CSV export through the app's own export events and return the header line
/// of the file it wrote.
fn export_csv_header(
    app: &mut App,
    rx: &mpsc::Receiver<AppEvent>,
    tx: &mpsc::Sender<AppEvent>,
    path: &std::path::Path,
) -> String {
    export_csv(app, rx, tx, path, false)
        .lines()
        .next()
        .expect("with a header")
        .to_string()
}

/// Run a CSV export through the app's own export events and return the whole file.
/// `source_file` asks it to name the file each row came from.
fn export_csv(
    app: &mut App,
    rx: &mpsc::Receiver<AppEvent>,
    tx: &mpsc::Sender<AppEvent>,
    path: &std::path::Path,
    source_file: bool,
) -> String {
    export_as(
        app,
        rx,
        tx,
        path,
        datui::export::export_modal::ExportFormat::Csv,
        source_file,
    );
    std::fs::read_to_string(path).expect("the export wrote a file")
}

/// Run an export in `format` through the app's own export events.
fn export_as(
    app: &mut App,
    rx: &mpsc::Receiver<AppEvent>,
    tx: &mpsc::Sender<AppEvent>,
    path: &std::path::Path,
    format: datui::export::export_modal::ExportFormat,
    source_file: bool,
) {
    let options = datui::ExportOptions {
        source_file,
        csv_delimiter: b',',
        csv_include_header: true,
        csv_compression: None,
        json_compression: None,
        ndjson_compression: None,
    };
    let start = AppEvent::Applied(datui::Applied::Export(datui::ExportRequest {
        path: path.to_path_buf(),
        format,
        options,
        overwrite: datui::export::output_file::Overwrite::Forbid,
    }));
    run_to_idle(app, rx, tx, start);
    assert_eq!(app.error_message(), None, "the export to {path:?} failed");
    assert_eq!(
        app.export_modal.path_error, None,
        "the export to {path:?} failed"
    );
}

/// Feed `first` to the app and pump until nothing is left to do.
fn run_to_idle(
    app: &mut App,
    rx: &mpsc::Receiver<AppEvent>,
    tx: &mpsc::Sender<AppEvent>,
    first: AppEvent,
) {
    if let Some(next) = app.event(first) {
        let _ = tx.send(next);
    }
    pump_until_idle(app, rx, tx);
}

/// Names in `dir` besides `keep`: what an export left behind.
fn leftovers(dir: &Path, keep: &[&str]) -> Vec<String> {
    std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|name| !keep.contains(&name.as_str()))
        .collect()
}

fn csv_request(
    path: &Path,
    overwrite: datui::export::output_file::Overwrite,
) -> datui::ExportRequest {
    datui::ExportRequest {
        path: path.to_path_buf(),
        format: datui::export::export_modal::ExportFormat::Csv,
        options: datui::ExportOptions {
            source_file: false,
            csv_delimiter: b',',
            csv_include_header: true,
            csv_compression: Some(datui::CompressionFormat::Gzip),
            json_compression: None,
            ndjson_compression: None,
        },
        overwrite,
    }
}

/// Read a CSV export back with every column as text, sorted by `key`.
fn read_csv_as_text(path: &std::path::Path, key: &str) -> DataFrame {
    CsvReadOptions::default()
        .with_infer_schema_length(Some(0))
        .try_into_reader_with_file_path(Some(path.to_path_buf()))
        .unwrap()
        .finish()
        .unwrap()
        .sort([key], Default::default())
        .unwrap()
}

fn text_column(df: &DataFrame, name: &str) -> Vec<Option<String>> {
    let text = df.column(name).unwrap().str().unwrap();
    (0..text.len())
        .map(|i| text.get(i).map(str::to_string))
        .collect()
}

/// The writer schema from an Avro file's header.
fn avro_schema(path: &Path) -> serde_json::Value {
    fn long(bytes: &[u8], at: &mut usize) -> i64 {
        let (mut value, mut shift) = (0u64, 0);
        loop {
            let byte = bytes[*at];
            *at += 1;
            value |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return (value >> 1) as i64 ^ -((value & 1) as i64);
            }
            shift += 7;
        }
    }
    fn bytes_at<'a>(bytes: &'a [u8], at: &mut usize) -> &'a [u8] {
        let len = long(bytes, at) as usize;
        *at += len;
        &bytes[*at - len..*at]
    }
    let bytes = std::fs::read(path).unwrap();
    assert_eq!(&bytes[..4], b"Obj\x01");
    let mut at = 4;
    loop {
        let count = long(&bytes, &mut at);
        assert_ne!(count, 0, "no avro.schema in the header");
        if count < 0 {
            long(&bytes, &mut at);
        }
        for _ in 0..count.abs() {
            let key = bytes_at(&bytes, &mut at);
            let value = bytes_at(&bytes, &mut at);
            if key == b"avro.schema" {
                return serde_json::from_slice(value).unwrap();
            }
        }
    }
}

/// Every record and field name in an Avro schema.
fn avro_schema_names(schema: &serde_json::Value, names: &mut Vec<String>) {
    use serde_json::Value;
    match schema {
        Value::Array(branches) => branches.iter().for_each(|b| avro_schema_names(b, names)),
        Value::Object(map) => {
            if let Some(Value::String(name)) = map.get("name") {
                names.push(name.clone());
            }
            for field in map
                .get("fields")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                names.push(field["name"].as_str().unwrap().to_string());
                avro_schema_names(&field["type"], names);
            }
            for key in ["type", "items"] {
                if let Some(inner) = map.get(key) {
                    avro_schema_names(inner, names);
                }
            }
        }
        _ => {}
    }
}

/// A `/`-separated path as this platform writes it.
fn native(path: &str) -> String {
    path.replace('/', std::path::MAIN_SEPARATOR_STR)
}

// ---------------------------------------------------------------------------
// Abandoning an in-flight load (Ctrl+O to the home screen)
// ---------------------------------------------------------------------------

/// Drains like the real main loop does: a handler that returns a follow-up event
/// queues it and *ends the pass*, so one frame is drawn between chain steps.
///
/// `pump_open_until_loaded` above chases the chain without breaking, which cannot
/// reproduce a keypress landing between two steps — exactly the window abandonment
/// has to survive. Returns the number of chain steps taken this pass.
fn drain_like_main_loop(
    app: &mut App,
    tx: &mpsc::Sender<AppEvent>,
    rx: &mpsc::Receiver<AppEvent>,
) -> usize {
    let mut steps = 0;
    loop {
        match rx.try_recv() {
            Ok(AppEvent::Crash(msg)) => panic!("Crash during load: {msg}"),
            Ok(event) => {
                if let Some(next) = app.event(event) {
                    tx.send(next).unwrap();
                    steps += 1;
                    break;
                }
            }
            Err(_) => break,
        }
    }
    steps
}

fn ctrl_o() -> AppEvent {
    AppEvent::Key(KeyEvent::new(KeyCode::Char('o'), KeyModifiers::CONTROL))
}

fn key(code: KeyCode) -> AppEvent {
    AppEvent::Key(KeyEvent::new(code, KeyModifiers::NONE))
}

/// The same, without the footer on the last row. The bar reports a load on its
/// own; assertions about what the *view* shows have to exclude it.
fn main_area_text(buf: &Buffer, area: Rect) -> String {
    let cells = (area.width as usize) * (area.height as usize - 1);
    buf.content()
        .iter()
        .take(cells)
        .map(|cell| cell.symbol())
        .collect()
}

/// The table area's rows, trimmed, once `path` is open and known to hold no rows.
fn empty_table_lines(path: PathBuf, options: OpenOptions) -> Vec<String> {
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path], options);
    assert_eq!(app.error_message(), None);
    let area = Rect::new(0, 0, 60, 8);
    let mut buffer = Buffer::empty(area);
    // The first frame sizes the table, which asks for its rows again.
    for _ in 0..2 {
        buffer = Buffer::empty(area);
        app.render(area, &mut buffer);
        pump_until(&mut app, &rx, &tx, |app| {
            !app.is_busy()
                && app
                    .data_table_state
                    .as_ref()
                    .and_then(|s| s.num_rows_if_valid())
                    == Some(0)
        });
    }
    app.render(area, &mut buffer);
    main_area_text(&buffer, area)
        .chars()
        .collect::<Vec<_>>()
        .chunks(area.width as usize)
        .map(|row| row.iter().collect::<String>().trim().to_string())
        .collect()
}

/// Opening a remote URL raises a "Continue with download?" confirmation. Declining it
/// used to quit datui, and Ctrl+O was swallowed while it was up, which made a remote
/// open the one thing in the app you could not back out of.
#[cfg(feature = "http")]
fn app_awaiting_open_confirmation() -> (App, mpsc::Receiver<AppEvent>) {
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    // HEAD refused at once, so the size probe answers unknown without its timeout. A
    // refused connection would be no server at all, which ends the open.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = PathBuf::from(format!(
        "http://{}/data.csv",
        listener.local_addr().unwrap()
    ));
    std::thread::spawn(move || {
        use std::io::{Read, Write};
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let mut head = Vec::new();
            let mut byte = [0u8; 1];
            while !head.ends_with(b"\r\n\r\n") && stream.read(&mut byte).unwrap_or(0) == 1 {
                head.push(byte[0]);
            }
            let _ = write!(
                stream,
                "HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            );
        }
    });
    let mut next = app.event(AppEvent::Open(vec![url], OpenOptions::default()));
    while let Some(ev) = next {
        if matches!(ev, AppEvent::Crash(_)) {
            break;
        }
        next = app.event(ev);
    }
    // The size probe runs on a background thread now, so the modal arrives by event
    // rather than before the open call returns.
    //
    // The budget is deliberately far longer than the probe should ever need. The
    // first HTTP agent built in a process loads the platform certificate store,
    // which is slow on a cold Windows runner, and a second was not enough: both
    // tests using this helper failed there on the v0.3.2 release commit, the first
    // time Windows had run them. What is being asserted is that datui asks before
    // downloading, not that it asks within any particular time.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while !app.awaiting_open_confirmation() && std::time::Instant::now() < deadline {
        while let Ok(ev) = rx.try_recv() {
            if let Some(follow_up) = app.event(ev) {
                app.event(follow_up);
            }
        }
        if let Ok(ev) = rx.recv_timeout(common::FRAME_WAIT)
            && let Some(follow_up) = app.event(ev)
        {
            app.event(follow_up);
        }
    }
    (app, rx)
}

/// Draw, ask for what the frame needs, take one answer, draw again — the shape of
/// `run()` around `terminal.draw`, so a state the real loop passes through for one
/// frame can be caught here too.
#[track_caller]
fn pump_home(
    app: &mut App,
    rx: &mpsc::Receiver<AppEvent>,
    area: Rect,
    buf: &mut Buffer,
    done: impl Fn(&App) -> bool,
) {
    for _ in ticks() {
        buf.reset();
        Widget::render(&mut *app, area, buf);
        app.frame_painted();
        app.request_what_the_frame_needs();
        if done(app) {
            return;
        }
        if let Ok(ev) = rx.recv_timeout(std::time::Duration::from_millis(500)) {
            let mut next = Some(ev);
            while let Some(ev) = next {
                next = app.event(ev);
            }
        }
    }
}

/// Feed background results back into the app until it is no longer busy.
#[track_caller]
fn pump_until_idle(app: &mut App, rx: &mpsc::Receiver<AppEvent>, tx: &mpsc::Sender<AppEvent>) {
    pump_until(app, rx, tx, |app| !app.is_busy());
}

/// A 100-row table: `a` 0..100, `c` = a % 3, `name` "alpha_N" for even and "beta_N" for odd `a`.
fn open_query_filter_fixture(
    name: &str,
) -> (App, mpsc::Receiver<AppEvent>, mpsc::Sender<AppEvent>) {
    open_query_filter_fixture_with(name, datui::AppConfig::default())
}

fn open_query_filter_fixture_with(
    name: &str,
    config: datui::AppConfig,
) -> (App, mpsc::Receiver<AppEvent>, mpsc::Sender<AppEvent>) {
    open_query_filter_fixture_at(&common::fixture_dir().join(name), config)
}

/// The same table, written at `csv_path`.
fn open_query_filter_fixture_at(
    csv_path: &Path,
    config: datui::AppConfig,
) -> (App, mpsc::Receiver<AppEvent>, mpsc::Sender<AppEvent>) {
    let mut df = df!(
        "a" => (0..100i64).collect::<Vec<_>>(),
        "c" => (0..100i64).map(|i| i % 3).collect::<Vec<_>>(),
        "name" => (0..100i64)
            .map(|i| if i % 2 == 0 { format!("alpha_{i}") } else { format!("beta_{i}") })
            .collect::<Vec<_>>(),
    )
    .unwrap();
    let mut file = File::create(csv_path).unwrap();
    CsvWriter::new(&mut file).finish(&mut df).unwrap();

    let (tx, rx) = mpsc::channel();
    let theme = datui::Theme::from_config(&config.theme).unwrap();
    let mut app = App::new_with_config(tx.clone(), common::test_runtime(), theme, config);
    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![csv_path.to_path_buf()],
        OpenOptions::default(),
    );
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(app.data_table_state.as_ref().unwrap().num_rows(), 100);
    (app, rx, tx)
}

fn current_rows(app: &App) -> usize {
    let state = app.data_table_state.as_ref().unwrap();
    state.lf().clone().collect().unwrap().height()
}

fn filter_stmt(
    column: &str,
    operator: datui::app::modals::filter_modal::FilterOperator,
    value: &str,
) -> datui::app::modals::filter_modal::FilterStatement {
    datui::app::modals::filter_modal::FilterStatement {
        columns: Vec::new(),
        column: column.to_string(),
        operator,
        value: value.to_string(),
        logical_op: datui::app::modals::filter_modal::LogicalOperator::And,
    }
}

/// Opens an inline CSV with the given options and settles the load.
fn open_csv_with(
    name: &str,
    contents: &str,
    options: OpenOptions,
) -> (App, mpsc::Receiver<AppEvent>, mpsc::Sender<AppEvent>) {
    open_csv_at(&common::fixture_dir().join(name), contents, options)
}

/// The same, written at `csv_path`.
fn open_csv_at(
    csv_path: &Path,
    contents: &str,
    options: OpenOptions,
) -> (App, mpsc::Receiver<AppEvent>, mpsc::Sender<AppEvent>) {
    std::fs::write(csv_path, contents).unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![csv_path.to_path_buf()], options);
    pump_until_idle(&mut app, &rx, &tx);
    assert!(app.data_table_state.is_some());
    (app, rx, tx)
}

/// ISO 8601 timestamps with `Z`, fractional seconds or an offset, as web APIs write them.
/// `mixed` has an offset on one value and none on the other.
const ISO_TIMESTAMPS_CSV: &str = "\
id,z,frac,offset,space,minutes,mixed
1,2013-01-01T10:00:00Z,2026-09-30T13:27:00.220Z,2026-09-30T13:27:00-05:00,2026-09-30 13:27:00+00,2026-09-30T13:27Z,2013-01-01T10:00:00Z
2,2013-01-01T11:00:00Z,2026-09-30T13:28:00.5Z,2026-09-30T13:27:00+00:00,2026-09-30 13:28:00+00,2026-09-30T13:28Z,2013-01-01T11:00:00
";

/// The first row's value of `name`, in microseconds since the epoch.
fn first_micros(app: &App, name: &str) -> i64 {
    let df = app
        .data_table_state
        .as_ref()
        .unwrap()
        .lf()
        .clone()
        .collect()
        .unwrap();
    df.column(name)
        .unwrap()
        .cast(&DataType::Int64)
        .unwrap()
        .get(0)
        .unwrap()
        .extract::<i64>()
        .unwrap()
}

fn assert_iso_timestamps_typed(app: &App) {
    let utc = DataType::Datetime(TimeUnit::Microseconds, Some(TimeZone::UTC));
    let schema = &app.data_table_state.as_ref().unwrap().schema();
    for name in ["z", "frac", "offset", "space", "minutes"] {
        assert_eq!(schema.get(name), Some(&utc), "{name}");
    }
    assert_eq!(schema.get("mixed"), Some(&DataType::String));
    assert_eq!(first_micros(app, "z"), 1_357_034_400_000_000);
    assert_eq!(first_micros(app, "frac"), 1_790_774_820_220_000);
    // -05:00 is five hours behind UTC.
    assert_eq!(first_micros(app, "offset"), 1_790_792_820_000_000);
    assert_eq!(first_micros(app, "minutes"), 1_790_774_820_000_000);
}

/// One key press, with whatever it asks for sent on as the event loop would.
fn press_and_send(app: &mut App, tx: &mpsc::Sender<AppEvent>, code: KeyCode) {
    if let Some(next) = press(app, code) {
        tx.send(next).unwrap();
    }
}

/// One column of the rows on screen, as text.
fn on_screen(app: &App, column: &str) -> Vec<String> {
    let df = app
        .data_table_state
        .as_ref()
        .unwrap()
        .copy_view_df()
        .unwrap();
    let series = df.column(column).unwrap().as_materialized_series().clone();
    series
        .iter()
        .map(|v| match v {
            AnyValue::Null => "null".to_string(),
            v => v.str_value().into_owned(),
        })
        .collect()
}

/// Salaries by department, 40 rows: `dept` cycles eng, ops, sales and a null every
/// fourth row; `salary` climbs by 5,000 from 60,000; `ts` is the hour `i % 24`.
#[cfg(feature = "sql")]
fn open_salary_fixture(name: &str) -> (App, mpsc::Receiver<AppEvent>, mpsc::Sender<AppEvent>) {
    let dir = common::fixture_dir().join(name);
    let n = 40i64;
    let df = df!(
        "id" => (0..n).collect::<Vec<_>>(),
        "dept" => (0..n)
            .map(|i| ["eng", "ops", "sales"].get((i % 4) as usize).copied())
            .collect::<Vec<_>>(),
        "salary" => (0..n).map(|i| 60_000 + i * 5_000).collect::<Vec<_>>(),
        "ts" => (0..n).map(|i| (i % 24) * 3_600_000_000).collect::<Vec<_>>(),
    )
    .unwrap()
    .lazy()
    .with_column(col("ts").cast(DataType::Datetime(TimeUnit::Microseconds, None)))
    .collect()
    .unwrap();
    write_parquet(&dir, "", df);
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![dir.join("data.parquet")],
        OpenOptions::default(),
    );
    pump_until_idle(&mut app, &rx, &tx);
    (app, rx, tx)
}

/// Run `sql` as the SQL prompt would and wait for its rows.
#[cfg(feature = "sql")]
fn run_sql(app: &mut App, rx: &mpsc::Receiver<AppEvent>, tx: &mpsc::Sender<AppEvent>, sql: &str) {
    app.event(AppEvent::Applied(datui::Applied::SqlQuery(sql.to_string())));
    pump_until_idle(app, rx, tx);
    let state = app.data_table_state.as_ref().unwrap();
    assert!(state.error().is_none(), "{sql}: {:?}", state.error());
}

/// Recents grouped by place: two recents in one directory, one in another, so the
/// home screen shows two place rows. Returns the app with the cursor on the first
/// place row, the three recents, and the app's cache.
///
/// `seed_store` records them in the recents store too, for a test that reads it back.
/// The store is this test's own, under `tmp`: the one the process shares is capped at
/// fifty recents, and tests opening files in parallel push these out of it.
fn app_with_recents_in_two_places(
    tmp: &tempfile::TempDir,
    seed_store: bool,
) -> (App, Vec<PathBuf>, datui::CacheManager) {
    common::isolate_cache();
    // As the store keeps them: `/var` is `/private/var` on macOS, and a Windows temp
    // directory is named `RUNNER~1` until canonicalized.
    let root = datui::canonical::canonicalize(tmp.path()).unwrap();
    let here = root.join("here");
    let there = root.join("there");
    std::fs::create_dir_all(&here).unwrap();
    std::fs::create_dir_all(&there).unwrap();
    let recents = vec![
        here.join("a.parquet"),
        here.join("b.parquet"),
        there.join("c.parquet"),
    ];
    for path in &recents {
        std::fs::write(path, b"x").unwrap();
    }
    // Recorded the way an open records them, so what the test forgets is what the
    // store holds. Oldest first: `push_recent` puts each at the front.
    let cache = datui::CacheManager::with_dir(root.join("cache"));
    if seed_store {
        for path in recents.iter().rev() {
            assert_eq!(
                cache.push_recent(path),
                datui::cache::HistoryUpdate::Written
            );
        }
    }

    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.use_cache(cache.clone());
    app.enter_home();
    app.home.rebuild(&recents);
    let row = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Place { path, .. } if *path == here))
        .expect("the directory two recents live in is a place row");
    app.home.selected = row;
    (app, recents, cache)
}

fn ctrl(c: char) -> AppEvent {
    AppEvent::Key(KeyEvent::new(KeyCode::Char(c), KeyModifiers::CONTROL))
}

/// The rendered list, one string per screen row, without the footer.
fn list_rows(buf: &Buffer, area: Rect) -> Vec<String> {
    (0..area.height - 1)
        .map(|y| {
            (0..area.width)
                .map(|x| buf[(x, y)].symbol().to_string())
                .collect::<String>()
        })
        .collect()
}

/// The note on the heading of the directory being browsed, once its listing is in:
/// where entering a lake table says the table itself is not read.
fn lake_heading(app: &mut App) -> String {
    app.home.rebuild(&[]);
    app.home
        .sections
        .first()
        .and_then(|section| section.subtitle.clone())
        .unwrap_or_default()
}

/// Options as the binary builds them from these arguments and this config text.
fn options_as_the_binary_does(argv: &[&str], config: &str) -> OpenOptions {
    use clap::Parser;
    use datui::config::{AppConfig, ConfigLayer};
    let args = datui_cli::Args::try_parse_from(argv).expect("parses");
    // The file, then `-c` over it, as the binary layers them.
    let layers = [
        ConfigLayer::parse(config).expect("config parses"),
        ConfigLayer::from_overrides(&args.config).expect("-c parses"),
    ];
    let config = AppConfig::from_layers(layers).expect("config reads");
    OpenOptions::from_args_and_config(&args, &config)
}

fn open_and_collect(paths: Vec<PathBuf>, options: OpenOptions) -> (App, DataFrame) {
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, paths, options);
    let df = app
        .data_table_state
        .as_ref()
        .expect("the file opened")
        .lf()
        .clone()
        .collect()
        .unwrap();
    (app, df)
}

fn names(df: &DataFrame) -> Vec<String> {
    df.get_column_names()
        .iter()
        .map(|n| n.to_string())
        .collect()
}

/// `body` compressed as a file ending `.{ext}` is.
fn compress(ext: &str, body: &[u8]) -> Vec<u8> {
    use std::io::Write;
    match ext {
        "gz" => {
            let mut enc = flate2::write::GzEncoder::new(Vec::new(), Default::default());
            enc.write_all(body).unwrap();
            enc.finish().unwrap()
        }
        "zst" => zstd::encode_all(body, 0).unwrap(),
        "bz2" => {
            let mut enc = bzip2::write::BzEncoder::new(Vec::new(), Default::default());
            enc.write_all(body).unwrap();
            enc.finish().unwrap()
        }
        "xz" => {
            let mut enc = xz2::write::XzEncoder::new(Vec::new(), 6);
            enc.write_all(body).unwrap();
            enc.finish().unwrap()
        }
        other => panic!("no compression {other}"),
    }
}

/// Run the app's events, from `first`, until it is idle with nothing left to do,
/// agreeing to any download it asks about.
fn settle_from(app: &mut App, rx: &mpsc::Receiver<AppEvent>, first: AppEvent) {
    let mut next = Some(first);
    loop {
        match next.take() {
            Some(ev) => next = app.event(ev),
            // A download is asked about first; Yes has the focus.
            None if app.awaiting_open_confirmation() => next = Some(key(KeyCode::Enter)),
            None => match next_event(app, rx) {
                Some(ev) => next = Some(ev),
                None => return,
            },
        }
    }
}

fn column_names(app: &App) -> Vec<String> {
    app.data_table_state
        .as_ref()
        .expect("a dataset")
        .schema()
        .iter_names()
        .map(|n| n.to_string())
        .collect()
}

/// `H` on the Info panel's Schema tab: the first row the other way. The panel opens
/// on Notes while there are unread ones, so this walks to Schema first.
fn header_from_schema_tab(app: &mut App, rx: &mpsc::Receiver<AppEvent>) {
    use datui::widgets::info::InfoTab;
    assert!(app.event(key(KeyCode::Char('i'))).is_none());
    assert_eq!(app.overlay, Overlay::Info);
    for _ in 0..16 {
        if app.info_modal.active_tab == InfoTab::Schema {
            break;
        }
        app.event(key(KeyCode::Right));
    }
    assert_eq!(app.info_modal.active_tab, InfoTab::Schema);
    settle_from(app, rx, key(KeyCode::Char('H')));
}

/// Serve `body` at `http://127.0.0.1:<port>/<name>` to every request. Returns the URL
/// and a count of the GETs, which is how many downloads there were.
#[cfg(feature = "http")]
fn serve_over_http(
    name: &str,
    body: Vec<u8>,
) -> (String, std::sync::Arc<std::sync::atomic::AtomicUsize>) {
    serve_over_http_stalling(name, body, None)
}

/// [`serve_over_http`], going quiet for `stall.1` after the first `stall.0` bytes of
/// each body.
#[cfg(feature = "http")]
fn serve_over_http_stalling(
    name: &str,
    body: Vec<u8>,
    stall: Option<(usize, std::time::Duration)>,
) -> (String, std::sync::Arc<std::sync::atomic::AtomicUsize>) {
    use std::io::{Read, Write};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}/{name}", listener.local_addr().unwrap());
    let fetched = Arc::new(AtomicUsize::new(0));
    let counter = fetched.clone();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let mut head = Vec::new();
            let mut byte = [0u8; 1];
            while !head.ends_with(b"\r\n\r\n") && stream.read(&mut byte).unwrap_or(0) == 1 {
                head.push(byte[0]);
            }
            let get = head.starts_with(b"GET");
            if get {
                counter.fetch_add(1, Ordering::SeqCst);
            }
            let _ = write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\n\
                 Content-Length: {}\r\nConnection: close\r\n\r\n",
                body.len(),
            );
            if !get {
                continue;
            }
            let (first, rest) = body.split_at(stall.map_or(0, |(at, _)| at.min(body.len())));
            let _ = stream.write_all(first).and_then(|()| stream.flush());
            if let Some((_, quiet)) = stall {
                // A server that goes quiet mid-body: the stall is the subject.
                std::thread::sleep(quiet);
            }
            let _ = stream.write_all(rest);
        }
    });
    (url, fetched)
}

/// Serve `body` at `http://127.0.0.1:<port>/<name>` without saying how long it is, to
/// HEAD or GET: the body runs until the connection closes. Returns the URL and a count
/// of the GETs.
#[cfg(feature = "http")]
fn serve_over_http_unsized(
    name: &str,
    body: Vec<u8>,
) -> (String, std::sync::Arc<std::sync::atomic::AtomicUsize>) {
    use std::io::{Read, Write};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}/{name}", listener.local_addr().unwrap());
    let fetched = Arc::new(AtomicUsize::new(0));
    let counter = fetched.clone();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let mut head = Vec::new();
            let mut byte = [0u8; 1];
            while !head.ends_with(b"\r\n\r\n") && stream.read(&mut byte).unwrap_or(0) == 1 {
                head.push(byte[0]);
            }
            let get = head.starts_with(b"GET");
            let _ = write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: text/csv\r\nConnection: close\r\n\r\n"
            );
            if get {
                counter.fetch_add(1, Ordering::SeqCst);
                let _ = stream.write_all(&body);
            }
        }
    });
    (url, fetched)
}

// ---------------------------------------------------------------------------
// Help on the home screen
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Views: V falls back to the list, and the modal never outlives the dataset
// ---------------------------------------------------------------------------

/// A helper for the modal tests: one key press with no modifiers.
fn press(app: &mut App, code: KeyCode) -> Option<AppEvent> {
    app.event(AppEvent::Key(KeyEvent::new(code, KeyModifiers::NONE)))
}

/// Step the open copy dialog's scope row (where it opens) with → until it reads
/// `scope`. The scope is sticky, so a test never assumes where it starts.
fn copy_scope(app: &mut App, scope: datui::app::modals::copy_modal::CopyScope) {
    for _ in 0..datui::app::modals::copy_modal::CopyScope::ALL.len() {
        if app.copy_modal.scope == scope {
            return;
        }
        press(app, KeyCode::Right);
    }
    assert_eq!(app.copy_modal.scope, scope);
}

/// Open Sort & Filter and start a new filter: ↑ to the tab bar, ↑ again wraps to
/// the last row, "add filter", and Space opens its editor.
fn start_new_filter(app: &mut App) {
    press(app, KeyCode::Char('s'));
    press(app, KeyCode::Up);
    press(app, KeyCode::Up);
    assert_eq!(
        app.sort_filter_modal.focus,
        datui::app::modals::sort_filter_modal::SortFilterField::AddFilter
    );
    press(app, KeyCode::Char(' '));
    assert!(app.sort_filter_modal.filter.editor.is_some());
}

/// Open Sort & Filter and walk to the Columns tab's list: up to the tab bar, →
/// to Columns, ↓ to find, ↓ to the list, on the table's column cursor.
fn open_columns_list(app: &mut App) {
    press(app, KeyCode::Char('s'));
    press(app, KeyCode::Up);
    assert_eq!(
        app.sort_filter_modal.focus,
        datui::app::modals::sort_filter_modal::SortFilterField::TabBar
    );
    press(app, KeyCode::Right);
    press(app, KeyCode::Down);
    press(app, KeyCode::Down);
    assert!(matches!(
        app.sort_filter_modal.focus,
        datui::app::modals::sort_filter_modal::SortFilterField::Column(_)
    ));
}

/// Step the copy dialog's format row, from the scope row, until it reads `format`.
fn copy_format(app: &mut App, format: datui::clipboard::CopyFormat) {
    press(app, KeyCode::Down);
    for _ in 0..datui::clipboard::CopyFormat::ALL.len() {
        if app.copy_modal.format == format {
            return;
        }
        press(app, KeyCode::Right);
    }
    assert_eq!(app.copy_modal.format, format);
}

fn q_style_config() -> datui::AppConfig {
    let mut config = datui::AppConfig::default();
    config.query.default_mode = QueryMode::Q;
    config
}

fn press_key(app: &mut App, code: KeyCode, modifiers: KeyModifiers) {
    let mut next = app.event(AppEvent::Key(KeyEvent::new(code, modifiers)));
    while let Some(ev) = next.take() {
        next = app.event(ev);
    }
}

fn run_and_settle(
    app: &mut App,
    event: AppEvent,
    rx: &mpsc::Receiver<AppEvent>,
    tx: &mpsc::Sender<AppEvent>,
) {
    let mut next = app.event(event);
    while let Some(ev) = next.take() {
        next = app.event(ev);
    }
    pump_until_idle(app, rx, tx);
}

fn type_text(app: &mut App, text: &str) {
    for c in text.chars() {
        press_key(app, KeyCode::Char(c), KeyModifiers::NONE);
    }
}

fn screen_at(app: &mut App, width: u16, height: u16) -> String {
    let area = Rect::new(0, 0, width, height);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    (0..height)
        .map(|y| {
            (0..width)
                .map(|x| buf[(x, y)].symbol().to_string())
                .collect::<String>()
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Press `c` with Ctrl held, as a text field receives it.
fn press_ctrl(app: &mut App, c: char) -> Option<AppEvent> {
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Char(c),
        KeyModifiers::CONTROL,
    )))
}

fn screen_text(app: &mut App) -> String {
    let area = Rect::new(0, 0, 120, 30);
    let mut buffer = Buffer::empty(area);
    app.render(area, &mut buffer);
    common::buffer_text(&buffer)
}

/// `id,key,val` in long form: ten ids, each with a `k1` and a `k2` row, values scaled by
/// `scale` so two files with the same columns give different results.
#[cfg(feature = "sql")]
fn long_csv(scale: i64) -> String {
    let mut csv = String::from("id,key,val\n");
    for id in 0..10 {
        csv.push_str(&format!(
            "{id},k1,{}\n{id},k2,{}\n",
            id * scale,
            id * 10 * scale
        ));
    }
    csv
}

/// Run `steps` on one file and save a view matching a second; then apply the view to
/// the second file and, in another app, run the same steps on it by hand. Returns the
/// view, what applying it showed, and what the steps showed.
#[cfg(feature = "sql")]
fn view_and_steps_on_the_next_file(
    name: &str,
    steps: &dyn Fn() -> Vec<AppEvent>,
) -> (datui::SavedView, DataFrame, DataFrame) {
    let next_path = common::fixture_dir().join(format!("{name}_next.csv"));
    let run = |app: &mut App, rx: &mpsc::Receiver<AppEvent>, tx: &mpsc::Sender<AppEvent>| {
        for step in steps() {
            app.event(step);
            pump_until_idle(app, rx, tx);
            let state = app.data_table_state.as_ref().unwrap();
            assert!(state.error().is_none(), "{:?}", state.error());
        }
    };
    let shown = |app: &App| {
        let state = app.data_table_state.as_ref().unwrap();
        state.visible_lf().collect().unwrap()
    };

    let (mut by_hand, rx, tx) = open_csv_at(&next_path, &long_csv(3), OpenOptions::default());
    run(&mut by_hand, &rx, &tx);
    let expected = shown(&by_hand);

    let (mut app, rx, tx) = open_csv_with(
        &format!("{name}_first.csv"),
        &long_csv(1),
        OpenOptions::default(),
    );
    run(&mut app, &rx, &tx);
    let view = app
        .create_view_from_current_state(
            name.to_string(),
            None,
            datui::view::MatchCriteria {
                exact_path: Some(next_path.clone()),
                relative_path: None,
                path_pattern: None,
                filename_pattern: None,
                schema_columns: None,
                schema_types: None,
                table: None,
            },
        )
        .unwrap();
    pump_open_until_loaded(&mut app, &rx, vec![next_path], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);
    app.event(key(KeyCode::Char('V')));
    pump_until_idle(&mut app, &rx, &tx);
    let state = app.data_table_state.as_ref().unwrap();
    assert!(state.error().is_none(), "{:?}", state.error());
    (view, shown(&app), expected)
}

/// What a test clipboard was given, copy by copy.
type Copies = std::sync::Arc<std::sync::Mutex<Vec<String>>>;

/// A clipboard for the inspector tests: keeps the text of every copy.
struct KeptCopies(Copies);

impl datui::clipboard::Destination for KeptCopies {
    fn write(&mut self, payload: datui::clipboard::Payload) -> Result<(), String> {
        self.0.lock().unwrap().push(payload.text);
        Ok(())
    }
    fn describe(&self) -> &'static str {
        "test"
    }
}

/// A Parquet file of awkward values, opened and drawn at 100×30: text with a
/// line break, a tab, a literal backslash, edge spaces, an empty string and a
/// null; floats Polars' display rounds; a list and bytes.
fn open_inspector_fixture(
    dir: &Path,
) -> (
    App,
    mpsc::Receiver<AppEvent>,
    mpsc::Sender<AppEvent>,
    Copies,
) {
    let path = dir.join("inspect.parquet");
    let tags: Vec<Series> = (0..6)
        .map(|i| Series::new("".into(), vec![format!("t{i}"), "x".to_string()]))
        .collect();
    let mut df = df!(
        "id" => [1i64, 2, 3, 4, 5, 6],
        "description" => [
            Some("line1\nline2"),
            Some("tab\tseparated"),
            Some(r"literal \n backslash"),
            Some("  padded  "),
            Some(""),
            None,
        ],
        "amount" => [1000000.125f64, -0.0, f64::NAN, f64::INFINITY, 0.1 + 0.2, 2.5],
        "tags" => tags,
        "blob" => [b"Hi\x00".as_slice(), b"a", b"b", b"c", b"d", b"e"],
    )
    .unwrap();
    ParquetWriter::new(File::create(&path).unwrap())
        .finish(&mut df)
        .unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path], OpenOptions::default());
    draw_inspector(&mut app);
    let copies = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    app.set_clipboard_destination(Box::new(KeptCopies(copies.clone())));
    (app, rx, tx, copies)
}

fn draw_inspector(app: &mut App) -> String {
    let area = Rect::new(0, 0, 100, 30);
    let mut buffer = Buffer::empty(area);
    app.render(area, &mut buffer);
    common::buffer_text(&buffer)
}

fn inspected_field(app: &App) -> String {
    app.inspector_modal.focused().unwrap().name.clone()
}

/// Every row of `app` drawn at `width`×`height`, one string per terminal row.
fn rows_at(app: &mut App, width: u16, height: u16) -> Vec<String> {
    let area = Rect::new(0, 0, width, height);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    (0..height)
        .map(|y| (0..width).map(|x| buf[(x, y)].symbol()).collect())
        .collect()
}

/// Fourteen fields of one order, as the #548 review's table has them: an empty
/// email, a null, a long URL, a list, a binary column and an integer first.
fn open_orders_fixture(dir: &Path) -> (App, mpsc::Receiver<AppEvent>, mpsc::Sender<AppEvent>) {
    let path = dir.join("orders.parquet");
    let url = format!(
        "https://shop.example.com/orders/{}?id=1",
        "segment/".repeat(20)
    );
    let tags: Vec<Series> = (0..3)
        .map(|i| Series::new("".into(), vec![format!("t{i}")]))
        .collect();
    let mut df = df!(
        "id" => [1i64, 2, 3],
        "customer_name" => [None, Some("Customer 1"), Some("Customer 2")],
        "email" => ["", "user1@example.com", "user2@example.com"],
        "notes" => ["one\ntwo", "x", "y"],
        "url" => [url.as_str(), "u", "v"],
        "payload_json" => [r#"{"a": 1}"#, "{}", "[]"],
        "tags" => tags,
        "amount" => [1.5f64, 2.5, 3.5],
        "created" => ["2024-01-01", "2024-01-02", "2024-01-03"],
        "updated_at" => ["t1", "t2", "t3"],
        "elapsed" => [1i64, 2, 3],
        "point" => [1i64, 2, 3],
        "blob" => [b"".as_slice(), b"ab", b"cd"],
        "region" => ["north", "south", "east"],
    )
    .unwrap();
    ParquetWriter::new(File::create(&path).unwrap())
        .finish(&mut df)
        .unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);
    (app, rx, tx)
}

/// A capped clipboard, as the terminal's is: keeps the text of every copy.
struct CappedCopies(Copies, usize);

impl datui::clipboard::Destination for CappedCopies {
    fn write(&mut self, payload: datui::clipboard::Payload) -> Result<(), String> {
        self.0.lock().unwrap().push(payload.text);
        Ok(())
    }
    fn describe(&self) -> &'static str {
        "terminal"
    }
    fn accepts(&self) -> datui::clipboard::Accepts {
        datui::clipboard::Accepts {
            html: false,
            base64_limit: Some(self.1),
        }
    }
}

/// A Parquet file whose dates and datetimes reach the ends of what they can
/// store, as files hold sentinels like `i64::MIN + 1` microseconds: row 1 the
/// least, row 2 the epoch, row 3 the greatest. Opened and loaded.
fn open_out_of_range_dates(dir: &Path) -> (App, mpsc::Receiver<AppEvent>, mpsc::Sender<AppEvent>) {
    let path = dir.join("oor.parquet");
    let edges = [i64::MIN + 1, 0, i64::MAX];
    let paris = TimeZone::opt_try_new(Some("Europe/Paris")).unwrap();
    let column = |name: &str, dtype: DataType| {
        Series::new(name.into(), edges)
            .cast(&dtype)
            .unwrap()
            .into_column()
    };
    let mut df = DataFrame::new(
        3,
        vec![
            Column::new("id".into(), [1i64, 2, 3]),
            Series::new("d".into(), [i32::MIN, 0, i32::MAX])
                .cast(&DataType::Date)
                .unwrap()
                .into_column(),
            column("t_ms", DataType::Datetime(TimeUnit::Milliseconds, None)),
            column("t_us", DataType::Datetime(TimeUnit::Microseconds, None)),
            column("t_ns", DataType::Datetime(TimeUnit::Nanoseconds, None)),
            column("t_tz", DataType::Datetime(TimeUnit::Microseconds, paris)),
            column("dur", DataType::Duration(TimeUnit::Microseconds)),
        ],
    )
    .unwrap();
    ParquetWriter::new(File::create(&path).unwrap())
        .finish(&mut df)
        .unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);
    (app, rx, tx)
}

/// Press `code` and handle every event it chains to.
fn press_through(app: &mut App, code: KeyCode) {
    let mut next = app.event(key(code));
    while let Some(event) = next {
        next = app.event(event);
    }
}

/// The screen at 300×30, wide enough for every column; fails on an error dialog.
#[track_caller]
fn draw_wide(app: &mut App, what: &str) -> String {
    let area = Rect::new(0, 0, 300, 30);
    let mut buffer = Buffer::empty(area);
    app.render(area, &mut buffer);
    let screen = common::buffer_text(&buffer);
    assert_eq!(app.error_message(), None, "{what}:\n{screen}");
    screen
}

/// A table `n` columns wide and 40 rows long, in a rotation of kinds so widths
/// differ: an integer `id_NNN`, a float `price_NNN`, text `label_NNN` (one long
/// value on row 7) and a short text `code_NNN`. Opened, then drawn at `size`.
fn open_wide_table(
    name: &str,
    n: usize,
    size: (u16, u16),
) -> (App, mpsc::Receiver<AppEvent>, mpsc::Sender<AppEvent>) {
    let rows = 40usize;
    let columns: Vec<Column> = (0..n)
        .map(|i| -> Column {
            match i % 4 {
                0 => Series::new(
                    format!("id_{i:03}").into(),
                    (0..rows as i64)
                        .map(|r| r * 1000 + i as i64)
                        .collect::<Vec<_>>(),
                )
                .into(),
                1 => Series::new(
                    format!("price_{i:03}").into(),
                    (0..rows).map(|r| r as f64 * 1.25).collect::<Vec<_>>(),
                )
                .into(),
                2 => Series::new(
                    format!("label_{i:03}").into(),
                    (0..rows)
                        .map(|r| {
                            if r == 7 {
                                "a much longer label than the rest".to_string()
                            } else {
                                format!("item {r}")
                            }
                        })
                        .collect::<Vec<_>>(),
                )
                .into(),
                _ => Series::new(
                    format!("code_{i:03}").into(),
                    (0..rows).map(|r| format!("c{r}")).collect::<Vec<_>>(),
                )
                .into(),
            }
        })
        .collect();
    let mut df = DataFrame::new_infer_height(columns).unwrap();
    let path = common::fixture_dir().join(name);
    ParquetWriter::new(File::create(&path).unwrap())
        .finish(&mut df)
        .unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);
    draw_sized(&mut app, size);
    (app, rx, tx)
}

/// Draw the app at `size`, as the run loop does after every key, and return the
/// screen one line per row.
fn draw_sized(app: &mut App, (width, height): (u16, u16)) -> String {
    app.event(AppEvent::Resize(width, height));
    let area = Rect::new(0, 0, width, height);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    (0..height)
        .map(|y| {
            (0..width)
                .map(|x| buf[(x, y)].symbol().to_string())
                .collect::<String>()
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Press a key, then draw, as the run loop does.
fn press_and_draw(app: &mut App, code: KeyCode, size: (u16, u16)) -> String {
    press_key(app, code, KeyModifiers::NONE);
    draw_sized(app, size)
}

/// A page of columns, Shift+← or Shift+→.
fn page_and_draw(app: &mut App, code: KeyCode, size: (u16, u16)) -> String {
    press_key(app, code, KeyModifiers::SHIFT);
    draw_sized(app, size)
}

fn type_and_draw(app: &mut App, text: &str, size: (u16, u16)) -> String {
    for c in text.chars() {
        press_key(app, KeyCode::Char(c), KeyModifiers::NONE);
    }
    draw_sized(app, size)
}

fn columns_shown(app: &App) -> Option<datui::widgets::column_paging::OnScreen> {
    app.data_table_state.as_ref().unwrap().columns_on_screen()
}

/// The scrolling columns on screen, first and last, counted from 1 with frozen ones.
fn range_shown(app: &App) -> Option<(usize, usize)> {
    columns_shown(app).map(|on| (on.first, on.last))
}

/// The column cursor's place, counted from 1, as the bar says it.
fn cursor_at(app: &App) -> usize {
    app.data_table_state
        .as_ref()
        .unwrap()
        .current_column_index()
        .unwrap()
        + 1
}

/// The header row: the line that names the columns.
fn header_line(screen: &str) -> &str {
    screen.lines().next().unwrap_or("")
}

/// Each column's name, its first value's text (1970-01-01, Polars' own) and its
/// second's, a date past the calendar written as its stored number.
const PAST_CALENDAR: [(&str, &str, &str); 5] = [
    ("d", "1970-01-01", "2147483647 days since 1970-01-01"),
    (
        "t_ms",
        "1970-01-01 00:00:00.000",
        "-9223372036854775807 ms since 1970-01-01 UTC",
    ),
    (
        "t_us",
        "1970-01-01 00:00:00.000000",
        "-9223372036854775807 us since 1970-01-01 UTC",
    ),
    (
        "t_ms_tz",
        "1970-01-01 01:00:00.000+01:00",
        "-9223372036854775807 ms since 1970-01-01 UTC",
    ),
    (
        "t_us_tz",
        "1970-01-01 01:00:00.000000+01:00",
        "-9223372036854775807 us since 1970-01-01 UTC",
    ),
];

/// A Parquet file of [`PAST_CALENDAR`]'s columns, with text `s` beside them, opened.
fn open_past_calendar(dir: &Path) -> (App, mpsc::Receiver<AppEvent>, mpsc::Sender<AppEvent>) {
    let path = dir.join("past.parquet");
    let paris = TimeZone::opt_try_new(Some("Europe/Paris")).unwrap();
    let datetime = |name: &str, unit, zone: Option<TimeZone>| {
        Series::new(name.into(), [0, i64::MIN + 1])
            .cast(&DataType::Datetime(unit, zone))
            .unwrap()
            .into_column()
    };
    let mut df = DataFrame::new(
        2,
        vec![
            Column::new("s".into(), ["a", "b"]),
            Series::new("d".into(), [0, i32::MAX])
                .cast(&DataType::Date)
                .unwrap()
                .into_column(),
            datetime("t_ms", TimeUnit::Milliseconds, None),
            datetime("t_us", TimeUnit::Microseconds, None),
            datetime("t_ms_tz", TimeUnit::Milliseconds, paris.clone()),
            datetime("t_us_tz", TimeUnit::Microseconds, paris),
        ],
    )
    .unwrap();
    ParquetWriter::new(File::create(&path).unwrap())
        .finish(&mut df)
        .unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);
    (app, rx, tx)
}

/// The view's column `name`, each value as the table shows it.
fn view_text(app: &App, name: &str) -> Vec<Option<String>> {
    let df = app
        .data_table_state
        .as_ref()
        .unwrap()
        .lf()
        .clone()
        .collect()
        .unwrap();
    let column = df.column(name).unwrap();
    (0..column.len())
        .map(|i| {
            let value = column.get(i).unwrap();
            (!value.is_null()).then(|| datui::exact::str_value(&value).into_owned())
        })
        .collect()
}

/// Run `query` from the query prompt and wait for its rows; fails on an error.
#[track_caller]
fn run_query(
    app: &mut App,
    rx: &mpsc::Receiver<AppEvent>,
    tx: &mpsc::Sender<AppEvent>,
    query: &str,
) {
    app.event(AppEvent::Applied(datui::Applied::QQuery(query.to_string())));
    pump_until_idle(app, rx, tx);
    assert_eq!(app.error_message(), None, "{query}");
    let state = app.data_table_state.as_ref().unwrap();
    assert!(state.error().is_none(), "{query}: {:?}", state.error());
}

/// Each group of a grouping keyed on the text of a [`PAST_CALENDAR`] column drills
/// into the one row holding it: `b` for the date past the calendar, else `a`.
#[track_caller]
fn assert_drills_to_its_row(app: &mut App, keys: &[Option<String>], past: &str, case: &str) {
    for (group, key) in keys.iter().enumerate() {
        let state = app.data_table_state.as_mut().unwrap();
        state.drill_down_into_group(group).unwrap();
        let row = if key.as_deref() == Some(past) {
            "b"
        } else {
            "a"
        };
        assert_eq!(
            view_text(app, "s"),
            [Some(row.to_string())],
            "{case}: {key:?}"
        );
        app.data_table_state.as_mut().unwrap().drill_up().unwrap();
    }
}

/// A Parquet file of nanosecond datetimes, naive (`n`) and in Paris (`z`): 1970,
/// then the ends of the nanosecond range, `i64::MAX` (2262-04-11) and
/// `i64::MIN + 1` (1677-09-21), opened.
fn open_ns_edges(dir: &Path) -> (App, mpsc::Receiver<AppEvent>, mpsc::Sender<AppEvent>) {
    let path = dir.join("ns_edges.parquet");
    let paris = TimeZone::opt_try_new(Some("Europe/Paris")).unwrap();
    let stamps = |name: &str, zone: Option<TimeZone>| {
        Series::new(name.into(), [0, i64::MAX, i64::MIN + 1])
            .cast(&DataType::Datetime(TimeUnit::Nanoseconds, zone))
            .unwrap()
            .into_column()
    };
    let mut df = DataFrame::new(3, vec![stamps("n", None), stamps("z", paris)]).unwrap();
    ParquetWriter::new(File::create(&path).unwrap())
        .finish(&mut df)
        .unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);
    (app, rx, tx)
}

// ---------------------------------------------------------------------------
// Value Counts (`F`)
// ---------------------------------------------------------------------------

/// `pay` (two nulls), `amount`, `id`: 10 rows.
const COUNTS_CSV: &str = "pay,amount,id\n\
card,10,1\ncash,5,2\ncard,10,3\n,7,4\ncard,2,5\ncash,10,6\n,1,7\ncard,3,8\ncheck,10,9\ncard,2,10\n";

fn count_values_done(app: &App) -> bool {
    app.value_counts.computing.is_none() && !app.is_busy()
}

/// Press `code` on Value Counts and wait for any count it starts.
fn counts_key(
    app: &mut App,
    rx: &mpsc::Receiver<AppEvent>,
    tx: &mpsc::Sender<AppEvent>,
    code: KeyCode,
) {
    press_and_send(app, tx, code);
    pump_until(app, rx, tx, count_values_done);
}

/// Each listed line as `(label, rows)`.
fn counted_lines(app: &App) -> Vec<(String, u64)> {
    use datui::analysis::value_counts::LineKind;
    let modal = &app.value_counts;
    let counts = modal.current().expect("counts on screen");
    counts
        .lines(modal.order)
        .iter()
        .map(|line| {
            let label = match line.kind {
                LineKind::Value(at) => counts.value(at).unwrap().str_value().into_owned(),
                LineKind::Null => "null".to_string(),
                LineKind::Other(n) => format!("other {n}"),
            };
            (label, line.rows)
        })
        .collect()
}

fn counts_screen(app: &mut App, width: u16, height: u16) -> String {
    let area = Rect::new(0, 0, width, height);
    let mut buffer = Buffer::empty(area);
    app.render(area, &mut buffer);
    (0..height)
        .map(|y| {
            (0..width)
                .map(|x| buffer[(x, y)].symbol().to_string())
                .collect::<String>()
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Six rows of every kind of value `+` and `-` filter on, and a list they do not.
fn open_quick_filter_table(name: &str) -> (App, mpsc::Receiver<AppEvent>, mpsc::Sender<AppEvent>) {
    let mut df = df!(
        "name" => &[Some("north"), Some("south"), None, Some("north"), Some("east"), Some("south")],
        "n" => &[1i64, 2, 3, 2, 2, 1],
        // 0.1 + 0.2 and 0.3 are both drawn 0.3, and are not equal.
        "x" => &[Some(0.1 + 0.2), Some(0.3), Some(1.0 / 3.0), Some(1.0 / 3.0), None, Some(2.5)],
        "day" => &[Some(19723i32), Some(19724), Some(19723), None, Some(19725), Some(19723)],
    )
    .unwrap()
    .lazy()
    .with_column(col("day").cast(DataType::Date))
    .collect()
    .unwrap();
    let tags: Vec<Series> = (0..6i64).map(|i| Series::new("".into(), &[i, i])).collect();
    df.with_column(Series::new("tags".into(), tags).into_column())
        .unwrap();
    let path = common::fixture_dir().join(name);
    ParquetWriter::new(File::create(&path).unwrap())
        .finish(&mut df)
        .unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);
    draw_sized(&mut app, (100, 24));
    (app, rx, tx)
}

/// A key at the table, and everything it sets off, to the frame after.
fn table_key(app: &mut App, rx: &mpsc::Receiver<AppEvent>, tx: &mpsc::Sender<AppEvent>, c: char) {
    run_and_settle(app, key(KeyCode::Char(c)), rx, tx);
    draw_sized(app, (100, 24));
}

fn strings(names: &[&str]) -> Vec<String> {
    names.iter().map(|n| n.to_string()).collect()
}

/// The rows the view holds, and its filters as `column op value`.
fn quick_view(app: &App) -> (usize, Vec<String>) {
    let state = app.data_table_state.as_ref().unwrap();
    let rows = state.lf().clone().collect().unwrap().height();
    let filters = state
        .view_filters()
        .iter()
        .map(|f| {
            format!("{} {} {}", f.column, f.operator.as_str(), f.value)
                .trim_end()
                .to_string()
        })
        .collect();
    (rows, filters)
}

fn key_event(c: char) -> AppEvent {
    key(KeyCode::Char(c))
}

/// The theme's style for the column cursor's header and the current cell.
fn cell_cursor_style() -> ratatui::style::Style {
    datui::config::Theme::from_config(&datui::config::ThemeConfig::default())
        .unwrap()
        .cell_cursor_style()
}

/// Whether a drawn cell carries `style`: its background and its modifiers.
fn carries(cell: &ratatui::buffer::Cell, style: ratatui::style::Style) -> bool {
    style.bg.is_none_or(|bg| cell.bg == bg) && cell.modifier.contains(style.add_modifier)
}

/// The header text drawn in the column cursor's style, trimmed.
fn header_in_cursor_style(buffer: &Buffer, width: u16) -> String {
    let style = cell_cursor_style();
    (0..width)
        .map(|x| &buffer[(x, 0)])
        .filter(|cell| carries(cell, style))
        .map(|cell| cell.symbol().to_string())
        .collect::<String>()
        .trim()
        .to_string()
}

/// Open `paths` with temp files written to `scratch`, and wait until the table is up.
fn open_with_scratch(
    paths: Vec<PathBuf>,
    scratch: &Path,
) -> (App, mpsc::Receiver<AppEvent>, mpsc::Sender<AppEvent>) {
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    let options = OpenOptions {
        temp_dir: Some(scratch.to_path_buf()),
        ..OpenOptions::default()
    };
    pump_open_until_loaded(&mut app, &rx, paths, options);
    pump_until_idle(&mut app, &rx, &tx);
    (app, rx, tx)
}

fn files_in(dir: &Path) -> usize {
    std::fs::read_dir(dir).unwrap().count()
}

/// The table a CSV opens as, collected, with `options`.
fn open_dialect(paths: Vec<PathBuf>, options: OpenOptions) -> DataFrame {
    common::ensure_sample_data();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    settle_from(&mut app, &rx, AppEvent::Open(paths, options));
    if let Some(message) = app.error_message() {
        panic!("the open failed: {message}");
    }
    app.data_table_state
        .as_ref()
        .expect("a dataset")
        .lf()
        .clone()
        .collect()
        .unwrap()
}

fn dialect_fixture(name: &str) -> PathBuf {
    PathBuf::from("tests/sample-data").join(name)
}

/// The options the binary builds from `flags`, as for `datui FLAGS file.csv`.
fn options_from_flags(flags: &[&str]) -> OpenOptions {
    use clap::Parser;
    let args = datui::Args::parse_from(std::iter::once("datui").chain(flags.iter().copied()));
    OpenOptions::from_args_and_config(&args, &datui::config::AppConfig::default())
}

fn names_of(df: &DataFrame) -> Vec<String> {
    df.get_column_names()
        .iter()
        .map(|n| n.to_string())
        .collect()
}

/// The sales table the Copy as Python tests build views over.
fn open_python_fixture() -> (
    App,
    mpsc::Receiver<AppEvent>,
    mpsc::Sender<AppEvent>,
    tempfile::TempDir,
) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("sales.csv");
    let mut csv = String::from("order_id,region,customer,amount,qty,day\n");
    let regions = ["north", "south", "east", "west"];
    let customers = ["Ada", "Bo", "Cy", "Di", "Ed"];
    for i in 0..60 {
        let amount = if i % 11 == 0 {
            String::new()
        } else {
            format!("{:.2}", (i * 37 % 97) as f64 * 1.25)
        };
        csv.push_str(&format!(
            "{i},{},{},{amount},{},2024-0{}-{:02}\n",
            regions[i % 4],
            customers[i % 5],
            i % 7,
            1 + i % 3,
            1 + i % 28
        ));
    }
    std::fs::write(&path, csv).unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);
    (app, rx, tx, dir)
}

fn python_filter(
    column: &str,
    operator: datui::app::modals::filter_modal::FilterOperator,
    value: &str,
    logical_op: datui::app::modals::filter_modal::LogicalOperator,
) -> datui::app::modals::filter_modal::FilterStatement {
    datui::app::modals::filter_modal::FilterStatement {
        columns: Vec::new(),
        column: column.to_string(),
        operator,
        value: value.to_string(),
        logical_op,
    }
}

/// The view as datui shows it, every row, as CSV.
fn view_csv(app: &App) -> String {
    let state = app.data_table_state.as_ref().unwrap();
    let columns: Vec<Expr> = state.get_column_order().iter().map(col).collect();
    let mut df = state.lf().clone().select(columns).collect().unwrap();
    let mut out = Vec::new();
    CsvWriter::new(&mut out).finish(&mut df).unwrap();
    String::from_utf8(out).unwrap()
}

/// Run the script datui writes for the view with the project's Python Polars and
/// compare its rows with datui's. `None` when there is no `.venv` to run it with.
fn run_python_script(app: &App) -> Option<(String, String)> {
    let python = if cfg!(windows) {
        Path::new(".venv/Scripts/python.exe")
    } else {
        Path::new(".venv/bin/python")
    };
    if !python.exists() {
        return None;
    }
    let state = app.data_table_state.as_ref().unwrap();
    let script = app.python_script(state);
    // Bytes, not text: Windows' text stdout writes `\r\n` and encodes in its code page.
    let program = format!(
        "{script}\nimport sys\nsys.stdout.buffer.write(df.collect().write_csv().encode())\n"
    );
    let output = std::process::Command::new(python)
        .arg("-c")
        .arg(&program)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "the script failed:\n{program}\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    Some((String::from_utf8(output.stdout).unwrap(), script))
}

/// Write a one-column frame holding `value` in the format the extension names.
fn write_marker(path: &Path, value: &str) {
    let mut df = df!("v" => [value]).unwrap();
    let file = File::create(path).unwrap();
    match path.extension().and_then(|e| e.to_str()).unwrap() {
        "csv" => CsvWriter::new(file).finish(&mut df).unwrap(),
        "tsv" => CsvWriter::new(file)
            .with_separator(b'\t')
            .finish(&mut df)
            .unwrap(),
        "parquet" => {
            ParquetWriter::new(file).finish(&mut df).unwrap();
        }
        "arrow" | "feather" | "ipc" => IpcWriter::new(file).finish(&mut df).unwrap(),
        "jsonl" | "ndjson" => JsonWriter::new(file)
            .with_json_format(JsonFormat::JsonLines)
            .finish(&mut df)
            .unwrap(),
        other => panic!("no writer for {other}"),
    }
}

fn marker_values(df: &DataFrame) -> Vec<String> {
    let mut values: Vec<String> = df
        .column("v")
        .unwrap()
        .str()
        .unwrap()
        .iter()
        .map(|v| v.unwrap_or("").to_string())
        .collect();
    values.sort();
    values
}

/// Names that read as globs where the system allows them in a file name.
fn glob_character_stems() -> Vec<(&'static str, &'static str)> {
    // (the literal name, a sibling its pattern also matches)
    let mut stems = vec![("d[1]", "d1"), ("e[ab]", "ea")];
    if cfg!(unix) {
        stems.extend([("a*b", "aZZb"), ("x?", "xy")]);
    }
    stems
}

/// The view's frame, collected.
fn view_frame(app: &App) -> DataFrame {
    app.data_table_state
        .as_ref()
        .unwrap()
        .lf()
        .clone()
        .collect()
        .unwrap()
}

/// Type `text` into whatever has the keys, as typed.
fn type_into(app: &mut App, text: &str) {
    for c in text.chars() {
        app.event(key(KeyCode::Char(c)));
    }
}

/// The Info panel's Schema tab, its cursor on `column`, and Enter: the type picker.
fn retype_from_schema(app: &mut App, column: &str) {
    // Back from a type, the picker leaves the panel open.
    if app.overlay != datui::Overlay::Info {
        app.event(key(KeyCode::Char('i')));
    }
    assert_eq!(
        app.overlay,
        datui::Overlay::Info,
        "{column}: the panel opens"
    );
    let at = app
        .data_table_state
        .as_ref()
        .unwrap()
        .schema()
        .index_of(column)
        .unwrap();
    // The panel opens on Notes when there are notes; Schema is the first tab.
    for _ in 0..8 {
        if app.info_modal.active_tab == datui::widgets::info::InfoTab::Schema {
            break;
        }
        app.event(key(KeyCode::Left));
    }
    for _ in 0..8 {
        app.event(key(KeyCode::Up));
    }
    for _ in 0..at {
        app.event(key(KeyCode::Down));
    }
    app.event(key(KeyCode::Enter));
    assert!(matches!(app.overlay, datui::Overlay::Retype { .. }));
}

fn summaries(app: &App) -> Vec<String> {
    app.data_table_state
        .as_ref()
        .unwrap()
        .notes()
        .into_iter()
        .map(|n| n.summary)
        .collect()
}

mod capture;
#[cfg(all(feature = "cloud", feature = "http"))]
mod catalog;
#[cfg(all(feature = "cloud", feature = "http"))]
mod public_datasets;
mod quality_export;
mod terminal_escape;

mod analysis;
mod chart;
mod data_quality;
mod export;
mod harness;
mod home_screen;
mod inspector;
mod loading;
mod query;
mod table_keys;
mod views;

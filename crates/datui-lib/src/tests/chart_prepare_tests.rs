use crate::chart_export::ChartExportFormat;
use crate::chart_modal::{Aggregate, ChartFocus, Mark};
use crate::chart_plot::{LinesData, PlotData};
use crate::*;
use std::sync::mpsc;

/// A histogram of `column` on `modal`: 10 bins, every row.
fn histogram_modal(modal: &mut ChartModal, column: &str) {
    modal.spec.mark = Mark::Histogram;
    modal.spec.encoding.x.field = Some(column.to_string());
    modal.hist_bins = 10;
    modal.row_limit = None;
}

fn histogram_request(column: &str) -> ChartRequest {
    let mut modal = ChartModal::new();
    histogram_modal(&mut modal, column);
    ChartRequest::from_modal(&modal).unwrap()
}

fn prepared_histogram(column: &str) -> PlotData {
    PlotData::Histogram(chart_data::HistogramData {
        column: column.to_string(),
        bins: Vec::new(),
        groups: Vec::new(),
        other: false,
        share: false,
        x_min: 0.0,
        x_max: 1.0,
        max_count: 0.0,
        rows: chart_data::RowsRead::default(),
        clipped: None,
    })
}

/// A preparation of `request` for `dataset`, started as a job a test ends by hand.
fn start_prep(app: &mut App, request: &ChartRequest, dataset: Option<u64>) -> jobs::Started {
    let prep = jobs::ChartPrep {
        request: request.clone(),
        dataset,
        cancel: Arc::default(),
    };
    app.job_for_tests(Job::ChartPrepare(Box::new(prep)), None)
}

/// End `started` with `outcome` and hand the end to the app.
fn end_prep(app: &mut App, started: jobs::Started, outcome: Result<PlotData, (&str, bool)>) {
    let ticket = started.ticket();
    started.end(match outcome {
        Ok(data) => Outcome::answered(Answer::ChartPrepared(Box::new((data, None)))),
        Err((message, panicked)) => Outcome::Failed {
            message: message.to_string(),
            panicked,
        },
    });
    app.event(AppEvent::JobEnded(ticket));
}

fn is_chart_prep(job: &Job) -> bool {
    matches!(job, Job::ChartPrepare(_))
}

/// Whether the preparation running is told to stop.
fn cancelled(app: &App) -> bool {
    match app.jobs.current(is_chart_prep) {
        Some((_, Job::ChartPrepare(prep))) => {
            prep.cancel.load(std::sync::atomic::Ordering::Relaxed)
        }
        _ => panic!("a preparation is running"),
    }
}

/// A result computed against a dataset that is no longer the one open is dropped
/// even when the job is still current.
#[test]
fn a_result_for_another_dataset_is_dropped() {
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    let request = histogram_request("a");
    let started = start_prep(&mut app, &request, Some(12345));
    end_prep(&mut app, started, Ok(prepared_histogram("a")));
    assert!(!app.chart.cache.satisfies(&request));
    assert!(!app.jobs.running(is_chart_prep));
}

fn chart_request(path: &str) -> ChartExportRequest {
    ChartExportRequest {
        path: PathBuf::from(path),
        format: ChartExportFormat::Png,
        options: chart_export::ExportOptions::default(),
        overwrite: Overwrite::Forbid,
        recipe: false,
    }
}

/// Leaving the dataset drops the chart state with it: nothing keeps spinning on the
/// home screen, the worker still running is waited for and its result discarded
/// (nothing else starts until it lands), and an export parked on data that will
/// never come stops holding the app busy.
#[test]
fn leaving_the_dataset_resets_chart_state() {
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    let request = histogram_request("a");
    let started = start_prep(&mut app, &request, None);
    app.chart.export_waiting = Some(chart_request("/tmp/x.png"));
    app.busy = true;

    app.abandon_load();
    assert!(!app.chart_preparing(), "nothing spins on the home screen");
    assert!(
        app.jobs.running(is_chart_prep) && app.jobs.current(is_chart_prep).is_none(),
        "the worker cannot be stopped mid-read, so it runs on, superseded"
    );
    assert!(app.chart.export_waiting.is_none());
    assert!(!app.is_busy());

    end_prep(&mut app, started, Ok(prepared_histogram("a")));
    assert!(
        !app.chart.cache.satisfies(&request),
        "a superseded answer is dropped"
    );
    assert!(!app.jobs.running(is_chart_prep), "and the next may start");
}

/// Going home in the one-frame window between `ChartExport` arming `busy` and the
/// deferred `DoChartExport`: the export must not be parked on a view that is gone,
/// leaving the home screen busy forever.
#[test]
fn a_chart_export_deferred_past_the_chart_view_releases_busy() {
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    let next = app
        .event(AppEvent::ChartExport(chart_request("/tmp/x.png")))
        .expect("ChartExport defers to DoChartExport");
    assert!(app.is_busy());

    app.enter_home();
    app.event(next);
    assert!(!app.is_busy());
    assert!(app.nothing_loading());
    assert!(app.chart.export_waiting.is_none());
}

/// Going home while the export file is being written: the app stops being busy,
/// and when the write finishes its result is ignored rather than reopening the
/// export modal over the home screen.
#[test]
fn leaving_the_dataset_abandons_an_export_write() {
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    let path = PathBuf::from("/tmp/x.png");
    let write = app.job_for_tests(
        Job::ChartExport {
            path: path.clone(),
            format: ChartExportFormat::Png,
        },
        Some("Exporting chart..."),
    );
    app.export_progress = Some(crate::ExportProgress::new(&path, "Exporting chart"));
    let task_generation = app.task_generation();

    app.abandon_load();
    assert!(!app.is_busy());
    assert!(app.nothing_loading());
    assert_eq!(app.task_generation(), task_generation);

    let ticket = write.ticket();
    write.end(Outcome::Failed {
        message: "disk full".to_string(),
        panicked: false,
    });
    app.event(AppEvent::JobEnded(ticket));
    assert!(!app.error_modal.active);
    assert!(!app.chart.export_modal.active);
    assert!(!app.is_busy());
}

/// A running export's bar names the phase and the file, then the bytes written
/// last, where their changing width moves nothing; a stale count is ignored.
#[test]
fn an_export_counts_the_bytes_it_has_written() {
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    let df = polars::df!("id" => [1i64, 2]).unwrap();
    app.data_table_state = Some(
        DataTableState::new(
            polars::prelude::IntoLazy::lazy(df),
            None,
            None,
            None,
            None,
            true,
        )
        .expect("a state"),
    );
    let export = app.job_for_tests(Job::Export, Some("Exporting..."));
    let ticket = export.ticket();
    app.export_progress = Some(crate::ExportProgress::new(
        &PathBuf::from("/tmp/out.csv"),
        "Collecting data",
    ));
    // An export that has ended.
    let older = app.job_for_tests(Job::Export, None);
    let passed = older.ticket();
    drop(older);
    assert!(app.jobs.end(passed).is_some());
    let bar = |app: &mut App| {
        let area = Rect::new(0, 0, 100, 24);
        let mut buf = Buffer::empty(area);
        (&mut *app).render(area, &mut buf);
        (0..area.width)
            .map(|x| buf[(x, area.height - 1)].symbol().to_string())
            .collect::<String>()
    };
    assert!(bar(&mut app).contains("Collecting data...  out.csv"));

    let writing = |ticket, bytes| AppEvent::JobProgress {
        ticket,
        progress: Progress::ExportWriting {
            phase: "Writing file",
            bytes,
        },
    };
    app.event(writing(ticket, 1_572_864));
    assert!(
        bar(&mut app).contains("Writing file...  out.csv  1.5 MiB"),
        "{}",
        bar(&mut app)
    );
    app.event(writing(passed, 9_999_999));
    assert!(
        bar(&mut app).contains("out.csv  1.5 MiB"),
        "{}",
        bar(&mut app)
    );

    // A count that lands after the export finished brings no progress back.
    export.end(Outcome::answered(Answer::Exported(PathBuf::from(
        "/tmp/out.csv",
    ))));
    app.event(AppEvent::JobEnded(ticket));
    app.event(writing(ticket, 2_000_000));
    assert!(app.nothing_loading());
    assert!(!app.is_busy());
    assert!(bar(&mut app).contains("Exported to"), "{}", bar(&mut app));
}

/// A selection that cannot be charted is remembered as failed rather than retried
/// after every event, which would spin the throbber forever.
#[test]
fn a_failed_preparation_is_remembered_not_retried() {
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    let request = histogram_request("a");
    let started = start_prep(&mut app, &request, None);
    end_prep(&mut app, started, Err(("duplicate column", false)));
    assert!(!app.jobs.running(is_chart_prep));
    assert!(matches!(
        app.chart.cache.get(&request),
        Some(Err(m)) if m == "duplicate column"
    ));
    assert!(!app.chart.cache.satisfies(&request));

    // With that selection on screen, nothing more is wanted.
    app.input_mode = InputMode::Chart;
    app.chart.modal.active = true;
    histogram_modal(&mut app.chart.modal, "a");
    assert_eq!(ChartRequest::from_modal(&app.chart.modal), Some(request));
    assert!(!app.chart_request_pending(), "not asked for again");
}

/// Moving past the selection in flight tells it to stop, so a count streaming the
/// whole view does not hold up the next chart. A count stopped part way is dropped,
/// not remembered as a failure; a result that finished anyway is kept.
#[test]
fn moving_on_cancels_the_preparation_in_flight() {
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    let a = histogram_request("a");
    app.input_mode = InputMode::Chart;
    app.chart.modal.active = true;
    histogram_modal(&mut app.chart.modal, "a");
    let started = start_prep(&mut app, &a, None);

    app.ensure_chart_data();
    assert!(!cancelled(&app), "still the selection on screen");
    app.chart.modal.spec.encoding.x.field = Some("b".to_string());
    app.ensure_chart_data();
    assert!(cancelled(&app), "moved past");

    end_prep(&mut app, started, Err(("count cancelled", false)));
    assert!(
        app.chart.cache.get(&a).is_none(),
        "not remembered as failed"
    );

    let started = start_prep(&mut app, &a, None);
    app.ensure_chart_data();
    end_prep(&mut app, started, Ok(prepared_histogram("a")));
    assert!(app.chart.cache.satisfies(&a), "a finished read is kept");

    // A count is of the whole view: another order or sample size of the same
    // category waits for the pass rather than starting it over.
    app.chart.modal.spec.mark = Mark::Bar;
    app.chart.modal.spec.encoding.x.field = Some("carrier".to_string());
    app.chart.modal.spec.encoding.y.aggregate = Aggregate::Count;
    let count = ChartRequest::from_modal(&app.chart.modal).unwrap();
    let _counting = start_prep(&mut app, &count, None);
    app.chart.modal.bar_order = chart_data::BarOrder::Label;
    app.chart.modal.row_limit = Some(100);
    app.ensure_chart_data();
    assert!(!cancelled(&app), "the same count");
    app.chart.modal.spec.encoding.x.field = Some("origin".to_string());
    app.ensure_chart_data();
    assert!(cancelled(&app), "another category");
}

/// Two selections that alternate stay prepared: neither is collected again when
/// the user toggles between them, whether they succeeded or failed.
#[test]
fn alternating_selections_keep_their_entries() {
    let mut cache = ChartCache::default();
    let a = histogram_request("a");
    let b = histogram_request("b");
    cache.insert(a.clone(), Ok(prepared_histogram("a")));
    cache.insert(b.clone(), Err("no numbers".into()));
    assert!(cache.satisfies(&a));
    assert!(matches!(cache.get(&b), Some(Err(m)) if m == "no numbers"));

    // Re-inserting replaces rather than duplicates, and moves to the back.
    cache.insert(a.clone(), Ok(prepared_histogram("a")));
    assert_eq!(cache.entries.len(), 2);

    // Fill to capacity, then one more evicts the least recently used: `b`.
    for i in 0..ChartCache::CAPACITY - 2 {
        cache.insert(
            histogram_request(&format!("c{i}")),
            Ok(prepared_histogram("c")),
        );
    }
    assert_eq!(cache.entries.len(), ChartCache::CAPACITY);
    assert!(cache.get(&b).is_some());
    cache.insert(histogram_request("one more"), Ok(prepared_histogram("d")));
    assert_eq!(cache.entries.len(), ChartCache::CAPACITY);
    assert!(cache.get(&b).is_none(), "the least recently used went");
    assert!(cache.satisfies(&a), "the refreshed one is still there");
}

fn xy_request(x: &str) -> ChartRequest {
    let mut modal = ChartModal::new();
    modal.spec.encoding.x.field = Some(x.to_string());
    modal.spec.encoding.y.field = vec!["y".to_string()];
    modal.row_limit = None;
    ChartRequest::from_modal(&modal).unwrap()
}

fn prepared_xy() -> PlotData {
    PlotData::Lines(LinesData::new(
        chart_data::GroupedSeries {
            names: vec!["y".to_string()],
            series: vec![vec![(0.0, 1.0)]],
            breaks: vec![Vec::new()],
            x_axis_kind: chart_data::XAxisTemporalKind::Numeric,
            rows: chart_data::RowsRead::default(),
            other: false,
        },
        None,
    ))
}

/// XY series are the payload that grows with the data, so fewer of them are kept
/// than small kinds, the one on screen is kept over one merely inserted later, and
/// only the one on screen carries a log-scale copy.
#[test]
fn xy_entries_are_few_and_the_one_on_screen_stays() {
    let has_log = |cache: &ChartCache, request: &ChartRequest| {
        matches!(
            cache.prepared(request),
            Some(PlotData::Lines(xy)) if xy.series_log.is_some()
        )
    };
    let mut cache = ChartCache::default();
    let (a, b, c) = (xy_request("a"), xy_request("b"), xy_request("c"));
    cache.insert(a.clone(), Ok(prepared_xy()));
    cache.insert(b.clone(), Ok(prepared_xy()));
    assert!(
        !has_log(&cache, &a) && !has_log(&cache, &b),
        "made when wanted"
    );
    cache.touch(&a, true);
    assert!(matches!(
        cache.prepared(&a),
        Some(PlotData::Lines(xy)) if xy.series_log.as_deref() == Some(&[vec![(0.0, 2f64.ln())]][..])
    ));
    assert!(!has_log(&cache, &b));

    cache.insert(c.clone(), Ok(prepared_xy()));
    assert!(cache.satisfies(&a), "on screen, so kept");
    assert!(!cache.satisfies(&b), "least recently used XY went");
    assert!(cache.satisfies(&c));
    assert_eq!(cache.entries.len(), ChartCache::XY_CAPACITY);

    // Small kinds are not counted against the XY cap, and vice versa.
    cache.insert(histogram_request("h"), Ok(prepared_histogram("h")));
    assert_eq!(cache.entries.len(), 3);

    cache.touch(&c, true);
    assert!(has_log(&cache, &c));
    assert!(
        !has_log(&cache, &a),
        "only the one on screen keeps its log copy"
    );
    cache.touch(&c, false);
    assert!(!has_log(&cache, &c), "a linear scale keeps none");
}

/// Writes a CSV with columns x and y where y = x * factor, so two datasets share a
/// schema but not values.
fn write_xy_csv(dir: &std::path::Path, name: &str, factor: i64) -> PathBuf {
    let path = dir.join(name);
    let mut body = String::from("x,y\n");
    for x in 0..5i64 {
        body.push_str(&format!("{x},{}\n", x * factor));
    }
    std::fs::write(&path, body).unwrap();
    path
}

/// Drive background results back into the app until `done`.
pub(super) fn pump(
    app: &mut App,
    rx: &mpsc::Receiver<AppEvent>,
    tx: &mpsc::Sender<AppEvent>,
    done: impl Fn(&App) -> bool,
) {
    // Waits on the channel between checks; the deadline is only a hang guard.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(300);
    loop {
        while let Ok(ev) = rx.try_recv() {
            if let Some(next) = app.event(ev) {
                let _ = tx.send(next);
            }
        }
        // As the run loop paints after every update.
        app.frame_painted();
        if done(app) {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "app never reached the expected state"
        );
        if let Ok(ev) = rx.recv_timeout(std::time::Duration::from_millis(50))
            && let Some(next) = app.event(ev)
        {
            let _ = tx.send(next);
        }
    }
}

pub(super) fn open(
    app: &mut App,
    rx: &mpsc::Receiver<AppEvent>,
    tx: &mpsc::Sender<AppEvent>,
    path: PathBuf,
) {
    // As `home_open_path` does before it emits the `Open`.
    app.input_mode = InputMode::Normal;
    if let Some(next) = app.event(AppEvent::Open(vec![path], OpenOptions::default())) {
        let _ = tx.send(next);
    }
    pump(app, rx, tx, |a| {
        a.data_table_state.is_some() && !a.is_busy()
    });
}

fn select_xy(app: &mut App) {
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Char('c'),
        KeyModifiers::NONE,
    )));
    assert_eq!(app.input_mode, InputMode::Chart);
    app.chart.modal.set_mark(Mark::Line);
    app.chart.modal.spec.encoding.x.field = Some("x".to_string());
    app.chart.modal.spec.encoding.y.field = vec!["y".to_string()];
    app.event(AppEvent::Resize(80, 24));
}

/// A chart still being prepared when the user goes home and opens another file with
/// the same columns must not land in the new dataset; the new dataset's own values
/// are what gets charted.
#[test]
fn a_prepare_from_the_previous_dataset_does_not_land_in_the_next() {
    crate::tests::ensure_sample_data();
    let dir = tempfile::tempdir().unwrap();
    let first = write_xy_csv(dir.path(), "first.csv", 1);
    let second = write_xy_csv(dir.path(), "second.csv", 100);
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), crate::tests::test_runtime());

    open(&mut app, &rx, &tx, first);
    select_xy(&mut app);
    assert!(app.chart_preparing());

    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Char('o'),
        KeyModifiers::CONTROL,
    )));
    assert_eq!(app.input_mode, InputMode::Home);
    assert!(!app.chart_preparing(), "nothing spins on the home screen");

    open(&mut app, &rx, &tx, second);
    select_xy(&mut app);
    pump(&mut app, &rx, &tx, |a| a.chart_data_ready());

    let request = ChartRequest::from_modal(&app.chart.modal).unwrap();
    let Some(PlotData::Lines(xy)) = app.chart.cache.prepared(&request) else {
        panic!("an XY chart is prepared");
    };
    assert_eq!(xy.series[0][4], (4.0, 400.0), "the second dataset's values");
    assert!(!app.chart_preparing());
}

/// A preparation that panics ends its job like any other: the selection is
/// remembered as failed, with a message that names no internals, and the next
/// preparation may start.
#[test]
fn a_panicked_preparation_is_remembered_and_frees_the_next() {
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    let request = histogram_request("a");
    let started = start_prep(&mut app, &request, None);
    end_prep(&mut app, started, Err(("see the log", true)));
    assert!(!app.jobs.running(is_chart_prep));
    assert!(matches!(
        app.chart.cache.get(&request),
        Some(Err(m)) if m == "Chart preparation panicked"
    ));
}

fn key(app: &mut App, code: KeyCode) {
    app.event(AppEvent::Key(KeyEvent::new(code, KeyModifiers::NONE)));
}

fn screen(app: &mut App) -> String {
    let area = ratatui::layout::Rect::new(0, 0, 100, 24);
    let mut buf = ratatui::buffer::Buffer::empty(area);
    app.render(area, &mut buf);
    buf.content().iter().map(|c| c.symbol()).collect()
}

/// Sorting or filtering the table between two looks at the chart keeps its X and
/// Y; the chart comes back as it was left, drawn from the new view.
#[test]
fn a_sort_or_filter_keeps_the_chart_columns() {
    crate::tests::ensure_sample_data();
    let dir = tempfile::tempdir().unwrap();
    let path = write_xy_csv(dir.path(), "keep.csv", 10);
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), crate::tests::test_runtime());
    open(&mut app, &rx, &tx, path);
    select_xy(&mut app);
    pump(&mut app, &rx, &tx, |a| a.chart_data_ready());
    key(&mut app, KeyCode::Esc);
    assert_eq!(app.input_mode, InputMode::Normal);

    app.event(AppEvent::Sort(vec!["y".to_string()], vec![true]));
    pump(&mut app, &rx, &tx, |a| !a.is_busy());
    key(&mut app, KeyCode::Char('c'));
    assert_eq!(app.input_mode, InputMode::Chart);
    assert_eq!(app.chart.modal.x().map(String::as_str), Some("x"));
    assert_eq!(app.chart.modal.y(), ["y"]);
    pump(&mut app, &rx, &tx, |a| a.chart_data_ready());
    let request = ChartRequest::from_modal(&app.chart.modal).unwrap();
    let Some(PlotData::Lines(xy)) = app.chart.cache.prepared(&request) else {
        panic!("an XY chart is prepared");
    };
    assert_eq!(
        xy.series[0],
        [
            (0.0, 0.0),
            (1.0, 10.0),
            (2.0, 20.0),
            (3.0, 30.0),
            (4.0, 40.0)
        ],
        "a descending sort still draws left to right"
    );
    key(&mut app, KeyCode::Esc);

    use crate::filter_modal::{FilterOperator, LogicalOperator};
    app.event(AppEvent::Filter(vec![FilterStatement {
        columns: Vec::new(),
        column: "x".to_string(),
        operator: FilterOperator::Lt,
        value: "3".to_string(),
        logical_op: LogicalOperator::And,
    }]));
    pump(&mut app, &rx, &tx, |a| !a.is_busy());
    key(&mut app, KeyCode::Char('c'));
    assert_eq!(app.chart.modal.x().map(String::as_str), Some("x"));
    assert_eq!(app.chart.modal.y(), ["y"]);
    pump(&mut app, &rx, &tx, |a| a.chart_data_ready());
    let request = ChartRequest::from_modal(&app.chart.modal).unwrap();
    let Some(PlotData::Lines(xy)) = app.chart.cache.prepared(&request) else {
        panic!("an XY chart is prepared");
    };
    assert_eq!(xy.series[0].len(), 3, "drawn from the filtered view");
}

/// Stepping through the aggregates prepares only where the steps stop: none was
/// drawn, then count, distinct and sum are passed over, and mean is grouped once
/// the steps have paused.
#[test]
fn quick_aggregate_steps_group_only_where_they_stop() {
    crate::tests::ensure_sample_data();
    let dir = tempfile::tempdir().unwrap();
    let path = write_xy_csv(dir.path(), "steps.csv", 10);
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), crate::tests::test_runtime());
    open(&mut app, &rx, &tx, path);
    select_xy(&mut app);
    pump(&mut app, &rx, &tx, |a| a.chart_data_ready());

    for aggregate in [
        Aggregate::Count,
        Aggregate::Distinct,
        Aggregate::Sum,
        Aggregate::Mean,
    ] {
        app.chart.modal.spec.encoding.y.aggregate = aggregate;
        app.event(AppEvent::Wake);
        assert!(
            !app.jobs.running(is_chart_prep),
            "{aggregate:?} waits for the next"
        );
        assert!(app.chart_preparing(), "and says it is coming");
    }
    pump(&mut app, &rx, &tx, |a| a.chart_data_ready());
    let request = ChartRequest::from_modal(&app.chart.modal).unwrap();
    assert_eq!(request.spec.encoding.y.aggregate, Aggregate::Mean);
    let mut passed = request.clone();
    for aggregate in [Aggregate::Count, Aggregate::Distinct, Aggregate::Sum] {
        passed.spec.encoding.y.aggregate = aggregate;
        assert!(
            app.chart.cache.get(&passed).is_none(),
            "{aggregate:?} never ran"
        );
    }
}

/// A selection that cannot be prepared says why on the chart, instead of drawing
/// empty axes.
#[test]
fn a_failed_preparation_shows_its_error() {
    crate::tests::ensure_sample_data();
    let dir = tempfile::tempdir().unwrap();
    let path = write_xy_csv(dir.path(), "fails.csv", 1);
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), crate::tests::test_runtime());
    open(&mut app, &rx, &tx, path);
    select_xy(&mut app);
    app.chart.modal.spec.encoding.y.field = vec!["gone".to_string()];
    app.event(AppEvent::Resize(100, 24));
    let request = ChartRequest::from_modal(&app.chart.modal).unwrap();
    pump(&mut app, &rx, &tx, |a| {
        a.chart.cache.get(&request).is_some()
    });
    assert!(matches!(app.chart.cache.get(&request), Some(Err(_))));
    let text = screen(&mut app);
    assert!(text.contains("gone"), "the error names the column: {text}");
}

/// The Bar tab charts a grouped result: a query averages a string column's groups,
/// the category and value are picked with the keys, and one bar per carrier is drawn
/// with its value beside it, largest first, then A to Z once the order changes.
#[test]
fn a_bar_chart_draws_a_grouped_string_column() {
    crate::tests::ensure_sample_data();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("flights.csv");
    let mut body = String::from("carrier,arr_delay\n");
    for (carrier, delays) in [
        ("UA", [3.0, 4.0]),
        ("AS", [-10.0, -9.0]),
        ("F9", [20.0, 24.0]),
        ("AA", [0.0, 1.0]),
    ] {
        for d in delays {
            body.push_str(&format!("{carrier},{d}\n"));
        }
    }
    std::fs::write(&path, body).unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), crate::tests::test_runtime());
    open(&mut app, &rx, &tx, path);
    if let Some(next) = app.event(AppEvent::QQuery(
        "select delay: avg arr_delay by carrier".to_string(),
    )) {
        let _ = tx.send(next);
    }
    pump(&mut app, &rx, &tx, |a| {
        !a.is_busy()
            && a.data_table_state
                .as_ref()
                .is_some_and(|s| s.schema().get("delay").is_some())
    });

    // `c` on carrier, text: a bar of its counts. Y takes delay, as it is.
    key(&mut app, KeyCode::Char('c'));
    assert_eq!(app.chart.modal.mark(), Mark::Bar);
    assert_eq!(app.chart.modal.x().map(String::as_str), Some("carrier"));
    app.chart.modal.focus = ChartFocus::Y;
    key(&mut app, KeyCode::Enter);
    assert_eq!(
        app.chart.modal.picker.as_ref().unwrap().items(),
        ["delay"],
        "a number to measure"
    );
    key(&mut app, KeyCode::Enter);
    key(&mut app, KeyCode::Tab);
    assert_eq!(app.chart.modal.focus, ChartFocus::Aggregate);
    key(&mut app, KeyCode::Left);
    assert_eq!(app.chart.modal.aggregate(), Aggregate::None);
    assert_eq!(app.chart.modal.y(), ["delay"]);
    app.event(AppEvent::Resize(100, 24));
    pump(&mut app, &rx, &tx, |a| a.chart_data_ready());

    let rows = |app: &mut App| -> Vec<String> {
        let area = ratatui::layout::Rect::new(0, 0, 100, 24);
        let mut buf = ratatui::buffer::Buffer::empty(area);
        app.render(area, &mut buf);
        (0..24)
            .map(|y| (42..100).map(|x| buf[(x, y)].symbol()).collect())
            .collect()
    };
    let starts = |rows: &[String]| -> Vec<String> {
        rows.iter()
            .filter_map(|r| {
                let mut words = r.split_whitespace();
                let label = words.next()?;
                let value = words.next()?;
                ["UA", "AS", "F9", "AA"]
                    .contains(&label)
                    .then(|| format!("{label} {value}"))
            })
            .collect()
    };
    let drawn = rows(&mut app);
    assert!(
        drawn
            .iter()
            .any(|r| r.contains("carrier") && r.contains("delay")),
        "{drawn:#?}"
    );
    assert_eq!(
        starts(&drawn),
        ["F9 22.00", "UA 3.50", "AA 0.50", "AS -9.50"],
        "{drawn:#?}"
    );
    let full = crate::glyphs::get().bar_eighths[7];
    let f9 = drawn
        .iter()
        .find(|r| r.trim_start().starts_with("F9"))
        .unwrap();
    assert!(
        f9.matches(full).count() > 20,
        "the largest bar is long: {f9:?}"
    );

    app.chart.modal.focus = ChartFocus::Order;
    key(&mut app, KeyCode::Right);
    assert_eq!(app.chart.modal.bar_order, chart_data::BarOrder::Label);
    pump(&mut app, &rx, &tx, |a| a.chart_data_ready());
    assert_eq!(
        starts(&rows(&mut app)),
        ["AA 0.50", "AS -9.50", "F9 22.00", "UA 3.50"]
    );

    // At 80 columns the footer keeps what the focused row takes and export,
    // beside help.
    let bar = |app: &mut App| -> String {
        let area = ratatui::layout::Rect::new(0, 0, 80, 24);
        let mut buf = ratatui::buffer::Buffer::empty(area);
        app.render(area, &mut buf);
        (0..80).map(|x| buf[(x, 23)].symbol()).collect()
    };
    let order = bar(&mut app);
    for chip in ["Order", "Export", "? keys"] {
        assert!(order.contains(chip), "{chip} in {order:?}");
    }
    key(&mut app, KeyCode::Up);
    let value = bar(&mut app);
    for chip in ["Space", "Pick", "Export", "? keys"] {
        assert!(value.contains(chip), "{chip} in {value:?}");
    }
}

/// Count, first in the Value picker, charts the rows per category of the raw table
/// with no query: exact past the sample size, and the note says they are counts of
/// every row.
#[test]
fn a_bar_chart_counts_rows_per_category() {
    crate::tests::ensure_sample_data();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("penguins.csv");
    let mut body = String::from("species,island\n");
    for (species, n) in [("Adelie", 152), ("Gentoo", 124), ("Chinstrap", 68)] {
        for _ in 0..n {
            body.push_str(&format!("{species},Biscoe\n"));
        }
    }
    body.push_str(",Dream\n");
    std::fs::write(&path, body).unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), crate::tests::test_runtime());
    open(&mut app, &rx, &tx, path);

    // `c` on species, text: a bar of its counts, exact whatever the sample size.
    key(&mut app, KeyCode::Char('c'));
    assert_eq!(app.chart.modal.mark(), Mark::Bar);
    assert_eq!(app.chart.modal.aggregate(), Aggregate::Count);
    app.chart.modal.row_limit = Some(100);
    app.event(AppEvent::Resize(100, 24));
    pump(&mut app, &rx, &tx, |a| a.chart_data_ready());

    let area = ratatui::layout::Rect::new(0, 0, 100, 24);
    let mut buf = ratatui::buffer::Buffer::empty(area);
    app.render(area, &mut buf);
    let drawn: Vec<String> = (0..24)
        .map(|y| (42..100).map(|x| buf[(x, y)].symbol()).collect())
        .collect();
    let starts: Vec<String> = drawn
        .iter()
        .filter_map(|r| {
            let mut words = r.split_whitespace();
            let label = words.next()?;
            let value = words.next()?;
            ["Adelie", "Gentoo", "Chinstrap", "species"]
                .contains(&label)
                .then(|| format!("{label} {value}"))
        })
        .collect();
    assert_eq!(
        starts,
        ["species count", "Adelie 152", "Gentoo 124", "Chinstrap 68"],
        "{drawn:#?}"
    );
    assert!(
        drawn.iter().any(|r| r.contains("all 345 rows")),
        "{drawn:#?}"
    );
    assert!(!drawn.iter().any(|r| r.contains("sample of")), "{drawn:#?}");
}

/// Esc leaves a worker running that cannot be cancelled; reopening the chart and
/// selecting again queues the new request behind it. The user is waiting on a
/// computation, so the throbber must show, and the request must then be prepared.
#[test]
fn a_reselection_behind_a_stale_worker_counts_as_preparing() {
    crate::tests::ensure_sample_data();
    let dir = tempfile::tempdir().unwrap();
    let path = write_xy_csv(dir.path(), "reselect.csv", 3);
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), crate::tests::test_runtime());
    open(&mut app, &rx, &tx, path);

    select_xy(&mut app);
    assert!(app.chart_preparing());
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Esc,
        KeyModifiers::NONE,
    )));
    assert_eq!(app.input_mode, InputMode::Normal);
    assert!(
        !app.chart_preparing(),
        "nothing is wanted while the chart is closed"
    );
    assert!(
        app.jobs.running(is_chart_prep) && app.jobs.current(is_chart_prep).is_none(),
        "the superseded worker is still running"
    );

    select_xy(&mut app);
    assert!(
        app.chart_preparing(),
        "a request waiting behind the orphan is being prepared, in effect"
    );
    pump(&mut app, &rx, &tx, |a| a.chart_data_ready());
    assert!(!app.chart_preparing());
}

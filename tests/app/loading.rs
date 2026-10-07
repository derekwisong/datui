//! Opening a dataset: formats, directories and partitions, schemas and drift notes, downloads, abandoned loads, the first rows and the row count.

use super::*;

/// An object store URL is scanned on a worker, and a failure names the store.
#[test]
fn test_open_object_store_urls_scan_on_a_worker_and_name_the_store() {
    for (url, names) in [
        ("s3://my-bucket/path/to/file.parquet", &["s3"][..]),
        (
            "gs://my-bucket/path/file.parquet",
            &["gcs", "gs://", "not enabled"][..],
        ),
    ] {
        let (tx, rx) = mpsc::channel();
        let mut app = App::new(tx, common::test_runtime());
        // The scan is spawned, so this returns nothing; the outcome comes over the channel.
        assert!(
            app.event(AppEvent::Open(
                vec![PathBuf::from(url)],
                OpenOptions::default()
            ))
            .is_none(),
            "{url}: scan should be spawned, not run inline"
        );
        let ended = await_scan_outcome(&rx);
        assert!(
            matches!(ended, AppEvent::JobEnded(t) if t.kind() == JobKind::Load),
            "{url}: expected a scan outcome"
        );
        // With the feature and real credentials the scan can succeed.
        let _ = app.event(ended);
        if let Some(message) = app.error_message() {
            let lower = message.to_lowercase();
            assert!(
                names.iter().any(|name| lower.contains(name)),
                "{url}: the error names the store: {message}"
            );
        }
    }
}

#[test]
fn test_open_http_url_attempts_load_or_returns_friendly_error() {
    let (tx, _) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    let path = PathBuf::from("https://example.com/data.csv");
    // The size is asked for, or the file scanned, on a worker: the open returns at once,
    // waiting on it.
    let next = app.event(AppEvent::Open(vec![path], OpenOptions::default()));
    assert!(next.is_none(), "nothing is read on the event thread");
    assert!(app.is_busy(), "the open is under way");
}

#[test]
fn test_multiple_remote_paths_returns_error() {
    let (tx, _) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    let paths = vec![
        PathBuf::from("s3://bucket/a.parquet"),
        PathBuf::from("s3://bucket/b.parquet"),
    ];
    let next = app.event(AppEvent::Open(paths, OpenOptions::default()));
    match next.as_ref() {
        Some(AppEvent::Crash(m)) => assert!(
            m.contains("one S3") || m.contains("one at a time"),
            "error should mention single S3 path: {}",
            m
        ),
        _ => panic!("expected Crash when opening multiple S3 URLs"),
    }
    let (tx2, _) = mpsc::channel();
    let mut app = App::new(tx2, common::test_runtime());
    let paths = vec![
        PathBuf::from("https://example.com/a.csv"),
        PathBuf::from("https://example.com/b.csv"),
    ];
    let next = app.event(AppEvent::Open(paths, OpenOptions::default()));
    match next.as_ref() {
        Some(AppEvent::Crash(m)) => assert!(
            m.contains("one") && (m.contains("HTTP") || m.contains("URL")),
            "error should mention single URL: {}",
            m
        ),
        _ => panic!("expected Crash when opening multiple HTTP URLs"),
    }
}

#[test]
fn test_csv_null_values_global() {
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());

    let test_data_dir = common::fixture_dir();
    let csv_path = test_data_dir.join("null_values_test.csv");
    std::fs::write(&csv_path, "x,y\n1,NA\n2,3\n4,N/A\n").unwrap();

    let opts = OpenOptions {
        null_values: Some(vec!["NA".to_string(), "N/A".to_string()]),
        ..OpenOptions::default()
    };
    pump_open_until_loaded(&mut app, &rx, vec![csv_path], opts);

    assert!(app.data_table_state.is_some());
    let state = app.data_table_state.as_ref().unwrap();
    let df = state.lf().clone().collect().unwrap();
    let y = df.column("y").unwrap();
    assert_eq!(
        y.null_count(),
        2,
        "NA and N/A should be parsed as null in column y"
    );
}

#[test]
fn test_csv_null_values_per_column() {
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());

    let test_data_dir = common::fixture_dir();
    let csv_path = test_data_dir.join("null_values_per_col_test.csv");
    std::fs::write(&csv_path, "a,b\nx,1\nempty,2\nz,3\n").unwrap();

    let opts = OpenOptions {
        null_values: Some(vec!["a=empty".to_string()]),
        ..OpenOptions::default()
    };
    pump_open_until_loaded(&mut app, &rx, vec![csv_path], opts);

    assert!(app.data_table_state.is_some());
    let state = app.data_table_state.as_ref().unwrap();
    let df = state.lf().clone().collect().unwrap();
    let a = df.column("a").unwrap();
    let b = df.column("b").unwrap();
    assert_eq!(a.null_count(), 1, "only 'empty' in column a should be null");
    assert_eq!(b.null_count(), 0, "column b has no per-column null spec");
}

/// Simulate the real main-loop startup: process events from the channel, render
/// the widget (which sets visible_rows and needs_recollect), then check the flag
/// and spawn another async collect.  Repeat many times to shake out races between
/// the initial tiny-buffer collect (visible_rows=0) and the corrected one.
#[test]
fn test_startup_buffer_race_does_not_lose_rows() {
    common::ensure_sample_data();
    let csv_path = PathBuf::from("tests/sample-data/large_dataset.parquet");
    let terminal_area = Rect::new(0, 0, 120, 50); // 50 rows → ~48 visible

    for iteration in 0..50 {
        let (tx, rx) = mpsc::channel();
        let mut app = App::new(tx.clone(), common::test_runtime());

        // Simulate what run() does: send Open, mark busy.
        tx.send(AppEvent::Open(
            vec![csv_path.clone()],
            OpenOptions::default(),
        ))
        .unwrap();

        // Process events like the main loop: drain channel, render, check needs_recollect.
        let mut completed = false;
        for _tick in ticks() {
            // Drain all pending events.
            loop {
                match rx.try_recv() {
                    Ok(AppEvent::Crash(msg)) => panic!("iteration {iteration}: Crash: {msg}"),
                    Ok(event) => {
                        if let Some(next) = app.event(event) {
                            tx.send(next).unwrap();
                        }
                    }
                    Err(_) => break,
                }
            }

            // Render into a buffer (this sets visible_rows and may set needs_recollect).
            let mut buf = Buffer::empty(terminal_area);
            app.render(terminal_area, &mut buf);

            // After render, check needs_recollect — same as the real main loop.
            let needs = app
                .data_table_state
                .as_mut()
                .map(|s| {
                    let n = s.needs_recollect;
                    s.needs_recollect = false;
                    n
                })
                .unwrap_or(false);
            if needs {
                app.spawn_async_collect("Loading buffer...");
            }

            // Check if we have data and are no longer busy.
            if app.data_table_state.is_some() && !app.is_busy() {
                completed = true;
                break;
            }

            // Until the next event, as the run loop sleeps on its channel.
            common::wait_for_event(&tx, &rx);
        }

        assert!(
            completed,
            "iteration {iteration}: timed out waiting for data to load"
        );

        let state = app.data_table_state.as_ref().unwrap();
        assert!(
            state.visible_rows > 0,
            "iteration {iteration}: visible_rows should be set by render"
        );

        // The buffer must cover at least the visible window.
        let buffered_rows = state.buffered_end().saturating_sub(state.buffered_start());
        assert!(
            buffered_rows >= state.visible_rows || buffered_rows >= state.num_rows(),
            "iteration {iteration}: buffer too small: {buffered_rows} buffered but \
             {visible} visible, {total} total rows",
            visible = state.visible_rows,
            total = state.num_rows(),
        );

        // display_slice_df must be Some (not None = no data to show).
        assert!(
            state.display_slice_df().is_some(),
            "iteration {iteration}: display_slice_df is None — buffer not sliced into display"
        );
    }
}

/// A finding's rows over a compressed CSV scan its decompressed copy: capture there
/// is refused as it is for the table, and the copy goes once a new dataset replaces
/// both.
#[test]
fn evidence_rows_over_a_decompressed_file_hold_it() {
    use datui::data_quality::{QualityCompute, QualityPrecision};
    use flate2::{Compression, write::GzEncoder};
    use std::io::Write;

    let source = tempfile::tempdir().unwrap();
    let scratch = tempfile::tempdir().unwrap();
    let gz = source.path().join("missing.csv.gz");
    let mut encoder = GzEncoder::new(File::create(&gz).unwrap(), Compression::default());
    writeln!(encoder, "id,v").unwrap();
    for row in 0..100 {
        let v = if row % 10 == 3 {
            String::new()
        } else {
            row.to_string()
        };
        writeln!(encoder, "{row},{v}").unwrap();
    }
    encoder.finish().unwrap();
    let decompressed = || -> Vec<PathBuf> {
        std::fs::read_dir(scratch.path())
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect()
    };
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    let options = OpenOptions {
        temp_dir: Some(scratch.path().to_path_buf()),
        ..OpenOptions::default()
    };
    pump_open_until_loaded(&mut app, &rx, vec![gz], options);
    pump_until_idle(&mut app, &rx, &tx);
    let file = decompressed().pop().expect("the decompressed copy");

    press(&mut app, KeyCode::Char('a'));
    app.analysis_modal.sidebar_state.select(Some(3));
    show_sample_form(&mut app);
    app.analysis_modal.quality.plan.method = datui::sampling::SampleMethod::EveryRow;
    app.analysis_modal.quality.plan.compute = QualityCompute::Full;
    assert!(press(&mut app, KeyCode::Enter).is_none());
    let next = press(&mut app, KeyCode::Enter);
    drain_quality(&mut app, &rx, next);
    let results = app.analysis_modal.quality.results.as_ref().unwrap();
    assert_eq!(results.precision, QualityPrecision::Exact);

    app.analysis_modal.quality.findings.check = Some("Missing values");
    app.analysis_modal.quality.table_state.select(Some(0));
    press(&mut app, KeyCode::Enter);
    assert!(app.analysis_modal.quality.observation_detail);
    press(&mut app, KeyCode::Enter);
    assert!(app.analysis_modal.quality.evidence_read.is_some());
    let mut next = press(&mut app, KeyCode::Enter);
    while let Some(event) = next {
        next = app.event(event);
    }
    drain_events(&mut app, &rx);
    pump_until_idle(&mut app, &rx, &tx);
    assert_ne!(app.overlay, Overlay::Analysis, "the rows are on screen");
    assert_eq!(app.data_table_state.as_ref().unwrap().num_rows(), 10);

    let Err(error) = app.capture_view() else {
        panic!("the rows scan a temp file");
    };
    assert!(error.to_string().contains("temporary file"), "{error}");
    assert!(file.exists());

    let other = source.path().join("other.csv");
    std::fs::write(&other, "a,b\n1,2\n").unwrap();
    pump_open_until_loaded(&mut app, &rx, vec![other], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(app.data_table_state.as_ref().unwrap().num_rows(), 1);
    assert!(
        decompressed().is_empty(),
        "the replaced dataset's copy went"
    );
}

/// Time stored as text: a role on it says, before Run, that it needs a format;
/// Text as time offers the formats that read the values on screen; and the run
/// then windows by it and measures the time between two of them, counting what
/// the format does not read on its own.
#[test]
fn text_read_as_time_in_setup_gives_windows_and_intervals() {
    use datui::analysis_modal::SetupRow;
    use datui::data_quality::{ObservationKind, QualityPage};

    let (mut app, rx, _tx, _path) = open_text_times_fixture("dq_setup_text_times.parquet");
    press(&mut app, KeyCode::Char('a'));
    app.analysis_modal.sidebar_state.select(Some(3));
    show_sample_form(&mut app);
    assert_eq!(app.analysis_modal.quality.page, QualityPage::Setup);

    // Roles on the two text columns: event on created, received on sent.
    app.analysis_modal.quality.plan_field = SetupRow::TimeRoles.index();
    press(&mut app, KeyCode::Char(' '));
    assert_eq!(app.analysis_modal.quality.page, QualityPage::TimeRoles);
    let candidates = |app: &App| app.analysis_modal.quality.plan.temporal_roles.clone();
    press(&mut app, KeyCode::Right);
    while candidates(&app)[0].column != "created" {
        press(&mut app, KeyCode::Right);
    }
    for _ in 0..5 {
        press(&mut app, KeyCode::Down);
    }
    press(&mut app, KeyCode::Right);
    while candidates(&app)
        .iter()
        .find(|role| role.role == datui::data_quality::TemporalRole::Received)
        .unwrap()
        .column
        != "sent"
    {
        press(&mut app, KeyCode::Right);
    }
    assert!(press(&mut app, KeyCode::Enter).is_none());
    assert_eq!(app.analysis_modal.quality.page, QualityPage::Setup);
    let area = Rect::new(0, 0, 100, 30);
    let render = |app: &mut App| {
        let mut buffer = Buffer::empty(area);
        app.render(area, &mut buffer);
        common::buffer_text(&buffer)
    };
    let screen = render(&mut app);
    assert!(screen.contains("created: text, no format"), "{screen}");

    // Text as time: the column, then the format that reads what is on screen.
    for column in ["created", "sent"] {
        app.analysis_modal.quality.plan_field = SetupRow::TextAsTime.index();
        assert!(press(&mut app, KeyCode::Char(' ')).is_none());
        type_text(&mut app, column);
        press(&mut app, KeyCode::Enter);
        let picker = app.analysis_modal.quality.picker.as_ref().unwrap();
        assert_eq!(picker.title, format!("Read {column} As"));
        assert!(
            picker.state.filtered()[0]
                .1
                .ends_with("datetime %m/%d/%Y %H:%M:%S"),
            "the format that reads the values on screen leads: {:?}",
            picker.state.filtered()[0]
        );
        assert!(press(&mut app, KeyCode::Enter).is_none());
    }
    let screen = render(&mut app);
    assert!(!screen.contains("is text: choose"), "{screen}");
    assert!(screen.contains("created read as datetime %m/%d/%Y %H:%M:%S"));
    assert!(
        screen.contains("Intervals      event to received"),
        "{screen}"
    );

    // A day window of created, now that it reads as time.
    app.analysis_modal.quality.plan_field = SetupRow::Grain.index();
    press(&mut app, KeyCode::Char(' '));
    type_text(&mut app, "day of created");
    press(&mut app, KeyCode::Enter);
    assert!(!app.is_busy());

    let next = press(&mut app, KeyCode::Enter);
    assert!(matches!(
        next,
        Some(AppEvent::AnalysisCompute(AnalysisTool::DataQuality))
    ));
    drain_quality(&mut app, &rx, next);
    let results = app.analysis_modal.quality.results.as_ref().unwrap();
    let labels = results
        .segments
        .iter()
        .map(|segment| segment.label.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        labels,
        [
            "2024-01-01",
            "2024-01-02",
            "2024-01-03",
            "2024-01-04",
            "2024-01-05",
            "created ∅"
        ]
    );
    assert!(!results.temporal.is_empty(), "the interval is measured");
    let unparsed: usize = results.temporal.iter().map(|t| t.unparsed_start).sum();
    let missing: usize = results.temporal.iter().map(|t| t.missing_start).sum();
    assert_eq!((unparsed, missing), (1, 4), "unread and missing apart");
    let finding = results
        .observations
        .iter()
        .find(|observation| observation.kind == ObservationKind::UnparsedTime)
        .expect("text the format does not read is a finding");
    assert_eq!(finding.column, "created");
    assert_eq!(finding.affected_rows, 1);
    // The column itself is still text to every other check.
    let created = results
        .columns
        .iter()
        .find(|column| column.name == "created")
        .unwrap();
    assert_eq!(created.dtype, polars::prelude::DataType::String);
}

/// Intervals from Setup to a count's rows: a start and end the roles do not
/// suggest is chosen in Setup, with the roles it leaves out named first; the
/// window clock and threshold are set there too. The report's list and an
/// interval's detail read nothing, and a count's rows open from the sample the
/// run kept, even once the file is gone.
#[test]
fn intervals_are_chosen_in_setup_and_inspected_without_a_read() {
    use datui::analysis_modal::SetupRow;
    use datui::data_quality::{IntervalClock, IntervalFact, QualityPage, QualityPrecision};

    let name = "dq_intervals_detail.parquet";
    let (mut app, rx, tx, path) = open_text_times_fixture(name);
    press(&mut app, KeyCode::Char('a'));
    app.analysis_modal.sidebar_state.select(Some(3));
    show_sample_form(&mut app);
    assert_eq!(app.analysis_modal.quality.page, QualityPage::Setup);
    let render = |app: &mut App, width, height| {
        let area = Rect::new(0, 0, width, height);
        let mut buffer = Buffer::empty(area);
        app.render(area, &mut buffer);
        common::buffer_text(&buffer)
    };

    for column in ["created", "sent"] {
        app.analysis_modal.quality.plan_field = SetupRow::TextAsTime.index();
        press(&mut app, KeyCode::Char(' '));
        type_text(&mut app, column);
        press(&mut app, KeyCode::Enter);
        press(&mut app, KeyCode::Enter);
    }
    // Created on created, processed on sent: roles no suggested pair joins.
    app.analysis_modal.quality.plan_field = SetupRow::TimeRoles.index();
    press(&mut app, KeyCode::Char(' '));
    for _ in 0..3 {
        press(&mut app, KeyCode::Down);
    }
    press(&mut app, KeyCode::Right);
    for _ in 0..3 {
        press(&mut app, KeyCode::Down);
    }
    press(&mut app, KeyCode::Right);
    press(&mut app, KeyCode::Right);
    press(&mut app, KeyCode::Enter);
    let plan = &app.analysis_modal.quality.plan;
    assert_eq!(
        plan.role_column(datui::data_quality::TemporalRole::Created),
        Some("created")
    );
    assert_eq!(
        plan.role_column(datui::data_quality::TemporalRole::Processed),
        Some("sent")
    );
    let screen = render(&mut app, 100, 30);
    assert!(
        screen.contains("In no interval: created, processed"),
        "Setup names the roles that measure nothing: {screen}"
    );

    // The pair, chosen: Esc puts the list back, Enter keeps it.
    app.analysis_modal.quality.plan_field = SetupRow::Intervals.index();
    assert!(press(&mut app, KeyCode::Char(' ')).is_none());
    assert_eq!(app.analysis_modal.quality.page, QualityPage::IntervalPairs);
    press(&mut app, KeyCode::Char(' '));
    assert_eq!(app.analysis_modal.quality.plan.interval_pairs().len(), 1);
    press(&mut app, KeyCode::Esc);
    assert_eq!(app.analysis_modal.quality.plan.intervals, None);
    press(&mut app, KeyCode::Char(' '));
    press(&mut app, KeyCode::Char(' '));
    press(&mut app, KeyCode::Enter);
    assert_eq!(app.analysis_modal.quality.page, QualityPage::Setup);
    assert_eq!(app.analysis_modal.setup_row(), SetupRow::Intervals);
    let screen = render(&mut app, 100, 30);
    assert!(screen.contains("created to processed"), "{screen}");
    assert!(!screen.contains("In no interval"), "{screen}");

    // A threshold, a daily grain, and intervals by the day they ended.
    app.analysis_modal.quality.plan_field = SetupRow::Latency.index();
    press(&mut app, KeyCode::Right);
    app.analysis_modal.quality.plan_field = SetupRow::Grain.index();
    press(&mut app, KeyCode::Char(' '));
    type_text(&mut app, "day of created");
    press(&mut app, KeyCode::Enter);
    app.analysis_modal.quality.plan_field = SetupRow::WindowBy.index();
    press(&mut app, KeyCode::Right);
    press(&mut app, KeyCode::Right);
    let plan = &app.analysis_modal.quality.plan;
    assert_eq!(plan.interval_clock, IntervalClock::End);
    assert_eq!(plan.latency_threshold_seconds, Some(3_600));
    assert!(!app.is_busy(), "nothing in Setup reads");

    // A sample smaller than the file, so the rows a count opens are the sample's.
    app.analysis_modal.quality.plan.dataset_rows = 300;
    app.analysis_modal.quality.plan.sample_seed = 7;
    let next = press(&mut app, KeyCode::Enter);
    let (finished, _) = drain_quality(&mut app, &rx, next);
    assert_eq!(finished, 1);
    let results = app.analysis_modal.quality.results.as_ref().unwrap();
    assert_eq!(results.precision, QualityPrecision::Sampled);
    assert!(!results.temporal.is_empty());
    assert!(
        results
            .temporal
            .iter()
            .all(|interval| interval.label() == "created to processed"
                && interval.threshold_seconds == Some(3_600))
    );

    // Every sent is an hour after its created: exactly the threshold, never over it.
    assert!(
        results
            .temporal
            .iter()
            .all(|interval| interval.above_threshold_count == Some(0))
    );
    // A count with rows behind it: a start missing or unread in some segment.
    let plan = app.analysis_modal.quality_result_plan().clone();
    let (index, fact, count) = results
        .temporal
        .iter()
        .enumerate()
        .find_map(|(index, interval)| {
            [IntervalFact::MissingStart, IntervalFact::UnparsedStart]
                .into_iter()
                .find_map(|fact| {
                    let (count, _) = interval.count(fact, &plan)?;
                    (count > 0).then_some((index, fact, count))
                })
        })
        .expect("the sample holds a missing or unread start");

    // The list and the detail are the report's measurements: no read.
    assert!(press(&mut app, KeyCode::Char('5')).is_none());
    assert_eq!(app.analysis_modal.quality.page, QualityPage::Intervals);
    let screen = render(&mut app, 80, 24);
    assert!(screen.contains("Over: duration > 1 hour"), "{screen}");
    assert!(screen.contains("created to processed"), "{screen}");
    for _ in 0..index {
        press(&mut app, KeyCode::Down);
    }
    assert!(press(&mut app, KeyCode::Enter).is_none());
    assert_eq!(app.analysis_modal.quality.page, QualityPage::IntervalDetail);
    assert!(!app.is_busy());
    for (width, height) in [(80, 24), (60, 20)] {
        let screen = render(&mut app, width, height);
        for label in [
            "Start",
            "End",
            "Segment",
            "Rows",
            "Both ends",
            "Missing start",
        ] {
            assert!(
                screen.contains(label),
                "{label} at {width}x{height}: {screen}"
            );
        }
    }

    // The count under the cursor; Enter opens its rows from memory.
    let position = app
        .analysis_modal
        .interval_facts()
        .iter()
        .position(|listed| *listed == fact)
        .unwrap();
    for _ in 0..position {
        press(&mut app, KeyCode::Down);
    }
    assert_eq!(app.analysis_modal.selected_interval_fact(), Some(fact));
    assert!(render(&mut app, 100, 30).contains("Show rows"));
    std::fs::remove_file(&path).unwrap();
    let mut next = press(&mut app, KeyCode::Enter);
    while let Some(event) = next {
        next = app.event(event);
    }
    drain_events(&mut app, &rx);
    pump_until_idle(&mut app, &rx, &tx);
    // With the file gone, only the kept sample could have answered.
    assert_ne!(app.overlay, Overlay::Analysis);
    assert_eq!(app.data_table_state.as_ref().unwrap().num_rows(), count);
    press(&mut app, KeyCode::Esc);
    assert_eq!(
        app.overlay,
        Overlay::Analysis,
        "Esc goes back to the detail"
    );
    assert_eq!(app.analysis_modal.quality.page, QualityPage::IntervalDetail);
    // Esc from the detail is the list, the interval still selected.
    press(&mut app, KeyCode::Esc);
    assert_eq!(app.analysis_modal.quality.page, QualityPage::Intervals);
    assert_eq!(
        app.analysis_modal.quality.table_state.selected(),
        Some(index)
    );
}

/// #415's trends and gaps. A trend's bar opens to what it spans, how much of it the
/// sample reached, its rate and how sure that is, with no read. A thin sample offers
/// a coarser window, staged in Setup for Enter, never run. Gaps appear only once
/// Setup states which windows rows are expected in, and stating them reads nothing.
#[test]
fn trends_and_gaps_are_inspected_without_a_read() {
    use datui::analysis_modal::SetupRow;
    use datui::data_quality::{QualityGrain, QualityPage, QualityStage};

    let (mut app, rx, _tx) = open_weekday_feed("dq_trends_gaps.csv");
    press(&mut app, KeyCode::Char('a'));
    app.analysis_modal.sidebar_state.select(Some(3));
    show_sample_form(&mut app);
    let render = |app: &mut App, width, height| {
        let area = Rect::new(0, 0, width, height);
        let mut buffer = Buffer::empty(area);
        app.render(area, &mut buffer);
        common::buffer_text(&buffer)
    };
    let daily = QualityGrain::TimeWindows {
        column: "day".into(),
        every: "1d".into(),
    };
    {
        let plan = &mut app.analysis_modal.quality.plan;
        plan.dataset_rows = 30;
        plan.sample_seed = 415;
        plan.grain = daily.clone();
    }
    // No stated windows: the row says so, and Setup reads nothing for it.
    app.analysis_modal.quality.plan_field = SetupRow::Expected.index();
    assert!(render(&mut app, 100, 30).contains("none: no window is a gap"));
    assert_eq!(
        run_quality_reads(&mut app, &rx),
        [QualityStage::ReadingSample],
        "one pass samples and counts the days"
    );
    let results = app.analysis_modal.quality.results.clone().unwrap();
    assert!(
        !results.unsampled_segments.is_empty(),
        "thirty rows miss some of 35 days"
    );

    // Trends says how many days the sample missed, and offers a coarser window.
    press(&mut app, KeyCode::Char('4'));
    assert_eq!(app.analysis_modal.quality.page, QualityPage::Trends);
    let trends = render(&mut app, 80, 24);
    assert!(trends.contains("not sampled"), "{trends}");
    assert!(trends.contains("w stages weekly"), "{trends}");
    assert!(
        !trends.contains("Expected"),
        "no gaps without stated windows"
    );
    assert!(press(&mut app, KeyCode::Char('g')).is_none());
    assert_eq!(app.analysis_modal.quality.page, QualityPage::Trends);

    // A bar opens to its facts, and the bars walk, from what the report holds.
    let amount = datui::quality_trends::trend_view(
        &results,
        datui::data_quality::QualityMetric::NullRate,
        1,
    )
    .lines
    .iter()
    .position(|line| line.names == ["amount"])
    .expect("an amount line");
    app.analysis_modal.quality.table_state.select(Some(amount));
    assert!(press(&mut app, KeyCode::Enter).is_none());
    assert_eq!(app.analysis_modal.quality.page, QualityPage::TrendDetail);
    assert_eq!(app.analysis_modal.quality.trend_line, amount);
    for size in [(80, 24), (60, 20)] {
        let detail = render(&mut app, size.0, size.1);
        for label in ["Span", "Segments", "Rows", "Null rate", "95% interval"] {
            assert!(detail.contains(label), "{label} at {size:?}:\n{detail}");
        }
        assert!(detail.contains("bar 1 of"), "{detail}");
    }
    assert!(press(&mut app, KeyCode::Down).is_none());
    let second = render(&mut app, 80, 24);
    assert!(second.contains("bar 2 of"), "{second}");
    assert!(second.contains("Previous bar"), "{second}");
    assert!(!app.is_busy() && app.analysis_modal.computing.is_none());
    press(&mut app, KeyCode::Esc);
    assert_eq!(app.analysis_modal.quality.page, QualityPage::Trends);
    assert_eq!(
        app.analysis_modal.quality.table_state.selected(),
        Some(amount)
    );

    // `w` stages weeks in Setup: Read says the days' counts serve them, nothing runs,
    // and Esc puts the grain back.
    assert!(press(&mut app, KeyCode::Char('w')).is_none());
    assert!(!app.is_busy());
    assert_eq!(app.analysis_modal.quality.page, QualityPage::Setup);
    assert_eq!(app.analysis_modal.setup_row(), SetupRow::Grain);
    assert_eq!(
        app.analysis_modal.quality.plan.grain,
        QualityGrain::TimeWindows {
            column: "day".into(),
            every: "1w".into(),
        }
    );
    let staged = render(&mut app, 100, 40);
    assert!(
        staged.contains("summed from earlier daily counts"),
        "{staged}"
    );
    assert!(staged.contains("Enter runs, Esc discards"), "{staged}");
    press(&mut app, KeyCode::Esc);
    assert_eq!(app.analysis_modal.quality.plan.grain, daily);
    assert_eq!(app.analysis_modal.quality.page, QualityPage::Trends);

    // Expected windows, stated in Setup: weekdays, typed dates past the data. The
    // editor's fields type every key, `q` and `?` included.
    press(&mut app, KeyCode::Char('e'));
    app.analysis_modal.quality.plan_field = SetupRow::Expected.index();
    press(&mut app, KeyCode::Char(' '));
    assert_eq!(
        app.analysis_modal.quality.page,
        QualityPage::ExpectedWindows
    );
    press(&mut app, KeyCode::Right);
    press(&mut app, KeyCode::Right);
    press(&mut app, KeyCode::Down);
    type_text(&mut app, "q?");
    assert_eq!(app.overlay, Overlay::Analysis, "q typed, not quit");
    press(&mut app, KeyCode::Enter);
    let editor = render(&mut app, 80, 24);
    assert!(
        editor.contains("q? is not a date or UTC timestamp"),
        "{editor}"
    );
    for _ in 0..2 {
        press(&mut app, KeyCode::Backspace);
    }
    type_text(&mut app, "2024-01-01");
    press(&mut app, KeyCode::Down);
    type_text(&mut app, "2024-03-04");
    assert!(press(&mut app, KeyCode::Enter).is_none());
    assert_eq!(app.analysis_modal.quality.page, QualityPage::Setup);
    let expected = app.analysis_modal.quality.plan.expected.clone().unwrap();
    assert!(expected.weekdays);
    assert_eq!(expected.before.as_deref(), Some("2024-03-04"));
    let setup = render(&mut app, 100, 40);
    assert!(setup.contains("Changed: Expected"), "{setup}");

    // Run checks them against the report on screen: no read, no run.
    assert!(press(&mut app, KeyCode::Enter).is_none());
    assert!(!app.is_busy() && app.analysis_modal.computing.is_none());
    assert_eq!(app.analysis_modal.quality.page, QualityPage::Trends);
    let trends = render(&mut app, 80, 24);
    assert!(trends.contains("Expected weekdays"), "{trends}");
    assert!(press(&mut app, KeyCode::Char('g')).is_none());
    assert_eq!(app.analysis_modal.quality.page, QualityPage::Gaps);
    let gaps = render(&mut app, 100, 30);
    // The missing week and the week after the data are empty; days the sample
    // missed are not.
    assert!(gaps.contains("2024-01-08 to 2024-01-12 empty"), "{gaps}");
    assert!(gaps.contains("2024-02-26 to 2024-03-01 empty"), "{gaps}");
    assert!(gaps.contains("not sampled"), "{gaps}");
    assert!(gaps.contains("on weekends, not expected"), "{gaps}");
    assert!(render(&mut app, 60, 20).contains("empty"));
    press(&mut app, KeyCode::Esc);
    assert_eq!(app.analysis_modal.quality.page, QualityPage::Trends);
}

/// The rows a run read are a snapshot of the session: a file changed on disk is
/// not noticed until it is opened again, as the reference says. Reopened, the
/// dataset is new, and Run reads the file as it is now.
#[test]
fn a_file_changed_on_disk_is_read_again_once_opened_again() {
    let (mut app, rx, tx, path) = open_quality_fixture("dq_changed_on_disk.csv", 1_000, 0);
    assert!(!run_quality_reads(&mut app, &rx).is_empty());
    assert_eq!(amount_nulls(&app), 0);

    write_quality_fixture(&path, 1_000, 2);
    // Back to the report: the session's, from its cache.
    press(&mut app, KeyCode::Esc);
    press(&mut app, KeyCode::Esc);
    assert_ne!(app.overlay, Overlay::Analysis);
    open_quality_setup(&mut app);
    assert!(app.analysis_modal.quality.from_cache);
    assert_eq!(amount_nulls(&app), 0, "the snapshot, not the file");

    // Opened again: a new dataset, and a run that reads it.
    press(&mut app, KeyCode::Esc);
    press(&mut app, KeyCode::Esc);
    pump_open_until_loaded(&mut app, &rx, vec![path], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);
    open_quality_setup(&mut app);
    assert!(!app.analysis_modal.quality.from_cache);
    assert!(app.analysis_modal.quality.page.is_setup());
    assert!(!run_quality_reads(&mut app, &rx).is_empty());
    assert_eq!(amount_nulls(&app), 500, "the file as it is now");
}

/// File metadata only reads no value, from Setup to the report: with the file gone
/// from disk, Setup still lays out and Run still reports what the schema says,
/// with no stage that reads the source and no row evaluated.
#[test]
fn metadata_only_reads_no_values() {
    let (mut app, rx, _tx, path) = open_quality_fixture("dq_metadata_only.parquet", 1_000, 7);
    app.analysis_modal.quality.plan.compute = datui::data_quality::QualityCompute::Metadata;
    std::fs::remove_file(&path).unwrap();
    let area = Rect::new(0, 0, 100, 30);
    let mut buffer = Buffer::empty(area);
    app.render(area, &mut buffer);
    assert!(common::buffer_text(&buffer).contains("File metadata only: no values read"));
    assert!(run_quality_reads(&mut app, &rx).is_empty());
    assert!(!app.modal_showing(), "nothing was read, so nothing failed");
    let results = app.analysis_modal.quality.results.as_ref().unwrap();
    assert_eq!(
        results.precision,
        datui::data_quality::QualityPrecision::Metadata
    );
    assert_eq!(results.evaluated_rows, 0);
    assert_eq!(results.columns.len(), 3);
}

/// After a transform that invalidates `num_rows`, `spawn_async_collect` should
/// dispatch a background `len()` first (no UI thread blocking) and then chain
/// into the actual buffer collect. This test verifies the two-phase load
/// completes and yields a valid buffer.
#[test]
fn test_async_collect_handles_invalidated_num_rows() {
    use datui::filter_modal::{FilterOperator, FilterStatement, LogicalOperator};

    let test_data_dir = common::fixture_dir();
    let csv_path = test_data_dir.join("invalidated_num_rows_test.csv");
    let mut df = polars::df!(
        "id" => (0..500i64).collect::<Vec<_>>(),
        "value" => (0..500i64).map(|i| i * 2).collect::<Vec<_>>(),
    )
    .unwrap();
    let mut file = File::create(&csv_path).unwrap();
    CsvWriter::new(&mut file).finish(&mut df).unwrap();

    let terminal_area = Rect::new(0, 0, 80, 30);
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![csv_path], OpenOptions::default());

    // Render so visible_rows is set; settle the bounce.
    let mut buf = Buffer::empty(terminal_area);
    app.render(terminal_area, &mut buf);
    drain_events(&mut app, &rx);

    // Apply a filter via the public event. This invalidates num_rows.
    let filter = FilterStatement {
        columns: Vec::new(),
        column: "value".to_string(),
        operator: FilterOperator::Lt,
        value: "200".to_string(),
        logical_op: LogicalOperator::And,
    };
    app.event(AppEvent::Filter(vec![filter]));

    // Drain the count and the page.
    for _ in ticks() {
        let mut buf = Buffer::empty(terminal_area);
        app.render(terminal_area, &mut buf);
        app.frame_painted();
        let needs = app
            .data_table_state
            .as_mut()
            .map(|s| {
                let n = s.needs_recollect;
                s.needs_recollect = false;
                n
            })
            .unwrap_or(false);
        if needs {
            app.spawn_async_collect("Loading buffer...");
        }
        while let Ok(ev) = rx.try_recv() {
            if let Some(next) = app.event(ev) {
                let _ = tx.send(next);
            }
        }
        if !needs && !work_pending(&app) {
            break;
        }
        common::wait_for_event(&tx, &rx);
    }

    assert!(
        !app.is_busy(),
        "filter + async len + collect should complete"
    );
    let state = app.data_table_state.as_ref().unwrap();
    // value < 200 → ids 0..100 → 100 rows
    assert_eq!(state.num_rows(), 100, "filtered row count should be 100");
    assert!(
        state.display_slice_df().is_some(),
        "display buffer should be populated after async len + collect"
    );
}

/// Opening a Parquet hive directory should paint the first buffer and resolve the exact
/// total row count via the footer-sum path (Fix 1 + Fix 2), without a full data scan.
#[test]
fn test_hive_dir_loads_and_counts_via_footers() {
    let dir = tempfile::tempdir().unwrap();
    let mk = |sub: &str, n: i64| {
        let d = dir.path().join(sub);
        std::fs::create_dir_all(&d).unwrap();
        let mut df = df!("v" => (0..n).collect::<Vec<i64>>()).unwrap();
        let f = File::create(d.join("data.parquet")).unwrap();
        ParquetWriter::new(f).finish(&mut df).unwrap();
    };
    // Hive layout across two partition keys; 30 + 12 + 8 = 50 rows total.
    mk("form_type=a/year=2020", 30);
    mk("form_type=a/year=2021", 12);
    mk("form_type=b/year=2020", 8);

    let terminal_area = Rect::new(0, 0, 80, 30);
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    let opts = OpenOptions {
        hive: true,
        ..OpenOptions::default()
    };
    pump_open_until_loaded(&mut app, &rx, vec![dir.path().to_path_buf()], opts);

    // Drive render -> buffer collect -> background count to completion.
    let mut counted = false;
    for _ in ticks() {
        let mut buf = Buffer::empty(terminal_area);
        app.render(terminal_area, &mut buf);
        app.frame_painted();
        let needs = app
            .data_table_state
            .as_mut()
            .map(|s| {
                let n = s.needs_recollect;
                s.needs_recollect = false;
                n
            })
            .unwrap_or(false);
        if needs {
            app.spawn_async_collect("Loading buffer...");
        }
        while let Ok(ev) = rx.try_recv() {
            if let Some(next) = app.event(ev) {
                let _ = tx.send(next);
            }
        }
        counted = app
            .data_table_state
            .as_ref()
            .and_then(|s| s.num_rows_if_valid())
            == Some(50);
        if !app.is_busy() && !needs && counted {
            break;
        }
        common::wait_for_event(&tx, &rx);
    }

    assert!(counted, "exact total should resolve to the footer sum (50)");
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(
        state.num_rows(),
        50,
        "hive dir total should equal footer sum"
    );
    assert!(
        state.display_slice_df().is_some(),
        "first buffer should be populated"
    );
}

/// The open's worker, not the install, finds out that a hive path is a directory, so
/// the dataset arrives already counting by its footers (#457). Through the scan route:
/// the footer-union route counts by the files it listed instead (#710).
#[test]
fn test_hive_dir_is_known_from_the_open() {
    let dir = tempfile::tempdir().unwrap();
    let part = dir.path().join("year=2020");
    std::fs::create_dir_all(&part).unwrap();
    let mut df = df!("v" => [1i64, 2, 3]).unwrap();
    ParquetWriter::new(File::create(part.join("data.parquet")).unwrap())
        .finish(&mut df)
        .unwrap();

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    let opts = OpenOptions {
        hive: true,
        single_spine_schema: false,
        ..OpenOptions::default()
    };
    pump_open_until_loaded(&mut app, &rx, vec![dir.path().to_path_buf()], opts);
    assert_eq!(
        app.data_table_state.as_ref().unwrap().parquet_count_dir(),
        Some(dir.path().to_path_buf())
    );
}

/// A local Hive directory past one wave of footers opens from its two ends, reads the
/// rest behind, and joins them: the column only a middle file has arrives, and the
/// count comes from the same footers — every footer is read exactly once (#643).
#[test]
fn test_a_local_hive_past_one_wave_opens_from_its_ends_and_reads_each_footer_once() {
    let dir = tempfile::tempdir().unwrap();
    let (files, total) = write_past_one_wave(dir.path());
    let (first, last) = (files[0].clone(), files[files.len() - 1].clone());
    let (reads, _counting) = count_footer_reads(dir.path());
    // The middle footers wait until the test has seen the dataset open without them:
    // a slow filesystem, held still.
    let gate = std::sync::Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
    let held = gate.clone();
    let _holding = datui::formats::schema_union::on_local_footer_read(dir.path(), move |path| {
        if path == first || path == last {
            return;
        }
        let (open, cv) = &*held;
        let mut open = open.lock().unwrap();
        while !*open {
            open = cv.wait(open).unwrap();
        }
    });

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    let opts = OpenOptions {
        hive: true,
        ..OpenOptions::default()
    };
    let mut next = Some(AppEvent::Open(vec![dir.path().to_path_buf()], opts));
    loop {
        if let Some(event) = next.take() {
            next = app.event(event);
            continue;
        }
        let opened = app
            .data_table_state
            .as_ref()
            .is_some_and(|s| s.footers_pending().is_some());
        if opened && !app.is_busy() {
            break;
        }
        next = next_event(&app, &rx);
        assert!(
            next.is_some() || opened,
            "the open stopped before its dataset"
        );
    }
    let state = app.data_table_state.as_ref().unwrap();
    assert!(
        !state.schema().contains("late"),
        "the dataset opened from its ends, before the middle footers were read"
    );
    assert_eq!(
        state.num_rows_if_valid(),
        None,
        "and waits for the pass to bring its count"
    );
    {
        let reads = reads.lock().unwrap();
        assert_eq!(reads.get(&files[0]), Some(&1), "the first file opened it");
        assert_eq!(reads.get(&files[files.len() - 1]), Some(&1), "and the last");
    }

    {
        let (open, cv) = &*gate;
        *open.lock().unwrap() = true;
        cv.notify_all();
    }
    drain_events(&mut app, &rx);

    let state = app.data_table_state.as_ref().unwrap();
    assert!(state.footers_pending().is_none(), "the rest joined");
    assert!(
        state.schema().contains("late"),
        "bringing the column only a middle file has"
    );
    assert_eq!(
        state.files_a_page_reads(0, 1),
        Some(1),
        "and a page now reads only the files holding its rows (#659)"
    );
    assert_eq!(
        state.num_rows_if_valid(),
        Some(total),
        "and the count, from the footers the pass read"
    );
    let reads = reads.lock().unwrap();
    assert_eq!(reads.len(), files.len(), "every footer was read");
    assert!(
        reads.values().all(|&n| n == 1),
        "each once: the ends are not read again, and the count is no second pass"
    );
}

/// A local Hive directory whose listing has not changed opens from the footers its
/// last full pass read, and reads none (#643). A file rewritten is a new listing.
#[test]
fn test_a_local_hive_reopened_unchanged_reads_no_footers() {
    let dir = tempfile::tempdir().unwrap();
    let (files, total) = write_past_one_wave(dir.path());
    let (reads, _counting) = count_footer_reads(dir.path());
    let (mut app, rx, _tx) = open_local_dataset_with_channel(dir.path());
    assert_eq!(
        reads.lock().unwrap().len(),
        files.len(),
        "the first open read every footer"
    );
    reads.lock().unwrap().clear();

    let opts = || OpenOptions {
        hive: true,
        ..OpenOptions::default()
    };
    pump_open_until_loaded(&mut app, &rx, vec![dir.path().to_path_buf()], opts());
    let state = app.data_table_state.as_ref().unwrap();
    assert!(
        reads.lock().unwrap().is_empty(),
        "a reopen of the same listing reads no footer"
    );
    assert!(state.footers_pending().is_none(), "and opens whole");
    assert!(state.schema().contains("late"), "with every file's columns");
    assert_eq!(state.num_rows_if_valid(), Some(total), "and its count");

    // One more row in the newest file: a different size, so a different listing.
    write_parquet(
        dir.path(),
        &format!("day={:03}", files.len() - 1),
        df!("v" => (0..files.len() as i64 + 1).collect::<Vec<_>>()).unwrap(),
    );
    pump_open_until_loaded(&mut app, &rx, vec![dir.path().to_path_buf()], opts());
    assert!(
        !reads.lock().unwrap().is_empty(),
        "a changed listing reads its footers again"
    );
    assert_eq!(
        app.data_table_state.as_ref().unwrap().num_rows_if_valid(),
        Some(total + 1),
        "and counts the row that was added"
    );
}

/// A local Hive directory past the footer sample (#710): the pass behind the open reads
/// a sample, the count reads only the footers it skipped, each footer is read once in
/// all, the shape is kept, and a reopen reads none. The files are one small Parquet
/// file written 25,000 times.
#[test]
fn test_a_local_hive_past_the_sample_reads_each_footer_once_and_reopens_reading_none() {
    let dir = tempfile::tempdir().unwrap();
    let files = datui::formats::schema_union::MAX_FOOTER_READS + 5_000;
    let mut bytes = Vec::new();
    ParquetWriter::new(&mut bytes)
        .finish(&mut df!("v" => [1i64, 2, 3]).unwrap())
        .unwrap();
    for i in 0..files {
        let sub = dir.path().join(format!("part={:02}", i / 1_000));
        if i % 1_000 == 0 {
            std::fs::create_dir_all(&sub).unwrap();
        }
        std::fs::write(sub.join(format!("f{i:05}.parquet")), &bytes).unwrap();
    }
    let (reads, _counting) = count_footer_reads(dir.path());

    let (mut app, rx, _tx) = open_local_dataset_with_channel(dir.path());
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.num_rows_if_valid(), Some(files * 3), "counted");
    assert_eq!(
        state.schema().get("part"),
        Some(&polars::prelude::DataType::Int64),
        "with its partition column"
    );
    {
        let reads = reads.lock().unwrap();
        assert_eq!(reads.len(), files, "every footer was read");
        assert!(
            reads.values().all(|&n| n == 1),
            "each once: the count read only what the sample skipped"
        );
    }
    reads.lock().unwrap().clear();

    let opts = OpenOptions {
        hive: true,
        ..OpenOptions::default()
    };
    pump_open_until_loaded(&mut app, &rx, vec![dir.path().to_path_buf()], opts);
    let state = app.data_table_state.as_ref().unwrap();
    assert!(
        reads.lock().unwrap().is_empty(),
        "the shape was kept, so a reopen reads no footer"
    );
    assert!(state.footers_pending().is_none(), "and opens whole");
    assert_eq!(state.num_rows_if_valid(), Some(files * 3), "and counted");
}

/// A local Hive directory reopened from its remembered footers reads its first page
/// from the files holding those rows and no others (#659). Every eighth file stores
/// `v` as text, which splits the scan into runs, and a scan of the whole dataset reads
/// ahead into them; every eighth file but one is empty, which a window passes over.
#[cfg(target_os = "linux")]
#[test]
fn test_a_local_hive_reopened_unchanged_pages_from_only_the_files_holding_its_rows() {
    let dir = tempfile::tempdir().unwrap();
    let (days, rows) = (datui::formats::schema_union::FOOTERS_AT_ONCE + 6, 50);
    let mut files = Vec::new();
    for day in 0..days {
        let v: Vec<i64> = (0..rows).map(|r| (day * rows + r) as i64).collect();
        let df = match day % 8 {
            1 => df!("v" => v.iter().map(i64::to_string).collect::<Vec<_>>()).unwrap(),
            2 => df!("v" => Vec::<i64>::new()).unwrap(),
            _ => df!("v" => &v).unwrap(),
        };
        let sub = format!("day={day:03}");
        write_parquet(dir.path(), &sub, df);
        files.push(dir.path().join(sub).join("data.parquet"));
    }
    // The first open reads every footer and remembers them.
    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());

    let watch = OpenWatch::new(&files);
    let opts = OpenOptions {
        hive: true,
        ..OpenOptions::default()
    };
    pump_open_until_loaded(&mut app, &rx, vec![dir.path().to_path_buf()], opts);
    let screen = painted(&mut app, &rx, &tx, Rect::new(0, 0, 100, 30));
    let state = app.data_table_state.as_ref().unwrap();
    let total = (0..days).filter(|day| day % 8 != 2).count() * rows;
    assert_eq!(state.num_rows_if_valid(), Some(total));
    let df = state
        .display_slice_df()
        .unwrap_or_else(|| panic!("rows on screen: {screen}"));
    assert_eq!(
        df.column("v").unwrap().get(0).unwrap(),
        AnyValue::Int64(0),
        "the first page is the first file's"
    );
    assert_eq!(
        df.column("day").unwrap().get(0).unwrap(),
        AnyValue::Int64(0),
        "partition values and all"
    );
    // The buffer reaches a few pages on, so a few files; never the dataset.
    let buffered = state.buffered_rows();
    let holding: std::collections::BTreeSet<PathBuf> = files
        .iter()
        .enumerate()
        .filter(|(day, _)| day % 8 != 2)
        .take(buffered.div_ceil(rows))
        .map(|(_, file)| file.clone())
        .collect();
    let opened = watch.opened();
    assert!(!opened.is_empty(), "the page was read from the files");
    assert!(
        opened.is_subset(&holding),
        "only the files holding the buffer's {buffered} rows were opened: {opened:?}"
    );
}

/// A file mid-write in a local Hive directory past one wave — the newest, where a
/// writer is, and one in the middle — leaves the rest of the dataset to open and
/// count (#643).
#[test]
fn test_a_local_hive_past_one_wave_opens_with_files_mid_write() {
    let dir = tempfile::tempdir().unwrap();
    let (files, total) = write_past_one_wave(dir.path());
    let middle = &files[40];
    let newest = dir.path().join("day=999");
    std::fs::create_dir_all(&newest).unwrap();
    // A writer's first bytes, and no footer yet.
    std::fs::write(newest.join("data.parquet"), b"PAR1\x15\x04").unwrap();
    std::fs::write(middle, b"PAR1").unwrap();

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let screen = painted(&mut app, &rx, &tx, Rect::new(0, 0, 100, 30));
    let state = app
        .data_table_state
        .as_ref()
        .expect("the dataset opened past the files mid-write");
    assert!(state.footers_pending().is_none(), "and the rest joined");
    assert_eq!(
        state.num_rows_if_valid(),
        Some(total - 41),
        "counting every file but the two that will not read"
    );
    assert!(
        state.display_slice_df().is_some(),
        "with rows on screen: {screen}"
    );
}

/// Three kinds of empty cell that used to look identical: a null the data holds, a
/// column the file was written without, and a column the file stores as text while the
/// dataset reads it as a number.
#[test]
fn test_absent_null_and_conflicting_cells_differ_on_screen() {
    let g = datui::glyphs::get();
    let dir = tempfile::tempdir().unwrap();
    // File one has `note` and a real null in it; it has no `extra` at all.
    write_parquet(
        dir.path(),
        "date=2024-01-01",
        df!("id" => &[1i64], "note" => &[None::<&str>], "n" => &[10i64]).unwrap(),
    );
    // File two has every column, and stores `n` as text, which the dataset reads as
    // the integer that most of its rows are.
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[2i64], "note" => &["hi"], "extra" => &["x"], "n" => &["oops"]).unwrap(),
    );
    write_parquet(
        dir.path(),
        "date=2024-01-03",
        df!("id" => &[3i64], "note" => &["yo"], "extra" => &["y"], "n" => &[30i64]).unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let area = Rect::new(0, 0, 100, 20);
    let text = painted(&mut app, &rx, &tx, area);

    assert!(
        text.contains(g.absent),
        "a file written without `extra` should show the absent glyph {:?}, got:\n{}",
        g.absent,
        text
    );
    assert!(
        text.contains(g.null),
        "the real null in `note` should still show the null glyph {:?}",
        g.null
    );
    assert!(
        text.contains(g.conflict),
        "the file storing `n` as text should show the conflict glyph {:?}",
        g.conflict
    );
    assert!(
        text.contains(&format!("extra{}", g.drift_mark)),
        "and `extra` is marked in the header as not being in every file"
    );
    assert!(
        !text.contains(&format!("id{}", g.drift_mark)),
        "while `id`, which every file has, is not"
    );
}

/// The accent reaches the bar from the dataset, and the config can turn it off.
///
/// `render/footer.rs` proves the accent is only a colour on the Info chip, but it is handed
/// a flag by hand; the app-side tests read `notes_unseen()`, an accessor. Nothing
/// joined the two, so an accent that never reached the bar — or one that ignored the
/// config — passed both.
#[test]
fn test_the_notes_accent_reaches_the_footer_and_the_config_can_stop_it() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(dir.path(), "date=2024-01-01", df!("id" => &[1i64]).unwrap());
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[2i64], "extra" => &["x"]).unwrap(),
    );

    let area = Rect::new(0, 0, 120, 24);
    let bar_of = |app: &mut App| -> (Vec<ratatui::style::Color>, String) {
        let mut buf = Buffer::empty(area);
        app.render(area, &mut buf);
        let row = area.height - 1;
        (
            (0..area.width).map(|x| buf[(x, row)].fg).collect(),
            (0..area.width)
                .map(|x| buf[(x, row)].symbol().to_string())
                .collect(),
        )
    };

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let _ = painted(&mut app, &rx, &tx, area);
    assert!(
        app.data_table_state.as_ref().unwrap().notes_unseen(),
        "the directories disagree, so there is a note and it has not been read"
    );
    let (accented, accented_text) = bar_of(&mut app);

    // Opening the panel clears it, and the bar goes back to its ordinary colours.
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Char('i'),
        KeyModifiers::NONE,
    )));
    let _ = painted(&mut app, &rx, &tx, area);
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Esc,
        KeyModifiers::NONE,
    )));
    let (plain, plain_text) = bar_of(&mut app);
    // The footer offers `i Notes`, in the accent, until the notes are read.
    assert!(accented_text.contains("i Notes"), "{accented_text:?}");
    assert!(!plain_text.contains("Notes"), "{plain_text:?}");
    let at = accented_text.find("Notes").unwrap();
    let at = accented_text[..at].chars().count();
    assert_ne!(
        accented[at],
        plain[at.min(plain.len() - 1)],
        "in the accent"
    );

    // And a user who does not want it never sees it, however many notes there are.
    let mut config = datui::config::AppConfig::default();
    config.display.notes_accent = false;
    let (tx2, rx2) = mpsc::channel();
    let mut off = App::new_with_config(
        tx2.clone(),
        common::test_runtime(),
        datui::config::Theme::from_config(&datui::config::AppConfig::default().theme)
            .expect("the default theme"),
        config,
    );
    pump_open_until_loaded(
        &mut off,
        &rx2,
        vec![dir.path().to_path_buf()],
        OpenOptions {
            hive: true,
            ..OpenOptions::default()
        },
    );
    let _ = painted(&mut off, &rx2, &tx2, area);
    assert!(
        off.data_table_state.as_ref().unwrap().notes_unseen(),
        "there is still a note to accent"
    );
    let (_, unaccented_text) = bar_of(&mut off);
    assert!(
        !unaccented_text.contains("Notes"),
        "the footer offers nothing: {unaccented_text:?}"
    );
}

/// The very first frame must mark the absent cells too.
///
/// Every other test here paints through `painted`, which renders up to sixty times, so
/// a mark that only arrives on the second frame looks identical to one that was always
/// there. In the app there is no second frame until something happens: datui draws the
/// dataset and waits. So this renders exactly once, into a fresh buffer, and reads the
/// glyph off it.
#[test]
fn test_the_first_frame_of_a_drifting_dataset_marks_its_absent_cells() {
    let g = datui::glyphs::get();
    let dir = tempfile::tempdir().unwrap();
    write_parquet(dir.path(), "date=2024-01-01", df!("id" => &[1i64]).unwrap());
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[2i64], "extra" => &["x"]).unwrap(),
    );

    let mut app = open_local_dataset(dir.path());
    let area = Rect::new(0, 0, 80, 12);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let screen: String = (0..area.height)
        .flat_map(|y| {
            (0..area.width)
                .map(move |x| (x, y))
                .map(|(x, y)| buf[(x, y)].symbol().to_string())
                .chain(std::iter::once("\n".to_string()))
        })
        .collect();

    assert!(
        screen.contains(g.absent),
        "the row from the file without `extra` is absent, not null, on the first \
         frame as much as the second:\n{screen}"
    );
}

/// Pins the row arithmetic that everything else rests on.
///
/// The scan numbers each run's rows from where that run's first file begins in the
/// dataset. Every other fixture here has files of one or two rows, which makes a run's
/// starting row and its file's *index* the same number — so using one for the other
/// would go unnoticed. These files hold 3, 5 and 2 rows, and the third conflicts, which
/// splits the scan after row 8.
#[test]
fn test_each_row_takes_its_glyph_from_the_file_it_came_from() {
    let dir = tempfile::tempdir().unwrap();
    // No `n` at all: its cells are absent.
    write_parquet(
        dir.path(),
        "date=2024-01-01",
        df!("id" => &[0i64, 1, 2]).unwrap(),
    );
    // `n` as an integer, and the most rows, so the dataset reads it as one.
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[3i64, 4, 5, 6, 7], "n" => &[30i64, 40, 50, 60, 70]).unwrap(),
    );
    // `n` as text: it cannot be read from here, so its cells conflict.
    write_parquet(
        dir.path(),
        "date=2024-01-03",
        df!("id" => &[8i64, 9], "n" => &["x", "y"]).unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let area = Rect::new(0, 0, 100, 24);
    let _ = painted(&mut app, &rx, &tx, area);

    let state = app.data_table_state.as_ref().unwrap();
    let dataset = state.dataset_schema().expect("read from the footers");
    let (first, middle, last) = (
        dataset.file_group[0],
        dataset.file_group[1],
        dataset.file_group[2],
    );
    assert_eq!(middle, 0, "the middle file is missing nothing");
    assert_ne!(first, middle, "the first file has no `n`");
    assert_ne!(last, middle, "the last file holds `n` as text");

    let groups = state.display_drift(area.height as usize);
    assert_eq!(
        groups,
        vec![
            first, first, first, middle, middle, middle, middle, middle, last, last
        ],
        "three rows from the first file, five from the second, two from the third"
    );

    // Scrolled, and to a row inside the middle file rather than onto a boundary: the
    // window starts where the view does, so the groups have to shift with it.
    let state = app.data_table_state.as_mut().unwrap();
    state.scroll_to(4);
    common::read_rows(&mut app, &rx);
    let state = app.data_table_state.as_mut().unwrap();
    assert_eq!(
        state.display_drift(6),
        vec![middle, middle, middle, middle, last, last],
        "from row 4: four more rows of the second file, then the third"
    );
}

/// Files in the directory that are not Parquet are counted, so a silent drop is not one.
///
/// A `.csv` sitting in a directory of Parquet is a file somebody thought was in the
/// table. datui reads none of it and, until now, said nothing at all about it — which is
/// the shape of problem this whole issue is about.
#[test]
fn test_files_that_are_not_parquet_are_counted_rather_than_dropped_in_silence() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(dir.path(), "date=2024-01-01", df!("id" => &[1i64]).unwrap());
    std::fs::write(
        dir.path().join("date=2024-01-01/extra.csv"),
        "id
1
",
    )
    .unwrap();
    std::fs::write(dir.path().join("notes.txt"), "read me").unwrap();
    // A writer's bookkeeping, which is not a file anyone meant as data.
    std::fs::write(dir.path().join("_SUCCESS"), "").unwrap();

    let app = open_local_dataset(dir.path());
    let state = app.data_table_state.as_ref().unwrap();
    let notes = state.notes();
    let skipped = notes
        .iter()
        .find(|note| note.summary.contains("not Parquet"))
        .unwrap_or_else(|| panic!("no note about the files that were not read: {notes:#?}"));
    assert_eq!(
        skipped.summary,
        "skipped: 2 files not Parquet, 1 writer bookkeeping file"
    );
    assert_eq!(skipped.scope, "in this directory's listing");
}

/// A flat mixed directory's read is reported once, not by the open and again by the
/// footer walk.
///
/// Both count the same stray: the open says "read as the commonest; 1 csv not read"
/// and the walk behind the footers said "in the directory, 1 file is not Parquet"
/// right under it — the same fact twice, in two wordings. The open's sentence names
/// the format and says why, so it is the one kept.
#[test]
fn test_a_mixed_parquet_directory_says_what_it_left_out_once() {
    let dir = tempfile::tempdir().unwrap();
    for name in ["a.parquet", "b.parquet"] {
        let f = File::create(dir.path().join(name)).unwrap();
        ParquetWriter::new(f)
            .finish(&mut df!("id" => &[1i64]).unwrap())
            .unwrap();
    }
    std::fs::write(dir.path().join("extra.csv"), "id\n1\n").unwrap();

    let app = open_local_dataset(dir.path());
    let state = app.data_table_state.as_ref().unwrap();
    let about: Vec<String> = state
        .notes()
        .iter()
        .filter(|n| n.summary.contains("not read") || n.summary.contains("not Parquet"))
        .map(|n| n.summary.clone())
        .collect();
    assert_eq!(
        about,
        [concat!(
            "mixed formats, read as the commonest: ",
            "1 csv not read"
        )],
        "one fact, said once"
    );
}

/// And a directory holding only what a writer leaves behind says nothing.
///
/// `_SUCCESS` beside the data is a job reporting that it finished. A note about it on
/// every directory any job ever wrote would put an accent on the Info key for the most
/// ordinary thing a directory can contain.
#[test]
fn test_a_writers_own_bookkeeping_is_not_worth_a_note() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(dir.path(), "date=2024-01-01", df!("id" => &[1i64]).unwrap());
    std::fs::write(dir.path().join("_SUCCESS"), "").unwrap();
    std::fs::write(dir.path().join("date=2024-01-01/.data.parquet.crc"), "").unwrap();
    // A table format's own log. Everything in here belongs to the writer, whatever it
    // is called — a directory of three hundred commits is six hundred files, and counting
    // them as somebody's mistake would put an accent on the Info key for the most
    // ordinary thing a directory of Parquet can be.
    std::fs::create_dir_all(dir.path().join("_delta_log")).unwrap();
    for commit in 0..5 {
        std::fs::write(
            dir.path().join(format!("_delta_log/{commit:020}.json")),
            "{}",
        )
        .unwrap();
        std::fs::write(
            dir.path()
                .join(format!("_delta_log/.{commit:020}.json.crc")),
            "",
        )
        .unwrap();
    }

    let app = open_local_dataset(dir.path());
    let state = app.data_table_state.as_ref().unwrap();
    assert!(
        !state
            .notes()
            .iter()
            .any(|note| note.summary.contains("not Parquet")),
        "nothing to say: {:#?}",
        state.notes()
    );
}

/// A table format's own Parquet is not the table's rows.
///
/// Delta writes its checkpoints as Parquet inside `_delta_log/`, with the table's own
/// columns among its own. Read as data they are extra rows in a table that does not
/// have them and columns nobody asked for — a wrong row count, silently. This is about
/// what the table *contains*, not about what a note says.
#[test]
fn test_a_table_formats_own_parquet_is_not_part_of_the_table() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(dir.path(), "date=2024-01-01", df!("id" => &[1i64]).unwrap());
    // A checkpoint, which is Parquet and is not the table.
    write_parquet(
        dir.path(),
        "_delta_log",
        df!("id" => &[99i64], "txn" => &["commit"]).unwrap(),
    );

    let app = open_local_dataset(dir.path());
    let state = app.data_table_state.as_ref().unwrap();
    let names: Vec<&str> = state.schema().iter_names().map(|n| n.as_str()).collect();
    assert_eq!(
        names,
        ["date", "id"],
        "the checkpoint's own columns are not the table's"
    );
    let ids: Vec<i64> = state
        .lf()
        .clone()
        .collect()
        .expect("the table reads")
        .column("id")
        .unwrap()
        .i64()
        .unwrap()
        .into_no_null_iter()
        .collect();
    assert_eq!(ids, [1], "and its rows are not the table's rows");
}

/// The same rule on disk as in a bucket: a directory with no data in it is nobody's
/// table.
///
/// The cloud half of this has a test; the local half had none, and a mutant that made
/// the rule never fire survived the whole suite. Iceberg keeps its log in a plain
/// `metadata/` — no underscore, no dot — so the name convention alone reads a table's
/// own files as somebody's mistakes.
#[test]
fn test_a_local_directory_with_no_data_in_it_is_nobodys_table() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(dir.path(), "data/date=1", df!("id" => &[1i64]).unwrap());
    std::fs::create_dir_all(dir.path().join("metadata")).unwrap();
    for name in ["v1.metadata.json", "v2.metadata.json", "snap-123.avro"] {
        std::fs::write(dir.path().join("metadata").join(name), "{}").unwrap();
    }
    // And a day that landed as CSV, which is part of the dataset and is a mistake.
    std::fs::create_dir_all(dir.path().join("data/date=2")).unwrap();
    std::fs::write(dir.path().join("data/date=2/part-0.csv"), "id\n2\n").unwrap();

    let app = open_local_dataset(dir.path());
    let state = app.data_table_state.as_ref().unwrap();
    let notes = state.notes();
    let about = notes
        .iter()
        .find(|note| note.summary.starts_with("skipped:"))
        .unwrap_or_else(|| panic!("no note about what was not read: {notes:#?}"));
    assert_eq!(
        about.summary, "skipped: 1 file not Parquet, 3 writer bookkeeping files",
        "the csv in the partition that has no data of its own, and Iceberg's three"
    );
}

/// The offer in the Notes tab, taken: the values a type conflict hid appear on screen.
#[test]
fn test_the_notes_tab_offers_to_read_a_conflicting_column_as_text() {
    let g = datui::glyphs::get();
    let dir = tempfile::tempdir().unwrap();
    write_parquet(
        dir.path(),
        "date=2024-01-01",
        df!("id" => &[0i64, 1], "n" => &[10i64, 20]).unwrap(),
    );
    // `n` as text here, so it is not read from this file at all.
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[2i64], "n" => &["sixty"]).unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let area = Rect::new(0, 0, 100, 24);
    let before = painted(&mut app, &rx, &tx, area);
    assert!(
        before.contains(g.conflict),
        "the row whose file stores `n` as text is a conflict to begin with"
    );
    assert!(
        !before.contains("sixty"),
        "and its value cannot be seen: {before}"
    );

    // Open the Info panel and walk to the Notes tab.
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Char('i'),
        KeyModifiers::NONE,
    )));
    // Walked by key rather than by setting the tab, so the keys the user presses are
    // the ones under test. Tab first: the panel opens on the body, where the arrows
    // move the schema table rather than the tab bar. The conflict note's own words say
    // when we have arrived.
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Tab,
        KeyModifiers::NONE,
    )));
    let mut panel = painted(&mut app, &rx, &tx, area);
    for _ in 0..6 {
        if panel.contains("and not read there") {
            break;
        }
        app.event(AppEvent::Key(KeyEvent::new(
            KeyCode::Right,
            KeyModifiers::NONE,
        )));
        panel = painted(&mut app, &rx, &tx, area);
    }
    assert!(
        panel.contains("and not read there"),
        "the Notes tab, showing the conflict note: {panel}"
    );

    // Walk to the note that carries the offer, and take it.
    let offered = |app: &App| -> Option<usize> {
        app.data_table_state
            .as_ref()
            .unwrap()
            .notes()
            .iter()
            .position(|note| note.read_as_text.is_some())
    };
    let at = offered(&app).expect("the conflict note offers to read the column as text");
    for _ in 0..at {
        app.event(AppEvent::Key(KeyEvent::new(
            KeyCode::Down,
            KeyModifiers::NONE,
        )));
    }
    let panel = painted(&mut app, &rx, &tx, area);
    assert!(
        panel.contains("Enter  read n as text"),
        "the panel says the offer is there: {panel}"
    );

    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Esc,
        KeyModifiers::NONE,
    )));
    let after = painted(&mut app, &rx, &tx, area);

    assert!(
        after.contains("sixty"),
        "the value the conflict hid is on screen: {after}"
    );
    assert!(
        after.contains("10") && after.contains("20"),
        "and so are the ones that were always readable: {after}"
    );
    assert!(
        !after.contains(g.conflict),
        "nothing conflicts any more: {after}"
    );

    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(
        state.read_as_text(),
        [polars::prelude::PlSmallStr::from("n")],
        "and the state says which column it is reading that way"
    );
    assert!(
        !state.notes().iter().any(|note| note.read_as_text.is_some()),
        "the offer is gone, having been taken: {:#?}",
        state.notes()
    );
}

/// The offer and the count of notes out of view share the last row without landing on
/// top of each other.
///
/// Both are drawn into the panel's bottom row. A `Paragraph` leaves the cells its text
/// does not reach alone, so two of them in one place is not a layout that loses — it is
/// one string written over another.
#[test]
fn test_the_offer_and_the_hidden_count_do_not_overwrite_each_other() {
    let dir = tempfile::tempdir().unwrap();
    // Several drifting columns, so there are more notes than a short panel can show.
    write_parquet(
        dir.path(),
        "date=2024-01-01",
        df!(
            "id" => &[0i64, 1],
            "measurement_value" => &[10i64, 20],
            "b" => &[1i64, 2],
            "c" => &[1i64, 2],
            "d" => &[1i64, 2],
        )
        .unwrap(),
    );
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!(
            "id" => &[2i64],
            "measurement_value" => &["sixty"],
            "b" => &["x"],
            "c" => &["x"],
            "d" => &["x"],
        )
        .unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    // Narrow, and short enough that the notes do not all fit: both halves of the last
    // row have something to say, and not enough room to say it in. 74 wide because
    // the sidebar clamp leaves the table 30 columns, so the panel itself gets the
    // 44 this test is about.
    let area = Rect::new(0, 0, 74, 12);
    let _ = painted(&mut app, &rx, &tx, area);
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Char('i'),
        KeyModifiers::NONE,
    )));
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Tab,
        KeyModifiers::NONE,
    )));
    let mut panel = painted(&mut app, &rx, &tx, area);
    for _ in 0..6 {
        if panel.contains("and not read there") {
            break;
        }
        app.event(AppEvent::Key(KeyEvent::new(
            KeyCode::Right,
            KeyModifiers::NONE,
        )));
        panel = painted(&mut app, &rx, &tx, area);
    }

    // Walk to a note carrying the offer, so the panel has both things to say.
    let offered = |app: &App| -> Option<usize> {
        app.data_table_state
            .as_ref()
            .unwrap()
            .notes()
            .iter()
            .position(|note| note.read_as_text.is_some())
    };
    let at = offered(&app).expect("a conflict note offers to read its column as text");
    for _ in 0..at {
        app.event(AppEvent::Key(KeyEvent::new(
            KeyCode::Down,
            KeyModifiers::NONE,
        )));
    }
    let panel = painted(&mut app, &rx, &tx, area);

    let count = (1..9)
        .flat_map(|n| [format!("{n} below"), format!("{n} above")])
        .find(|text| panel.contains(text.as_str()))
        .unwrap_or_else(|| panic!("the panel is short enough to be hiding notes: {panel}"));
    assert!(
        panel.contains("Enter  read"),
        "the offer shares the row with the count: {panel}"
    );
    // The blank column between them is the whole of it. Drawn into the same rect, the
    // count lands on the offer's last characters and there is no gap — the offer's
    // text runs straight into "2 below" with no way to tell where one ends.
    assert!(
        panel.contains(&format!(" {count}")),
        "the two must not run together where they meet: {panel}"
    );
}

/// Taking the offer from the Info panel rebuilds the scan on the UI thread and reads
/// its rows in the background, like any other change to the view (#458).
#[test]
fn test_reading_a_column_as_text_from_the_panel_reads_in_the_background() {
    use datui::widgets::info::InfoTab;

    let dir = tempfile::tempdir().unwrap();
    write_parquet(
        dir.path(),
        "date=2024-01-01",
        df!("id" => &[0i64, 1, 2], "n" => &[1i64, 10, 20]).unwrap(),
    );
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[3i64], "n" => &["sixty"]).unwrap(),
    );
    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let area = Rect::new(0, 0, 100, 24);
    let _ = painted(&mut app, &rx, &tx, area);

    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Char('i'),
        KeyModifiers::NONE,
    )));
    assert_eq!(app.info_modal.active_tab, InfoTab::Notes);
    let state = app.data_table_state.as_ref().unwrap();
    app.info_modal.notes_selected_index = state
        .notes()
        .iter()
        .position(|note| note.read_as_text.is_some())
        .expect("the conflict note offers to read n as text");
    assert!(state.is_num_rows_valid());
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));

    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.schema().get("n"), Some(&DataType::String));
    // Nothing is counted on this thread: the count is the footers' from the open, and
    // reading a column as text changes no rows.
    assert_eq!(state.num_rows_if_valid(), Some(4));
    assert!(app.is_busy(), "its rows are being read");
    drain_events(&mut app, &rx);
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.num_rows_if_valid(), Some(4));
    let shown = state.display_df().expect("the rows are read");
    assert_eq!(shown.column("n").unwrap().dtype(), &DataType::String);
}

/// Reading a column as text does not undo the widening, so the note about it stays.
///
/// One file wrote `n` as an integer and another as a float, which widen together — so
/// the column is read as a float and the integer file's `7` shows as `7.0`, text read
/// or not. Only the types that *conflict* are read at their own type. The note that
/// explains the `7.0` is the widening note, and an earlier version of this deleted it.
#[test]
fn test_reading_as_text_keeps_the_note_about_a_widened_type() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(
        dir.path(),
        "date=2024-01-01",
        df!("id" => &[0i64], "n" => &[7i64]).unwrap(),
    );
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[1i64, 2], "n" => &[1.5f64, 2.5]).unwrap(),
    );
    write_parquet(
        dir.path(),
        "date=2024-01-03",
        df!("id" => &[3i64], "n" => &["sixty"]).unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let area = Rect::new(0, 0, 100, 24);
    let _ = painted(&mut app, &rx, &tx, area);

    let state = app.data_table_state.as_mut().unwrap();
    let widening = "n is stored as more than one type";
    assert!(
        state
            .notes()
            .iter()
            .any(|n| n.summary.starts_with(widening)),
        "the integer and the float widened together to begin with"
    );
    assert!(
        state.read_column_as_text("n").unwrap(),
        "the offer is taken"
    );

    let state = app.data_table_state.as_ref().unwrap();
    let text: Vec<String> = state
        .lf()
        .clone()
        .collect()
        .unwrap()
        .column("n")
        .unwrap()
        .str()
        .unwrap()
        .iter()
        .map(|value| value.unwrap_or("null").to_string())
        .collect();
    assert_eq!(
        text,
        ["7.0", "1.5", "2.5", "sixty"],
        "the file that wrote 7 still reads 7.0: widening is not what the text read undoes"
    );
    let notes = state.notes();
    assert!(
        notes.iter().any(|n| n.summary.starts_with(widening)),
        "so the note explaining that 7.0 has to stay: {notes:#?}"
    );
}

/// A partition written for a day nothing happened holds no rows, and datui says so.
///
/// Worth saying because the dataset then has fewer days of data than it has directories,
/// and the file is invisible in every other way: it adds no rows, changes no schema,
/// and moves nothing on screen.
#[test]
fn test_a_partition_that_holds_no_rows_is_worth_a_note() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(
        dir.path(),
        "date=2024-01-01",
        df!("id" => &[0i64, 1], "n" => &[10i64, 20]).unwrap(),
    );
    // A day nothing happened: the directory is there, the file is there, the rows are
    // not.
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => Vec::<i64>::new(), "n" => Vec::<i64>::new()).unwrap(),
    );
    write_parquet(
        dir.path(),
        "date=2024-01-03",
        df!("id" => &[2i64], "n" => &[30i64]).unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let area = Rect::new(0, 0, 100, 24);
    let _ = painted(&mut app, &rx, &tx, area);

    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(current_rows(&app), 3, "the empty day adds nothing");
    let notes = state.notes();
    let empty = notes
        .iter()
        .find(|note| note.summary.contains("no rows"))
        .unwrap_or_else(|| panic!("nothing said about the empty day: {notes:#?}"));
    assert_eq!(empty.summary, "1 file holds no rows");
    assert_eq!(empty.scope, "in all 3 footers");
}

/// The control: every file holding rows says nothing.
#[test]
fn test_a_dataset_whose_files_all_hold_rows_says_nothing_about_empty_ones() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(
        dir.path(),
        "date=2024-01-01",
        df!("id" => &[0i64], "n" => &[10i64]).unwrap(),
    );
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[1i64], "n" => &[20i64]).unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let area = Rect::new(0, 0, 100, 24);
    let _ = painted(&mut app, &rx, &tx, area);

    let state = app.data_table_state.as_ref().unwrap();
    assert!(
        !state.notes().iter().any(|n| n.summary.contains("no rows")),
        "{:#?}",
        state.notes()
    );
}

/// A pipeline that renamed its partition key partway through.
///
/// The note says the shape and claims nothing about what it costs. What it costs varies:
/// this directory does not open at all, because the scan reads its partition columns off
/// one branch of the tree and the files under the other key fail it — but which branch
/// wins is whatever the filesystem hands back first, so this asserts that *something*
/// went wrong rather than which key won. An earlier version asserted the key, passed
/// here and failed on CI.
#[test]
fn test_a_directory_whose_partition_key_changed_says_the_directories_differ() {
    let dir = tempfile::tempdir().unwrap();
    for day in ["date=2024-01-01", "date=2024-01-02", "date=2024-01-03"] {
        write_parquet(
            dir.path(),
            day,
            df!("id" => &[0i64], "n" => &[1i64]).unwrap(),
        );
    }
    // The day the pipeline changed.
    write_parquet(
        dir.path(),
        "dt=2024-01-04",
        df!("id" => &[1i64], "n" => &[2i64]).unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let area = Rect::new(0, 0, 100, 24);
    let frame = painted(&mut app, &rx, &tx, area);
    assert!(
        frame.contains("Schema field not found"),
        "a renamed key stops this directory opening, whichever key the scan took — the \
         note exists to explain a screen like this one:\n{frame}"
    );

    let state = app.data_table_state.as_ref().unwrap();
    let notes = state.notes();
    let layout = notes
        .iter()
        .find(|note| note.summary.contains("mixed partition keys"))
        .unwrap_or_else(|| panic!("nothing said about the changed key: {notes:#?}"));
    assert_eq!(
        layout.summary,
        "mixed partition keys: 3 files by date, 1 file by dt"
    );
    assert_eq!(layout.scope, "in the names of 4 files");
}

/// The very same disagreement, and this one opens.
///
/// What decides it is not which branch the scan reads by — it is which file name sorts
/// first. The paths are handed to Polars sorted and it takes the hive schema from the
/// first of them, so a `data.parquet` at the root (which sorts above both `date=` and
/// `dt=`) means no file's key is ever checked and the column comes back null. Name it
/// `loose.parquet` and the same directory will not open at all.
///
/// A byte sort of path strings, so this holds on any filesystem — and it is why the
/// note says the shape and not the cost: it cannot see a filename's spelling.
#[test]
fn test_directories_that_differ_may_still_open_and_the_note_claims_only_the_shape() {
    let dir = tempfile::tempdir().unwrap();
    for day in ["date=2024-01-01", "date=2024-01-02", "date=2024-01-03"] {
        write_parquet(
            dir.path(),
            day,
            df!("id" => &[0i64], "n" => &[1i64]).unwrap(),
        );
    }
    write_parquet(
        dir.path(),
        "dt=2024-01-04",
        df!("id" => &[1i64], "n" => &[2i64]).unwrap(),
    );
    // At the root, and named so that it sorts before both partition directories.
    write_parquet(
        dir.path(),
        "",
        df!("id" => &[9i64], "n" => &[9i64]).unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let area = Rect::new(0, 0, 100, 24);
    let frame = painted(&mut app, &rx, &tx, area);
    assert!(
        !frame.contains("Error"),
        "the same disagreement as the test above, and this one opens: {frame}"
    );
    assert_eq!(current_rows(&app), 5, "every file is read");

    let state = app.data_table_state.as_ref().unwrap();
    let notes = state.notes();
    let layout = notes
        .iter()
        .find(|note| note.summary.contains("mixed partition keys"))
        .unwrap_or_else(|| panic!("the directories still differ: {notes:#?}"));
    assert_eq!(
        layout.summary, "mixed partition keys: 3 files by date, 1 file by dt",
        "said of a dataset that opened, which is why it says nothing about cost"
    );
    assert_eq!(
        layout.scope, "in the names of 5 files",
        "the file at the root is one of the names read, though it is no layout"
    );
}

/// The control: a directory partitioned the one way opens, and says nothing about keys.
#[test]
fn test_a_directory_partitioned_the_one_way_says_nothing_about_its_keys() {
    let dir = tempfile::tempdir().unwrap();
    for day in ["date=2024-01-01", "date=2024-01-02"] {
        write_parquet(
            dir.path(),
            day,
            df!("id" => &[0i64], "n" => &[1i64]).unwrap(),
        );
    }

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let area = Rect::new(0, 0, 100, 24);
    let frame = painted(&mut app, &rx, &tx, area);
    assert!(!frame.contains("Error"), "it opens: {frame}");

    let state = app.data_table_state.as_ref().unwrap();
    assert!(
        !state
            .notes()
            .iter()
            .any(|note| note.summary.contains("mixed partition keys")),
        "{:#?}",
        state.notes()
    );
}

/// The counter the loading screen reads is the one the real open writes to.
///
/// Every other test here drives the counter from one side: the render tests set it by
/// hand, the pass test calls the pass directly with a counter of its own. Neither says
/// the two are connected — with only those, pointing the open at the non-reporting
/// pass leaves the feature completely dead in the running app and the suite green.
#[test]
fn test_opening_a_directory_reports_its_footers_to_the_app() {
    let dir = tempfile::tempdir().unwrap();
    for day in ["date=2024-01-01", "date=2024-01-02", "date=2024-01-03"] {
        write_parquet(
            dir.path(),
            day,
            df!("id" => &[0i64], "n" => &[1i64]).unwrap(),
        );
    }

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let _ = painted(&mut app, &rx, &tx, Rect::new(0, 0, 100, 24));

    assert_eq!(
        app.footer_progress().last_pass().begun,
        1,
        "the open ran its footer pass against the app's own counter"
    );
    assert_eq!(
        app.footer_progress().last_pass().read,
        3,
        "and counted each of the three footers off it"
    );
    assert_eq!(
        app.footer_progress().reading(),
        None,
        "with nothing left on screen once they landed"
    );
}

/// A second open starts its own count rather than inheriting the first one's.
///
/// Abandoning a load cancels nothing — the footers keep being read — so a counter
/// shared across loads reports the abandoned directory's progress under the next file's
/// name, which is what a user opening a small CSV after a large directory would see.
#[test]
fn test_each_open_counts_its_own_footers() {
    let first = tempfile::tempdir().unwrap();
    for day in ["date=2024-01-01", "date=2024-01-02", "date=2024-01-03"] {
        write_parquet(first.path(), day, df!("id" => &[0i64]).unwrap());
    }
    let second = tempfile::tempdir().unwrap();
    write_parquet(
        second.path(),
        "date=2024-01-01",
        df!("id" => &[0i64]).unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(first.path());
    let _ = painted(&mut app, &rx, &tx, Rect::new(0, 0, 100, 24));
    let counter_of_the_first = app.footer_progress().clone();
    assert_eq!(counter_of_the_first.last_pass().read, 3);

    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![second.path().to_path_buf()],
        OpenOptions {
            hive: true,
            ..OpenOptions::default()
        },
    );
    let _ = painted(&mut app, &rx, &tx, Rect::new(0, 0, 100, 24));

    // The pointer comparison is the one that bites, and it is first because of that.
    // `begin` stores zero, so a second pass resets a shared counter as surely as a
    // fresh one and the count below reads 1 either way — it says what the counter
    // should hold, and the line under it is what makes holding it mean anything.
    assert!(
        !Arc::ptr_eq(&counter_of_the_first, app.footer_progress()),
        "the second open has a counter of its own"
    );
    assert_eq!(
        app.footer_progress().last_pass().read,
        1,
        "counting its one footer, not the three before it"
    );

    // And the behaviour, not just the mechanism: the first open's pass goes on running
    // after it is abandoned, so a shared counter would paint its progress onto the
    // screen of the file that replaced it. Driven by hand, since a real abandoned pass
    // finishes too fast to catch — and rendered while the app is still *loading*,
    // because an idle app never consults the counter at all and the assertion would
    // hold however broken the wiring was.
    counter_of_the_first.begin(6541);
    for _ in 0..4102 {
        counter_of_the_first.advance();
    }
    app.set_loading_phase("Reading schema", 40);
    let mut buf = ratatui::buffer::Buffer::empty(Rect::new(0, 0, 100, 24));
    app.render(Rect::new(0, 0, 100, 24), &mut buf);
    let frame: String = (0..24)
        .flat_map(|y| {
            (0..100)
                .map(move |x| (x, y))
                .map(|(x, y)| buf[(x, y)].symbol().to_string())
                .chain(std::iter::once("\n".to_string()))
        })
        .collect();
    assert!(
        frame.contains("Reading schema"),
        "the app is on its loading screen, where the counter is read:\n{frame}"
    );
    assert!(
        !frame.contains("6,541"),
        "and the abandoned directory's count does not appear under the file that \
         replaced it:\n{frame}"
    );
}

/// The footer shows the same count the loading body does.
///
/// Both derive it from `App::loading_phase`, and the point of that is that one wait
/// cannot be described two ways. The truncation test in `render/footer.rs` builds the bar
/// with a hand-written string, so it says the bar cuts a long message properly and
/// nothing about whether the bar is ever given the count at all: deleting the line
/// that hands it over leaves the body counting and the bar still saying "Caching
/// schema", with the suite green.
#[test]
fn test_the_footer_counts_the_footers_the_loading_screen_does() {
    let (tx, _rx) = std::sync::mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.set_loading_phase("Reading schema", 40);
    app.footer_progress().begin(6541);
    for _ in 0..1203 {
        app.footer_progress().advance();
    }

    let area = Rect::new(0, 0, 100, 24);
    let mut buf = ratatui::buffer::Buffer::empty(area);
    app.render(area, &mut buf);
    let rows: Vec<String> = common::buffer_lines(&buf);

    let body = rows.iter().find(|r| r.contains("Reading footers"));
    assert!(body.is_some(), "the body counts them:\n{}", rows.join("\n"));
    let bar = rows.last().expect("a footer");
    assert!(
        bar.contains("Reading footers: 1,203 of 6,541"),
        "and so does the bar, rather than the phase the body has stopped showing: \
         {bar:?}"
    );
    // Not beside the per-phase constant, which is 40 here and would read as this
    // count's progress. 1,203 of 6,541 is 18%.
    assert!(
        !bar.contains('%'),
        "a real fraction is not to be shown beside a made-up percentage: {bar:?}"
    );
}

/// The bar says the footers are still arriving, after the dataset is on screen.
///
/// A cloud prefix of many files opens from two of them and reads the rest behind the
/// data. Nothing is blocked and nothing is wrong, so it is said in the footer
/// rather than on a loading screen — but it is said, because otherwise columns appear
/// minutes later with no explanation.
#[test]
fn test_the_bar_says_the_footers_are_still_arriving_while_the_data_is_up() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(
        dir.path(),
        "date=2024-01-01",
        df!("id" => &[0i64, 1, 2]).unwrap(),
    );
    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let _ = painted(&mut app, &rx, &tx, Rect::new(0, 0, 100, 24));
    // The same rows as a staged open leaves them: counted, with a pass still out.
    let waiting = || {
        let mut state = datui::table::DataTableState::from_lazyframe(
            df!("id" => &[0i64, 1, 2]).unwrap().lazy(),
            &OpenOptions::default(),
        )
        .unwrap()
        .with_open(datui::table::OpenFacts {
            footers_pending: Some(std::sync::Arc::new(|_| None)),
            ..Default::default()
        });
        assert!(state.count_landed(state.len_generation(), 3, None));
        state
    };
    // Said only for a dataset that is itself waiting. The counter is shared with every
    // open, and one abandoned half way through goes on counting: without the dataset's
    // own say-so this bar would count a directory the user walked away from.
    app.data_table_state = Some(waiting());
    app.footer_progress().begin(6541);
    for _ in 0..1203 {
        app.footer_progress().advance();
    }

    let area = Rect::new(0, 0, 100, 24);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let bar: String = (area.height - 2..area.height)
        .flat_map(|y| (0..area.width).map(move |x| (x, y)))
        .map(|at| buf[at].symbol().to_string())
        .collect();
    assert!(
        bar.contains("footers 1,203 / 6,541"),
        "the bar says what is still arriving: {bar:?}"
    );
    // And it prints the count, because this dataset has one: every file of it was read
    // at the open. A spinner here would be spinning over a number in hand. What is not
    // shown is a count that has not been taken — see
    // `a_count_that_has_arrived_is_not_held_back_with_the_columns`, where the dataset
    // says a count is still coming exactly while it has none.
    assert!(
        bar.contains("/ 3"),
        "the count it does have is shown: {bar:?}"
    );

    // And says nothing for a dataset that is not the one waiting: the directory this user
    // gave up on goes on reading its footers, and this is not it.
    app.data_table_state
        .as_mut()
        .expect("a dataset")
        .give_up_on_pending_footers();
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let other: String = (area.height - 2..area.height)
        .flat_map(|y| (0..area.width).map(move |x| (x, y)))
        .map(|at| buf[at].symbol().to_string())
        .collect();
    assert!(
        !other.contains("footers 1,203"),
        "a count belonging to a directory the user left is not this dataset's: {other:?}"
    );
    app.data_table_state = Some(waiting());

    // And stops saying it the moment they have.
    app.footer_progress().done();
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let bar: String = (area.height - 2..area.height)
        .flat_map(|y| (0..area.width).map(move |x| (x, y)))
        .map(|at| buf[at].symbol().to_string())
        .collect();
    assert!(
        !bar.contains("footers 1,203"),
        "and nothing once they are all in: {bar:?}"
    );
}

/// One frame, one number — while the pass is still running.
///
/// The body and the bar are painted a millisecond apart, with the threads reading the
/// footers moving the counter in between. Reading it once each let them print
/// different numbers for the same wait: measured at four thousand frames out of four
/// thousand. It also let the bar decide there was no count running just after the body
/// had shown one, putting the phase's flat percentage back beside it.
#[test]
fn test_one_frame_says_one_number_while_the_footers_are_still_arriving() {
    let (tx, _rx) = std::sync::mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.set_loading_phase("Reading schema", 40);
    app.footer_progress().begin(200_000);

    // A reader, going as fast as the real ones do between two paints.
    let counter = app.footer_progress().clone();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stopping = stop.clone();
    let reading = std::thread::spawn(move || {
        while !stopping.load(std::sync::atomic::Ordering::Relaxed) {
            counter.advance();
        }
    });

    let area = Rect::new(0, 0, 100, 24);
    // The count itself, not the line: the body is centred and the bar carries the rest
    // of the status beside it, so the two lines differ everywhere except here.
    let number = |row: &str| -> Option<String> {
        row.split("Reading footers: ").nth(1).map(|rest| {
            rest.chars()
                .take_while(|c| c.is_ascii_digit() || *c == ',')
                .collect()
        })
    };
    for frame in 0..200 {
        let mut buf = ratatui::buffer::Buffer::empty(area);
        app.render(area, &mut buf);
        let rows: Vec<String> = common::buffer_lines(&buf);
        let body = rows
            .iter()
            .find(|r| r.contains("Reading footers"))
            .and_then(|r| number(r));
        let bar = rows.last().and_then(|r| number(r));
        assert_eq!(
            body,
            bar,
            "frame {frame} said two things about one wait:\n{}",
            rows.join("\n")
        );
        assert!(
            body.is_some(),
            "frame {frame} stopped counting mid-pass:\n{}",
            rows.join("\n")
        );
    }

    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    reading.join().unwrap();
}

/// The control for the test above: a directory whose files agree shows neither glyph, so
/// the assertions there are about the data and not about some other part of the screen.
#[test]
fn test_a_uniform_dataset_shows_no_absent_or_conflicting_cells() {
    let g = datui::glyphs::get();
    let dir = tempfile::tempdir().unwrap();
    write_parquet(
        dir.path(),
        "date=2024-01-01",
        df!("id" => &[1i64], "note" => &[None::<&str>]).unwrap(),
    );
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[2i64], "note" => &["hi"]).unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let text = painted(&mut app, &rx, &tx, Rect::new(0, 0, 100, 20));
    // The table, without the footer (the last row), whose separator is the
    // same dot.
    let text: String = text.chars().take(100 * 19).collect();
    assert!(text.contains(g.null), "the real null still shows");
    assert!(!text.contains(g.absent), "nothing is absent here");
    assert!(!text.contains(g.conflict), "nothing conflicts here");
    let state = app.data_table_state.as_ref().unwrap();
    assert!(!state.drifts(), "and the scan stamped no drift column");
}

/// The drift column is the state's own bookkeeping. It must not reach the schema, the
/// column order, or an export.
#[test]
fn test_the_hidden_drift_column_is_never_part_of_the_data() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(dir.path(), "date=2024-01-01", df!("id" => &[1i64]).unwrap());
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[2i64], "extra" => &["x"]).unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let state = app.data_table_state.as_ref().unwrap();
    assert!(state.drifts(), "this dataset does drift");

    let names: Vec<&str> = state.schema().iter_names().map(|n| n.as_str()).collect();
    assert_eq!(
        names,
        ["date", "id", "extra"],
        "no hidden column in the schema"
    );
    let source: Vec<&str> = state
        .source_schema()
        .iter_names()
        .map(|n| n.as_str())
        .collect();
    assert_eq!(
        source,
        ["date", "id", "extra"],
        "nor in the columns a saved view matches on"
    );
    assert!(
        !state.get_column_order().iter().any(|c| c.starts_with("__")),
        "nor in the column order"
    );

    // What actually reaches a file is what matters, so drive the real export rather
    // than the accessor the export is supposed to use.
    let out = dir.path().join("out.csv");
    let header = export_csv_header(&mut app, &rx, &tx, &out);
    assert_eq!(header, "date,id,extra", "nor in what an export writes");
}

/// A reset returns to the data as opened, so the cells that stand for a file the
/// column was never in read as absent again.
#[cfg(feature = "sql")]
#[test]
fn test_a_reset_brings_back_the_absent_cells() {
    let g = datui::glyphs::get();
    let dir = tempfile::tempdir().unwrap();
    write_parquet(
        dir.path(),
        "date=2024-01-01",
        df!("id" => &[1i64, 4]).unwrap(),
    );
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[2i64, 3], "extra" => &["x", "y"]).unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let area = Rect::new(0, 0, 100, 20);
    assert!(
        painted(&mut app, &rx, &tx, area).contains(g.absent),
        "absent at open"
    );

    let state = app.data_table_state.as_mut().unwrap();
    state.sql_query("select * from df".to_string());
    common::read_rows(&mut app, &rx);
    let state = app.data_table_state.as_mut().unwrap();
    assert!(state.error().is_none(), "the query: {:?}", state.error());
    assert!(
        !state.drifts(),
        "a query's rows stand for no file, so nulls are plain nulls"
    );

    let state = app.data_table_state.as_mut().unwrap();
    state.reset();
    common::read_rows(&mut app, &rx);
    let state = app.data_table_state.as_mut().unwrap();
    assert!(state.error().is_none(), "the reset: {:?}", state.error());
    assert!(state.drifts(), "and the reset puts the files back");
    assert!(
        painted(&mut app, &rx, &tx, area).contains(g.absent),
        "so the absent cells read as absent again"
    );
}

/// Counting a many-file scan's rows must not kill the app.
///
/// A dataset whose files disagree on a column's type is read as a union of scans, one
/// per run of files. `len()` is `UInt32`, and summing it across a union widens to
/// `UInt128`, which the streaming engine panics on rather than erroring — and datui
/// runs streaming by default. Anything that invalidates the row count (a filter, a
/// query, a reset) took the whole process down with it.
#[test]
fn test_counting_a_union_of_scans_does_not_panic() {
    let dir = tempfile::tempdir().unwrap();
    // `n` is text in one file and a number in the other, so the two are scanned apart.
    write_parquet(
        dir.path(),
        "date=2024-01-01",
        df!("id" => &[1i64, 4], "n" => &["a", "b"]).unwrap(),
    );
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[2i64, 3], "n" => &[10i64, 20]).unwrap(),
    );

    let (mut app, rx, _tx) = open_local_dataset_with_channel(dir.path());
    let state = app.data_table_state.as_mut().unwrap();
    state.fuzzy_search("a".to_string());
    assert!(state.error().is_none(), "fuzzy search: {:?}", state.error());
    common::read_rows(&mut app, &rx);
    let state = app.data_table_state.as_mut().unwrap();
    assert!(
        state.error().is_none(),
        "collect after the search: {:?}",
        state.error()
    );
}

/// Opening a directory whose files disagree leaves something to say, and the Info key
/// carries a quiet accent until the panel has been opened.
#[test]
fn test_a_drifting_dataset_has_notes_and_offers_them_once() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(dir.path(), "date=2024-01-01", df!("id" => &[1i64]).unwrap());
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[2i64], "extra" => &["x"]).unwrap(),
    );

    let mut app = open_local_dataset(dir.path());
    let state = app.data_table_state.as_ref().unwrap();
    let notes = state.notes();
    assert_eq!(notes.len(), 1, "one column is not in every file");
    assert_eq!(
        notes[0].summary,
        "extra is in 1 of 2 files, only date=2024-01-02; absent from the rest, \
         not null"
    );
    assert_eq!(
        notes[0].scope, "in all 2 footers",
        "and says what it is based on"
    );
    assert!(state.notes_unseen(), "not offered yet");

    // Pressing i opens the panel, which is the offer being taken up.
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Char('i'),
        KeyModifiers::NONE,
    )));
    let state = app.data_table_state.as_ref().unwrap();
    assert!(!state.notes_unseen(), "the accent has done its job");
}

/// More notes than the panel is tall must not be dropped on the floor: the panel says
/// how many are out of view, and the cursor reaches them.
#[test]
fn test_notes_past_the_fold_are_counted_and_reachable() {
    let dir = tempfile::tempdir().unwrap();
    // Six columns, each arriving one day later, is six notes.
    write_parquet(dir.path(), "date=2024-01-01", df!("id" => &[1i64]).unwrap());
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[2i64], "a" => &["x"], "b" => &["x"], "c" => &["x"],
            "d" => &["x"], "e" => &["x"], "f" => &["x"])
        .unwrap(),
    );

    let mut app = open_local_dataset(dir.path());
    assert_eq!(app.data_table_state.as_ref().unwrap().notes().len(), 6);

    // Unread notes put their tab in front, so opening the panel is the whole walk.
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Char('i'),
        KeyModifiers::NONE,
    )));
    // A short panel cannot show six notes at two lines each plus a gap.
    let area = Rect::new(0, 0, 100, 16);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let screen: String = buf.content().iter().map(|c| c.symbol()).collect();

    // Derived rather than counted out here: how many notes fit depends on how long
    // they are, and a note says more now than it used to. What has to hold is that the
    // ones that do not fit are counted rather than dropped.
    let shown = ["a", "b", "c", "d", "e", "f"]
        .iter()
        .filter(|name| screen.contains(&format!("{name} is in 1 of 2 files")))
        .count();
    assert!(
        (2..6).contains(&shown),
        "some notes fit and some do not, which is what this is about — and a panel \
         this tall holds at least two, or the panel has got much greedier than the \
         note got longer: {shown} of 6"
    );
    assert!(
        screen.contains(&format!("{} below", 6 - shown)),
        "and the ones out of view are counted, got:\n{screen}"
    );
    assert!(
        screen.contains("a is in 1 of 2 files"),
        "the first note is shown"
    );
    assert!(
        screen.contains("in all 2 footers"),
        "and the scope of the note the cursor is on, got:\n{screen}"
    );

    // The cursor reaches the last note, which scrolls it into view.
    for _ in 0..6 {
        app.event(AppEvent::Key(KeyEvent::new(
            KeyCode::Char('j'),
            KeyModifiers::NONE,
        )));
    }
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let screen: String = buf.content().iter().map(|c| c.symbol()).collect();
    assert!(
        screen.contains("f is in 1 of 2 files"),
        "the last note is reachable, got:\n{screen}"
    );
    assert!(
        screen.contains("above"),
        "and the panel says what scrolled off the top, got:\n{screen}"
    );

    // Too short to hold a note is not the same as having none to hold: the panel
    // says which it is, and never claims there is nothing to say.
    for height in 5u16..26 {
        let area = Rect::new(0, 0, 100, height);
        let mut buf = Buffer::empty(area);
        app.render(area, &mut buf);
        let screen: String = buf.content().iter().map(|c| c.symbol()).collect();
        let drew_a_note = screen.contains("is in 1 of");
        let said_no_room = screen.contains("no room");
        assert!(
            drew_a_note ^ said_no_room,
            "a {height}-row panel draws a note or says it has no room for one, \
             exactly one of the two: {screen:?}"
        );
    }

    // A summary without its scope line under it is the misreading the scope line
    // exists to prevent, so no height may produce one.
    for height in 5u16..26 {
        let area = Rect::new(0, 0, 100, height);
        let mut buf = Buffer::empty(area);
        app.render(area, &mut buf);
        let rows: Vec<String> = (0..height)
            .map(|y| {
                (0..area.width)
                    .map(|x| buf[(x, y)].symbol().to_string())
                    .collect::<String>()
            })
            .collect();
        for (i, row) in rows.iter().enumerate() {
            if !row.contains("is in 1 of 2 files") {
                continue;
            }
            // The line a note rests on follows its summary, and always before the
            // next note begins.
            let found = rows[i + 1..]
                .iter()
                .take_while(|later| !later.contains("is in 1 of 2 files"))
                .any(|later| later.contains("in all 2 footers"));
            assert!(
                found,
                "at height {height}, a note is drawn with no basis under it:\n{}",
                row.trim_end()
            );
        }
    }

    // A note needs its summary and its basis, so one row cannot hold one. Say that
    // rather than draw half a note.
    let area = Rect::new(0, 0, 100, 5);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let screen: String = buf.content().iter().map(|c| c.symbol()).collect();
    assert!(
        screen.contains("selected: no room"),
        "too short for a whole note says so, got:\n{screen}"
    );
}

/// A panel exactly as tall as one note draws it, rather than reporting no room.
#[test]
fn test_a_note_that_fills_the_panel_is_drawn_not_refused() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(dir.path(), "date=2024-01-01", df!("id" => &[1i64]).unwrap());
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[2i64], "a" => &["x"], "b" => &["x"]).unwrap(),
    );

    let mut app = open_local_dataset(dir.path());
    // Unread notes put their tab in front, so opening the panel is the whole walk.
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Char('i'),
        KeyModifiers::NONE,
    )));
    // The shortest panel that draws a note, found rather than written down: how tall
    // that is depends on how long a note is, and a note says more than it used to.
    let mut drawn_at = |height: u16| -> String {
        let area = Rect::new(0, 0, 100, height);
        let mut buf = Buffer::empty(area);
        app.render(area, &mut buf);
        buf.content().iter().map(|c| c.symbol()).collect()
    };
    let shortest = (4u16..15)
        .find(|height| drawn_at(*height).contains("is in 1 of 2 files"))
        .expect("some panel in this range draws a note");
    // Nine: the note, the panel's rows, the blank row above its footer (#650) and the
    // rule above the status footer.
    assert!(
        shortest <= 9,
        "a note fits in a short panel; {shortest} rows to draw one means the panel has \
         got greedier, and the loop below would pass on one height and prove nothing"
    );

    // From there up, every height draws one. The bug this guards is a panel that has
    // the room and refuses anyway, which showed as a gap in the middle of this range.
    for height in shortest..15 {
        let screen = drawn_at(height);
        assert!(
            screen.contains("is in 1 of 2 files"),
            "a {height}-row panel has room for a note — {shortest} rows is enough — so \
             it draws one: {screen:?}"
        );
        assert!(
            !screen.contains("no room"),
            "and does not claim otherwise: {screen:?}"
        );
    }
}

/// A directory whose files agree has nothing to say, and nothing to show for it.
#[test]
fn test_a_uniform_dataset_has_no_notes() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(dir.path(), "date=2024-01-01", df!("id" => &[1i64]).unwrap());
    write_parquet(dir.path(), "date=2024-01-02", df!("id" => &[2i64]).unwrap());

    let app = open_local_dataset(dir.path());
    let state = app.data_table_state.as_ref().unwrap();
    assert!(state.notes().is_empty());
    assert!(!state.notes_unseen(), "so no accent either");
}

/// A dataset may already have a column called `source_file` — a directory of per-file
/// extracts is exactly this feature's audience — and adding one by that name would
/// replace it, silently, in the file the user takes away.
#[test]
fn test_naming_source_files_never_overwrites_a_column_of_that_name() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(
        dir.path(),
        "date=2024-01-01",
        df!("id" => &[1i64], "source_file" => &["mine-A"]).unwrap(),
    );
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[2i64], "source_file" => &["mine-B"], "extra" => &["x"]).unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let out = dir.path().join("collide.csv");
    let csv = export_csv(&mut app, &rx, &tx, &out, true);
    let lines: Vec<&str> = csv.lines().collect();

    assert_eq!(
        lines[0], "date,id,source_file,extra,source_file_1",
        "the dataset keeps its own column and datui's goes on the end under another name"
    );
    assert!(
        lines[1].contains("mine-A"),
        "the dataset's own values survive: {}",
        lines[1]
    );
    assert!(lines[2].contains("mine-B"), "both of them: {}", lines[2]);
}

/// A column only a middle file has used to vanish: the schema was one file's, and that
/// file did not have it.
#[test]
fn test_a_column_only_one_local_file_has_is_shown() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(
        dir.path(),
        "date=2024-03-01",
        df!("id" => &[1i64, 2]).unwrap(),
    );
    write_parquet(
        dir.path(),
        "date=2024-03-02",
        df!("id" => &[3i64], "oops" => &["x"]).unwrap(),
    );
    write_parquet(
        dir.path(),
        "date=2024-03-03",
        df!("id" => &[4i64, 5]).unwrap(),
    );

    let app = open_local_dataset(dir.path());
    let state = app.data_table_state.as_ref().unwrap();
    let names: Vec<&str> = state.schema().iter_names().map(|n| n.as_str()).collect();
    assert_eq!(
        names,
        ["date", "id", "oops"],
        "the partition key, then every column any file has"
    );
    let dataset = state.dataset_schema().expect("read from the footers");
    assert_eq!(dataset.origin.to_string(), "all 3 footers");
    let drifting: Vec<String> = dataset.drifting().map(|c| c.name.to_string()).collect();
    assert_eq!(drifting, ["oops"]);
}

/// Files written with different integer widths used to fail the strict local scan.
#[test]
fn test_local_files_of_different_integer_widths_open_as_the_wider_one() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(
        dir.path(),
        "date=2024-01-01",
        df!("n" => &[1i32, 2]).unwrap(),
    );
    write_parquet(dir.path(), "date=2024-01-02", df!("n" => &[3i64]).unwrap());

    let app = open_local_dataset(dir.path());
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(
        state.schema().get("n"),
        Some(&polars::prelude::DataType::Int64)
    );
}

/// One unreadable file must not stop the rest of the directory from opening.
#[test]
fn test_one_unreadable_local_file_does_not_stop_the_open() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(dir.path(), "date=2024-01-01", df!("id" => &[1i64]).unwrap());
    let bad = dir.path().join("date=2024-01-02");
    std::fs::create_dir_all(&bad).unwrap();
    std::fs::write(bad.join("data.parquet"), b"not a parquet file").unwrap();
    write_parquet(dir.path(), "date=2024-01-03", df!("id" => &[3i64]).unwrap());

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let screen = painted(&mut app, &rx, &tx, Rect::new(0, 0, 100, 24));
    let state = app.data_table_state.as_ref().unwrap();
    let names: Vec<&str> = state.schema().iter_names().map(|n| n.as_str()).collect();
    assert_eq!(names, ["date", "id"]);
    let dataset = state.dataset_schema().expect("read from the footers");
    assert_eq!(dataset.unreadable, [1], "named, and left out of the scan");
    // The dataset opening is the claim, so the rows are what has to be there. A schema
    // computed over a file that would not parse says nothing about whether the other
    // two can be read through it.
    assert!(
        !screen.contains("Error"),
        "and on screen, without an error: {screen}"
    );
    // The ids, not the partition names: `date=2024-01-01` and `date=2024-01-03` put a
    // `1` and a `3` on screen whatever the rows say, so checking for those characters
    // is a check on the directory's own names.
    let state = app.data_table_state.as_ref().unwrap();
    let ids = state
        .lf()
        .clone()
        .collect()
        .expect("the two readable files are readable")
        .column("id")
        .unwrap()
        .i64()
        .unwrap()
        .into_no_null_iter()
        .collect::<Vec<i64>>();
    assert_eq!(ids, [1, 3], "the two readable files' rows are there");
}

/// A table of no rows draws its header, each column typed, and says it is empty.
#[test]
fn an_empty_table_shows_its_header_and_says_so() {
    common::ensure_sample_data();
    let dir = common::fixture_dir();
    let csv = dir.join("empty_table_header.csv");
    std::fs::write(&csv, "x,y\n").unwrap();
    let lines = empty_table_lines(csv, OpenOptions::default());
    assert_eq!(&lines[..3], ["x    y", "str  str", "No rows"], "{lines:#?}");

    let lines = empty_table_lines(
        PathBuf::from("tests/sample-data/empty.parquet"),
        OpenOptions::default(),
    );
    assert_eq!(lines[2], "No rows", "{lines:#?}");
    assert!(
        lines[0].contains("id") && lines[0].contains("name"),
        "{lines:#?}"
    );
}

/// Ctrl+O at any point during a load must abandon it: whatever is on screen when the
/// user goes home is what is still there afterwards. The load runs to completion in
/// the background and its results are dropped.
///
/// Parameterised over how many chain steps have run, because the pipeline has many
/// interstitial frames and each one is a place the user can press the key.
#[test]
fn test_abandoned_load_never_installs_itself_afterwards() {
    common::ensure_sample_data();
    let path = PathBuf::from("tests/sample-data/large_dataset.parquet");
    let area = Rect::new(0, 0, 120, 50);

    for abandon_after in 0..8 {
        let (tx, rx) = mpsc::channel();
        let mut app = App::new(tx.clone(), common::test_runtime());
        tx.send(AppEvent::Open(vec![path.clone()], OpenOptions::default()))
            .unwrap();

        let mut steps = 0usize;
        let mut abandoned_at: Option<(Option<PathBuf>, bool)> = None;
        let mut ticks_since_abandon = 0usize;
        // The loop waits on the chain and on the abandoned work, never on a tick
        // count a loaded machine can outrun; the clock is only a safety net.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);

        loop {
            assert!(
                std::time::Instant::now() < deadline,
                "abandon_after {abandon_after}: timed out, abandoned: {}",
                abandoned_at.is_some()
            );
            let drained = drain_like_main_loop(&mut app, &tx, &rx);
            steps += drained;

            // Abandon once the chain has taken `abandon_after` steps, or as soon as it
            // has finished if it was shorter than that — going home after a completed
            // load must be just as inert.
            let chain_done = !app.is_busy() && app.data_table_state.is_some();
            if abandoned_at.is_none() && (steps >= abandon_after || chain_done) {
                app.event(ctrl_o());
                abandoned_at = Some((
                    app.open_path().map(Path::to_path_buf),
                    app.data_table_state.is_some(),
                ));
            }

            let mut buf = Buffer::empty(area);
            app.render(area, &mut buf);

            // Done when the abandoned scan and schema have reported back and their
            // results were handled: they had every chance to install themselves.
            // A few ticks more give the unleased row count its chance too.
            if abandoned_at.is_some() {
                ticks_since_abandon += 1;
                if ticks_since_abandon >= 40 && drained == 0 && !app.background_work_in_flight() {
                    break;
                }
            }
            common::wait_for_event(&tx, &rx);
        }

        let (path_at_abandon, had_state) = abandoned_at.expect("never reached the abandon point");

        assert_eq!(
            app.input_mode,
            InputMode::Home,
            "abandon_after {abandon_after}: should still be at home"
        );
        assert!(
            !app.is_busy(),
            "abandon_after {abandon_after}: abandoning a load must clear busy"
        );
        assert_eq!(
            app.open_path().map(Path::to_path_buf),
            path_at_abandon,
            "abandon_after {abandon_after}: the abandoned load swapped its dataset in afterwards"
        );
        assert_eq!(
            app.data_table_state.is_some(),
            had_state,
            "abandon_after {abandon_after}: data_table_state changed after abandonment"
        );
    }
}

/// The regression this whole change exists for: abandon a slow load, open something
/// else, and the abandoned load must not overwrite the dataset you actually asked
/// for — including its row count, which is counted by a separate background task.
#[test]
fn test_abandoned_load_does_not_corrupt_the_next_open() {
    common::ensure_sample_data();
    let big = PathBuf::from("tests/sample-data/large_dataset.parquet");
    let small = PathBuf::from("tests/sample-data/sales.parquet");
    let area = Rect::new(0, 0, 120, 50);

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    tx.send(AppEvent::Open(vec![big], OpenOptions::default()))
        .unwrap();

    // Let the big load get underway, then leave.
    for _tick in 0..3 {
        drain_like_main_loop(&mut app, &tx, &rx);
        let mut buf = Buffer::empty(area);
        app.render(area, &mut buf);
    }
    app.event(ctrl_o());
    assert_eq!(app.input_mode, InputMode::Home);

    tx.send(AppEvent::Open(vec![small.clone()], OpenOptions::default()))
        .unwrap();

    // Until the second open has settled and the abandoned work has reported back,
    // however long a loaded machine takes; the clock is only a safety net.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
    for tick in 0.. {
        assert!(
            std::time::Instant::now() < deadline,
            "the second open never settled"
        );
        let drained = drain_like_main_loop(&mut app, &tx, &rx);
        let mut buf = Buffer::empty(area);
        app.render(area, &mut buf);
        app.frame_painted();
        let needs = app
            .data_table_state
            .as_mut()
            .map(|s| {
                let n = s.needs_recollect;
                s.needs_recollect = false;
                n
            })
            .unwrap_or(false);
        if needs {
            app.spawn_async_collect("Loading buffer...");
        }
        let settled = drained == 0
            && !needs
            && !app.is_busy()
            && !app.background_work_in_flight()
            && !app.row_count_pending()
            && app.data_table_state.is_some();
        if tick >= 40 && settled {
            break;
        }
        common::wait_for_event(&tx, &rx);
    }

    assert_eq!(
        app.open_path(),
        Some(small.as_path()),
        "the dataset opened after abandoning should be the one on screen"
    );
    let state = app
        .data_table_state
        .as_ref()
        .expect("second open should have installed a dataset");
    assert_eq!(
        state.num_rows_if_valid(),
        Some(5000),
        "row count belongs to the dataset that is open, not the abandoned one"
    );
}

#[cfg(feature = "http")]
#[test]
fn test_ctrl_o_escapes_the_download_confirmation() {
    let (mut app, _rx) = app_awaiting_open_confirmation();
    assert!(app.awaiting_open_confirmation());

    app.event(ctrl_o());

    assert_eq!(
        app.input_mode,
        InputMode::Home,
        "Ctrl+O should work while the download confirmation is up"
    );
    assert!(
        !app.awaiting_open_confirmation(),
        "leaving should clear the pending download, not leave it armed"
    );
}

/// A file datui cannot read is hidden until Ctrl+A shows it, and says why on Enter.
#[test]
fn a_file_datui_cannot_read_is_hidden_until_shown() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("sales.csv"), "a\n1\n").unwrap();
    std::fs::write(dir.path().join("model.onnx"), "onnx").unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    app.enter_home();
    app.event(key(KeyCode::Char('~')));
    for c in dir.path().to_str().unwrap().chars() {
        app.event(key(KeyCode::Char(c)));
    }
    app.event(key(KeyCode::Enter));
    let ctrl_a = || AppEvent::Key(KeyEvent::new(KeyCode::Char('a'), KeyModifiers::CONTROL));
    app.event(ctrl_a());
    assert!(app.home.filter.is_empty(), "Ctrl+A is not typed");

    let row_of = |app: &App, name: &str| {
        app.home.visible().iter().position(
            |row| matches!(row, datui::home::Row::Entry { entry, .. } if entry.name == name),
        )
    };
    for _tick in ticks() {
        drain_like_main_loop(&mut app, &tx, &rx);
        if row_of(&app, "model.onnx").is_some() {
            break;
        }
        common::wait_for_event(&tx, &rx);
    }
    let model = row_of(&app, "model.onnx").expect("listed");
    app.home.selected = model;
    // Enter does nothing: the row is dimmed and its pane says why.
    app.home.status = None;
    assert!(app.event(key(KeyCode::Enter)).is_none(), "nothing opened");
    assert_eq!(app.home.status, None);

    app.event(ctrl_a());
    assert!(row_of(&app, "model.onnx").is_none());
    assert!(row_of(&app, "sales.csv").is_some());
}

/// Timestamps with `Z` or an offset load as UTC Datetime, with the string typing on
/// (the default) and with Polars' own date inference.
#[test]
fn test_iso_timestamps_with_offsets_load_as_utc_datetime() {
    use datui::ParseStringsTarget;
    for parse_strings in [Some(ParseStringsTarget::All), None] {
        let options = OpenOptions {
            parse_strings: parse_strings.clone(),
            ..OpenOptions::default()
        };
        let (app, _rx, _tx) = open_csv_with("iso_timestamps.csv", ISO_TIMESTAMPS_CSV, options);
        assert_iso_timestamps_typed(&app);
    }
}

/// `parse_dates = false` keeps them text, string typing or not.
#[test]
fn test_iso_timestamps_stay_text_without_parse_dates() {
    use datui::ParseStringsTarget;
    let options = OpenOptions {
        parse_strings: Some(ParseStringsTarget::All),
        parse_dates: false,
        ..OpenOptions::default()
    };
    let (app, _rx, _tx) = open_csv_with("iso_timestamps_as_text.csv", ISO_TIMESTAMPS_CSV, options);
    let schema = &app.data_table_state.as_ref().unwrap().schema();
    assert_eq!(schema.get("z"), Some(&DataType::String));
    assert_eq!(schema.get("id"), Some(&DataType::Int64));
}

/// NDJSON strings get the same timestamps; a string of digits stays a string.
#[test]
fn test_iso_timestamps_in_ndjson_load_as_utc_datetime() {
    let mut jsonl = String::new();
    for line in ISO_TIMESTAMPS_CSV.lines().skip(1) {
        let v: Vec<&str> = line.split(',').collect();
        jsonl.push_str(&format!(
            "{{\"id\":{},\"zip\":\"0{}\",\"z\":\"{}\",\"frac\":\"{}\",\"offset\":\"{}\",\"space\":\"{}\",\"minutes\":\"{}\",\"mixed\":\"{}\"}}\n",
            v[0], v[0], v[1], v[2], v[3], v[4], v[5], v[6]
        ));
    }
    let (app, _rx, _tx) = open_csv_with("iso_timestamps.jsonl", &jsonl, OpenOptions::default());
    assert_iso_timestamps_typed(&app);
    let schema = &app.data_table_state.as_ref().unwrap().schema();
    assert_eq!(schema.get("zip"), Some(&DataType::String));
}

/// A column that gives seconds on some values and not others stays text: a format
/// read from the first value must not match the front of a longer one and drop the
/// rest.
#[test]
fn test_datetimes_of_mixed_precision_stay_text() {
    use datui::ParseStringsTarget;
    let csv = "t\n2024-01-01 10:00\n2024-01-02 11:30:15\n";
    let options = OpenOptions {
        parse_strings: Some(ParseStringsTarget::All),
        ..OpenOptions::default()
    };
    let (app, _rx, _tx) = open_csv_with("datetimes_mixed_precision.csv", csv, options);
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.schema().get("t"), Some(&DataType::String));
    let df = state.lf().clone().collect().unwrap();
    let t = df.column("t").unwrap().str().unwrap();
    assert_eq!(t.get(1), Some("2024-01-02 11:30:15"));
}

/// Opening a directory measures what it cost, and the Info panel says so.
///
/// The end of the wiring rather than any one link in it: the open records into the
/// meter the app holds, the panel is handed that same meter, and the Resources tab
/// prints it. Each of those is guarded on its own elsewhere; none of those guards would
/// notice the panel being handed a meter nobody wrote to, which is the failure a user
/// would actually see — a Measurements section that says a dataset of three files was
/// found in no time at all.
///
/// Only the counts are asserted. The times are real elapsed times, so the only claim
/// that holds on every machine is that they were taken.
#[test]
fn test_opening_a_directory_measures_it_and_the_info_panel_says_so() {
    let dir = tempfile::tempdir().unwrap();
    for day in 1..=3 {
        write_parquet(
            dir.path(),
            &format!("date=2024-01-0{day}"),
            df!("id" => &[day as i64]).unwrap(),
        );
    }

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let area = Rect::new(0, 0, 100, 30);
    let _ = painted(&mut app, &rx, &tx, area);

    // `i` opens the panel on the Schema tab; one step right is Resources.
    for k in [KeyCode::Char('i'), KeyCode::Right] {
        if let Some(next) = app.event(key(k)) {
            let _ = tx.send(next);
        }
    }
    pump_until_idle(&mut app, &rx, &tx);

    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let text = common::buffer_text(&buf);

    assert!(
        text.contains("Measurements"),
        "the Resources tab says what the open cost; got:\n{text}"
    );
    assert!(
        text.contains("Listing:") && text.contains("Footers:"),
        "naming the two stretches it timed; got:\n{text}"
    );
    assert!(
        text.contains("3 files"),
        "the listing found three files; got:\n{text}"
    );
    // Three: the open reads a footer from each file, and those footers settle the row
    // count too, so no count pass reads them again (#643).
    assert!(
        text.contains("3 footers read"),
        "and the footer row counts the open's one pass over three files; got:\n{text}"
    );
    assert!(
        !text.contains("requests"),
        "a local directory is read, not requested, so no request count is claimed; got:\n{text}"
    );
}

/// A route that measures and then gives up leaves nothing on the dataset another route
/// built.
///
/// The routes are tried cheapest first, and the early ones measure before they discover
/// they cannot finish. A directory whose only Parquet sits under a `_delta_log` is the
/// case that reaches the screen: the hive route walks it, counts the checkpoint as the
/// writer's own bookkeeping, records a listing of no files and a footer pass over none,
/// and then gives up — while the full scan's glob does match the checkpoint and opens it.
/// Sharing one meter across the attempts paints `0 files` on a dataset showing rows.
#[test]
fn test_a_route_that_gave_up_leaves_no_figures_on_the_dataset_that_opened() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("_delta_log");
    std::fs::create_dir_all(&log).unwrap();
    let mut frame = df!("id" => &[1i64, 2, 3]).unwrap();
    let f = File::create(log.join("00000.checkpoint.parquet")).unwrap();
    ParquetWriter::new(f).finish(&mut frame).unwrap();

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![dir.path().to_path_buf()],
        OpenOptions {
            hive: true,
            ..OpenOptions::default()
        },
    );
    let area = Rect::new(0, 0, 100, 30);
    let _ = painted(&mut app, &rx, &tx, area);

    let state = app
        .data_table_state
        .as_ref()
        .expect("the full scan opened the checkpoint");
    let listing = state.measurements().listing();
    assert!(
        listing.is_none_or(|c| c.files != Some(0)),
        "the hive route walked, found nothing it would read, and gave up — its empty \
         listing must not end up on the dataset that did open; got {listing:?}"
    );

    for k in [KeyCode::Char('i'), KeyCode::Right] {
        if let Some(next) = app.event(key(k)) {
            let _ = tx.send(next);
        }
    }
    pump_until_idle(&mut app, &rx, &tx);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let text = common::buffer_text(&buf);
    assert!(
        !text.contains("0 files"),
        "and the Resources tab shows no listing of no files; got:\n{text}"
    );
}

/// A dataset Polars opened shows no measurements, even though its rows are counted
/// afterwards.
///
/// `read.parquet_schema = "first"` turns off the footer pass and hands the directory
/// straight to Polars, so there is nothing for datui to report. But the row count is
/// still taken from the footers afterwards, against the same dataset — and that pass
/// writing into the meter would raise a section out of nothing, headed by what opening
/// the dataset cost and containing only work done after it was already on screen.
#[test]
fn test_a_dataset_polars_opened_reports_nothing_about_opening_it() {
    let dir = tempfile::tempdir().unwrap();
    for day in 1..=3 {
        write_parquet(
            dir.path(),
            &format!("date=2024-01-0{day}"),
            df!("id" => &[day as i64]).unwrap(),
        );
    }

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![dir.path().to_path_buf()],
        OpenOptions {
            hive: true,
            single_spine_schema: false,
            ..OpenOptions::default()
        },
    );
    let area = Rect::new(0, 0, 100, 30);
    let _ = painted(&mut app, &rx, &tx, area);

    let state = app.data_table_state.as_ref().expect("the directory opened");
    assert_eq!(
        state.measurements().footers(),
        None,
        "the count pass ran, and belongs to no open this meter measured"
    );
    assert_eq!(
        state.measurements().total(),
        None,
        "and with neither stretch there is nothing to total"
    );

    for k in [KeyCode::Char('i'), KeyCode::Right] {
        if let Some(next) = app.event(key(k)) {
            let _ = tx.send(next);
        }
    }
    pump_until_idle(&mut app, &rx, &tx);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let text = common::buffer_text(&buf);
    assert!(
        !text.contains("Listing:") && !text.contains("Footers:") && !text.contains("Total:"),
        "so the Resources tab says nothing about what the open cost; got:\n{text}"
    );
    // The page it is showing is datui's own work on any route, and is reported.
    assert!(
        text.contains("Last page:"),
        "while what the page on screen cost is measured whichever route opened the \
         dataset; got:\n{text}"
    );
}

/// A second open does not inherit the first's figures, and a *failed* second open does
/// not give its figures to the dataset it left on screen.
///
/// The meter belongs to the dataset, not to the app, which is what makes the second
/// half true: a load that never reaches the screen never has its meter installed, so
/// the dataset still up keeps its own. An app-held meter gets this wrong in a way that
/// is hard to see — the panel keeps its heading and its shape, and only the numbers
/// underneath belong to something else.
#[test]
fn test_a_second_open_measures_itself_and_not_the_dataset_before_it() {
    let first = tempfile::tempdir().unwrap();
    for day in 1..=3 {
        write_parquet(
            first.path(),
            &format!("date=2024-01-0{day}"),
            df!("id" => &[day as i64]).unwrap(),
        );
    }
    let second = tempfile::tempdir().unwrap();
    write_parquet(
        second.path(),
        "date=2024-02-01",
        df!("id" => &[9i64]).unwrap(),
    );
    // Seven files that are named like Parquet and are not, so the open fails after its
    // listing and its footer pass have both been measured.
    let broken = tempfile::tempdir().unwrap();
    let broken_part = broken.path().join("date=2024-03-01");
    std::fs::create_dir_all(&broken_part).unwrap();
    for i in 0..7 {
        std::fs::write(broken_part.join(format!("f{i}.parquet")), b"not parquet").unwrap();
    }

    let hive = || OpenOptions {
        hive: true,
        ..OpenOptions::default()
    };
    let files_of = |app: &App| {
        app.data_table_state
            .as_ref()
            .and_then(|s| s.measurements().listing())
            .and_then(|c| c.files)
    };

    let (mut app, rx, tx) = open_local_dataset_with_channel(first.path());
    let area = Rect::new(0, 0, 100, 30);
    let _ = painted(&mut app, &rx, &tx, area);
    let meter_of_the_first = app
        .data_table_state
        .as_ref()
        .unwrap()
        .measurements()
        .clone();
    assert_eq!(
        files_of(&app),
        Some(3),
        "the first open measured its own three files"
    );

    // A second open that succeeds replaces both the dataset and its figures.
    pump_open_until_loaded(&mut app, &rx, vec![second.path().to_path_buf()], hive());
    let _ = painted(&mut app, &rx, &tx, area);
    assert!(
        !std::sync::Arc::ptr_eq(
            &meter_of_the_first,
            app.data_table_state.as_ref().unwrap().measurements()
        ),
        "the second dataset was installed with a meter of its own"
    );
    assert_eq!(
        files_of(&app),
        Some(1),
        "and reports the one file it found, not the four both directories hold between them"
    );

    // A third open that fails leaves the second dataset up — and leaves its figures
    // alone. The failed load measured seven files; none of them may appear here.
    let meter_of_the_second = app
        .data_table_state
        .as_ref()
        .unwrap()
        .measurements()
        .clone();
    pump_open_until_loaded(&mut app, &rx, vec![broken.path().to_path_buf()], hive());
    let _ = painted(&mut app, &rx, &tx, area);
    assert!(
        std::sync::Arc::ptr_eq(
            &meter_of_the_second,
            app.data_table_state.as_ref().unwrap().measurements()
        ),
        "the dataset on screen is still the second one, with the meter it was \
         installed with"
    );
    assert_eq!(
        files_of(&app),
        Some(1),
        "so the panel still says one file — not the seven the load that failed walked"
    );
}

/// Inside a directory of notes the one row says what is hidden and the bar offers to
/// show it. Enter does, and Ctrl+A hides them again with a flash on the bar rather than
/// a line beside the filter that outlives it.
#[test]
fn test_enter_on_the_hidden_row_shows_the_files() {
    let tmp = tempfile::tempdir().expect("tempdir");
    for i in 0..3 {
        std::fs::write(tmp.path().join(format!("note{i}.md")), b"x").unwrap();
    }
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.browsing = Some(tmp.path().to_path_buf());
    app.home.rebuild(&[]);
    app.home.select_first_entry();

    let area = Rect::new(0, 0, 120, 24);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let screen = common::buffer_text(&buf);
    assert!(screen.contains("3 files with no reader"), "{screen:?}");
    let bar: String = (0..area.width)
        .map(|x| buf[(x, area.height - 1)].symbol().to_string())
        .collect();
    assert!(bar.contains("Show"), "{bar:?}");

    app.event(key(KeyCode::Enter));
    assert!(!app.home.hide_unreadable);
    assert!(matches!(
        app.home.selected_row(),
        Some(datui::home::Row::Entry { entry, .. }) if entry.name == "note0.md"
    ));

    app.event(ctrl('a'));
    assert!(app.home.hide_unreadable);
    assert_eq!(app.home.status, None);
    assert_eq!(app.flash_message(), Some("Hiding files with no reader"));
}

/// The count is of what is listed, which the header and the `more` row agree on. It
/// sits on the section's rule; the bar keeps only the order (#547 D11).
#[test]
fn test_the_rule_counts_datasets_past_the_cap() {
    common::isolate_cache();
    let tmp = tempfile::tempdir().expect("tempdir");
    let recents: Vec<PathBuf> = (0..12)
        .map(|i| {
            let dir = tmp.path().join(format!("place{i}"));
            std::fs::create_dir_all(&dir).unwrap();
            let path = dir.join("data.parquet");
            std::fs::write(&path, b"x").unwrap();
            path
        })
        .collect();
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.rebuild(&recents);
    for section in 1..app.home.sections.len() {
        app.home.set_collapsed(section, true);
    }
    let area = Rect::new(0, 0, 200, 14);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let screen = common::buffer_text(&buf);
    assert!(screen.contains("more in"), "the cap is drawn: {screen:?}");
    let bar: String = (0..area.width)
        .map(|x| buf[(x, area.height - 1)].symbol().to_string())
        .collect();
    assert!(screen.contains("RECENT  12 "), "{screen:?}");
    assert!(bar.contains("by recent"), "{bar:?}");
    assert!(!bar.contains("datasets"), "{bar:?}");
}

/// The `… N more` row stands for the places the cap hides. `Enter` on it shows them
/// all, and nothing is opened.
#[test]
fn test_enter_on_the_more_row_expands_recent() {
    common::isolate_cache();
    let tmp = tempfile::tempdir().expect("tempdir");
    let recents: Vec<PathBuf> = (0..12)
        .map(|i| {
            let dir = tmp.path().join(format!("place{i}"));
            std::fs::create_dir_all(&dir).unwrap();
            let path = dir.join("data.parquet");
            std::fs::write(&path, b"x").unwrap();
            path
        })
        .collect();

    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.rebuild(&recents);
    // A short screen, so the cap bites: one place, then the more row.
    let area = Rect::new(0, 0, 120, 14);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let screen = common::buffer_text(&buf);
    assert!(screen.contains("more in"), "the cap is drawn: {screen:?}");

    let row = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::More { .. }))
        .expect("the more row is listed");
    app.home.selected = row;
    app.event(key(KeyCode::Enter));

    assert_eq!(app.input_mode, InputMode::Home, "nothing was opened");
    assert_eq!(app.home.browsing, None);
    let places = app
        .home
        .visible()
        .iter()
        .filter(|r| matches!(r, datui::home::Row::Place { .. }))
        .count();
    assert_eq!(places, 12, "every place is shown");
    assert!(
        !app.home
            .visible()
            .iter()
            .any(|r| matches!(r, datui::home::Row::More { places: 1.., .. })),
        "RECENT is whole"
    );
}

/// ← on a place past RECENT's cap, RECENT shown whole, cuts it back with the cursor on
/// the more row, as in a directory's section; ← on one of its first places folds it.
#[test]
fn test_left_cuts_recent_back_from_a_place_past_the_cap() {
    common::isolate_cache();
    let tmp = tempfile::tempdir().expect("tempdir");
    let recents: Vec<PathBuf> = (0..12)
        .map(|i| {
            let dir = tmp.path().join(format!("place{i:02}"));
            std::fs::create_dir_all(&dir).unwrap();
            let path = dir.join("data.parquet");
            std::fs::write(&path, b"x").unwrap();
            path
        })
        .collect();
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.rebuild(&recents);
    let area = Rect::new(0, 0, 120, 14);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let places = |app: &App| {
        app.home
            .visible()
            .iter()
            .filter(|r| matches!(r, datui::home::Row::Place { .. }))
            .count()
    };
    let shown = places(&app);
    assert!(shown < 12);
    app.home.selected = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::More { places: 1.., .. }))
        .unwrap();
    app.event(key(KeyCode::Right));
    assert_eq!(places(&app), 12, "→ on the more row shows every place");

    app.home.selected = app
        .home
        .visible()
        .iter()
        .rposition(|r| matches!(r, datui::home::Row::Place { .. }))
        .unwrap();
    app.event(key(KeyCode::Left));
    assert_eq!(places(&app), shown, "cut back");
    assert!(matches!(
        app.home.selected_row(),
        Some(datui::home::Row::More { places: 1.., .. })
    ));

    app.event(key(KeyCode::Right));
    app.home.selected = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Place { .. }))
        .unwrap();
    app.event(key(KeyCode::Left));
    assert!(app.home.is_collapsed(0), "← on a first place folds RECENT");
}

/// A Delta table's root is not a directory of Parquet files, and the home screen says so.
///
/// Its data files agree on a schema, so the one-table rule called it `multi` and Enter
/// read every file under it as one table — tombstoned rows back, every rewritten
/// version together, compaction counted twice. Nothing warned.
#[test]
fn test_a_delta_table_is_labelled_and_not_opened_as_one_table() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let table = tmp.path().join("orders");
    std::fs::create_dir_all(table.join("_delta_log")).unwrap();
    std::fs::write(table.join("_delta_log/00000000000000000000.json"), b"{}").unwrap();
    for part in ["part-0.parquet", "part-1.parquet", "part-2.parquet"] {
        std::fs::write(table.join(part), b"x").unwrap();
    }

    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.browsing = Some(tmp.path().to_path_buf());
    app.home.rebuild(&[]);

    let row = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Entry { entry, .. } if entry.name == "orders"))
        .expect("the table is listed");
    app.home.selected = row;
    // A listing looks into nothing, so what this row is has to be found before it
    // can be acted on. In the app a background pass does it, highlighted row first;
    // here the same call does it on the spot.
    app.home.classify_now(8);
    assert_eq!(
        app.home.selected_entry().map(|e| e.kind),
        Some(datui::home::discover::EntryKind::Delta),
        "the log says what this is"
    );

    let area = Rect::new(0, 0, 200, 24);
    let screen = |app: &mut App| {
        let mut buf = Buffer::empty(area);
        app.render(area, &mut buf);
        common::buffer_text(&buf)
    };
    let listing = screen(&mut app);
    assert!(
        listing.contains("delta"),
        "the row says what it is: {listing}"
    );
    assert!(
        !listing.contains("multi"),
        "and does not offer it as a directory of files: {listing}"
    );

    // Enter goes inside rather than reading every file under it as one table.
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    assert_eq!(
        app.home.browsing.as_deref(),
        Some(table.as_path()),
        "Enter went inside the table"
    );
    assert!(app.data_table_state.is_none(), "and opened nothing");
    let status = lake_heading(&mut app);
    assert!(
        status.contains("delta") && status.contains("not read"),
        "and says why: {status:?}"
    );
}

/// A sampled column count shows as a floor on the row and in the details pane.
///
/// The `Entry` flag had a test; what reaches the user had none — reverting either render
/// site left the suite green.
#[test]
fn test_a_sampled_column_count_is_marked_on_screen() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let directory = tmp.path().join("events");
    std::fs::create_dir_all(&directory).unwrap();

    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.browsing = Some(tmp.path().to_path_buf());
    app.home.rebuild(&[]);

    let row = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Entry { entry, .. } if entry.name == "events"))
        .expect("the directory is listed");
    app.home.selected = row;

    // As a directory past the footer budget comes back from measurement.
    for section in app.home.sections_mut().iter_mut() {
        for entry in section.rows.iter_mut().filter(|e| e.name == "events") {
            entry.kind = datui::home::discover::EntryKind::MultiFile;
            entry.rows = None;
            entry.cols = Some(39);
            entry.cols_sampled = true;
            entry.columns = vec!["id".to_string()];
        }
    }

    let area = Rect::new(0, 0, 160, 24);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let screen: String = common::buffer_text(&buf);

    assert!(
        screen.contains("39+"),
        "the row and the pane say the count is a floor: {screen}"
    );
    // Both render sites, named separately: a `contains` over the whole screen would be
    // satisfied by either one alone.
    let times = datui::glyphs::get().times;
    assert!(
        screen.contains(&format!("? {times} 39+")),
        "the row's shape says the width is a floor: {screen}"
    );
    assert!(
        screen.contains("columns   39+"),
        "and so does the details pane: {screen}"
    );
}

/// A background probe answering does not cancel the open the user asked for.
///
/// The first gate was `home_generation`, which means "the listing was rebuilt" and not
/// "the user navigated": a probe of some other root answering bumps it. On a home screen
/// with network roots — the only kind where this path runs at all — that made Enter do
/// nothing, at random, with no message.
#[test]
fn test_a_probe_answering_does_not_cancel_an_open_in_flight() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let table = tmp.path().join("orders");
    std::fs::create_dir_all(table.join("_delta_log")).unwrap();
    std::fs::write(table.join("_delta_log/00000000000000000000.json"), b"{}").unwrap();
    std::fs::write(table.join("part-0.parquet"), b"x").unwrap();

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.network_check = |_| true;
    app.home.rebuild(std::slice::from_ref(&table));
    let row = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Entry { entry, .. } if entry.path == table))
        .expect("the table is listed under Recent");
    app.home.selected = row;

    let mut follow = app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    while let Some(event) = follow {
        follow = app.event(event);
    }

    // A listing the user did not ask for lands while the look is out. Through the event,
    // because it is the handler that refreshes the home screen — which is what the first
    // gate mistook for the user having navigated.
    app.event(AppEvent::HomeProbeReady {
        root: PathBuf::from("/mnt/somewhere-else"),
        rows: Some(Vec::new()),
        cut_short: false,
    });

    while let Some(event) = next_event(&app, &rx) {
        let mut follow = app.event(event);
        while let Some(next) = follow {
            follow = app.event(next);
        }
        if !app.is_busy() {
            break;
        }
    }

    assert_eq!(
        app.home.browsing.as_deref(),
        Some(table.as_path()),
        "the answer was still the one the user was waiting for"
    );
}

/// A directory datui offers as one dataset is read as whatever is actually in it.
///
/// A directory the home screen labels `multi` is opened with `hive: true`, and every such
/// directory used to go straight to the Parquet scanner however it was filled. A
/// directory of CSVs therefore failed the way a directory of `.json.gz` did: Parquet
/// seeks to the last four bytes looking for `PAR1`, finds something else, and says the
/// file must end with it — a complaint about files that were never the problem.
#[test]
fn test_a_directory_opened_as_one_dataset_is_read_as_what_it_holds() {
    let tmp = tempfile::TempDir::new().unwrap();
    std::fs::write(tmp.path().join("part-0.csv"), "a,b\n1,x\n2,y\n").unwrap();
    std::fs::write(tmp.path().join("part-1.csv"), "a,b\n3,z\n").unwrap();

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![tmp.path().to_path_buf()],
        OpenOptions {
            hive: true,
            ..OpenOptions::default()
        },
    );

    let state = app
        .data_table_state
        .as_ref()
        .expect("a directory of CSVs should open as one table of CSVs");
    let df = state.lf().clone().collect().unwrap();
    assert_eq!(
        df.height(),
        3,
        "both files' rows, concatenated: {:?}",
        df.get_column_names()
    );
}

/// A directory holding two different formats is not one table, and the complaint names
/// the directory rather than Parquet's magic number.
///
/// Asserting on the message, not merely on the failure: opening this directory failed
/// before the fix too — with "must end with PAR1", about files nobody asked to be
/// Parquet. A test that only checked that nothing loaded would pass either way and be
/// about nothing.
#[test]
fn test_a_directory_of_two_formats_is_read_as_the_one_it_mostly_holds() {
    let tmp = tempfile::TempDir::new().unwrap();
    std::fs::write(tmp.path().join("a.csv"), "a\n1\n").unwrap();
    std::fs::write(tmp.path().join("b.csv"), "a\n2\n").unwrap();
    std::fs::write(tmp.path().join("c.json"), "[{\"a\":1}]").unwrap();

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![tmp.path().to_path_buf()],
        OpenOptions {
            hive: true,
            ..OpenOptions::default()
        },
    );

    let table = app
        .data_table_state
        .as_ref()
        .expect("two CSVs and a stray JSON is a directory of CSVs");
    assert_eq!(table.headers(), vec!["a"], "read as CSV, not as JSON");
    assert_eq!(
        table.num_rows_if_valid(),
        Some(2),
        "both CSVs, and not the JSON beside them"
    );
}

/// A directory of CSVs with some unrelated directory beside them is still a directory of
/// CSVs.
///
/// The other edge of the same rule: what sends a directory to the Parquet hive scan is a
/// `key=value` partition under it, not merely having a subdirectory. Treating any
/// subdirectory as "the data is deeper" handed an ordinary directory of CSVs back to the
/// scan that cannot read them.
#[test]
fn test_a_directory_of_csvs_beside_an_unrelated_directory_still_reads_as_csvs() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("part-0.csv"), "id\n1\n2\n").unwrap();
    std::fs::write(dir.path().join("part-1.csv"), "id\n3\n").unwrap();
    std::fs::create_dir_all(dir.path().join("archive")).unwrap();

    let app = open_local_dataset(dir.path());
    let state = app
        .data_table_state
        .as_ref()
        .expect("a directory of CSVs should open as one table of CSVs");
    assert_eq!(state.lf().clone().collect().unwrap().height(), 3);
}

/// A hive dataset of something other than Parquet says which files it holds.
///
/// Hive partitioning is a Parquet-only capability in the reader datui uses, so a tree
/// of `date=…/part.json` cannot be read as one table here. It used to reach the
/// Parquet scan anyway and fail with "the file must end with PAR1" — a complaint
/// about files nobody asked to be Parquet, naming neither the directory nor the format.
#[test]
fn test_a_hive_of_json_names_the_files_rather_than_parquets_magic_number() {
    let dir = tempfile::tempdir().unwrap();
    for day in ["2009-03-07", "2009-03-08"] {
        let part = dir.path().join(format!("date={day}"));
        std::fs::create_dir_all(&part).unwrap();
        std::fs::write(part.join("part.json"), "[{\"id\":1}]").unwrap();
    }

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    let complaint = pump_open_until_error(
        &mut app,
        &rx,
        vec![dir.path().to_path_buf()],
        OpenOptions {
            hive: true,
            ..OpenOptions::default()
        },
    )
    .expect("a hive of JSON cannot be read as one table");

    assert!(
        complaint.contains(".json") && complaint.contains("Parquet"),
        "it should say what the files are and why that is a problem: {complaint}"
    );
    assert!(
        !complaint.contains("PAR1"),
        "nothing here was ever Parquet: {complaint}"
    );
}

/// And a hive of Parquet still opens, however deeply it is partitioned.
///
/// The check above follows the partitions down to see what they hold; it must not
/// change what happens to the datasets that were always fine.
#[test]
fn test_a_nested_hive_of_parquet_still_opens() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(
        dir.path(),
        "year=2024/month=01",
        df!("id" => &[1i64, 2]).unwrap(),
    );
    write_parquet(
        dir.path(),
        "year=2024/month=02",
        df!("id" => &[3i64]).unwrap(),
    );

    let app = open_local_dataset(dir.path());
    let state = app
        .data_table_state
        .as_ref()
        .expect("a nested hive of Parquet is a dataset");
    assert_eq!(state.lf().clone().collect().unwrap().height(), 3);
}

/// The `(all files)` row opens the directory it names, whatever the directory is
/// labelled.
///
/// The label describes; this row is the promise that the description cannot lock you
/// out. A `mixed` directory is the case: `open_what_it_is` reads the label back, and a
/// `Directory` sent through it goes *inside* — which, on a row that is already inside,
/// is nowhere.
#[test]
fn test_enter_on_the_whole_directory_row_opens_rather_than_descending() {
    let tmp = tempfile::tempdir().expect("tempdir");
    std::fs::write(tmp.path().join("sales.csv"), b"a,b\n1,2\n").unwrap();
    std::fs::write(tmp.path().join("notes.json"), b"{}").unwrap();

    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.browsing = Some(tmp.path().to_path_buf());
    app.home.rebuild(&[]);

    let row = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Door { .. }))
        .expect("every directory carries the row");
    app.home.selected = row;

    let was = app.home.browsing.clone();

    // → does nothing here. This row is inside the directory it opens, so going inside is
    // nowhere: it would re-enter the listing already on screen and lose the cursor and
    // the filter on the way. ← / → fold, the way they do on a file row. Checked before
    // Enter, because Enter puts the app in its loading view and the home bar is gone.
    let area = Rect::new(0, 0, 200, 24);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let bar: String = (0..area.width)
        .map(|x| buf[(x, area.height - 1)].symbol().to_string())
        .collect();
    assert!(
        !bar.contains("Inside"),
        "the bar offers a door to nowhere: {bar:?}"
    );
    app.event(key(KeyCode::Right));
    assert_eq!(
        app.home.browsing, was,
        "→ on the row that opens this directory must not re-enter it"
    );
    assert_eq!(
        app.home.selected, row,
        "and must not move the cursor off it"
    );

    // Enter opens it.
    let next = app.event(key(KeyCode::Enter));
    assert!(
        matches!(next, Some(AppEvent::Open(..))),
        "Enter should open the directory rather than move the cursor"
    );
    assert_eq!(
        app.home.browsing, was,
        "and did not step into the directory it is already in"
    );
}

/// #275's done-when for this phase: from a directory the rule turns away, one table is
/// still reachable in two keystrokes.
///
/// A directory whose files each bring a column the other lacks is a place to look inside
/// rather than a dataset — that is `is_nested`, and it is stricter than the containment
/// threshold it replaced. What makes a strict rule affordable is the other door: `→`
/// steps in, and the first row in there opens the union anyway.
#[test]
fn test_a_directory_the_nesting_rule_turns_away_is_still_two_keys_from_one_table() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let sales = tmp.path().join("sales");
    std::fs::create_dir_all(&sales).unwrap();
    for (name, last) in [("old.parquet", "amount"), ("new.parquet", "amt")] {
        let mut frame = polars::prelude::DataFrame::new(
            1,
            vec![
                polars::prelude::Column::new("id".into(), &[1i32]),
                polars::prelude::Column::new(last.into(), &[2i32]),
            ],
        )
        .unwrap();
        let file = std::fs::File::create(sales.join(name)).unwrap();
        polars::prelude::ParquetWriter::new(file)
            .finish(&mut frame)
            .unwrap();
    }

    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.browsing = Some(tmp.path().to_path_buf());
    app.home.rebuild(&[]);

    let row = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Entry { entry, .. } if entry.name == "sales"))
        .expect("the directory is listed");
    app.home.selected = row;
    app.home.classify_now(8);
    app.home.measure_now(8);
    assert_eq!(
        app.home.selected_entry().map(|e| e.kind),
        Some(datui::home::discover::EntryKind::Directory),
        "neither file's columns are in the other's"
    );

    // One key in.
    app.event(key(KeyCode::Right));
    assert_eq!(app.home.browsing.as_deref(), Some(sales.as_path()));
    app.home.rebuild(&[]);

    // The second key opens the union.
    let row = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Door { .. }))
        .expect("the directory carries the row");
    app.home.selected = row;
    assert!(
        matches!(app.event(key(KeyCode::Enter)), Some(AppEvent::Open(..))),
        "the door opens what the rule declined to open in one key"
    );
}

/// `datui <dir>` does what `Enter` on that directory's row does.
///
/// A directory used to be `Unsupported file type` unless `--hive` was passed, while
/// pyarrow, Polars, pandas and Spark all open one. Naming a directory is the request to
/// read it, so the command line answers the same as the other two doors onto a path.
#[test]
fn test_the_command_line_reads_a_directory_the_way_enter_does() {
    let tmp = tempfile::TempDir::new().unwrap();
    let parquet = |dir: &Path, name: &str, mut frame: DataFrame| {
        std::fs::create_dir_all(dir).unwrap();
        ParquetWriter::new(File::create(dir.join(name)).unwrap())
            .finish(&mut frame)
            .unwrap();
    };
    let table = |cols: &[&str]| {
        DataFrame::new(
            1,
            cols.iter()
                .map(|c| Column::new((*c).into(), &[1i32]))
                .collect(),
        )
        .unwrap()
    };

    // An app and the channel its background work answers on. The look at a directory is
    // an event now, not a call, so the test drives the same chain `run()` does.
    let app = || {
        let (tx, rx) = mpsc::channel();
        (App::new(tx, common::test_runtime()), rx)
    };
    let named_with =
        |app: &mut App, rx: &mpsc::Receiver<AppEvent>, dir: &Path, options: OpenOptions| {
            let mut next = Some(AppEvent::OpenNamed(vec![dir.to_path_buf()], options));
            // Follow the chain to whatever it settles on: the look goes to a worker and
            // answers here, and what it answers with is the decision. Settling on the
            // home screen is an outcome, not a timeout — a helper that could only tell
            // the two apart by waiting would make every such case cost the wait, and
            // would quietly pass a chain that had stopped.
            loop {
                if app.home.browsing.is_some() {
                    return None;
                }
                match next.take() {
                    Some(AppEvent::Open(paths, options)) => {
                        return Some(AppEvent::Open(paths, options));
                    }
                    Some(ev) => next = app.event(ev),
                    None => match next_event(app, rx) {
                        Some(ev) => next = Some(ev),
                        None => panic!("the chain stopped without settling on anything"),
                    },
                }
            }
        };
    // The options the binary really passes, not `OpenOptions::default()`. They differ
    // in exactly the fields a guard over "did the user set anything" reads — the CLI
    // fills in `infer_schema_length` and `parse_strings` on every run with no flags —
    // so a test built on the defaults cannot see a guard that fires on every open.
    let as_the_binary_does = |dir: &Path| {
        use clap::Parser;
        let args =
            datui_cli::Args::try_parse_from(["datui", dir.to_str().unwrap()]).expect("parses");
        OpenOptions::from_args_and_config(&args, &datui::config::AppConfig::default())
    };
    let named = |app: &mut App, rx: &mpsc::Receiver<AppEvent>, dir: &Path| {
        let options = as_the_binary_does(dir);
        named_with(app, rx, dir, options)
    };

    // One table across several files: read as one, on the directory route.
    let one = tmp.path().join("one");
    parquet(&one, "a.parquet", table(&["id", "ts"]));
    parquet(&one, "b.parquet", table(&["id", "ts"]));
    let (mut a, rx_a) = app();
    match named(&mut a, &rx_a, &one) {
        Some(AppEvent::Open(paths, options)) => {
            assert_eq!(paths, vec![one.clone()]);
            assert!(
                options.hive,
                "the look-then-open route is what reads a directory"
            );
        }
        _ => panic!("a directory of one table opens as one table"),
    }

    // Separate tables: a place to look inside, browsed into rather than refused. The
    // `(all files)` row in there is the keystroke that unions them anyway.
    let several = tmp.path().join("several");
    parquet(&several, "by_block.parquet", table(&["block", "fee"]));
    parquet(
        &several,
        "daily.parquet",
        table(&["day", "price", "volume"]),
    );
    let (mut b, rx_b) = app();
    assert!(
        named(&mut b, &rx_b, &several).is_none(),
        "a directory of separate tables is somewhere to look, not a refusal"
    );
    assert_eq!(b.home.browsing.as_deref(), Some(several.as_path()));
    assert_eq!(b.input_mode, InputMode::Home);

    // A hive root reads as one table too, and still by the directory route.
    let hive = tmp.path().join("hive");
    parquet(&hive.join("day=1"), "part.parquet", table(&["id"]));
    parquet(&hive.join("day=2"), "part.parquet", table(&["id"]));
    let (mut c, rx_c) = app();
    assert!(
        matches!(named(&mut c, &rx_c, &hive), Some(AppEvent::Open(_, o)) if o.hive),
        "a hive root is read through its partitions"
    );

    // A lake root is not a directory of Parquet files, however much it looks like one.
    let delta = tmp.path().join("delta");
    std::fs::create_dir_all(delta.join("_delta_log")).unwrap();
    std::fs::write(
        delta.join("_delta_log").join("00000000000000000000.json"),
        "{}",
    )
    .unwrap();
    parquet(&delta, "part-00000.parquet", table(&["id"]));
    let (mut d, rx_d) = app();
    assert!(
        named(&mut d, &rx_d, &delta).is_none(),
        "it is not read as Parquet"
    );
    assert_eq!(d.home.browsing.as_deref(), Some(delta.as_path()));
    assert!(
        lake_heading(&mut d).contains("delta"),
        "and its heading says why"
    );

    // And the read really happens: the whole chain, look and all, is the table.
    let (mut loaded, rx) = app();
    let event = named(&mut loaded, &rx, &one).expect("a directory of one table opens");
    let AppEvent::Open(paths, options) = event else {
        panic!("the rule opens it")
    };
    pump_open_until_loaded(&mut loaded, &rx, paths, options);
    assert_eq!(
        loaded.data_table_state.as_ref().map(|s| s.num_rows()),
        Some(2),
        "one row from each file, read as one table"
    );

    // A file is untouched, and not even looked at: it goes straight to the open.
    assert!(matches!(
        App::route_named_paths(vec![one.join("a.parquet")], OpenOptions::default()),
        AppEvent::Open(..)
    ));
    // `--hive` is an answer already given, so it is not second-guessed either.
    let forced = OpenOptions {
        hive: true,
        ..OpenOptions::default()
    };
    assert!(
        matches!(
            App::route_named_paths(vec![several.clone()], forced),
            AppEvent::Open(..)
        ),
        "--hive still means read this as one, whatever the directory looks like"
    );

    // And so is `--no-header`. The rule takes each file's first row of data for its
    // column names, finds them all different and calls the directory separate tables — so
    // without this it sends the user to the home screen, for a directory the flag reads
    // perfectly as one table.
    let headless = tmp.path().join("headless");
    std::fs::create_dir_all(&headless).unwrap();
    std::fs::write(headless.join("a.csv"), "alice,30\nbob,25\n").unwrap();
    std::fs::write(headless.join("b.csv"), "carol,41\n").unwrap();
    let (mut g, rx_g) = app();
    let told = OpenOptions {
        has_header: Some(false),
        ..OpenOptions::default()
    };
    assert!(
        named_with(&mut g, &rx_g, &headless, told).is_some(),
        "the user has said how to read these; datui does not judge them by another reading"
    );
}

/// A directory of several formats is read as the commonest, and says what it left out.
///
/// Refusing the whole of a thousand CSVs over one stray JSON was datui deciding that a
/// directory it could read was not worth reading. It reads it now — and a read that
/// silently drops a file is the other half of the same mistake, so the dataset says
/// which formats were passed over and how many of each.
#[test]
fn test_a_mixed_directory_reads_as_the_commonest_format_and_says_what_it_left_out() {
    let tmp = tempfile::TempDir::new().unwrap();
    let dir = tmp.path();
    for name in ["a.csv", "b.csv", "c.csv"] {
        std::fs::write(dir.join(name), b"x,y\n1,2\n").unwrap();
    }
    std::fs::write(dir.join("notes.json"), b"{\"x\": 1}").unwrap();

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![dir.to_path_buf()],
        OpenOptions {
            hive: true,
            ..OpenOptions::default()
        },
    );

    let state = app
        .data_table_state
        .as_ref()
        .expect("the directory opens rather than being refused over the stray");
    assert_eq!(state.num_rows(), 3, "one row from each CSV");

    let notes = state.notes();
    let said = notes
        .iter()
        .find(|n| n.summary.contains("mixed formats"))
        .unwrap_or_else(|| panic!("the read says what it left out, got {notes:?}"));
    assert!(
        said.summary.contains("1 json"),
        "by format and count: {:?}",
        said.summary
    );
    assert!(
        state.has_notes(),
        "and the Info key offers it, which is the only way anyone finds out"
    );
}

/// A remote row datui has no reader for is named a file, not left Unknown.
///
/// `entry_for_path` had a name and nothing else to go on, and left anything whose
/// extension it did not recognize as `Unknown` — which → enters. So a Recent of
/// `s3://bucket/data.dat` took → into an empty prefix listing with no explanation and
/// Esc as the only way out (#283). Excluding `Unknown` from what → enters was the other
/// way to fix it, and it is the label deciding access one indirection along: before
/// anything has looked into it, every row on a share is `Unknown`, including every
/// directory that costs most to reach. So the row is named instead.
#[test]
fn test_a_remote_name_datui_cannot_read_is_still_a_file_not_a_prefix() {
    let kind = |url: &str| {
        let mut home = datui::home::HomeState {
            network_check: |_| true,
            ..Default::default()
        };
        home.rebuild(&[PathBuf::from(url)]);
        home.sections
            .iter()
            .flat_map(|s| s.rows.iter())
            .find(|r| r.path == Path::new(url))
            .map(|r| r.kind)
            .unwrap_or_else(|| panic!("{url} is listed under RECENT"))
    };

    // An extension datui reads: a file, as it always was.
    assert_eq!(
        kind("s3://bucket/data.psv"),
        datui::home::discover::EntryKind::File
    );
    // One it does not: still a file. It is certainly not a prefix.
    assert_eq!(
        kind("s3://bucket/data.dat"),
        datui::home::discover::EntryKind::File,
        "→ must not offer to go inside it"
    );
    // No extension: genuinely ambiguous — it may be a prefix, or a part file written
    // without one — so it stays Unknown and → goes in, which is the trade #279 made.
    assert_eq!(
        kind("s3://bucket/exports"),
        datui::home::discover::EntryKind::Unknown
    );
    // A trailing slash is a prefix whatever the name has in it.
    assert_eq!(
        kind("s3://bucket/2024.01.15/"),
        datui::home::discover::EntryKind::Unknown,
        "a dotted prefix is not a file"
    );
}

/// A directory of files written without extensions opens as one table.
///
/// Spark and GBIF both write part files with no extension. `occurrence.parquet/000001` is
/// read by its directory's name; the same files under a directory named anything else
/// were not data at all as far as datui was concerned — nothing listed them and nothing
/// opened them. The bytes say what the names do not. On a local disk a listing asks them
/// too, a few bytes a file, so the row agrees with the open.
#[test]
fn test_a_directory_of_files_written_without_extensions_still_opens() {
    let tmp = tempfile::TempDir::new().unwrap();
    let parts = tmp.path().join("parts");
    std::fs::create_dir_all(&parts).unwrap();
    let mut frame = DataFrame::new(1, vec![Column::new("id".into(), &[1i32])]).unwrap();
    for name in ["000000", "000001"] {
        ParquetWriter::new(File::create(parts.join(name)).unwrap())
            .finish(&mut frame)
            .unwrap();
    }

    // No name in there says data, and the signatures do.
    assert_eq!(
        datui::home::discover::classify_directory(&parts),
        datui::home::discover::EntryKind::MultiFile,
        "the bytes say one Parquet table"
    );

    // And the read finds them anyway.
    match datui::home::discover::directory_format(&parts) {
        datui::home::discover::DirectoryFormat::One(format, files) => {
            assert_eq!(format, datui::FileFormat::Parquet);
            assert_eq!(files.len(), 2, "both of them");
        }
        other => panic!("the bytes say Parquet, got {other:?}"),
    }

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![parts.clone()],
        OpenOptions {
            hive: true,
            ..OpenOptions::default()
        },
    );
    assert_eq!(
        app.data_table_state.as_ref().map(|s| s.num_rows()),
        Some(2),
        "one row from each part"
    );

    // And it is reachable from the home screen: the directory is a place to look inside,
    // and the door inside it reads the whole of what it holds. Two keys, which is the
    // rule for every directory the nesting test turns away — not a dead end, which is
    // what a directory nothing listed and nothing opened was.
    let (tx, _rx) = mpsc::channel();
    let mut home = App::new(tx, common::test_runtime());
    home.enter_home();
    home.home.browsing = Some(parts.clone());
    home.home.rebuild(&[]);
    let door = home
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Door { .. }))
        .expect("the directory carries the door");
    home.home.selected = door;
    assert!(
        matches!(home.event(key(KeyCode::Enter)), Some(AppEvent::Open(..))),
        "the door opens it"
    );

    // A directory whose names do say something is not opened file by file to find out.
    // `LICENSE` beside the Parquet is not sniffed, and not read.
    let named = tmp.path().join("named");
    std::fs::create_dir_all(&named).unwrap();
    ParquetWriter::new(File::create(named.join("a.parquet")).unwrap())
        .finish(&mut frame)
        .unwrap();
    std::fs::write(named.join("LICENSE"), b"MIT").unwrap();
    match datui::home::discover::directory_format(&named) {
        datui::home::discover::DirectoryFormat::One(datui::FileFormat::Parquet, files) => {
            assert_eq!(files.len(), 1, "the LICENSE is not one of them");
        }
        other => panic!("the names settled it, got {other:?}"),
    }
}

/// The nesting rule reaches the formats that have no footer.
///
/// A directory of forty unrelated CSVs was labelled `40 csv`, `Enter` promised one table
/// because nothing had looked, and the read then refused it — the permissive rule with
/// the strict reader, which is the pairing #275 exists to stop. The names at the front
/// of a CSV are the same evidence `is_nested` takes from a Parquet footer, so the same
/// rule now answers for both.
#[test]
fn test_a_directory_of_csv_is_judged_by_its_headers_like_one_of_parquet() {
    let tmp = tempfile::TempDir::new().unwrap();
    let directory = |name: &str, files: &[&str]| {
        let dir = tmp.path().join(name);
        std::fs::create_dir_all(&dir).unwrap();
        for (i, body) in files.iter().enumerate() {
            std::fs::write(dir.join(format!("part-{i}.csv")), body).unwrap();
        }
        dir
    };
    let looked_at = |dir: &Path| {
        let mut entry = datui::home::discover::Entry::directory(dir);
        entry.kind = datui::home::discover::EntryKind::Unknown;
        datui::home::look_into_as(&entry, &Default::default())
    };

    // Files that agree: one table, as before.
    let same = directory("same", &["a,b\n1,2\n", "a,b\n3,4\n", "a,b\n5,6\n"]);
    assert_eq!(
        looked_at(&same).kind,
        datui::home::discover::EntryKind::MultiFile,
        "identical headers are one table"
    );

    // A column added along the way: still one table. This is the shape the rule is for,
    // and the one a stricter test would refuse.
    let drift = directory(
        "drift",
        &["id,ts\n1,5\n", "id,ts\n2,6\n", "id,ts,region\n3,7,eu\n"],
    );
    assert_eq!(
        looked_at(&drift).kind,
        datui::home::discover::EntryKind::MultiFile,
        "a column added later is schema drift, not separate tables"
    );

    // Separate tables: somewhere to look inside, not one table.
    let apart = directory("apart", &["a,b\n1,2\n", "x,y,z\n3,4,5\n", "q\n9\n"]);
    let judged = looked_at(&apart);
    assert_eq!(
        judged.kind,
        datui::home::discover::EntryKind::Directory,
        "files that each bring something the others lack are not one table"
    );
    assert_eq!(
        judged.rows, None,
        "and no row count, which would be a sum of unrelated things"
    );
    assert!(
        judged.columns.iter().any(|c| c == "q"),
        "but the union of columns, so a column search still finds the directory: {:?}",
        judged.columns
    );

    // A headerless file gives its first row of *data* as the names, because datui
    // reads a CSV as having a header. datui cannot read such a directory as one table at
    // all without `--no-header`: every file would contribute its own first row as
    // column names and the union would be a wide sheet of nulls. So it is not offered
    // as one — and the data values are not offered as column names either.
    let headless = directory("headless", &["1,2\n3,4\n", "5,6\n", "7,8\n"]);
    let judged = looked_at(&headless);
    assert_eq!(
        judged.kind,
        datui::home::discover::EntryKind::Directory,
        "a directory datui can only read as nulls is not one table"
    );
    assert!(
        judged.columns.is_empty(),
        "and data values are not offered as column names: {:?}",
        judged.columns
    );
    // Opened anyway, through the door, it says what it is seeing and what to do.
    let said = datui::notes::from_the_open(
        &[],
        None,
        datui::formats::schema_union::Disagreement {
            headerless: true,
            ..Default::default()
        },
        false,
    );
    assert!(
        said.iter()
            .any(|n| n.summary.contains("no header row") && n.summary.contains("--no-header")),
        "got {said:?}"
    );

    // Compression does not hide the header: it is still at the front of the file.
    let zipped = tmp.path().join("zipped");
    std::fs::create_dir_all(&zipped).unwrap();
    for (name, body) in [("a.csv.gz", "a,b\n1,2\n"), ("b.csv.gz", "x,y,z\n3,4,5\n")] {
        let file = File::create(zipped.join(name)).unwrap();
        let mut gz = flate2::write::GzEncoder::new(file, flate2::Compression::default());
        std::io::Write::write_all(&mut gz, body.as_bytes()).unwrap();
        gz.finish().unwrap();
    }
    assert_eq!(
        looked_at(&zipped).kind,
        datui::home::discover::EntryKind::Directory,
        "a gzipped CSV is judged by its header like any other"
    );

    // Two files are the fewest that can disagree; one decides nothing.
    let alone = directory("alone", &["a,b\n1,2\n"]);
    assert_eq!(
        looked_at(&alone).kind,
        datui::home::discover::EntryKind::Directory,
        "a directory of one data file was never a multi-file dataset"
    );
}

/// Naming a path on the command line draws a frame before anything asks the filesystem
/// about it.
///
/// Looking at a directory reads footers, or the front of a spread of its files, and for a
/// directory of large Parquet that is seconds — 4.6 of them on a real one, and seventeen
/// on one with a deep subtree. It used to happen in `run()` before the first
/// `terminal.draw`, so the whole of it was a blank terminal: no name, no spinner, and
/// no key that worked. So did the `exists` and `is_dir` that decide whether there is
/// anything to look at, and a local-looking path can be a mount that does not answer.
/// All of it is a worker's now, after the first frame.
#[test]
fn test_looking_at_a_directory_happens_after_the_first_frame() {
    let tmp = tempfile::TempDir::new().unwrap();
    let directory = tmp.path().join("data");
    std::fs::create_dir_all(&directory).unwrap();
    std::fs::write(directory.join("a.csv"), "a,b\n1,2\n").unwrap();

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    let follow_up = app.event(AppEvent::OpenNamed(
        vec![directory.clone()],
        OpenOptions::default(),
    ));
    // Handling it decided nothing: no home screen, no load, and a spinner while a
    // worker asks the filesystem.
    assert!(follow_up.is_none(), "the question goes to a worker");
    assert!(app.is_busy(), "and the screen says something is happening");
    assert!(
        app.home.browsing.is_none() && app.data_table_state.is_none(),
        "nothing has been opened or browsed into"
    );

    // The worker's answer is that a directory was named, which is looked at next.
    let mut next = None;
    while next.is_none() {
        let event = next_event(&app, &rx).expect("the worker answers");
        next = app.event(event);
    }
    match next {
        Some(AppEvent::LookThenOpenDirectory(dir, _)) => assert_eq!(dir, directory),
        _ => panic!("a named directory is looked at before it is opened"),
    }

    // A file is not looked at at all — there is nothing to find out — so it goes
    // straight to the open.
    assert!(matches!(
        App::route_named_paths(vec![directory.join("a.csv")], OpenOptions::default()),
        AppEvent::Open(..)
    ));
}

/// A CSV setting does not decide a directory of Parquet.
///
/// `has_header` and the skips are reader settings for delimited text. Nothing about
/// them can change how a Parquet file is read, so a directory of Parquet must reach the
/// same verdict whatever they say — which it does because the rule reads Parquet
/// through its footers and the settings never touch that path.
#[test]
fn test_a_csv_setting_does_not_decide_a_directory_of_parquet() {
    let tmp = tempfile::TempDir::new().unwrap();
    let apart = tmp.path().join("apart");
    std::fs::create_dir_all(&apart).unwrap();
    for (name, cols) in [
        ("by_block.parquet", vec!["block", "fee"]),
        ("daily.parquet", vec!["day", "price", "volume"]),
    ] {
        let mut frame = DataFrame::new(
            1,
            cols.iter()
                .map(|c| Column::new((*c).into(), &[1i32]))
                .collect(),
        )
        .unwrap();
        ParquetWriter::new(File::create(apart.join(name)).unwrap())
            .finish(&mut frame)
            .unwrap();
    }

    for options in [
        OpenOptions::default(),
        OpenOptions {
            has_header: Some(false),
            skip_rows: Some(3),
            ..OpenOptions::default()
        },
    ] {
        let (tx, rx) = mpsc::channel();
        let mut app = App::new(tx, common::test_runtime());
        let mut next = Some(AppEvent::OpenNamed(vec![apart.clone()], options));
        loop {
            if app.home.browsing.is_some() {
                break;
            }
            match next.take() {
                Some(AppEvent::Open(..)) => {
                    panic!("a CSV setting cannot make two Parquet tables into one")
                }
                Some(ev) => next = app.event(ev),
                None => match next_event(&app, &rx) {
                    Some(ev) => next = Some(ev),
                    None => panic!("the chain stopped without settling"),
                },
            }
        }
        assert_eq!(app.home.browsing.as_deref(), Some(apart.as_path()));
    }
}

/// A directory with no data files in it is never forced down the one-table route.
///
/// `EntryKind::Directory` covers a directory of separate tables *and* one with nothing
/// readable in it. Opening the second as one table reaches the Parquet hive scan on a
/// tree that has no Parquet, so `datui ~/src --no-header` ended in an error modal
/// rather than the home screen it used to give.
#[test]
fn test_a_directory_with_no_data_is_not_forced_open() {
    let tmp = tempfile::TempDir::new().unwrap();
    let src = tmp.path().join("src");
    std::fs::create_dir_all(src.join("nested")).unwrap();
    std::fs::write(src.join("main.rs"), "fn main() {}\n").unwrap();

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    let mut next = Some(AppEvent::OpenNamed(
        vec![src.clone()],
        OpenOptions {
            has_header: Some(false),
            ..OpenOptions::default()
        },
    ));
    loop {
        if app.home.browsing.is_some() {
            break;
        }
        match next.take() {
            Some(AppEvent::Open(..)) => {
                panic!("a directory with nothing readable in it has no table to open")
            }
            Some(ev) => next = app.event(ev),
            None => match next_event(&app, &rx) {
                Some(ev) => next = Some(ev),
                None => panic!("the chain stopped without settling"),
            },
        }
    }
    assert_eq!(app.home.browsing.as_deref(), Some(src.as_path()));
}

/// `--delimiter` is what the file is read with (#290), on every CSV route: one file,
/// several, and a compressed one read both lazily and in memory.
#[test]
fn test_delimiter_flag_splits_the_columns() {
    common::isolate_cache();
    let tmp = tempfile::TempDir::new().unwrap();
    let body = "id|name|city\n1|ann|oslo\n2|bob|rome\n";
    let one = tmp.path().join("one.csv");
    let two = tmp.path().join("two.csv");
    std::fs::write(&one, body).unwrap();
    std::fs::write(&two, body).unwrap();
    let gz = tmp.path().join("zipped.csv.gz");
    let mut enc =
        flate2::write::GzEncoder::new(File::create(&gz).unwrap(), flate2::Compression::default());
    std::io::Write::write_all(&mut enc, body.as_bytes()).unwrap();
    enc.finish().unwrap();

    let (_, df) = open_and_collect(
        vec![one.clone()],
        options_as_the_binary_does(&["datui", "x"], ""),
    );
    assert_eq!(names(&df), ["id|name|city"], "without the flag: one column");

    let with_flag = |extra: &[&str]| {
        let mut argv = vec!["datui", "x", "--delimiter", "|"];
        argv.extend_from_slice(extra);
        options_as_the_binary_does(&argv, "")
    };
    use datui::ReadMode::{Decompressed, InMemory, Lazy};
    for (what, paths, opts, read) in [
        ("one file", vec![one.clone()], with_flag(&[]), Lazy),
        (
            "two files",
            vec![one.clone(), two.clone()],
            with_flag(&[]),
            Lazy,
        ),
        (
            "gzip, lazily",
            vec![gz.clone()],
            with_flag(&[]),
            Decompressed,
        ),
        (
            "gzip, in memory",
            vec![gz.clone()],
            with_flag(&["-c", "read.decompress_in_memory=true"]),
            InMemory,
        ),
    ] {
        let rows = 2 * paths.len();
        let (app, df) = open_and_collect(paths, opts);
        assert_eq!(names(&df), ["id", "name", "city"], "{what}");
        assert_eq!(df.height(), rows, "{what}");
        // How the Info panel says it is read.
        let state = app.data_table_state.as_ref().unwrap();
        assert_eq!(state.read_mode(), Some(read), "{what}");
    }
}

/// Every delimited format opens with every compression (#567): by its name
/// (`x.tsv.gz`), by `--format` beside a compression suffix, and by `--format` and
/// `--compression` on a name that says neither; decompressed lazily and in memory.
#[test]
fn test_every_delimited_format_opens_with_every_compression() {
    common::isolate_cache();
    let tmp = tempfile::TempDir::new().unwrap();
    for (format, sep) in [("csv", ','), ("tsv", '\t'), ("psv", '|')] {
        let body = format!("id{sep}name\n1{sep}ann\n2{sep}bob\n");
        for (ext, compression) in [
            ("gz", "gzip"),
            ("zst", "zstd"),
            ("bz2", "bzip2"),
            ("xz", "xz"),
        ] {
            let bytes = compress(ext, body.as_bytes());
            let named = tmp.path().join(format!("data.{format}.{ext}"));
            let bare = tmp.path().join(format!("{format}-{ext}.bin"));
            std::fs::write(&named, &bytes).unwrap();
            std::fs::write(&bare, &bytes).unwrap();
            let named_arg = named.to_str().unwrap();
            let bare_arg = bare.to_str().unwrap();
            for in_memory in [false, true] {
                let mut flagged = vec!["datui"];
                if in_memory {
                    flagged.extend(["-c", "read.decompress_in_memory=true"]);
                }
                for (what, path, extra) in [
                    ("by name", &named, vec![named_arg]),
                    ("--format", &named, vec![named_arg, "--format", format]),
                    (
                        "--format --compression",
                        &bare,
                        vec![bare_arg, "--format", format, "--compression", compression],
                    ),
                ] {
                    let mut argv = flagged.clone();
                    argv.extend(extra);
                    let (_, df) =
                        open_and_collect(vec![path.clone()], options_as_the_binary_does(&argv, ""));
                    let case = format!("{format}.{ext} {what} (in memory: {in_memory})");
                    assert_eq!(names(&df), ["id", "name"], "{case}");
                    assert_eq!(df.height(), 2, "{case}");
                }
            }
        }
    }
}

/// A directory holding one compressed delimited file opens as that file (#576), named
/// as `datui --hive dir/` names it. The scan used to decompress it into a copy it then
/// dropped, so the frame scanned a file that was gone; it is decompressed by the load,
/// which keeps the copy with the dataset.
#[test]
fn test_a_directory_of_one_compressed_delimited_file_opens() {
    common::isolate_cache();
    let tmp = tempfile::TempDir::new().unwrap();
    for (format, sep) in [("csv", ','), ("tsv", '\t'), ("psv", '|')] {
        let body = format!("id{sep}name\n1{sep}ann\n2{sep}bob\n");
        for ext in ["gz", "zst", "bz2", "xz"] {
            let case = format!("{format}.{ext}");
            let dir = tmp.path().join(format!("{format}-{ext}"));
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(
                dir.join(format!("data.{format}.{ext}")),
                compress(ext, body.as_bytes()),
            )
            .unwrap();

            let (tx, rx) = mpsc::channel();
            let mut app = App::new(tx, common::test_runtime());
            let mut next = app.event(AppEvent::OpenNamed(
                vec![dir.clone()],
                OpenOptions {
                    hive: true,
                    ..OpenOptions::default()
                },
            ));
            loop {
                match next.take() {
                    Some(AppEvent::Crash(message)) => panic!("{case}: {message}"),
                    Some(event) => next = app.event(event),
                    None => match next_event(&app, &rx) {
                        Some(event) => next = Some(event),
                        None => break,
                    },
                }
            }
            let state = app.data_table_state.as_ref().unwrap_or_else(|| {
                panic!(
                    "{case}: the directory opened ({:?}, {:?})",
                    app.input_mode,
                    app.error_message()
                )
            });
            assert!(
                state.scans_a_temp_file(),
                "{case}: the dataset holds its copy"
            );
            let df = state.lf().clone().collect().unwrap();
            assert_eq!(names(&df), ["id", "name"], "{case}");
            assert_eq!(df.height(), 2, "{case}");
        }
    }
}

/// A file's layout in `[csv]` does not reach the open (#289). It used to
/// apply to every file, and `skip_rows = 2` turned this one's third row into its header.
#[test]
fn test_layout_keys_in_config_do_not_reach_the_open() {
    common::isolate_cache();
    let tmp = tempfile::TempDir::new().unwrap();
    let csv = tmp.path().join("plain.csv");
    std::fs::write(&csv, "id,name\n1,ann\n2,bob\n3,cid\n").unwrap();
    let config = "[csv]\n\
                  delimiter = 59\n\
                  has_header = false\n\
                  skip_lines = 1\n\
                  skip_rows = 2\n\
                  skip_tail_rows = 1\n";

    let opts = options_as_the_binary_does(&["datui", "x"], config);
    assert_eq!(opts.delimiter, None);
    assert_eq!(opts.has_header, None);
    assert_eq!(opts.skip_lines, None);
    assert_eq!(opts.skip_rows, None);
    assert_eq!(opts.skip_tail_rows, None);

    let (_, df) = open_and_collect(vec![csv], opts);
    assert_eq!(names(&df), ["id", "name"]);
    assert_eq!(df.height(), 3);
}

/// TSV and PSV are read by the CSV reader, so the CSV options mean the same for them:
/// trimmed names, null values, the tail skip. They used to honor only the header and
/// the leading skips.
#[test]
fn test_tsv_and_psv_take_every_csv_option() {
    common::isolate_cache();
    let tmp = tempfile::TempDir::new().unwrap();
    for (name, sep) in [("data.tsv", "\t"), ("data.psv", "|")] {
        let path = tmp.path().join(name);
        let body = format!("id{sep} name\n1{sep}NA\n2{sep}bob\n3{sep}FOOTER\n");
        std::fs::write(&path, body).unwrap();
        let opts =
            options_as_the_binary_does(&["datui", "x", "--null", "NA", "--footer-rows", "1"], "");
        let (_, df) = open_and_collect(vec![path], opts);
        assert_eq!(names(&df), ["id", "name"], "{name}");
        assert_eq!(df.height(), 2, "{name}");
        assert_eq!(df.column("name").unwrap().null_count(), 1, "{name}");
    }
}

/// `H` on the Schema tab reads a headerless CSV's first row as data, under generated
/// names, and back. The Info notes say so first: every column name a number is a
/// first row of data.
#[test]
fn h_turns_a_csv_header_off_and_on() {
    common::isolate_cache();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("adult.csv");
    std::fs::write(&path, "39,77516\n50,83311\n38,215646\n").unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    settle_from(
        &mut app,
        &rx,
        AppEvent::Open(vec![path], OpenOptions::default()),
    );
    assert_eq!(column_names(&app), ["39", "77516"]);
    let notes = app.data_table_state.as_ref().unwrap().notes();
    assert!(
        notes.iter().any(|n| n.summary.contains("H on Schema")),
        "{notes:?}"
    );

    header_from_schema_tab(&mut app, &rx);
    assert!(app.at_table(), "the read takes the screen");
    assert_eq!(column_names(&app), ["column_1", "column_2"]);
    let notes = app.data_table_state.as_ref().unwrap().notes();
    assert!(
        !notes.iter().any(|n| n.summary.contains("H on Schema")),
        "read as data, there is nothing to say: {notes:?}"
    );

    header_from_schema_tab(&mut app, &rx);
    assert_eq!(column_names(&app), ["39", "77516"]);

    // At the table, H moves a column now; it reads nothing again.
    settle_from(&mut app, &rx, key(KeyCode::Char('l')));
    settle_from(&mut app, &rx, key(KeyCode::Char('H')));
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.headers(), ["77516", "39"]);
    assert_eq!(column_names(&app), ["39", "77516"], "the schema is as read");
}

/// A format that carries its own column names has no header to turn off: the Schema
/// tab's `H` does nothing and its footer does not offer it.
#[test]
fn h_does_nothing_on_parquet() {
    common::ensure_sample_data();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    let path = PathBuf::from("tests/sample-data/people.parquet");
    settle_from(
        &mut app,
        &rx,
        AppEvent::Open(vec![path], OpenOptions::default()),
    );
    let before = column_names(&app);
    assert!(!app.header_toggle_offered());
    assert!(app.event(key(KeyCode::Char('i'))).is_none());
    assert!(app.event(key(KeyCode::Char('H'))).is_none());
    assert!(!app.is_busy());
    assert_eq!(
        app.overlay,
        Overlay::Info,
        "nothing to read, the panel stays"
    );
    assert_eq!(column_names(&app), before);
}

/// A catalog file downloaded without a question that runs past its limit stops and
/// asks once; agreed to, it is fetched whole and opens, and nothing asks again.
#[cfg(feature = "http")]
#[test]
fn an_unasked_download_past_its_limit_asks_once() {
    use datui::UnaskedDownload;
    use std::sync::atomic::Ordering;
    common::isolate_cache();
    let mut body = b"id,name\n".to_vec();
    for i in 0..300 {
        body.extend_from_slice(format!("{i},row\n").as_bytes());
    }
    let (url, fetched) = serve_over_http_unsized("growing.csv", body);
    let dir = tempfile::tempdir().unwrap();
    let options = OpenOptions {
        temp_dir: Some(dir.path().to_path_buf()),
        download_unasked: Some(UnaskedDownload {
            limit: 1_000,
            listed: Some(500),
        }),
        ..OpenOptions::default()
    };

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    let mut asked = Vec::new();
    let mut next = Some(AppEvent::Open(vec![PathBuf::from(&url)], options));
    loop {
        match next.take() {
            Some(ev) => next = app.event(ev),
            None if app.awaiting_open_confirmation() => {
                asked.push((
                    app.confirmation_modal.message.clone(),
                    fetched.load(Ordering::SeqCst),
                ));
                next = Some(key(KeyCode::Enter));
            }
            None => match next_event(&app, &rx) {
                Some(ev) => next = Some(ev),
                None => break,
            },
        }
    }
    assert_eq!(asked.len(), 1, "asked once: {asked:?}");
    let (message, downloads) = &asked[0];
    assert_eq!(*downloads, 1, "it started without asking");
    assert!(message.contains("passed 50 MB"), "{message}");
    assert!(message.contains("File size: unknown"), "{message}");
    assert_eq!(
        fetched.load(Ordering::SeqCst),
        2,
        "fetched whole once agreed"
    );
    assert_eq!(column_names(&app), ["id", "name"]);
    assert_eq!(app.error_message(), None);
}

/// Ctrl+O while an HTTP server has gone quiet mid-body stops the download and
/// removes its file, long before the server would have sent the rest.
#[cfg(feature = "http")]
#[test]
fn an_abandoned_http_download_stops_while_the_server_is_silent() {
    use std::time::{Duration, Instant};
    common::isolate_cache();
    let quiet = Duration::from_secs(20);
    let mut body = b"id,name\n".to_vec();
    body.resize(64 * 1024, b'x');
    let (url, _) = serve_over_http_stalling("stalled.csv", body, Some((1024, quiet)));
    let dir = tempfile::tempdir().unwrap();
    let options = OpenOptions {
        temp_dir: Some(dir.path().to_path_buf()),
        ..OpenOptions::default()
    };
    // Sizes from the files, not their directory entries: Windows updates an entry's
    // size only when the writer closes the file. One gone since the listing is gone.
    let files = || {
        std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|f| std::fs::metadata(f.unwrap().path()).ok())
            .map(|m| m.len())
            .collect::<Vec<_>>()
    };

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut next = Some(AppEvent::Open(vec![PathBuf::from(&url)], options));
    while files() != [1024] {
        assert!(Instant::now() < deadline, "the first KiB never landed");
        next = match next.take() {
            Some(event) => app.event(event),
            None if app.awaiting_open_confirmation() => Some(key(KeyCode::Enter)),
            None => rx.recv_timeout(Duration::from_millis(10)).ok(),
        };
    }

    let began = Instant::now();
    let mut next = Some(ctrl_o());
    while let Some(event) = next {
        next = app.event(event);
    }
    while !files().is_empty() {
        assert!(
            began.elapsed() < quiet / 4,
            "still downloading: {:?}",
            files()
        );
        // Nothing reports the file's removal; the time it may take is the assertion.
        std::thread::sleep(Duration::from_millis(10));
    }
    // The stopped worker reports, for a load nobody is waiting on.
    loop {
        let event = rx
            .recv_timeout(Duration::from_secs(10))
            .expect("the stopped download reports");
        let failed = matches!(event, AppEvent::JobEnded(t) if t.kind() == JobKind::Load);
        let mut next = Some(event);
        while let Some(event) = next {
            next = app.event(event);
        }
        if failed {
            break;
        }
    }
    assert_eq!(app.error_message(), None);
}

/// Quitting while an HTTP server has gone quiet mid-body removes the partial file
/// before the session ends, rather than leaving it to a worker the process may not
/// wait for (#510).
#[cfg(feature = "http")]
#[test]
fn quitting_mid_http_download_removes_the_partial_file() {
    use std::time::{Duration, Instant};
    common::isolate_cache();
    let mut body = b"id,name\n".to_vec();
    body.resize(64 * 1024, b'x');
    let (url, _) = serve_over_http_stalling(
        "quit_mid_download.csv",
        body,
        Some((1024, Duration::from_secs(20))),
    );
    let dir = tempfile::tempdir().unwrap();
    let options = OpenOptions {
        temp_dir: Some(dir.path().to_path_buf()),
        ..OpenOptions::default()
    };
    let files = || {
        std::fs::read_dir(dir.path())
            .unwrap()
            // The file's size, not the directory entry's: on Windows that changes only
            // when the writer closes the file.
            .map(|f| std::fs::metadata(f.unwrap().path()).map_or(0, |m| m.len()))
            .collect::<Vec<_>>()
    };
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut next = Some(AppEvent::Open(vec![PathBuf::from(&url)], options));
    while files() != [1024] {
        assert!(Instant::now() < deadline, "the first KiB never landed");
        next = match next.take() {
            Some(event) => app.event(event),
            None if app.awaiting_open_confirmation() => Some(key(KeyCode::Enter)),
            None => rx.recv_timeout(Duration::from_millis(10)).ok(),
        };
    }

    let sweep = app.exit_sweep();
    drop(app);
    drop(sweep);
    assert!(files().is_empty(), "left behind: {:?}", files());
}

/// A compressed CSV over HTTP is downloaded and decompressed into temporary files, but
/// the dataset is the URL: the header, the Info panel and a view saved on it name that.
#[cfg(feature = "http")]
#[test]
fn a_compressed_csv_over_http_is_its_url() {
    use flate2::{Compression, write::GzEncoder};
    use std::io::Write;
    common::isolate_cache();
    let mut gz = GzEncoder::new(Vec::new(), Compression::default());
    gz.write_all(b"id,name\n1,a\n2,b\n").unwrap();
    let (url, _) = serve_over_http("http_gz_location.csv.gz", gz.finish().unwrap());

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    settle_from(
        &mut app,
        &rx,
        AppEvent::Open(vec![PathBuf::from(&url)], OpenOptions::default()),
    );
    assert_eq!(column_names(&app), ["id", "name"]);
    assert_eq!(app.open_path(), Some(Path::new(&url)));

    // Reread from the copy on hand, still under the URL.
    header_from_schema_tab(&mut app, &rx);
    assert_eq!(column_names(&app), ["column_1", "column_2"]);
    assert_eq!(app.open_path(), Some(Path::new(&url)));

    app.data_table_state
        .as_mut()
        .unwrap()
        .sort_by(vec!["column_1".to_string()], vec![true]);
    app.event(key(KeyCode::Char('v')));
    app.event(key(KeyCode::Char('s')));
    assert_eq!(app.view_modal.name_input.value(), "http_gz_location.csv");
    assert_eq!(app.view_modal.exact_path_input.value(), url);
}

/// An Arrow IPC stream over HTTP is downloaded, then converted, and the dataset is
/// the URL. Only the converted copy is kept, and read again: the download goes once it
/// is converted.
#[cfg(feature = "http")]
#[test]
fn an_arrow_stream_over_http_is_converted_and_named_by_its_url() {
    common::isolate_cache();
    common::ensure_sample_data();
    let body = std::fs::read("tests/sample-data/people_stream.arrow").unwrap();
    let (url, fetched) = serve_over_http("people_stream.arrow", body);
    let scratch = tempfile::tempdir().unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    let options = OpenOptions {
        temp_dir: Some(scratch.path().to_path_buf()),
        ..OpenOptions::default()
    };
    settle_from(
        &mut app,
        &rx,
        AppEvent::Open(vec![PathBuf::from(&url)], options),
    );
    let state = app.data_table_state.as_ref().expect("the stream opens");
    assert_eq!(state.num_rows(), 1000);
    assert_eq!(app.open_path(), Some(Path::new(&url)));
    assert_eq!(files_in(scratch.path()), 1, "the converted copy alone");
    assert_eq!(state.read_mode(), Some(datui::ReadMode::Converted));
    assert!(state.fetched(), "downloaded, then converted");

    settle_from(
        &mut app,
        &rx,
        AppEvent::Open(vec![PathBuf::from(&url)], OpenOptions::default()),
    );
    assert_eq!(app.data_table_state.as_ref().unwrap().num_rows(), 1000);
    assert_eq!(fetched.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(files_in(scratch.path()), 1, "the copy, not converted again");
}

/// A download that will not read is named by the URL in the error, not by the temp
/// file it landed in: a Parquet that is not one, which fails its scan, and JSON that
/// is not JSON (#511).
#[cfg(feature = "http")]
#[test]
fn a_download_that_will_not_read_is_named_by_its_url() {
    common::isolate_cache();
    let dir = tempfile::tempdir().unwrap();
    for (name, body) in [
        ("broken_511.parquet", b"id,v\n1,2\n".to_vec()),
        ("broken_511.json", b"{not json".to_vec()),
    ] {
        let (url, _) = serve_over_http(name, body);
        let options = OpenOptions {
            temp_dir: Some(dir.path().to_path_buf()),
            ..OpenOptions::default()
        };
        let (tx, rx) = mpsc::channel();
        let mut app = App::new(tx, common::test_runtime());
        settle_from(
            &mut app,
            &rx,
            AppEvent::Open(vec![PathBuf::from(&url)], options),
        );
        let message = app.error_message().expect("the open failed");
        assert!(message.contains(&url), "{name}: {message}");
        assert!(
            !message.contains(&*dir.path().to_string_lossy()),
            "{name}: {message}"
        );
    }
}

/// Frozen columns too wide for the window (#462): the table keeps as many frozen as
/// fit beside a usable scrolling column and breaks the rule to say so; the freeze
/// stays set, Sort & Filter still changes it at that size, and a wider window
/// freezes every column asked for again.
#[test]
fn a_freeze_survives_a_narrow_window() {
    let names = ["alpha", "bravo", "charlie", "delta", "echo", "foxtrot"];
    let test_data_dir = common::fixture_dir();
    let csv_path = test_data_dir.join("frozen_narrow_window.csv");
    let columns: Vec<Column> = names
        .iter()
        .map(|n| {
            let values: Vec<String> = (0..50).map(|i| format!("{n}-value-{i:03}")).collect();
            Series::new((*n).into(), values).into()
        })
        .collect();
    let mut df = DataFrame::new_infer_height(columns).unwrap();
    CsvWriter::new(&mut File::create(&csv_path).unwrap())
        .finish(&mut df)
        .unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![csv_path], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);

    let g = datui::glyphs::get();
    let draw = |app: &mut App, width: u16, height: u16| {
        app.event(AppEvent::Resize(width, height));
        let area = Rect::new(0, 0, width, height);
        let mut buf = Buffer::empty(area);
        app.render(area, &mut buf);
        common::buffer_text(&buf)
    };
    let freeze_through = |app: &mut App, name: &str| {
        open_columns_list(app);
        let sort = &mut app.sort_filter_modal.sort;
        let row = sort
            .filtered_columns()
            .iter()
            .position(|(_, c)| c.name == name)
            .unwrap();
        sort.table_state.select(Some(row));
        press(app, KeyCode::Char('L'));
    };
    let apply = |app: &mut App| {
        if let Some(next) = press(app, KeyCode::Enter) {
            let _ = tx.send(next);
        }
        pump_until_idle(app, &rx, &tx);
    };

    draw(&mut app, 60, 20);
    freeze_through(&mut app, "delta");
    apply(&mut app);
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.locked_columns_count(), 4);

    let narrow = draw(&mut app, 60, 20);
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.locked_columns_count(), 4, "the freeze is kept");
    assert!(state.frozen_shown() < 4, "{narrow}");
    assert!(narrow.contains(g.rule_broken), "{narrow}");
    // A view saved now keeps the freeze asked for, not what this window fits.
    let view = app
        .create_view_from_current_state(
            "narrow".to_string(),
            None,
            datui::view::MatchCriteria {
                exact_path: None,
                relative_path: None,
                path_pattern: None,
                filename_pattern: None,
                schema_columns: None,
                schema_types: None,
                table: None,
            },
        )
        .unwrap();
    assert_eq!(view.settings.locked_columns_count, 4);

    // Sort & Filter opens and changes the freeze at this size: L on a frozen
    // column pulls the boundary back to it.
    freeze_through(&mut app, "delta");
    let with_sidebar = draw(&mut app, 60, 20);
    assert_eq!(app.overlay, Overlay::SortFilter);
    assert!(with_sidebar.contains("Sort & Filter"), "{with_sidebar}");
    apply(&mut app);
    assert_eq!(
        app.data_table_state
            .as_ref()
            .unwrap()
            .locked_columns_count(),
        3
    );

    let wide = draw(&mut app, 160, 30);
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.frozen_shown(), 3, "{wide}");
    assert!(wide.contains(g.rule), "{wide}");
    assert!(!wide.contains(g.rule_broken), "{wide}");
}

/// Every dialog takes the same keys (`datui::form`): ↓ / ↑ move between fields from
/// the moment it opens, ← / → and Space step a choice, Space toggles a checkbox.
#[test]
fn every_dialog_moves_between_fields_with_the_arrows_on_open() {
    use datui::copy_modal::{CopyFocus, CopyScope};
    use datui::export_modal::{ExportFocus, ExportFormat};
    use datui::pivot_melt_modal::{PivotMeltFocus, PivotMeltTab};
    let (mut app, _rx, _tx) = open_query_filter_fixture("forms_arrows_on_open.csv");

    // Export opens on the path; ↓ is the delimiter, ↑ ↑ the format.
    press(&mut app, KeyCode::Char('e'));
    assert_eq!(app.export_modal.focus, ExportFocus::PathInput);
    press(&mut app, KeyCode::Down);
    assert_eq!(app.export_modal.focus, ExportFocus::CsvDelimiter);
    press(&mut app, KeyCode::Up);
    press(&mut app, KeyCode::Up);
    assert_eq!(app.export_modal.focus, ExportFocus::FormatSelector);
    press(&mut app, KeyCode::Right);
    assert_eq!(app.export_modal.selected_format, ExportFormat::Tsv);
    press(&mut app, KeyCode::Left);
    press(&mut app, KeyCode::Left);
    assert_eq!(
        app.export_modal.selected_format,
        ExportFormat::Avro,
        "a choice wraps"
    );
    press(&mut app, KeyCode::Char(' '));
    assert_eq!(app.export_modal.selected_format, ExportFormat::Csv);
    press(&mut app, KeyCode::Up);
    assert_eq!(
        app.export_modal.focus,
        ExportFocus::Compression,
        "↑ from the first field wraps to the last"
    );
    press(&mut app, KeyCode::Up);
    assert_eq!(app.export_modal.focus, ExportFocus::CsvIncludeHeader);
    press(&mut app, KeyCode::Char(' '));
    assert!(!app.export_modal.csv_include_header, "Space toggles");
    press(&mut app, KeyCode::Esc);
    assert!(!matches!(app.overlay, Overlay::Export { .. }));

    // Copy opens on the scope; Space takes the next, ↓ the format.
    press(&mut app, KeyCode::Char('y'));
    assert_eq!(app.copy_modal.focus, CopyFocus::Scope);
    let scope = app.copy_modal.scope;
    press(&mut app, KeyCode::Char(' '));
    assert_ne!(app.copy_modal.scope, scope);
    copy_scope(&mut app, CopyScope::Row);
    press(&mut app, KeyCode::Down);
    assert_eq!(app.copy_modal.focus, CopyFocus::Format);
    press(&mut app, KeyCode::Esc);
    assert_ne!(app.overlay, Overlay::Copy);

    // Pivot & Melt opens on its tab bar, a choice: → is Melt, ↓ the first row.
    press(&mut app, KeyCode::Char('p'));
    assert_eq!(app.pivot_melt_modal.focus, PivotMeltFocus::TabBar);
    press(&mut app, KeyCode::Right);
    assert_eq!(app.pivot_melt_modal.active_tab, PivotMeltTab::Melt);
    press(&mut app, KeyCode::Down);
    assert_eq!(app.pivot_melt_modal.focus, PivotMeltFocus::MeltIndex);
    press(&mut app, KeyCode::Down);
    assert_eq!(app.pivot_melt_modal.focus, PivotMeltFocus::MeltStrategy);
    press(&mut app, KeyCode::Char(' '));
    assert_eq!(
        app.pivot_melt_modal.melt_value_strategy,
        datui::pivot_melt_modal::MeltValueStrategy::ByPattern,
        "Space takes the next value"
    );
    press(&mut app, KeyCode::Down);
    assert_eq!(
        app.pivot_melt_modal.focus,
        PivotMeltFocus::MeltPattern,
        "the strategy's own row joins the walk"
    );
}

/// The Info panel opens with the body focused, and the arrows switch tabs from
/// there too: a fresh `i` then `→` reaches the next tab without a Tab first.
#[test]
fn test_info_panel_arrows_switch_tabs_from_the_body() {
    use datui::widgets::info::InfoTab;
    let (mut app, _rx, _tx) = open_query_filter_fixture("info_arrows.csv");

    press(&mut app, KeyCode::Char('i'));
    assert!(app.overlay.shows(&Overlay::Info));
    assert_eq!(app.info_modal.active_tab, InfoTab::Schema);

    press(&mut app, KeyCode::Right);
    assert_ne!(
        app.info_modal.active_tab,
        InfoTab::Schema,
        "Right switches tabs with the body focused"
    );

    press(&mut app, KeyCode::Left);
    assert_eq!(app.info_modal.active_tab, InfoTab::Schema);
}

/// The Info panel's file size and Parquet tab are read on a worker after `i`, and
/// drawn once they land; a file gone since it was opened says so instead (#457).
#[test]
fn test_info_panel_reads_the_file_facts_off_the_ui_thread() {
    use datui::widgets::info::FileFacts;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("facts.parquet");
    let mut frame = df!("id" => (0..50i64).collect::<Vec<_>>()).unwrap();
    ParquetWriter::new(File::create(&path).unwrap())
        .finish(&mut frame)
        .unwrap();
    let size = std::fs::metadata(&path).unwrap().len();

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path.clone()], OpenOptions::default());
    let area = Rect::new(0, 0, 80, 24);
    let _ = painted(&mut app, &rx, &tx, area);
    assert!(
        app.file_facts().is_none(),
        "nothing is read before it is asked"
    );

    for k in [KeyCode::Char('i'), KeyCode::Right] {
        if let Some(next) = app.event(key(k)) {
            let _ = tx.send(next);
        }
    }
    assert!(!app.is_busy(), "the read holds no keys");
    pump_until(&mut app, &rx, &tx, |app| {
        !matches!(app.file_facts(), Some(FileFacts::Reading))
    });
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let text = common::buffer_text(&buf);
    assert!(
        text.contains("50 rows in 1 row group") && text.contains("Format version:"),
        "the Parquet tab says what its footer says; got:\n{text}"
    );
    if let Some(next) = app.event(key(KeyCode::Right)) {
        let _ = tx.send(next);
    }
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let text = common::buffer_text(&buf);
    assert!(
        text.contains(&datui::numfmt::bytes(size)),
        "the Resources tab shows the file's size; got:\n{text}"
    );

    // The same file, gone before the panel asks: the next open's read fails, once.
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path.clone()], OpenOptions::default());
    let _ = painted(&mut app, &rx, &tx, area);
    std::fs::remove_file(&path).unwrap();
    for k in [KeyCode::Char('i'), KeyCode::Right, KeyCode::Right] {
        if let Some(next) = app.event(key(k)) {
            let _ = tx.send(next);
        }
    }
    pump_until(&mut app, &rx, &tx, |app| {
        !matches!(app.file_facts(), Some(FileFacts::Reading))
    });
    assert!(
        matches!(app.file_facts(), Some(FileFacts::Failed(_))),
        "a file that is gone is a failed read: {:?}",
        app.file_facts()
    );
    assert!(!app.is_busy() && !app.modal_showing());
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let text = common::buffer_text(&buf);
    assert!(
        text.contains("File size:") && !text.contains("reading..."),
        "the panel stops waiting; got:\n{text}"
    );
}

/// The accented `i` chip promises unread notes; pressing it lands on the Notes
/// tab. A second open, nothing unread, lands on Schema as before.
#[test]
fn i_opens_on_notes_while_they_are_unread() {
    use datui::widgets::info::InfoTab;

    let dir = tempfile::tempdir().unwrap();
    write_parquet(dir.path(), "date=2024-01-01", df!("id" => &[1i64]).unwrap());
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[2i64], "extra" => &["x"]).unwrap(),
    );

    let mut app = open_local_dataset(dir.path());
    assert!(app.data_table_state.as_ref().unwrap().notes_unseen());

    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Char('i'),
        KeyModifiers::NONE,
    )));
    assert_eq!(
        app.info_modal.active_tab,
        InfoTab::Notes,
        "unread notes put their tab in front"
    );

    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Esc,
        KeyModifiers::NONE,
    )));
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Char('i'),
        KeyModifiers::NONE,
    )));
    assert_eq!(
        app.info_modal.active_tab,
        InfoTab::Schema,
        "read notes stay where they were; the panel opens on Schema"
    );
}

/// A date that does not parse is said as a format problem, with the values.
#[cfg(feature = "sql")]
#[test]
fn a_date_that_does_not_parse_is_explained_in_the_prompt() {
    let (mut app, rx, tx) = open_query_filter_fixture("sql_date_error.csv");
    press_key(&mut app, KeyCode::Char(':'), KeyModifiers::NONE);
    type_text(&mut app, "SELECT STRPTIME(name, '%Y-%m-%d') AS d FROM df");
    press_key(&mut app, KeyCode::Enter, KeyModifiers::NONE);
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(app.query_prompt_mode(), Some(QueryMode::Sql));
    let error = app.query_prompt_error().expect("the reason is shown");
    assert!(error.contains("match the format"), "{error}");
    assert!(error.contains("SUBSTR(name, 1, n)"), "{error}");
    // Counted in the batch the run stopped in: exact only when that was all of it.
    assert!(
        error.starts_with("name: 100 of 100 values do not match the format")
            || error.starts_with("At least"),
        "{error}"
    );
    assert!(!error.contains("strict=False"), "{error}");
}

/// #548 M1: the footer keeps help at 60×20 too, beside the column position. A binary
/// column's type row says binary (D2, D10).
#[test]
fn test_inspect_chip_and_binary_type_at_narrow_and_wide_sizes() {
    let dir = tempfile::tempdir().unwrap();
    let (mut app, rx, tx) = open_orders_fixture(dir.path());
    for (width, height) in [(60, 20), (80, 24), (200, 50)] {
        let text = painted(&mut app, &rx, &tx, Rect::new(0, 0, width, height));
        let rows = rows_at(&mut app, width, height);
        let bar = rows.last().unwrap();
        assert!(bar.contains("? keys"), "{width}x{height}: {bar}");
        assert!(bar.contains("1 / 3"), "{width}x{height}: {bar}");
        if width == 200 {
            // The type row, not the `‹binary›` stub in the cells.
            assert!(text.contains(" binary"), "{text}");
        }
    }
}

/// A date or datetime past the calendar's range draws as its stored number on
/// every screen that shows it, where Polars' formatting panicked (#494): the
/// table, the inspector, Info, Describe, Distribution, Data Quality split by
/// it, a chart over it, and a `by` query's lists.
#[test]
fn out_of_range_dates_draw_on_every_screen() {
    let dir = common::fixture_dir();
    let (mut app, rx, tx) = open_out_of_range_dates(&dir);
    let least = [
        "-2147483648 days since 1970-01-01",
        "-9223372036854775807 ms since 1970-01-01 UTC",
        "-9223372036854775807 us since 1970-01-01 UTC",
        // Every nanosecond count is a date.
        "1677-09-21 00:12:43.145224193",
    ];
    let screen = draw_wide(&mut app, "table");
    for text in least {
        assert!(screen.contains(text), "{text}:\n{screen}");
    }
    assert!(
        screen.contains("2147483647 days since 1970-01-01"),
        "{screen}"
    );

    press_through(&mut app, KeyCode::Char(' '));
    let screen = draw_wide(&mut app, "inspector");
    assert!(screen.contains(least[2]), "{screen}");
    press_through(&mut app, KeyCode::Esc);

    press_through(&mut app, KeyCode::Char('i'));
    pump_until_idle(&mut app, &rx, &tx);
    for tab in 0..4 {
        draw_wide(&mut app, &format!("info tab {tab}"));
        press_through(&mut app, KeyCode::Tab);
    }
    press_through(&mut app, KeyCode::Esc);

    // Describe, Distribution and Data Quality, each opened from the table.
    for tool in [0, 1, 3] {
        press_through(&mut app, KeyCode::Char('a'));
        app.analysis_modal.sidebar_state.select(Some(tool));
        // The first Enter shows the Sample form, unless a sample is already set.
        press_through(&mut app, KeyCode::Enter);
        if !app.is_busy() {
            press_through(&mut app, KeyCode::Enter);
        }
        drain_events(&mut app, &rx);
        let screen = draw_wide(&mut app, &format!("tool {tool}"));
        if tool == 0 {
            assert!(screen.contains(least[0]), "Describe's min:\n{screen}");
        }
        for _ in 0..6 {
            if app.overlay != Overlay::Analysis {
                break;
            }
            press_through(&mut app, KeyCode::Esc);
        }
        assert_ne!(app.overlay, Overlay::Analysis);
    }

    // Data Quality split by a datetime: by partition each value is its own
    // segment, named as the table names it; in time windows one past the
    // calendar falls in none, as a null does, where truncating it overflowed.
    use datui::data_quality::{QualityGrain, QualityPage};
    for (grain, segment, segments) in [
        (
            QualityGrain::Partition("t_ms".into()),
            format!("t_ms={}", least[1]),
            3,
        ),
        (
            QualityGrain::TimeWindows {
                column: "t_us".into(),
                every: "1d".into(),
            },
            "1970-01-01".to_string(),
            2,
        ),
        (
            QualityGrain::TimeWindows {
                column: "d".into(),
                every: "1w".into(),
            },
            "week of 1969-12-29".to_string(),
            2,
        ),
    ] {
        press_through(&mut app, KeyCode::Char('a'));
        app.analysis_modal.sidebar_state.select(Some(3));
        press_through(&mut app, KeyCode::Enter);
        press_through(&mut app, KeyCode::Char('e'));
        assert_eq!(app.analysis_modal.quality.page, QualityPage::Setup);
        // The time roles list a few of each column's values on screen.
        app.analysis_modal.quality.page = QualityPage::TimeRoles;
        let screen = draw_wide(&mut app, "time roles");
        assert!(screen.contains(least[2]), "{screen}");
        app.analysis_modal.quality.page = QualityPage::Setup;
        app.analysis_modal.quality.plan.grain = grain.clone();
        press_through(&mut app, KeyCode::Enter);
        drain_events(&mut app, &rx);
        let labels: Vec<String> = app
            .analysis_modal
            .quality
            .results
            .as_ref()
            .unwrap()
            .segments
            .iter()
            .map(|s| s.label.clone())
            .collect();
        assert!(labels.contains(&segment), "{grain:?}: {labels:?}");
        assert_eq!(labels.len(), segments, "{grain:?}: {labels:?}");
        for page in [
            QualityPage::Overview,
            QualityPage::Columns,
            QualityPage::Segments,
            QualityPage::Trends,
        ] {
            app.analysis_modal.quality.page = page;
            draw_wide(&mut app, &format!("{grain:?} {page:?}"));
        }
        for _ in 0..6 {
            if app.overlay != Overlay::Analysis {
                break;
            }
            press_through(&mut app, KeyCode::Esc);
        }
        assert_ne!(app.overlay, Overlay::Analysis);
    }

    // A chart over the datetimes: the axis falls back to the stored numbers.
    press_through(&mut app, KeyCode::Char('c'));
    press_through(&mut app, KeyCode::Char('1'));
    app.chart.modal.focus = datui::chart_modal::ChartFocus::X;
    press_through(&mut app, KeyCode::Char(' '));
    type_text(&mut app, "t_us");
    press_through(&mut app, KeyCode::Enter);
    app.chart.modal.focus = datui::chart_modal::ChartFocus::Y;
    press_through(&mut app, KeyCode::Char(' '));
    type_text(&mut app, "id");
    press_through(&mut app, KeyCode::Char(' '));
    press_through(&mut app, KeyCode::Enter);
    assert_eq!(app.chart.modal.x().map(String::as_str), Some("t_us"));
    assert_eq!(app.chart.modal.y(), ["id"]);
    pump_until_chart_ready(&mut app, &rx, &tx);
    draw_wide(&mut app, "chart");
    press_through(&mut app, KeyCode::Esc);
    assert!(app.at_table());

    // Each group's datetimes as a list.
    app.data_table_state
        .as_mut()
        .unwrap()
        .query("select t_us, d by id".to_string());
    pump_until_idle(&mut app, &rx, &tx);
    let screen = draw_wide(&mut app, "by");
    assert!(screen.contains(&format!("[{}]", least[2])), "{screen}");
}

/// The column cursor follows its column by name through reorder, freeze and hide,
/// and the view follows it at the next draw (#574).
#[test]
fn column_cursor_follows_reorder_freeze_and_hide() {
    let size = (80, 24);
    let (mut app, rx, tx) = open_wide_table("cursor_follows.parquet", 120, size);
    let current = |app: &App| {
        app.data_table_state
            .as_ref()
            .unwrap()
            .current_column()
            .unwrap()
            .to_string()
    };
    press_and_draw(&mut app, KeyCode::Char('g'), size);
    type_and_draw(&mut app, "label_062", size);
    press_and_draw(&mut app, KeyCode::Enter, size);
    assert_eq!(current(&app), "label_062");
    let mut order = app.data_table_state.as_ref().unwrap().headers();

    // Moved to the front and frozen: still on it, at the left.
    order.retain(|c| c != "label_062");
    order.insert(0, "label_062".to_string());
    run_and_settle(&mut app, AppEvent::ColumnOrder(order.clone(), 1), &rx, &tx);
    draw_sized(&mut app, size);
    assert_eq!(current(&app), "label_062");
    assert_eq!(cursor_at(&app), 1);

    // Moved to the end, unfrozen: the view goes there with it.
    order.remove(0);
    order.push("label_062".to_string());
    run_and_settle(&mut app, AppEvent::ColumnOrder(order.clone(), 0), &rx, &tx);
    let screen = draw_sized(&mut app, size);
    assert_eq!(current(&app), "label_062");
    assert_eq!(cursor_at(&app), 120);
    assert_eq!(range_shown(&app).unwrap().1, 120, "{screen}");
    assert!(header_line(&screen).contains("label_062"), "{screen}");

    // Hidden: the column now in its place takes the cursor, here the last one.
    order.pop();
    let last = order.last().unwrap().clone();
    run_and_settle(&mut app, AppEvent::ColumnOrder(order.clone(), 0), &rx, &tx);
    draw_sized(&mut app, size);
    assert_eq!(current(&app), last);
    assert_eq!(cursor_at(&app), 119);
}

/// A narrow terminal: a column wider than the window still pages one column at a
/// time, both ways, and a resize keeps the first column where the scroll left it.
#[test]
fn wide_table_pages_in_a_narrow_window() {
    use datui::widgets::column_widths::WidthChoice;
    let small = (60, 20);
    let (mut app, _rx, _tx) = open_wide_table("wide_nav_narrow.parquet", 12, small);
    app.data_table_state
        .as_mut()
        .unwrap()
        .set_width_choices([("label_002".to_string(), WidthChoice::Manual(100))]);
    draw_sized(&mut app, small);
    let start = columns_shown(&app).unwrap();
    assert_eq!(start.first, 1);
    assert_eq!(
        start.last, 3,
        "the wide column is drawn cut, last: {start:?}"
    );

    page_and_draw(&mut app, KeyCode::Right, small);
    let wide = columns_shown(&app).unwrap();
    assert_eq!((wide.first, wide.last), (3, 3), "the wide column alone");
    page_and_draw(&mut app, KeyCode::Right, small);
    assert_eq!(columns_shown(&app).unwrap().first, 4, "and past it");
    page_and_draw(&mut app, KeyCode::Left, small);
    assert_eq!(columns_shown(&app).unwrap().first, 3);
    page_and_draw(&mut app, KeyCode::Left, small);
    assert_eq!(columns_shown(&app).unwrap().first, 1);

    // A resize keeps the first column; the last page is planned in the new room.
    press_and_draw(&mut app, KeyCode::Char('}'), small);
    let narrow_last = columns_shown(&app).unwrap();
    assert_eq!(narrow_last.last, 12);
    let wide_size = (120, 30);
    draw_sized(&mut app, wide_size);
    assert_eq!(
        columns_shown(&app).map(|o| o.first),
        Some(narrow_last.first)
    );
    // A resize narrower than the cursor's place scrolls just enough to keep it.
    press_and_draw(&mut app, KeyCode::Char('{'), wide_size);
    press_and_draw(&mut app, KeyCode::Char('l'), wide_size);
    press_and_draw(&mut app, KeyCode::Char('l'), wide_size);
    assert_eq!(cursor_at(&app), 3);
    assert_eq!(columns_shown(&app).unwrap().first, 1, "whole at 120");
    draw_sized(&mut app, small);
    assert_eq!(
        range_shown(&app),
        Some((3, 3)),
        "cut at 60, so the wide column is brought on alone"
    );
    draw_sized(&mut app, wide_size);
    press_and_draw(&mut app, KeyCode::Char('{'), wide_size);
    press_and_draw(&mut app, KeyCode::Char('}'), wide_size);
    press_and_draw(&mut app, KeyCode::Char('}'), wide_size);
    let wide_last = columns_shown(&app).unwrap();
    assert!(wide_last.first < narrow_last.first, "{wide_last:?}");
    assert_eq!(wide_last.last, 12);
    let screen = draw_sized(&mut app, wide_size);
    let expected = "col 12/12".to_string();
    assert!(
        screen.lines().last().unwrap().contains(&expected),
        "{screen}"
    );
}

/// A date that a microsecond datetime cannot count, `Date(i32::MAX)`, cast to one or
/// met with one, is null, where Polars panicked naming it (#526). In range it is
/// Polars' own.
#[test]
fn a_date_past_what_a_datetime_counts_is_null_as_one() {
    let dir = common::fixture_dir();
    let (mut app, rx, tx) = open_past_calendar(&dir);
    let some = |s: &str| Some(s.to_string());
    let midnight = "1970-01-01 00:00:00";
    let t_us = "-9223372036854775807 us since 1970-01-01 UTC";
    run_query(&mut app, &rx, &tx, "select x: d ^ t_us");
    assert_eq!(view_text(&app, "x"), [some(midnight), some(t_us)]);
    #[cfg(feature = "sql")]
    for (sql, expected) in [
        (
            "SELECT CAST(d AS TIMESTAMP) AS x FROM df",
            [some(midnight), None],
        ),
        (
            "SELECT COALESCE(d, t_us) AS x FROM df",
            [some(midnight), some(t_us)],
        ),
        (
            "SELECT CASE WHEN s = 'a' THEN t_us ELSE d END AS x FROM df",
            [some(midnight), None],
        ),
        (
            "SELECT GREATEST(d, t_us) AS x FROM df",
            [some(midnight), some(t_us)],
        ),
    ] {
        run_sql(&mut app, &rx, &tx, sql);
        assert_eq!(app.error_message(), None, "{sql}");
        assert_eq!(view_text(&app, "x"), expected, "{sql}");
    }
}

/// The copies an open writes, a decompressed CSV and a converted Arrow stream, are
/// read from a temp directory named like a glob, not from what its name matches (#632).
#[test]
fn copies_in_a_temp_directory_named_like_a_glob_open() {
    use std::io::Write;
    common::ensure_sample_data();
    let dir = tempfile::tempdir().unwrap();
    let scratch = dir.path().join("t[1]");
    std::fs::create_dir(&scratch).unwrap();
    std::fs::create_dir(dir.path().join("t1")).unwrap();
    let gz = dir.path().join("rows.csv.gz");
    let mut encoder =
        flate2::write::GzEncoder::new(File::create(&gz).unwrap(), flate2::Compression::default());
    encoder.write_all(b"id\n1\n2\n").unwrap();
    encoder.finish().unwrap();
    let stream = Path::new("tests/sample-data").join("people_stream.arrow");
    let people = LazyFrame::scan_ipc(
        PlRefPath::try_from_path(&Path::new("tests/sample-data").join("people.arrow")).unwrap(),
        Default::default(),
        Default::default(),
    )
    .unwrap()
    .collect()
    .unwrap()
    .height();
    for (path, rows) in [(gz, 2), (stream, people)] {
        let (app, _rx, _tx) = open_with_scratch(vec![path.clone()], &scratch);
        let state = app
            .data_table_state
            .as_ref()
            .unwrap_or_else(|| panic!("{} opens", path.display()));
        assert_eq!(state.num_rows(), rows, "{}", path.display());
        assert_eq!(files_in(&scratch), 1, "{}: the copy", path.display());
    }
}

/// Arrow IPC streams, the format of a Hugging Face `datasets` cache, open: plain, with
/// LZ4 and ZSTD buffers, and in the legacy layout without a name to go on. Each is
/// converted once to an IPC file in the temp directory, which goes with the dataset.
#[test]
fn arrow_ipc_streams_open() {
    common::ensure_sample_data();
    let sample = Path::new("tests/sample-data");
    let people = LazyFrame::scan_ipc(
        PlRefPath::try_from_path(&sample.join("people.arrow")).unwrap(),
        Default::default(),
        Default::default(),
    )
    .unwrap()
    .collect()
    .unwrap();
    let columns: Vec<String> = people
        .get_column_names()
        .iter()
        .map(|c| c.to_string())
        .collect();
    for name in [
        "people_stream.arrow",
        "people_stream_lz4.arrow",
        "people_stream_zstd.arrow",
        "people_stream_legacy",
    ] {
        let scratch = tempfile::tempdir().unwrap();
        let (mut app, rx, tx) = open_with_scratch(vec![sample.join(name)], scratch.path());
        let state = app
            .data_table_state
            .as_ref()
            .unwrap_or_else(|| panic!("{name} opens"));
        assert_eq!(state.num_rows(), people.height(), "{name}");
        assert_eq!(state.headers(), columns, "{name}");
        assert_eq!(files_in(scratch.path()), 1, "{name}: the converted copy");
        // Scanned again as the copy, it is still read converted, and not downloaded.
        assert_eq!(
            state.read_mode(),
            Some(datui::ReadMode::Converted),
            "{name}"
        );
        assert!(!state.fetched(), "{name}");
        assert_eq!(
            app.open_path(),
            Some(sample.join(name).as_path()),
            "{name}: named by the stream, not the copy"
        );

        pump_open_until_loaded(
            &mut app,
            &rx,
            vec![sample.join("people.arrow")],
            OpenOptions::default(),
        );
        pump_until_idle(&mut app, &rx, &tx);
        assert_eq!(
            files_in(scratch.path()),
            0,
            "{name}: the copy went with its dataset"
        );
    }
}

/// `.arrows`, the extension Arrow gives streams, is Arrow.
#[test]
fn an_arrows_file_opens_as_a_stream() {
    common::ensure_sample_data();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("people.arrows");
    std::fs::copy("tests/sample-data/people_stream.arrow", &path).unwrap();
    let scratch = tempfile::tempdir().unwrap();
    let (app, _rx, _tx) = open_with_scratch(vec![path], scratch.path());
    let state = app.data_table_state.as_ref().expect("the stream opens");
    assert_eq!(state.num_rows(), 1000);
}

/// A directory of Hugging Face shards opens as one table, its JSON files left aside.
#[test]
fn a_directory_of_arrow_ipc_stream_shards_opens_as_one_table() {
    common::ensure_sample_data();
    let scratch = tempfile::tempdir().unwrap();
    let (app, _rx, _tx) = open_with_scratch(
        vec![PathBuf::from("tests/sample-data/hf_shards")],
        scratch.path(),
    );
    let state = app.data_table_state.as_ref().expect("the shards open");
    assert_eq!(state.num_rows(), 1000);
    assert!(state.headers().contains(&"first_name".to_string()));
    assert_eq!(files_in(scratch.path()), 1, "one copy of all three");
}

/// A Hugging Face cache opens its train split, both shards, and names the other
/// splits; `--table` opens another. The files `map()` wrote, with columns of their
/// own, are left out and said to be. A split that is not there is refused by name.
#[test]
fn a_hugging_face_cache_opens_one_split() {
    common::ensure_sample_data();
    let cache = PathBuf::from("tests/sample-data/hf_cache");
    let open = |table: Option<&str>| {
        let scratch = tempfile::tempdir().unwrap();
        let (tx, rx) = mpsc::channel();
        let mut app = App::new(tx, common::test_runtime());
        let options = OpenOptions {
            temp_dir: Some(scratch.path().to_path_buf()),
            table: table.map(str::to_string),
            ..OpenOptions::default()
        };
        settle_from(&mut app, &rx, AppEvent::Open(vec![cache.clone()], options));
        (app, scratch)
    };

    let (app, _scratch) = open(None);
    assert_eq!(app.error_message(), None);
    let state = app
        .data_table_state
        .as_ref()
        .expect("the train split opens");
    assert_eq!(state.num_rows(), 600, "train's two shards");
    let rows = state.lf().clone().collect().unwrap();
    assert_eq!(
        rows.column("id").unwrap().get(0).unwrap(),
        AnyValue::Int64(1),
        "shard 0 first"
    );
    assert_eq!(state.other_tables(), ["validation", "test"]);
    let notes: Vec<String> = state.notes().into_iter().map(|n| n.summary).collect();
    assert!(
        notes
            .iter()
            .any(|n| n == "2 cache files written by map() not read"),
        "{notes:?}"
    );
    assert!(
        !notes.iter().any(|n| n.contains("commonest")),
        "the JSON is the cache's own: {notes:?}"
    );

    let (app, _scratch) = open(Some("validation"));
    assert_eq!(app.error_message(), None);
    let state = app.data_table_state.as_ref().expect("validation opens");
    assert_eq!(state.num_rows(), 200);
    assert_eq!(state.other_tables(), ["train", "test"]);

    let (app, _scratch) = open(Some("dev"));
    let message = app.error_message().expect("no split named dev");
    assert!(
        message.contains("No split named dev; this directory holds train, validation, test"),
        "{message}"
    );
}

/// A DatasetDict saved with `save_to_disk` opens one split's directory, train first,
/// and names the others; `--table` opens another.
#[test]
fn a_saved_dataset_dict_opens_one_split() {
    common::ensure_sample_data();
    let open = |table: Option<&str>| {
        let scratch = tempfile::tempdir().unwrap();
        let (tx, rx) = mpsc::channel();
        let mut app = App::new(tx, common::test_runtime());
        let options = OpenOptions {
            temp_dir: Some(scratch.path().to_path_buf()),
            table: table.map(str::to_string),
            ..OpenOptions::default()
        };
        let dict = PathBuf::from("tests/sample-data/hf_dict");
        settle_from(&mut app, &rx, AppEvent::Open(vec![dict], options));
        (app, scratch)
    };
    let (app, _scratch) = open(None);
    assert_eq!(app.error_message(), None);
    let state = app.data_table_state.as_ref().expect("train opens");
    assert_eq!(state.num_rows(), 700);
    assert_eq!(state.other_tables(), ["test"]);
    let (app, _scratch) = open(Some("test"));
    assert_eq!(app.error_message(), None);
    let state = app.data_table_state.as_ref().expect("test opens");
    assert_eq!(state.num_rows(), 300);
    assert_eq!(state.other_tables(), ["train"]);
    let (app, _scratch) = open(Some("dev"));
    let message = app.error_message().expect("no split named dev");
    assert!(message.contains("No split named dev"), "{message}");
}

/// A stream among IPC files, not first, is found when the scan fails on it. Only the
/// stream is converted; the IPC file is read where it is, and the rows keep the order
/// of the names.
#[test]
fn a_stream_behind_an_ipc_file_opens_with_it() {
    common::ensure_sample_data();
    let scratch = tempfile::tempdir().unwrap();
    let dir = PathBuf::from("tests/sample-data/arrow_mixed");
    let (app, _rx, _tx) = open_with_scratch(vec![dir.clone()], scratch.path());
    assert_eq!(app.error_message(), None);
    let state = app.data_table_state.as_ref().expect("the files open");
    assert_eq!(state.num_rows(), 1000);
    let ids: Vec<i64> = state
        .lf()
        .clone()
        .collect()
        .unwrap()
        .column("id")
        .unwrap()
        .i64()
        .unwrap()
        .into_no_null_iter()
        .collect();
    assert_eq!(ids, (1..=1000).collect::<Vec<_>>(), "in name order");
    assert_eq!(files_in(scratch.path()), 1, "a copy of the stream");
    let copy: u64 = std::fs::read_dir(scratch.path())
        .unwrap()
        .flatten()
        .map(|e| e.metadata().unwrap().len())
        .sum();
    let file = std::fs::metadata(dir.join("a.arrow")).unwrap().len();
    let stream = std::fs::metadata(dir.join("b.arrow")).unwrap().len();
    assert!(
        copy < file + stream / 2 && copy >= stream / 2,
        "the stream only: {copy}"
    );
}

/// A directory of CSV opened as one table counts its rows by a scan. The Parquet
/// footer count it was given found no Parquet, and the count stayed unknown.
#[test]
fn a_directory_of_csv_counts_its_rows() {
    let dir = common::fixture_dir().join("csv_directory_count");
    std::fs::create_dir_all(&dir).unwrap();
    for part in ["a", "b"] {
        let rows: String = (0..3_000).map(|i| format!("{i},{part}\n")).collect();
        std::fs::write(dir.join(format!("{part}.csv")), format!("n,part\n{rows}")).unwrap();
    }
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    let options = OpenOptions {
        hive: true,
        ..OpenOptions::default()
    };
    settle_from(&mut app, &rx, AppEvent::Open(vec![dir], options));
    assert!(app.error_message().is_none(), "{:?}", app.error_message());
    let state = app.data_table_state.as_ref().expect("a dataset");
    assert_eq!(state.num_rows_if_valid(), Some(6_000));
}

/// A padded logger export: `--comment` finds the header past two comment lines,
/// the names lose their padding, and padded numbers are numbers with the blank cells
/// null, with or without `--skip-initial-space`. That flag leaves the typing to
/// `--infer-types`: off, the values lose their padding and stay text; limited to a
/// column, only that column is typed.
#[test]
fn a_padded_log_reads_with_comment_char() {
    let log = dialect_fixture("dialect_padded_log.csv");
    let names = [
        "Lcl Date",
        "Lcl Time",
        "UTCOfst",
        "Latitude",
        "bus1volts",
        "E1 CHT1",
    ];
    for flags in [
        &["--comment", "#"][..],
        &["--comment", "#", "--skip-initial-space"],
    ] {
        let df = open_dialect(vec![log.clone()], options_from_flags(flags));
        assert_eq!(names_of(&df), names, "{flags:?}");
        assert_eq!(df.height(), 21, "{flags:?}");
        let latitude = df.column("Latitude").unwrap();
        assert_eq!(latitude.dtype(), &DataType::Float64, "{flags:?}");
        assert_eq!(
            latitude.null_count(),
            1,
            "the blank cell is null: {flags:?}"
        );
        assert_eq!(
            df.column("bus1volts").unwrap().f64().unwrap().get(0),
            Some(25.1),
            "{flags:?}"
        );
        assert_eq!(df.column("Lcl Date").unwrap().dtype(), &DataType::Date);
    }

    let df = open_dialect(
        vec![log.clone()],
        options_from_flags(&[
            "--comment",
            "#",
            "--skip-initial-space",
            "--infer-types=off",
        ]),
    );
    let latitude = df.column("Latitude").unwrap();
    assert_eq!(latitude.dtype(), &DataType::String);
    assert_eq!(latitude.null_count(), 1);
    assert_eq!(latitude.str().unwrap().get(1), Some("40.100000"));
    assert_eq!(df.column("bus1volts").unwrap().dtype(), &DataType::String);

    let df = open_dialect(
        vec![log.clone()],
        options_from_flags(&[
            "--comment",
            "#",
            "--skip-initial-space",
            "--infer-types=Latitude",
        ]),
    );
    assert_eq!(df.column("Latitude").unwrap().dtype(), &DataType::Float64);
    let volts = df.column("bus1volts").unwrap();
    assert_eq!(volts.dtype(), &DataType::String);
    assert_eq!(volts.str().unwrap().get(0), Some("25.1"));

    // A cell of spaces in a text column is empty text as read, and null once the
    // padding is skipped.
    let plain = open_dialect(vec![log.clone()], options_from_flags(&["--comment", "#"]));
    let skipped = open_dialect(
        vec![log],
        options_from_flags(&["--comment", "#", "--skip-initial-space"]),
    );
    assert_eq!(plain.column("UTCOfst").unwrap().null_count(), 0);
    let utc = skipped.column("UTCOfst").unwrap();
    assert_eq!(utc.null_count(), 1);
    assert_eq!(utc.str().unwrap().get(1), Some("-05:00"));
}

/// Header names are trimmed whatever the flags: with the header found by
/// `--skip-lines`, and with string parsing off.
#[test]
fn header_names_are_always_trimmed() {
    let log = dialect_fixture("dialect_padded_log.csv");
    let df = open_dialect(
        vec![log],
        options_from_flags(&["--skip-lines", "2", "--infer-types=off"]),
    );
    assert_eq!(names_of(&df)[3], "Latitude");
    // Without parsing or skipping, the value keeps its padding: nothing asked for less.
    assert_eq!(
        df.column("Latitude").unwrap().str().unwrap().get(1),
        Some("    40.100000")
    );
}

/// `--header-rows` takes the header from the lines it names and joins several with
/// `header_join`, with or without `--comment`; the data starts after the last.
#[test]
fn header_rows_name_and_join_the_columns() {
    let two = dialect_fixture("dialect_two_header_rows.csv");
    let df = open_dialect(
        vec![two.clone()],
        options_from_flags(&["--header-rows", "1,2"]),
    );
    assert_eq!(names_of(&df), ["station", "temp degC", "pressure hPa"]);
    assert_eq!(df.height(), 3);
    assert_eq!(df.column("temp degC").unwrap().dtype(), &DataType::Float64);

    let joined = OpenOptions {
        header_join: "_".into(),
        ..options_from_flags(&["--header-rows", "1,2"])
    };
    assert_eq!(
        names_of(&open_dialect(vec![two.clone()], joined)),
        ["station", "temp_degC", "pressure_hPa"]
    );

    // --skip-rows counts rows after the header.
    let df = open_dialect(
        vec![two.clone()],
        options_from_flags(&["--header-rows", "1,2", "--skip-rows", "1"]),
    );
    assert_eq!(df.height(), 2);
    assert_eq!(
        df.column("station").unwrap().str().unwrap().get(0),
        Some("B")
    );

    // A per-column null value names the column as it is shown.
    let df = open_dialect(
        vec![two],
        options_from_flags(&["--header-rows", "1,2", "--null", "station=B"]),
    );
    assert_eq!(df.column("station").unwrap().null_count(), 1);

    // The units line is a comment, and a header line all the same when it is named.
    let log = dialect_fixture("dialect_padded_log.csv");
    let df = open_dialect(
        vec![log.clone()],
        options_from_flags(&["--comment", "#", "--header-rows", "3,2"]),
    );
    assert_eq!(
        names_of(&df),
        [
            "Lcl Date yyyy-mm-dd",
            "Lcl Time hh:mm:ss",
            "UTCOfst hh:mm",
            "Latitude degrees",
            "bus1volts volts",
            "E1 CHT1 deg F"
        ]
    );
    assert_eq!(df.height(), 21);

    // Without --comment the lines above the header are passed over all the same.
    let df = open_dialect(vec![log], options_from_flags(&["--header-rows", "3"]));
    assert_eq!(names_of(&df)[3], "Latitude");
    assert_eq!(df.height(), 21);
    assert_eq!(df.column("Latitude").unwrap().dtype(), &DataType::Float64);
}

/// Comment lines among the data are skipped, not read as rows.
#[test]
fn comment_lines_among_the_data_are_skipped() {
    let mid = dialect_fixture("dialect_mid_comments.csv");
    let df = open_dialect(vec![mid.clone()], options_from_flags(&["--comment", "#"]));
    assert_eq!(names_of(&df), ["id", "value"]);
    assert_eq!(df.height(), 3);
    assert_eq!(df.column("value").unwrap().dtype(), &DataType::Int64);
    let df = open_dialect(
        vec![mid],
        options_from_flags(&["--comment", "#", "--header-rows", "2"]),
    );
    assert_eq!(names_of(&df), ["id", "value"]);
    assert_eq!(df.height(), 3);
}

/// Compressed, the file reads the same: decompressed to a scanned copy, and in memory.
#[test]
fn the_dialect_reads_a_compressed_log_the_same() {
    let gz = dialect_fixture("dialect_padded_log.csv.gz");
    let flags = [
        "--comment",
        "#",
        "--header-rows",
        "3,2",
        "--skip-initial-space",
    ];
    for in_memory in [false, true] {
        let options = OpenOptions {
            decompress_in_memory: in_memory,
            ..options_from_flags(&flags)
        };
        let df = open_dialect(vec![gz.clone()], options);
        assert_eq!(
            names_of(&df)[3],
            "Latitude degrees",
            "in memory: {in_memory}"
        );
        assert_eq!(df.height(), 21);
        assert_eq!(df.column("UTCOfst hh:mm").unwrap().null_count(), 1);
        assert_eq!(
            df.column("Latitude degrees").unwrap().dtype(),
            &DataType::Float64
        );
    }
}

/// A directory of such logs is one table, each file named from its own header lines,
/// and a single file's padding does not split it.
#[test]
fn a_directory_of_padded_logs_is_one_table() {
    common::ensure_sample_data();
    let dir = common::fixture_dir().join("logs");
    std::fs::create_dir_all(&dir).unwrap();
    let text = std::fs::read_to_string(dialect_fixture("dialect_padded_log.csv")).unwrap();
    std::fs::write(dir.join("a.csv"), &text).unwrap();
    // The same columns, padded to other widths.
    std::fs::write(
        dir.join("b.csv"),
        text.replacen("  Lcl Date, Lcl Time,", "Lcl Date,Lcl Time   ,", 1),
    )
    .unwrap();
    for flags in [
        &["--comment", "#"][..],
        &[
            "--comment",
            "#",
            "--header-rows",
            "3,2",
            "--skip-initial-space",
        ],
    ] {
        let (tx, rx) = mpsc::channel();
        let mut app = App::new(tx, common::test_runtime());
        settle_from(
            &mut app,
            &rx,
            AppEvent::OpenNamed(vec![dir.clone()], options_from_flags(flags)),
        );
        assert!(
            app.home.browsing.is_none(),
            "one table, not a listing: {flags:?}"
        );
        let df = app
            .data_table_state
            .as_ref()
            .unwrap()
            .lf()
            .clone()
            .collect()
            .unwrap();
        assert_eq!(df.height(), 42, "{flags:?}");
        assert_eq!(df.width(), 6, "{flags:?}");
    }
}

/// `H` reads the lines `--header-rows` named as data, and back as the header.
#[test]
fn h_reads_header_rows_as_data_and_back() {
    common::ensure_sample_data();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    settle_from(
        &mut app,
        &rx,
        AppEvent::Open(
            vec![dialect_fixture("dialect_two_header_rows.csv")],
            options_from_flags(&["--header-rows", "1,2"]),
        ),
    );
    assert_eq!(column_names(&app), ["station", "temp degC", "pressure hPa"]);
    header_from_schema_tab(&mut app, &rx);
    assert_eq!(column_names(&app), ["column_1", "column_2", "column_3"]);
    assert_eq!(app.data_table_state.as_ref().unwrap().num_rows(), 5);
    header_from_schema_tab(&mut app, &rx);
    assert_eq!(column_names(&app), ["station", "temp degC", "pressure hPa"]);
}

/// A log with nothing after its header lines yet is a table with its columns and no
/// rows, alone, compressed or among other logs; one that ends before the header
/// says so.
#[test]
fn header_rows_on_a_file_with_no_rows_yet() {
    common::ensure_sample_data();
    let dir = common::fixture_dir().join("dialect_no_rows");
    std::fs::create_dir_all(&dir).unwrap();
    let header = "#device_info\n#yyyy-mm-dd, degrees\n  Lcl Date,     Latitude\n";
    let empty = dir.join("empty.csv");
    std::fs::write(&empty, header).unwrap();
    let gz = dir.join("empty.csv.gz");
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    std::io::Write::write_all(&mut encoder, header.as_bytes()).unwrap();
    std::fs::write(&gz, encoder.finish().unwrap()).unwrap();
    // A per-column null value has no rows to read the columns from.
    let flags = [
        "--comment",
        "#",
        "--header-rows",
        "3,2",
        "--null",
        "Latitude degrees=-",
    ];
    let names = ["Lcl Date yyyy-mm-dd", "Latitude degrees"];
    for (path, in_memory) in [(&empty, false), (&gz, false), (&gz, true)] {
        let options = OpenOptions {
            decompress_in_memory: in_memory,
            ..options_from_flags(&flags)
        };
        let df = open_dialect(vec![path.clone()], options);
        assert_eq!(names_of(&df), names, "{path:?}, in memory: {in_memory}");
        assert_eq!(df.height(), 0);
    }

    let logs = dir.join("logs");
    std::fs::create_dir_all(&logs).unwrap();
    std::fs::write(logs.join("a.csv"), header).unwrap();
    std::fs::write(
        logs.join("b.csv"),
        format!("{header}2024-03-01,    40.100000\n"),
    )
    .unwrap();
    let df = open_dialect(vec![logs], options_from_flags(&flags));
    assert_eq!(names_of(&df), names);
    assert_eq!(df.height(), 1);

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    settle_from(
        &mut app,
        &rx,
        AppEvent::Open(vec![empty], options_from_flags(&["--header-rows", "5"])),
    );
    let message = app.error_message().expect("an error");
    assert!(message.contains("past the end of the file"), "{message}");
}

/// Dropping footer rows counts the whole file before the first row: the loading
/// screen and the footer say so while it does.
#[test]
fn test_a_footer_count_says_so_on_screen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("vendor.csv");
    std::fs::write(&path, "a,b\n1,2\n3,4\nTOTAL,6\n").unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    let options = OpenOptions {
        skip_tail_rows: Some(1),
        ..OpenOptions::default()
    };
    let mut next = app.event(AppEvent::Open(vec![path], options));
    let area = Rect::new(0, 0, 100, 24);
    let mut buffer = Buffer::empty(area);
    app.render(area, &mut buffer);
    let screen: String = buffer.content().iter().map(|cell| cell.symbol()).collect();
    assert!(
        screen.contains("Counting rows to skip the footer"),
        "{screen}"
    );
    while let Some(event) = next.take().or_else(|| next_event(&app, &rx)) {
        next = app.event(event);
    }
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(app.data_table_state.as_ref().unwrap().num_rows(), 2);
}

/// A file read whole into memory past `[read] memory_warning` is put to
/// the user before the read: Enter reads it, Esc goes home without reading, and 0
/// never asks.
#[test]
fn test_a_large_in_memory_read_asks_first() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("big.json");
    // Over 1 MiB of JSON.
    let rows: Vec<String> = (0..40_000)
        .map(|i| format!("{{\"id\":{i},\"name\":\"row number {i}\"}}"))
        .collect();
    std::fs::write(&path, format!("[{}]", rows.join(","))).unwrap();
    assert!(std::fs::metadata(&path).unwrap().len() > 1024 * 1024);
    let app_with = |mb: u64| {
        let mut config = datui::AppConfig::default();
        config.read.memory_warning = datui::config::ByteSize::mib(mb);
        let theme = datui::Theme::from_config(&config.theme).unwrap();
        let (tx, rx) = mpsc::channel();
        let app = App::new_with_config(tx.clone(), common::test_runtime(), theme, config);
        (app, rx, tx)
    };
    // Opened until the question is up, as a download's is: the open waits on it.
    let ask = |app: &mut App, rx: &mpsc::Receiver<AppEvent>| {
        let mut next = Some(AppEvent::Open(vec![path.clone()], OpenOptions::default()));
        while !app.awaiting_open_confirmation() {
            let event = next
                .take()
                .or_else(|| next_event(app, rx))
                .expect("the open asks");
            next = app.event(event);
        }
    };

    let (mut app, rx, tx) = app_with(1);
    ask(&mut app, &rx);
    assert!(app.confirmation_modal.active, "the read is asked about");
    let message = app.confirmation_modal.message.clone();
    assert!(
        message.starts_with("big.json: JSON reads ") && message.contains("into memory"),
        "{message}"
    );
    assert!(app.data_table_state.is_none(), "nothing was read");
    let mut next = app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    while let Some(event) = next.take().or_else(|| next_event(&app, &rx)) {
        next = app.event(event);
    }
    pump_until_idle(&mut app, &rx, &tx);
    assert!(!app.confirmation_modal.active);
    assert_eq!(app.data_table_state.as_ref().unwrap().num_rows(), 40_000);

    let (mut app, rx, _tx) = app_with(1);
    ask(&mut app, &rx);
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Esc,
        KeyModifiers::NONE,
    )));
    drain_events(&mut app, &rx);
    assert!(!app.confirmation_modal.active);
    assert_eq!(app.input_mode, InputMode::Home);
    assert!(app.data_table_state.is_none(), "declined, nothing was read");

    let (mut app, rx, _tx) = app_with(0);
    pump_open_until_loaded(&mut app, &rx, vec![path], OpenOptions::default());
    assert!(!app.confirmation_modal.active, "0 never asks");
    assert!(app.data_table_state.is_some());
}

/// A file whose name holds `[`, `*` or `?` opens as that file, in every scanned
/// format, not as the pattern its name spells (#625).
#[test]
fn a_file_named_like_a_glob_opens_as_itself() {
    common::isolate_cache();
    let tmp = tempfile::TempDir::new().unwrap();
    let mut wrong = Vec::new();
    for ext in ["csv", "tsv", "parquet", "arrow", "jsonl"] {
        for (stem, sibling) in glob_character_stems() {
            let literal = tmp.path().join(format!("{stem}.{ext}"));
            write_marker(&literal, "literal");
            write_marker(&tmp.path().join(format!("{sibling}.{ext}")), "sibling");
            let (_, df) = open_and_collect(vec![literal], OpenOptions::default());
            let read = marker_values(&df);
            if read != ["literal"] {
                wrong.push(format!("{stem}.{ext}: {read:?}"));
            }
        }
    }
    assert!(wrong.is_empty(), "read as patterns: {wrong:#?}");
}

/// The same for several files named at once, and for a directory with such a name.
#[test]
fn files_and_directories_named_like_globs_open_together() {
    common::isolate_cache();
    let tmp = tempfile::TempDir::new().unwrap();
    for ext in ["csv", "parquet", "arrow"] {
        let mut paths = Vec::new();
        for (stem, sibling) in glob_character_stems() {
            let literal = tmp.path().join(format!("{stem}.{ext}"));
            write_marker(&literal, stem);
            write_marker(&tmp.path().join(format!("{sibling}.{ext}")), "sibling");
            paths.push(literal);
        }
        let (_, df) = open_and_collect(paths, OpenOptions::default());
        let mut want: Vec<String> = glob_character_stems()
            .into_iter()
            .map(|(stem, _)| stem.to_string())
            .collect();
        want.sort();
        assert_eq!(marker_values(&df), want, "{ext}");
    }

    let dir = tmp.path().join("set[1]");
    std::fs::create_dir(&dir).unwrap();
    write_marker(&dir.join("part.parquet"), "inside");
    let decoy = tmp.path().join("set1");
    std::fs::create_dir(&decoy).unwrap();
    write_marker(&decoy.join("part.parquet"), "decoy");
    let (_, df) = open_and_collect(vec![dir.clone()], OpenOptions::default());
    assert_eq!(marker_values(&df), ["inside"]);

    // `--hive` names a file the same way.
    let file = tmp.path().join("d[1].parquet");
    let options = OpenOptions {
        hive: true,
        ..OpenOptions::default()
    };
    let (_, df) = open_and_collect(vec![file], options);
    assert_eq!(marker_values(&df), ["d[1]"]);
}

/// The status footer: at rest the dataset, the position and `? keys`; once the column
/// cursor moves, the column's keys; a sort, a query and a filter in pipeline order;
/// a find's keys; and a line more for a prompt, taken from the table's bottom.
#[test]
fn the_footer_says_what_is_in_effect_and_the_mode_s_keys() {
    use datui::filter_modal::FilterOperator;
    let (mut app, rx, tx) = open_query_filter_fixture("footer_states.csv");
    let none = KeyModifiers::NONE;
    let footer = |app: &mut App, width: u16| {
        screen_at(app, width, 24)
            .lines()
            .last()
            .unwrap()
            .trim_end()
            .to_string()
    };
    let rest = footer(&mut app, 120);
    assert!(rest.contains("footer_states.csv"), "{rest}");
    assert!(rest.contains("1 / 100"), "{rest}");
    assert!(rest.ends_with("? keys"), "{rest}");
    assert!(!rest.contains("Filter"), "no mode keys at rest: {rest}");

    // The column cursor moved: its keys, until a key that is not about the column.
    press_key(&mut app, KeyCode::Char('l'), none);
    let moved = footer(&mut app, 120);
    for hint in ["+/- Filter", "[/] Sort", "F Counts"] {
        assert!(moved.contains(hint), "{hint}: {moved}");
    }
    press_key(&mut app, KeyCode::Char('j'), none);
    assert!(!footer(&mut app, 120).contains("Filter"));

    // `]` sorts by the cursor's column descending, `[` ascending; the same key again
    // takes the sort away, back to the natural order.
    let g = datui::glyphs::get();
    let sorted = |app: &App| {
        let state = app.data_table_state.as_ref().unwrap();
        (
            state.view_sort_columns().to_vec(),
            state.view_sort_descending().to_vec(),
        )
    };
    run_and_settle(
        &mut app,
        AppEvent::Key(KeyEvent::new(KeyCode::Char(']'), none)),
        &rx,
        &tx,
    );
    assert_eq!(sorted(&app), (vec!["c".to_string()], vec![true]));
    let line = footer(&mut app, 120);
    assert!(line.contains(&format!("c {}", g.sort_desc)), "{line}");
    run_and_settle(
        &mut app,
        AppEvent::Key(KeyEvent::new(KeyCode::Char('['), none)),
        &rx,
        &tx,
    );
    assert_eq!(sorted(&app), (vec!["c".to_string()], vec![false]));
    run_and_settle(
        &mut app,
        AppEvent::Key(KeyEvent::new(KeyCode::Char('['), none)),
        &rx,
        &tx,
    );
    assert_eq!(sorted(&app), (Vec::new(), Vec::new()));
    let state = app.data_table_state.as_ref().unwrap();
    assert!(state.view_sort_ascending(), "not left reversed");
    let first = state.lf().clone().collect().unwrap();
    assert_eq!(
        first.column("a").unwrap().get(0).unwrap(),
        AnyValue::Int64(0)
    );

    // A query, then a filter on it: dataset › query › filter.
    run_and_settle(
        &mut app,
        AppEvent::QQuery("select where a < 50".to_string()),
        &rx,
        &tx,
    );
    run_and_settle(
        &mut app,
        AppEvent::Filter(vec![filter_stmt("a", FilterOperator::Gt, "10")]),
        &rx,
        &tx,
    );
    let line = footer(&mut app, 120);
    let at = |s: &str| line.find(s).unwrap_or_else(|| panic!("{s:?} in {line:?}"));
    assert!(at("footer_states.csv") < at("query") && at("query") < at("a > 10"));
    assert!(line.contains("1 / 39"), "{line}");
    // Short of room, the filter is counted and the name goes; help stays.
    let narrow = footer(&mut app, 40);
    assert!(!narrow.contains("footer_states"), "{narrow}");
    assert!(
        narrow.ends_with('?') || narrow.ends_with("? keys"),
        "{narrow}"
    );

    // The find prompt is a line under the status line, the table a row shorter.
    let before = screen_at(&mut app, 120, 24);
    press_key(&mut app, KeyCode::Char('/'), none);
    for c in "alpha_2".chars() {
        press_key(&mut app, KeyCode::Char(c), none);
    }
    let typing = screen_at(&mut app, 120, 24);
    let lines: Vec<&str> = typing.lines().collect();
    assert!(lines[23].trim_start().starts_with("/ alpha_2"), "{typing}");
    assert!(lines[22].contains("Enter Next"), "{typing}");
    assert!(lines[23].contains("on screen"), "{typing}");
    assert_eq!(
        before.lines().nth(1),
        typing.lines().nth(1),
        "the table's top stays put"
    );
    run_and_settle(
        &mut app,
        AppEvent::Key(KeyEvent::new(KeyCode::Enter, none)),
        &rx,
        &tx,
    );
    let found = footer(&mut app, 120);
    assert!(
        found.contains("n/N Next") && found.contains("Esc Clear"),
        "{found}"
    );
    assert!(found.contains("match 1"), "{found}");
}

/// "Combine into datetime" from the cell menu makes what a spec's derived column
/// makes from the same columns, before the first of them.
#[test]
fn combine_into_datetime_matches_the_specs_column() {
    let path = common::fixture_dir().join("combine_like_spec.csv");
    std::fs::write(
        &path,
        "Lcl Date,Lcl Time,UTCOfst,v\n2024-03-01,10:00:00,-05:00,1\n2024-03-01,10:00:01,+0530,2\n,,,3\n",
    )
    .unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path.clone()], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);

    // The cursor is on Lcl Date; the menu's last line combines.
    app.open_context_menu(ratatui::layout::Position { x: 2, y: 2 });
    app.event(key(KeyCode::Up));
    app.event(key(KeyCode::Enter));
    assert!(matches!(app.overlay, datui::Overlay::Combine { .. }));
    // Date, Time, then the UTC offset: Space picks it.
    app.event(key(KeyCode::Tab));
    app.event(key(KeyCode::Tab));
    app.event(key(KeyCode::Char(' ')));
    type_into(&mut app, "UTC");
    app.event(key(KeyCode::Enter));
    app.event(key(KeyCode::Enter));
    assert_eq!(app.input_mode, datui::InputMode::Normal);
    pump_until_idle(&mut app, &rx, &tx);
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(
        &state.get_column_order()[..2],
        ["datetime", "Lcl Date"],
        "before its first source"
    );
    let made = view_frame(&app).column("datetime").unwrap().clone();

    let spec = datui::formats::Spec::parse(
        r#"
name = "acme.combine"
kind = "delimited"
[columns]
time = { from = ["Lcl Date", "Lcl Time", "UTCOfst"], as = "datetime" }
"#,
        None,
    )
    .unwrap();
    let (tx, rx) = mpsc::channel();
    let mut by_spec = App::new(tx.clone(), common::test_runtime());
    by_spec.set_formats(datui::formats::Registry::of(vec![spec]));
    pump_open_until_loaded(
        &mut by_spec,
        &rx,
        vec![path],
        OpenOptions {
            spec_name: Some("acme.combine".into()),
            ..OpenOptions::default()
        },
    );
    assert!(
        by_spec.error_message().is_none(),
        "{:?}",
        by_spec.error_message()
    );
    let derived = view_frame(&by_spec).column("time").unwrap().clone();
    assert_eq!(made.dtype(), derived.dtype());
    assert!(
        made.as_materialized_series()
            .equals_missing(derived.as_materialized_series()),
        "{made:?} {derived:?}"
    );
}

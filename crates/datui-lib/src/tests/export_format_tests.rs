use crate::app::background::Counted;
use crate::export::export_modal::ExportFormat;
use crate::table::OpenFacts;
use crate::*;
use std::path::Path;

fn opts() -> OpenOptions {
    OpenOptions::default()
}

#[test]
fn a_collect_in_flight_serves_the_frame_but_not_a_changed_frame() {
    use crate::app::modals::filter_modal::{FilterOperator, FilterStatement, LogicalOperator};
    use polars::prelude::IntoLazy;

    let (tx, _rx) = std::sync::mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    let lf = polars::df!("a" => (0..100).collect::<Vec<i32>>())
        .unwrap()
        .lazy();
    let mut state = DataTableState::from_lazyframe(lf, &opts()).unwrap();
    state.visible_rows = 10;
    let dataset = state.len_generation();
    let columns = InflightCollect::columns_of(&state);
    app.data_table_state = Some(state);
    let generation = app.task_generation();
    let _read = app.job_for_tests(
        Job::Rows(InflightCollect {
            began: std::time::Instant::now(),
            files: None,
            dataset,
            columns,
            start: 0,
            end: 50,
        }),
        Some(App::LOADING_BUFFER),
    );

    // The frame that sized the table asks again: the collect on its way covers
    // the view, so nothing new is planned.
    assert!(app.spawn_async_collect(App::LOADING_BUFFER));
    assert_eq!(app.task_generation(), generation);

    // A filter changes the data underneath; those rows no longer answer.
    app.event(AppEvent::Filter(vec![FilterStatement {
        columns: Vec::new(),
        column: "a".to_string(),
        operator: FilterOperator::Lt,
        value: "50".to_string(),
        logical_op: LogicalOperator::And,
    }]));
    assert_ne!(
        app.task_generation(),
        generation,
        "a fresh collect was planned"
    );
    let inflight = app.rows_in_flight().expect("the new collect is recorded");
    assert_ne!(inflight.dataset, dataset);
}

#[test]
fn a_short_read_on_a_remote_scan_is_the_count() {
    use crate::app::modals::filter_modal::{FilterOperator, FilterStatement, LogicalOperator};
    use polars::prelude::IntoLazy;

    // A frame whose len() cannot be taken: a short read has to answer without it.
    let unreadable = LazyFrame::scan_parquet(
        polars::prelude::PlRefPath::new("/nonexistent/for-this-test.parquet"),
        Default::default(),
    )
    .unwrap();
    let job = LenCount {
        len_generation: 7,
        count_dir: None,
        files: None,
        counter: None,
        lf: unreadable,
        streaming: false,
        meter: Arc::new(crate::loading::measurements::Meter::default()),
        progress: Default::default(),
    };
    let rows = |counted: Result<Counted, ()>| counted.map(|c| c.rows);
    assert_eq!(
        rows(job.after_collect(1_000, 30, 70)),
        Ok(1_030),
        "short: known"
    );
    assert_eq!(
        rows(job.after_collect(1_000, 70, 70)),
        Err(()),
        "full: counted"
    );
    assert_eq!(
        rows(job.after_collect(1_000, 0, 70)),
        Err(()),
        "deep and empty: counted"
    );

    // Through the harness: a filtered remote frame gets its count from the
    // collect that came back short, and the len() never runs alongside it.
    let (tx, rx) = std::sync::mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    let lf = polars::df!("a" => (0..100).collect::<Vec<i32>>())
        .unwrap()
        .lazy();
    let mut state = DataTableState::from_lazyframe(lf, &opts())
        .unwrap()
        .with_open(OpenFacts {
            remote_source: true,
            ..Default::default()
        });
    state.visible_rows = 10;
    state.deferred(|s| {
        s.filter(vec![FilterStatement {
            columns: Vec::new(),
            column: "a".to_string(),
            operator: FilterOperator::Lt,
            value: "50".to_string(),
            logical_op: LogicalOperator::And,
        }])
    });
    assert!(!state.is_num_rows_valid());
    let dataset = state.len_generation();
    app.data_table_state = Some(state);
    assert!(app.spawn_async_collect("Filtering..."));
    assert_eq!(app.counting.len_count_inflight, Some(dataset));

    let wait = std::time::Duration::from_secs(20);
    let first = rx.recv_timeout(wait).expect("the collect lands");
    assert!(
        matches!(first, AppEvent::JobEnded(t) if t.kind() == crate::JobKind::Rows),
        "the buffer comes first"
    );
    let second = rx.recv_timeout(wait).expect("the count follows");
    assert!(
        matches!(
            second,
            AppEvent::BackgroundLenReady {
                len_generation,
                num_rows: 50,
                ..
            } if len_generation == dataset
        ),
        "the short read of 50 rows is the count"
    );
    app.event(first);
    app.event(second);
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.num_rows_if_valid(), Some(50));
    assert_eq!(app.counting.len_count_inflight, None);
}

#[test]
fn end_on_an_uncounted_remote_dataset_waits_for_the_count() {
    use polars::prelude::IntoLazy;
    let (tx, rx) = std::sync::mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    let lf = polars::df!("a" => (0..1_000).collect::<Vec<i32>>())
        .unwrap()
        .lazy();
    let whole = lf.clone();
    let mut state = DataTableState::from_lazyframe(lf, &opts())
        .unwrap()
        .with_open(OpenFacts {
            remote_source: true,
            remote_files: Some(crate::table::RemoteFiles {
                urls: Arc::new(vec!["one".to_string(), "two".to_string()]),
                scan: Arc::new(
                    move |urls: &[String], _as_text: &[polars::prelude::PlSmallStr]| {
                        Ok(if urls.len() == 2 {
                            whole.clone()
                        } else if urls[0] == "one" {
                            whole.clone().slice(0, 400)
                        } else {
                            whole.clone().slice(400, 600)
                        })
                    },
                ),
                count: Arc::new(|_| Ok(vec![vec![400], vec![300, 300]])),
                offsets: None,
            }),
            ..Default::default()
        });
    state.visible_rows = 10;
    let dataset = state.len_generation();
    app.data_table_state = Some(state);

    // No jump to a guess: the count starts, and nothing is busy.
    assert!(app.jump_key(Scroll::End).is_none());
    assert!(!app.is_busy());
    assert_eq!(app.counting.end_after_count, Some(dataset));
    assert_eq!(app.counting.len_count_inflight, Some(dataset));
    assert_eq!(app.data_table_state.as_ref().unwrap().start_row(), 0);

    let counted = rx
        .recv_timeout(std::time::Duration::from_secs(20))
        .expect("the count");
    assert!(matches!(
        &counted,
        AppEvent::BackgroundLenReady {
            num_rows: 1_000,
            file_row_groups: Some(_),
            ..
        }
    ));
    // With the count in, End goes.
    let next = app.event(counted);
    assert!(
        matches!(next, Some(AppEvent::Scroll(Scroll::End))),
        "the jump follows the count"
    );
    assert_eq!(app.counting.end_after_count, None);
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.num_rows_if_valid(), Some(1_000));
}

/// A local frame of `rows` rows of `a`, filtered to `a < keep`, with no count yet.
fn filtered_local(rows: i32, keep: i32) -> (App, std::sync::mpsc::Receiver<AppEvent>, u64) {
    use crate::app::modals::filter_modal::{FilterOperator, FilterStatement, LogicalOperator};
    use polars::prelude::IntoLazy;
    let (tx, rx) = std::sync::mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    let lf = polars::df!("a" => (0..rows).collect::<Vec<i32>>())
        .unwrap()
        .lazy();
    let mut state = DataTableState::from_lazyframe(lf, &opts()).unwrap();
    state.visible_rows = 10;
    state.deferred(|s| {
        s.filter(vec![FilterStatement {
            columns: Vec::new(),
            column: "a".to_string(),
            operator: FilterOperator::Lt,
            value: keep.to_string(),
            logical_op: LogicalOperator::And,
        }])
    });
    assert!(!state.is_num_rows_valid());
    let dataset = state.len_generation();
    app.data_table_state = Some(state);
    (app, rx, dataset)
}

fn recv(rx: &std::sync::mpsc::Receiver<AppEvent>) -> AppEvent {
    rx.recv_timeout(std::time::Duration::from_secs(60))
        .expect("background work reports back")
}

/// Handle events until the count for `dataset` lands, and return it with what its
/// handling asked for next.
fn until_counted(
    app: &mut App,
    rx: &std::sync::mpsc::Receiver<AppEvent>,
    dataset: u64,
) -> (usize, Option<AppEvent>) {
    loop {
        let event = recv(rx);
        if let AppEvent::BackgroundLenFailed { len_generation } = &event {
            assert_ne!(*len_generation, dataset, "the count failed");
        }
        let counted = match &event {
            AppEvent::BackgroundLenReady {
                len_generation,
                num_rows,
                ..
            } if *len_generation == dataset => Some(*num_rows),
            _ => None,
        };
        let next = app.event(event);
        if let Some(rows) = counted {
            return (rows, next);
        }
    }
}

/// A full count of a local filtered frame does not start until its first page is
/// in and painted, and starts once however often the view asks for rows.
#[test]
fn a_local_count_waits_for_its_page_to_be_painted() {
    let (mut app, rx, dataset) = filtered_local(100_000, 50_000);
    assert!(app.spawn_async_collect("Filtering..."));
    assert_eq!(
        app.counting.len_count_inflight,
        Some(dataset),
        "a count is coming"
    );
    assert_eq!(app.counting.count_after_paint, Some(dataset));
    assert!(app.row_count_pending());
    // Frames painted, and the view asking again, while the page is read.
    app.frame_painted();
    assert!(app.spawn_async_collect("Filtering..."));
    app.frame_painted();
    assert!(!app.count_waits_for_a_frame());
    assert_eq!(app.counting.counts_spawned.get(), 0);

    let ready = recv(&rx);
    assert!(matches!(ready, AppEvent::JobEnded(t) if t.kind() == crate::JobKind::Rows));
    app.event(ready);
    assert_eq!(
        app.counting.counts_spawned.get(),
        0,
        "the rows are in, not yet painted"
    );
    assert!(app.count_waits_for_a_frame());

    app.frame_painted();
    assert_eq!(app.counting.counts_spawned.get(), 1);
    // Scrolling and painting again do not start another.
    for _ in 0..3 {
        app.handle_scroll(|state| state.half_page_down());
        app.frame_painted();
    }
    assert_eq!(app.counting.counts_spawned.get(), 1);
    assert_eq!(until_counted(&mut app, &rx, dataset).0, 50_000);
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.num_rows_if_valid(), Some(50_000));
    assert_eq!(app.counting.len_count_inflight, None);
    assert!(!app.row_count_pending());
}

/// A first page that comes back short is the count: none is taken, for some rows
/// or for none at all.
#[test]
fn a_short_first_page_is_the_count_of_a_local_frame() {
    for keep in [30, 0] {
        let (mut app, rx, dataset) = filtered_local(100_000, keep);
        assert!(app.spawn_async_collect("Filtering..."));
        let ready = recv(&rx);
        assert!(matches!(ready, AppEvent::JobEnded(t) if t.kind() == crate::JobKind::Rows));
        app.event(ready);
        let state = app.data_table_state.as_ref().unwrap();
        assert_eq!(state.len_generation(), dataset);
        assert_eq!(state.num_rows_if_valid(), Some(keep as usize));
        assert_eq!(app.counting.count_after_paint, None);
        assert_eq!(app.counting.len_count_inflight, None);
        assert!(!app.row_count_pending());
        app.frame_painted();
        assert_eq!(app.counting.counts_spawned.get(), 0, "{keep} rows");
    }
}

/// A first page exactly as long as the frame cannot say it is the end: the count
/// is taken after the paint, and it is exact.
#[test]
fn a_page_that_fills_exactly_is_still_counted() {
    let (mut probe, _rx, _) = filtered_local(100_000, 50_000);
    probe.spawn_async_collect("Filtering...");
    let inflight = probe.rows_in_flight().expect("a page is read");
    let page = inflight.end - inflight.start;

    let (mut app, rx, dataset) = filtered_local(100_000, page as i32);
    app.spawn_async_collect("Filtering...");
    app.event(recv(&rx));
    assert_eq!(
        app.data_table_state.as_ref().unwrap().num_rows_if_valid(),
        None
    );
    assert!(app.count_waits_for_a_frame());
    app.frame_painted();
    assert_eq!(app.counting.counts_spawned.get(), 1);
    assert_eq!(until_counted(&mut app, &rx, dataset).0, page);
}

/// End on a local frame not yet counted waits for the count rather than going to
/// the end of the rows read so far, and starts it at once if it was waiting on the
/// paint.
#[test]
fn end_before_the_paint_starts_the_count_and_waits_for_it() {
    let (mut app, rx, dataset) = filtered_local(100_000, 50_000);
    app.spawn_async_collect("Filtering...");
    app.event(recv(&rx));
    assert_eq!(app.counting.counts_spawned.get(), 0);

    assert!(app.jump_key(Scroll::End).is_none());
    assert_eq!(app.counting.end_after_count, Some(dataset));
    assert_eq!(app.counting.counts_spawned.get(), 1, "started for the End");
    assert_eq!(app.status_message.as_deref(), Some(App::COUNTING_FOR_END));
    assert!(app.row_count_pending(), "the bar spins while it counts");
    assert_eq!(app.data_table_state.as_ref().unwrap().start_row(), 0);
    // The paint that follows, and End again, do not start a second.
    app.frame_painted();
    assert!(app.jump_key(Scroll::End).is_none());
    assert_eq!(app.counting.counts_spawned.get(), 1);

    let (rows, next) = until_counted(&mut app, &rx, dataset);
    assert_eq!(rows, 50_000);
    assert!(
        matches!(next, Some(AppEvent::Scroll(Scroll::End))),
        "the jump follows"
    );
    assert_eq!(app.counting.end_after_count, None);
}

/// A held count that End starts is marked running, whatever cleared the marker
/// meanwhile (a return from Data Quality's rows does): a second End waits on it
/// rather than starting another.
#[test]
fn a_count_end_starts_is_marked_running() {
    let (mut app, rx, dataset) = filtered_local(100_000, 50_000);
    app.spawn_async_collect("Filtering...");
    app.event(recv(&rx));
    app.counting.len_count_inflight = None;
    assert!(app.jump_key(Scroll::End).is_none());
    assert_eq!(app.counting.len_count_inflight, Some(dataset));
    assert!(app.jump_key(Scroll::End).is_none());
    app.frame_painted();
    assert_eq!(app.counting.counts_spawned.get(), 1);
    assert_eq!(until_counted(&mut app, &rx, dataset).0, 50_000);
}

/// A count that failed is not started again by scrolling or painting; End asks
/// for it again.
#[test]
fn a_failed_count_is_retried_by_end_not_by_scrolling() {
    let (mut app, rx, dataset) = filtered_local(100_000, 50_000);
    app.counting.len_count_failed = Some(dataset);
    app.spawn_async_collect("Filtering...");
    assert_eq!(app.counting.len_count_inflight, None);
    assert_eq!(app.counting.count_after_paint, None);
    app.event(recv(&rx));
    app.frame_painted();
    app.handle_scroll(|state| state.half_page_down());
    app.frame_painted();
    assert_eq!(app.counting.counts_spawned.get(), 0);
    assert!(!app.row_count_pending());

    assert!(app.jump_key(Scroll::End).is_none());
    assert_eq!(app.counting.counts_spawned.get(), 1);
    let (rows, _) = until_counted(&mut app, &rx, dataset);
    assert_eq!(rows, 50_000);
    assert_eq!(app.counting.len_count_failed, None);
}

/// A page that fails to read takes the count waiting on it down with it, marked
/// failed, and never starts it.
#[test]
fn a_page_that_fails_fails_the_count_waiting_on_it() {
    use polars::prelude::*;
    let (tx, rx) = std::sync::mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    let values: Vec<String> = (0..1_000)
        .map(|i| {
            if i == 5 {
                "x".to_string()
            } else {
                i.to_string()
            }
        })
        .collect();
    let lf = df!("s" => values)
        .unwrap()
        .lazy()
        .with_column(col("s").strict_cast(DataType::Int64));
    let mut state = DataTableState::from_lazyframe(lf, &opts()).unwrap();
    state.visible_rows = 10;
    state.invalidate_num_rows();
    let dataset = state.len_generation();
    app.data_table_state = Some(state);

    app.spawn_async_collect(App::LOADING_BUFFER);
    assert_eq!(app.counting.count_after_paint, Some(dataset));
    let failed = recv(&rx);
    assert!(matches!(failed, AppEvent::JobEnded(t) if t.kind() == crate::JobKind::Rows));
    app.event(failed);
    assert_eq!(app.counting.count_after_paint, None);
    assert_eq!(app.counting.len_count_inflight, None);
    assert_eq!(app.counting.len_count_failed, Some(dataset));
    app.frame_painted();
    assert_eq!(app.counting.counts_spawned.get(), 0);
}

/// A page whose worker dies takes the count waiting on it down too, as a page that
/// fails to read does.
#[test]
fn a_page_whose_worker_dies_fails_the_count_waiting_on_it() {
    let (mut app, rx, dataset) = filtered_local(100_000, 50_000);
    app.jobs.worker_dies = crate::tests::worker_dies_once(|job| matches!(job, Job::Rows(_)));
    app.spawn_async_collect(App::LOADING_BUFFER);
    assert_eq!(app.counting.count_after_paint, Some(dataset));
    let died = recv(&rx);
    assert!(matches!(
        died,
        AppEvent::JobEnded(t) if t.kind() == crate::JobKind::Rows
    ));
    app.event(died);
    assert_eq!(app.counting.count_after_paint, None);
    assert_eq!(app.counting.len_count_inflight, None);
    assert_eq!(app.counting.len_count_failed, Some(dataset));
    assert!(!app.count_waits_for_a_frame());
    app.frame_painted();
    assert_eq!(app.counting.counts_spawned.get(), 0);
}

/// A count waiting on a paint for a frame the view has left is never started:
/// the frame that replaced it gets the one count.
#[test]
fn a_count_for_a_replaced_frame_never_starts() {
    use crate::app::modals::filter_modal::{FilterOperator, FilterStatement, LogicalOperator};
    let (mut app, rx, first) = filtered_local(100_000, 50_000);
    app.spawn_async_collect("Filtering...");
    assert_eq!(app.counting.count_after_paint, Some(first));
    app.event(AppEvent::Filter(vec![FilterStatement {
        columns: Vec::new(),
        column: "a".to_string(),
        operator: FilterOperator::Lt,
        value: "40000".to_string(),
        logical_op: LogicalOperator::And,
    }]));
    let second = app.data_table_state.as_ref().unwrap().len_generation();
    assert_ne!(second, first);
    assert_eq!(app.counting.count_after_paint, Some(second));
    assert_eq!(app.counting.len_count_inflight, Some(second));
    while !app.count_waits_for_a_frame() {
        let event = recv(&rx);
        app.event(event);
    }
    app.frame_painted();
    assert_eq!(app.counting.counts_spawned.get(), 1);
    assert_eq!(until_counted(&mut app, &rx, second).0, 40_000);

    // Another dataset put on screen retires a count still waiting on the last.
    let (mut app, _rx, dataset) = filtered_local(100_000, 50_000);
    app.spawn_async_collect("Filtering...");
    let other = DataTableState::from_lazyframe(
        polars::prelude::IntoLazy::lazy(polars::df!("b" => [1, 2, 3]).unwrap()),
        &opts(),
    )
    .unwrap();
    app.install_for_tests(other, None, &opts(), None);
    assert_ne!(app.counting.count_after_paint, Some(dataset));
    assert_ne!(app.counting.len_count_inflight, Some(dataset));
    app.frame_painted();
    assert_eq!(
        app.counting.counts_spawned.get(),
        0,
        "the old dataset is not counted"
    );
}

/// A count that reads only footers is not held for the paint: it reads no data.
#[test]
fn a_footer_count_starts_with_the_page() {
    use polars::prelude::*;
    let dir = tempfile::tempdir().unwrap();
    let mut file = std::fs::File::create(dir.path().join("part.parquet")).unwrap();
    let mut df = df!("a" => (0..700).collect::<Vec<i32>>()).unwrap();
    ParquetWriter::new(&mut file).finish(&mut df).unwrap();

    let (tx, rx) = std::sync::mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    let mut state = DataTableState::from_lazyframe(df.lazy(), &opts())
        .unwrap()
        .with_open(OpenFacts {
            parquet_count_dir: Some(dir.path().to_path_buf()),
            ..Default::default()
        });
    state.visible_rows = 10;
    state.invalidate_num_rows();
    let dataset = state.len_generation();
    app.data_table_state = Some(state);

    app.spawn_async_collect(App::LOADING_BUFFER);
    assert_eq!(
        app.counting.counts_spawned.get(),
        1,
        "started beside the page"
    );
    assert_eq!(app.counting.count_after_paint, None);
    assert_eq!(until_counted(&mut app, &rx, dataset).0, 700);
}

#[test]
fn a_filter_applied_from_the_end_shows_its_rows() {
    // End on a 10,000-row remote object, then a filter matching 100 rows: the view
    // comes back to the top and the count is the filter's, not the old position.
    use crate::app::modals::filter_modal::{FilterOperator, FilterStatement, LogicalOperator};
    use polars::prelude::IntoLazy;

    let (tx, rx) = std::sync::mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    let lf = polars::df!("a" => (0..10_000).collect::<Vec<i32>>())
        .unwrap()
        .lazy();
    let mut state = DataTableState::from_lazyframe(lf, &opts())
        .unwrap()
        .with_open(OpenFacts {
            remote_source: true,
            row_groups: vec![vec![10_000]],
            ..Default::default()
        });
    state.visible_rows = 10;
    assert!(state.deferred(DataTableState::scroll_to_end));
    app.data_table_state = Some(state);

    app.event(AppEvent::Filter(vec![FilterStatement {
        columns: Vec::new(),
        column: "a".to_string(),
        operator: FilterOperator::Lt,
        value: "100".to_string(),
        logical_op: LogicalOperator::And,
    }]));
    let wait = std::time::Duration::from_secs(20);
    for _ in 0..2 {
        let event = rx.recv_timeout(wait).expect("the collect, then the count");
        app.event(event);
    }
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.num_rows_if_valid(), Some(100));
    assert_eq!(state.start_row(), 0);
    assert!(
        state.buffered_start() == 0 && state.buffered_end() >= 10,
        "the first page is on hand: {}..{}",
        state.buffered_start(),
        state.buffered_end()
    );
}

#[test]
fn compressed_csv_still_defaults_to_csv() {
    // `sales.csv.gz` has extension `gz`; the `.csv` that decides this is in the
    // stem. Reading the extension alone offered no export default at all.
    assert_eq!(
        App::export_format_for(Path::new("sales.csv.gz"), None),
        Some(ExportFormat::Csv)
    );
    assert_eq!(
        App::export_format_for(Path::new("sales.csv.zst"), None),
        Some(ExportFormat::Csv)
    );
}

#[test]
fn plain_extensions_map_to_their_formats() {
    for (name, expected) in [
        ("a.parquet", Some(ExportFormat::Parquet)),
        ("a.csv", Some(ExportFormat::Csv)),
        ("a.tsv", Some(ExportFormat::Tsv)),
        ("a.psv", Some(ExportFormat::Psv)),
        ("a.tsv.gz", Some(ExportFormat::Tsv)),
        ("a.json", Some(ExportFormat::Json)),
        ("a.ndjson", Some(ExportFormat::Ndjson)),
        ("a.jsonl", Some(ExportFormat::Ndjson)),
        ("a.arrow", Some(ExportFormat::Ipc)),
        ("a.feather", Some(ExportFormat::Ipc)),
        ("a.avro", Some(ExportFormat::Avro)),
        ("a.xlsx", None),
        ("a.orc", None),
        ("a.unknown", None),
    ] {
        assert_eq!(
            App::export_format_for(Path::new(name), None),
            expected,
            "{name}"
        );
    }
}

#[test]
fn the_format_read_beats_the_extension() {
    assert_eq!(
        App::export_format_for(Path::new("mislabelled.csv"), Some(FileFormat::Parquet)),
        Some(ExportFormat::Parquet)
    );
}

/// A destination refusal reads as one; an encoder's error of the same kind
/// does not borrow its wording.
#[test]
fn export_errors_name_a_refusal_only_for_a_refusal() {
    let refused: std::io::Error = crate::export::output_file::Refused::NotAFile.into();
    assert_eq!(
        App::format_export_error(&refused.into()),
        "Cannot write: it is not a regular file."
    );
    let encoder = std::io::Error::new(std::io::ErrorKind::InvalidInput, "stream error");
    let message = App::format_export_error(&encoder.into());
    assert!(!message.contains("regular file"), "{message}");
}

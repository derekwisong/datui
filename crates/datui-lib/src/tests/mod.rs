use std::path::Path;
use std::process::Command;
use std::sync::Once;

static INIT: Once = Once::new();

#[cfg(feature = "cloud")]
mod cloud_recent_facts {
    use crate::dataset_files::DatasetFile;
    use crate::discover::EntryKind;
    use crate::schema_union::FileFooter;
    use polars::prelude::{DataType, Field, Schema};
    use std::sync::Arc;

    fn file(key: &str, size: u64, stamp: u64) -> DatasetFile {
        DatasetFile {
            key: key.to_string(),
            size,
            stamp,
            etag: None,
        }
    }

    /// What an open of `full` would record from the footers at `read`.
    fn facts_of_open(
        full: &str,
        files: &[DatasetFile],
        read: &[usize],
        footers: &[Option<FileFooter>],
    ) -> Option<(std::path::PathBuf, crate::cache::DatasetFacts)> {
        static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
        let runtime = RUNTIME.get_or_init(|| tokio::runtime::Runtime::new().unwrap());
        let source = crate::dataset_files::StoreFiles::new(
            full,
            String::new(),
            None,
            Arc::new(object_store::memory::InMemory::new()),
            Default::default(),
            runtime.handle(),
        );
        crate::dataset_files::facts_of(Arc::new(source), files.to_vec(), read, footers)
    }

    fn footer(rows: &[usize]) -> Option<FileFooter> {
        let schema = Schema::from_iter([
            Field::new("id".into(), DataType::Int64),
            Field::new("amount".into(), DataType::Float64),
        ]);
        Some(FileFooter {
            schema: Arc::new(schema),
            row_group_rows: rows.to_vec(),
            row_group_bytes: rows.iter().map(|r| r * 8).collect(),
            file_bytes: 0,
            column_bytes: Vec::new(),
        })
    }

    /// What the home screen shows for a recent opened from a bucket comes from the
    /// record the open wrote: rows, columns, the kind and what it holds, under the
    /// URL as opened.
    #[test]
    fn an_open_records_what_the_home_screen_will_show() {
        let files = vec![
            file("sales/year=2024/part-0.parquet", 1000, 10),
            file("sales/year=2025/part-0.parquet", 2000, 20),
        ];
        let read = vec![0, 1];
        let footers = vec![footer(&[5, 7]), footer(&[8])];
        let (path, facts) =
            facts_of_open("s3://bucket/sales/", &files, &read, &footers).expect("facts");
        assert_eq!(path, std::path::PathBuf::from("s3://bucket/sales/"));
        assert_eq!(facts.rows, Some(20));
        assert_eq!(facts.cols, Some(3), "the partition column counts");
        assert!(!facts.cols_sampled);
        assert_eq!(facts.kind, Some(EntryKind::Hive));
        assert_eq!(facts.holds.formats, vec![("parquet".to_string(), 2)]);
        assert_eq!(facts.size, 3000);
        assert_eq!(facts.mtime, 20);
        assert_eq!(facts.classified_by, crate::discover::CLASSIFIER_VERSION);
        assert!(facts.columns.iter().any(|c| c == "amount"));

        // A flat prefix is a directory of files; one object is a file.
        let flat = vec![file("sales/a.parquet", 1, 1), file("sales/b.parquet", 1, 1)];
        let (_, facts) = facts_of_open(
            "s3://bucket/sales/",
            &flat,
            &[0, 1],
            &[footer(&[1]), footer(&[1])],
        )
        .unwrap();
        assert_eq!(facts.kind, Some(EntryKind::MultiFile));
        let one = vec![file("sales/a.parquet", 1, 1)];
        let (_, facts) =
            facts_of_open("s3://bucket/sales/a.parquet", &one, &[0], &[footer(&[4])]).unwrap();
        assert_eq!(facts.kind, Some(EntryKind::File));
        assert!(facts.holds.is_empty());
        assert_eq!(facts.rows, Some(4));
    }

    /// `--hive` on a prefix with no trailing slash lists the prefix's files and is a
    /// directory; on a single object it lists that object and is a file.
    #[test]
    fn a_prefix_without_its_slash_is_still_a_directory() {
        let files = vec![file("sales/a.parquet", 1, 1), file("sales/b.parquet", 1, 1)];
        let (_, facts) = facts_of_open(
            "s3://bucket/sales",
            &files,
            &[0, 1],
            &[footer(&[1]), footer(&[1])],
        )
        .unwrap();
        assert_eq!(facts.kind, Some(EntryKind::MultiFile));
        let one = vec![file("sales/a.parquet", 1, 1)];
        let (_, facts) =
            facts_of_open("s3://bucket/sales/a.parquet", &one, &[0], &[footer(&[1])]).unwrap();
        assert_eq!(facts.kind, Some(EntryKind::File));
    }

    /// A sampled read does not replace a whole one: the shape cache forgets a
    /// dataset long before the index does, and a reopen reads a sample first.
    #[test]
    fn a_sample_never_replaces_a_whole_record() {
        let whole = crate::cache::DatasetFacts {
            rows: Some(20),
            ..Default::default()
        };
        let sample = crate::cache::DatasetFacts {
            rows: None,
            cols_sampled: true,
            ..Default::default()
        };
        assert!(crate::dataset_files::facts_worth_recording(None, &sample));
        assert!(crate::dataset_files::facts_worth_recording(
            Some(&sample),
            &sample
        ));
        assert!(crate::dataset_files::facts_worth_recording(
            Some(&sample),
            &whole
        ));
        assert!(crate::dataset_files::facts_worth_recording(
            Some(&whole),
            &whole
        ));
        assert!(!crate::dataset_files::facts_worth_recording(
            Some(&whole),
            &sample
        ));
    }

    /// One object opened from a bucket is recorded from the footer the open read:
    /// its rows and columns, no size, which the row then does not show as zero.
    #[test]
    fn one_object_is_recorded_from_its_footer() {
        let tmp = tempfile::tempdir().unwrap();
        let cache = crate::cache::CacheManager::with_dir(tmp.path().to_path_buf());
        let schema = Schema::from_iter([Field::new("id".into(), DataType::Int64)]);
        let footer = crate::cloud_hive::FileFooter {
            schema: Arc::new(schema),
            row_group_rows: vec![3, 4],
            row_group_bytes: Vec::new(),
            file_bytes: 0,
            column_bytes: Vec::new(),
        };
        crate::App::record_cloud_object_facts(Some(&cache), "s3://bucket/x.parquet", &footer);
        let known = cache.load_dataset_facts();
        let facts = known
            .get(std::path::Path::new("s3://bucket/x.parquet"))
            .expect("recorded");
        assert_eq!(facts.rows, Some(7));
        assert_eq!(facts.cols, Some(1));
        assert_eq!(facts.kind, Some(EntryKind::File));
        assert_eq!(facts.size, 0);
        assert!(
            facts.mtime > 0,
            "dated, so the index does not evict it first"
        );
    }

    /// A sampled read knows the columns and not the rows, and says the width is a
    /// floor.
    #[test]
    fn a_sampled_read_records_columns_but_no_row_count() {
        let files: Vec<DatasetFile> = (0..5)
            .map(|i| file(&format!("x/p{i}.parquet"), 10, 1))
            .collect();
        let (_, facts) = facts_of_open(
            "s3://bucket/x/",
            &files,
            &[0, 4],
            &[footer(&[1]), footer(&[1])],
        )
        .unwrap();
        assert_eq!(facts.rows, None);
        assert_eq!(facts.cols, Some(2));
        assert!(facts.cols_sampled);
    }
}

/// Ensures that sample data files are generated before tests run.
/// This function uses `std::sync::Once` to ensure it only runs once,
/// even if called from multiple tests.
pub fn ensure_sample_data() {
    INIT.call_once(|| {
        // When the lib is in crates/datui-lib, repo root is CARGO_MANIFEST_DIR/../..
        let repo_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let sample_data_dir = repo_root.join("tests/sample-data");

        // Check if key files exist to determine if we need to generate data
        // We check for a few representative files that should always be generated
        let key_files = [
            "people.parquet",
            "sales.parquet",
            "large_dataset.parquet",
            "empty.parquet",
            "pivot_long.parquet",
            "melt_wide.parquet",
            "infer_schema_length_data.csv",
        ];

        let needs_generation = !sample_data_dir.exists()
            || key_files
                .iter()
                .any(|file| !sample_data_dir.join(file).exists());

        if needs_generation {
            // Get the path to the Python script (at repo root)
            let script_path = repo_root.join("scripts/generate_sample_data.py");
            if !script_path.exists() {
                panic!(
                    "Sample data generation script not found at: {}. \
                    Please ensure you're running tests from the repository root.",
                    script_path.display()
                );
            }

            // Try to find Python (python3 or python)
            let python_cmd = if Command::new("python3").arg("--version").output().is_ok() {
                "python3"
            } else if Command::new("python").arg("--version").output().is_ok() {
                "python"
            } else {
                panic!(
                    "Python not found. Please install Python 3 to generate test data. \
                    The script requires: polars>=0.20.0 and numpy>=1.24.0"
                );
            };

            // Run the generation script
            let output = Command::new(python_cmd)
                .arg(script_path)
                .output()
                .unwrap_or_else(|e| {
                    panic!(
                        "Failed to run sample data generation script: {}. \
                        Make sure Python is installed and the script is executable.",
                        e
                    );
                });

            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                let stdout = String::from_utf8_lossy(&output.stdout);
                panic!(
                    "Sample data generation failed!\n\
                    Exit code: {:?}\n\
                    stdout:\n{}\n\
                    stderr:\n{}",
                    output.status.code(),
                    stdout,
                    stderr
                );
            }
        }
    });
}

/// Whether the app is still waiting on background work: `busy`, the row count, or
/// the buffer collect, a load-ahead included. What a test driving the app without
/// a terminal waits on: a quiet channel says only that nothing arrived lately,
/// which on a loaded machine is not the same thing. Work the app has abandoned is
/// not waited on; a cancelled analysis can run for minutes.
pub fn work_pending(app: &crate::App) -> bool {
    app.is_busy() || app.row_count_pending() || app.rows_in_flight().is_some()
}

/// Returns a tokio runtime handle for use in tests.
///
/// The cache and config need no setup here: `CacheManager::new` and
/// `ConfigManager::new` point themselves at scratch directories in a unit test
/// (`cache::isolate_cache`).
pub fn test_runtime() -> tokio::runtime::Handle {
    static RT: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("test tokio runtime")
    })
    .handle()
    .clone()
}

/// For `App::worker_dies`: the first job `dies` picks panics as it starts, and
/// every job after it runs.
pub(crate) fn worker_dies_once(dies: fn(&crate::Job) -> bool) -> Option<crate::jobs::WorkerDies> {
    let mut died = false;
    Some(Box::new(move |job| {
        if died || !dies(job) {
            return false;
        }
        died = true;
        true
    }))
}

/// For `Jobs::worker_waits`: the worker of the first job `which` picks waits, before
/// its work starts, until the sender returned sends or is dropped. Every other job
/// runs.
pub(crate) fn worker_waits_once(
    which: fn(&crate::Job) -> bool,
) -> (
    Option<crate::jobs::WorkerWaits>,
    std::sync::mpsc::Sender<()>,
) {
    let (release, gate) = std::sync::mpsc::channel();
    let mut gate = Some(gate);
    let waits: crate::jobs::WorkerWaits =
        Box::new(move |job| if which(job) { gate.take() } else { None });
    (Some(waits), release)
}

/// The footer read, the size probe and a download go through the store Polars
/// scans with, found in its cache by bucket and options, so the same object
/// yields the same store.
#[cfg(feature = "cloud")]
#[test]
fn one_object_store_serves_the_footer_the_probe_and_the_scan() {
    let cloud = crate::config::CloudConfig {
        s3_endpoint_url: Some("http://127.0.0.1:1".to_string()),
        s3_region: Some("us-east-1".to_string()),
        s3_access_key_id: Some("testing".to_string()),
        s3_secret_access_key: Some("testing".to_string()),
        ..Default::default()
    };
    let rt = test_runtime();
    let path = std::path::Path::new("s3://bucket/obj.parquet");
    let (url, _, store) = super::App::cloud_store_for(path, &cloud, &rt).expect("a store");
    assert_eq!(url, "s3://bucket/obj.parquet");
    let (_, _, again) = super::App::cloud_store_for(path, &cloud, &rt).expect("the same store");
    assert!(
        std::sync::Arc::ptr_eq(&store, &again),
        "built once, served twice"
    );
    let (_, _, probe) =
        super::App::cloud_store_for(std::path::Path::new("s3://bucket/"), &cloud, &rt)
            .expect("the bucket's store");
    assert!(
        std::sync::Arc::ptr_eq(&store, &probe),
        "the probe shares it"
    );
}

/// Quitting while a bucket listing was still out used to panic on the listing's
/// thread. A timer stands in for the request's timeout, which is what tripped.
#[cfg(feature = "cloud")]
#[test]
fn a_runtime_shut_down_under_a_waiting_thread_does_not_panic_it() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("tokio runtime");
    let handle = rt.handle().clone();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let waiter = std::thread::spawn(move || {
        super::wait_on_runtime(&handle, async move {
            let _ = started_tx.send(());
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
        })
    });
    started_rx.recv().expect("future started");
    rt.shutdown_background();
    let outcome = waiter.join().expect("the waiting thread must not panic");
    assert!(outcome.is_none());
}

/// Path to the tests/sample-data directory (at repo root). Call `ensure_sample_data()` first if needed.
pub fn sample_data_dir() -> std::path::PathBuf {
    ensure_sample_data();
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("tests/sample-data")
}

/// End pressed while the footers are still coming waits for them, then jumps.
///
/// The pass is already reading every footer and those footers hold the count, so a
/// count started here would read all of them a second time. Worse, the join takes a
/// fresh `len_generation` on its way past, so the answer would come back to a
/// question nothing could match it to and the jump would never happen — the user
/// would be left with "Counting rows to find the end..." and no end.
#[test]
fn end_pressed_while_the_footers_are_coming_jumps_when_they_land() {
    use crate::widgets::datatable::{DataTableState, FootersFound, RemoteFiles};
    use crate::{App, AppEvent, OpenOptions};
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
    use polars::prelude::*;
    use std::sync::Arc;

    let rows = || df!("id" => (0..100i64).collect::<Vec<_>>()).unwrap().lazy();
    let dataset_of = |lf: LazyFrame| {
        let mut lf = lf;
        let schema = Arc::new((*lf.collect_schema().unwrap()).clone());
        let footer = crate::schema_union::FileFooter {
            schema,
            row_group_rows: vec![100],
            file_bytes: 0,
            row_group_bytes: Vec::new(),
            column_bytes: Vec::new(),
        };
        crate::schema_union::union_sampled(1, &[0], &[Some(footer)])
    };

    let mut state = DataTableState::from_schema_and_lazyframe(
        dataset_of(rows()).schema.clone(),
        rows(),
        &OpenOptions::default(),
        None,
    )
    .unwrap()
    .with_open(crate::widgets::datatable::OpenFacts {
        remote_source: true,
        remote_files: Some(RemoteFiles {
            urls: Arc::new(vec!["one".to_string()]),
            scan: Arc::new(move |_u: &[String], _t: &[PlSmallStr]| Ok(rows())),
            count: Arc::new(|_| Ok(vec![vec![100]])),
            offsets: None,
        }),
        footers_pending: Some(Arc::new(move |_| {
            Some(FootersFound {
                estimate: None,
                dataset: dataset_of(rows()),
                lf: rows(),
                file_rows: vec![100],
                files: vec!["one".to_string()],
                row_groups: vec![vec![100]],
                remote: Some(crate::widgets::datatable::RemoteRead {
                    urls: vec!["one".to_string()],
                    scan: Arc::new(move |_u: &[String], _t: &[PlSmallStr]| Ok(rows())),
                    count: Arc::new(|_| Ok(vec![vec![100]])),
                }),
            })
        })),
        ..Default::default()
    });
    state.visible_rows = 10;

    let (tx, rx) = std::sync::mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    app.install_for_tests(state, None, &OpenOptions::default(), None);

    // End, while the footers are still on their way.
    let _ = app.key(&KeyEvent::new(KeyCode::End, KeyModifiers::NONE));
    assert!(
        app.len_count_inflight.is_none(),
        "no second pass over the footers already being read"
    );

    // They land.
    let reported = loop {
        let event = rx
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect("the pass reports back");
        if matches!(event, AppEvent::BackgroundFootersJoined { .. }) {
            break event;
        }
    };
    let _ = app.handle(&reported);
    // The jump the key asked for is queued behind the join, as a jump always is.
    while let Ok(event) = rx.try_recv() {
        let _ = app.handle(&event);
    }

    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.num_rows_if_valid(), Some(100), "counted by the pass");
    assert!(
        state.start_row() > 0,
        "and the end is where the view went, rather than the key being swallowed"
    );
    assert_ne!(
        app.status_message.as_deref(),
        Some("Counting rows to find the end..."),
        "with nothing left saying it is counting rows that have been counted"
    );
    assert!(
        app.end_when_the_footers_land.is_none(),
        "and the key is spent, not left waiting on the next dataset"
    );
}

/// A dataset whose footer pass read a sample of its files shows the row count the
/// sample estimates. Past `[read] exact_count_files` the count waits to be asked: `c`
/// in the Info panel reads every footer, with a progress line Esc stops, and the
/// estimate stands until a count lands exact.
#[test]
fn a_sampled_dataset_shows_an_estimate_until_it_is_counted() {
    use crate::render::footer::Total;
    use crate::render::main_view::MainViewContent;
    use crate::schema_union::RowEstimate;
    use crate::widgets::datatable::{DataTableState, FootersFound, RemoteFiles, RemoteRead};
    use crate::{App, OpenOptions};
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
    use polars::prelude::*;
    use std::sync::{Arc, Mutex, mpsc};

    let rows = || df!("id" => (0..100i64).collect::<Vec<_>>()).unwrap().lazy();
    let dataset_of = |lf: LazyFrame| {
        let mut lf = lf;
        let schema = Arc::new((*lf.collect_schema().unwrap()).clone());
        let footer = crate::schema_union::FileFooter {
            schema,
            row_group_rows: vec![40],
            file_bytes: 0,
            row_group_bytes: Vec::new(),
            column_bytes: Vec::new(),
        };
        crate::schema_union::union_sampled(3, &[0], &[Some(footer)])
    };
    let urls = || vec!["a".to_string(), "b".to_string(), "c".to_string()];
    // A count that says it has read one footer of three, then waits to be let go.
    let (go, gate) = mpsc::channel::<()>();
    let gate = Arc::new(Mutex::new(gate));
    let counter: crate::widgets::datatable::FileCounter = Arc::new(move |progress| {
        let pass = progress.pass(3);
        pass.advance();
        let _ = gate.lock().unwrap().recv();
        if progress.is_cancelled() {
            return Err("cancelled".to_string());
        }
        pass.advance();
        pass.advance();
        Ok(vec![vec![40], vec![30], vec![30]])
    });
    let joined = counter.clone();
    let mut state = DataTableState::from_schema_and_lazyframe(
        dataset_of(rows()).schema.clone(),
        rows(),
        &OpenOptions::default(),
        None,
    )
    .unwrap()
    .with_open(crate::widgets::datatable::OpenFacts {
        remote_source: true,
        remote_files: Some(RemoteFiles {
            urls: Arc::new(urls()),
            scan: Arc::new(move |_u: &[String], _t: &[PlSmallStr]| Ok(rows())),
            count: counter,
            offsets: None,
        }),
        footers_pending: Some(Arc::new(move |_| {
            Some(FootersFound {
                estimate: Some(RowEstimate {
                    rows: 120,
                    sampled: 2,
                    files: 3,
                }),
                dataset: dataset_of(rows()),
                lf: rows(),
                file_rows: Vec::new(),
                files: urls(),
                row_groups: Vec::new(),
                remote: Some(RemoteRead {
                    urls: urls(),
                    scan: Arc::new(move |_u: &[String], _t: &[PlSmallStr]| Ok(rows())),
                    count: joined.clone(),
                }),
            })
        })),
        ..Default::default()
    });
    state.visible_rows = 10;

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    app.app_config.read.exact_count_files = 2;
    app.install_for_tests(state, None, &OpenOptions::default(), None);
    let pump = |app: &mut App, done: &dyn Fn(&App) -> bool| {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
        while !done(app) {
            assert!(std::time::Instant::now() < deadline, "never got there");
            if let Ok(event) = rx.recv_timeout(std::time::Duration::from_millis(50)) {
                let mut next = app.event(&event);
                while let Some(event) = next {
                    next = app.event(&event);
                }
            }
        }
    };
    let total = |app: &App| {
        app.footer(MainViewContent::Datatable, false)
            .position
            .map(|p| p.total)
    };
    pump(&mut app, &|app| {
        app.data_table_state
            .as_ref()
            .is_some_and(|s| s.footers_pending().is_none())
    });
    assert_eq!(total(&app), Some(Total::Estimated(120)));
    assert!(
        app.len_count_inflight.is_none(),
        "too many files to count unasked"
    );

    // `c` in the Info panel counts; the footer says how far, and Esc stops it.
    let key = |app: &mut App, code: KeyCode| {
        let _ = app.key(&KeyEvent::new(code, KeyModifiers::NONE));
    };
    key(&mut app, KeyCode::Char('i'));
    key(&mut app, KeyCode::Char('c'));
    key(&mut app, KeyCode::Esc);
    pump(&mut app, &|app| app.footers_counted().is_some());
    let line = app
        .footer_progress_line(MainViewContent::Datatable)
        .expect("a progress line");
    assert!(line.stoppable);
    assert_eq!(line.counts[0].noun, "files");
    assert_eq!((line.counts[0].done, line.counts[0].total), (1, Some(3)));
    key(&mut app, KeyCode::Esc);
    go.send(()).unwrap();
    pump(&mut app, &|app| app.len_count_inflight.is_none());
    assert_eq!(
        total(&app),
        Some(Total::Estimated(120)),
        "stopped at the estimate"
    );

    // Asked again, it counts to the end.
    key(&mut app, KeyCode::Char('i'));
    key(&mut app, KeyCode::Char('c'));
    key(&mut app, KeyCode::Esc);
    go.send(()).unwrap();
    pump(&mut app, &|app| {
        app.data_table_state
            .as_ref()
            .is_some_and(|s| s.is_num_rows_valid())
    });
    assert_eq!(total(&app), Some(Total::Known(100)));
    assert!(app.row_estimate().is_none());
}

/// `#` over a sort numbers the rows by their place in the source where a row index
/// costs nothing; on a dataset in a store it would keep the filters out of the scan,
/// so `#` counts the view there and the footer says so.
#[test]
fn row_numbers_over_a_sort_fall_back_to_the_view_in_a_store() {
    use crate::widgets::datatable::{DataTableState, OpenFacts};
    use polars::prelude::*;

    let frame = || df!("v" => [3i64, 1, 2]).unwrap().lazy();
    let open = |remote: bool| {
        let mut state = DataTableState::from_lazyframe(frame(), &crate::OpenOptions::default())
            .unwrap()
            .with_open(OpenFacts {
                remote_source: remote,
                ..Default::default()
            });
        state.sort_by(vec!["v".to_string()], vec![false]);
        state
    };

    let mut local = open(false);
    assert!(local.toggle_row_numbers(), "numbered over the sort");
    assert!(local.carries_source_rows());
    assert!(!local.row_numbers_count_the_view());

    let mut remote = open(true);
    assert!(
        !remote.toggle_row_numbers(),
        "no row index under a store's filters"
    );
    assert!(remote.row_numbers());
    assert!(!remote.carries_source_rows());
    assert!(remote.row_numbers_count_the_view());
    assert_eq!(
        remote.row_numbers_from(0, 3),
        [1, 2, 3],
        "the view's places"
    );
}

/// Every key the busy classifier lets through must read nothing.
///
/// `key_acts_while_busy` admits column scroll and help while other work is in
/// flight, and `App::handle` runs them inline on the thread that draws and reads
/// the keyboard. The premise is that none of them touches the source. Column
/// scroll broke it: it called `collect`, which counts the rows when the count has
/// not landed, and on a staged-open cloud hive that count is a metadata read per
/// object — a freeze no keystroke can interrupt.
///
/// The admitted set is *asked for* rather than written down again, so a key added
/// to the classifier is covered by this the moment it is added. Reverting
/// `scroll_right`/`scroll_left` to `self.collect()` fails it, and so does admitting
/// a key that collects — `Char('R')`, say.
#[test]
fn a_key_that_acts_while_busy_reads_nothing() {
    use crate::widgets::datatable::DataTableState;
    use crate::{App, OpenOptions};
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
    use polars::prelude::*;
    use std::sync::Arc;

    let rows = || {
        df!(
            "a" => (0..100i64).collect::<Vec<_>>(),
            "b" => (0..100i64).collect::<Vec<_>>(),
            "c" => (0..100i64).collect::<Vec<_>>(),
            "d" => (0..100i64).collect::<Vec<_>>(),
        )
        .unwrap()
        .lazy()
    };
    let mut lf = rows();
    let schema = Arc::new((*lf.collect_schema().unwrap()).clone());

    // Every key the table view can see, so the filter below is the classifier's
    // answer rather than a copy of its arms.
    let mut candidates: Vec<KeyCode> = (b'a'..=b'z')
        .chain(b'A'..=b'Z')
        .map(|c| KeyCode::Char(c as char))
        .collect();
    candidates.extend((1..=12).map(KeyCode::F));
    candidates.extend("[]{}#,<>=".chars().map(KeyCode::Char));
    candidates.extend([
        KeyCode::Left,
        KeyCode::Right,
        KeyCode::Up,
        KeyCode::Down,
        KeyCode::Home,
        KeyCode::End,
        KeyCode::PageUp,
        KeyCode::PageDown,
        KeyCode::Enter,
        KeyCode::Esc,
        KeyCode::Tab,
        KeyCode::Backspace,
    ]);

    let staged = || {
        let mut state = DataTableState::from_schema_and_lazyframe(
            schema.clone(),
            rows(),
            &OpenOptions::default(),
            None,
        )
        .unwrap();
        state.visible_rows = 10;
        state.visible_termcols = 1;
        // A page on screen, then the count dropped: what a staged open looks like
        // while its footers are still being read. With a buffer in hand the scroll
        // keys do their real work, so this asks whether that work counts.
        assert!(state.count_landed(state.len_generation(), 100, None));
        state.collect();
        state.scroll_right();
        state.invalidate_num_rows();
        state
    };

    // Shift+arrows page, which plans from widths and may land at the draw.
    let keys = candidates
        .into_iter()
        .map(|code| KeyEvent::new(code, KeyModifiers::NONE))
        .chain(
            [KeyCode::Left, KeyCode::Right].map(|code| KeyEvent::new(code, KeyModifiers::SHIFT)),
        );

    let mut admitted = 0;
    for key in keys {
        let code = key.code;
        let (tx, _rx) = std::sync::mpsc::channel();
        let mut app = App::new(tx, crate::tests::test_runtime());
        app.install_for_tests(staged(), None, &OpenOptions::default(), None);
        if !app.key_acts_while_busy(&key) {
            continue;
        }
        admitted += 1;
        assert_eq!(
            app.data_table_state
                .as_ref()
                .and_then(|s| s.num_rows_if_valid()),
            None,
            "{code:?}: the staged open should start without a count"
        );

        // The event the key returns is part of what the key does: `Char('R')`
        // acts by handing back `AppEvent::Reset`, and dropping it here would let
        // an admitted key that collects through the check.
        if let Some(next) = app.key(&key) {
            let _ = app.handle(&next);
        }
        // A page waiting on the widths lands at the next draw, which must read
        // nothing either.
        let area = ratatui::layout::Rect::new(0, 0, 12, 12);
        ratatui::widgets::Widget::render(&mut app, area, &mut ratatui::buffer::Buffer::empty(area));

        assert_eq!(
            app.data_table_state
                .as_ref()
                .and_then(|s| s.num_rows_if_valid()),
            None,
            "{code:?} acts while busy, so it must not count the rows"
        );
    }
    assert!(
        admitted >= 12,
        "the classifier should admit the view keys; it admitted {admitted}"
    );
}

/// End pressed at one dataset does not move the view of the next.
///
/// Abandoning a load cancels nothing, so the prefix the user pressed End on goes on
/// reading its footers after they have left it. Without the dataset's name on it,
/// the key would be spent on whatever is on screen when they land — a directory the
/// user has only just opened jumping to its end on its own.
#[test]
fn end_pressed_at_one_dataset_does_not_move_the_next() {
    use crate::widgets::datatable::{DataTableState, FootersFound, RemoteFiles};
    use crate::{App, OpenOptions};
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
    use polars::prelude::*;
    use std::sync::Arc;

    let rows = || df!("id" => (0..100i64).collect::<Vec<_>>()).unwrap().lazy();
    let dataset_of = |lf: LazyFrame| {
        let mut lf = lf;
        let schema = Arc::new((*lf.collect_schema().unwrap()).clone());
        let footer = crate::schema_union::FileFooter {
            schema,
            row_group_rows: vec![100],
            file_bytes: 0,
            row_group_bytes: Vec::new(),
            column_bytes: Vec::new(),
        };
        crate::schema_union::union_sampled(1, &[0], &[Some(footer)])
    };
    let staged = move || {
        let mut state = DataTableState::from_schema_and_lazyframe(
            dataset_of(rows()).schema.clone(),
            rows(),
            &OpenOptions::default(),
            None,
        )
        .unwrap()
        .with_open(crate::widgets::datatable::OpenFacts {
            remote_source: true,
            remote_files: Some(RemoteFiles {
                urls: Arc::new(vec!["one".to_string()]),
                scan: Arc::new(move |_u: &[String], _t: &[PlSmallStr]| Ok(rows())),
                count: Arc::new(|_| Ok(vec![vec![100]])),
                offsets: None,
            }),
            footers_pending: Some(Arc::new(move |_| {
                Some(FootersFound {
                    estimate: None,
                    dataset: dataset_of(rows()),
                    lf: rows(),
                    file_rows: vec![100],
                    files: vec!["one".to_string()],
                    row_groups: vec![vec![100]],
                    remote: Some(crate::widgets::datatable::RemoteRead {
                        urls: vec!["one".to_string()],
                        scan: Arc::new(move |_u: &[String], _t: &[PlSmallStr]| Ok(rows())),
                        count: Arc::new(|_| Ok(vec![vec![100]])),
                    }),
                })
            })),
            ..Default::default()
        });
        state.visible_rows = 10;
        state
    };

    let (tx, _rx) = std::sync::mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    app.install_for_tests(staged(), None, &OpenOptions::default(), None);
    let _ = app.key(&KeyEvent::new(KeyCode::End, KeyModifiers::NONE));
    assert!(
        app.end_when_the_footers_land.is_some(),
        "the key is waiting on this dataset's footers"
    );

    // The user goes elsewhere before they land.
    app.install_for_tests(staged(), None, &OpenOptions::default(), None);
    assert!(
        app.end_when_the_footers_land.is_none(),
        "and does not take the key with them"
    );
    assert_eq!(
        app.data_table_state.as_ref().unwrap().start_row(),
        0,
        "the directory they opened is where they left it, at the top"
    );
}

/// Once the count is known, nothing is still counting.
///
/// A staged open declines the standalone row count, because the pass reading the
/// footers is bringing it. The marker that says a count is running must not be set
/// for a count that was never started: it is cleared only by a count coming back,
/// so it would stay set for the rest of the session — leaving a spinner where the
/// row count goes, the event loop redrawing for it, and `End` waiting on a count
/// that is not running.
#[test]
fn a_staged_open_does_not_leave_a_count_running_that_never_ran() {
    use crate::widgets::datatable::{DataTableState, FootersFound, RemoteFiles};
    use crate::{App, AppEvent, OpenOptions};
    use polars::prelude::*;
    use std::sync::Arc;

    let narrow = || df!("id" => (0..100i64).collect::<Vec<_>>()).unwrap().lazy();
    let wide = || {
        df!("id" => (0..100i64).collect::<Vec<_>>(), "oops" => vec!["a"; 100])
            .unwrap()
            .lazy()
    };
    let dataset_of = |lf: LazyFrame| {
        let mut lf = lf;
        let schema = Arc::new((*lf.collect_schema().unwrap()).clone());
        let footer = crate::schema_union::FileFooter {
            schema,
            row_group_rows: vec![100],
            file_bytes: 0,
            row_group_bytes: Vec::new(),
            column_bytes: Vec::new(),
        };
        crate::schema_union::union_sampled(1, &[0], &[Some(footer)])
    };

    let mut state = DataTableState::from_schema_and_lazyframe(
        dataset_of(narrow()).schema.clone(),
        narrow(),
        &OpenOptions::default(),
        None,
    )
    .unwrap()
    .with_open(crate::widgets::datatable::OpenFacts {
        remote_source: true,
        remote_files: Some(RemoteFiles {
            urls: Arc::new(vec!["one".to_string()]),
            scan: Arc::new(move |_u: &[String], _t: &[PlSmallStr]| Ok(narrow())),
            count: Arc::new(|_| Ok(vec![vec![100]])),
            offsets: None,
        }),
        footers_pending: Some(Arc::new(move |_| {
            Some(FootersFound {
                estimate: None,
                dataset: dataset_of(wide()),
                lf: wide(),
                file_rows: vec![100],
                files: vec!["one".to_string()],
                row_groups: vec![vec![100]],
                remote: Some(crate::widgets::datatable::RemoteRead {
                    urls: vec!["one".to_string()],
                    scan: Arc::new(move |_u: &[String], _t: &[PlSmallStr]| Ok(wide())),
                    count: Arc::new(|_| Ok(vec![vec![100]])),
                }),
            })
        })),
        ..Default::default()
    });
    state.visible_rows = 10;

    let (tx, rx) = std::sync::mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    app.install_for_tests(state, None, &OpenOptions::default(), None);
    app.spawn_async_collect(App::LOADING_BUFFER);

    // Two answers are in flight here — the pass's and the collect's — and either
    // can reach the queue first. Taking whatever arrives first and calling it the
    // pass's is a race: when the collect wins, the count the pass carries has not
    // been applied yet and the assert below reads `None`. It loses that race about
    // once in a few hundred runs on a loaded machine, which is every so often on
    // CI. So take events until the pass's own has been handled.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let event = rx
            .recv_timeout(deadline.saturating_duration_since(std::time::Instant::now()))
            .expect("the pass reports back");
        let is_the_pass = matches!(event, AppEvent::BackgroundFootersJoined { .. });
        let _ = app.handle(&event);
        if is_the_pass {
            break;
        }
    }

    assert_eq!(
        app.data_table_state.as_ref().unwrap().num_rows_if_valid(),
        Some(100),
        "the pass brought the count with it"
    );
    assert!(
        app.len_count_inflight.is_none(),
        "so nothing is still counting, and the row count is a number rather than a \
         spinner for the rest of the session"
    );
}

/// A read of the rows that fails names the file the user opened, not the temp copy
/// the frame scans: here a compressed CSV's decompressed copy (#511).
#[test]
fn a_failed_read_names_the_file_opened_not_its_temp_copy() {
    use crate::widgets::datatable::DataTableState;
    use crate::{App, OpenOptions};
    use std::io::Write;
    use std::path::PathBuf;

    let source = tempfile::tempdir().unwrap();
    let scratch = tempfile::tempdir().unwrap();
    let gz = source.path().join("rows.csv.gz");
    let mut encoder = flate2::write::GzEncoder::new(
        std::fs::File::create(&gz).unwrap(),
        flate2::Compression::default(),
    );
    encoder.write_all(b"id\n1\n2\n").unwrap();
    encoder.finish().unwrap();
    let options = OpenOptions {
        temp_dir: Some(scratch.path().to_path_buf()),
        ..OpenOptions::default()
    };
    let state = DataTableState::from_csv(&gz, &options).unwrap();
    let copy = state.temp_files()[0].to_path_buf();
    assert!(copy.starts_with(scratch.path()));

    let (tx, _rx) = std::sync::mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    app.install_for_tests(state, Some(PathBuf::from(&gz)), &options, None);
    let reason = format!("bad row\nIt stopped at {}.", copy.display());
    app.rows_failed(true, true, &reason, None);
    assert_eq!(
        app.error_message(),
        Some(format!("bad row\nIt stopped at {}.", gz.display()).as_str())
    );
}

/// A pass that cannot read the footers leaves the dataset working, not waiting.
///
/// While one is pending the dataset declines to count itself, because counting
/// means reading the same footers the pass is already fetching. If a failed pass
/// said nothing, that would be permanent: no exact row count for the rest of the
/// session, no windowed reads, and nothing on screen to say why.
#[test]
fn a_pass_that_cannot_read_the_footers_stops_the_dataset_waiting_for_it() {
    use crate::widgets::datatable::DataTableState;
    use crate::{App, AppEvent, OpenOptions};
    use polars::prelude::*;
    use std::sync::Arc;

    let frame = || df!("id" => &[1i64]).unwrap().lazy();
    let mut lf = frame();
    let schema = Arc::new((*lf.collect_schema().unwrap()).clone());

    let (tx, rx) = std::sync::mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    let state =
        DataTableState::from_schema_and_lazyframe(schema, frame(), &OpenOptions::default(), None)
            .unwrap()
            .with_open(crate::widgets::datatable::OpenFacts {
                // The network was there for the open's two footers and gone for the rest.
                footers_pending: Some(Arc::new(|_progress| None)),
                ..Default::default()
            });
    app.install_for_tests(state, None, &OpenOptions::default(), None);

    let reported = rx
        .recv_timeout(std::time::Duration::from_secs(10))
        .expect("a pass that failed still says so");
    assert!(
        matches!(reported, AppEvent::BackgroundFootersJoined { .. }),
        "and says it the same way a pass that succeeded does"
    );
    let _ = app.handle(&reported);

    assert!(
        app.data_table_state
            .as_ref()
            .unwrap()
            .footers_pending()
            .is_none(),
        "the dataset is not left waiting for footers that are not coming"
    );
}

/// A count not yet taken is not printed as the total.
///
/// The control bar takes its number straight from the field, so the only thing
/// between a user and `Rows: 70` on a prefix of six thousand files is the pending
/// flag. The integration test beside this one has a dataset whose count is real and
/// asserts it is shown; this is the direction that goes wrong.
#[test]
fn a_count_not_yet_taken_is_not_printed_as_the_total() {
    use crate::widgets::datatable::DataTableState;
    use crate::{App, OpenOptions};
    use polars::prelude::*;
    use ratatui::buffer::Buffer;
    use ratatui::layout::Rect;
    use ratatui::widgets::Widget;
    use std::sync::Arc;

    let rows = || df!("id" => (0..70i64).collect::<Vec<_>>()).unwrap().lazy();
    let mut lf = rows();
    let schema = Arc::new((*lf.collect_schema().unwrap()).clone());
    let mut state =
        DataTableState::from_schema_and_lazyframe(schema, rows(), &OpenOptions::default(), None)
            .unwrap()
            .with_open(crate::widgets::datatable::OpenFacts {
                footers_pending: Some(Arc::new(|_| None)),
                ..Default::default()
            });
    // As a staged open leaves it: the number it holds is as far as the buffer
    // reached, a pass is still out, and no count has been taken.
    // A provisional, not `count_landed`, which would mark it as a count that had been
    // taken — the state this reproduces is a provisional left by a short read.
    state.set_provisional_rows(70);
    assert!(
        state.counts_itself_later(),
        "the fixture is a dataset whose count is still coming"
    );

    let (tx, _rx) = std::sync::mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    app.install_for_tests(state, None, &OpenOptions::default(), None);
    app.busy = false;

    let area = Rect::new(0, 0, 100, 24);
    let mut buf = Buffer::empty(area);
    (&mut app).render(area, &mut buf);
    let bar: String = (0..area.width)
        .map(|x| buf[(x, area.height - 1)].symbol().to_string())
        .collect();
    assert!(
        !bar.contains("70 rows"),
        "a partial is not a total: {bar:?}"
    );
    // And the spinner standing in for it turns. Nothing is `busy` and no count is
    // in flight, which is all the run loop used to ask.
    assert!(app.something_is_spinning());
}

/// A pass that brings no count still leaves rows on screen.
///
/// When the pass samples past `MAX_FOOTER_READS`, or a footer fails on the second
/// read, it comes back with no row groups — so there is no count, and an End waiting
/// on it defers to the ordinary one instead of jumping. The join has already dropped
/// the buffer by then, so if the jump is taken to have read the page, nothing reads
/// it: a table with no rows in it until the next keypress.
#[test]
fn a_pass_that_brings_no_count_still_leaves_rows_on_screen() {
    use crate::widgets::datatable::{DataTableState, FootersFound, RemoteFiles};
    use crate::{App, AppEvent, OpenOptions};
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
    use polars::prelude::*;
    use std::sync::Arc;

    let rows = || df!("id" => (0..100i64).collect::<Vec<_>>()).unwrap().lazy();
    let wider = || {
        df!("id" => (0..100i64).collect::<Vec<_>>(), "oops" => vec!["a"; 100])
            .unwrap()
            .lazy()
    };
    let dataset_of = |lf: LazyFrame| {
        let mut lf = lf;
        let schema = Arc::new((*lf.collect_schema().unwrap()).clone());
        let footer = crate::schema_union::FileFooter {
            schema,
            row_group_rows: vec![100],
            file_bytes: 0,
            row_group_bytes: Vec::new(),
            column_bytes: Vec::new(),
        };
        crate::schema_union::union_sampled(1, &[0], &[Some(footer)])
    };

    let mut state = DataTableState::from_schema_and_lazyframe(
        dataset_of(rows()).schema.clone(),
        rows(),
        &OpenOptions::default(),
        None,
    )
    .unwrap()
    .with_open(crate::widgets::datatable::OpenFacts {
        remote_source: true,
        remote_files: Some(RemoteFiles {
            urls: Arc::new(vec!["one".to_string()]),
            scan: Arc::new(move |_u: &[String], _t: &[PlSmallStr]| Ok(rows())),
            count: Arc::new(|_| Ok(vec![vec![100]])),
            offsets: None,
        }),
        // No row groups: a dataset sampled past the footer limit, or one whose
        // footer would not parse the second time.
        footers_pending: Some(Arc::new(move |_| {
            Some(FootersFound {
                estimate: None,
                dataset: dataset_of(wider()),
                lf: wider(),
                file_rows: Vec::new(),
                files: Vec::new(),
                row_groups: Vec::new(),
                remote: None,
            })
        })),
        ..Default::default()
    });
    state.visible_rows = 10;

    let (tx, rx) = std::sync::mpsc::channel();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let mut app = App::new(tx, runtime.handle().clone());
    app.install_for_tests(state, None, &OpenOptions::default(), None);
    let _ = app.key(&KeyEvent::new(KeyCode::End, KeyModifiers::NONE));

    let reported = loop {
        let event = rx
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect("the pass reports back");
        if matches!(event, AppEvent::BackgroundFootersJoined { .. }) {
            break event;
        }
    };
    let _ = app.handle(&reported);

    assert!(
        app.rows_in_flight().is_some(),
        "the join dropped the buffer, so something has to read it back"
    );
}

/// End on a sorted staged dataset waits for the pass, as it does on a plain one.
///
/// A sort is rebuilt over the joined scan, so the join lands underneath it and takes
/// a fresh `len_generation` on its way past. A count started before that comes back
/// answering a question nothing can match it to: the jump never happens, the status
/// line goes on saying it is counting, and `end_after_count` is stranded at a dead
/// generation — where a later failure for any other count prints "Could not count
/// the rows to find the end" about something the user never asked for.
#[test]
fn end_on_a_sorted_dataset_still_reading_its_footers_waits_for_the_pass() {
    use crate::widgets::datatable::{DataTableState, RemoteFiles};
    use crate::{App, OpenOptions};
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
    use polars::prelude::*;
    use std::sync::Arc;

    let rows = || df!("id" => (0..100i64).collect::<Vec<_>>()).unwrap().lazy();
    let mut lf = rows();
    let schema = Arc::new((*lf.collect_schema().unwrap()).clone());
    let mut state =
        DataTableState::from_schema_and_lazyframe(schema, rows(), &OpenOptions::default(), None)
            .unwrap()
            .with_open(crate::widgets::datatable::OpenFacts {
                remote_source: true,
                remote_files: Some(RemoteFiles {
                    urls: Arc::new(vec!["one".to_string()]),
                    scan: Arc::new(move |_u: &[String], _t: &[PlSmallStr]| Ok(rows())),
                    count: Arc::new(|_| Ok(vec![vec![100]])),
                    offsets: None,
                }),
                footers_pending: Some(Arc::new(|_| None)),
                ..Default::default()
            });
    state.visible_rows = 10;

    let (tx, _rx) = std::sync::mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    app.install_for_tests(state, None, &OpenOptions::default(), None);

    // Sorted — which is one of the things the staging exists to let you do while
    // the footers read — and then End.
    let state = app.data_table_state.as_mut().unwrap();
    state.deferred(|s| s.sort(vec!["id".to_string()], false));
    assert!(
        state.scan_is_the_root(),
        "a sort is rebuilt over whatever the root becomes, so the join lands under it"
    );

    let _ = app.key(&KeyEvent::new(KeyCode::End, KeyModifiers::NONE));
    assert_eq!(
        app.end_when_the_footers_land,
        Some(app.dataset_generation),
        "so End waits for the pass rather than starting a count the join will orphan"
    );
    assert!(
        app.end_after_count.is_none(),
        "and nothing is left waiting on a count that will never be matched"
    );
}

/// A count the join orphaned does not strand End, nor speak for a later count.
///
/// End on a query over a staged dataset takes the ordinary count — the pass is
/// bringing the *dataset's* count, which is not the query's. But the pass's columns
/// are held while the query is up and go in the moment the user leaves it, and that
/// join takes a fresh `len_generation` past the count already running. What comes
/// back then answers a frame that is gone.
///
/// Both halves of that were wrong. The flag was cleared only on the matching branch,
/// so it sat on a dead generation for the rest of the session — the view never moved
/// and nothing was said. And `BackgroundLenFailed` took the flag without checking
/// whose count had failed, so the next count to fail for any reason printed "Could
/// not count the rows to find the end" about a key pressed on a different frame.
#[test]
fn a_count_the_join_orphaned_does_not_strand_end_or_speak_for_a_later_one() {
    use crate::widgets::datatable::{DataTableState, FootersFound, RemoteFiles};
    use crate::{App, AppEvent, OpenOptions};
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
    use polars::prelude::*;
    use std::sync::Arc;

    let rows = || df!("id" => (0..100i64).collect::<Vec<_>>()).unwrap().lazy();
    let wide = || {
        df!("id" => (0..100i64).collect::<Vec<_>>(), "extra" => vec!["a"; 100])
            .unwrap()
            .lazy()
    };
    let dataset_of = |lf: LazyFrame| {
        let mut lf = lf;
        let schema = Arc::new((*lf.collect_schema().unwrap()).clone());
        let footer = crate::schema_union::FileFooter {
            schema,
            row_group_rows: vec![100],
            file_bytes: 0,
            row_group_bytes: Vec::new(),
            column_bytes: Vec::new(),
        };
        crate::schema_union::union_sampled(1, &[0], &[Some(footer)])
    };
    let mut lf = rows();
    let schema = Arc::new((*lf.collect_schema().unwrap()).clone());
    let mut state =
        DataTableState::from_schema_and_lazyframe(schema, rows(), &OpenOptions::default(), None)
            .unwrap()
            .with_open(crate::widgets::datatable::OpenFacts {
                remote_source: true,
                remote_files: Some(RemoteFiles {
                    urls: Arc::new(vec!["one".to_string()]),
                    scan: Arc::new(move |_u: &[String], _t: &[PlSmallStr]| Ok(rows())),
                    count: Arc::new(|_| Ok(vec![vec![100]])),
                    offsets: None,
                }),
                footers_pending: Some(Arc::new(move |_| {
                    Some(FootersFound {
                        estimate: None,
                        dataset: dataset_of(wide()),
                        lf: wide(),
                        file_rows: vec![100],
                        files: vec!["one".to_string()],
                        row_groups: vec![vec![100]],
                        remote: Some(crate::widgets::datatable::RemoteRead {
                            urls: vec!["one".to_string()],
                            scan: Arc::new(move |_u: &[String], _t: &[PlSmallStr]| Ok(wide())),
                            count: Arc::new(|_| Ok(vec![vec![100]])),
                        }),
                    })
                })),
                ..Default::default()
            });
    state.visible_rows = 10;

    let (tx, _rx) = std::sync::mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    app.install_for_tests(state, None, &OpenOptions::default(), None);

    // A question of the dataset, whose answer has a count of its own.
    let state = app.data_table_state.as_mut().unwrap();
    state.deferred(|s| s.query("select doubled: id * 2".to_string()));
    let orphaned = app.data_table_state.as_ref().unwrap().len_generation();

    // End, which takes that count rather than waiting for the pass.
    let _ = app.key(&KeyEvent::new(KeyCode::End, KeyModifiers::NONE));
    assert_eq!(
        app.end_after_count,
        Some(orphaned),
        "the jump is waiting on the query's own count"
    );

    // The pass lands while the query is up, so its columns are held.
    let live = app.dataset_generation;
    let found = app
        .data_table_state
        .as_ref()
        .and_then(|state| state.footers_pending())
        .and_then(|pass| pass(&app.footer_progress));
    App::record_footers(&app.pending_footers_result, live, found);
    let _ = app.handle(&AppEvent::BackgroundFootersJoined { generation: live });
    assert!(
        app.footers_held.is_some(),
        "held rather than joined, because a query is the root"
    );

    // The user leaves the query, and the join goes in underneath the count.
    let state = app.data_table_state.as_mut().unwrap();
    state.deferred(|s| s.query(String::new()));
    let _ = app.handle(&AppEvent::Update);
    let joined = app.data_table_state.as_ref().unwrap().len_generation();
    assert_ne!(
        joined, orphaned,
        "the join took a fresh generation past the count that was already running"
    );

    // And the count comes back, answering a frame that is gone.
    let before = app.data_table_state.as_ref().unwrap().start_row();
    let next = app.event(&AppEvent::BackgroundLenReady {
        len_generation: orphaned,
        num_rows: 100,
        file_row_groups: None,
    });
    assert_eq!(
        app.end_after_count, None,
        "the jump is not left waiting on a generation nothing will ever match"
    );
    assert_ne!(
        app.status_message.as_deref(),
        Some(App::COUNTING_FOR_END),
        "and the line does not go on saying it is counting for an end nobody awaits"
    );

    // Retired, not re-issued: nothing jumps on the strength of the stale answer.
    let mut follow = next;
    while let Some(event) = follow {
        follow = app.event(&event);
    }
    assert_eq!(
        app.data_table_state.as_ref().unwrap().start_row(),
        before,
        "and the view stays where it is rather than moving on a stale answer"
    );
}

/// A count that failed for a frame that is gone does not answer for the End on this
/// one.
///
/// Counts for two frames can be in flight at once — a join or a query takes a fresh
/// `len_generation` without stopping the count already running — so a failure
/// arriving is not necessarily the failure of the count End is waiting on.
/// `BackgroundLenFailed` took the flag without looking at whose count had failed:
/// the older one failing dropped the live End on the floor and printed "Could not
/// count the rows to find the end" about it, while the count that End was actually
/// waiting on was still running and about to succeed.
#[test]
fn a_count_that_failed_for_another_frame_does_not_answer_for_this_end() {
    use crate::widgets::datatable::{DataTableState, RemoteFiles};
    use crate::{App, AppEvent, OpenOptions};
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
    use polars::prelude::*;
    use std::sync::Arc;

    let rows = || df!("id" => (0..100i64).collect::<Vec<_>>()).unwrap().lazy();
    let mut lf = rows();
    let schema = Arc::new((*lf.collect_schema().unwrap()).clone());
    let mut state =
        DataTableState::from_schema_and_lazyframe(schema, rows(), &OpenOptions::default(), None)
            .unwrap()
            .with_open(crate::widgets::datatable::OpenFacts {
                remote_source: true,
                remote_files: Some(RemoteFiles {
                    urls: Arc::new(vec!["one".to_string()]),
                    scan: Arc::new(move |_u: &[String], _t: &[PlSmallStr]| Ok(rows())),
                    count: Arc::new(|_| Ok(vec![vec![100]])),
                    offsets: None,
                }),
                ..Default::default()
            });
    state.visible_rows = 10;

    let (tx, _rx) = std::sync::mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    app.install_for_tests(state, None, &OpenOptions::default(), None);

    // End on the frame that is here, whose count is running.
    let live = app.data_table_state.as_ref().unwrap().len_generation();
    let _ = app.key(&KeyEvent::new(KeyCode::End, KeyModifiers::NONE));
    assert_eq!(
        app.end_after_count,
        Some(live),
        "the jump is waiting on this frame's count"
    );
    app.status_message = None;

    // And a count for some frame that is long gone fails.
    let _ = app.handle(&AppEvent::BackgroundLenFailed {
        len_generation: live.wrapping_sub(1),
    });

    assert_eq!(
        app.end_after_count,
        Some(live),
        "the End is still waiting on its own count, which has not failed"
    );
    assert_eq!(
        app.status_message, None,
        "and nothing is said about a count the user is not waiting on"
    );
}

/// An App on a remote dataset of a hundred rows that has not been counted yet, its
/// buffer forty rows in.
///
/// The receiver comes back with it so a test can read what the App sent.
fn uncounted_remote_app() -> (crate::App, std::sync::mpsc::Receiver<crate::AppEvent>) {
    use crate::widgets::datatable::{DataTableState, RemoteFiles};
    use crate::{App, OpenOptions};
    use polars::prelude::*;
    use std::sync::Arc;

    let frame = || df!("id" => (0..100i64).collect::<Vec<_>>()).unwrap().lazy();
    let mut lf = frame();
    let schema = Arc::new((*lf.collect_schema().unwrap()).clone());
    let mut state =
        DataTableState::from_schema_and_lazyframe(schema, frame(), &OpenOptions::default(), None)
            .unwrap()
            .with_open(crate::widgets::datatable::OpenFacts {
                remote_source: true,
                remote_files: Some(RemoteFiles {
                    urls: Arc::new(vec!["one".to_string()]),
                    scan: Arc::new(move |_u: &[String], _t: &[PlSmallStr]| Ok(frame())),
                    count: Arc::new(|_| Ok(vec![vec![100]])),
                    offsets: None,
                }),
                ..Default::default()
            });
    state.visible_rows = 10;
    // As far as the buffer reached, of a hundred. This is the number the bar prints
    // when nothing tells it the count failed, and printing it is the harm: a
    // confident partial where a "?" belongs.
    state.set_provisional_rows(40);

    let (tx, rx) = std::sync::mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    app.install_for_tests(state, None, &OpenOptions::default(), None);
    (app, rx)
}

/// The bottom line of a rendered App — the control bar, as a string.
fn control_bar(app: &mut crate::App) -> String {
    use ratatui::buffer::Buffer;
    use ratatui::layout::Rect;
    use ratatui::widgets::Widget;

    let area = Rect::new(0, 0, 120, 24);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    (0..area.width)
        .map(|x| buf[(x, area.height - 1)].symbol().to_string())
        .collect()
}

/// The bar says `?` when this frame's count failed, rather than the partial the
/// buffer happened to reach.
///
/// The widget's own `?` has a test; what had none is the App deciding to ask for it.
/// Two bugs were found in and around `len_count_failed` and the suite noticed
/// neither, because nothing rendered the bar: deleting the write that produces `?`
/// left the whole workspace green.
#[test]
fn the_bar_says_question_mark_when_the_count_failed() {
    use crate::AppEvent;

    let (mut app, _rx) = uncounted_remote_app();
    let live = app.data_table_state.as_ref().unwrap().len_generation();

    assert!(
        !control_bar(&mut app).contains("/ ?"),
        "nothing has failed yet"
    );

    let _ = app.handle(&AppEvent::BackgroundLenFailed {
        len_generation: live,
    });

    let bar = control_bar(&mut app);
    assert!(
        bar.contains("/ ?"),
        "the count failed, so the total is unknown: {bar:?}"
    );
}

/// A count that failed for a frame that is gone does not take the `?` off the frame
/// that is here.
///
/// `len_count_failed` is one slot and the bar reads it against the frame on screen.
/// Written for whichever count failed last, an orphan — a join, a query, a filter or
/// a sort takes a fresh `len_generation` without stopping the count already running
/// — overwrote the live frame's own failure. `count_unknown` then went false and the
/// bar printed the number the buffer had reached, plainly, on a dataset whose count
/// failed.
#[test]
fn a_dead_frames_failed_count_leaves_this_frames_question_mark_alone() {
    use crate::AppEvent;

    let (mut app, _rx) = uncounted_remote_app();
    let live = app.data_table_state.as_ref().unwrap().len_generation();

    let _ = app.handle(&AppEvent::BackgroundLenFailed {
        len_generation: live,
    });
    assert!(
        control_bar(&mut app).contains("/ ?"),
        "this frame's count failed"
    );

    // And now a count orphaned by an earlier frame change fails too.
    let _ = app.handle(&AppEvent::BackgroundLenFailed {
        len_generation: live.wrapping_sub(1),
    });

    let bar = control_bar(&mut app);
    assert!(
        bar.contains("/ ?"),
        "a stranger's failure says nothing about this frame: {bar:?}"
    );
}

/// A look, set up as `ClassifyThenOpen` leaves it.
#[cfg(test)]
fn a_look_is_out(app: &mut crate::App, path: &std::path::Path, jump: bool) -> crate::jobs::Started {
    let look = crate::Job::Classify(crate::jobs::Classify {
        path: path.to_path_buf(),
        browsing: app.home.browsing.clone(),
        jump,
    });
    app.job_for_tests(look, Some(crate::App::LOOKING))
}

/// Whether a look is out and still wanted.
#[cfg(test)]
fn a_look_waits(app: &crate::App) -> Option<std::path::PathBuf> {
    match app
        .jobs
        .current(|job| matches!(job, crate::Job::Classify(_)))
    {
        Some((_, crate::Job::Classify(look))) => Some(look.path.clone()),
        _ => None,
    }
}

/// The look answers what its path turned out to be.
#[cfg(test)]
fn the_look_answers(
    app: &mut crate::App,
    look: crate::jobs::Started,
    found: Option<crate::discover::EntryKind>,
) -> Option<crate::AppEvent> {
    let ticket = look.ticket();
    look.end(crate::Outcome::answered(crate::Answer::Kind(found)));
    app.event(&crate::AppEvent::JobEnded(ticket))
}

/// An answer nobody is waiting for is dropped — and the busy state it was holding
/// goes with it, except where something else has taken that over.
///
/// The look holds the keys and this answer is the only thing that comes back, so a
/// drop that does not give them back holds every key for the rest of the session.
/// The exception is the one the other handlers rely on: an advanced generation means
/// an `Open` or a collect took the wait over, and clearing it here takes the throbber
/// off a load that is still running.
#[test]
fn a_classify_answer_nobody_is_waiting_for_leaves_the_right_busy_behind() {
    use crate::{App, InputMode};

    type MovedOn = fn(&mut App);
    let cases: Vec<(&str, MovedOn, bool)> = vec![
        (
            "they went back to the data",
            |app: &mut App| app.input_mode = InputMode::Normal,
            false,
        ),
        (
            "the browse moved under it",
            |app: &mut App| app.home.browsing = Some(std::path::PathBuf::from("/elsewhere")),
            false,
        ),
        (
            "an open took the generation",
            |app: &mut App| {
                app.jobs.advance();
                app.busy = true;
            },
            true,
        ),
    ];

    for (what, moved_on, busy_after) in cases {
        let (tx, _rx) = std::sync::mpsc::channel();
        let mut app = App::new(tx, crate::tests::test_runtime());
        app.enter_home();
        let path = std::path::PathBuf::from("/mnt/share/orders");
        let look = a_look_is_out(&mut app, &path, false);

        moved_on(&mut app);
        let moved_to = app.home.browsing.clone();

        let follow = the_look_answers(&mut app, look, Some(crate::discover::EntryKind::MultiFile));

        assert!(follow.is_none(), "nothing was opened when {what}");
        assert_eq!(
            app.home.browsing, moved_to,
            "and it did not browse into the answer's path when {what}"
        );
        assert_eq!(
            app.is_busy(),
            busy_after,
            "busy after {what}: an answer puts down the busy it was holding, and \
             only that one"
        );
        assert!(
            a_look_waits(&app).is_none(),
            "and the look is no longer outstanding when {what}"
        );
    }
}

/// A newer look replaces an older one, and the older answer touches nothing.
///
/// Every key acts on the home screen even while `busy`, so a second Enter is
/// reachable. Refusing it meant a look at a share that never answers killed the
/// feature for the rest of the session, silently — and the older answer must not put
/// down the busy state the newer one is holding.
#[test]
fn a_newer_look_replaces_an_older_one() {
    use crate::{App, AppEvent};

    let (tx, _rx) = std::sync::mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    app.enter_home();
    let first = std::path::PathBuf::from("/mnt/share/aaa");
    let look = a_look_is_out(&mut app, &first, false);

    // A second Enter, at a row the user moved to while the first was out.
    let second = std::path::PathBuf::from("/mnt/share/bbb");
    let _ = app.event(&AppEvent::ClassifyThenOpen {
        path: second.clone(),
        jump: false,
    });
    assert_eq!(
        a_look_waits(&app),
        Some(second.clone()),
        "the newer look is the one being waited on"
    );

    // And the older answer arrives.
    let follow = the_look_answers(&mut app, look, Some(crate::discover::EntryKind::MultiFile));

    assert!(follow.is_none(), "the stale answer opened nothing");
    assert_eq!(
        a_look_waits(&app),
        Some(second),
        "and did not cancel the look that replaced it"
    );
    assert!(app.is_busy(), "nor put down its busy state");
}

/// Going home puts down a look that may never answer.
///
/// The case the whole path exists for is a share that has gone away, where the worker
/// sits forever. Without this the keyboard waits with it: `abandon_load` clears
/// `busy` only for a load, and a look is not one.
#[test]
fn going_home_does_not_wait_on_a_look_that_may_never_answer() {
    use crate::App;

    let (tx, _rx) = std::sync::mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    app.enter_home();
    let _look = a_look_is_out(&mut app, std::path::Path::new("/mnt/gone/orders"), false);
    app.home.status = Some("Looking at orders...".to_string());

    app.enter_home();

    assert!(
        !app.is_busy(),
        "the keyboard is not waiting on a dead share"
    );
    assert!(a_look_waits(&app).is_none(), "and the look is put down");
    assert_eq!(app.home.status, None, "with its line");
}

/// A typed path the worker could not find comes back to the prompt with the text in
/// it, the way a local one never left.
#[test]
fn a_typed_path_that_is_not_there_comes_back_to_the_prompt() {
    use crate::App;

    let (tx, _rx) = std::sync::mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    app.enter_home();
    let path = std::path::PathBuf::from("/mnt/share/nope");
    let look = a_look_is_out(&mut app, &path, true);

    let follow = the_look_answers(&mut app, look, None);

    assert!(follow.is_none());
    assert!(
        app.home
            .status
            .as_deref()
            .is_some_and(|s| s.contains("No such path")),
        "it says so: {:?}",
        app.home.status
    );
    assert!(app.home.path_input_active, "and the prompt is back");
    assert_eq!(
        app.home.path_input,
        path.display().to_string(),
        "with the path still in it"
    );
}

/// A parked End's message does not follow the user off the dataset.
///
/// It parks without setting `busy`, so `abandon_load`'s cleanup — which is a load's
/// — did not reach it, and Ctrl+O left it set. Painted on the home screen it replaces
/// every key chip on the bar with a sentence about a dataset the user has left, and
/// the failure that follows writes an error there that nothing ever clears.
#[test]
fn a_parked_end_does_not_put_its_message_on_the_home_screen() {
    use crate::AppEvent;
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

    let (mut app, _rx) = uncounted_remote_app();
    let waiting = app.data_table_state.as_ref().unwrap().len_generation();
    let _ = app.key(&KeyEvent::new(KeyCode::End, KeyModifiers::NONE));
    assert!(control_bar(&mut app).contains("Counting rows"), "parked");

    app.enter_home();

    let bar = control_bar(&mut app);
    assert!(
        !bar.contains("Counting rows"),
        "the home bar is the home screen's: {bar:?}"
    );
    assert!(
        bar.contains("keys"),
        "and it still has its keys rather than a sentence: {bar:?}"
    );

    // And the count it was waiting on then fails, with the user somewhere else.
    // (Both halves of the fix are exercised: `abandon_load` clears the message on
    // the way out, and the gate below keeps it off a view that is not the table.)
    let _ = app.handle(&AppEvent::BackgroundLenFailed {
        len_generation: waiting,
    });
    let bar = control_bar(&mut app);
    assert!(
        !bar.contains("Could not count the rows"),
        "an error about a dataset they have left is not the home screen's news: \
         {bar:?}"
    );
}

/// And it does not reappear on the next dataset either.
///
/// The message is deliberately *not* cleared on the way out: the End is still parked,
/// and coming back to the same table with Esc should still say so. What must not
/// happen is it greeting a different dataset — which it does not, because opening one
/// puts up its own line. This pins that, since nothing else would notice if the order
/// of those two ever changed.
#[test]
fn a_parked_end_does_not_put_its_message_on_the_next_dataset() {
    use crate::{OpenOptions, widgets::datatable::DataTableState};
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
    use polars::prelude::*;
    use std::sync::Arc;

    let (mut app, _rx) = uncounted_remote_app();
    let _ = app.key(&KeyEvent::new(KeyCode::End, KeyModifiers::NONE));
    assert!(control_bar(&mut app).contains("Counting rows"), "parked");

    app.enter_home();

    // And they open something else, which installs its own frame.
    let rows = || df!("id" => &[1i64, 2, 3]).unwrap().lazy();
    let mut lf = rows();
    let schema = Arc::new((*lf.collect_schema().unwrap()).clone());
    let next =
        DataTableState::from_schema_and_lazyframe(schema, rows(), &OpenOptions::default(), None)
            .unwrap();
    app.install_for_tests(next, None, &OpenOptions::default(), None);
    app.busy = false;

    let bar = control_bar(&mut app);
    assert!(
        !bar.contains("Counting rows"),
        "the new dataset's bar is not the old one's: {bar:?}"
    );
}

/// The chart view's bar is the chart's, not a parked End's.
///
/// `abandon_load` does not run here — the user has not left the dataset — so this is
/// the gate on its own: the message is still set, and the view it belongs to is not
/// the one on screen. Once the chart is ready `chart_preparing()` goes false, and
/// before the gate the bar read "Counting rows to find the end..." where the chart keys
/// belong.
#[test]
fn a_parked_end_does_not_put_its_message_on_the_chart_view() {
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

    let (mut app, _rx) = uncounted_remote_app();
    let _ = app.key(&KeyEvent::new(KeyCode::End, KeyModifiers::NONE));
    assert!(control_bar(&mut app).contains("Counting rows"), "parked");

    app.input_mode = crate::InputMode::Chart;
    app.chart_modal.active = true;

    let bar = control_bar(&mut app);
    assert!(
        app.status_message.is_some(),
        "the End is still waiting, and the field still says so"
    );
    assert!(
        !bar.contains("Counting rows"),
        "but the chart's bar is the chart's: {bar:?}"
    );
}

/// An End waiting on a count whose frame is gone, whose count then fails, is retired
/// without saying anything.
///
/// The frame it was counting has been replaced, so its failure says nothing about the
/// one on screen and cannot answer the End that was waiting on it. Reached by nothing
/// in the suite until now: deleting the branch left every test green.
#[test]
fn a_failed_count_for_a_frame_that_is_gone_retires_its_end_quietly() {
    use crate::AppEvent;
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

    let (mut app, _rx) = uncounted_remote_app();
    let waiting = app.data_table_state.as_ref().unwrap().len_generation();
    let _ = app.key(&KeyEvent::new(KeyCode::End, KeyModifiers::NONE));
    assert_eq!(app.end_after_count, Some(waiting));
    assert_eq!(
        app.status_message.as_deref(),
        Some(crate::App::COUNTING_FOR_END),
        "the status says the count is running"
    );

    // A question of the dataset takes a fresh generation out from under the count.
    let state = app.data_table_state.as_mut().unwrap();
    state.deferred(|s| s.query("select doubled: id * 2".to_string()));
    assert_ne!(
        app.data_table_state.as_ref().unwrap().len_generation(),
        waiting,
        "the frame the count belongs to is gone"
    );

    let _ = app.handle(&AppEvent::BackgroundLenFailed {
        len_generation: waiting,
    });

    assert_eq!(
        app.end_after_count, None,
        "the End it belonged to is retired"
    );
    let bar = control_bar(&mut app);
    assert!(
        !bar.contains("Counting rows"),
        "the status it put up comes down: {bar:?}"
    );
    assert!(
        !bar.contains("Could not count the rows"),
        "and does not become an error about a frame the user is no longer looking \
         at: {bar:?}"
    );
}

/// The bar says a count is running for an End, and says when it failed.
///
/// Both messages were written and painted by nothing: the status line was shown only
/// while `busy`, and an End waiting on a remote count parks without setting it —
/// deliberately, so keys keep working. Three code paths existed to take a message
/// down that could never appear.
#[test]
fn the_bar_says_it_is_counting_for_an_end_and_says_when_that_failed() {
    use crate::AppEvent;
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

    let (mut app, _rx) = uncounted_remote_app();
    let waiting = app.data_table_state.as_ref().unwrap().len_generation();

    let _ = app.key(&KeyEvent::new(KeyCode::End, KeyModifiers::NONE));
    assert!(
        !app.is_busy(),
        "the jump parked rather than blocking the keyboard"
    );
    let bar = control_bar(&mut app);
    assert!(
        bar.contains("Counting rows"),
        "and the line says why the view has not moved: {bar:?}"
    );

    let _ = app.handle(&AppEvent::BackgroundLenFailed {
        len_generation: waiting,
    });
    let bar = control_bar(&mut app);
    assert!(
        bar.contains("Could not count the rows"),
        "and says so when the count it was waiting on fails: {bar:?}"
    );
}

/// An End pressed on the directory the user walked away from does not move the one
/// they opened next.
///
/// `end_after_count` names a `len_generation`, which says nothing about which
/// dataset it belonged to — so it has to be put down when a dataset is, the way
/// `end_when_the_footers_land` already is. Without that, a stale count returning
/// after the user has opened something else hands the new dataset the old one's
/// key: it starts a count it was never asked for and scrolls itself to the bottom
/// when that count lands.
#[test]
fn an_end_pressed_on_the_dataset_they_left_does_not_move_the_next_one() {
    use crate::widgets::datatable::{DataTableState, RemoteFiles};
    use crate::{App, AppEvent, OpenOptions};
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
    use polars::prelude::*;
    use std::sync::Arc;

    let remote_state = |n: i64| {
        let rows = move || df!("id" => (0..n).collect::<Vec<_>>()).unwrap().lazy();
        let mut lf = rows();
        let schema = Arc::new((*lf.collect_schema().unwrap()).clone());
        let mut state = DataTableState::from_schema_and_lazyframe(
            schema,
            rows(),
            &OpenOptions::default(),
            None,
        )
        .unwrap()
        .with_open(crate::widgets::datatable::OpenFacts {
            remote_source: true,
            remote_files: Some(RemoteFiles {
                urls: Arc::new(vec!["one".to_string()]),
                scan: Arc::new(move |_u: &[String], _t: &[PlSmallStr]| Ok(rows())),
                count: Arc::new(move |_| Ok(vec![vec![n as usize]])),
                offsets: None,
            }),
            ..Default::default()
        });
        state.visible_rows = 10;
        state
    };

    let (tx, _rx) = std::sync::mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    app.install_for_tests(remote_state(100), None, &OpenOptions::default(), None);
    let theirs = app.data_table_state.as_ref().unwrap().len_generation();

    // End on the first directory, before its count lands.
    let _ = app.key(&KeyEvent::new(KeyCode::End, KeyModifiers::NONE));
    assert_eq!(
        app.end_after_count,
        Some(theirs),
        "the jump is waiting on that directory's count"
    );

    // And they open another one instead.
    app.install_for_tests(remote_state(500), None, &OpenOptions::default(), None);
    assert_eq!(
        app.end_after_count, None,
        "the key they pressed in the directory they left does not come with them"
    );

    // The first directory's count finally arrives.
    let mut follow = app.event(&AppEvent::BackgroundLenReady {
        len_generation: theirs,
        num_rows: 100,
        file_row_groups: None,
    });
    while let Some(event) = follow {
        follow = app.event(&event);
    }
    let next = app.data_table_state.as_ref().unwrap().len_generation();
    assert_ne!(
        app.end_after_count,
        Some(next),
        "and the directory on screen has not inherited it"
    );

    // Even once its own count lands, as it would.
    let mut follow = app.event(&AppEvent::BackgroundLenReady {
        len_generation: next,
        num_rows: 500,
        file_row_groups: None,
    });
    while let Some(event) = follow {
        follow = app.event(&event);
    }
    assert_eq!(
        app.data_table_state.as_ref().unwrap().start_row(),
        0,
        "the directory they are looking at stays where they left it, at the top"
    );
}

/// The next event on `rx`, waiting for it.
fn recv(rx: &std::sync::mpsc::Receiver<crate::AppEvent>) -> crate::AppEvent {
    rx.recv_timeout(std::time::Duration::from_secs(30))
        .expect("the job reports back")
}

/// Every job holds the generation while it runs, and lets go in the step that
/// hands its answer over: not before, so nothing can bump the generation between
/// the answer and what it starts, and not after, so a frame drawn once the answer
/// is handled finds the generation free (#221, #490).
#[test]
fn a_job_holds_the_generation_until_its_answer_is_handled() {
    use crate::{App, AppEvent, Job, JobKind};

    let (tx, rx) = std::sync::mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    assert!(!app.work_a_bump_would_strand(), "nothing is running yet");

    let (go, wait) = std::sync::mpsc::channel::<()>();
    let ticket = app.spawn_job(Job::QualityReport, Some("Working..."), move |_| {
        wait.recv().ok();
        Ok(crate::Answer::QualityReportWritten(
            std::path::PathBuf::from("report.json"),
        ))
    });
    assert_eq!(ticket.kind(), JobKind::QualityReport);
    assert!(
        app.work_a_bump_would_strand(),
        "the job holds the generation"
    );
    assert!(app.is_busy());

    go.send(()).unwrap();
    let ended = recv(&rx);
    assert!(matches!(ended, AppEvent::JobEnded(t) if t == ticket));
    assert!(
        app.work_a_bump_would_strand(),
        "an answer not yet handled still holds it"
    );
    let _ = app.handle(&ended);
    assert!(
        !app.work_a_bump_would_strand(),
        "and handling it lets go, with nothing left to arrive"
    );
    assert!(!app.is_busy());
    assert!(
        rx.recv_timeout(std::time::Duration::from_millis(50))
            .is_err(),
        "no second event releases anything"
    );
}

/// A job the user waits on holds the keys and its line on the bar; its end gives
/// both back in one place, whatever the job, and leaves a line that is not its
/// own. An open's answer hands the wait to its next phase in the same step.
#[test]
fn a_job_puts_down_the_keys_and_the_line_it_held() {
    use crate::{Answer, App, AppEvent, Job, OpenOptions, Outcome};
    use polars::prelude::IntoLazy;

    let (tx, _rx) = std::sync::mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    let end = |app: &mut App, job: crate::jobs::Started, outcome: Outcome| {
        let ticket = job.ticket();
        job.end(outcome);
        app.event(&AppEvent::JobEnded(ticket))
    };
    let written = || Answer::QualityReportWritten(std::path::PathBuf::from("r.json"));

    let report = app.job_for_tests(Job::QualityReport, Some("Writing the report..."));
    assert!(app.is_busy(), "keys wait on it");
    assert_eq!(app.status_message.as_deref(), Some("Writing the report..."));
    assert!(end(&mut app, report, Outcome::answered(written())).is_none());
    assert!(!app.is_busy(), "and come back with its answer");
    assert_eq!(app.status_message, None, "with its line");

    let report = app.job_for_tests(Job::QualityReport, Some("Writing the report..."));
    app.status_message = Some(App::COUNTING_FOR_END.to_string());
    end(
        &mut app,
        report,
        Outcome::Failed {
            message: "disk full".to_string(),
            panicked: false,
        },
    );
    assert!(!app.is_busy());
    assert_eq!(
        app.status_message.as_deref(),
        Some(App::COUNTING_FOR_END),
        "a line that is not the job's stays"
    );

    let load = app.open_for_tests("a.csv");
    let scan = app.job_for_tests(Job::Load(load), Some("Scanning input..."));
    let next = end(
        &mut app,
        scan,
        Outcome::answered(Answer::Load(Box::new(
            crate::loading::LoadAnswer::Scanned {
                lf: Box::new(polars::df!("a" => [1i32]).unwrap().lazy()),
                path: None,
                options: OpenOptions::default(),
            },
        ))),
    );
    assert!(
        next.is_none(),
        "the next phase starts in the answer's own step"
    );
    assert!(app.is_busy(), "the open's wait goes on into its next phase");
    assert_eq!(
        app.status_message.as_deref(),
        Some("Reading schema..."),
        "under the next phase's line"
    );
}

/// A job's end leaves the line a job still running shows, even when the two say
/// the same: a view's pivot hands "Applying view..." to the read of its rows.
#[test]
fn a_job_leaves_the_line_another_job_still_shows() {
    use crate::{Answer, App, AppEvent, Job, Outcome};

    let (tx, _rx) = std::sync::mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    let first = app.job_for_tests(Job::QualityReport, Some("Writing the report..."));
    let second = app.job_for_tests(Job::QualityReport, Some("Writing the report..."));
    let ticket = first.ticket();
    first.end(Outcome::answered(Answer::QualityReportWritten(
        std::path::PathBuf::from("a.json"),
    )));
    app.event(&AppEvent::JobEnded(ticket));
    assert!(app.is_busy(), "the second still holds the keys");
    assert_eq!(
        app.status_message.as_deref(),
        Some("Writing the report..."),
        "and its line"
    );
    drop(second);
}

/// Going home quiets the read of the first page, and takes its line down with
/// the wait: the rows still land, and the bar does not say "Loading buffer..."
/// over a table nobody is waiting on.
#[test]
fn going_home_takes_down_the_line_of_the_rows_it_stops_waiting_on() {
    use crate::{App, InflightCollect, Job};

    let (tx, _rx) = std::sync::mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    app.loading.first_rows_for_tests();
    let read = app.job_for_tests(
        Job::Rows(InflightCollect::for_tests(0, 100)),
        Some(App::LOADING_BUFFER),
    );
    assert!(app.is_busy());
    app.enter_home();
    assert!(!app.is_busy(), "nobody waits on the rows");
    assert_eq!(
        app.status_message, None,
        "and the bar says nothing about them"
    );
    assert!(app.rows_in_flight().is_some(), "though they still land");
    drop(read);
}

/// A cancelled sample read is a read still going, as a cancelled run is: nothing
/// reads beside it until it ends. Work an open replaced is not a cancel, and
/// holds nothing up on the next dataset.
#[test]
fn a_cancelled_read_waits_out_its_worker_and_a_replaced_one_does_not() {
    use crate::{App, AppEvent, Job};

    let (tx, rx) = std::sync::mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    let sample = app.job_for_tests(Job::SampleRows, Some("Reading the sample..."));
    app.cancel_analysis();
    assert!(!app.is_busy());
    assert!(
        app.cancelled_analysis_running().is_some(),
        "the cancelled read is still going"
    );
    drop(sample);
    app.event(&recv(&rx));
    assert!(app.cancelled_analysis_running().is_none(), "until it ends");

    let run = app.job_for_tests(
        Job::Analysis(crate::jobs::AnalysisRun::default()),
        Some("Running analysis..."),
    );
    app.jobs.advance();
    assert!(
        app.cancelled_analysis_running().is_none(),
        "replaced, not cancelled"
    );
    drop(run);
    let _ = app.handle(&recv(&rx));
    assert!(!matches!(rx.try_recv(), Ok(AppEvent::JobEnded(_))));
}

/// A scan from a load the app has moved past is dropped rather than applied: it
/// starts no next phase and changes nothing the screen says about the open that
/// replaced it. What makes leaving a slow load safe: the work keeps running (Polars
/// has no cancellation), so the only thing between an abandoned load and a
/// clobbered screen is whether its load is the one in flight.
#[test]
fn a_superseded_scan_does_not_continue_the_load() {
    use crate::loading::LoadAnswer;
    use crate::{Answer, App, AppEvent, Job, OpenOptions, Outcome};
    use polars::prelude::IntoLazy;

    let (tx, _rx) = std::sync::mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    let scanned = || {
        Answer::Load(Box::new(LoadAnswer::Scanned {
            lf: Box::new(polars::df!("a" => [1i32]).unwrap().lazy()),
            path: Some(std::path::PathBuf::from("whatever.parquet")),
            options: OpenOptions::default(),
        }))
    };

    let first = app.open_for_tests("first.parquet");
    let scan = app.job_for_tests(Job::Load(first), Some("Scanning input..."));
    let second = app.open_for_tests("second.parquet");
    assert_ne!(first, second);
    let ticket = scan.ticket();
    scan.end(Outcome::answered(scanned()));
    assert!(app.event(&AppEvent::JobEnded(ticket)).is_none());
    assert!(
        app.jobs
            .current(|job| matches!(job, Job::Load(_)))
            .is_none(),
        "a superseded scan must not continue the load pipeline"
    );
    assert_eq!(
        app.load_shown()
            .map(|(phase, _, path, _)| (phase.to_string(), path.map(Path::to_path_buf))),
        Some((
            "Scanning input".to_string(),
            Some(std::path::PathBuf::from("second.parquet"))
        )),
        "nor change what the screen says about the open that replaced it"
    );

    // Nor does the first's dataset install, read after all.
    let state = crate::widgets::datatable::DataTableState::from_lazyframe(
        polars::df!("a" => [1i32]).unwrap().lazy(),
        &OpenOptions::default(),
    )
    .unwrap();
    let read = Answer::Load(Box::new(LoadAnswer::SchemaRead {
        state: Box::new(state),
        path: Some(std::path::PathBuf::from("first.parquet")),
        options: OpenOptions::default(),
        debug_label: None,
    }));
    assert!(app.answer_for_tests(Job::Load(first), read).is_none());
    assert!(app.data_table_state.is_none(), "nothing was installed");
    assert!(
        app.awaiting_dataset(),
        "the second open is still on its way"
    );

    // The current one does.
    assert!(app.answer_for_tests(Job::Load(second), scanned()).is_none());
    assert!(
        app.jobs
            .current(|job| matches!(job, Job::Load(_)))
            .is_some(),
        "the schema is read next"
    );
    assert_eq!(
        app.load_shown().map(|shown| shown.0),
        Some("Reading schema")
    );
}

/// An analysis answer from a run the app has moved past changes nothing on
/// screen, whatever the tool: every one of them is judged by its job.
#[test]
fn stale_analysis_answers_are_ignored() {
    use crate::data_quality::{DataQualityResults, QualityPrecision};
    use crate::statistics::AnalysisResults;
    use crate::{Answer, App, AppEvent, Job, Outcome};

    let (tx, _rx) = std::sync::mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    let results = || AnalysisResults {
        column_statistics: vec![],
        total_rows: 999_999,
        sample_size: None,
        per_value: None,
        sample_seed: 0,
        correlation_matrix: None,
        distribution_analyses: vec![],
    };
    let answers = vec![
        Answer::Described(results()),
        Answer::Distributions(results()),
        Answer::Correlations(results()),
        Answer::DataQuality {
            results: Box::new(DataQualityResults {
                total_rows: Some(999_999),
                evaluated_rows: 1,
                precision: QualityPrecision::Sampled,
                sample_seed: 1,
                columns: vec![],
                observations: vec![],
                segments: vec![],
                temporal: vec![],
                identity: None,
                category_variants: vec![],
                shared_nulls: vec![],
                source_files: None,
                per_value: None,
                footers_read: None,
                reads: None,
                examples: vec![],
                unsampled_segments: vec![],
                intent: None,
                source: None,
            }),
            kept: None,
            plan: Box::default(),
        },
    ];
    app.analysis_modal.active = true;
    app.analysis_modal.selected_tool = Some(crate::analysis_modal::AnalysisTool::DataQuality);
    let runs: Vec<_> = answers
        .iter()
        .map(|_| {
            app.job_for_tests(
                Job::Analysis(crate::jobs::AnalysisRun::default()),
                Some("Running analysis..."),
            )
        })
        .collect();
    app.jobs.advance();
    for (run, answer) in runs.into_iter().zip(answers) {
        let ticket = run.ticket();
        run.end(Outcome::answered(answer));
        app.event(&AppEvent::JobEnded(ticket));
    }
    let modal = &app.analysis_modal;
    assert!(modal.describe_results.is_none());
    assert!(modal.distribution_results.is_none());
    assert!(modal.correlation_results.is_none());
    assert!(modal.data_quality_results.is_none());
}

/// Work a cancel passed holds nothing up. Polars cannot stop the query, so the
/// worker runs on; the advance made its answer stale, and the table must not wait
/// on it. A job on the new generation still counts, and the old one ending does not
/// release it.
#[test]
fn a_cancelled_job_does_not_hold_the_generation() {
    use crate::{App, AppEvent, Job};

    let (tx, rx) = std::sync::mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    let old = app.job_for_tests(
        Job::Analysis(crate::jobs::AnalysisRun::default()),
        Some("Running analysis..."),
    );
    app.jobs.advance();
    assert!(
        !app.work_a_bump_would_strand(),
        "the abandoned worker is not waited on"
    );
    assert!(app.cancelled_work_running(), "though it is still running");

    let current = app.job_for_tests(
        Job::Analysis(crate::jobs::AnalysisRun::default()),
        Some("Running analysis..."),
    );
    assert!(app.work_a_bump_would_strand());

    drop(old);
    let _ = app.handle(&recv(&rx));
    assert!(
        app.work_a_bump_would_strand(),
        "the current job still holds"
    );
    assert!(!app.cancelled_work_running());
    drop(current);
    let ended = recv(&rx);
    assert!(matches!(ended, AppEvent::JobEnded(_)));
    let _ = app.handle(&ended);
    assert!(!app.work_a_bump_would_strand());
}

/// A worker that panics fails its job the way an error would, in one step: the
/// failure is shown, the job's own marker goes, and the generation is free.
///
/// The hazard `schema_union::Pass` was built for: a hold that never comes back down
/// is a permanent "something is waiting", and here that would mean the buffer never
/// collects again for the rest of the session. And a panic that only reached the log
/// would leave the spinner up with nothing on screen saying why, or a pivot the form
/// still waits on.
#[test]
fn a_panicking_worker_ends_its_job() {
    use crate::{App, AppEvent, Job, JobKind};

    let (tx, rx) = std::sync::mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    let generation = app.task_generation();
    app.input_mode = crate::InputMode::PivotMelt;
    let ticket = app.spawn_job(
        Job::Pivot,
        Some(App::COMPUTING_PIVOT),
        |_| -> std::result::Result<crate::Answer, String> { panic!("worker died") },
    );
    assert!(app.work_a_bump_would_strand());
    assert!(app.is_busy());
    assert!(app.pivot_computing(), "the form waits on it");

    let failed = recv(&rx);
    assert!(
        matches!(&failed, AppEvent::JobEnded(t) if *t == ticket && t.kind() == JobKind::Pivot),
        "the panic ends the job it stopped"
    );
    let _ = app.handle(&failed);
    assert!(!app.is_busy(), "the spinner comes down");
    assert!(app.status_message.is_none());
    assert!(app.error_modal.active, "and the user is told");
    assert!(
        app.error_modal.message.contains("worker died"),
        "{}",
        app.error_modal.message
    );
    assert!(!app.pivot_computing(), "the pivot is no longer waited on");
    assert!(
        !app.work_a_bump_would_strand(),
        "the generation is free again rather than held forever"
    );
    assert_eq!(app.task_generation(), generation, "and nothing bumped it");
    assert!(
        rx.recv_timeout(std::time::Duration::from_millis(50))
            .is_err()
    );
}

/// Enough of an event to say which it was in a failed assertion.
fn describe(event: &crate::AppEvent) -> String {
    use crate::AppEvent;
    match event {
        AppEvent::JobEnded(ticket) => format!("JobEnded({:?})", ticket.kind()),
        AppEvent::BackgroundLenReady { num_rows, .. } => {
            format!("BackgroundLenReady({num_rows})")
        }
        AppEvent::BackgroundLenFailed { .. } => "BackgroundLenFailed".to_string(),
        _ => "another event".to_string(),
    }
}

/// A job that returns an error and one that panics end the same way: one end,
/// with the reason shown, and nothing after it.
#[test]
fn an_error_and_a_panic_end_a_job_the_same_way() {
    use crate::{App, AppEvent, Job};

    let (tx, rx) = std::sync::mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    for (what, dies) in [("returns an error", false), ("panics", true)] {
        app.error_modal.hide();
        app.spawn_job(
            Job::QualityReport,
            Some("Writing the report..."),
            move |_| {
                assert!(!dies, "worker died");
                Err::<crate::Answer, _>("disk full".to_string())
            },
        );
        let ended = recv(&rx);
        assert!(
            matches!(ended, AppEvent::JobEnded(_)),
            "a job that {what}: {}",
            describe(&ended)
        );
        let _ = app.handle(&ended);
        assert!(
            rx.recv_timeout(std::time::Duration::from_millis(50))
                .is_err(),
            "a job that {what} ends once"
        );
        assert!(!app.is_busy(), "a job that {what} is over");
        assert!(app.error_modal.active, "a job that {what} says why");
        assert!(
            app.error_modal
                .message
                .contains(if dies { "worker died" } else { "disk full" }),
            "a job that {what}: {}",
            app.error_modal.message
        );
        assert!(!app.work_a_bump_would_strand());
    }
}

/// A failure puts down only what its own job started. One from a job that has been
/// superseded — by an advance, a newer job of its kind, or the screen it was for
/// going away — or from another job sharing the generation, leaves the work in
/// flight alone: its spinner, its record, and the screen with no error on it.
#[test]
fn a_failure_leaves_other_work_alone() {
    use crate::{
        AnalysisProgress, App, AppEvent, ChartExportFormat, InflightCollect, Job, Outcome,
    };
    use std::path::PathBuf;

    // An open replaced long since.
    let gone = crate::loading::LoadId::for_tests(u64::MAX);

    let (tx, _rx) = std::sync::mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    let fail = |app: &mut App, job: crate::jobs::Started| {
        let ticket = job.ticket();
        job.end(Outcome::Failed {
            message: "not this one".to_string(),
            panicked: false,
        });
        app.event(&AppEvent::JobEnded(ticket));
    };
    let untouched = |app: &App, what: &str| {
        assert!(app.is_busy(), "{what}: still busy");
        assert!(!app.error_modal.active, "{what}: no error shown");
        assert!(
            app.jobs
                .current(|job| matches!(job, Job::Analysis(_)))
                .is_some(),
            "{what}: the analysis is still waited on"
        );
    };
    let analysis = || Job::Analysis(crate::jobs::AnalysisRun::default());

    // Jobs on a generation the analysis's has passed.
    let kinds = [
        analysis(),
        Job::SampleRows,
        Job::Load(gone),
        Job::OpenNamed(gone),
        Job::Rows(InflightCollect::for_tests(0, 100)),
        Job::Pivot,
        Job::DrillRow,
        Job::InspectRow { frame: 0, row: 0 },
        Job::InspectJson { token: 0 },
        Job::Export,
        Job::Copy,
        Job::QualityReport,
    ];
    let passed: Vec<_> = kinds
        .iter()
        .map(|job| app.job_for_tests(job.clone(), Some("Working...")))
        .collect();
    app.jobs.advance();

    // An analysis is computing; a load-ahead beside it, on the same generation,
    // dies. Its own record goes, and nothing else.
    let running = app.job_for_tests(analysis(), Some("Running analysis..."));
    app.analysis_modal.computing = Some(AnalysisProgress::new("Running analysis"));
    let ahead = app.job_for_tests(Job::Rows(InflightCollect::for_tests(0, 100)), None);
    fail(&mut app, ahead);
    untouched(&app, "a load-ahead");
    assert!(app.analysis_modal.computing.is_some());
    assert!(
        app.rows_in_flight().is_none(),
        "the load-ahead's record goes"
    );

    for (job, started) in kinds.iter().zip(passed) {
        fail(&mut app, started);
        untouched(&app, &format!("{job:?} from a passed generation"));
    }

    // An older look at a path, which a newer one superseded.
    let look = |path: &str| {
        Job::Classify(crate::jobs::Classify {
            path: PathBuf::from(path),
            browsing: None,
            jump: false,
        })
    };
    let older = app.job_for_tests(look("/older"), None);
    app.jobs.supersede(|job| matches!(job, Job::Classify(_)));
    let _newer = app.job_for_tests(look("/newer"), None);
    fail(&mut app, older);
    untouched(&app, "an older look");

    // The same for a look at a directory named to open.
    let older = app.job_for_tests(
        Job::LookAtDirectory {
            load: gone,
            path: PathBuf::from("/older"),
        },
        None,
    );
    app.jobs
        .supersede(|job| matches!(job, Job::LookAtDirectory { .. }));
    fail(&mut app, older);
    untouched(&app, "an older look at a directory");

    // A chart export its dataset's departure superseded.
    let older = app.job_for_tests(
        Job::ChartExport {
            path: PathBuf::from("/tmp/old.png"),
            format: ChartExportFormat::Png,
        },
        None,
    );
    app.jobs
        .supersede(|job| matches!(job, Job::ChartExport { .. }));
    fail(&mut app, older);
    untouched(&app, "an older chart export");
    assert!(!app.chart_export_modal.active);

    // An open is no longer waited on once the user has gone home from it.
    let load = app.open_for_tests("gone.csv");
    let open = app.job_for_tests(Job::Load(load), None);
    app.abandon_load();
    fail(&mut app, open);
    untouched(&app, "an abandoned open");

    // And the analysis's own failure ends it.
    fail(&mut app, running);
    assert!(!app.is_busy());
    assert!(app.analysis_modal.computing.is_none());
    assert!(app.error_modal.active);
}

/// A count that was started answers whatever its worker does: a count that panics,
/// or a collect it rides in that dies before reaching it, reports it failed rather
/// than leaving the row count spinning for the session.
#[test]
fn a_count_answers_however_its_worker_ends() {
    use crate::{App, AppEvent, DataTableState, LenCount, OpenOptions, OwedCount};
    use polars::prelude::IntoLazy;

    let (tx, rx) = std::sync::mpsc::channel();
    let mut app = App::new(tx.clone(), crate::tests::test_runtime());
    let lf = polars::df!("a" => [1i32, 2, 3]).unwrap().lazy();
    let state = DataTableState::from_lazyframe(lf, &OpenOptions::default()).unwrap();
    let generation = state.len_generation();
    let owed = || OwedCount::new(LenCount::for_state(&state), tx.clone());
    let answer = |app: &mut App| {
        app.len_count_inflight = Some(generation);
        let event = recv(&rx);
        app.event(&event);
        assert_eq!(
            app.len_count_inflight, None,
            "the count is no longer waited on"
        );
        assert!(rx.try_recv().is_err(), "once");
        event
    };

    owed().answer(|_| panic!("count died"));
    let failed = answer(&mut app);
    assert!(
        matches!(failed, AppEvent::BackgroundLenFailed { .. }),
        "{}",
        describe(&failed)
    );

    // The worker carrying it died first.
    drop(owed());
    let failed = answer(&mut app);
    assert!(
        matches!(failed, AppEvent::BackgroundLenFailed { .. }),
        "{}",
        describe(&failed)
    );

    owed().answer(LenCount::run);
    let counted = answer(&mut app);
    assert!(
        matches!(counted, AppEvent::BackgroundLenReady { num_rows: 3, .. }),
        "{}",
        describe(&counted)
    );
}

/// An open's first phase holds the generation before the errands behind it get
/// their turn.
///
/// `App::handle` runs the owed re-read and the owed collect at its tail, after
/// `dispatch_event` has handled the open. `Open` bumps `task_generation`; had it
/// left its scan to a later step, nothing would hold the generation at that tail, an
/// owed collect would go in there and bump the generation out from under the open,
/// and the file would never open. The scan is started in the same handler instead,
/// and holds the generation itself.
#[test]
fn an_open_holds_the_generation_before_the_errands_behind_it() {
    use crate::widgets::datatable::DataTableState;
    use crate::{App, AppEvent, OpenOptions};
    use polars::prelude::*;
    use std::sync::Arc;

    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("next.csv");
    std::fs::write(&path, "name,age\nada,36\n").expect("write csv");

    let rows = || df!("id" => &[1i64, 2, 3]).unwrap().lazy();
    let mut lf = rows();
    let schema = Arc::new((*lf.collect_schema().unwrap()).clone());
    let state =
        DataTableState::from_schema_and_lazyframe(schema, rows(), &OpenOptions::default(), None)
            .unwrap();

    let (tx, _rx) = std::sync::mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    app.install_for_tests(state, None, &OpenOptions::default(), None);

    // A collect owed to the dataset on screen, waiting for the generation to be free.
    app.owe_rows_for_tests("Loading buffer...");
    assert!(
        !app.work_a_bump_would_strand(),
        "nothing holds the generation: the errand would go in on the next event"
    );

    // And the user opens something else.
    let out = app
        .handle(&AppEvent::Open(vec![path], OpenOptions::default()))
        .expect("the open is not a key");
    assert!(out.is_none(), "the open started its scan itself");
    assert!(
        app.work_a_bump_would_strand(),
        "and the scan holds the generation"
    );
    assert!(
        app.rows_owed(),
        "the collect is still owed rather than run: running it here would bump the \
         generation the open has just taken for its scan"
    );
}

/// An open parked on the download confirmation holds the generation while it waits.
///
/// The one errand that waits on neither a worker nor a continuation: nothing is
/// running, the open is very much unfinished, and the wait is as long as the user
/// takes to answer. A collect starting meanwhile bumps `task_generation`, and the
/// download they are about to agree to then answers a generation nothing matches.
#[cfg(feature = "http")]
#[test]
fn a_download_waiting_on_the_user_holds_the_generation() {
    use crate::loading::{LoadAnswer, PendingDownload};
    use crate::{Answer, App, Job, OpenOptions};
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

    let (tx, _rx) = std::sync::mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    let url = "https://example.invalid/data.parquet";
    let load = app.open_for_tests(url);
    assert!(!app.work_a_bump_would_strand(), "nothing is running yet");

    let pending = PendingDownload::Http {
        url: url.to_string(),
        size: Some(1024),
        options: OpenOptions::default(),
    };
    let _ = app.answer_for_tests(
        Job::Load(load),
        Answer::Load(Box::new(LoadAnswer::Sized(pending))),
    );

    assert!(app.confirmation_modal.active, "the user is being asked");
    assert!(
        app.work_a_bump_would_strand(),
        "and the generation is held for as long as they take to answer"
    );

    // Declining puts the errand down, and the generation with it.
    let _ = app.key(&KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE));
    assert!(
        !app.work_a_bump_would_strand(),
        "nothing waits on it once the download is declined"
    );
}

/// A server that compresses on request (GitHub Pages, where the public NYC flights
/// file lives) still gets its size read: asked for gzip, it answers with the
/// compressed length, which ureq strips, and the confirmation said "unknown".
#[cfg(feature = "http")]
#[test]
fn the_size_probe_reads_a_compressing_server() {
    use std::io::{Read, Write};

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!(
        "http://{}/flights.csv",
        listener.local_addr().expect("bound")
    );
    std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut request = Vec::new();
        let mut chunk = [0u8; 1024];
        while !request.windows(4).any(|w| w == b"\r\n\r\n") {
            match stream.read(&mut chunk) {
                Ok(0) | Err(_) => break,
                Ok(n) => request.extend_from_slice(&chunk[..n]),
            }
        }
        let asks_for_gzip = String::from_utf8_lossy(&request)
            .to_ascii_lowercase()
            .lines()
            .any(|l| l.starts_with("accept-encoding:") && l.contains("gzip"));
        let headers = if asks_for_gzip {
            "Content-Encoding: gzip\r\nContent-Length: 9404410"
        } else {
            "Content-Length: 33206996"
        };
        let _ = write!(
            stream,
            "HTTP/1.1 200 OK\r\n{headers}\r\nConnection: close\r\n\r\n"
        );
    });

    assert_eq!(
        crate::App::fetch_remote_size_http(&url).expect("the probe never fails"),
        Some(33_206_996),
        "the file's own length, not the compressed one and not none"
    );
}

/// The size probe is labeled as such, and once the confirmation is up nothing
/// spins: datui is waiting on a key, and the bar names the modal's keys (#385).
#[cfg(feature = "http")]
#[test]
fn the_download_confirmation_is_not_busy() {
    use crate::{App, AppEvent, OpenOptions};
    use ratatui::buffer::Buffer;
    use ratatui::layout::Rect;
    use ratatui::widgets::Widget;

    fn screen(app: &mut App) -> String {
        let area = Rect::new(0, 0, 120, 24);
        let mut buf = Buffer::empty(area);
        app.render(area, &mut buf);
        buf.content().iter().map(|c| c.symbol()).collect()
    }
    let spinning = |text: &str| {
        crate::glyphs::get()
            .spinner
            .iter()
            .any(|frame| text.contains(frame))
    };

    let (tx, rx) = std::sync::mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    // Nothing listens on the discard port: the probe fails fast, and answers that
    // the size is unknown.
    let url = "http://127.0.0.1:9/flights.parquet";
    let mut next = Some(AppEvent::Open(
        vec![std::path::PathBuf::from(url)],
        OpenOptions::default(),
    ));
    while let Some(event) = next {
        next = app.handle(&event).expect("no keys here");
    }

    assert!(app.is_busy(), "the probe is running");
    let bar = control_bar(&mut app);
    assert!(bar.contains("Checking size"), "the probe is named: {bar}");
    assert!(!bar.contains("Scanning"), "nothing is scanned yet: {bar}");

    let answered = rx
        .recv_timeout(std::time::Duration::from_secs(60))
        .expect("the probe answers");
    assert!(matches!(answered, AppEvent::JobEnded(_)));
    let _ = app.handle(&answered);

    assert!(app.awaiting_open_confirmation(), "the user is being asked");
    assert!(!app.is_busy(), "and nothing is running while they decide");
    let bar = control_bar(&mut app);
    assert!(!bar.contains("..."), "nothing said to be running: {bar}");
    assert!(
        !bar.contains("Checking") && !bar.contains("Scanning"),
        "and no phase: {bar}"
    );
    assert!(!app.something_is_spinning(), "the run loop turns nothing");
    // The ASCII frames (`|`, `/`, `-`) are also the modal's border, so only the
    // Unicode ones can be looked for on screen.
    if crate::glyphs::active_is_unicode() {
        let text = screen(&mut app);
        assert!(!spinning(&text), "no spinner anywhere: {text}");
    }
}

/// A destination that takes what it is given, uncapped like the native one or
/// capped at `limit` bytes of base64 like the terminal's, so a copy test does not
/// depend on whether the machine has a display.
struct TestClipboard(Option<usize>);

impl crate::clipboard::Destination for TestClipboard {
    fn write(&mut self, _: crate::clipboard::Payload) -> Result<(), String> {
        Ok(())
    }
    fn describe(&self) -> &'static str {
        "test"
    }
    fn accepts(&self) -> crate::clipboard::Accepts {
        crate::clipboard::Accepts {
            html: self.0.is_none(),
            base64_limit: self.0,
        }
    }
}

fn uncapped_clipboard(app: &mut crate::App) {
    app.set_clipboard_destination(Box::new(TestClipboard(None)));
}

/// A capped destination's table copy is read only as far as its cap, so the
/// guard asks about the smaller of the estimate and the cap: under the 10 MB
/// that asks, an unknown size or a large estimate collects without asking.
#[test]
fn a_capped_table_copy_asks_only_past_what_the_cap_could_hold() {
    use crate::widgets::datatable::DataTableState;
    use crate::{App, AppEvent, OpenOptions};
    use polars::prelude::*;
    use std::sync::Arc;

    let copy = |footer_width: Option<usize>, limit: usize| {
        let rows = || {
            df!("id" => &[1i64, 2, 3], "blob" => &[b"a".as_slice(), b"b", b"c"])
                .unwrap()
                .lazy()
        };
        let mut lf = rows();
        let schema = Arc::new((*lf.collect_schema().unwrap()).clone());
        let state = DataTableState::from_schema_and_lazyframe(
            schema,
            rows(),
            &OpenOptions::default(),
            None,
        )
        .unwrap()
        .with_open(crate::widgets::datatable::OpenFacts {
            column_bytes: footer_width
                .map(|width| vec![("blob".to_string(), width)])
                .unwrap_or_default(),
            ..Default::default()
        });
        let (tx, _rx) = std::sync::mpsc::channel();
        let mut app = App::new(tx, crate::tests::test_runtime());
        app.install_for_tests(state, None, &OpenOptions::default(), None);
        let state = app.data_table_state.as_mut().unwrap();
        assert!(state.count_landed(state.len_generation(), 3, None));
        app.set_clipboard_destination(Box::new(TestClipboard(Some(limit))));
        app.copy_modal.scope = crate::copy_modal::CopyScope::Table;
        let next = app.perform_copy();
        (app, next)
    };

    // 12 MiB of base64 against the terminal's 100 KB: read to the cap, no question.
    let (app, next) = copy(Some(3 * 1024 * 1024), 100 * 1024);
    assert!(!app.confirmation_modal.active);
    assert!(matches!(next, Some(AppEvent::CopyTable { .. })));
    // Unmeasured blobs: the cap bounds the read all the same.
    let (app, next) = copy(None, 100 * 1024);
    assert!(!app.confirmation_modal.active);
    assert!(matches!(next, Some(AppEvent::CopyTable { .. })));
    // A cap raised past 10 MB asks, as an uncapped copy does.
    let (app, next) = copy(Some(3 * 1024 * 1024), 64 * 1024 * 1024);
    assert!(app.confirmation_modal.active && next.is_none());
    let (app, next) = copy(None, 64 * 1024 * 1024);
    assert!(app.confirmation_modal.active && next.is_none());
}

/// A whole-table copy whose size is not known yet (the row count is still
/// being read) must ask first, never collect an unknown amount unprompted.
#[test]
fn a_table_copy_with_no_size_yet_asks_first() {
    use crate::widgets::datatable::DataTableState;
    use crate::{App, OpenOptions};
    use polars::prelude::*;
    use std::sync::Arc;

    let rows = || df!("id" => &[1i64, 2, 3]).unwrap().lazy();
    let mut lf = rows();
    let schema = Arc::new((*lf.collect_schema().unwrap()).clone());
    let state =
        DataTableState::from_schema_and_lazyframe(schema, rows(), &OpenOptions::default(), None)
            .unwrap();
    let (tx, _rx) = std::sync::mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    app.install_for_tests(state, None, &OpenOptions::default(), None);

    let state = app.data_table_state.as_mut().unwrap();
    state.invalidate_num_rows();
    assert!(state.estimated_copy_bytes().is_none());

    uncapped_clipboard(&mut app);
    app.copy_modal.scope = crate::copy_modal::CopyScope::Table;
    let _ = app.perform_copy();
    assert!(
        app.confirmation_modal.active,
        "an unknown size asks; it never collects unprompted"
    );
    assert!(app.pending_copy.is_some());
}

/// A Table copy writes binary as base64, so the guard counts it at that size
/// from the footer's width: large blobs ask first, small ones copy, and blobs no
/// footer measured ask, since the buffer only ever holds a stub for them (#429).
#[test]
fn a_table_copy_counts_binary_at_its_base64_size() {
    use crate::widgets::datatable::DataTableState;
    use crate::{App, AppEvent, OpenOptions};
    use polars::prelude::*;
    use std::sync::Arc;

    let copy = |footer_width: Option<usize>| {
        let rows = || {
            df!("id" => &[1i64, 2, 3], "blob" => &[b"a".as_slice(), b"b", b"c"])
                .unwrap()
                .lazy()
        };
        let mut lf = rows();
        let schema = Arc::new((*lf.collect_schema().unwrap()).clone());
        let state = DataTableState::from_schema_and_lazyframe(
            schema,
            rows(),
            &OpenOptions::default(),
            None,
        )
        .unwrap()
        .with_open(crate::widgets::datatable::OpenFacts {
            column_bytes: footer_width
                .map(|width| vec![("blob".to_string(), width)])
                .unwrap_or_default(),
            ..Default::default()
        });
        let (tx, _rx) = std::sync::mpsc::channel();
        let mut app = App::new(tx, crate::tests::test_runtime());
        app.install_for_tests(state, None, &OpenOptions::default(), None);
        let state = app.data_table_state.as_mut().unwrap();
        assert!(state.count_landed(state.len_generation(), 3, None));
        uncapped_clipboard(&mut app);
        app.copy_modal.scope = crate::copy_modal::CopyScope::Table;
        let next = app.perform_copy();
        (app, next)
    };

    // Three rows of 3 MiB are 12 MiB of base64, past the 10 MiB that asks; the
    // bytes alone, or the stub the buffer holds, would not be.
    let (app, next) = copy(Some(3 * 1024 * 1024));
    assert!(app.confirmation_modal.active, "large blobs ask first");
    assert!(app.pending_copy.is_some() && next.is_none());

    let (app, next) = copy(Some(100));
    assert!(!app.confirmation_modal.active, "small blobs copy");
    assert!(matches!(next, Some(AppEvent::CopyTable { .. })));

    let (app, next) = copy(None);
    assert!(app.confirmation_modal.active, "unmeasured blobs ask");
    assert!(app.pending_copy.is_some() && next.is_none());
}

/// A local directory's footers, read to open it, give its binary columns their
/// width as a cloud object's do, so a Table copy is sized rather than asked about.
#[test]
fn a_local_directorys_footers_size_its_binary_columns() {
    use crate::{App, AppEvent, OpenOptions};
    use polars::prelude::*;

    let copy = |blob: usize| {
        let dir = tempfile::tempdir().unwrap();
        // Two files, three rows, every blob distinct.
        for (file, rows) in [(0u8, 2u8), (1, 1)] {
            let blobs: Vec<Vec<u8>> = (0..rows).map(|i| vec![file * 10 + i; blob]).collect();
            let mut df = df!(
                "id" => (0..rows as i64).collect::<Vec<_>>(),
                "blob" => blobs.iter().map(Vec::as_slice).collect::<Vec<_>>(),
            )
            .unwrap();
            let f = std::fs::File::create(dir.path().join(format!("{file}.parquet"))).unwrap();
            ParquetWriter::new(f).finish(&mut df).unwrap();
        }
        let options = OpenOptions {
            hive: true,
            ..OpenOptions::default()
        };
        let state =
            App::schema_state_from_local_hive(Some(dir.path()), &options, &Default::default())
                .map(|(state, facts)| state.with_open(facts))
                .expect("the local footer route");
        let (tx, _rx) = std::sync::mpsc::channel();
        let mut app = App::new(tx, crate::tests::test_runtime());
        app.install_for_tests(state, None, &options, None);
        let state = app.data_table_state.as_mut().unwrap();
        assert!(state.count_landed(state.len_generation(), 3, None));
        uncapped_clipboard(&mut app);
        app.copy_modal.scope = crate::copy_modal::CopyScope::Table;
        let next = app.perform_copy();
        (app, next)
    };

    // Three rows of 3 MiB are 12 MiB of base64, past the 10 MiB that asks.
    let (app, next) = copy(3 * 1024 * 1024);
    assert!(next.is_none());
    let message = &app.confirmation_modal.message;
    assert!(message.starts_with("This copies about 12"), "{message}");

    let (app, next) = copy(100);
    assert!(!app.confirmation_modal.active, "small blobs copy");
    assert!(matches!(next, Some(AppEvent::CopyTable { .. })));
}

/// A confirmation names its keys in its own footer; the status footer adds no mode
/// keys over it.
#[test]
fn a_confirmation_keeps_its_keys_in_its_own_footer() {
    use crate::App;
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rows.csv");
    std::fs::write(&path, "id\n1\n2\n3\n").unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    let mut app = App::new(tx.clone(), crate::tests::test_runtime());
    chart_prepare_tests::open(&mut app, &rx, &tx, path);
    let _ = app.key(&KeyEvent::new(KeyCode::Char('l'), KeyModifiers::NONE));
    assert!(
        control_bar(&mut app).contains("+/- Filter"),
        "the column's keys"
    );

    app.data_table_state.as_mut().unwrap().invalidate_num_rows();
    uncapped_clipboard(&mut app);
    app.copy_modal.scope = crate::copy_modal::CopyScope::Table;
    let _ = app.perform_copy();
    assert!(app.confirmation_modal.active, "an unknown size asks");
    let bar = control_bar(&mut app);
    assert!(!bar.contains("Filter"), "not the table's: {bar}");
}

/// A count landing while a load is in flight does not cancel the load.
///
/// `BackgroundLenReady` answers an End by jumping to the end, which reaches
/// `spawn_async_collect` with no key pressed and, on a large remote dataset, minutes
/// after the one that was — long enough for the user to have opened something else.
/// That bump threw the open's answer away and left the open waiting on it, so the
/// loading screen stayed up and the file never opened, silently, for the rest of the
/// session.
#[test]
fn a_count_landing_during_a_load_does_not_bump_the_generation() {
    use crate::widgets::datatable::{DataTableState, RemoteFiles};
    use crate::{App, AppEvent, OpenOptions};
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
    use polars::prelude::*;
    use std::sync::Arc;

    let rows = || df!("id" => (0..100i64).collect::<Vec<_>>()).unwrap().lazy();
    let mut lf = rows();
    let schema = Arc::new((*lf.collect_schema().unwrap()).clone());
    let mut state =
        DataTableState::from_schema_and_lazyframe(schema, rows(), &OpenOptions::default(), None)
            .unwrap()
            .with_open(crate::widgets::datatable::OpenFacts {
                remote_source: true,
                remote_files: Some(RemoteFiles {
                    urls: Arc::new(vec!["one".to_string()]),
                    scan: Arc::new(move |_u: &[String], _t: &[PlSmallStr]| Ok(rows())),
                    count: Arc::new(|_| Ok(vec![vec![100]])),
                    offsets: None,
                }),
                ..Default::default()
            });
    state.visible_rows = 10;

    let (tx, _rx) = std::sync::mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    app.install_for_tests(state, None, &OpenOptions::default(), None);

    // End on a dataset whose rows are not counted yet: the jump waits for the count.
    let waiting = app.data_table_state.as_ref().unwrap().len_generation();
    let _ = app.key(&KeyEvent::new(KeyCode::End, KeyModifiers::NONE));
    assert_eq!(app.end_after_count, Some(waiting));

    // Meanwhile the user opens something else, which is waiting on this generation.
    let lease = app.hold_the_generation();
    let opening = app.task_generation();

    // And the count lands.
    let mut follow = app.event(&AppEvent::BackgroundLenReady {
        len_generation: waiting,
        num_rows: 100,
        file_row_groups: None,
    });
    while let Some(event) = follow {
        follow = app.event(&event);
    }

    assert_eq!(
        app.task_generation(),
        opening,
        "the open is still waiting on the generation the jump would have bumped"
    );
    assert!(
        app.rows_owed(),
        "and the jump's collect is owed rather than dropped"
    );

    // The open finishes, and the jump gets its turn.
    drop(lease);
    let _ = app.handle(&AppEvent::Update);
    assert!(
        !app.rows_owed(),
        "the collect the jump asked for runs once nothing is waiting"
    );
    assert_ne!(
        app.task_generation(),
        opening,
        "and it is what bumps the generation, now that it is safe to"
    );
}

/// A dataset waiting on an owed re-read still says its count is coming.
///
/// The number a staged open holds is only as far as the buffer reached. While the
/// pass is out the dataset says so itself, and the bar shows a spinner. A pass that
/// fails takes that away — it gives up on the footers the moment it lands — and the
/// count that would replace it is the one the errand is waiting to start. Between
/// the two the bar has nothing marking the number provisional, and prints a prefix
/// of six thousand files as `Rows: 70`, plainly, for as long as the work in front of
/// the errand takes.
#[test]
fn a_dataset_owed_a_re_read_does_not_print_its_partial_as_the_total() {
    use crate::widgets::datatable::DataTableState;
    use crate::{App, AppEvent, OpenOptions};
    use polars::prelude::*;
    use ratatui::buffer::Buffer;
    use ratatui::layout::Rect;
    use ratatui::widgets::Widget;
    use std::sync::Arc;

    let rows = || df!("id" => (0..70i64).collect::<Vec<_>>()).unwrap().lazy();
    let mut lf = rows();
    let schema = Arc::new((*lf.collect_schema().unwrap()).clone());
    let mut state =
        DataTableState::from_schema_and_lazyframe(schema, rows(), &OpenOptions::default(), None)
            .unwrap()
            .with_open(crate::widgets::datatable::OpenFacts {
                footers_pending: Some(Arc::new(|_| None)),
                ..Default::default()
            });
    // As a staged open leaves it: a provisional from a short read, a pass still out.
    state.set_provisional_rows(70);

    let (tx, _rx) = std::sync::mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    app.install_for_tests(state, None, &OpenOptions::default(), None);
    app.busy = false;

    let bar_says_seventy = |app: &mut App| {
        let area = Rect::new(0, 0, 100, 24);
        let mut buf = Buffer::empty(area);
        (&mut *app).render(area, &mut buf);
        (0..area.width)
            .map(|x| buf[(x, area.height - 1)].symbol().to_string())
            .collect::<String>()
            .contains("70 rows")
    };
    assert!(
        !bar_says_seventy(&mut app),
        "while the pass is out the dataset says its count is coming"
    );

    // An export is running and holds a lease, so the errand the failure raises has
    // to wait.
    app.export_progress = Some(crate::ExportProgress {
        file_path: std::path::PathBuf::from("/tmp/out.csv"),
        current_phase: "Collecting".to_string(),
        written: None,
    });
    let _lease = app.hold_the_generation();
    let live = app.dataset_generation;
    App::record_footers(&app.pending_footers_result, live, None);
    let _ = app.handle(&AppEvent::BackgroundFootersJoined { generation: live });
    assert!(
        app.reread_owed.is_some(),
        "the fixture is a dataset owed a re-read it cannot have yet"
    );

    app.busy = false;
    assert!(
        !bar_says_seventy(&mut app),
        "and it goes on saying so while the count it is owed waits its turn"
    );
}

/// A query over a dataset still reading its footers still gets counted.
///
/// The pass is bringing the *dataset's* count, which is not the count of a query's
/// result — nobody else is going to take that one. Declining it because a pass is
/// out leaves the row count spinning for as long as the query is open, and `End`
/// saying it is counting rows while nothing is counting anything. It costs nothing
/// to take: a frame that is not the scan does not read footers for its count.
#[test]
fn a_query_over_a_dataset_still_reading_its_footers_is_counted() {
    use crate::widgets::datatable::{DataTableState, RemoteFiles};
    use crate::{App, OpenOptions};
    use polars::prelude::*;
    use std::sync::Arc;

    let rows = || df!("id" => (0..100i64).collect::<Vec<_>>()).unwrap().lazy();
    let mut lf = rows();
    let schema = Arc::new((*lf.collect_schema().unwrap()).clone());

    let mut state =
        DataTableState::from_schema_and_lazyframe(schema, rows(), &OpenOptions::default(), None)
            .unwrap()
            .with_open(crate::widgets::datatable::OpenFacts {
                remote_source: true,
                remote_files: Some(RemoteFiles {
                    urls: Arc::new(vec!["one".to_string()]),
                    scan: Arc::new(move |_u: &[String], _t: &[PlSmallStr]| Ok(rows())),
                    count: Arc::new(|_| Ok(vec![vec![100]])),
                    offsets: None,
                }),
                footers_pending: Some(Arc::new(|_| None)),
                ..Default::default()
            });
    state.visible_rows = 10;
    // Still reading, so as the scan it rightly declines to count itself.
    assert!(
        state.counts_itself_later(),
        "the pass is bringing this dataset's count"
    );

    let (tx, _rx) = std::sync::mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    app.install_for_tests(state, None, &OpenOptions::default(), None);

    // And then the user asks a question of it, whose answer has a count of its own.
    let state = app.data_table_state.as_mut().unwrap();
    state.deferred(|s| s.query("select doubled: id * 2".to_string()));
    assert!(
        !state.counts_itself_later(),
        "which is not the count the pass is bringing, and nothing else will take it"
    );

    app.spawn_async_collect(App::LOADING_BUFFER);
    assert!(
        app.len_count_inflight.is_some(),
        "so it is taken, rather than the row count spinning while the query is open"
    );
}

/// Columns found for the dataset before this one join nothing to this one.
///
/// Abandoning a load cancels nothing: the footers of a prefix the user has moved on
/// from keep being read, and land afterwards. Without a generation that counts
/// datasets put on screen they would be joined to whatever is there now — a directory
/// gaining a column from a different directory entirely.
#[test]
fn a_pass_from_the_dataset_before_this_one_joins_nothing_to_it() {
    use crate::widgets::datatable::DataTableState;
    use crate::{App, OpenOptions};
    use polars::prelude::*;
    use std::sync::Arc;

    let dataset_of = |lf: LazyFrame| {
        let mut lf = lf;
        let schema = Arc::new((*lf.collect_schema().unwrap()).clone());
        let footer = crate::schema_union::FileFooter {
            schema,
            row_group_rows: vec![1],
            file_bytes: 0,
            row_group_bytes: Vec::new(),
            column_bytes: Vec::new(),
        };
        crate::schema_union::union_sampled(1, &[0], &[Some(footer)])
    };
    let state_of = |lf: LazyFrame| {
        DataTableState::from_schema_and_lazyframe(
            dataset_of(lf.clone()).schema.clone(),
            lf,
            &OpenOptions::default(),
            None,
        )
        .unwrap()
    };

    let first = || df!("id" => &[1i64]).unwrap().lazy();
    let its_columns = || df!("id" => &[1i64], "oops" => &["a"]).unwrap().lazy();
    let second = || df!("other" => &[2i64]).unwrap().lazy();

    let (tx, rx) = std::sync::mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());

    // The first dataset, staged.
    let state = state_of(first()).with_open(crate::widgets::datatable::OpenFacts {
        footers_pending: Some(Arc::new(move |_progress| {
            Some(crate::widgets::datatable::FootersFound {
                estimate: None,
                dataset: dataset_of(its_columns()),
                lf: its_columns(),
                file_rows: Vec::new(),
                files: Vec::new(),
                row_groups: Vec::new(),
                remote: None,
            })
        })),
        ..Default::default()
    });
    app.install_for_tests(state, None, &OpenOptions::default(), None);
    let reported = rx
        .recv_timeout(std::time::Duration::from_secs(10))
        .expect("the first dataset's pass reports back");

    // The user opens something else before those columns arrive. Nothing here sets
    // the generation by hand: if opening a dataset does not move it, this test is
    // the one that notices.
    app.install_for_tests(state_of(second()), None, &OpenOptions::default(), None);

    let _ = app.handle(&reported);
    assert_eq!(
        app.data_table_state.as_ref().unwrap().get_column_order(),
        ["other"],
        "the directory on screen does not gain a column from the directory before it"
    );
    assert!(
        app.footers_held.is_none(),
        "and they are not kept waiting for a dataset that is gone"
    );
}

/// A footer pass that could not read them waits for work already asked for, too.
///
/// The failure branch re-reads for a different reason than the success branch — the
/// pass brought no count, so the dataset has to go and count itself the ordinary way
/// — but it goes through the same collect, and that collect bumps `task_generation`
/// just the same. It used to run on the spot, the one way into the collect that
/// asked nothing about what was already running: an export in its collect phase
/// never wrote its file and said nothing about it.
///
/// Revert `reread_owed` and this fails on the first assert: the generation moves
/// while the export is still waiting on it.
#[test]
fn a_pass_that_failed_waits_for_work_already_asked_for() {
    use crate::widgets::datatable::DataTableState;
    use crate::{App, AppEvent, OpenOptions};
    use polars::prelude::*;
    use std::sync::Arc;

    let frame = || df!("id" => &[1i64]).unwrap().lazy();
    let dataset_of = |lf: LazyFrame| {
        let mut lf = lf;
        let schema = Arc::new((*lf.collect_schema().unwrap()).clone());
        let footer = crate::schema_union::FileFooter {
            schema,
            row_group_rows: vec![1],
            file_bytes: 0,
            row_group_bytes: Vec::new(),
            column_bytes: Vec::new(),
        };
        crate::schema_union::union_sampled(1, &[0], &[Some(footer)])
    };

    let (tx, _rx) = std::sync::mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    let state = DataTableState::from_schema_and_lazyframe(
        dataset_of(frame()).schema.clone(),
        frame(),
        &OpenOptions::default(),
        None,
    )
    .unwrap();
    app.install_for_tests(state, None, &OpenOptions::default(), None);

    // An export is collecting: it holds a lease on this exact generation, and its
    // answer is thrown away if anything bumps it.
    app.export_progress = Some(crate::ExportProgress {
        file_path: std::path::PathBuf::from("/tmp/out.csv"),
        current_phase: "Collecting".to_string(),
        written: None,
    });
    let lease = app.hold_the_generation();
    let waiting_on = app.task_generation();

    // The pass comes back empty-handed for the dataset on screen.
    let live = app.dataset_generation;
    App::record_footers(&app.pending_footers_result, live, None);
    let _ = app.handle(&AppEvent::BackgroundFootersJoined { generation: live });

    assert_eq!(
        app.task_generation(),
        waiting_on,
        "the export is still waiting on the answer this app would have thrown away"
    );
    assert_eq!(
        app.reread_owed,
        Some(live),
        "and the re-read the dataset is owed is remembered, not dropped"
    );

    // The export finishes, and the errand gets its turn on the next event.
    app.export_progress = None;
    drop(lease);
    let _ = app.handle(&AppEvent::Update);
    let _ = app.handle(&AppEvent::Update);

    assert!(
        app.task_generation() != waiting_on,
        "the dataset gets the collect it was owed once nothing is waiting on the \
         generation — without it, it never counts itself at all"
    );
    assert!(
        app.reread_owed.is_none(),
        "and the errand is done rather than run again on every event"
    );
}

/// Columns arriving during work already asked for wait for it, rather than
/// cancelling it.
///
/// The re-read after a join goes through the ordinary collect, which bumps
/// `task_generation` — the token the export is waiting on. Bumped underneath one,
/// the export's own answer is thrown away when it arrives: in its collect phase
/// that means the file is never written and nothing is said about it. The pass runs
/// for minutes on the prefixes this is for, so an export started at the open is
/// certain to be inside that window.
#[test]
fn columns_arriving_during_work_already_asked_for_wait_for_it() {
    use crate::widgets::datatable::{DataTableState, FootersFound};
    use crate::{App, AppEvent, OpenOptions};
    use polars::prelude::*;
    use std::sync::Arc;

    let frame = || df!("id" => &[1i64]).unwrap().lazy();
    let wider = || df!("id" => &[1i64], "oops" => &["a"]).unwrap().lazy();
    let dataset_of = |lf: LazyFrame| {
        let mut lf = lf;
        let schema = Arc::new((*lf.collect_schema().unwrap()).clone());
        let footer = crate::schema_union::FileFooter {
            schema,
            row_group_rows: vec![1],
            file_bytes: 0,
            row_group_bytes: Vec::new(),
            column_bytes: Vec::new(),
        };
        crate::schema_union::union_sampled(1, &[0], &[Some(footer)])
    };

    let (tx, _rx) = std::sync::mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    let state = DataTableState::from_schema_and_lazyframe(
        dataset_of(frame()).schema.clone(),
        frame(),
        &OpenOptions::default(),
        None,
    )
    .unwrap();
    app.install_for_tests(state, None, &OpenOptions::default(), None);

    // Work whose answer the join would throw away. A lease stands for all of it —
    // an open, an export, an analysis — which is the point: the decision is no
    // longer a list of the kinds that happen to exist today. The chart is here
    // beside it because it is the one that a bump would *not* strand: it is
    // prepared against the frame, and the join takes a fresh one of those too.
    type Start = fn(&mut App) -> Option<crate::jobs::Hold>;
    let under_way: Vec<(&str, Start)> = vec![
        ("leased background work", |app: &mut App| {
            Some(app.hold_the_generation())
        }),
        ("a chart", |app: &mut App| {
            let mut modal = crate::chart_modal::ChartModal::new();
            modal.spec.encoding.x.field = Some("id".to_string());
            app.chart_inflight = Some(crate::ChartInflight {
                dataset: None,
                request: crate::ChartRequest::from_modal(&modal).expect("an x range"),
                stale: false,
                cancel: Default::default(),
            });
            None
        }),
    ];
    let put_away = |app: &mut App, lease: Option<crate::jobs::Hold>| {
        // The hold is released by dropping it; the event after it lets the errands in.
        drop(lease);
        let _ = app.handle(&AppEvent::Update);
        app.chart_inflight = None;
    };

    for (what, start) in under_way {
        let lease = start(&mut app);
        let waiting_on = app.task_generation();
        app.footers_held = Some((
            app.dataset_generation,
            FootersFound {
                estimate: None,
                dataset: dataset_of(wider()),
                lf: wider(),
                file_rows: Vec::new(),
                files: Vec::new(),
                row_groups: Vec::new(),
                remote: None,
            },
        ));
        let _ = app.handle(&AppEvent::Update);

        assert_eq!(
            app.task_generation(),
            waiting_on,
            "{what} is still waiting on the answer this app would have thrown away"
        );
        assert!(
            app.footers_held.is_some(),
            "and the columns wait their turn behind {what}"
        );
        put_away(&mut app, lease);
    }

    // Nothing under way now, and they go in.
    let _ = app.handle(&AppEvent::Update);
    assert_eq!(
        app.data_table_state
            .as_ref()
            .unwrap()
            .get_column_order()
            .last()
            .map(String::as_str),
        Some("oops"),
        "once nothing is waiting on an answer, the columns join"
    );
}

/// Columns held while the user was in a query are not joined to the next dataset.
///
/// Held columns outlive the dataset they belong to: the user is inside a query when
/// they arrive, so they wait — and the user may then open something else entirely
/// rather than clear the query. Opening clears the slot but not what is already
/// held, and the first keypress on the new directory is where the held columns would
/// go in: one directory's schema, scan and file list installed into another.
#[test]
fn columns_held_for_one_dataset_are_not_given_to_the_next() {
    use crate::widgets::datatable::{DataTableState, FootersFound};
    use crate::{App, AppEvent, OpenOptions};
    use polars::prelude::*;
    use std::sync::Arc;

    let first = || df!("id" => &[1i64]).unwrap().lazy();
    let its_columns = || df!("id" => &[1i64], "oops" => &["a"]).unwrap().lazy();
    let second = || df!("other" => &[2i64]).unwrap().lazy();
    let dataset_of = |lf: LazyFrame| {
        let mut lf = lf;
        let schema = Arc::new((*lf.collect_schema().unwrap()).clone());
        let footer = crate::schema_union::FileFooter {
            schema,
            row_group_rows: vec![1],
            file_bytes: 0,
            row_group_bytes: Vec::new(),
            column_bytes: Vec::new(),
        };
        crate::schema_union::union_sampled(1, &[0], &[Some(footer)])
    };
    let state_of = |lf: LazyFrame| {
        DataTableState::from_schema_and_lazyframe(
            dataset_of(lf.clone()).schema.clone(),
            lf,
            &OpenOptions::default(),
            None,
        )
        .unwrap()
    };

    let (tx, _rx) = std::sync::mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    app.install_for_tests(state_of(first()), None, &OpenOptions::default(), None);

    // The user is in a query when this dataset's columns arrive, so they wait.
    app.data_table_state
        .as_mut()
        .unwrap()
        .query("select doubled: id * 2".to_string());
    app.footers_held = Some((
        app.dataset_generation,
        FootersFound {
            estimate: None,
            dataset: dataset_of(its_columns()),
            lf: its_columns(),
            file_rows: Vec::new(),
            files: Vec::new(),
            row_groups: Vec::new(),
            remote: None,
        },
    ));
    let _ = app.handle(&AppEvent::Update);
    assert!(app.footers_held.is_some(), "waiting, as they should be");

    // And instead of clearing the query, the user opens something else.
    app.install_for_tests(state_of(second()), None, &OpenOptions::default(), None);
    let _ = app.handle(&AppEvent::Update);

    assert_eq!(
        app.data_table_state.as_ref().unwrap().get_column_order(),
        ["other"],
        "the directory now on screen is not given the last one's columns"
    );
    assert!(
        app.footers_held.is_none(),
        "and they are let go rather than waiting on for a third dataset"
    );
}

/// The event that wakes the app does not decide whose answer is in the slot.
///
/// Two passes run at once when a second large prefix is opened, and the newer one
/// can overwrite the slot before the older one's event is handled. Deciding by the
/// event would drain the newer answer and throw it away on the older event's
/// generation — and the newer event, arriving next, would find the slot empty. The
/// dataset on screen would wait for columns that had already been and gone, with
/// nothing to say so: the pass is over, so even the count in the bar is silent.
#[test]
fn a_late_event_from_an_old_pass_does_not_throw_away_the_live_answer() {
    use crate::widgets::datatable::{DataTableState, FootersFound};
    use crate::{App, AppEvent, OpenOptions};
    use polars::prelude::*;
    use std::sync::Arc;

    let frame = || df!("id" => &[1i64]).unwrap().lazy();
    let wider = || df!("id" => &[1i64], "oops" => &["a"]).unwrap().lazy();
    let dataset_of = |lf: LazyFrame| {
        let mut lf = lf;
        let schema = Arc::new((*lf.collect_schema().unwrap()).clone());
        let footer = crate::schema_union::FileFooter {
            schema,
            row_group_rows: vec![1],
            file_bytes: 0,
            row_group_bytes: Vec::new(),
            column_bytes: Vec::new(),
        };
        crate::schema_union::union_sampled(1, &[0], &[Some(footer)])
    };

    let (tx, _rx) = std::sync::mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    let state = DataTableState::from_schema_and_lazyframe(
        dataset_of(frame()).schema.clone(),
        frame(),
        &OpenOptions::default(),
        None,
    )
    .unwrap();
    app.install_for_tests(state, None, &OpenOptions::default(), None);

    // This dataset's own pass has finished and put its answer in the slot.
    let live = app.dataset_generation;
    App::record_footers(
        &app.pending_footers_result,
        live,
        Some(FootersFound {
            estimate: None,
            dataset: dataset_of(wider()),
            lf: wider(),
            file_rows: Vec::new(),
            files: Vec::new(),
            row_groups: Vec::new(),
            remote: None,
        }),
    );

    // And the event that reaches the loop first belongs to the prefix the user
    // opened before this one, whose pass was slower.
    let _ = app.handle(&AppEvent::BackgroundFootersJoined {
        generation: live.wrapping_sub(1),
    });

    assert_eq!(
        app.data_table_state
            .as_ref()
            .unwrap()
            .get_column_order()
            .last()
            .map(String::as_str),
        Some("oops"),
        "the answer in the slot is this dataset's, and it is the one that is used"
    );
}

/// A slower pass from an older dataset does not displace a newer one's answer.
///
/// Opening a second large prefix does not stop the first one reading, so two passes
/// can be in flight and finish in either order. If the older one wrote last, the
/// generation in the slot would disagree with the generation on the event and both
/// would be thrown away — leaving the dataset on screen permanently short of the
/// columns its own pass had already found.
#[test]
fn an_older_pass_finishing_late_does_not_displace_a_newer_one() {
    use crate::App;
    use polars::prelude::*;
    use std::sync::{Arc, Mutex};

    let found = |name: &str| {
        let mut lf = df!(name => &[1i64]).unwrap().lazy();
        let schema = Arc::new((*lf.collect_schema().unwrap()).clone());
        let footer = crate::schema_union::FileFooter {
            schema,
            row_group_rows: vec![1],
            file_bytes: 0,
            row_group_bytes: Vec::new(),
            column_bytes: Vec::new(),
        };
        crate::widgets::datatable::FootersFound {
            estimate: None,
            dataset: crate::schema_union::union_sampled(1, &[0], &[Some(footer)]),
            lf,
            file_rows: Vec::new(),
            files: Vec::new(),
            row_groups: Vec::new(),
            remote: None,
        }
    };
    let name_in = |slot: &Mutex<Option<(u64, Option<crate::widgets::datatable::FootersFound>)>>| {
        slot.lock().unwrap().as_ref().map(|(g, f)| {
            let f = f.as_ref().expect("recorded with something in it");
            (
                *g,
                f.dataset.schema.iter_names().next().unwrap().to_string(),
            )
        })
    };

    let slot = Mutex::new(None);
    assert!(
        App::record_footers(&slot, 7, Some(found("newer"))),
        "the newer pass answers first"
    );
    assert!(
        !App::record_footers(&slot, 6, Some(found("older"))),
        "and the older one, finishing after it, is turned away"
    );
    assert_eq!(
        name_in(&slot),
        Some((7, "newer".to_string())),
        "so what is waiting is still the newer dataset's"
    );

    // The ordinary case is unaffected: a pass for the dataset now on screen goes in
    // over whatever an abandoned one left behind.
    assert!(
        App::record_footers(&slot, 8, Some(found("newest"))),
        "a later dataset's pass takes the slot"
    );
    assert_eq!(name_in(&slot), Some((8, "newest".to_string())));
}

/// Columns that arrive while the user is inside a query wait for them to leave it.
///
/// The scan a query is built on is not the frame on screen: rebuilding it wider
/// underneath takes away the columns the query named, and the table goes to a
/// Polars "unable to find column" where a moment ago there were rows. So the
/// columns are held, and get in when the view comes back to the data.
#[test]
fn columns_arriving_under_a_query_wait_rather_than_break_it() {
    use crate::widgets::datatable::DataTableState;
    use crate::{App, AppEvent, OpenOptions};
    use polars::prelude::*;
    use std::sync::Arc;

    let frame = || df!("id" => &[1i64, 2], "v" => &[10i64, 20]).unwrap().lazy();
    let wider = || {
        df!("id" => &[1i64, 2], "v" => &[10i64, 20], "oops" => &["a", "b"])
            .unwrap()
            .lazy()
    };
    let dataset_of = |lf: LazyFrame| {
        let mut lf = lf;
        let schema = Arc::new((*lf.collect_schema().unwrap()).clone());
        let footer = crate::schema_union::FileFooter {
            schema,
            row_group_rows: vec![2],
            file_bytes: 0,
            row_group_bytes: Vec::new(),
            column_bytes: Vec::new(),
        };
        crate::schema_union::union_sampled(1, &[0], &[Some(footer)])
    };

    let (tx, _rx) = std::sync::mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    let state = DataTableState::from_schema_and_lazyframe(
        dataset_of(frame()).schema.clone(),
        frame(),
        &OpenOptions::default(),
        None,
    )
    .unwrap();
    app.install_for_tests(state, None, &OpenOptions::default(), None);

    // The user asks a question of the two columns that are there.
    let table = app.data_table_state.as_mut().unwrap();
    table.query("select doubled: v * 2".to_string());
    assert!(
        table.error().is_none(),
        "the query runs: {:?}",
        table.error()
    );
    let asked = table.get_column_order().to_vec();

    // And the rest of the footers land underneath it.
    let generation = app.dataset_generation;
    app.footers_held = Some((
        generation,
        crate::widgets::datatable::FootersFound {
            estimate: None,
            dataset: dataset_of(wider()),
            lf: wider(),
            file_rows: Vec::new(),
            files: Vec::new(),
            row_groups: Vec::new(),
            remote: None,
        },
    ));
    let _ = app.handle(&AppEvent::Update);

    let table = app.data_table_state.as_ref().unwrap();
    assert_eq!(
        table.get_column_order(),
        asked.as_slice(),
        "the query's own columns are still what is on screen"
    );
    assert!(
        table.error().is_none(),
        "and it has not been broken out from under: {:?}",
        table.error()
    );
    assert!(
        app.footers_held.is_some(),
        "the columns are kept, not thrown away"
    );

    // The user clears the query — an empty one is how that is said — and now they
    // can get in.
    app.data_table_state.as_mut().unwrap().query(String::new());
    let _ = app.handle(&AppEvent::Update);
    assert_eq!(
        app.data_table_state
            .as_ref()
            .unwrap()
            .get_column_order()
            .last()
            .map(String::as_str),
        Some("oops"),
        "the columns join once the view is back on the data"
    );
    assert!(app.footers_held.is_none(), "with nothing left waiting");
}

/// A dataset that opened from two footers gets the rest, through the app.
///
/// The cloud test covers the pass itself; this covers everything between it and the
/// screen — that the app starts it without marking itself busy, that what it finds
/// reaches the dataset on screen, and that a pass belonging to a dataset the user
/// has since left cannot join its columns to the one that replaced it.
#[test]
fn a_staged_open_joins_what_its_footers_found() {
    use crate::widgets::datatable::DataTableState;
    use crate::{App, AppEvent, OpenOptions};
    use polars::prelude::*;
    use std::sync::Arc;

    let frame = || df!("id" => &[1i64, 2], "v" => &[10i64, 20]).unwrap().lazy();
    let wider = || {
        df!("id" => &[1i64, 2], "v" => &[10i64, 20], "oops" => &["a", "b"])
            .unwrap()
            .lazy()
    };
    let dataset_of = |lf: LazyFrame| {
        let mut lf = lf;
        let schema = Arc::new((*lf.collect_schema().unwrap()).clone());
        let footer = crate::schema_union::FileFooter {
            schema,
            row_group_rows: vec![2],
            file_bytes: 0,
            row_group_bytes: Vec::new(),
            column_bytes: Vec::new(),
        };
        crate::schema_union::union_sampled(1, &[0], &[Some(footer)])
    };

    // The wider frame counts every time it is read, so the join can be asked
    // whether it read the data — which, on the thread drawing the screen and
    // against a dataset in a bucket, is the one thing it must not do.
    let reads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counted = {
        let reads = reads.clone();
        move || {
            let reads = reads.clone();
            wider().with_column(col("id").map(
                move |s| {
                    reads.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    Ok(s)
                },
                |_schema: &Schema, field: &Field| Ok(field.clone()),
            ))
        }
    };

    let (tx, rx) = std::sync::mpsc::channel();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let mut app = App::new(tx, runtime.handle().clone());
    let state = DataTableState::from_schema_and_lazyframe(
        dataset_of(frame()).schema.clone(),
        frame(),
        &OpenOptions::default(),
        None,
    )
    .unwrap()
    .with_open(crate::widgets::datatable::OpenFacts {
        // What the pass behind the open will find: one column more.
        footers_pending: Some(Arc::new(move |_progress| {
            Some(crate::widgets::datatable::FootersFound {
                estimate: None,
                dataset: dataset_of(counted()),
                lf: counted(),
                file_rows: Vec::new(),
                files: Vec::new(),
                row_groups: Vec::new(),
                remote: None,
            })
        })),
        ..Default::default()
    });
    // Installed the way an open installs it, rather than dropped into the field:
    // handing the pass over is one line of `install_dataset`, and a test that
    // starts the pass itself would not notice that line going missing.
    app.install_for_tests(state, None, &OpenOptions::default(), None);
    assert!(
        !app.is_busy(),
        "the dataset is on screen and must keep working while the rest are read"
    );
    let joined = rx
        .recv_timeout(std::time::Duration::from_secs(10))
        .expect("the pass reports back");
    let AppEvent::BackgroundFootersJoined { generation } = joined else {
        panic!("expected the footers to be reported, got another event");
    };

    // It lands after a glance at the home screen, which leaves the dataset up and
    // puts down whatever open was in flight. One keystroke there and back must not
    // strand the dataset on two footers for the rest of the session.
    app.abandon_load();
    // The re-read runs off this thread and would count too, as soon as it runs. It
    // dies before it reads, so what is counted below is this thread's alone.
    app.jobs.worker_dies = crate::tests::worker_dies_once(|job| matches!(job, crate::Job::Rows(_)));
    let _ = app.handle(&AppEvent::BackgroundFootersJoined { generation });
    // Read again, not asked to be read again. The join drops the buffer, so a
    // request that goes on to be ignored — as a step of the open's chain is, once
    // the load is over — leaves the table with nothing to show at the moment it was
    // to show more.
    assert!(
        app.rows_in_flight().is_some(),
        "the rows on screen were read through the narrow frame and are read again"
    );
    assert_eq!(
        app.data_table_state
            .as_ref()
            .unwrap()
            .get_column_order()
            .last()
            .map(String::as_str),
        Some("oops"),
        "the column the pass found joins the dataset on screen"
    );
    assert_eq!(
        reads.load(std::sync::atomic::Ordering::Relaxed),
        0,
        "and nothing was read to do it: this runs on the thread drawing the frame, \
         and against a bucket the read it would do is a `len()` over every file"
    );
}

/// Only one query type is returned; SQL overrides fuzzy over DSL. Used when saving views.
#[test]
fn test_active_query_settings_only_one_set() {
    use super::active_query_settings;

    let (q, sql, fuzzy) = active_query_settings("", "", "");
    assert!(q.is_none() && sql.is_none() && fuzzy.is_none());

    let (q, sql, fuzzy) = active_query_settings("select a", "SELECT 1", "foo");
    assert!(q.is_none() && sql.as_deref() == Some("SELECT 1") && fuzzy.is_none());

    let (q, sql, fuzzy) = active_query_settings("select a", "", "foo bar");
    assert!(q.is_none() && sql.is_none() && fuzzy.as_deref() == Some("foo bar"));

    let (q, sql, fuzzy) = active_query_settings("  select a  ", "", "");
    assert!(q.as_deref() == Some("select a") && sql.is_none() && fuzzy.is_none());
}

mod export_format_tests;

mod quality_memory_tests;

mod probe_slot_tests;

/// #455: a home-screen worker that panics still answers, so what marks it in flight
/// stops waiting and the next request is made.
#[cfg(feature = "cloud")]
mod cloud_row_tests;

mod home_worker_panic_tests;

mod classify_batch_tests;

mod quality_sample_tests;

mod chart_prepare_tests;

mod view_rollback_tests;

#[cfg(feature = "sql")]
mod view_matching_tests;

#[cfg(feature = "cloud")]
mod peek_answer_tests;

#[cfg(feature = "cloud")]
mod cloud_csv_prefix_tests;

mod feedback_ladder_tests;

mod sort_filter_sync_tests;

/// A change to the view made from a key or an event is planned on the UI thread and
/// its rows are read in the background, as every other is (#458).
mod background_read_tests;

mod read_mode_tests;

mod spec_source_tests;

mod file_facts_tests;

/// What the home screen reads from the cache is read on a worker: a cache file that
/// never finishes opening (a FIFO with no writer, which is what a stalled mount looks
/// like to `open`) leaves the screen drawn and the keys working, and the listing comes
/// in when the read does.
#[cfg(unix)]
mod startup_reads_tests;

/// The row inspector's reads of the fields the buffer does not hold.
mod inspector_tests;

/// The inspector's layout at the sizes the canon names.
mod inspector_layout_tests;

/// The docs' queries parse, and run on the datasets they name (#683).
#[cfg(feature = "sql")]
mod doc_queries_tests;

/// A catalog dataset's codebook in the Info panel and the inspector (#734).
mod codebook_tests;

//! Cloud discovery and listing, against real object stores.
//!
//! Every test here is `#[ignore]`d and gated on an environment variable. They talk to
//! services over the network, using whatever credentials the machine has, and neither
//! of those belongs in a run of `cargo test`. CI would either skip them, which is a
//! test that lies, or need credentials, which is a secret in a workflow for the sake of
//! a developer convenience.
//!
//! Run them with `--test-threads=1`. They share one cache directory and one process, so
//! an object opened by one test is in Recent for the next, and assertions that say "some
//! section contains this row" are satisfied by the wrong section. Each test here names
//! the section it means for that reason.
//!
//! They exist because the alternative is worse. The code they cover signs requests and
//! parses responses from services that cannot be usefully mocked: a fake that agrees
//! with my reading of the S3 signing rules proves only that I am self-consistent. These
//! were run against a live MinIO and a live Google Cloud Storage project before the
//! feature was merged, and they are committed so the next person can do the same
//! without reconstructing the setup.
//!
//! # Google Cloud Storage
//!
//! ```bash
//! gcloud auth application-default login
//! DATUI_LIVE_GCS=1 cargo test --test cloud_live_test -- --ignored --nocapture
//! ```
//!
//! # MinIO, or anything else speaking S3
//!
//! ```bash
//! docker run -d --name datui-minio -p 127.0.0.1:9000:9000 \
//!   -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
//!   quay.io/minio/minio server /data
//! docker run --rm --network host --entrypoint sh quay.io/minio/mc -c "
//!   mc alias set local http://127.0.0.1:9000 minioadmin minioadmin
//!   mc mb -p local/datui-sales local/datui-logs
//!   printf 'id,region,amount\n1,north,10\n' > /tmp/sales.csv
//!   mc cp /tmp/sales.csv local/datui-sales/top-level.csv
//!   mc cp /tmp/sales.csv local/datui-sales/2024/january.csv"
//! DATUI_LIVE_S3=http://127.0.0.1:9000 cargo test --test cloud_live_test -- --ignored --nocapture
//! ```

#![cfg(feature = "cloud")]

mod common;

use datui::cloud_browse::{self, Environment, ProviderKind};
use datui::cloud_sources;
use datui::config::CloudConfig;

/// The MinIO credentials the documented container runs with. Not a secret in any sense:
/// they are the defaults printed in MinIO's own quick-start, and this only ever points
/// at a container on the loopback interface.
const MINIO_KEY: &str = "minioadmin";
const MINIO_SECRET: &str = "minioadmin";

fn minio_config(endpoint: &str) -> CloudConfig {
    CloudConfig {
        s3_endpoint_url: Some(endpoint.to_string()),
        s3_access_key_id: Some(MINIO_KEY.to_string()),
        s3_secret_access_key: Some(MINIO_SECRET.to_string()),
        s3_region: Some("us-east-1".to_string()),
        ..CloudConfig::default()
    }
}

#[test]
#[ignore = "talks to Google Cloud Storage; set DATUI_LIVE_GCS=1"]
fn gcs_is_discovered_and_its_buckets_listed() {
    if std::env::var("DATUI_LIVE_GCS").is_err() {
        eprintln!("skipped: set DATUI_LIVE_GCS=1 to run");
        return;
    }

    let config = CloudConfig::default();
    let env = Environment::current();
    let sources = cloud_sources::discover(&config, &env);
    let gcs = sources
        .iter()
        .find(|p| p.kind == ProviderKind::Gcs)
        .expect("GCS should be discovered after `gcloud auth application-default login`");

    println!("source: {} ({})", gcs.label, gcs.origin);
    println!("project: {:?}", gcs.project);
    assert!(
        gcs.project.is_some(),
        "a logged-in gcloud writes quota_project_id, so a project should have been found"
    );

    let runtime = common::test_runtime();
    let buckets = runtime
        .block_on(cloud_browse::list_buckets(gcs))
        .expect("listing buckets");
    println!("buckets: {buckets:?}");
    assert!(
        !buckets.is_empty(),
        "the project has no buckets, so this proves nothing; create one and re-run"
    );

    // Listing the first bucket exercises the other half: a delimited listing turning
    // objects and prefixes into home-screen rows.
    let url = format!("gs://{}", buckets[0]);
    let rows = runtime
        .block_on(cloud_browse::list_objects(&url, &config))
        .expect("listing objects");
    println!("{} holds {} rows at the top level", url, rows.len());
    for row in rows.iter().take(10) {
        println!("  {:?} {} {:?}", row.kind, row.name, row.size);
    }
    for row in &rows {
        assert!(
            row.path.to_string_lossy().starts_with(&url),
            "every row should be addressable by the URL it came from: {:?}",
            row.path
        );
    }
}

#[test]
#[ignore = "talks to a local MinIO; set DATUI_LIVE_S3=http://127.0.0.1:9000"]
fn minio_is_discovered_and_its_buckets_listed() {
    let Ok(endpoint) = std::env::var("DATUI_LIVE_S3") else {
        eprintln!("skipped: set DATUI_LIVE_S3 to an endpoint to run");
        return;
    };

    let config = minio_config(&endpoint);
    let env = Environment::current();
    let sources = cloud_sources::discover(&config, &env);
    let s3 = sources
        .iter()
        .find(|p| p.kind == ProviderKind::S3)
        .expect("configured keys should be enough to discover S3");

    println!("source: {} ({})", s3.label, s3.origin);
    assert!(
        s3.label.contains("S3-compatible"),
        "a custom endpoint should not be labelled Amazon: {}",
        s3.label
    );

    let runtime = common::test_runtime();
    let buckets = runtime
        .block_on(cloud_browse::list_buckets(s3))
        .expect("listing buckets");
    println!("buckets: {buckets:?}");
    assert!(
        buckets.contains(&"datui-sales".to_string()),
        "expected the documented test bucket; got {buckets:?}"
    );

    let rows = runtime
        .block_on(cloud_browse::list_objects("s3://datui-sales", &config))
        .expect("listing objects");
    for row in &rows {
        println!("  {:?} {} {:?}", row.kind, row.name, row.size);
    }

    // A prefix and an object at the same level, which is the case a listing has to get
    // right: one is somewhere to descend into and the other is something to open.
    let names: Vec<&str> = rows.iter().map(|r| r.name.as_str()).collect();
    assert!(names.contains(&"top-level.csv"), "got {names:?}");
    assert!(names.contains(&"2024"), "got {names:?}");

    let prefix = rows
        .iter()
        .find(|r| r.name == "2024")
        .expect("the prefix row");
    assert_eq!(prefix.kind, datui::discover::EntryKind::Directory);
    let object = rows
        .iter()
        .find(|r| r.name == "top-level.csv")
        .expect("the object row");
    assert_eq!(object.kind, datui::discover::EntryKind::File);
    assert!(object.size.unwrap_or(0) > 0, "an object should have a size");

    // Descending, which is what pressing Enter on the prefix will do.
    let nested = runtime
        .block_on(cloud_browse::list_objects(
            &prefix.path.to_string_lossy(),
            &config,
        ))
        .expect("listing the prefix");
    let nested_names: Vec<&str> = nested.iter().map(|r| r.name.as_str()).collect();
    assert!(
        nested_names.contains(&"january.csv"),
        "got {nested_names:?}"
    );
    assert!(
        !nested_names.iter().any(|n| n.contains('/')),
        "a delimited listing returns leaf names, not paths: {nested_names:?}"
    );
}

/// Drive the app the way the main loop does, until `done` or the deadline.
///
/// Cloud work lands by event from a worker, so nothing here can be asserted
/// synchronously after a keypress. The alternative to pumping is sleeping for a fixed
/// interval and hoping, which is the shape of a test that fails one run in twenty.
/// Feed an event and everything it leads to, reporting any crash on the way.
///
/// `App::event` returns the next link in a load chain; crashes are handled by the main
/// loop rather than by `event`, so a test that only feeds events back in watches a
/// failure disappear and then asserts against a screen that never changed.
fn drive(app: &mut datui::App, first: datui::AppEvent) -> Option<String> {
    let mut next = Some(first);
    let mut crash = None;
    while let Some(event) = next {
        if let datui::AppEvent::Crash(message) = &event {
            crash = Some(message.clone());
        }
        next = app.event(&event);
    }
    crash
}

fn pump_until(
    app: &mut datui::App,
    rx: &std::sync::mpsc::Receiver<datui::AppEvent>,
    seconds: u64,
    done: impl Fn(&datui::App) -> bool,
) -> bool {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(seconds);
    while std::time::Instant::now() < deadline {
        if done(app) {
            return true;
        }
        while let Ok(event) = rx.try_recv() {
            match &event {
                datui::AppEvent::Crash(message) => eprintln!("crash: {message}"),
                datui::AppEvent::BackgroundError { message, .. } => {
                    eprintln!("background error: {message}")
                }
                datui::AppEvent::BackgroundDownloadReady { temp_path, .. } => {
                    eprintln!("download ready: {temp_path:?}")
                }
                datui::AppEvent::BackgroundLazyFrameReady { path, .. } => {
                    eprintln!("lazyframe ready: {path:?}")
                }
                _ => {}
            }
            // Follow the whole chain, not one link of it. A load is a sequence of
            // events, each returned by the handler of the last, and stopping after one
            // leaves the dataset built but never installed — which looks from the
            // outside exactly like a load that failed without saying so.
            if let Some(crash) = drive(app, event) {
                eprintln!("crash: {crash}");
            }
        }
        // A frame, then the ask that follows one, because that is the loop this stands
        // in for. What is worth measuring, classifying or peeking into is decided from
        // what the last frame drew — a pump that only drains events models an app whose
        // window is never painted, where none of those passes ever runs.
        draw_a_frame(app);
        app.request_what_the_frame_needs();
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    done(app)
}

/// Render off-screen, for the passes that read what the last frame drew.
fn draw_a_frame(app: &mut datui::App) {
    let area = ratatui::layout::Rect::new(0, 0, 120, 30);
    let mut buf = ratatui::buffer::Buffer::empty(area);
    ratatui::widgets::Widget::render(app, area, &mut buf);
}

fn key(code: crossterm::event::KeyCode) -> datui::AppEvent {
    datui::AppEvent::Key(crossterm::event::KeyEvent::new(
        code,
        crossterm::event::KeyModifiers::NONE,
    ))
}

/// An app pointed at the MinIO endpoint, on the home screen.
fn minio_app(endpoint: &str) -> (datui::App, std::sync::mpsc::Receiver<datui::AppEvent>) {
    common::isolate_cache();
    let mut config = datui::config::AppConfig {
        cloud: minio_config(endpoint),
        ..Default::default()
    };
    // Nothing here should depend on what is in this checkout or on this desktop.
    config.data.use_desktop_recents = false;

    let (tx, rx) = std::sync::mpsc::channel();
    let mut app = datui::App::new_with_config(
        tx,
        common::test_runtime(),
        datui::Theme {
            colors: std::collections::HashMap::new(),
        },
        config,
    );
    app.enter_home();
    (app, rx)
}

/// The section with this exact title, if the listing has one.
///
/// Tests name the section they mean rather than searching every section for a row.
/// Once one of these tests has opened an object, that object is in Recent, so "some
/// section contains top-level.csv" is satisfied before the bucket has even answered.
fn section_named<'a>(app: &'a datui::App, title: &str) -> Option<&'a datui::home::Section> {
    app.home.sections.iter().find(|s| s.title == title)
}

/// Wait for a source's buckets, then step into it from the `CLOUD` section the way a
/// user does: cursor on its row, Enter.
fn enter_source(
    app: &mut datui::App,
    rx: &std::sync::mpsc::Receiver<datui::AppEvent>,
    id: &str,
) -> bool {
    let listed = pump_until(app, rx, 30, |app| {
        app.home
            .cloud
            .iter()
            .any(|s| s.id == id && !s.buckets.is_empty())
    });
    if !listed {
        return false;
    }
    let Some(label) = app
        .home
        .cloud
        .iter()
        .find(|s| s.id == id)
        .map(|s| s.label.clone())
    else {
        return false;
    };
    // The listing is rebuilt on a worker, so the row can lag the source by a frame.
    let shown = pump_until(app, rx, 10, |app| {
        app.home
            .visible()
            .iter()
            .any(|row| matches!(row, datui::home::Row::Entry { entry, .. } if entry.name == label))
    });
    if !shown || !select_row(app, &label) {
        return false;
    }
    app.event(&key(crossterm::event::KeyCode::Enter));
    pump_until(app, rx, 10, |app| {
        section_named(app, &label).is_some_and(|s| !s.rows.is_empty())
    })
}

/// Put the cursor on the visible row with this name, and say whether it was found.
///
/// The `(all files)` row counts: it is a row on screen with a name, drawn like any
/// other. What it is not is one of its section's `rows` — it is `Row::Door`, because
/// its path is the directory's and a path-keyed map cannot tell the two apart.
fn select_row(app: &mut datui::App, name: &str) -> bool {
    for (index, row) in app.home.visible().iter().enumerate() {
        if let datui::home::Row::Entry { entry, .. } | datui::home::Row::Door { entry, .. } = row
            && entry.name == name
        {
            app.home.selected = index;
            return true;
        }
    }
    false
}

#[test]
#[ignore = "drives the home screen against a local MinIO; set DATUI_LIVE_S3"]
fn the_home_screen_lists_buckets_and_descends_into_one() {
    let Ok(endpoint) = std::env::var("DATUI_LIVE_S3") else {
        eprintln!("skipped: set DATUI_LIVE_S3 to an endpoint to run");
        return;
    };
    let (mut app, rx) = minio_app(&endpoint);

    // The source is a row under CLOUD; its buckets are one Enter down.
    assert!(
        enter_source(&mut app, &rx, "s3-default"),
        "the default S3 source should list and open: {:?}",
        app.home.cloud
    );

    let section = section_named(&app, "S3-compatible").expect("the source's buckets");
    let names: Vec<&str> = section.rows.iter().map(|r| r.name.as_str()).collect();
    println!(
        "section {:?} subtitle {:?}",
        section.title, section.subtitle
    );
    println!("buckets on screen: {names:?}");
    assert!(names.contains(&"datui-sales"), "got {names:?}");
    assert!(
        names.contains(&"datui-empty"),
        "an empty bucket is still a bucket"
    );
    let host = endpoint
        .split_once("://")
        .map_or(endpoint.as_str(), |(_, h)| h);
    assert!(
        section
            .subtitle
            .as_deref()
            .is_some_and(|s| s.contains(host)),
        "the note names the endpoint: {:?}",
        section.subtitle
    );

    // Every bucket is a row to step into, never expanded in place, and never measured.
    // Expanding them would be a billed request per bucket on every start, and measuring
    // one would mean reading object bytes to fill in a column nobody asked for.
    for row in &section.rows {
        assert_eq!(row.kind, datui::discover::EntryKind::Directory);
        assert!(
            row.rows.is_none(),
            "a bucket row must not carry a row count"
        );
        assert!(row.cols.is_none(), "nor a column count");
    }

    // The fuzzy filter is the one recents use, and it reaches cloud rows because they
    // are ordinary rows in an ordinary section rather than a bespoke widget.
    app.home.filter = "sales".to_string();
    let visible: Vec<String> = app
        .home
        .visible()
        .iter()
        .filter_map(|row| match row {
            datui::home::Row::Entry { entry, .. } => Some(entry.name.clone()),
            _ => None,
        })
        .collect();
    assert!(
        visible.iter().any(|n| n == "datui-sales"),
        "filtering should match a bucket by name; got {visible:?}"
    );
    assert!(
        !visible.iter().any(|n| n == "datui-logs"),
        "and should exclude the others; got {visible:?}"
    );

    // Enter on the bucket, through the real key handler rather than by reaching into
    // state, so what is tested is what a keypress does.
    assert!(select_row(&mut app, "datui-sales"), "the bucket row");
    app.event(&key(crossterm::event::KeyCode::Enter));
    let descended = pump_until(&mut app, &rx, 30, |app| {
        section_named(app, "s3://datui-sales")
            .is_some_and(|s| s.rows.iter().any(|r| r.name == "top-level.csv"))
    });
    assert!(
        descended,
        "descending into a bucket should list its top level"
    );
    assert_eq!(
        app.home.browsing.as_deref(),
        Some(std::path::Path::new("s3://datui-sales"))
    );

    for section in &app.home.sections {
        println!(
            "section {:?} unavailable={} rows={:?}",
            section.title,
            section.unavailable,
            section.rows.iter().map(|r| &r.name).collect::<Vec<_>>()
        );
    }
    let rows = &section_named(&app, "s3://datui-sales")
        .expect("the browsed section")
        .rows;
    let names: Vec<&str> = rows.iter().map(|r| r.name.as_str()).collect();
    println!("inside s3://datui-sales: {names:?}");
    assert!(
        names.contains(&"2024"),
        "prefixes should be rows: {names:?}"
    );
    assert!(names.contains(&"orders.parquet"), "got {names:?}");

    let parquet = rows
        .iter()
        .find(|r| r.name == "orders.parquet")
        .expect("the parquet object");
    assert!(
        parquet.size.unwrap_or(0) > 1_000_000,
        "size comes from the listing itself"
    );
    assert!(
        parquet.rows.is_none() && parquet.columns.is_empty(),
        "no footer was read, so there is no row count and no column names: {parquet:?}"
    );

    // And down one more level, into a prefix rather than a bucket.
    assert!(select_row(&mut app, "2024"), "the prefix row");
    app.event(&key(crossterm::event::KeyCode::Enter));
    let deeper = pump_until(&mut app, &rx, 30, |app| {
        section_named(app, "s3://datui-sales/2024")
            .is_some_and(|s| s.rows.iter().any(|r| r.name == "january.csv"))
    });
    assert!(deeper, "a prefix should descend like a directory");
    let deep_names: Vec<&str> = section_named(&app, "s3://datui-sales/2024")
        .expect("the browsed prefix")
        .rows
        .iter()
        .map(|r| r.name.as_str())
        .collect();
    println!("inside s3://datui-sales/2024: {deep_names:?}");
    assert!(deep_names.contains(&"march.parquet"), "got {deep_names:?}");
}

#[test]
#[ignore = "opens S3 data end to end against a local MinIO; set DATUI_LIVE_S3"]
fn an_s3_object_opens_and_lands_in_recents() {
    let Ok(endpoint) = std::env::var("DATUI_LIVE_S3") else {
        eprintln!("skipped: set DATUI_LIVE_S3 to an endpoint to run");
        return;
    };
    let (mut app, rx) = minio_app(&endpoint);

    // Driven the way a user drives it: find the bucket, step in, press Enter on an
    // object. Sending `Open` directly skips what the home screen does around it, and a
    // test that skips that is testing a path nobody takes.
    assert!(
        enter_source(&mut app, &rx, "s3-default"),
        "the bucket should be listed"
    );
    assert!(select_row(&mut app, "datui-sales"), "the bucket row");
    app.event(&key(crossterm::event::KeyCode::Enter));

    let inside = pump_until(&mut app, &rx, 30, |app| {
        section_named(app, "s3://datui-sales")
            .is_some_and(|s| s.rows.iter().any(|r| r.name == "top-level.csv"))
    });
    assert!(inside, "the bucket contents should be listed");
    assert!(select_row(&mut app, "top-level.csv"), "the object row");

    if let Some(crash) = drive(&mut app, key(crossterm::event::KeyCode::Enter)) {
        panic!("opening the object crashed: {crash}");
    }

    // A remote open asks before spending egress.
    let asked = pump_until(&mut app, &rx, 30, |app| {
        app.awaiting_download_confirmation() || app.data_table_state.is_some()
    });
    assert!(asked, "opening a remote object should either ask or load");
    if app.awaiting_download_confirmation() {
        // The modal opens focused on No, which is the right default for a prompt that
        // spends somebody's egress budget. Moving to Yes is part of what a user does.
        println!("confirmation raised, accepting");
        app.event(&key(crossterm::event::KeyCode::Left));
        if let Some(crash) = drive(&mut app, key(crossterm::event::KeyCode::Enter)) {
            panic!("confirming the download crashed: {crash}");
        }
    }

    let loaded = pump_until(&mut app, &rx, 60, |app| {
        app.data_table_state.is_some() && !app.is_busy()
    });
    assert!(
        loaded,
        "the dataset should load; input_mode={:?} status={:?} busy={}",
        app.input_mode,
        app.home.status,
        app.is_busy()
    );

    let state = app.data_table_state.as_ref().expect("a loaded table");
    println!("loaded {} columns", state.headers().len());
    assert_eq!(
        state.headers(),
        vec!["order_id", "region", "customer", "amount", "ts"],
        "the CSV header should have been read from the object"
    );

    // And it should be offered next time. Recents is the other half of "open them
    // easily": a bucket visited once should not need finding again.
    //
    // Read through a cache manager of its own. `isolate_cache` points DATUI_CACHE_DIR at
    // a per-process directory, so this is the same store the app just wrote to, without
    // the app having to expose it.
    let cache = datui::CacheManager::new("datui").expect("cache manager");
    let recorded = pump_until(&mut app, &rx, 20, |_| {
        cache
            .load_recents()
            .iter()
            .any(|p| p.to_string_lossy().contains("datui-sales/top-level.csv"))
    });
    let recents = cache.load_recents();
    println!("recents now: {recents:?}");
    assert!(
        recorded,
        "an opened S3 object should be recorded in recents; got {recents:?}"
    );
}

#[test]
#[ignore = "renders the home screen against a local MinIO; set DATUI_LIVE_S3"]
fn the_cloud_section_renders_legibly() {
    let Ok(endpoint) = std::env::var("DATUI_LIVE_S3") else {
        eprintln!("skipped: set DATUI_LIVE_S3 to an endpoint to run");
        return;
    };
    let (mut app, rx) = minio_app(&endpoint);
    pump_until(&mut app, &rx, 30, |app| {
        ["s3-default", "gcs-default"].iter().all(|id| {
            app.home
                .cloud
                .iter()
                .any(|s| s.id == *id && !s.buckets.is_empty())
        })
    });

    // Rendered rather than inspected. A row can hold the right facts and still read
    // badly: a note that crowds out the name, a count in the wrong place. Printing the
    // buffer is the only way to see that from a test. Tall enough that section order
    // cannot decide the outcome once Recent exists.
    let screen = screen_text(&mut app, 100, 60);
    println!("{screen}");
    assert!(screen.contains("CLOUD"), "one heading for every source");
    assert!(
        screen.contains("☁ S3-compatible") && screen.contains("☁ Google Cloud"),
        "each source is a row marked as living in an object store"
    );
    assert!(
        !screen.contains("☁ crates/"),
        "a local directory must not be marked as cloud"
    );
    assert!(
        screen.contains("◦ crates/"),
        "a local directory should carry the local marker"
    );

    // Inside a source: its buckets, each a place to step into.
    assert!(
        enter_source(&mut app, &rx, "s3-default"),
        "into the S3 source"
    );
    let buckets = screen_text(&mut app, 100, 30);
    println!("{buckets}");
    for bucket in ["datui-sales", "datui-logs", "datui-events", "datui-empty"] {
        assert!(
            buckets.contains(&format!("☁ {bucket}/")),
            "{bucket} should be on screen"
        );
    }

    // And inside a bucket, where objects carry sizes and prefixes are somewhere to go.
    assert!(select_row(&mut app, "datui-sales"), "the bucket row");
    app.event(&key(crossterm::event::KeyCode::Enter));
    pump_until(&mut app, &rx, 30, |app| {
        section_named(app, "s3://datui-sales")
            .is_some_and(|s| s.rows.iter().any(|r| r.name == "orders.parquet"))
    });
    let inside_screen = screen_text(&mut app, 100, 30);
    println!("{inside_screen}");
    assert!(
        inside_screen.contains("2024/ prefix"),
        "a prefix inside a bucket should say so"
    );
    assert!(
        inside_screen.contains("orders.parquet"),
        "objects should be listed"
    );

    // The other provider looks the same.
    app.event(&key(crossterm::event::KeyCode::Esc));
    app.event(&key(crossterm::event::KeyCode::Esc));
    assert_eq!(app.home.browsing, None, "back at the home listing");
    assert!(
        enter_source(&mut app, &rx, "gcs-default"),
        "into the GCS source"
    );
    let gcs = screen_text(&mut app, 100, 30);
    println!("{gcs}");
    let first = app
        .home
        .sections
        .first()
        .and_then(|s| s.rows.first())
        .expect("a Google Cloud Storage bucket")
        .name
        .clone();
    assert!(
        gcs.contains(&format!("☁ {first}/")),
        "a GCS bucket reads as a place too"
    );
}

/// Two S3-compatible servers, each with a bucket called `data` holding a different
/// `table.parquet`, named in `[[cloud.sources]]` as `lab` and `corp`.
///
/// ```bash
/// DATUI_LIVE_S3_PAIR=http://127.0.0.1:9101,http://127.0.0.1:9102 \
///   cargo test --test cloud_live_test -- --ignored --nocapture two_servers
/// ```
///
/// The servers need `data/table.parquet`: `people.parquet` on the first and
/// `sales.parquet` on the second, from `tests/sample-data`. Any keys work against a
/// local MinIO or moto; this test sets `LAB_KEY`, `LAB_SECRET`, `CORP_KEY` and
/// `CORP_SECRET` to match what it expects the servers to accept.
#[test]
#[ignore = "talks to two local S3-compatible servers; set DATUI_LIVE_S3_PAIR"]
fn two_servers_with_the_same_bucket_open_their_own_objects() {
    let Ok(pair) = std::env::var("DATUI_LIVE_S3_PAIR") else {
        eprintln!("skipped: set DATUI_LIVE_S3_PAIR=<endpoint>,<endpoint> to run");
        return;
    };
    common::isolate_cache();
    let cloud = pair_config(&pair);

    // Each source lists its own server's buckets, and the rows it returns stay tied to it.
    let runtime = common::test_runtime();
    let env = Environment::current();
    for found in cloud_sources::discover(&cloud, &env)
        .iter()
        .filter(|s| s.id == "lab" || s.id == "corp")
    {
        let buckets = runtime
            .block_on(cloud_browse::list_buckets(found))
            .expect("listing buckets");
        assert!(
            buckets.contains(&"data".to_string()),
            "{}: {buckets:?}",
            found.id
        );
        let url = found.bucket_url("data");
        let rows = runtime
            .block_on(cloud_browse::list_objects(&url, &cloud))
            .expect("listing objects");
        let paths: Vec<String> = rows
            .iter()
            .map(|r| r.path.to_string_lossy().into_owned())
            .collect();
        println!("{url}: {paths:?}");
        assert!(
            paths.contains(&format!("s3://{}@data/table.parquet", found.id)),
            "rows should keep their source: {paths:?}"
        );
    }

    let headers_of = |url: &str| -> Vec<String> {
        let mut config = datui::config::AppConfig {
            cloud: cloud.clone(),
            ..Default::default()
        };
        config.data.use_desktop_recents = false;
        let (tx, rx) = std::sync::mpsc::channel();
        let mut app = datui::App::new_with_config(
            tx,
            common::test_runtime(),
            datui::Theme {
                colors: std::collections::HashMap::new(),
            },
            config,
        );
        let open = datui::AppEvent::Open(
            vec![std::path::PathBuf::from(url)],
            datui::OpenOptions::default(),
        );
        if let Some(crash) = drive(&mut app, open) {
            panic!("opening {url} crashed: {crash}");
        }
        let loaded = pump_until(&mut app, &rx, 60, |app| {
            app.data_table_state.is_some() && !app.is_busy()
        });
        assert!(loaded, "{url} should load");
        app.data_table_state.as_ref().expect("a table").headers()
    };
    let local_headers = |file: &str| -> Vec<String> {
        let path = format!("{}/tests/sample-data/{file}", env!("CARGO_MANIFEST_DIR"));
        polars::prelude::LazyFrame::scan_parquet(
            polars::prelude::PlRefPath::new(path.as_str()),
            Default::default(),
        )
        .and_then(|mut lf| lf.collect_schema())
        .expect("local schema")
        .iter_names()
        .map(|n| n.to_string())
        .collect()
    };

    let lab = headers_of("s3://lab@data/table.parquet");
    let corp = headers_of("s3://corp@data/table.parquet");
    println!("lab: {lab:?}\ncorp: {corp:?}");
    assert_eq!(lab, local_headers("people.parquet"));
    assert_eq!(corp, local_headers("sales.parquet"));
}

/// `[[cloud.sources]]` for the two servers in `DATUI_LIVE_S3_PAIR`, `lab` and `corp`.
fn pair_config(pair: &str) -> CloudConfig {
    let (lab_endpoint, corp_endpoint) = pair.split_once(',').expect("two endpoints");
    // SAFETY: set before the runtime or any worker starts reading the environment, and
    // these tests are run on their own.
    unsafe {
        std::env::set_var("LAB_KEY", "key9101");
        std::env::set_var("LAB_SECRET", "secret9101");
        std::env::set_var("CORP_KEY", "key9102");
        std::env::set_var("CORP_SECRET", "secret9102");
    }
    let source =
        |name: &str, label: Option<&str>, endpoint: &str| datui::config::CloudSourceConfig {
            name: name.to_string(),
            label: label.map(str::to_string),
            kind: Some("s3".to_string()),
            endpoint_url: Some(endpoint.to_string()),
            region: Some("us-east-1".to_string()),
            access_key_id_env: Some(format!("{}_KEY", name.to_uppercase())),
            secret_access_key_env: Some(format!("{}_SECRET", name.to_uppercase())),
            ..Default::default()
        };
    let cloud = CloudConfig {
        sources: vec![
            source("lab", Some("Lab MinIO"), lab_endpoint),
            source("corp", None, corp_endpoint),
        ],
        ..CloudConfig::default()
    };
    cloud.validate().expect("valid sources");
    cloud
}

fn screen_text(app: &mut datui::App, width: u16, height: u16) -> String {
    let area = ratatui::layout::Rect::new(0, 0, width, height);
    let mut buf = ratatui::buffer::Buffer::empty(area);
    ratatui::widgets::Widget::render(app, area, &mut buf);
    let mut screen = String::new();
    for y in 0..area.height {
        let mut row = String::new();
        for x in 0..area.width {
            row.push_str(buf[(x, y)].symbol());
        }
        screen.push_str(row.trim_end());
        screen.push('\n');
    }
    screen
}

#[test]
#[ignore = "renders the CLOUD section against two local S3-compatible servers; set DATUI_LIVE_S3_PAIR"]
fn the_cloud_section_lists_sources_and_steps_through_them() {
    let Ok(pair) = std::env::var("DATUI_LIVE_S3_PAIR") else {
        eprintln!("skipped: set DATUI_LIVE_S3_PAIR=<endpoint>,<endpoint> to run");
        return;
    };
    common::isolate_cache();
    let mut config = datui::config::AppConfig {
        cloud: pair_config(&pair),
        ..Default::default()
    };
    config.data.use_desktop_recents = false;
    let (tx, rx) = std::sync::mpsc::channel();
    let mut app = datui::App::new_with_config(
        tx,
        common::test_runtime(),
        datui::Theme {
            colors: std::collections::HashMap::new(),
        },
        config,
    );
    app.enter_home();

    let listed = pump_until(&mut app, &rx, 30, |app| {
        ["lab", "corp"].iter().all(|id| {
            app.home
                .cloud
                .iter()
                .any(|s| s.id == *id && s.status == datui::home::CloudStatus::Listed)
        })
    });
    assert!(listed, "both sources should list: {:?}", app.home.cloud);
    // Tall enough for the CLOUD rows below whatever earlier tests left in Recent.
    let home = screen_text(&mut app, 110, 60);
    println!("{home}");
    assert!(home.contains("CLOUD"), "one CLOUD section");
    assert!(
        home.contains("Lab MinIO") && home.contains("1 bucket"),
        "{home}"
    );

    assert!(select_row(&mut app, "Lab MinIO"), "the source row");
    let source_pane = screen_text(&mut app, 110, 30);
    println!("{source_pane}");
    assert!(
        source_pane.contains("endpoint"),
        "the details pane names the endpoint"
    );

    app.event(&key(crossterm::event::KeyCode::Enter));
    let inside = pump_until(&mut app, &rx, 10, |app| {
        section_named(app, "Lab MinIO").is_some_and(|s| s.rows.iter().any(|r| r.name == "data"))
    });
    assert!(inside, "entering a source lists its buckets");
    let buckets = screen_text(&mut app, 110, 20);
    println!("{buckets}");
    let sep = datui::glyphs::get().trail;
    assert!(
        buckets.contains(&format!("cloud {sep} Lab MinIO")),
        "the trail"
    );

    assert!(select_row(&mut app, "data"), "the bucket row");
    app.event(&key(crossterm::event::KeyCode::Enter));
    let objects = pump_until(&mut app, &rx, 30, |app| {
        app.home
            .sections
            .first()
            .is_some_and(|s| s.rows.iter().any(|r| r.name == "table.parquet"))
    });
    assert!(objects, "entering a bucket lists its objects");
    let bucket = screen_text(&mut app, 110, 20);
    println!("{bucket}");
    assert!(bucket.contains(&format!("cloud {sep} Lab MinIO {sep} data")));

    // Backspace climbs back out through the source to the home listing.
    app.event(&key(crossterm::event::KeyCode::Backspace));
    assert_eq!(
        app.home.browsing.as_deref(),
        Some(std::path::Path::new("cloud://lab"))
    );
    app.event(&key(crossterm::event::KeyCode::Esc));
    assert_eq!(app.home.browsing, None, "Esc from the source returns home");
}

/// Walk a cloud source down to `demo/penguins.parquet` through the home screen and open
/// it: Enter on the source, then on each row named in `steps`.
fn open_through_home(
    app: &mut datui::App,
    rx: &std::sync::mpsc::Receiver<datui::AppEvent>,
    id: &str,
    steps: &[&str],
) -> Vec<String> {
    assert!(
        enter_source(app, rx, id),
        "{id} should list: {:?}",
        app.home.cloud.iter().find(|s| s.id == id)
    );
    println!("{}", screen_text(app, 120, 24));
    for step in steps {
        let shown = pump_until(app, rx, 60, |app| {
            app.home.visible().iter().any(
                |row| matches!(row, datui::home::Row::Entry { entry, .. } if entry.name == *step),
            )
        });
        let why = app
            .home
            .sections
            .first()
            .map(|s| (s.title.clone(), s.unavailable_note.clone()));
        assert!(shown, "{step} should be listed; section {why:?}");
        assert!(select_row(app, step), "{step}");
        println!("{}", screen_text(app, 120, 24));
        if let Some(crash) = drive(app, key(crossterm::event::KeyCode::Enter)) {
            panic!("Enter on {step} crashed: {crash}");
        }
    }
    let loaded = pump_until(app, rx, 120, |app| {
        app.data_table_state.is_some() && !app.is_busy()
    });
    assert!(
        loaded,
        "the dataset should load; status={:?}",
        app.home.status
    );
    app.data_table_state.as_ref().expect("a table").headers()
}

fn live_app() -> (datui::App, std::sync::mpsc::Receiver<datui::AppEvent>) {
    common::isolate_cache();
    let mut config = datui::config::AppConfig::default();
    config.data.use_desktop_recents = false;
    config.cloud = datui::OpenOptions::default().effective_cloud(&config.cloud);
    let (tx, rx) = std::sync::mpsc::channel();
    let mut app = datui::App::new_with_config(
        tx,
        common::test_runtime(),
        datui::Theme {
            colors: std::collections::HashMap::new(),
        },
        config,
    );
    app.enter_home();
    (app, rx)
}

/// A signed-in `az`, and a storage account holding the `datui-test` container described
/// in #168: `demo/`, `sample-data/`, `edge/`.
///
/// ```bash
/// az login
/// DATUI_LIVE_AZURE=<account> cargo test --test cloud_live_test -- --ignored --nocapture azure
/// ```
#[test]
#[ignore = "browses a real Azure storage account; set DATUI_LIVE_AZURE=<account>"]
fn an_azure_container_browses_and_opens_through_az_login() {
    let Ok(account) = std::env::var("DATUI_LIVE_AZURE") else {
        eprintln!("skipped: set DATUI_LIVE_AZURE to a storage account to run");
        return;
    };
    let (mut app, rx) = live_app();
    let headers = open_through_home(
        &mut app,
        &rx,
        "az",
        &[&account, "datui-test", "demo", "penguins.parquet"],
    );
    println!("{headers:?}");
    assert!(headers.iter().any(|h| h == "species"), "{headers:?}");
}

/// The AWS CLI signed in (any method, `aws login` and SSO included), and a bucket
/// holding the same `demo/`, `sample-data/` and `edge/` as the Azure container.
///
/// ```bash
/// DATUI_LIVE_AWS=<bucket> cargo test --test cloud_live_test -- --ignored --nocapture aws_bucket
/// ```
#[test]
#[ignore = "browses a real S3 bucket; set DATUI_LIVE_AWS=<bucket>"]
fn an_aws_bucket_browses_and_opens_through_the_active_profile() {
    let Ok(bucket) = std::env::var("DATUI_LIVE_AWS") else {
        eprintln!("skipped: set DATUI_LIVE_AWS to a bucket to run");
        return;
    };
    let (mut app, rx) = live_app();
    let headers = open_through_home(
        &mut app,
        &rx,
        "s3-default",
        &[&bucket, "demo", "penguins.parquet"],
    );
    println!("{headers:?}");
    assert!(headers.iter().any(|h| h == "species"), "{headers:?}");
}

/// Open every file under `edge/` in the test container and bucket: names with spaces,
/// non-ASCII, `+ = & #`, `%`, an uppercase extension, no extension, deep directories.
///
/// ```bash
/// DATUI_LIVE_EDGE="abfss://datui-test@<account>.dfs.core.windows.net/edge/,s3://<bucket>/edge/" \
///   cargo test --test cloud_live_test -- --ignored --nocapture awkward_names
/// ```
#[test]
#[ignore = "opens awkwardly named objects in real stores; set DATUI_LIVE_EDGE"]
fn awkward_names_list_and_open() {
    let Ok(roots) = std::env::var("DATUI_LIVE_EDGE") else {
        eprintln!("skipped: set DATUI_LIVE_EDGE to comma-separated edge/ URLs to run");
        return;
    };
    let config = datui::OpenOptions::default().effective_cloud(&CloudConfig::default());
    let runtime = common::test_runtime();
    let mut failures = Vec::new();
    for root in roots.split(',') {
        let mut pending = vec![root.to_string()];
        let mut files = Vec::new();
        while let Some(dir) = pending.pop() {
            let rows = runtime
                .block_on(cloud_browse::list_objects(&dir, &config))
                .unwrap_or_else(|e| panic!("listing {dir}: {e}"));
            for row in rows {
                let path = row.path.to_string_lossy().into_owned();
                match row.kind {
                    datui::discover::EntryKind::Directory => pending.push(path),
                    _ => files.push((row.name, path)),
                }
            }
        }
        println!("{root}: {} files", files.len());
        for (name, url) in files {
            let (tx, rx) = std::sync::mpsc::channel();
            let mut app = datui::App::new_with_config(
                tx,
                common::test_runtime(),
                datui::Theme {
                    colors: std::collections::HashMap::new(),
                },
                datui::config::AppConfig {
                    cloud: config.clone(),
                    ..Default::default()
                },
            );
            let mut options = datui::OpenOptions::default();
            if name == "no-extension" {
                options.format = Some(datui::FileFormat::Parquet);
            }
            let open = datui::AppEvent::Open(vec![std::path::PathBuf::from(&url)], options);
            if let Some(crash) = drive(&mut app, open) {
                failures.push(format!("{url}: {crash}"));
                continue;
            }
            let done = pump_until(&mut app, &rx, 60, |app| {
                app.awaiting_download_confirmation()
                    || (app.data_table_state.is_some() && !app.is_busy())
            });
            if app.awaiting_download_confirmation() {
                app.event(&key(crossterm::event::KeyCode::Left));
                drive(&mut app, key(crossterm::event::KeyCode::Enter));
                pump_until(&mut app, &rx, 60, |app| {
                    app.data_table_state.is_some() && !app.is_busy()
                });
            }
            match app.data_table_state.as_ref() {
                Some(state) if done => println!("  ok   {name}: {} columns", state.headers().len()),
                _ => failures.push(format!(
                    "{url}: did not load (status {:?})",
                    app.home.status
                )),
            }
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
}

/// An `mc` alias and an s3cmd config pointing at the two servers of
/// `DATUI_LIVE_S3_PAIR` appear as sources with no datui config, list, and open.
#[test]
#[ignore = "talks to two local S3-compatible servers; set DATUI_LIVE_S3_PAIR"]
fn mc_and_s3cmd_configs_are_sources_that_open() {
    let Ok(pair) = std::env::var("DATUI_LIVE_S3_PAIR") else {
        eprintln!("skipped: set DATUI_LIVE_S3_PAIR=<endpoint>,<endpoint> to run");
        return;
    };
    let (first, second) = pair.split_once(',').expect("two endpoints");
    let dir = tempfile::TempDir::new().expect("temp dir");
    std::fs::write(
        dir.path().join("config.json"),
        format!(
            r#"{{"version": "10", "aliases": {{"lab": {{"url": "{first}", "accessKey": "key9101", "secretKey": "secret9101", "api": "S3v4", "path": "auto"}}}}}}"#
        ),
    )
    .unwrap();
    let host = second.split_once("://").map_or(second, |(_, h)| h);
    std::fs::write(
        dir.path().join("s3cfg"),
        format!(
            "[default]\naccess_key = key9102\nsecret_key = secret9102\nhost_base = {host}\nhost_bucket = {host}\nuse_https = False\n"
        ),
    )
    .unwrap();
    // SAFETY: set before the runtime starts; run with --test-threads=1.
    unsafe {
        std::env::set_var("MC_CONFIG_DIR", dir.path());
        std::env::set_var("S3CMD_CONFIG", dir.path().join("s3cfg"));
    }
    let config = CloudConfig::default();
    let runtime = common::test_runtime();
    let sources = cloud_sources::discover(&config, &Environment::current());
    for id in ["mc-lab", "s3cfg"] {
        let source = sources.iter().find(|s| s.id == id).unwrap_or_else(|| {
            panic!(
                "{id} in {:?}",
                sources.iter().map(|s| &s.id).collect::<Vec<_>>()
            )
        });
        let buckets = runtime
            .block_on(cloud_browse::list_buckets(source))
            .expect("listing buckets");
        assert!(buckets.contains(&"data".to_string()), "{id}: {buckets:?}");
    }
    let open = |url: &str| -> Vec<String> {
        let (tx, rx) = std::sync::mpsc::channel();
        let mut app = datui::App::new_with_config(
            tx,
            common::test_runtime(),
            datui::Theme {
                colors: std::collections::HashMap::new(),
            },
            datui::config::AppConfig::default(),
        );
        let event = datui::AppEvent::Open(
            vec![std::path::PathBuf::from(url)],
            datui::OpenOptions::default(),
        );
        if let Some(crash) = drive(&mut app, event) {
            panic!("{url}: {crash}");
        }
        assert!(pump_until(&mut app, &rx, 60, |app| app
            .data_table_state
            .is_some()
            && !app.is_busy()));
        app.data_table_state.as_ref().unwrap().headers()
    };
    assert!(open("s3://mc-lab@data/table.parquet").contains(&"first_name".to_string()));
    assert!(open("s3://s3cfg@data/table.parquet").contains(&"product".to_string()));
}

/// Open `url` in a fresh app, confirming a download when asked, and return the headers.
fn open_url(url: &str, config: &CloudConfig) -> Result<Vec<String>, String> {
    let (tx, rx) = std::sync::mpsc::channel();
    let mut app = datui::App::new_with_config(
        tx,
        common::test_runtime(),
        datui::Theme {
            colors: std::collections::HashMap::new(),
        },
        datui::config::AppConfig {
            cloud: config.clone(),
            ..Default::default()
        },
    );
    let open = datui::AppEvent::Open(
        vec![std::path::PathBuf::from(url)],
        datui::OpenOptions::default(),
    );
    if let Some(crash) = drive(&mut app, open) {
        return Err(crash);
    }
    pump_until(&mut app, &rx, 120, |app| {
        app.awaiting_download_confirmation() || (app.data_table_state.is_some() && !app.is_busy())
    });
    if app.awaiting_download_confirmation() {
        app.event(&key(crossterm::event::KeyCode::Left));
        drive(&mut app, key(crossterm::event::KeyCode::Enter));
        pump_until(&mut app, &rx, 120, |app| {
            app.data_table_state.is_some() && !app.is_busy()
        });
    }
    match app.data_table_state.as_ref() {
        Some(state) if !app.is_busy() => Ok(state.headers()),
        _ => Err(format!("did not load: {:?}", app.home.status)),
    }
}

/// The first data file under `root`: down the first few prefixes of each level, taking
/// Parquet of any size (only its footer is read) or anything else under 8 MB.
fn first_openable(
    root: &str,
    config: &CloudConfig,
    runtime: &tokio::runtime::Handle,
) -> Result<Option<String>, String> {
    let mut pending = vec![root.to_string()];
    let mut listings = 0;
    while let Some(dir) = pending.pop() {
        listings += 1;
        if listings > 30 {
            break;
        }
        let rows = runtime
            .block_on(cloud_browse::list_objects(&dir, config))
            .map_err(|e| format!("listing {dir}: {e}"))?;
        if listings == 1 && rows.is_empty() {
            return Err(format!("{dir} lists nothing"));
        }
        let parquet =
            |r: &datui::discover::Entry| datui::discover::is_parquet_key(&r.path.to_string_lossy());
        if let Some(row) = rows
            .iter()
            .filter(|r| r.kind == datui::discover::EntryKind::File)
            .find(|r| parquet(r) || r.size.is_some_and(|s| s < 8 << 20))
        {
            return Ok(Some(row.path.to_string_lossy().into_owned()));
        }
        let mut dirs: Vec<String> = rows
            .iter()
            .filter(|r| r.kind == datui::discover::EntryKind::Directory)
            .take(3)
            .map(|r| r.path.to_string_lossy().into_owned())
            .collect();
        dirs.reverse();
        pending.extend(dirs);
    }
    Ok(None)
}

/// Every built-in public dataset lists and opens, with whatever credentials this
/// machine has for other stores, or none. The weekly `Public datasets` workflow runs
/// this with none.
///
/// ```bash
/// DATUI_LIVE_PUBLIC=1 cargo test --test cloud_live_test -- --ignored --nocapture public
/// ```
#[test]
#[ignore = "reads public datasets over the network; set DATUI_LIVE_PUBLIC=1"]
fn every_public_dataset_lists_and_opens() {
    if std::env::var("DATUI_LIVE_PUBLIC").is_err() {
        eprintln!("skipped: set DATUI_LIVE_PUBLIC=1 to run");
        return;
    }
    let config = datui::OpenOptions::default().effective_cloud(&CloudConfig::default());
    let runtime = common::test_runtime();
    let mut failures = Vec::new();
    for dataset in cloud_sources::builtin_datasets() {
        let started = std::time::Instant::now();
        let found = match first_openable(&dataset.url, &config, &runtime) {
            Ok(Some(url)) => url,
            Ok(None) => {
                failures.push(format!("{}: no data file found", dataset.name));
                continue;
            }
            Err(e) => {
                failures.push(format!("{}: {e}", dataset.name));
                continue;
            }
        };
        match open_url(&found, &config) {
            Ok(headers) if !headers.is_empty() => println!(
                "ok   {} ({:.1}s): {found}, {} columns",
                dataset.name,
                started.elapsed().as_secs_f32(),
                headers.len()
            ),
            Ok(_) => failures.push(format!("{}: {found} has no columns", dataset.name)),
            Err(e) => failures.push(format!("{}: {found}: {e}", dataset.name)),
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
}

/// The public datasets as the home screen shows them: a row under CLOUD, datasets by
/// name, and a file opened from inside one. The trail names the dataset, and Backspace
/// from the dataset's root returns to the datasets.
#[test]
#[ignore = "reads public datasets over the network; set DATUI_LIVE_PUBLIC=1"]
fn public_datasets_browse_and_open_from_the_home_screen() {
    if std::env::var("DATUI_LIVE_PUBLIC").is_err() {
        eprintln!("skipped: set DATUI_LIVE_PUBLIC=1 to run");
        return;
    }
    let (mut app, rx) = live_app();
    let headers = open_through_home(
        &mut app,
        &rx,
        "public",
        &["BigQuery sample data", "us-states", "us-states.parquet"],
    );
    assert!(headers.iter().any(|h| h == "name"), "{headers:?}");

    let (mut app, rx) = live_app();
    assert!(enter_source(&mut app, &rx, "public"));
    let text = screen_text(&mut app, 120, 30);
    assert!(
        text.contains("NOAA daily weather") && text.contains("CC0"),
        "{text}"
    );
    assert!(select_row(&mut app, "BigQuery sample data"));
    app.event(&key(crossterm::event::KeyCode::Enter));
    assert!(pump_until(&mut app, &rx, 60, |app| app
        .home
        .visible()
        .iter()
        .any(
            |row| matches!(row, datui::home::Row::Entry { entry, .. } if entry.name == "us-states")
        )));
    let sep = datui::glyphs::get().trail;
    let text = screen_text(&mut app, 120, 30);
    assert!(
        text.contains(&format!("Public datasets {sep} BigQuery sample data")),
        "{text}"
    );
    app.event(&key(crossterm::event::KeyCode::Backspace));
    assert_eq!(
        app.home.browsing,
        Some(std::path::PathBuf::from("cloud://public")),
        "back to the datasets"
    );
}

/// The quirks the public list is chosen to exercise: a bucket in another region, Parquet
/// with no extension, folder markers and Spark files, and a public bucket that is not on
/// the list at all, which is tried and remembered.
#[test]
#[ignore = "reads public datasets over the network; set DATUI_LIVE_PUBLIC=1"]
fn public_data_quirks() {
    if std::env::var("DATUI_LIVE_PUBLIC").is_err() {
        eprintln!("skipped: set DATUI_LIVE_PUBLIC=1 to run");
        return;
    }
    let config = datui::OpenOptions::default().effective_cloud(&CloudConfig::default());
    let runtime = common::test_runtime();

    assert_eq!(
        cloud_browse::s3_bucket_region("aws-public-blockchain").as_deref(),
        Some("us-east-2")
    );

    // GBIF: CC BY-NC, so not on the list, and Parquet part files with no extension.
    let snapshots = runtime
        .block_on(cloud_browse::list_objects(
            "s3://gbif-open-data-us-east-1/occurrence/",
            &config,
        ))
        .expect("GBIF lists");
    let latest = snapshots
        .iter()
        .map(|r| r.path.to_string_lossy().into_owned())
        .max()
        .expect("a snapshot");
    let parts = runtime
        .block_on(cloud_browse::list_objects(
            &format!("{latest}/occurrence.parquet/"),
            &config,
        ))
        .expect("the snapshot's part files list");
    let part = parts
        .iter()
        .filter(|r| r.name.chars().all(|c| c.is_ascii_digit()))
        .min_by_key(|r| r.size)
        .expect("a part file with no extension");
    let headers = open_url(&part.path.to_string_lossy(), &config).expect("the part file opens");
    assert!(headers.iter().any(|h| h == "gbifid"), "{headers:?}");
    let places = cloud_sources::take_public_places();
    println!("public places found: {places:?}");

    // Azure Open Datasets: hive directories with marker blobs beside them.
    let yellow = runtime
        .block_on(cloud_browse::list_objects(
            "abfss://nyctlc@azureopendatastorage.dfs.core.windows.net/yellow/",
            &config,
        ))
        .expect("nyctlc lists");
    assert!(
        yellow
            .iter()
            .all(|r| r.kind == datui::discover::EntryKind::Directory),
        "folder markers are not files: {:?}",
        yellow
            .iter()
            .filter(|r| r.kind != datui::discover::EntryKind::Directory)
            .map(|r| &r.name)
            .collect::<Vec<_>>()
    );
    let spark = runtime
        .block_on(cloud_browse::list_objects(
            "abfss://nyctlc@azureopendatastorage.dfs.core.windows.net/yellow/puYear=2018/puMonth=1/",
            &config,
        ))
        .expect("a partition lists");
    assert!(
        // `is_marker` rather than a leading-underscore test: a `_`-prefixed name that is
        // not a marker — `_manifest.parquet`, `_common_metadata` — is a real object
        // somebody may open, and the listing is meant to show it.
        spark.iter().all(|r| !cloud_browse::is_marker(&r.name)),
        "{:?}",
        spark.iter().map(|r| &r.name).collect::<Vec<_>>()
    );
}

/// Google Cloud through `gcloud`: the login lists its projects, a project lists its
/// buckets, and an object opens with the same token.
///
/// ```bash
/// gcloud auth login
/// DATUI_LIVE_GCLOUD=1 cargo test --test cloud_live_test -- --ignored --nocapture gcloud
/// # only `gcloud auth login`, no application-default login:
/// HOME=$(mktemp -d) CLOUDSDK_CONFIG=~/.config/gcloud RUSTUP_HOME=~/.rustup DATUI_LIVE_GCLOUD=1 \
///   cargo test --test cloud_live_test -- --ignored --nocapture gcloud
/// ```
#[test]
#[ignore = "talks to Google Cloud through gcloud; set DATUI_LIVE_GCLOUD=1"]
fn gcloud_lists_projects_and_their_buckets_and_opens() {
    if std::env::var("DATUI_LIVE_GCLOUD").is_err() {
        eprintln!("skipped: set DATUI_LIVE_GCLOUD=1 to run");
        return;
    }
    let config = datui::OpenOptions::default().effective_cloud(&CloudConfig::default());
    let sources = cloud_sources::discover(&config, &Environment::current());
    let google = sources
        .iter()
        .find(|s| s.id == cloud_sources::DEFAULT_GCS)
        .expect("a Google source");
    println!(
        "{}: origin {}, configuration {:?}",
        google.id, google.origin, google.gcloud
    );
    let runtime = common::test_runtime();
    let projects = runtime
        .block_on(cloud_browse::list_first_level(google))
        .expect("projects list");
    println!("{} projects", projects.len());
    assert!(!projects.is_empty());
    assert!(projects.iter().all(|p| {
        p.place
            .to_string_lossy()
            .starts_with("cloud://gcs-default/")
    }));
    let mut buckets = 0;
    for project in &projects {
        let rows = runtime
            .block_on(cloud_browse::list_account(
                &google.id,
                &project.name,
                &config,
            ))
            .unwrap_or_else(|e| panic!("{}: {e}", project.name));
        assert!(
            rows.iter()
                .all(|r| r.path.to_string_lossy().starts_with("gs://"))
        );
        buckets += rows.len();
    }
    println!("{buckets} buckets across them");
    assert!(buckets > 0, "no buckets in any project proves nothing");
    let first = projects
        .iter()
        .find_map(|project| {
            runtime
                .block_on(cloud_browse::list_account(
                    &google.id,
                    &project.name,
                    &config,
                ))
                .ok()
                .and_then(|rows| rows.into_iter().next())
        })
        .expect("a bucket");
    let inside = runtime
        .block_on(cloud_browse::list_objects(
            &first.path.to_string_lossy(),
            &config,
        ))
        .expect("a bucket of the login's lists with its token");
    println!("the first bucket holds {} rows at the top", inside.len());

    // Public, but outside the built-in dataset. Remembered as signed, so the open goes
    // through the login's token provider rather than finding the object public. (Not
    // proof the token is good: Google ignores a bad token on a URL whose bucket name is
    // percent-encoded, which is how object_store sends it. The listing above is.)
    cloud_sources::remember_access("gs://cloud-samples-data", false);
    let headers = open_url(
        "gs://cloud-samples-data/ml-engine/iris/classification/evaluate.csv",
        &config,
    )
    .expect("opens");
    assert!(!headers.is_empty(), "{headers:?}");
}

/// The Google Cloud row on the home screen steps into a project and then a bucket.
/// Prints nothing: the names are the login's own.
#[test]
#[ignore = "talks to Google Cloud through gcloud; set DATUI_LIVE_GCLOUD=1"]
fn gcloud_projects_browse_from_the_home_screen() {
    if std::env::var("DATUI_LIVE_GCLOUD").is_err() {
        eprintln!("skipped: set DATUI_LIVE_GCLOUD=1 to run");
        return;
    }
    let (mut app, rx) = live_app();
    assert!(
        enter_source(&mut app, &rx, "gcs-default"),
        "{:?}",
        app.home.cloud
    );
    let rows = |app: &datui::App| -> Vec<(String, std::path::PathBuf)> {
        app.home
            .visible()
            .iter()
            .filter_map(|row| match row {
                datui::home::Row::Entry { entry, .. } => {
                    Some((entry.name.clone(), entry.path.clone()))
                }
                _ => None,
            })
            .collect()
    };
    let projects = rows(&app);
    assert!(
        projects
            .iter()
            .all(|(_, p)| p.to_string_lossy().starts_with("cloud://gcs-default/"))
    );
    let text = screen_text(&mut app, 120, 30);
    assert!(text.contains("project"), "rows say project");
    // Into each project until one has a bucket.
    let mut entered_bucket = false;
    for (name, _) in projects {
        // The listing is rebuilt on a worker after Backspace.
        pump_until(&mut app, &rx, 10, |app| {
            rows(app).iter().any(|(n, _)| n == &name)
        });
        assert!(select_row(&mut app, &name));
        app.event(&key(crossterm::event::KeyCode::Enter));
        pump_until(&mut app, &rx, 30, |app| {
            app.home.browsing.as_ref().is_some_and(|b| {
                app.home.probed.contains_key(b) || app.home.probe_errors.contains_key(b)
            })
        });
        let sep = datui::glyphs::get().trail;
        let text = screen_text(&mut app, 160, 30);
        assert!(
            text.contains(&format!("Google Cloud {sep} {name}")),
            "the trail names the project"
        );
        pump_until(&mut app, &rx, 5, |app| {
            !rows(app).is_empty() || app.home.sections.iter().any(|s| s.unavailable)
        });
        let buckets = rows(&app);
        if let Some((bucket, path)) = buckets.first().cloned() {
            assert!(path.to_string_lossy().starts_with("gs://"));
            assert!(select_row(&mut app, &bucket));
            app.event(&key(crossterm::event::KeyCode::Enter));
            pump_until(&mut app, &rx, 30, |app| {
                app.home
                    .browsing
                    .as_ref()
                    .is_some_and(|b| app.home.probed.contains_key(b))
            });
            let text = screen_text(&mut app, 160, 30);
            assert!(
                text.contains(&format!("{name} {sep} {bucket}")),
                "the trail goes through the project"
            );
            app.event(&key(crossterm::event::KeyCode::Backspace));
            assert_eq!(
                app.home
                    .browsing
                    .as_deref()
                    .map(|p| p.to_string_lossy().into_owned()),
                Some(format!("cloud://gcs-default/{name}")),
                "Backspace from a bucket returns to its project"
            );
            entered_bucket = true;
            break;
        }
        app.event(&key(crossterm::event::KeyCode::Backspace));
    }
    assert!(entered_bucket, "no project had a bucket");
}

/// The same `datui-test` container read three more ways: with the key the fallback
/// fetches (Resource Graph, then `listKeys`), through a connection string built from it,
/// and through a SAS token. The key is never printed.
///
/// ```bash
/// SAS=$(az storage container generate-sas --account-name <account> --name datui-test \
///   --permissions rl --expiry $(date -u -d '+1 hour' +%Y-%m-%dT%H:%MZ) --auth-mode login \
///   --as-user -o tsv)
/// DATUI_LIVE_AZURE_KEYS=<account> DATUI_LIVE_AZURE_SAS="$SAS" \
///   cargo test --test cloud_live_test -- --ignored --nocapture --test-threads=1 azure_keys
/// ```
#[test]
#[ignore = "fetches a real account's keys; set DATUI_LIVE_AZURE_KEYS=<account>"]
fn azure_keys_connection_strings_and_sas_tokens_open() {
    let Ok(account) = std::env::var("DATUI_LIVE_AZURE_KEYS") else {
        eprintln!("skipped: set DATUI_LIVE_AZURE_KEYS to a storage account to run");
        return;
    };
    let url = format!("abfss://datui-test@{account}.dfs.core.windows.net/demo/penguins.parquet");
    let key = datui::azure::fetch_account_key(
        &account,
        &datui::azure::AzureAuth::AzCli,
        &Environment::current(),
    )
    .expect("the signed-in owner can fetch the account's keys");
    assert!(!key.is_empty());
    assert_eq!(
        datui::azure::remembered_key(&account).as_deref(),
        Some(key.as_str())
    );

    // With the key remembered, the az source reads with it.
    let config = datui::OpenOptions::default().effective_cloud(&CloudConfig::default());
    let resolved = cloud_sources::resolve(&url, &config).expect("resolves");
    assert!(matches!(
        resolved.azure.auth,
        datui::azure::AzureAuth::Key(_)
    ));
    let headers = open_url(&url, &config).expect("opens with the key");
    assert!(headers.iter().any(|h| h == "species"), "{headers:?}");

    // A connection string in the environment, and a source naming it.
    let connection = format!(
        "DefaultEndpointsProtocol=https;AccountName={account};AccountKey={key};EndpointSuffix=core.windows.net"
    );
    // SAFETY: run with --test-threads=1.
    unsafe { std::env::set_var("DATUI_LIVE_CONNECTION", &connection) };
    let config = CloudConfig {
        sources: vec![datui::config::CloudSourceConfig {
            name: "connstr".to_string(),
            kind: Some("azure".to_string()),
            connection_string_env: Some("DATUI_LIVE_CONNECTION".to_string()),
            ..Default::default()
        }],
        ..config
    };
    let listed = common::test_runtime()
        .block_on(cloud_browse::list_account("connstr", &account, &config))
        .expect("containers list with the connection string");
    assert!(listed.iter().any(|r| r.name == "datui-test"));
    let headers = open_url(&url, &config).expect("opens with the connection string");
    assert!(headers.iter().any(|h| h == "species"), "{headers:?}");

    if let Ok(sas) = std::env::var("DATUI_LIVE_AZURE_SAS") {
        unsafe { std::env::set_var("DATUI_LIVE_SAS", &sas) };
        let config = CloudConfig {
            sources: vec![datui::config::CloudSourceConfig {
                name: "sas".to_string(),
                kind: Some("azure".to_string()),
                account: Some(account.clone()),
                sas_env: Some("DATUI_LIVE_SAS".to_string()),
                ..Default::default()
            }],
            ..CloudConfig::default()
        };
        let rows = common::test_runtime()
            .block_on(cloud_browse::list_objects(
                &format!("abfss://datui-test@{account}.dfs.core.windows.net/demo/"),
                &config,
            ))
            .expect("a directory lists with the SAS");
        assert!(rows.iter().any(|r| r.name == "penguins.parquet"));
    }
}

/// Azurite through `AZURE_STORAGE_CONNECTION_STRING=UseDevelopmentStorage=true`: the
/// account lists its containers, a directory lists, and a Parquet file opens.
///
/// ```bash
/// npx azurite-blob --location /tmp/azurite --blobPort 10000 &
/// az storage container create --name demo --connection-string "UseDevelopmentStorage=true"
/// az storage blob upload --container-name demo --name small/data.parquet \
///   --file tests/sample-data/people.parquet --connection-string "UseDevelopmentStorage=true"
/// DATUI_LIVE_AZURITE=1 cargo test --test cloud_live_test -- --ignored --nocapture azurite
/// ```
#[test]
#[ignore = "talks to a local Azurite; set DATUI_LIVE_AZURITE=1"]
fn azurite_through_a_development_connection_string() {
    if std::env::var("DATUI_LIVE_AZURITE").is_err() {
        eprintln!("skipped: set DATUI_LIVE_AZURITE=1 to run");
        return;
    }
    // SAFETY: run with --test-threads=1.
    unsafe {
        std::env::set_var(
            "AZURE_STORAGE_CONNECTION_STRING",
            "UseDevelopmentStorage=true",
        )
    };
    let config = CloudConfig::default();
    let sources = cloud_sources::discover(&config, &Environment::current());
    let source = sources
        .iter()
        .find(|s| s.id == cloud_sources::DEFAULT_AZURE_ENV)
        .expect("the environment's account");
    let runtime = common::test_runtime();
    let accounts = runtime
        .block_on(cloud_browse::list_first_level(source))
        .expect("the account");
    assert_eq!(accounts[0].name, "devstoreaccount1");
    let containers = runtime
        .block_on(cloud_browse::list_account(
            cloud_sources::DEFAULT_AZURE_ENV,
            "devstoreaccount1",
            &config,
        ))
        .expect("containers list");
    assert!(
        containers.iter().any(|c| c.name == "demo"),
        "{containers:?}"
    );
    let rows = runtime
        .block_on(cloud_browse::list_objects(
            "abfss://demo@devstoreaccount1.dfs.core.windows.net/small/",
            &config,
        ))
        .expect("a directory lists");
    assert!(rows.iter().any(|r| r.name == "data.parquet"), "{rows:?}");
    let headers = open_url(
        "abfss://demo@devstoreaccount1.dfs.core.windows.net/small/data.parquet",
        &config,
    )
    .expect("opens");
    assert!(!headers.is_empty());
    unsafe { std::env::remove_var("AZURE_STORAGE_CONNECTION_STRING") };
}

/// The opt-ins against real services: a `secret_command` supplying the secret for the
/// first server of `DATUI_LIVE_S3_PAIR` (keys `key9101`/`secret9101`), a `.env` from
/// `[cloud] env_files` naming the second, and, with `DATUI_LIVE_GCS=1`, a Google source
/// logging in with `credentials_file` pointed at the application-default login.
#[test]
#[ignore = "talks to two local S3-compatible servers; set DATUI_LIVE_S3_PAIR"]
fn secret_commands_env_files_and_credentials_files() {
    let Ok(pair) = std::env::var("DATUI_LIVE_S3_PAIR") else {
        eprintln!("skipped: set DATUI_LIVE_S3_PAIR=<endpoint>,<endpoint> to run");
        return;
    };
    let (first, second) = pair.split_once(',').expect("two endpoints");
    let dir = tempfile::TempDir::new().expect("temp dir");
    let secret_file = dir.path().join("lab-secret");
    std::fs::write(&secret_file, "secret9101\n").unwrap();
    std::fs::write(
        dir.path().join(".env"),
        format!(
            "# not a cloud variable, so not read\nDATABASE_URL=postgres://x\n\
             export AWS_ACCESS_KEY_ID=key9102\nAWS_SECRET_ACCESS_KEY=secret9102\n\
             AWS_ENDPOINT_URL={second}\nAWS_REGION=us-east-1\nLAB_KEY=key9101\n"
        ),
    )
    .unwrap();
    let file_config = CloudConfig {
        env_files: vec![".env".to_string()],
        sources: vec![datui::config::CloudSourceConfig {
            name: "lab".to_string(),
            kind: Some("s3".to_string()),
            endpoint_url: Some(first.to_string()),
            region: Some("us-east-1".to_string()),
            access_key_id_env: Some("LAB_KEY".to_string()),
            secret_command: Some(format!("cat {}", secret_file.display())),
            ..Default::default()
        }],
        ..Default::default()
    };
    assert!(datui::cloud_env::load(&file_config, dir.path()).is_empty());
    assert_eq!(datui::cloud_env::var("DATABASE_URL"), None);
    assert!(std::env::var("LAB_KEY").is_err(), "nothing is exported");
    let config = datui::OpenOptions::default().effective_cloud(&file_config);
    assert_eq!(config.s3_endpoint_url.as_deref(), Some(second));

    let lab = open_url("s3://lab@data/table.parquet", &config).expect("secret_command");
    assert!(lab.contains(&"first_name".to_string()), "{lab:?}");
    let default = open_url("s3://data/table.parquet", &config).expect("the .env's server");
    assert!(default.contains(&"product".to_string()), "{default:?}");

    if std::env::var("DATUI_LIVE_GCS").is_ok() {
        let google = CloudConfig {
            sources: vec![datui::config::CloudSourceConfig {
                name: "adc-file".to_string(),
                kind: Some("gcs".to_string()),
                credentials_file: Some(
                    "~/.config/gcloud/application_default_credentials.json".to_string(),
                ),
                ..Default::default()
            }],
            ..Default::default()
        };
        let source = cloud_sources::discover(&google, &Environment::current())
            .into_iter()
            .find(|s| s.id == "adc-file")
            .unwrap();
        assert_eq!(source.problem, None);
        let projects = common::test_runtime()
            .block_on(cloud_browse::list_first_level(&source))
            .expect("projects list with the credentials file");
        println!("{} projects through credentials_file", projects.len());
        assert!(!projects.is_empty());
    }
    datui::cloud_env::load(&CloudConfig::default(), dir.path());
}

/// Partitioned directories in a public dataset are labelled `hive` once peeked into, open
/// as one dataset with their partition column, and can still be browsed with →, where
/// a row opens the whole directory.
#[test]
#[ignore = "reads public datasets over the network; set DATUI_LIVE_PUBLIC=1"]
fn partitioned_cloud_directories_are_hive_datasets() {
    if std::env::var("DATUI_LIVE_PUBLIC").is_err() {
        eprintln!("skipped: set DATUI_LIVE_PUBLIC=1 to run");
        return;
    }
    let (mut app, rx) = live_app();
    assert!(enter_source(&mut app, &rx, "public"));
    let row_kind = |app: &datui::App, name: &str| {
        app.home.visible().iter().find_map(|row| match row {
            datui::home::Row::Entry { entry, .. } | datui::home::Row::Door { entry, .. }
                if entry.name == name =>
            {
                Some(entry.kind)
            }
            _ => None,
        })
    };
    let step = |app: &mut datui::App, name: &str| {
        assert!(
            pump_until(app, &rx, 60, |app| row_kind(app, name).is_some()),
            "{name} should be listed"
        );
        assert!(select_row(app, name), "{name}");
        if let Some(crash) = drive(app, key(crossterm::event::KeyCode::Enter)) {
            panic!("Enter on {name}: {crash}");
        }
    };
    step(&mut app, "Bitcoin and Ethereum");
    step(&mut app, "btc");
    let labelled = pump_until(&mut app, &rx, 60, |app| {
        row_kind(app, "blocks") == Some(datui::discover::EntryKind::Hive)
            && row_kind(app, "transactions") == Some(datui::discover::EntryKind::Hive)
    });
    println!("{}", screen_text(&mut app, 120, 20));
    assert!(labelled, "blocks and transactions are partitioned by date");

    // → goes inside, where one row stands for the whole directory.
    assert!(select_row(&mut app, "blocks"));
    app.event(&key(crossterm::event::KeyCode::Right));
    assert!(
        pump_until(&mut app, &rx, 60, |app| row_kind(
            app,
            "blocks (all partitions)"
        )
        .is_some()),
        "a row for every partition"
    );
    println!("{}", screen_text(&mut app, 120, 20));
    app.event(&key(crossterm::event::KeyCode::Backspace));

    // Enter opens the directory as one dataset, with the partition as a column.
    step(&mut app, "blocks");
    let loaded = pump_until(&mut app, &rx, 120, |app| {
        app.data_table_state.is_some() && !app.is_busy()
    });
    assert!(loaded, "blocks should open: {:?}", app.home.status);
    let headers = app.data_table_state.as_ref().unwrap().headers();
    println!("{headers:?}");
    assert!(headers.iter().any(|h| h == "date"), "{headers:?}");

    // End goes to the last block: counted from the files' footers, then read from the
    // last files alone.
    let _ = screen_text(&mut app, 160, 30);
    let pressed = std::time::Instant::now();
    if let Some(crash) = drive(&mut app, key(crossterm::event::KeyCode::End)) {
        panic!("End: {crash}");
    }
    let at_end = pump_until(&mut app, &rx, 120, |app| {
        !app.is_busy()
            && app.data_table_state.as_ref().is_some_and(|s| {
                s.num_rows_if_valid()
                    .is_some_and(|n| n > 900_000 && s.start_row + s.visible_rows >= n)
            })
    });
    let text = screen_text(&mut app, 160, 30);
    println!("End after {:?}\n{text}", pressed.elapsed());
    assert!(at_end, "End reaches the last block");
    assert!(
        !text.contains("2009-01-"),
        "the last rows, not the first: {text}"
    );

    // A directory of Parquet part files with no extension is one dataset too.
    let config = datui::OpenOptions::default().effective_cloud(&CloudConfig::default());
    let runtime = common::test_runtime();
    let snapshot = runtime
        .block_on(cloud_browse::list_objects(
            "s3://gbif-open-data-us-east-1/occurrence/",
            &config,
        ))
        .expect("GBIF lists")
        .into_iter()
        .map(|r| r.path.to_string_lossy().into_owned())
        .max()
        .expect("a snapshot");
    let parts = format!("{snapshot}/occurrence.parquet");
    let (kind, holds) = runtime
        .block_on(cloud_browse::peek_kind(&parts, &config))
        .expect("peeking a public prefix");
    assert_eq!(kind, datui::discover::EntryKind::MultiFile);
    assert_eq!(
        holds.one_format(),
        Some("parquet"),
        "and the row says what is in there: {:?}",
        holds.line(true)
    );
    let headers = open_url(&format!("{parts}/"), &config).expect("the part files open as one");
    assert!(headers.iter().any(|h| h == "gbifid"), "{headers:?}");
}

/// Bitcoin transactions: 1.4 billion rows over thousands of daily files whose nested
/// columns grew over the years. They open, count from footers, and read their end and
/// their middle without a schema error.
///
/// Not in the weekly workflow: the count reads a footer from every file, about 100 MB.
///
/// ```bash
/// DATUI_LIVE_PUBLIC=1 cargo test --release --test cloud_live_test -- --ignored --nocapture bitcoin_transactions
/// ```
#[test]
#[ignore = "reads 100 MB of public footers; set DATUI_LIVE_PUBLIC=1"]
fn bitcoin_transactions_open_count_and_reach_any_row() {
    if std::env::var("DATUI_LIVE_PUBLIC").is_err() {
        eprintln!("skipped: set DATUI_LIVE_PUBLIC=1 to run");
        return;
    }
    let config = datui::OpenOptions::default().effective_cloud(&CloudConfig::default());
    let (tx, rx) = std::sync::mpsc::channel();
    let mut app = datui::App::new_with_config(
        tx,
        common::test_runtime(),
        datui::Theme {
            colors: std::collections::HashMap::new(),
        },
        datui::config::AppConfig {
            cloud: config,
            ..Default::default()
        },
    );
    let started = std::time::Instant::now();
    let open = datui::AppEvent::Open(
        vec![std::path::PathBuf::from(
            "s3://aws-public-blockchain/v1.0/btc/transactions/",
        )],
        datui::OpenOptions::default(),
    );
    if let Some(crash) = drive(&mut app, open) {
        panic!("{crash}");
    }
    assert!(pump_until(&mut app, &rx, 120, |app| app
        .data_table_state
        .is_some()
        && !app.is_busy()));
    let headers = app.data_table_state.as_ref().unwrap().headers();
    println!("opened after {:?}: {headers:?}", started.elapsed());
    assert!(headers.iter().any(|h| h == "inputs"), "{headers:?}");

    let _ = screen_text(&mut app, 200, 30);
    drive(&mut app, key(crossterm::event::KeyCode::End));
    let at_end = pump_until(&mut app, &rx, 300, |app| {
        !app.is_busy()
            && app.data_table_state.as_ref().is_some_and(|s| {
                s.num_rows_if_valid()
                    .is_some_and(|n| n > 1_000_000_000 && s.start_row + s.visible_rows >= n)
            })
    });
    println!("at the end after {:?}", started.elapsed());
    assert!(at_end, "End reaches the last transaction");

    let middle = {
        let state = app.data_table_state.as_mut().unwrap();
        let middle = state.num_rows / 2;
        state.scroll_to(middle);
        middle
    };
    drive(&mut app, datui::AppEvent::Collect);
    pump_until(&mut app, &rx, 2, |_| false);
    assert!(pump_until(&mut app, &rx, 180, |app| !app.is_busy()));
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.start_row, middle);
    assert!(state.error.is_none(), "{:?}", state.error);
    println!("{}", screen_text(&mut app, 200, 12));
}

/// A directory is offered as one table only when its files agree on a schema, and the
/// question is settled from a few footers while browsing rather than by opening it.
///
/// Public datasets, so this needs no credentials. The negative case — a directory holding
/// one Parquet file per table — has no public home worth hard-coding; the in-memory
/// tests in `cloud_browse` cover it from both directions.
#[test]
#[ignore = "reads public datasets over the network; set DATUI_LIVE_PUBLIC=1"]
fn a_partitioned_dataset_is_not_mistaken_for_separate_tables() {
    if std::env::var("DATUI_LIVE_PUBLIC").is_err() {
        eprintln!("skipped: set DATUI_LIVE_PUBLIC=1 to run");
        return;
    }
    let runtime = common::test_runtime();
    let config = CloudConfig::default();

    // One GBIF snapshot: many part files, all the same table, and named `000001`
    // rather than anything ending `.parquet` — only the directory says what they are.
    let parts = "s3://gbif-open-data-us-east-1/occurrence/2026-06-01/occurrence.parquet/";
    let (kind, holds) = runtime
        .block_on(cloud_browse::peek_kind(parts, &config))
        .expect("peeking a public prefix");
    println!("{parts} -> {kind:?} {:?}", holds.line(true));
    assert_eq!(
        kind,
        datui::discover::EntryKind::MultiFile,
        "the parts of one snapshot are one table"
    );

    // The prefix above them is partitioned, which is decided from names alone.
    let all = "s3://aws-public-blockchain/v1.0/btc/transactions/";
    let (kind, _) = runtime
        .block_on(cloud_browse::peek_kind(all, &config))
        .expect("peeking a public prefix");
    println!("{all} -> {kind:?}");
    assert_eq!(kind, datui::discover::EntryKind::Hive);
}

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
    let providers = cloud_browse::detect(&config, &env);
    let gcs = providers
        .iter()
        .find(|p| p.kind == ProviderKind::Gcs)
        .expect("GCS should be discovered after `gcloud auth application-default login`");

    println!("provider: {} ({})", gcs.label, gcs.note);
    println!("project: {:?}", gcs.project);
    assert!(
        gcs.can_list_buckets(),
        "a logged-in gcloud writes quota_project_id, so a project should have been found"
    );

    let runtime = common::test_runtime();
    let buckets = runtime
        .block_on(cloud_browse::list_buckets(gcs, &config))
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
    let providers = cloud_browse::detect(&config, &env);
    let s3 = providers
        .iter()
        .find(|p| p.kind == ProviderKind::S3)
        .expect("configured keys should be enough to discover S3");

    println!("provider: {} ({})", s3.label, s3.note);
    assert!(
        s3.label.contains("S3-compatible"),
        "a custom endpoint should not be labelled Amazon: {}",
        s3.label
    );

    let runtime = common::test_runtime();
    let buckets = runtime
        .block_on(cloud_browse::list_buckets(s3, &config))
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
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    done(app)
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

/// Put the cursor on the visible row with this name, and say whether it was found.
fn select_row(app: &mut datui::App, name: &str) -> bool {
    for (index, row) in app.home.visible().iter().enumerate() {
        if let datui::home::Row::Entry { entry, .. } = row {
            if entry.name == name {
                app.home.selected = index;
                return true;
            }
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

    // Wait for rows, not for the section. The section appears first, empty, with
    // "listing buckets" beside the title, so a wait on the heading alone is satisfied
    // before a single bucket is known.
    let listed = pump_until(&mut app, &rx, 30, |app| {
        section_named(app, "S3-compatible (127.0.0.1:9000)").is_some_and(|s| !s.rows.is_empty())
    });
    assert!(
        listed,
        "the provider should appear as a section on the home screen"
    );

    let section = app
        .home
        .sections
        .iter()
        .find(|s| s.title.contains("S3-compatible"))
        .expect("the cloud section");
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
    assert_eq!(
        section.subtitle.as_deref(),
        Some("cloud · datui config"),
        "the section should say where the credentials came from"
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
    let listed = pump_until(&mut app, &rx, 30, |app| {
        app.home
            .sections
            .iter()
            .any(|s| s.rows.iter().any(|r| r.name == "datui-sales"))
    });
    assert!(listed, "the bucket should be listed");
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
        section_named(app, "S3-compatible (127.0.0.1:9000)").is_some_and(|s| !s.rows.is_empty())
    });

    // Rendered rather than inspected. A section can hold the right rows and still read
    // badly: a note that crowds out the title, a name truncated to nothing, a count in
    // the wrong place. Printing the buffer is the only way to see that from a test.
    // Tall enough that section order cannot decide the outcome. Once any test in this
    // file has opened an object, Recent exists too, and on a short screen the cloud
    // section's rows fall off the bottom — which is a fact about the viewport, not about
    // the rendering being asserted here.
    let area = ratatui::layout::Rect::new(0, 0, 100, 60);
    let mut buf = ratatui::buffer::Buffer::empty(area);
    ratatui::widgets::Widget::render(&mut app, area, &mut buf);

    let mut screen = String::new();
    for y in 0..area.height {
        let mut row = String::new();
        for x in 0..area.width {
            row.push_str(buf[(x, y)].symbol());
        }
        screen.push_str(row.trim_end());
        screen.push('\n');
    }
    println!("{screen}");

    // And the screen that matters more: inside a bucket, where objects carry sizes and
    // prefixes are somewhere to go next.
    assert!(select_row(&mut app, "datui-sales"), "the bucket row");
    app.event(&key(crossterm::event::KeyCode::Enter));
    pump_until(&mut app, &rx, 30, |app| {
        section_named(app, "s3://datui-sales")
            .is_some_and(|s| s.rows.iter().any(|r| r.name == "orders.parquet"))
    });
    let mut inside = ratatui::buffer::Buffer::empty(area);
    ratatui::widgets::Widget::render(&mut app, area, &mut inside);
    let mut inside_screen = String::new();
    for y in 0..area.height {
        let mut row = String::new();
        for x in 0..area.width {
            row.push_str(inside[(x, y)].symbol());
        }
        inside_screen.push_str(row.trim_end());
        inside_screen.push('\n');
    }
    println!("{inside_screen}");
    assert!(
        inside_screen.contains("2024/ prefix"),
        "a prefix inside a bucket should say so"
    );
    assert!(
        inside_screen.contains("orders.parquet"),
        "objects should be listed"
    );

    // The provider, its provenance, and the buckets all have to survive to the screen.
    assert!(
        screen.contains("S3-COMPATIBLE (127.0.0.1:9000)") || screen.contains("S3-compatible"),
        "the provider should be a visible heading"
    );
    assert!(
        screen.contains("cloud · datui config"),
        "the heading should say where the credentials came from"
    );
    for bucket in ["datui-sales", "datui-logs", "datui-events", "datui-empty"] {
        assert!(screen.contains(bucket), "{bucket} should be on screen");
    }
    // Buckets are places, and a place is written with a trailing slash here.
    assert!(
        screen.contains("datui-sales/"),
        "a bucket should read as somewhere to step into"
    );
}

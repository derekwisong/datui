//! With `[cloud] list_on_start` unset, opening the home screen asks no store anything:
//! a source is listed when it is entered. A local stand-in counts the requests.

#![cfg(feature = "cloud")]

mod common;

use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// A server that answers every request with a `ListBuckets` result naming `bucket`,
/// and counts them.
fn counting_server(bucket: &'static str) -> (String, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let endpoint = format!("http://{}", listener.local_addr().expect("address"));
    let requests = Arc::new(AtomicUsize::new(0));
    let counted = requests.clone();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let mut head = Vec::new();
            let mut byte = [0u8; 1];
            while !head.ends_with(b"\r\n\r\n") && stream.read(&mut byte).unwrap_or(0) == 1 {
                head.push(byte[0]);
            }
            counted.fetch_add(1, Ordering::SeqCst);
            let body = format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListAllMyBucketsResult>\
                 <Buckets><Bucket><Name>{bucket}</Name></Bucket></Buckets>\
                 </ListAllMyBucketsResult>"
            );
            let _ = write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/xml\r\nContent-Length: {}\r\n\
                 Connection: close\r\n\r\n{body}",
                body.len()
            );
        }
    });
    (endpoint, requests)
}

fn pump(
    app: &mut datui::App,
    rx: &std::sync::mpsc::Receiver<datui::AppEvent>,
    seconds: u64,
    done: impl Fn(&datui::App) -> bool,
) -> bool {
    let deadline = Instant::now() + Duration::from_secs(seconds);
    while Instant::now() < deadline {
        if done(app) {
            return true;
        }
        if let Ok(event) = rx.recv_timeout(Duration::from_millis(20)) {
            let mut next = Some(event);
            while let Some(event) = next {
                next = app.event(&event);
            }
        }
    }
    done(app)
}

fn key(code: crossterm::event::KeyCode) -> datui::AppEvent {
    datui::AppEvent::Key(crossterm::event::KeyEvent::new(
        code,
        crossterm::event::KeyModifiers::NONE,
    ))
}

fn source<'a>(app: &'a datui::App, id: &str) -> &'a datui::home::CloudSource {
    app.home
        .cloud
        .iter()
        .find(|s| s.id == id)
        .unwrap_or_else(|| panic!("{id} in {:?}", app.home.cloud))
}

fn row_shown(app: &datui::App, name: &str) -> bool {
    app.home
        .visible()
        .iter()
        .any(|row| matches!(row, datui::home::Row::Entry { entry, .. } if entry.name == name))
}

fn select(app: &mut datui::App, name: &str) {
    let index = app
        .home
        .visible()
        .iter()
        .position(|row| matches!(row, datui::home::Row::Entry { entry, .. } if entry.name == name))
        .unwrap_or_else(|| panic!("a row named {name}"));
    app.home.selected = index;
}

#[test]
fn a_source_is_listed_when_entered_not_when_the_home_screen_opens() {
    common::isolate_cache();
    // SAFETY: the only test in this binary, and set before the runtime starts.
    unsafe {
        std::env::set_var("DATUI_TEST_KEY", "key");
        std::env::set_var("GONE_KEY", "gone-key");
        std::env::set_var("DATUI_TEST_SECRET", "secret");
        std::env::set_var("CACHED_KEY", "cached-key");
        std::env::set_var("CACHED_SECRET", "cached-secret");
        // A login `s3-default` would be found by, were found logins shown.
        std::env::set_var("AWS_ACCESS_KEY_ID", "found-key");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "found-secret");
    }
    let (endpoint, requests) = counting_server("fast-bucket");
    // Keys of their own: the same server with the same key would be one source.
    let lab = |name: &str, key: &str| datui::config::CloudSourceConfig {
        name: name.to_string(),
        kind: Some("s3".to_string()),
        endpoint_url: Some(endpoint.clone()),
        region: Some("us-east-1".to_string()),
        access_key_id_env: Some(key.to_string()),
        secret_access_key_env: Some("DATUI_TEST_SECRET".to_string()),
        ..Default::default()
    };

    let mut config = datui::config::AppConfig::default();
    config.data.use_desktop_recents = false;
    config.cloud.sources = vec![
        lab("lab", "DATUI_TEST_KEY"),
        // Hidden from elsewhere after its row is drawn.
        lab("gone", "GONE_KEY"),
        // AWS itself, so its buckets are plain `s3://bucket` URLs that need the source
        // remembered. Never listed here: a request to it would leave the machine.
        datui::config::CloudSourceConfig {
            name: "cached".to_string(),
            kind: Some("s3".to_string()),
            access_key_id_env: Some("CACHED_KEY".to_string()),
            secret_access_key_env: Some("CACHED_SECRET".to_string()),
            ..Default::default()
        },
    ];
    // Only the configured sources, whatever this machine is logged in to.
    config.cloud.discover = Some(datui::config::CloudDiscover::None);
    config.cloud.hide = vec!["public".to_string()];
    assert_eq!(
        config.cloud.list_on_start, None,
        "the default is under test"
    );

    // What an earlier run listed for `cached`.
    let env = datui::cloud_browse::Environment::current();
    let cached = datui::cloud_sources::discover(&config.cloud, &env)
        .into_iter()
        .find(|s| s.id == "cached")
        .expect("the cached source");
    datui::CacheManager::new("datui")
        .expect("isolated cache")
        .save_cloud_listing(
            "cached",
            datui::cache::CloudListing {
                fingerprint: cached.fingerprint(),
                buckets: vec!["from-last-run".to_string()],
                listed_at: 1,
            },
        );

    let (tx, rx) = std::sync::mpsc::channel();
    let mut app = datui::App::new_with_config(
        tx,
        common::test_runtime(),
        datui::Theme {
            colors: std::collections::HashMap::new(),
        },
        config.clone(),
    );
    app.enter_home();

    assert!(
        pump(&mut app, &rx, 5, |app| row_shown(app, "lab")
            && row_shown(app, "cached")),
        "both rows: {:?}",
        app.home.cloud
    );
    // Long enough for a listing that was going to start to have reached the server.
    pump(&mut app, &rx, 1, |_| false);
    let ids: Vec<&str> = app.home.cloud.iter().map(|s| s.id.as_str()).collect();
    assert_eq!(ids, ["cached", "gone", "lab"], "found logins are not shown");
    assert_eq!(requests.load(Ordering::SeqCst), 0, "no request at launch");
    let lab = source(&app, "lab");
    assert_eq!(lab.status, datui::home::CloudStatus::Unlisted);
    assert_eq!(lab.count_text(), "not listed");
    assert!(!lab.busy(), "no spinner for a listing nobody asked for");

    let last_run = source(&app, "cached");
    assert_eq!(
        last_run.buckets,
        [std::path::PathBuf::from("s3://from-last-run")]
    );
    assert!(!last_run.busy(), "the cached rows are not being refreshed");
    // A bucket under Recent still opens with the login that listed it last time.
    let resolved =
        datui::cloud_sources::resolve_with("s3://from-last-run/x.parquet", &config.cloud, &env)
            .expect("resolves");
    assert_eq!(resolved.source_id, "cached");

    // Entering the source is the request.
    select(&mut app, "lab");
    app.event(&key(crossterm::event::KeyCode::Enter));
    assert!(
        pump(&mut app, &rx, 10, |app| source(app, "lab").status
            == datui::home::CloudStatus::Listed),
        "entering lists it: {:?}",
        source(&app, "lab")
    );
    assert_eq!(requests.load(Ordering::SeqCst), 1);
    assert_eq!(
        source(&app, "lab").buckets,
        [std::path::PathBuf::from("s3://lab@fast-bucket")]
    );

    // Once a session: back out and in again, and nothing more is asked.
    app.event(&key(crossterm::event::KeyCode::Backspace));
    assert!(pump(&mut app, &rx, 5, |app| app.home.browsing.is_none()
        && row_shown(app, "lab")));
    select(&mut app, "lab");
    app.event(&key(crossterm::event::KeyCode::Enter));
    pump(&mut app, &rx, 1, |_| false);
    assert_eq!(requests.load(Ordering::SeqCst), 1, "listed once a session");

    // A source gone since its row was drawn says so, rather than waiting for good.
    app.event(&key(crossterm::event::KeyCode::Backspace));
    assert!(pump(&mut app, &rx, 5, |app| app.home.browsing.is_none()
        && row_shown(app, "gone")));
    datui::CacheManager::new("datui")
        .expect("isolated cache")
        .hide_cloud_source("gone");
    select(&mut app, "gone");
    app.event(&key(crossterm::event::KeyCode::Enter));
    assert!(
        pump(&mut app, &rx, 5, |app| matches!(
            &source(app, "gone").status,
            datui::home::CloudStatus::Failed { short, .. } if short == "not found"
        )),
        "{:?}",
        source(&app, "gone")
    );
    assert!(app.home.awaiting_listing().is_none(), "nothing to wait for");
    assert_eq!(requests.load(Ordering::SeqCst), 1);
}

//! The `CLOUD` section against local stand-ins for S3 servers, so it runs with every
//! `cargo test`: nothing here leaves the machine.

#![cfg(feature = "cloud")]

mod common;

use std::io::{Read, Write};
use std::net::TcpListener;
use std::time::{Duration, Instant};

/// A server that answers every request with a `ListBuckets` result naming `bucket`.
fn answering_server(bucket: &'static str) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let endpoint = format!("http://{}", listener.local_addr().expect("address"));
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            // Read the request head before answering, so the client is not reset
            // mid-write.
            let mut head = Vec::new();
            let mut byte = [0u8; 1];
            while !head.ends_with(b"\r\n\r\n") && stream.read(&mut byte).unwrap_or(0) == 1 {
                head.push(byte[0]);
            }
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
    endpoint
}

/// A server that accepts connections and never says anything.
fn silent_server() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let endpoint = format!("http://{}", listener.local_addr().expect("address"));
    std::thread::spawn(move || {
        let mut held = Vec::new();
        for stream in listener.incoming().flatten() {
            held.push(stream);
        }
    });
    endpoint
}

fn source(name: &str, endpoint: &str) -> datui::config::CloudSourceConfig {
    datui::config::CloudSourceConfig {
        name: name.to_string(),
        kind: Some("s3".to_string()),
        endpoint_url: Some(endpoint.to_string()),
        region: Some("us-east-1".to_string()),
        access_key_id_env: Some("DATUI_TEST_KEY".to_string()),
        secret_access_key_env: Some("DATUI_TEST_SECRET".to_string()),
        ..Default::default()
    }
}

#[test]
fn structured_public_dataset_metadata_reaches_the_home_screen() {
    common::isolate_cache();
    let mut config = datui::config::AppConfig::default();
    config.data.use_desktop_recents = false;
    config.cloud.hide = ["s3-default", "gcs-default", "az", "azure-env"]
        .map(String::from)
        .to_vec();
    config.cloud.sources = vec![datui::config::CloudSourceConfig {
        name: "public".to_string(),
        label: Some("Curated public data".to_string()),
        public: Some(true),
        datasets: vec![datui::config::PublicDatasetConfig {
            name: "Weather".to_string(),
            url: "s3://weather/parquet/".to_string(),
            description: "Daily observations".to_string(),
            publisher: "Example agency".to_string(),
            license: "CC0".to_string(),
            homepage: "https://example.com/weather".to_string(),
            ..Default::default()
        }],
        ..Default::default()
    }];

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

    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline && !app.home.cloud.iter().any(|source| source.id == "public") {
        if let Ok(event) = rx.recv_timeout(Duration::from_millis(20)) {
            let mut next = Some(event);
            while let Some(event) = next {
                next = app.event(&event);
            }
        }
    }

    let source = app
        .home
        .cloud
        .iter()
        .find(|source| source.id == "public")
        .expect("configured public source");
    let place = std::path::PathBuf::from("s3://weather/parquet/");
    assert_eq!(source.label, "Curated public data");
    assert_eq!(source.buckets, std::slice::from_ref(&place));
    assert_eq!(
        source.names.get(&place).map(String::as_str),
        Some("Weather")
    );
    let details = source.place_details.get(&place).expect("dataset details");
    assert!(
        details
            .iter()
            .any(|(key, value)| key == "about" && value == "Daily observations")
    );
    assert!(
        details
            .iter()
            .any(|(key, value)| key == "license" && value == "CC0")
    );
}

#[test]
fn a_source_that_never_answers_does_not_hold_up_the_others() {
    common::isolate_cache();
    // SAFETY: the only test in this binary, and set before the runtime starts.
    unsafe {
        std::env::set_var("DATUI_TEST_KEY", "key");
        std::env::set_var("DATUI_TEST_SECRET", "secret");
    }
    let mut config = datui::config::AppConfig::default();
    config.data.use_desktop_recents = false;
    config.cloud.sources = vec![
        // Listed first, so a one-at-a-time listing would wait on it.
        source("a-silent", &silent_server()),
        source("b-answering", &answering_server("fast-bucket")),
    ];
    // Whatever this machine happens to be logged in to is not part of the test.
    config.cloud.hide = ["s3-default", "gcs-default", "az", "azure-env"]
        .map(String::from)
        .to_vec();
    config.cloud.list_on_start = Some(true);

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

    let started = Instant::now();
    let deadline = started + Duration::from_secs(15);
    let listed = |app: &datui::App, id: &str| {
        app.home
            .cloud
            .iter()
            .any(|s| s.id == id && s.status == datui::home::CloudStatus::Listed)
    };
    while Instant::now() < deadline && !listed(&app, "b-answering") {
        if let Ok(event) = rx.recv_timeout(Duration::from_millis(50)) {
            let mut next = Some(event);
            while let Some(event) = next {
                next = app.event(&event);
            }
        }
    }

    assert!(
        listed(&app, "b-answering"),
        "the answering source should list: {:?}",
        app.home.cloud
    );
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "and should not wait for the silent one: took {:?}",
        started.elapsed()
    );
    let silent = app
        .home
        .cloud
        .iter()
        .find(|s| s.id == "a-silent")
        .expect("the silent source is still a row");
    assert!(silent.busy(), "still listing: {silent:?}");
    let answering = app
        .home
        .cloud
        .iter()
        .find(|s| s.id == "b-answering")
        .expect("the answering source");
    assert_eq!(
        answering.buckets,
        [std::path::PathBuf::from("s3://b-answering@fast-bucket")]
    );
}

/// A peek that failed, or whose task was lost, comes back as failed: out of the set
/// that spins, and labelled `?` rather than `dir`, which would claim no data inside.
#[test]
fn a_failed_peek_stops_spinning_and_is_not_an_answer() {
    common::isolate_cache();
    let (tx, _rx) = std::sync::mpsc::channel();
    let mut app = datui::App::new_with_config(
        tx,
        common::test_runtime(),
        datui::Theme {
            colors: std::collections::HashMap::new(),
        },
        datui::config::AppConfig::default(),
    );
    let path = std::path::PathBuf::from("gs://pitscope/seasons");
    app.home.peeking.insert(path.clone());
    app.event(&datui::AppEvent::HomeCloudKinds {
        kinds: Vec::new(),
        failed: vec![path.clone()],
    });
    assert!(app.home.peeking.is_empty(), "no spinner left behind");
    assert!(app.home.peek_failed.contains(&path));
    assert!(!app.home.cloud_kinds.contains_key(&path), "not an answer");

    let mut row = datui::discover::Entry::directory(&path);
    row.name = "seasons".to_string();
    assert_eq!(
        app.home.cloud_look(&row),
        Some(datui::home::CloudLook::Failed)
    );
}

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

//! AWS profiles, end to end against a local stand-in for S3 that records which key
//! signed each request. The bug this guards against: with `AWS_PROFILE=work`, listing
//! found no keys at all and opening signed with the first keys in
//! `~/.aws/credentials`, which belonged to `default`.

#![cfg(feature = "cloud")]

mod common;

use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// A server that records each request's `Authorization` header and answers every
/// `GET /` with a bucket list and anything else with 404.
fn recording_server() -> (String, Arc<Mutex<Vec<String>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let endpoint = format!("http://{}", listener.local_addr().expect("address"));
    let seen = Arc::new(Mutex::new(Vec::new()));
    let record = seen.clone();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let mut head = Vec::new();
            let mut byte = [0u8; 1];
            while !head.ends_with(b"\r\n\r\n") && stream.read(&mut byte).unwrap_or(0) == 1 {
                head.push(byte[0]);
            }
            let head = String::from_utf8_lossy(&head).to_string();
            let request_line = head.lines().next().unwrap_or("").to_string();
            let authorization = head
                .lines()
                .find(|l| l.to_ascii_lowercase().starts_with("authorization:"))
                .unwrap_or("")
                .to_string();
            record
                .lock()
                .unwrap()
                .push(format!("{request_line} | {authorization}"));
            if request_line.starts_with("GET / ") {
                let body = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListAllMyBucketsResult>\
                     <Buckets><Bucket><Name>work-bucket</Name></Bucket></Buckets>\
                     </ListAllMyBucketsResult>";
                let _ = write!(
                    stream,
                    "HTTP/1.1 200 OK\r\nContent-Type: application/xml\r\nContent-Length: {}\r\n\
                     Connection: close\r\n\r\n{body}",
                    body.len()
                );
            } else {
                let _ = write!(
                    stream,
                    "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                );
            }
        }
    });
    (endpoint, seen)
}

#[test]
fn listing_and_opening_sign_with_the_active_profile() {
    common::isolate_cache();
    let dir = tempfile::TempDir::new().expect("temp dir");
    let config_file = dir.path().join("config");
    let credentials_file = dir.path().join("credentials");
    std::fs::write(
        &config_file,
        "[default]\nregion = us-east-1\n\n[profile work]\nregion = eu-west-1\n",
    )
    .unwrap();
    // `default` first: the old behaviour took whichever keys came first.
    std::fs::write(
        &credentials_file,
        "[default]\naws_access_key_id = AKIADEFAULT\naws_secret_access_key = default-secret\n\n\
         [work]\naws_access_key_id = AKIAWORK\naws_secret_access_key = work-secret\n",
    )
    .unwrap();
    let (endpoint, seen) = recording_server();

    // SAFETY: the only test in this binary, and set before the runtime starts.
    unsafe {
        for key in [
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_SESSION_TOKEN",
            "AWS_REGION",
            "AWS_DEFAULT_REGION",
            "AWS_ENDPOINT_URL_S3",
            "AWS_ENDPOINT",
        ] {
            std::env::remove_var(key);
        }
        std::env::set_var("AWS_CONFIG_FILE", &config_file);
        std::env::set_var("AWS_SHARED_CREDENTIALS_FILE", &credentials_file);
        std::env::set_var("AWS_PROFILE", "work");
        std::env::set_var("AWS_ENDPOINT_URL", &endpoint);
    }

    let mut config = datui::config::AppConfig::default();
    config.data.use_desktop_recents = false;
    config.cloud.hide = ["gcs-default", "az", "azure-env"]
        .map(String::from)
        .to_vec();
    let options = datui::OpenOptions::default();
    config.cloud = options.effective_cloud(&config.cloud);

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

    let pump = |app: &mut datui::App, seconds: u64, done: &dyn Fn(&datui::App) -> bool| {
        let deadline = Instant::now() + Duration::from_secs(seconds);
        while Instant::now() < deadline && !done(app) {
            if let Ok(event) = rx.recv_timeout(Duration::from_millis(50)) {
                let mut next = Some(event);
                while let Some(event) = next {
                    next = app.event(&event);
                }
            }
        }
        done(app)
    };

    let listed = pump(&mut app, 20, &|app| {
        app.home
            .cloud
            .iter()
            .any(|s| s.id == "s3-default" && !s.buckets.is_empty())
    });
    assert!(
        listed,
        "the default source should list: {:?}",
        app.home.cloud
    );

    // Open an object through the same source. It does not exist, so the open fails,
    // but not before the size probe and the scan have signed their requests.
    let requests_before = seen.lock().unwrap().len();
    let open = datui::AppEvent::Open(
        vec![std::path::PathBuf::from("s3://work-bucket/data.parquet")],
        datui::OpenOptions::default(),
    );
    let mut next = Some(open);
    while let Some(event) = next {
        next = app.event(&event);
    }
    // Until the object itself is asked for: the second profile's listing can land after
    // the snapshot above and would otherwise end the wait early.
    pump(&mut app, 20, &|_| {
        seen.lock()
            .unwrap()
            .iter()
            .any(|r| r.contains("/work-bucket/data.parquet"))
    });

    let seen = seen.lock().unwrap().clone();
    println!("{seen:#?}");
    assert!(
        seen.len() > requests_before,
        "opening should have reached the endpoint"
    );
    // The default source lists as `work`. The `default` profile is a source of its own
    // now, so it lists too, under its own keys.
    let listings: Vec<&String> = seen.iter().filter(|r| r.starts_with("GET / ")).collect();
    assert!(
        listings
            .iter()
            .any(|r| r.contains("Credential=AKIAWORK/") && r.contains("/eu-west-1/")),
        "the default source should list as the work profile: {listings:#?}"
    );
    assert!(
        listings
            .iter()
            .any(|r| r.contains("Credential=AKIADEFAULT/")),
        "the default profile lists as itself: {listings:#?}"
    );
    // Opening a plain URL is the default source's, so the work profile's.
    let object_requests: Vec<&String> = seen
        .iter()
        .filter(|r| r.contains("/work-bucket/data.parquet"))
        .collect();
    assert!(!object_requests.is_empty(), "the object was requested");
    for request in object_requests {
        assert!(
            request.contains("Credential=AKIAWORK/") && request.contains("/eu-west-1/"),
            "every request for the object should be signed by the work profile: {request}"
        );
    }
}

//! `[[sources]]` collections on the home screen: local and remote datasets under one
//! heading, opened and browsed the way the rest of the home screen opens and browses.
#![cfg(all(feature = "cloud", feature = "http"))]

mod common;

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use datui::config::{AppConfig, CloudDiscover};
use datui::{App, AppEvent};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::mpsc::Receiver;
use std::time::{Duration, Instant};

fn key(code: KeyCode) -> AppEvent {
    AppEvent::Key(KeyEvent::new(code, KeyModifiers::NONE))
}

fn drive(app: &mut App, event: AppEvent) {
    let mut next = Some(event);
    while let Some(event) = next {
        if let AppEvent::Crash(message) = &event {
            panic!("{message}");
        }
        next = app.event(&event);
    }
}

/// Handle events and draw frames until `done`, as the event loop would.
#[track_caller]
fn pump(app: &mut App, rx: &Receiver<AppEvent>, done: impl Fn(&App) -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !done(app) && Instant::now() < deadline {
        if let Ok(event) = rx.recv_timeout(Duration::from_millis(20)) {
            drive(app, event);
        }
        let area = ratatui::layout::Rect::new(0, 0, 140, 40);
        let mut buffer = ratatui::buffer::Buffer::empty(area);
        ratatui::widgets::Widget::render(&mut *app, area, &mut buffer);
        app.request_what_the_frame_needs();
    }
    assert!(
        done(app),
        "timed out: status {:?}, sections {:?}",
        app.home.status,
        app.home
            .sections
            .iter()
            .map(|s| s.title.clone())
            .collect::<Vec<_>>()
    );
}

/// The defaults with `extra` merged over them, and no login found on this machine in
/// the way.
fn config_with(extra: &str) -> AppConfig {
    common::isolate_cache();
    let mut config = AppConfig::default();
    config.data.use_desktop_recents = false;
    config.cloud.discover = Some(CloudDiscover::None);
    config.merge(toml::from_str(extra).expect("test config parses"));
    config.validate().expect("test config validates");
    config
}

/// An app at the home screen with [`config_with`] `extra`.
fn home_with(extra: &str) -> (App, Receiver<AppEvent>) {
    let (tx, rx) = std::sync::mpsc::channel();
    let mut app = App::new_with_config(
        tx,
        common::test_runtime(),
        datui::Theme {
            colors: std::collections::HashMap::new(),
        },
        config_with(extra),
    );
    app.enter_home();
    (app, rx)
}

fn section_titles(app: &App) -> Vec<String> {
    app.home.sections.iter().map(|s| s.title.clone()).collect()
}

fn select(app: &mut App, name: &str) {
    let index = app
        .home
        .visible()
        .iter()
        .position(|row| matches!(row, datui::home::Row::Entry { entry, .. } if entry.name == name))
        .unwrap_or_else(|| panic!("{name} is not on screen"));
    app.home.selected = index;
}

/// Serve `body` as `name` over plain HTTP on a local port, and return its URL.
fn serve(name: &str, body: &'static str) -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}/{name}", listener.local_addr().unwrap());
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let mut head = Vec::new();
            let mut byte = [0u8; 1];
            while !head.ends_with(b"\r\n\r\n") && stream.read(&mut byte).unwrap_or(0) == 1 {
                head.push(byte[0]);
            }
            let get = head.starts_with(b"GET");
            let _ = write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: text/csv\r\nContent-Length: {}\r\n\
                 Connection: close\r\n\r\n{}",
                body.len(),
                if get { body } else { "" }
            );
        }
    });
    url
}

fn toml_path(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

#[test]
fn one_collection_holds_local_s3_and_web_datasets() {
    let dir = tempfile::TempDir::new().unwrap();
    let sales = dir.path().join("sales.csv");
    std::fs::write(&sales, "month,total\njan,10\nfeb,12\n").unwrap();
    let archive = dir.path().join("archive");
    std::fs::create_dir_all(archive.join("old")).unwrap();
    std::fs::write(archive.join("2024.csv"), "month,total\njan,1\n").unwrap();
    let gone = dir.path().join("gone.parquet");
    let penguins = serve(
        "penguins.csv",
        "species,island\nAdelie,Torgersen\nGentoo,Biscoe\n",
    );
    let config = format!(
        r#"
[[cloud.connections]]
name = "onprem"
kind = "s3"
endpoint_url = "http://127.0.0.1:9"
access_key_id_env = "DATUI_TEST_UNSET_ONPREM_KEY"
secret_access_key_env = "DATUI_TEST_UNSET_ONPREM_SECRET"

[[sources]]
name = "my-datasets"
label = "My datasets"

[[sources.datasets]]
name = "Sales"
path = "{sales}"
description = "Monthly sales"

[[sources.datasets]]
name = "Archive"
path = "{archive}"

[[sources.datasets]]
name = "Gone"
path = "{gone}"

[[sources.datasets]]
name = "Weather"
url = "s3://noaa-ghcn-pds/parquet/"
auth = "anonymous"

[[sources.datasets]]
name = "Penguins"
url = "{penguins}"

[[sources.datasets]]
name = "Orders"
url = "s3://datui-test-orders/2024/"
connection = "onprem"
"#,
        sales = toml_path(&sales),
        archive = toml_path(&archive),
        gone = toml_path(&gone),
    );
    let (mut app, rx) = home_with(&config);
    pump(&mut app, &rx, |app| {
        app.home
            .sections
            .iter()
            .any(|s| s.title == "My datasets" && s.rows.len() == 6)
    });
    let titles = section_titles(&app);
    assert!(
        titles.iter().position(|t| t == "My datasets")
            < titles.iter().position(|t| t == "Public datasets"),
        "the built-in catalog follows: {titles:?}"
    );
    assert!(app.home.cloud.is_empty(), "no collection is a cloud row");

    // What each row is reading comes from the config, not from asking the store.
    let details = |app: &App, path: &Path| app.home.place_details(path).unwrap().to_vec();
    assert!(details(&app, &sales).contains(&("about".to_string(), "Monthly sales".to_string())));
    let weather = PathBuf::from("s3://noaa-ghcn-pds/parquet/");
    assert!(details(&app, &weather).contains(&("login".to_string(), "none".to_string())));
    let orders = PathBuf::from("s3://datui-test-orders/2024/");
    assert!(details(&app, &orders).contains(&("login".to_string(), "onprem".to_string())));

    // Weather is read anonymously; Orders only through its connection, whose keys are
    // not set here, so it says whose they are rather than trying anyone else's.
    let env = datui::cloud_browse::Environment::current();
    let cloud = &config_with(&config).cloud;
    let read =
        datui::cloud_sources::resolve_with("s3://noaa-ghcn-pds/parquet/by_year/", cloud, &env)
            .unwrap();
    assert_eq!(read.signing, datui::cloud_sources::Signing::Unsigned);
    assert_eq!(read.source_id, "my-datasets");
    let refused =
        datui::cloud_sources::resolve_with("s3://datui-test-orders/2024/q1.csv", cloud, &env)
            .expect_err("the connection has no keys");
    assert!(refused.contains("onprem"), "{refused}");

    // A missing file stays listed, and says so where it was asked for.
    select(&mut app, "Gone");
    drive(&mut app, key(KeyCode::Enter));
    assert_eq!(app.home.browsing, None);
    assert!(
        app.home
            .status
            .as_deref()
            .is_some_and(|s| s.contains("does not exist")),
        "{:?}",
        app.home.status
    );

    // A local directory is browsed, and Esc comes back.
    select(&mut app, "Archive");
    drive(&mut app, key(KeyCode::Enter));
    assert_eq!(app.home.browsing.as_deref(), Some(archive.as_path()));
    pump(&mut app, &rx, |app| {
        app.home.visible().iter().any(
            |row| matches!(row, datui::home::Row::Entry { entry, .. } if entry.name == "2024.csv"),
        )
    });
    drive(&mut app, key(KeyCode::Esc));
    assert_eq!(app.home.browsing, None);
    pump(&mut app, &rx, |app| {
        app.home.sections.iter().any(|s| s.title == "My datasets")
    });

    // A local file opens.
    select(&mut app, "Sales");
    drive(&mut app, key(KeyCode::Enter));
    pump(&mut app, &rx, |app| {
        app.data_table_state.is_some() && !app.is_busy()
    });
    assert_eq!(
        app.data_table_state.as_ref().unwrap().headers(),
        ["month", "total"]
    );

    // A web file opens through the download the home screen always uses.
    drive(
        &mut app,
        AppEvent::Key(KeyEvent::new(KeyCode::Char('o'), KeyModifiers::CONTROL)),
    );
    pump(&mut app, &rx, |app| {
        app.home.sections.iter().any(|s| s.title == "My datasets")
    });
    select(&mut app, "Penguins");
    drive(&mut app, key(KeyCode::Enter));
    pump(&mut app, &rx, |app| {
        app.awaiting_download_confirmation()
            || app
                .data_table_state
                .as_ref()
                .is_some_and(|t| t.headers().first().map(String::as_str) == Some("species"))
    });
    if app.awaiting_download_confirmation() {
        drive(&mut app, key(KeyCode::Enter));
    }
    pump(&mut app, &rx, |app| {
        !app.is_busy()
            && app
                .data_table_state
                .as_ref()
                .is_some_and(|t| t.headers().first().map(String::as_str) == Some("species"))
    });
}

#[test]
fn the_catalog_is_replaced_dropped_or_hidden() {
    let dir = tempfile::TempDir::new().unwrap();
    let local = dir.path().join("mine.csv");
    std::fs::write(&local, "a\n1\n").unwrap();
    let listed = |extra: &str| {
        let (mut app, rx) = home_with(extra);
        pump(&mut app, &rx, |app| !app.home.sections.is_empty());
        section_titles(&app)
    };

    let default = listed("");
    assert!(
        default.iter().any(|t| t == "Public datasets"),
        "{default:?}"
    );

    let replaced = listed(&format!(
        "[[sources]]\nname = \"public\"\nlabel = \"Curated\"\n[[sources.datasets]]\nname = \"Mine\"\npath = \"{}\"\n",
        toml_path(&local)
    ));
    assert!(replaced.iter().any(|t| t == "Curated"), "{replaced:?}");
    assert!(
        !replaced.iter().any(|t| t == "Public datasets"),
        "replaced whole: {replaced:?}"
    );

    let dropped = listed("[data]\nbuiltin_catalog = false\n");
    assert!(
        !dropped.iter().any(|t| t == "Public datasets"),
        "{dropped:?}"
    );

    let hidden = listed(&format!(
        "[data]\nhide_sources = [\"public\"]\n[[sources]]\nname = \"public\"\nlabel = \"Curated\"\n[[sources.datasets]]\nname = \"Mine\"\npath = \"{}\"\n",
        toml_path(&local)
    ));
    assert!(
        !hidden
            .iter()
            .any(|t| t == "Curated" || t == "Public datasets"),
        "a hidden replacement is hidden too: {hidden:?}"
    );
}

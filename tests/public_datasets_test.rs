//! A named web file in a collection uses the normal download and open path.

#![cfg(all(feature = "cloud", feature = "http"))]

mod common;

use std::io::{Read, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use datui::{App, AppEvent};

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
        // A job that failed says so on screen.
        if let Some(message) = app.error_message() {
            panic!("{message}");
        }
    }
}

#[track_caller]
fn pump(app: &mut App, rx: &std::sync::mpsc::Receiver<AppEvent>, done: impl Fn(&App) -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !done(app) && Instant::now() < deadline {
        if let Ok(event) = rx.recv_timeout(Duration::from_millis(20)) {
            drive(app, event);
        }
        let area = ratatui::layout::Rect::new(0, 0, 120, 30);
        let mut buffer = ratatui::buffer::Buffer::empty(area);
        ratatui::widgets::Widget::render(&mut *app, area, &mut buffer);
        app.request_what_the_frame_needs();
    }
    assert!(
        done(app),
        "timed out: {:?}, rows: {:?}",
        app.home.status,
        app.home.visible()
    );
}

#[test]
fn a_web_file_in_a_collection_is_fetched_only_when_opened() {
    common::isolate_cache();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}/foods.csv", listener.local_addr().unwrap());
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
            let body = "food,protein_g\nyogurt,10\noatmeal,3\n";
            let _ = write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: text/csv\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                if head.starts_with(b"GET") { body } else { "" }
            );
        }
    });
    let config = common::layered_config(&[
        "[data]\nuse_desktop_recents = false\n[cloud]\ndiscover = false\n",
        &format!(
            "[[sources]]\nname = \"public\"\n[[sources.datasets]]\nname = \"Foods\"\nurl = {url:?}\n"
        ),
    ]);
    config.validate().unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    let mut app = App::new_with_config(
        tx,
        common::test_runtime(),
        datui::Theme {
            colors: Default::default(),
        },
        config,
    );
    app.enter_home();
    pump(&mut app, &rx, |app| {
        app.home.visible().iter().any(
            |row| matches!(row, datui::home::Row::Entry { entry, .. } if entry.name == "Foods"),
        )
    });
    app.home.selected = app
        .home
        .visible()
        .iter()
        .position(
            |row| matches!(row, datui::home::Row::Entry { entry, .. } if entry.name == "Foods"),
        )
        .unwrap();
    assert_eq!(app.what_enter_does(), datui::WhatEnter::OpensFile);
    assert_eq!(
        requests.load(Ordering::SeqCst),
        0,
        "listing does not fetch a file"
    );
    drive(&mut app, key(KeyCode::Right));
    assert_eq!(app.home.browsing, None, "a web file has no inside");
    assert_eq!(
        requests.load(Ordering::SeqCst),
        0,
        "Right does not list an HTTP URL"
    );
    drive(&mut app, key(KeyCode::Enter));
    pump(&mut app, &rx, App::awaiting_open_confirmation);
    drive(&mut app, key(KeyCode::Enter));
    pump(&mut app, &rx, |app| {
        app.data_table_state.is_some() && !app.is_busy()
    });
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.headers(), ["food", "protein_g"]);
    assert!(requests.load(Ordering::SeqCst) > 0);
    drive(&mut app, key(KeyCode::Char('q')));
    assert_eq!(app.input_mode, datui::InputMode::Home);
    assert_eq!(
        app.home.browsing, None,
        "back to the listing it was opened from"
    );
}

/// A web file opened from a collection comes back under Recent by the collection's
/// name for it, with the shape its open measured: nothing lists a web file, so the
/// open is what has to remember it (#547 D12).
#[test]
fn a_downloaded_dataset_comes_back_named_and_measured() {
    common::isolate_cache();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}/penguins.csv", listener.local_addr().unwrap());
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let mut head = Vec::new();
            let mut byte = [0u8; 1];
            while !head.ends_with(b"\r\n\r\n") && stream.read(&mut byte).unwrap_or(0) == 1 {
                head.push(byte[0]);
            }
            let body = "species,island,mass\nAdelie,Torgersen,3750\nGentoo,Biscoe,5000\nChinstrap,Dream,3800\n";
            let _ = write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: text/csv\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                if head.starts_with(b"GET") { body } else { "" }
            );
        }
    });
    let config = common::layered_config(&[
        "[data]\nuse_desktop_recents = false\n[cloud]\ndiscover = false\n",
        &format!(
            "[[sources]]\nname = \"birds\"\n[[sources.datasets]]\nname = \"Palmer penguins\"\nurl = {url:?}\n"
        ),
    ]);
    config.validate().unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    let mut app = App::new_with_config(
        tx,
        common::test_runtime(),
        datui::Theme {
            colors: Default::default(),
        },
        config,
    );
    let cache = tempfile::TempDir::new().unwrap();
    let manager = datui::CacheManager::with_dir(cache.path().to_path_buf());
    app.use_cache(manager.clone());
    app.enter_home();
    let named = |app: &App| {
        app.home.visible().iter().position(|row| {
            matches!(row, datui::home::Row::Entry { entry, .. } if entry.name == "Palmer penguins")
        })
    };
    pump(&mut app, &rx, |app| named(app).is_some());
    app.home.selected = named(&app).unwrap();
    drive(&mut app, key(KeyCode::Enter));
    pump(&mut app, &rx, App::awaiting_open_confirmation);
    drive(&mut app, key(KeyCode::Enter));
    pump(&mut app, &rx, |app| {
        app.data_table_state.is_some() && !app.is_busy()
    });
    // Its shape is kept under the URL it was opened from.
    let key_url = std::path::PathBuf::from(&url);
    pump(&mut app, &rx, |_| {
        manager
            .load_dataset_facts()
            .get(&key_url)
            .is_some_and(|facts| facts.rows == Some(3) && facts.cols == Some(3))
    });

    drive(&mut app, key(KeyCode::Char('q')));
    // Under Recent, not only the collection's own row, which shares its URL.
    let recent = |app: &App| {
        app.home
            .sections
            .iter()
            .find(|s| s.title == datui::home::HomeState::RECENT_SECTION)
            .and_then(|s| {
                s.rows
                    .iter()
                    .find(|e| e.path == key_url && e.rows.is_some())
                    .cloned()
            })
    };
    pump(&mut app, &rx, |app| recent(app).is_some());
    let entry = recent(&app).unwrap();
    assert_eq!(entry.name, "Palmer penguins");
    assert_eq!((entry.rows, entry.cols), (Some(3), Some(3)));
}

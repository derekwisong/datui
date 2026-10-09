//! Catalogs on the home screen: local and remote datasets under one heading, opened
//! and browsed the way the rest of the home screen opens and browses; `catalog.toml`,
//! which Ctrl+D writes; and the Documentation view.

use crate::common;
use crate::fake_s3;

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use datui::config::AppConfig;
use datui::{App, AppEvent};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::mpsc::Receiver;
use std::time::{Duration, Instant};

fn key(code: KeyCode) -> AppEvent {
    AppEvent::Key(KeyEvent::new(code, KeyModifiers::NONE))
}

fn ctrl(c: char) -> AppEvent {
    AppEvent::Key(KeyEvent::new(KeyCode::Char(c), KeyModifiers::CONTROL))
}

fn drive(app: &mut App, event: AppEvent) {
    let mut next = Some(event);
    while let Some(event) = next {
        if let AppEvent::Crash(message) = &event {
            panic!("{message}");
        }
        next = app.event(event);
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

/// The defaults with `extra` merged over them, the catalog files in `dir` read, and no
/// login found on this machine in the way.
fn config_in(dir: &Path, extra: &str) -> AppConfig {
    common::isolate_cache();
    let mut config = common::layered_config(&[
        "[home]\ndesktop_recents = false\n[cloud]\ndiscover = false\n",
        extra,
    ]);
    config
        .read_catalog_files(Some(dir))
        .expect("test catalogs read");
    config.validate().expect("test config validates");
    config
}

/// An app at the home screen with [`config_in`] `dir` and `extra`.
fn home_in(dir: &Path, extra: &str) -> (App, Receiver<AppEvent>) {
    let (tx, rx) = std::sync::mpsc::channel();
    let mut app = App::new_with_config(
        tx,
        common::test_runtime(),
        datui::Theme {
            colors: std::collections::HashMap::new(),
        },
        config_in(dir, extra),
    );
    app.enter_home();
    (app, rx)
}

/// An app whose `catalog.toml` is `mine`, in a directory of its own.
fn home_with_mine(mine: &str) -> (App, Receiver<AppEvent>, tempfile::TempDir) {
    let dir = tempfile::TempDir::new().unwrap();
    std::fs::write(dir.path().join("catalog.toml"), mine).unwrap();
    let (app, rx) = home_in(dir.path(), "");
    (app, rx, dir)
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
    app.home.select(index);
}

/// The footer: the last line drawn.
fn footer(app: &mut App) -> String {
    screen(app)
        .lines()
        .last()
        .unwrap_or_default()
        .trim_end()
        .to_string()
}

fn screen(app: &mut App) -> String {
    let area = ratatui::layout::Rect::new(0, 0, 140, 40);
    let mut buffer = ratatui::buffer::Buffer::empty(area);
    ratatui::widgets::Widget::render(&mut *app, area, &mut buffer);
    (0..area.height)
        .map(|y| {
            (0..area.width)
                .map(|x| buffer[(x, y)].symbol())
                .collect::<String>()
                + "\n"
        })
        .collect()
}

/// Serve `body` as `name` over plain HTTP on a local port, and return its URL. A HEAD
/// gets the length and no body.
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
fn one_catalog_holds_local_s3_and_web_datasets() {
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
    let config = r#"
[[cloud.connections]]
name = "onprem"
kind = "s3"
endpoint_url = "http://127.0.0.1:9"
access_key_id_env = "DATUI_TEST_UNSET_ONPREM_KEY"
secret_access_key_env = "DATUI_TEST_UNSET_ONPREM_SECRET"
"#;
    let mine = format!(
        r#"
[sales]
name = "Sales"
path = "{sales}"
description = "Monthly sales"

[archive]
name = "Archive"
path = "archive"

[gone]
name = "Gone"
path = "{gone}"

[weather]
name = "Weather"
url = "s3://noaa-ghcn-pds/parquet/"
auth = "anonymous"

[penguins]
name = "Penguins"
url = "{penguins}"

[orders]
name = "Orders"
url = "s3://datui-test-orders/2024/"
connection = "onprem"
"#,
        sales = toml_path(&sales),
        gone = toml_path(&gone),
    );
    std::fs::write(dir.path().join("catalog.toml"), mine).unwrap();
    let (mut app, rx) = home_in(dir.path(), config);
    pump(&mut app, &rx, |app| {
        app.home
            .sections
            .iter()
            .any(|s| s.title == "My datasets" && s.rows.len() == 6)
    });
    let titles = section_titles(&app);
    assert!(
        titles.iter().position(|t| t == "My datasets")
            < titles.iter().position(|t| t == "Example datasets"),
        "the bundled catalog follows: {titles:?}"
    );
    assert!(app.home.cloud.is_empty(), "no catalog is a cloud row");
    let section = app
        .home
        .sections
        .iter()
        .find(|s| s.title == "My datasets")
        .unwrap();
    assert_eq!(section.origin, Some("catalog.toml"));

    // What each row is reading comes from the catalog, not from asking the store.
    let details = |app: &App, path: &Path| app.home.place_details(path).unwrap().to_vec();
    assert!(details(&app, &sales).contains(&("about".to_string(), "Monthly sales".to_string())));
    let weather = PathBuf::from("s3://noaa-ghcn-pds/parquet/");
    assert!(details(&app, &weather).contains(&("login".to_string(), "none".to_string())));
    let orders = PathBuf::from("s3://datui-test-orders/2024/");
    assert!(details(&app, &orders).contains(&("login".to_string(), "onprem".to_string())));

    // Weather is read anonymously; Orders only through its connection, whose keys are
    // not set here, so it says whose they are rather than trying anyone else's.
    let env = datui::cloud::cloud_browse::Environment::current();
    let cloud = &config_in(dir.path(), config).cloud;
    let read = datui::cloud::cloud_sources::resolve_with(
        "s3://noaa-ghcn-pds/parquet/by_year/",
        cloud,
        &env,
    )
    .unwrap();
    assert_eq!(read.signing, datui::cloud::cloud_sources::Signing::Unsigned);
    assert_eq!(read.source_id, "mine");
    let refused = datui::cloud::cloud_sources::resolve_with(
        "s3://datui-test-orders/2024/q1.csv",
        cloud,
        &env,
    )
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

    // A directory, written relative to the catalog, is browsed, and Esc comes back.
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
    drive(&mut app, ctrl('o'));
    pump(&mut app, &rx, |app| {
        app.home.sections.iter().any(|s| s.title == "My datasets")
    });
    select(&mut app, "Penguins");
    drive(&mut app, key(KeyCode::Enter));
    pump(&mut app, &rx, |app| {
        app.awaiting_open_confirmation()
            || app
                .data_table_state
                .as_ref()
                .is_some_and(|t| t.headers().first().map(String::as_str) == Some("species"))
    });
    if app.awaiting_open_confirmation() {
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
fn catalogs_are_listed_replaced_or_hidden() {
    let dir = tempfile::TempDir::new().unwrap();
    let local = dir.path().join("mine.csv");
    std::fs::write(&local, "a\n1\n").unwrap();
    let team = dir.path().join("team");
    std::fs::create_dir_all(&team).unwrap();
    std::fs::write(
        team.join("acme.toml"),
        format!(
            "label = \"Acme\"\n[mine]\nname = \"Mine\"\npath = \"{}\"\n",
            toml_path(&local)
        ),
    )
    .unwrap();
    std::fs::write(
        team.join("examples.toml"),
        format!(
            "label = \"Curated\"\n[mine]\nname = \"Mine\"\npath = \"{}\"\n",
            toml_path(&local)
        ),
    )
    .unwrap();
    // A layer read from no file anchors nothing: the paths are whole here.
    let at = |name: &str| toml_path(&team.join(name));
    let listed = |extra: &str| {
        let (mut app, rx) = home_in(dir.path(), extra);
        pump(&mut app, &rx, |app| !app.home.sections.is_empty());
        section_titles(&app)
    };

    let default = listed("");
    assert!(
        default.iter().any(|t| t == "Example datasets"),
        "{default:?}"
    );
    assert!(
        !default.iter().any(|t| t == "My datasets"),
        "no catalog.toml"
    );

    let teamed = listed(&format!("catalogs = [\"{}\"]\n", at("acme.toml")));
    let acme = teamed.iter().position(|t| t == "Acme");
    let public = teamed.iter().position(|t| t == "Example datasets");
    assert!(acme.is_some() && acme < public, "{teamed:?}");

    let replaced = listed(&format!("catalogs = [\"{}\"]\n", at("examples.toml")));
    assert!(replaced.iter().any(|t| t == "Curated"), "{replaced:?}");
    assert!(
        !replaced.iter().any(|t| t == "Example datasets"),
        "replaced whole: {replaced:?}"
    );

    let hidden = listed(&format!(
        "catalogs = [\"{}\"]\n[home]\nhide = [\"examples\", \"acme\"]\n",
        at("acme.toml")
    ));
    assert!(
        !hidden
            .iter()
            .any(|t| t == "Acme" || t == "Example datasets"),
        "{hidden:?}"
    );

    // A missing listed file is skipped; a broken one names its line.
    let skipped = listed(&format!("catalogs = [\"{}\"]\n", at("nowhere.toml")));
    assert!(
        skipped.iter().any(|t| t == "Example datasets"),
        "{skipped:?}"
    );
    std::fs::write(team.join("broken.toml"), "[a]\nname = \"A\"\n").unwrap();
    let mut config =
        common::layered_config(&[&format!("catalogs = [\"{}\"]\n", at("broken.toml"))]);
    config
        .read_catalog_files(Some(dir.path()))
        .expect("a broken file is left out");
    let error = config.broken_catalogs[0].full();
    assert!(
        error.contains("broken.toml:1: [a]: say where it is"),
        "{error}"
    );

    // On the home screen its section says so, in one line.
    let (mut app, rx) = home_in(
        dir.path(),
        &format!("catalogs = [\"{}\"]\n", at("broken.toml")),
    );
    pump(&mut app, &rx, |app| {
        app.home.sections.iter().any(|s| s.title == "broken")
    });
    let section = app
        .home
        .sections
        .iter()
        .find(|s| s.title == "broken")
        .unwrap();
    assert!(section.unavailable);
    let note = section.unavailable_note.clone().unwrap_or_default();
    assert!(
        note.contains("broken.toml:1 [a]: say where it is"),
        "{note}"
    );
    let shown = screen(&mut app);
    assert!(shown.contains("BROKEN"), "{shown}");
    assert!(shown.contains("broken.toml:1"), "{shown}");
}

/// A catalog.toml with a mistake is left out, and Ctrl+D refuses to write over it.
#[test]
fn ctrl_d_never_writes_over_a_broken_catalog_toml() {
    let data = tempfile::TempDir::new().unwrap();
    let file = data.path().join("sales.csv");
    std::fs::write(&file, "a\n1\n").unwrap();
    let dir = tempfile::TempDir::new().unwrap();
    let mine = dir.path().join("catalog.toml");
    let broken = "# keep me\n[sales]\nname = \"Sales\"\n";
    std::fs::write(&mine, broken).unwrap();
    let team = dir.path().join("team.toml");
    std::fs::write(
        &team,
        format!(
            "[sales]\nname = \"Sales\"\npath = \"{}\"\n",
            toml_path(&file)
        ),
    )
    .unwrap();
    let (mut app, rx) = home_in(
        dir.path(),
        &format!("catalogs = [\"{}\"]\n", toml_path(&team)),
    );
    pump(&mut app, &rx, |app| {
        app.home
            .sections
            .iter()
            .any(|s| s.title == "TEAM" || s.title == "team")
    });
    assert!(
        app.home
            .sections
            .iter()
            .any(|s| s.unavailable && s.origin == Some("catalog.toml")),
        "catalog.toml's section says it is broken"
    );
    select(&mut app, "Sales");
    drive(&mut app, ctrl('d'));
    assert!(app.error_message().is_some(), "refused out loud");
    assert_eq!(std::fs::read_to_string(&mine).unwrap(), broken, "untouched");
}

/// A catalog entry's documentation and bookmarks (#734): the bookmark is listed under
/// its dataset, Enter opens it whole, and the dataset opened knows what its columns
/// mean.
#[test]
fn a_bookmark_is_listed_under_its_dataset_and_opens_with_its_documentation() {
    use polars::prelude::*;
    let mut df = df!(
        "ID" => ["USW00094728", "USW00094728"],
        "DATE" => ["20240101", "20240102"],
        "DATA_VALUE" => [56i64, 72],
        "Q_FLAG" => [None, Some("S")],
    )
    .unwrap();
    let mut bytes = Vec::new();
    ParquetWriter::new(&mut bytes).finish(&mut df).unwrap();
    let objects = [
        (
            "ghcn/by_year/YEAR=2024/ELEMENT=TMAX/part-0.parquet".to_string(),
            bytes.clone(),
        ),
        (
            "ghcn/by_year/YEAR=2024/ELEMENT=TMIN/part-0.parquet".to_string(),
            bytes,
        ),
    ]
    .into_iter()
    .collect();
    let s3 = fake_s3::FakeS3::serve("weather", objects);
    let config = format!(
        r#"
[[cloud.connections]]
name = "lab"
kind = "s3"
endpoint_url = "{endpoint}"
region = "us-east-1"
addressing = "path"
access_key_id_env = "CARGO_PKG_NAME"
secret_access_key_env = "CARGO_PKG_NAME"
"#,
        endpoint = s3.endpoint
    );
    let dir = tempfile::TempDir::new().unwrap();
    std::fs::write(
        dir.path().join("catalog.toml"),
        r#"
label = "Mine"

[weather]
name = "Weather"
url = "s3://weather/ghcn/"
connection = "lab"
documentation = "https://example.com/readme.txt"

columns.Q_FLAG = { description = "Quality flag", values = { "" = "did not fail any quality assurance check" } }

bookmarks."Daily highs, 2024" = "by_year/YEAR=2024/ELEMENT=TMAX/"
"#,
    )
    .unwrap();
    let (mut app, rx) = home_in(dir.path(), &config);
    pump(&mut app, &rx, |app| {
        app.home.sections.iter().any(|s| s.title == "Mine")
    });
    let rows: Vec<(String, bool)> = app
        .home
        .visible()
        .iter()
        .filter_map(|row| match row {
            datui::home::Row::Entry { entry, nested, .. } => Some((entry.name.clone(), *nested)),
            _ => None,
        })
        .collect();
    let at = rows
        .iter()
        .position(|(name, _)| name == "Weather")
        .expect("the dataset is listed");
    assert_eq!(
        rows[at + 1],
        ("Daily highs, 2024".to_string(), true),
        "the bookmark sits under its dataset: {rows:?}"
    );

    // The details pane says what the columns mean, and where that comes from; the
    // footer, not the pane, offers the whole page.
    select(&mut app, "Weather");
    let shown = screen(&mut app);
    assert!(shown.contains("COLUMNS"), "{shown}");
    assert!(!shown.contains("^E Documentation"), "{shown}");
    assert!(shown.contains("Quality flag"), "{shown}");
    assert!(shown.contains("https://example.com/readme.txt"), "{shown}");

    // Ctrl+E opens the whole page, a bookmark's too, and Esc comes back. The footer
    // offers it on a catalog row.
    select(&mut app, "Daily highs, 2024");
    let line = footer(&mut app);
    assert!(line.contains("^E Docs"), "{line}");
    drive(&mut app, ctrl('e'));
    assert!(app.info.documentation.is_open());
    let line = footer(&mut app);
    assert!(
        line.contains("documentation") && line.ends_with("? keys"),
        "{line}"
    );
    assert!(!line.contains("^E"), "the page names its own keys: {line}");
    let page = screen(&mut app);
    assert!(page.contains("Documentation"), "{page}");
    assert!(page.contains("BOOKMARKS"), "{page}");
    assert!(page.contains("Q_FLAG"), "{page}");
    // Typing goes to the page, not the filter.
    drive(&mut app, key(KeyCode::Char('j')));
    assert!(app.home.filter.is_empty());
    drive(&mut app, key(KeyCode::Esc));
    assert!(!app.info.documentation.is_open());

    // Enter opens the bookmark whole, and the dataset carries the notes.
    select(&mut app, "Daily highs, 2024");
    assert_eq!(app.what_enter_does(), datui::WhatEnter::OpensDirectory);
    drive(&mut app, key(KeyCode::Enter));
    pump(&mut app, &rx, |app| {
        app.data_table_state.is_some() && !app.is_busy()
    });
    let headers = app.data_table_state.as_ref().unwrap().headers();
    assert!(headers.iter().any(|h| h == "Q_FLAG"), "{headers:?}");
    let book = app.info.codebook.as_ref().expect("the notes came with it");
    assert_eq!(
        book.column("Q_FLAG")
            .and_then(|c| c.legend_line(None))
            .as_deref(),
        Some("blank = did not fail any quality assurance check")
    );
    // Info offers the same page on its Documentation tab.
    let (label, entry) = app
        .info
        .catalog_entry
        .clone()
        .expect("the entry came with it");
    assert_eq!((label.as_str(), entry.name.as_str()), ("Mine", "Weather"));
    assert!(app.info.info_documentation.is_open());
}

/// Ctrl+D adds a row to catalog.toml, keeping what is written there, and on a row of
/// catalog.toml forgets it.
#[test]
fn ctrl_d_adds_a_row_to_catalog_toml_and_forgets_it() {
    let data = tempfile::TempDir::new().unwrap();
    let lake = data.path().join("lake");
    std::fs::create_dir_all(&lake).unwrap();
    std::fs::write(lake.join("a.csv"), "x\n1\n").unwrap();
    let dir = tempfile::TempDir::new().unwrap();
    let team = dir.path().join("team.toml");
    std::fs::write(
        &team,
        format!(
            "label = \"Team\"\n[lake]\nname = \"Lake\"\npath = \"{}\"\n",
            toml_path(&lake)
        ),
    )
    .unwrap();
    let mine = dir.path().join("catalog.toml");
    std::fs::write(&mine, "# my notes\nlabel = \"Mine\"\n").unwrap();
    let (mut app, rx) = home_in(
        dir.path(),
        &format!("catalogs = [\"{}\"]\n", toml_path(&team)),
    );
    pump(&mut app, &rx, |app| {
        app.home.sections.iter().any(|s| s.title == "Team")
    });

    // Moving the selection never moves the footer: each slot keeps its place on
    // every row, offered or not, and so does help.
    let column = |line: &str, text: &str| line.find(text).map(|at| line[..at].chars().count());
    let mut seen: [std::collections::BTreeSet<usize>; 4] = Default::default();
    let mut offered = [0; 3];
    let rows = app.home.visible().len();
    for row in 0..rows {
        app.home.select(row);
        let line = footer(&mut app);
        for (i, text) in ["Enter", "^D", "^E", "? keys"].iter().enumerate() {
            if let Some(at) = column(&line, text) {
                seen[i].insert(at);
                if i < 3 {
                    offered[i] += 1;
                }
            }
        }
    }
    for (i, at) in seen.iter().enumerate() {
        assert!(at.len() <= 1, "slot {i} moved between rows: {at:?}");
    }
    assert_eq!(seen[3].len(), 1, "help is on every row");
    assert!(offered[1] > 0 && offered[2] > 0, "{offered:?}");
    assert!(
        offered[1] < rows || offered[2] < rows,
        "some row lacks a slot, so the check means something: {offered:?}"
    );

    select(&mut app, "Lake");
    let line = footer(&mut app);
    assert!(
        line.contains("^D Add"),
        "the footer names what Ctrl+D does: {line}"
    );
    drive(&mut app, ctrl('d'));
    pump(&mut app, &rx, |app| {
        app.home.sections.iter().any(|s| s.title == "Mine")
    });
    let text = std::fs::read_to_string(&mine).unwrap();
    assert!(text.starts_with("# my notes\n"), "{text}");
    assert!(text.contains("[lake]\nname = \"Lake\""), "{text}");
    assert_eq!(
        std::fs::read_to_string(&team)
            .unwrap()
            .matches("[lake]")
            .count(),
        1,
        "only catalog.toml is written"
    );

    // The row under Mine is catalog.toml's: Ctrl+D there forgets it.
    let mine_section = app
        .home
        .sections
        .iter()
        .position(|s| s.title == "Mine")
        .unwrap();
    let at = app
        .home
        .visible()
        .iter()
        .position(|row| {
            matches!(row, datui::home::Row::Entry { section, entry, .. }
                if *section == mine_section && entry.name == "Lake")
        })
        .unwrap();
    app.home.select(at);
    let line = footer(&mut app);
    assert!(line.contains("^D Forget"), "{line}");
    drive(&mut app, ctrl('d'));
    pump(&mut app, &rx, |app| {
        !app.home.sections.iter().any(|s| s.title == "Mine")
    });
    let text = std::fs::read_to_string(&mine).unwrap();
    assert_eq!(text, "# my notes\nlabel = \"Mine\"\n");

    // A row of another catalog's is added again; the team file still has it.
    select(&mut app, "Lake");
    drive(&mut app, ctrl('d'));
    pump(&mut app, &rx, |app| {
        app.home.sections.iter().any(|s| s.title == "Mine")
    });
}

/// A web file's row says what its catalog says it weighs until a HEAD measures it.
#[test]
fn a_web_files_size_is_a_hint_until_a_head_measures_it() {
    let url = serve("small.csv", "a,b\n1,2\n");
    let (mut app, rx, _dir) = home_with_mine(&format!(
        "[small]\nname = \"Small\"\nurl = \"{url}\"\nsize = 4096\n"
    ));
    pump(&mut app, &rx, |app| {
        app.home.sections.iter().any(|s| s.title == "My datasets")
    });
    select(&mut app, "Small");
    assert!(screen(&mut app).contains("~4.0 KiB"), "the hint, marked");
    app.info.head_web_rows = true;
    pump(&mut app, &rx, |app| {
        app.home
            .selected_entry()
            .is_some_and(|e| e.name == "Small" && e.size == Some(8))
    });
    let shown = screen(&mut app);
    assert!(!shown.contains("~4.0 KiB"), "{shown}");
}

/// A format spec that documents its records, as a catalog documents a dataset.
const ORDERS_SPEC: &str = r#"name = "acme.orders"
description = "Order entry capture"
documentation = "https://example.com/orders.pdf"
match = { glob = "*.ord" }

[records]
framing = "length_prefixed"
size = "len"
type = "kind"
fields = [{ name = "len", type = "u2" }, { name = "kind", type = "str", size = 1 }]

[[variants]]
name = "add"
when = "A"
description = "An order added to the book"
fields = [
  { name = "price", type = "u4", description = "Limit price", unit = "USD" },
  { name = "side", type = "u1", enum = { 1 = "BUY", 2 = "SELL" } },
]

[[variants]]
name = "exec"
when = "E"
fields = [{ name = "shares", type = "u4", description = "Shares executed" }]
"#;

/// A file of [`ORDERS_SPEC`]: an add and an exec, each led by its whole length.
fn orders_bytes() -> Vec<u8> {
    let mut out = vec![8, 0, b'A'];
    out.extend(1_500u32.to_le_bytes());
    out.push(1);
    out.extend([7, 0, b'E']);
    out.extend(100u32.to_le_bytes());
    out
}

/// The specs' directory and registry, and a directory of order files.
fn orders_files() -> (
    tempfile::TempDir,
    datui::formats::Registry,
    tempfile::TempDir,
) {
    let formats = tempfile::TempDir::new().unwrap();
    std::fs::write(formats.path().join("orders.toml"), ORDERS_SPEC).unwrap();
    let registry = datui::formats::Registry::load(&[formats.path().to_path_buf()]);
    assert!(registry.errors.is_empty(), "{:?}", registry.errors);
    let data = tempfile::TempDir::new().unwrap();
    std::fs::write(data.path().join("day.ord"), orders_bytes()).unwrap();
    std::fs::write(data.path().join("lab.ord"), orders_bytes()).unwrap();
    (formats, registry, data)
}

/// An app with the specs of `registry`, its catalogs read from `dir`.
fn app_with_specs(dir: &Path, registry: datui::formats::Registry) -> (App, Receiver<AppEvent>) {
    let (tx, rx) = std::sync::mpsc::channel();
    let mut app = App::new_with_config(
        tx,
        common::test_runtime(),
        datui::Theme {
            colors: std::collections::HashMap::new(),
        },
        config_in(dir, ""),
    );
    app.set_formats(registry);
    (app, rx)
}

fn spec_row_listed(app: &App, name: &str) -> bool {
    app.home.visible().iter().any(|row| {
        matches!(row, datui::home::Row::Entry { entry, .. }
            if entry.name == name && entry.format_spec.is_some())
    })
}

fn doc_lines(state: &datui::widgets::documentation::DocState) -> Vec<String> {
    state.lines().iter().map(|l| format!("{l:?}")).collect()
}

/// Ctrl+E documents a file a format spec reads: its description and link, its record
/// types and its column notes; the open file's Info panel has the same page.
#[test]
fn ctrl_e_documents_a_file_a_format_spec_reads() {
    let (_formats, registry, data) = orders_files();
    let dir = tempfile::TempDir::new().unwrap();
    let (mut app, rx) = app_with_specs(dir.path(), registry);
    app.home.browsing = Some(data.path().to_path_buf());
    app.enter_home();
    pump(&mut app, &rx, |app| spec_row_listed(app, "day.ord"));
    select(&mut app, "day.ord");
    let line = footer(&mut app);
    assert!(line.contains("^E Docs"), "{line}");
    drive(&mut app, ctrl('e'));
    assert!(app.info.documentation.is_open());
    let lines = doc_lines(&app.info.documentation).join("\n");
    for said in [
        "About(\"Order entry capture\")",
        "Field(\"format spec\", \"acme.orders\")",
        "Link(\"documentation\", \"https://example.com/orders.pdf\")",
        "Section(\"RECORD TYPES\", 2)",
        "RecordType(\"add\"",
        "An order added to the book",
        "Limit price (USD)",
        "Shares executed",
    ] {
        assert!(lines.contains(said), "{said} in\n{lines}");
    }
    let page = screen(&mut app);
    assert!(page.contains("day.ord"), "{page}");
    assert!(page.contains("RECORD TYPES"), "{page}");

    let line = footer(&mut app);
    assert!(line.ends_with("? keys"), "{line}");

    // ? and F1 show the view's own keys, and closing the help leaves the view up.
    use datui_cli::keys::Context;
    for help in [key(KeyCode::Char('?')), key(KeyCode::F(1))] {
        drive(&mut app, help);
        assert_eq!(app.help_context(), Some(Context::Documentation));
        drive(&mut app, key(KeyCode::Esc));
        assert_eq!(app.help_context(), None);
        assert!(app.info.documentation.is_open());
    }

    drive(&mut app, key(KeyCode::Esc));
    assert!(!app.info.documentation.is_open());

    // Opened, the Info panel's Documentation tab shows the same page.
    select(&mut app, "day.ord");
    drive(&mut app, key(KeyCode::Enter));
    pump(&mut app, &rx, |app| {
        app.data_table_state.is_some() && !app.is_busy()
    });
    assert_eq!(app.error_message(), None);
    assert!(app.info.info_documentation.is_open());
    // Named for the file just opened, not the one before it.
    assert_eq!(
        app.info.info_documentation.doc.as_ref().map(|d| d.title()),
        Some("day.ord")
    );
    let lines = doc_lines(&app.info.info_documentation).join("\n");
    assert!(lines.contains("Section(\"RECORD TYPES\", 2)"), "{lines}");
    assert!(lines.contains("Limit price (USD)"), "{lines}");
}

/// A catalog entry for a file a format spec reads layers its word over the spec's: its
/// description and its link win, and of a column, each of description, unit and legend
/// it gives; the spec fills the rest, and its record types stay.
#[test]
fn a_catalog_entry_layers_over_a_format_specs_documentation() {
    let (_formats, registry, data) = orders_files();
    let dir = tempfile::TempDir::new().unwrap();
    std::fs::write(
        dir.path().join("catalog.toml"),
        format!(
            r#"label = "Mine"

[lab]
name = "Lab orders"
path = "{}"
description = "Orders from the lab"
documentation = "https://example.com/lab.txt"
columns.price = {{ description = "Price the lab quotes" }}
columns.side = {{ description = "Side of the book" }}
columns.shares = {{ unit = "lots" }}
"#,
            toml_path(&data.path().join("lab.ord"))
        ),
    )
    .unwrap();
    let (mut app, rx) = app_with_specs(dir.path(), registry);
    app.enter_home();
    pump(&mut app, &rx, |app| spec_row_listed(app, "Lab orders"));
    select(&mut app, "Lab orders");
    drive(&mut app, ctrl('e'));
    assert!(app.info.documentation.is_open());
    let lines = doc_lines(&app.info.documentation);
    let text = lines.join("\n");
    assert_eq!(lines[0], "About(\"Orders from the lab\")", "{text}");
    for said in [
        "Field(\"catalog\", \"Mine\")",
        "Field(\"format spec\", \"acme.orders\")",
        "Link(\"documentation\", \"https://example.com/lab.txt\")",
        "Section(\"RECORD TYPES\", 2)",
        // The spec's unit and legend stay under the catalog's description.
        "Column { name: \"price\", about: \"Price the lab quotes (USD)\", values: 0 }",
        "Column { name: \"side\", about: \"Side of the book\", values: 2 }",
        // The spec's description stays under the catalog's unit.
        "Column { name: \"shares\", about: \"Shares executed (lots)\", values: 0 }",
    ] {
        assert!(text.contains(said), "{said} in\n{text}");
    }
    for unsaid in ["Order entry capture", "orders.pdf", "Limit price"] {
        assert!(!text.contains(unsaid), "{unsaid} in\n{text}");
    }
}

/// The details pane lists a documented file's column notes under COLUMNS: a file a
/// format spec reads shows the spec's, and one a catalog lists too shows the two
/// merged as the Documentation page merges them.
#[test]
fn the_details_pane_shows_a_spec_files_column_notes() {
    let (_formats, registry, data) = orders_files();
    let dir = tempfile::TempDir::new().unwrap();
    std::fs::write(
        dir.path().join("catalog.toml"),
        format!(
            r#"label = "Mine"

[lab]
name = "Lab orders"
path = "{}"
columns.price = {{ description = "Price the lab quotes" }}
columns.shares = {{ unit = "lots" }}
"#,
            toml_path(&data.path().join("lab.ord"))
        ),
    )
    .unwrap();
    let (mut app, rx) = app_with_specs(dir.path(), registry.clone());
    app.home.browsing = Some(data.path().to_path_buf());
    app.enter_home();
    pump(&mut app, &rx, |app| spec_row_listed(app, "day.ord"));

    // The spec alone: its notes.
    select(&mut app, "day.ord");
    let shown = screen(&mut app);
    for said in [
        "COLUMNS",
        "Limit price (USD)",
        "side    2 values",
        "Shares executed",
    ] {
        assert!(shown.contains(said), "{said} in\n{shown}");
    }
    assert!(!shown.contains("^E Documentation"), "{shown}");

    // The catalog over the spec, field by field.
    let (mut app, rx) = app_with_specs(dir.path(), registry);
    app.enter_home();
    pump(&mut app, &rx, |app| spec_row_listed(app, "Lab orders"));
    select(&mut app, "Lab orders");
    let shown = screen(&mut app);
    for said in [
        "COLUMNS",
        "Price the lab quotes (USD)",
        "Shares executed (lots)",
    ] {
        assert!(shown.contains(said), "{said} in\n{shown}");
    }
    assert!(!shown.contains("Limit price"), "{shown}");
}

/// `o` on a documentation link asks with the whole URL, as the browser will get it,
/// and Enter hands it on; nothing happens on other lines, a link that is not http or
/// https says so, and where no local browser would show it, `o` says that instead.
/// Enter's `OpenLink` is never handled here, so no browser starts.
#[test]
fn o_opens_a_documentation_link_after_asking() {
    let data = tempfile::TempDir::new().unwrap();
    let csv = data.path().join("a.csv");
    std::fs::write(&csv, "x\n1\n").unwrap();
    let other = data.path().join("b.csv");
    std::fs::write(&other, "x\n2\n").unwrap();
    let long = format!("https://bücher.example/{}?q=1&r=^2", "docs/".repeat(30));
    let (mut app, rx, _dir) = home_with_mine(&format!(
        r#"label = "Mine"

[linked]
name = "Linked"
path = "{csv}"
homepage = "http://example.org/x"
documentation = "{long}"

[odd]
name = "Odd"
path = "{other}"
homepage = "https://user:pw@example.com/"
"#,
        csv = toml_path(&csv),
        other = toml_path(&other),
    ));
    pump(&mut app, &rx, |app| {
        app.home.sections.iter().any(|s| s.title == "Mine")
    });
    // The page's own chips: the line holding Copy and Back.
    let chips = |app: &mut App| {
        screen(app)
            .lines()
            .find(|l| l.contains("Copy") && l.contains("Back"))
            .expect("the page's footer")
            .to_string()
    };
    let to_link = |app: &mut App, label: &str| {
        drive(app, key(KeyCode::Char('g')));
        for _ in 0..40 {
            let lines = app.info.documentation.lines();
            if matches!(lines.get(app.info.documentation.cursor),
                Some(datui::widgets::documentation::DocLine::Link(l, _)) if *l == label)
            {
                return;
            }
            drive(app, key(KeyCode::Char('j')));
        }
        panic!("no {label} link");
    };

    // No local desktop: no `o` in the footer, and `o` says why.
    app.home_app.local_desktop = false;
    select(&mut app, "Linked");
    drive(&mut app, ctrl('e'));
    to_link(&mut app, "documentation");
    let line = chips(&mut app);
    assert!(!line.contains("Open"), "{line}");
    drive(&mut app, key(KeyCode::Char('o')));
    assert!(!app.confirmation_modal.active);
    assert_eq!(
        app.flash_message(),
        Some("o opens links on a local desktop; y copies it")
    );
    drive(&mut app, key(KeyCode::Esc));

    app.home_app.local_desktop = true;
    select(&mut app, "Linked");
    drive(&mut app, ctrl('e'));
    // The first line is the description, not a link: `o` does nothing there.
    assert!(app.info.documentation.link().is_none());
    assert!(!chips(&mut app).contains("Open"));
    drive(&mut app, key(KeyCode::Char('o')));
    assert!(!app.confirmation_modal.active);
    assert_eq!(app.flash_message(), None);

    to_link(&mut app, "documentation");
    let line = chips(&mut app);
    assert!(line.find("Open") < line.find("Copy"), "{line}");
    assert!(line.contains("Open"), "{line}");
    drive(&mut app, key(KeyCode::Char('o')));
    assert!(app.confirmation_modal.active);
    let url = format!(
        "https://xn--bcher-kva.example/{}?q=1&r=^2",
        "docs/".repeat(30)
    );
    assert_eq!(app.confirmation_modal.message, format!("Open {url}?"));
    // Wrapped in the dialog, never cut: every character is on screen.
    let shown: String = screen(&mut app)
        .chars()
        .filter(|c| !c.is_whitespace() && !"│╭╮╰╯─".contains(*c))
        .collect();
    assert!(shown.contains(&format!("Open{url}?")), "{shown}");
    // Enter hands the checked URL on; the run loop starts the browser.
    let next = app.event(key(KeyCode::Enter));
    match next {
        Some(AppEvent::Applied(datui::Applied::OpenLink(sent))) => assert_eq!(sent, url),
        _ => panic!("Enter on Open hands the link on"),
    }
    assert!(!app.confirmation_modal.active);

    // http asks the same way; Esc declines and nothing is pending after.
    to_link(&mut app, "homepage");
    drive(&mut app, key(KeyCode::Char('o')));
    assert_eq!(app.confirmation_modal.message, "Open http://example.org/x?");
    drive(&mut app, key(KeyCode::Esc));
    assert!(!app.confirmation_modal.active);
    assert!(
        app.info.documentation.is_open(),
        "Esc closed only the question"
    );
    drive(&mut app, key(KeyCode::Esc));

    // A link with a password in it is never offered to the browser. (Catalogs
    // already refuse another scheme here.)
    select(&mut app, "Odd");
    drive(&mut app, ctrl('e'));
    to_link(&mut app, "homepage");
    drive(&mut app, key(KeyCode::Char('o')));
    assert!(!app.confirmation_modal.active);
    assert_eq!(
        app.flash_message(),
        Some("Not opened: a user name or password; y copies it")
    );
}

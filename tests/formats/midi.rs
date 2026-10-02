//! Standard MIDI Files: one row per event, time through the tempo map, and the
//! header, tempo and tracks on the Info panel's MIDI tab.
//!
//! Fixtures are written by `scripts/generate_sample_data.py` under
//! `tests/sample-data/midi/`. Anything a test writes goes to `fixture_dir()`.

use crate::common::{self, drain_events, pump_open_until_loaded};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use datui::{App, AppEvent, InputMode, OpenOptions};
use polars::prelude::*;
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::widgets::Widget;
use std::path::PathBuf;
use std::sync::mpsc;

fn midi() -> PathBuf {
    common::ensure_sample_data();
    PathBuf::from("tests/sample-data/midi")
}

fn open(path: PathBuf) -> (App, mpsc::Receiver<AppEvent>) {
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path], OpenOptions::default());
    assert_eq!(app.error_message(), None, "the file opens");
    (app, rx)
}

fn frame(app: &App) -> DataFrame {
    app.data_table_state
        .as_ref()
        .expect("a dataset is open")
        .lf()
        .clone()
        .collect()
        .expect("collect")
}

fn strings(df: &DataFrame, column: &str) -> Vec<Option<String>> {
    df.column(column)
        .unwrap()
        .str()
        .unwrap()
        .iter()
        .map(|v| v.map(str::to_string))
        .collect()
}

fn micros(df: &DataFrame, column: &str) -> Vec<Option<i64>> {
    df.column(column)
        .unwrap()
        .duration()
        .unwrap()
        .physical()
        .iter()
        .collect()
}

fn press(app: &mut App, code: KeyCode) {
    app.event(&AppEvent::Key(KeyEvent::new(code, KeyModifiers::NONE)));
}

fn screen(app: &mut App) -> String {
    let area = Rect::new(0, 0, 120, 30);
    let mut buffer = Buffer::empty(area);
    app.render(area, &mut buffer);
    (0..area.height)
        .map(|y| {
            (0..area.width)
                .map(|x| buffer[(x, y)].symbol())
                .collect::<String>()
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn a_midi_file_opens_as_its_events() {
    let (mut app, rx) = open(midi().join("song.mid"));
    let df = frame(&app);
    assert_eq!(
        df.get_column_names()
            .iter()
            .map(|n| n.as_str())
            .collect::<Vec<_>>(),
        [
            "track",
            "tick",
            "time",
            "kind",
            "channel",
            "note",
            "note_name",
            "velocity",
            "controller",
            "value",
            "length",
            "text"
        ]
    );
    assert_eq!(df.height(), 30, "9 + 15 + 6 events in three tracks");
    let kind = strings(&df, "kind");
    let text = strings(&df, "text");
    let time = micros(&df, "time");
    let length = micros(&df, "length");
    let at = |k: &str, n: usize| {
        kind.iter()
            .enumerate()
            .filter(|(_, v)| v.as_deref() == Some(k))
            .nth(n)
            .unwrap_or_else(|| panic!("a {k} #{n}"))
            .0
    };
    // The conductor track: its meter, key, tempo and sysex as text.
    assert_eq!(text[at("time_signature", 0)].as_deref(), Some("4/4"));
    assert_eq!(text[at("key_signature", 0)].as_deref(), Some("G major"));
    assert_eq!(text[at("tempo", 0)].as_deref(), Some("120 bpm"));
    assert_eq!(text[at("tempo", 1)].as_deref(), Some("90 bpm"));
    assert_eq!(text[at("sysex", 0)].as_deref(), Some("F0 7E 7F 09 01 F7"));
    // Two bars of 4/4 at 120 bpm.
    assert_eq!(time[at("marker", 0)], Some(4_000_000));
    // Running status: C4, E4 and G4 from one status byte, each half a second long.
    let names = strings(&df, "note_name");
    let first = at("note_on", 0);
    assert_eq!(
        names[first..first + 3],
        [Some("C4".into()), Some("E4".into()), Some("G4".into())]
    );
    assert_eq!(length[first], Some(500_000));
    // The C5 at the end is never released: its length is null, and a note says so.
    let c5 = at("note_on", 3);
    assert_eq!(names[c5].as_deref(), Some("C5"));
    assert_eq!(length[c5], None);
    // The bass's note on at velocity 0 is a note off, ending a note of a second.
    let bass = at("note_on", 4);
    assert_eq!(length[bass], Some(1_000_000));
    let channel = df.column("channel").unwrap().u32().unwrap();
    assert_eq!(channel.get(bass), Some(2), "channels count from 1");

    let state = app.data_table_state.as_ref().unwrap();
    let summary = state.midi().expect("a MIDI summary");
    assert_eq!((summary.notes, summary.unended), (6, 1));
    assert!(
        state
            .notes()
            .iter()
            .any(|n| n.summary.contains("never end")),
        "{:?}",
        state.notes()
    );

    // `i` shows the note first, then, once read, the MIDI tab.
    press(&mut app, KeyCode::Char('i'));
    drain_events(&mut app, &rx);
    assert_eq!(app.input_mode, InputMode::Info);
    let notes = screen(&mut app);
    assert!(notes.contains("never end"), "{notes}");
    press(&mut app, KeyCode::Esc);
    press(&mut app, KeyCode::Char('i'));
    drain_events(&mut app, &rx);
    let text = screen(&mut app);
    for expected in [
        "MIDI format 1",
        "480 ticks per quarter",
        "3 tracks",
        "Length: 0:04.0",
        "30 events",
        "6 notes (1 never ends)",
        "Tempo: 120 bpm (90-120, 1 change)",
        "Time: 4/4",
        "Key: G major",
        "Copyright: (c) datui tests",
        "1 Song",
        "2 Piano",
        "Acoustic Grand",
        "3 Bass",
        "ch 2",
    ] {
        assert!(text.contains(expected), "{expected:?} on screen:\n{text}");
    }
}

#[test]
fn format_0_and_smpte_files_keep_their_own_time() {
    let (app, _rx) = open(midi().join("drums.mid"));
    let df = frame(&app);
    // 96 ticks at 100 bpm and 96 a quarter: 0.6 s.
    assert_eq!(
        micros(&df, "time"),
        [Some(0), Some(0), Some(600_000), Some(600_000)]
    );
    let channel = df.column("channel").unwrap().u32().unwrap();
    assert_eq!(channel.get(1), Some(10), "the drum channel");

    let (app, _rx) = open(midi().join("smpte.mid"));
    let df = frame(&app);
    assert_eq!(micros(&df, "time")[1], Some(1_000_000));
    let summary = app.data_table_state.as_ref().unwrap().midi().unwrap();
    assert_eq!(
        summary.division.map(|d| d.label()).as_deref(),
        Some("25 fps, 40 ticks per frame")
    );
}

#[test]
fn a_midi_file_is_known_by_its_bytes_whatever_it_is_named() {
    let dir = common::fixture_dir();
    let plain = dir.join("song");
    std::fs::copy(midi().join("song.mid"), &plain).unwrap();
    for path in [plain, midi().join("song.rmi")] {
        let (app, _rx) = open(path.clone());
        assert_eq!(frame(&app).height(), 30, "{}", path.display());
    }
}

#[test]
fn a_directory_of_songs_is_one_table_without_the_broken_one() {
    let (mut app, rx) = open(midi().join("corpus"));
    let df = frame(&app);
    assert_eq!(df.get_column_names()[0].as_str(), "file");
    assert_eq!(df.height(), 34, "drums 4 and song 30");
    let state = app.data_table_state.as_ref().unwrap();
    let summary = state.midi().unwrap();
    assert_eq!(summary.files, 2);
    assert_eq!(summary.unreadable.len(), 1);
    assert_eq!(summary.unreadable[0].0, "broken.mid");
    assert!(
        state
            .notes()
            .iter()
            .any(|n| n.summary.contains("broken.mid")),
        "{:?}",
        state.notes()
    );

    // The MIDI tab gives totals, not the first song's key, and lists the broken file.
    for key in [KeyCode::Char('i'), KeyCode::Esc, KeyCode::Char('i')] {
        press(&mut app, key);
        drain_events(&mut app, &rx);
    }
    let text = screen(&mut app);
    for expected in [
        "MIDI · 2 files · 4 tracks",
        "Tempo: 90-120 bpm",
        "Unreadable",
        "broken.mid",
    ] {
        assert!(text.contains(expected), "{expected:?} on screen:\n{text}");
    }
    assert!(!text.contains("Key:"), "{text}");
}

#[test]
fn a_midi_file_cut_short_is_an_error() {
    let dir = common::fixture_dir();
    let header_only = dir.join("header_only.mid");
    std::fs::write(&header_only, b"MThd\0\0\0\x06\0\x01\xff\xff\x01\xe0").unwrap();
    for path in [midi().join("cut_short.mid"), header_only] {
        let (tx, rx) = mpsc::channel();
        let mut app = App::new(tx, common::test_runtime());
        let message =
            super::pump_open_until_error(&mut app, &rx, vec![path.clone()], OpenOptions::default())
                .unwrap_or_else(|| panic!("{} opens with an error", path.display()));
        assert!(message.contains("MIDI"), "{}: {message}", path.display());
    }
}

//! WAV, Broadcast WAV, RF64 and AIFF audio: the sample frames as a table, the format,
//! metadata and markers on the Info panel's Audio tab.
//!
//! Fixtures are written by `scripts/generate_sample_data.py` under
//! `tests/sample-data/audio/`. Anything a test writes goes to `fixture_dir()`.

use crate::common::{self, drain_events, pump_open_until_loaded};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use datui::{App, AppEvent, OpenOptions, Overlay};
use polars::prelude::*;
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::widgets::Widget;
use std::path::PathBuf;
use std::sync::mpsc;

fn audio() -> PathBuf {
    common::ensure_sample_data();
    PathBuf::from("tests/sample-data/audio")
}

fn open_with(path: PathBuf, options: OpenOptions) -> (App, mpsc::Receiver<AppEvent>) {
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path], options);
    assert_eq!(app.error_message(), None, "the file opens");
    (app, rx)
}

fn open(name: &str) -> (App, mpsc::Receiver<AppEvent>) {
    open_with(audio().join(name), OpenOptions::default())
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

fn names(df: &DataFrame) -> Vec<String> {
    df.get_column_names()
        .iter()
        .map(|n| n.to_string())
        .collect()
}

fn ints(df: &DataFrame, column: &str) -> Vec<i64> {
    df.column(column)
        .unwrap()
        .cast(&DataType::Int64)
        .unwrap()
        .i64()
        .unwrap()
        .into_no_null_iter()
        .collect()
}

/// Press a key and handle what it asks for next, as the event loop would.
fn press(app: &mut App, code: KeyCode) {
    let mut next = app.event(AppEvent::Key(KeyEvent::new(code, KeyModifiers::NONE)));
    while let Some(event) = next {
        next = app.event(event);
    }
}

/// The screen as lines of text.
fn screen(app: &mut App, width: u16, height: u16) -> String {
    let area = Rect::new(0, 0, width, height);
    let mut buffer = Buffer::empty(area);
    app.render(area, &mut buffer);
    common::buffer_text(&buffer)
}

/// The Audio tab of the dataset on screen.
fn audio_tab(app: &App) -> datui::formats::text_formats::Detail {
    let detail = app
        .data_table_state
        .as_ref()
        .unwrap()
        .format_detail()
        .cloned()
        .expect("an Audio tab");
    assert_eq!(detail.tab, "Audio");
    detail
}

/// The markers on an Audio tab, in order.
fn marker_values(detail: &datui::formats::text_formats::Detail) -> Vec<&str> {
    detail
        .list
        .iter()
        .filter(|(k, _)| k.starts_with("marker "))
        .map(|(_, v)| match v {
            datui::formats::model_files::MetaValue::Text(text) => text.as_str(),
            other => panic!("a marker is text: {other:?}"),
        })
        .collect()
}

#[test]
fn a_wav_file_opens_as_its_frames() {
    let (app, _rx) = open("tone.wav");
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(
        state.num_rows_if_valid(),
        Some(4000),
        "counted from the file's size"
    );
    let df = frame(&app);
    assert_eq!(names(&df), ["frame", "seconds", "ch1", "ch2"]);
    assert_eq!(df.column("ch1").unwrap().dtype(), &DataType::Int16);

    let ch2 = ints(&df, "ch2");
    assert_eq!(ch2[1000], 32767, "the clipped run");
    assert_eq!(ch2[2000], 0, "the silent run");
    // 8 kHz: frame 8 is a millisecond in.
    assert_eq!(
        df.column("seconds").unwrap().f64().unwrap().get(8),
        Some(0.001)
    );
}

#[test]
fn the_last_page_of_a_recording_is_read_from_its_own_frames() {
    let (mut app, rx) = open("tone.wav");
    let _ = screen(&mut app, 120, 30);
    press(&mut app, KeyCode::End);
    drain_events(&mut app, &rx);
    let text = screen(&mut app, 120, 30);
    assert!(text.contains("3999"), "the last frame on screen:\n{text}");
    let state = app.data_table_state.as_ref().unwrap();
    assert!(
        state.buffered_start() > 0,
        "the buffer starts deep in the file, not at its first frame"
    );
}

#[test]
fn a_broadcast_wav_shows_its_format_metadata_and_markers_on_the_audio_tab() {
    let (mut app, rx) = open("take.wav");
    let df = frame(&app);
    assert_eq!(
        df.column("ch1").unwrap().dtype(),
        &DataType::Int32,
        "24-bit"
    );
    assert_eq!(df.height(), 4800);
    let ch1 = ints(&df, "ch1");
    let ch2 = ints(&df, "ch2");
    assert_eq!(ch1[12], -ch2[12], "the channels are mirror images");

    let detail = audio_tab(&app);
    assert!(detail.lines[0].contains("(Broadcast WAV)"), "{detail:?}");
    let markers: Vec<&str> = marker_values(&detail);
    assert_eq!(markers.len(), 2);
    assert!(markers[1].ends_with("Action"), "{markers:?}");
    assert!(
        markers[1].contains("0:00.025 long"),
        "1,200 frames: {markers:?}"
    );

    // `i` opens on the Audio tab.
    press(&mut app, KeyCode::Char('i'));
    drain_events(&mut app, &rx);
    assert_eq!(app.overlay, Overlay::Info);
    let text = screen(&mut app, 120, 40);
    for expected in [
        "WAV (Broadcast WAV)",
        "2 channels",
        "48,000 Hz",
        "Samples: 24-bit integer",
        "Frames: 4,800",
        "Length: 0:00.100",
        "bext.description",
        "Scene 12A, take 3",
        "ixml.scene",
        "12A",
        "info.title",
        "marker 2",
        "0:00.050",
        "Action",
    ] {
        assert!(text.contains(expected), "{expected:?} on screen:\n{text}");
    }
}

#[test]
fn the_audio_tab_fits_a_small_terminal_and_scrolls() {
    let (mut app, rx) = open("take.wav");
    press(&mut app, KeyCode::Char('i'));
    drain_events(&mut app, &rx);
    let text = screen(&mut app, 80, 24);
    assert!(
        text.contains("below"),
        "the list says what is out of view:\n{text}"
    );
    press(&mut app, KeyCode::End);
    let text = screen(&mut app, 80, 24);
    assert!(
        text.contains("Action"),
        "the last marker at the end:\n{text}"
    );
    assert!(app.info_modal.detail_scroll > 0);
}

#[test]
fn the_extensible_mask_names_the_channels() {
    let (app, _rx) = open("surround.wav");
    let df = frame(&app);
    assert_eq!(
        names(&df),
        ["frame", "seconds", "L", "R", "C", "LFE", "BL", "BR"]
    );
    assert_eq!(df.column("LFE").unwrap().dtype(), &DataType::Float32);
}

#[test]
fn an_aiff_file_opens_with_its_marker() {
    let (app, _rx) = open("loop.aiff");
    let df = frame(&app);
    assert_eq!(df.height(), 441);
    let detail = audio_tab(&app);
    assert!(detail.lines[0].contains("44,100 Hz"), "{detail:?}");
    let markers = marker_values(&detail);
    assert!(markers[0].ends_with("Loop"), "{markers:?}");
    assert!(markers[0].contains("frame 220"), "{markers:?}");
}

#[test]
fn normalize_shows_integer_samples_as_float() {
    let options = OpenOptions {
        normalize: true,
        ..OpenOptions::default()
    };
    let (app, _rx) = open_with(audio().join("tone.wav"), options);
    let df = frame(&app);
    let ch2 = df.column("ch2").unwrap();
    assert_eq!(ch2.dtype(), &DataType::Float32);
    let peak = ch2.f32().unwrap().get(1000).unwrap();
    assert!((peak - 32767.0 / 32768.0).abs() < 1e-6, "{peak}");
}

#[test]
fn audio_with_no_extension_is_known_by_its_first_bytes() {
    let dir = common::fixture_dir().join("audio_sniff");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("recording");
    std::fs::copy(audio().join("tone.wav"), &path).unwrap();
    let (app, _rx) = open_with(path, OpenOptions::default());
    assert_eq!(frame(&app).height(), 4000);
}

#[test]
fn a_query_reads_the_samples_it_needs() {
    let (mut app, rx) = open("tone.wav");
    app.event(AppEvent::QQuery("select ch2 where ch2 = 32767".to_string()));
    drain_events(&mut app, &rx);
    assert_eq!(app.error_message(), None);
    let state = app.data_table_state.as_ref().unwrap();
    let df = state.lf().clone().collect().unwrap();
    assert_eq!(df.height(), 20, "the clipped run");
}

#[test]
fn a_compressed_wav_is_refused_by_name() {
    let dir = common::fixture_dir().join("audio_refused");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("mulaw.wav");
    let mut fmt = Vec::new();
    // mu-law, mono, 8 kHz, 8-bit.
    for v in [7u16, 1] {
        fmt.extend_from_slice(&v.to_le_bytes());
    }
    fmt.extend_from_slice(&8000u32.to_le_bytes());
    fmt.extend_from_slice(&8000u32.to_le_bytes());
    fmt.extend_from_slice(&1u16.to_le_bytes());
    fmt.extend_from_slice(&8u16.to_le_bytes());
    let mut body = b"WAVEfmt ".to_vec();
    body.extend_from_slice(&16u32.to_le_bytes());
    body.extend(fmt);
    body.extend_from_slice(b"data\x02\x00\x00\x00\x00\x00");
    let mut bytes = b"RIFF".to_vec();
    bytes.extend_from_slice(&(body.len() as u32).to_le_bytes());
    bytes.extend(body);
    std::fs::write(&path, bytes).unwrap();

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path], OpenOptions::default());
    let err = app.error_message().unwrap_or_default();
    assert!(err.contains("mu-law"), "{err}");
}

/// A full Data Quality run over an audio file reads its samples whole: the clipped
/// run, the run of silence and the offset on the second channel are found, and
/// nothing on the clean first channel.
#[test]
fn data_quality_finds_clipping_silence_and_dc_offset() {
    use datui::data_quality::{ObservationKind, QualityCompute};
    let (mut app, rx) = open("tone.wav");
    press(&mut app, KeyCode::Char('a'));
    app.analysis_modal.sidebar_state.select(Some(3));
    press(&mut app, KeyCode::Enter);
    let plan = &mut app.analysis_modal.quality.plan;
    plan.method = datui::sampling::SampleMethod::EveryRow;
    plan.compute = QualityCompute::Full;
    // A full scan asks first; the second Enter runs it.
    press(&mut app, KeyCode::Enter);
    press(&mut app, KeyCode::Enter);
    drain_events(&mut app, &rx);
    let results = app
        .analysis_modal
        .quality
        .results
        .as_ref()
        .expect("the run finished");
    let found = |kind: ObservationKind, column: &str| {
        results
            .observations
            .iter()
            .find(|o| o.kind == kind && o.column == column)
            .cloned()
    };
    let clipping = found(ObservationKind::Clipping, "ch2").expect("clipping on ch2");
    assert_eq!(clipping.affected_rows, 20);
    assert!(
        clipping.fact.starts_with("1 run of 3+ samples"),
        "{}",
        clipping.fact
    );
    let zeros = found(ObservationKind::ZeroRuns, "ch2").expect("silence on ch2");
    assert_eq!(zeros.affected_rows, 500);
    let dc = found(ObservationKind::DcOffset, "ch2").expect("an offset on ch2");
    assert!(dc.fact.contains("% of full scale"), "{}", dc.fact);
    for kind in [
        ObservationKind::Clipping,
        ObservationKind::ZeroRuns,
        ObservationKind::DcOffset,
    ] {
        assert!(
            found(kind, "ch1").is_none(),
            "{kind:?} on the clean channel"
        );
    }
}

//! SafeTensors and GGUF model files: the tensor list as a table, the header's
//! metadata and totals on the Info panel's Model tab.
//!
//! Fixtures are written by `scripts/generate_sample_data.py` under
//! `tests/sample-data/models/`. Anything a test writes goes to `fixture_dir()`.

use crate::common::{self, drain_events, pump_open_until_loaded};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use datui::model_files::{MetaValue, ModelKind};
use datui::{App, AppEvent, InputMode, OpenOptions};
use polars::prelude::*;
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::widgets::Widget;
use std::path::PathBuf;
use std::sync::mpsc;

fn models() -> PathBuf {
    common::ensure_sample_data();
    PathBuf::from("tests/sample-data/models")
}

fn open(paths: Vec<PathBuf>) -> (App, mpsc::Receiver<AppEvent>) {
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, paths, OpenOptions::default());
    assert_eq!(app.error_message(), None, "the model opens");
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

fn names(df: &DataFrame) -> Vec<String> {
    df.get_column_names()
        .iter()
        .map(|n| n.to_string())
        .collect()
}

fn strings(df: &DataFrame, column: &str) -> Vec<String> {
    df.column(column)
        .unwrap()
        .str()
        .unwrap()
        .iter()
        .map(|v| v.expect("no nulls").to_string())
        .collect()
}

fn press(app: &mut App, code: KeyCode) {
    app.event(&AppEvent::Key(KeyEvent::new(code, KeyModifiers::NONE)));
}

/// The screen as lines of text.
fn screen(app: &mut App) -> Vec<String> {
    let area = Rect::new(0, 0, 120, 30);
    let mut buffer = Buffer::empty(area);
    app.render(area, &mut buffer);
    (0..area.height)
        .map(|y| {
            (0..area.width)
                .map(|x| buffer[(x, y)].symbol())
                .collect::<String>()
        })
        .collect()
}

#[test]
fn a_safetensors_file_opens_as_its_tensors() {
    let (app, _rx) = open(vec![models().join("tiny.safetensors")]);
    let df = frame(&app);
    assert_eq!(
        names(&df),
        [
            "name",
            "dtype",
            "shape",
            "params",
            "bytes",
            "offset_start",
            "offset_end"
        ]
    );
    assert_eq!(
        strings(&df, "name"),
        ["embed.weight", "layer.0.weight", "layer.0.bias", "step"],
        "in the order the data is written"
    );
    assert_eq!(strings(&df, "dtype"), ["F32", "F16", "F32", "I64"]);
    let params: Vec<u64> = df
        .column("params")
        .unwrap()
        .u64()
        .unwrap()
        .into_no_null_iter()
        .collect();
    assert_eq!(params, [128, 64, 8, 1], "a scalar has one parameter");
    let bytes: Vec<u64> = df
        .column("bytes")
        .unwrap()
        .u64()
        .unwrap()
        .into_no_null_iter()
        .collect();
    assert_eq!(bytes, [512, 128, 32, 8]);
    assert_eq!(
        df.column("shape").unwrap().dtype(),
        &DataType::List(Box::new(DataType::UInt64))
    );

    let model = app.data_table_state.as_ref().unwrap().model().unwrap();
    assert_eq!(model.kind, ModelKind::SafeTensors);
    assert_eq!((model.tensors, model.params, model.bytes), (4, 201, 680));
    assert!(
        model
            .metadata
            .contains(&("format".to_string(), MetaValue::Text("pt".to_string()))),
        "__metadata__ goes to the summary: {:?}",
        model.metadata
    );
}

#[test]
fn a_gguf_file_opens_with_its_metadata_on_the_model_tab() {
    let (mut app, rx) = open(vec![models().join("tiny.gguf")]);
    let df = frame(&app);
    assert_eq!(
        names(&df),
        ["name", "type", "shape", "params", "bytes", "offset"]
    );
    assert_eq!(strings(&df, "type"), ["Q4_K", "Q8_0", "F32"]);
    let bytes: Vec<u64> = df
        .column("bytes")
        .unwrap()
        .u64()
        .unwrap()
        .into_no_null_iter()
        .collect();
    assert_eq!(bytes, [100 * 144, 256 * 256 / 32 * 34, 256 * 4]);

    let model = app.data_table_state.as_ref().unwrap().model().unwrap();
    assert_eq!(model.kind, ModelKind::Gguf { version: 3 });
    assert_eq!(model.types[0].name, "Q8_0", "most parameters first");
    let value = |key: &str| {
        model
            .metadata
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.clone())
            .unwrap_or_else(|| panic!("no {key}"))
    };
    assert_eq!(
        value("tokenizer.ggml.tokens"),
        MetaValue::List {
            of: "strings",
            len: 100,
            items: vec![]
        },
        "the vocabulary is its length"
    );
    let MetaValue::Text(template) = value("tokenizer.chat_template") else {
        panic!("a chat template is text");
    };
    assert!(template.contains("{% endfor %}"), "kept whole: {template}");

    // `i` opens on the Model tab, and the template is there in full.
    press(&mut app, KeyCode::Char('i'));
    drain_events(&mut app, &rx);
    assert_eq!(app.input_mode, InputMode::Info);
    let text = screen(&mut app).join("\n");
    for expected in [
        "GGUF v3",
        "Parameters: 91,392 (91.4K)",
        "Q8_0 72%",
        "general.architecture",
        "llama",
        "[100 strings]",
        "{% for message in messages %}",
        "{% endfor %}",
    ] {
        assert!(text.contains(expected), "{expected:?} on screen:\n{text}");
    }

    // The arrows scroll the metadata, which fits here, so nothing moves past its end.
    press(&mut app, KeyCode::Down);
    assert_eq!(app.info_modal.model_scroll, 1);
    let _ = screen(&mut app);
    assert_eq!(app.info_modal.model_scroll, 0, "clamped to what there is");
}

#[test]
fn the_model_tab_scrolls_a_long_value_into_view() {
    let (mut app, rx) = open(vec![models().join("tiny.gguf")]);
    press(&mut app, KeyCode::Char('i'));
    drain_events(&mut app, &rx);
    // A short screen, so the metadata does not fit.
    let area = Rect::new(0, 0, 80, 16);
    let draw = |app: &mut App| {
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
    };
    let first = draw(&mut app);
    assert!(first.contains("below"), "says more is below:\n{first}");
    assert!(!first.contains("{% endfor %}"), "{first}");
    press(&mut app, KeyCode::End);
    let last = draw(&mut app);
    assert!(
        last.contains("{% endfor %}"),
        "End reaches the last line:\n{last}"
    );
    assert!(last.contains("above"), "{last}");
}

#[test]
fn a_sharded_checkpoint_opens_as_one_table_from_its_index_or_its_directory() {
    let sharded = models().join("sharded");
    for path in [
        sharded.join("model.safetensors.index.json"),
        sharded.clone(),
    ] {
        let (app, _rx) = open(vec![path.clone()]);
        let df = frame(&app);
        assert_eq!(names(&df)[0], "file", "{}", path.display());
        assert_eq!(
            strings(&df, "file"),
            [
                "model-00001-of-00002.safetensors",
                "model-00002-of-00002.safetensors",
                "model-00002-of-00002.safetensors"
            ],
            "{}",
            path.display()
        );
        let state = app.data_table_state.as_ref().unwrap();
        let model = state.model().unwrap();
        assert_eq!((model.files, model.tensors), (2, 3));
        assert!(
            !state.has_notes(),
            "the config beside the shards is not data left out: {:?}",
            state.notes()
        );
    }
    // The index's own metadata comes along.
    let (app, _rx) = open(vec![sharded.join("model.safetensors.index.json")]);
    let model = app.data_table_state.as_ref().unwrap().model().unwrap();
    assert!(
        model.metadata.iter().any(|(k, _)| k == "total_size"),
        "{:?}",
        model.metadata
    );
}

#[test]
fn a_model_file_is_known_by_its_bytes_whatever_it_is_named() {
    let dir = common::fixture_dir();
    let gguf = dir.join("weights.bin");
    std::fs::copy(models().join("tiny.gguf"), &gguf).unwrap();
    let safetensors = dir.join("checkpoint");
    std::fs::copy(models().join("tiny.safetensors"), &safetensors).unwrap();
    let (app, _rx) = open(vec![gguf]);
    assert_eq!(frame(&app).height(), 3);
    let (app, _rx) = open(vec![safetensors]);
    assert_eq!(frame(&app).height(), 4);
}

#[test]
fn a_corrupt_header_is_an_error_not_a_crash() {
    let dir = common::fixture_dir();
    // A real file with its last bytes missing, as a download cut short leaves it.
    let cut = |name: &str| {
        let mut bytes = std::fs::read(models().join(name)).unwrap();
        bytes.truncate(bytes.len() - 1);
        bytes
    };
    let cases: [(&str, Vec<u8>); 5] = [
        ("cut.safetensors", cut("tiny.safetensors")),
        ("cut.gguf", cut("tiny.gguf")),
        // A header length of nearly 2^64.
        ("huge.safetensors", {
            let mut b = u64::MAX.to_le_bytes().to_vec();
            b.extend_from_slice(b"{}");
            b
        }),
        // Valid length, broken JSON.
        ("broken.safetensors", {
            let mut b = 4u64.to_le_bytes().to_vec();
            b.extend_from_slice(b"{\"a\"");
            b
        }),
        // A tensor count no file could hold.
        ("huge.gguf", {
            let mut b = b"GGUF".to_vec();
            b.extend_from_slice(&3u32.to_le_bytes());
            b.extend_from_slice(&u64::MAX.to_le_bytes());
            b.extend_from_slice(&0u64.to_le_bytes());
            b
        }),
    ];
    for (name, bytes) in cases {
        let path = dir.join(name);
        std::fs::write(&path, bytes).unwrap();
        let (tx, rx) = mpsc::channel();
        let mut app = App::new(tx, common::test_runtime());
        let message =
            super::pump_open_until_error(&mut app, &rx, vec![path], OpenOptions::default())
                .unwrap_or_else(|| panic!("{name} opens with an error"));
        assert!(
            message.contains("SafeTensors") || message.contains("GGUF"),
            "{name}: {message}"
        );
    }
}

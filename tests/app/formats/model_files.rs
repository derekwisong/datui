//! SafeTensors and GGUF model files: the tensor list as a table, the header's
//! metadata and totals on the Info panel's Model tab.
//!
//! Fixtures are written by `scripts/generate_sample_data.py` under
//! `tests/sample-data/models/`. Anything a test writes goes to `fixture_dir()`.

use crate::common::{self, drain_events, pump_open_until_loaded};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use datui::formats::model_files::MetaValue;
use datui::formats::text_formats::Detail;
use datui::{App, AppEvent, OpenOptions, Overlay};
use polars::prelude::*;
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::widgets::Widget;
use std::path::PathBuf;
use std::sync::mpsc;

/// The Model tab of the dataset on screen: what the model's header said.
fn model_tab(app: &App) -> Detail {
    let detail = app
        .data_table_state
        .as_ref()
        .unwrap()
        .format_detail()
        .cloned()
        .expect("a Model tab");
    assert_eq!(detail.tab, "Model");
    detail
}

/// Whether a line of `detail` says `text`.
fn says(detail: &Detail, text: &str) -> bool {
    detail.lines.iter().any(|line| line.contains(text))
}

/// The metadata value `key` of `detail`'s list.
fn meta(detail: &Detail, key: &str) -> Option<MetaValue> {
    detail
        .list
        .iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.clone())
}

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
    app.event(AppEvent::Key(KeyEvent::new(code, KeyModifiers::NONE)));
}

/// The screen as lines of text.
fn screen(app: &mut App) -> Vec<String> {
    let area = Rect::new(0, 0, 120, 30);
    let mut buffer = Buffer::empty(area);
    app.render(area, &mut buffer);
    common::buffer_lines(&buffer)
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

    let model = model_tab(&app);
    assert!(model.lines[0].starts_with("SafeTensors"), "{model:?}");
    for said in ["4 tensors", "Parameters: 201", "Size: 680 B"] {
        assert!(says(&model, said), "{said}: {model:?}");
    }
    assert_eq!(
        meta(&model, "format"),
        Some(MetaValue::Text("pt".to_string())),
        "__metadata__ goes to the tab"
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

    let model = model_tab(&app);
    assert!(model.lines[0].starts_with("GGUF v3"), "{model:?}");
    assert!(
        says(&model, "Types: Q8_0"),
        "most parameters first: {model:?}"
    );
    let value = |key: &str| meta(&model, key).unwrap_or_else(|| panic!("no {key}"));
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
    assert_eq!(app.overlay, Overlay::Info);
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
    assert_eq!(app.info_modal.detail_scroll, 1);
    let _ = screen(&mut app);
    assert_eq!(app.info_modal.detail_scroll, 0, "clamped to what there is");
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
        common::buffer_text(&buffer)
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
        let model = model_tab(&app);
        assert!(
            says(&model, "3 tensors") && says(&model, "2 files"),
            "{model:?}"
        );
        assert!(
            !state.has_notes(),
            "the config beside the shards is not data left out: {:?}",
            state.notes()
        );
    }
    // The index's own metadata comes along.
    let (app, _rx) = open(vec![sharded.join("model.safetensors.index.json")]);
    let model = model_tab(&app);
    assert!(meta(&model, "total_size").is_some(), "{:?}", model.list);
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
            super::pump_open_until_error(&mut app, &rx, vec![path.clone()], OpenOptions::default())
                .unwrap_or_else(|| panic!("{name} opens with an error"));
        assert!(
            message.starts_with(&format!("\"{}\": ", path.display())),
            "{name}: {message}"
        );
    }
}

/// Remote model files, read by their headers over HTTP and from S3, served by the
/// in-process stand-in (`common/fake_s3.rs`), which counts every byte it sends.
#[cfg(feature = "cloud")]
mod remote {
    use super::{frame, meta, model_tab, models, says, strings};
    use crate::common::{next_event, pump_open_until_loaded};
    use crate::fake_s3::FakeS3;
    use datui::formats::model_files::FIRST_SAFETENSORS_RANGE;
    use datui::{App, AppConfig, AppEvent, OpenOptions};
    use std::collections::BTreeMap;
    use std::path::PathBuf;
    use std::sync::mpsc;

    /// Data past the header, so a download would show on the wire.
    const PADDING: usize = 4 << 20;

    fn padded(name: &str) -> Vec<u8> {
        let mut bytes = std::fs::read(models().join(name)).unwrap();
        bytes.resize(bytes.len() + PADDING, 0);
        bytes
    }

    fn serve(objects: &[(&str, Vec<u8>)]) -> FakeS3 {
        FakeS3::serve(
            "lake",
            objects
                .iter()
                .map(|(key, bytes)| (key.to_string(), bytes.clone()))
                .collect::<BTreeMap<_, _>>(),
        )
    }

    fn app(s3: &FakeS3) -> (App, mpsc::Receiver<AppEvent>) {
        let config = AppConfig {
            cloud: s3.cloud_config(),
            ..AppConfig::default()
        };
        let theme = datui::Theme::from_config(&config.theme).unwrap();
        let (tx, rx) = mpsc::channel();
        let app = App::new_with_config(tx, crate::common::test_runtime(), theme, config);
        (app, rx)
    }

    fn open(s3: &FakeS3, url: &str) -> App {
        let (mut app, rx) = app(s3);
        pump_open_until_loaded(
            &mut app,
            &rx,
            vec![PathBuf::from(url)],
            OpenOptions::default(),
        );
        app
    }

    /// The header length a SafeTensors file starts with.
    fn header_len(bytes: &[u8]) -> u64 {
        u64::from_le_bytes(bytes[..8].try_into().unwrap())
    }

    /// From S3, a SafeTensors file costs one ranged GET of its first 64 KiB, which holds
    /// its length and its JSON; the rest of the file is not asked for, and the table is
    /// the local one's.
    #[test]
    fn an_s3_safetensors_file_is_read_by_its_header_alone() {
        let bytes = padded("tiny.safetensors");
        let s3 = serve(&[("models/tiny.safetensors", bytes.clone())]);
        let app = open(&s3, "s3://lake/models/tiny.safetensors");
        assert_eq!(app.error_message(), None);
        let wire = s3.wire.count();
        assert!(8 + header_len(&bytes) < FIRST_SAFETENSORS_RANGE);
        assert_eq!(wire.gets, 1, "{wire:?}");
        assert_eq!(wire.bytes, FIRST_SAFETENSORS_RANGE, "{wire:?}");
        assert_eq!(frame(&app).height(), 4);
        assert!(says(&model_tab(&app), "4 tensors"));
        assert!(!app.awaiting_open_confirmation(), "nothing to download");
    }

    /// Over HTTP, a GGUF header is read forward in ranges until its tensor infos end:
    /// a small fraction of the file.
    #[cfg(feature = "http")]
    #[test]
    fn an_http_gguf_file_is_read_by_its_header_alone() {
        let bytes = padded("tiny.gguf");
        let s3 = serve(&[("models/tiny.gguf", bytes.clone())]);
        let url = format!("{}/lake/models/tiny.gguf", s3.endpoint);
        let app = open(&s3, &url);
        assert_eq!(app.error_message(), None);
        let wire = s3.wire.count();
        assert!(
            wire.bytes < (bytes.len() - PADDING) as u64 + (1 << 20),
            "{wire:?} of {} bytes",
            bytes.len()
        );
        assert_eq!(frame(&app).height(), 3);
        assert!(model_tab(&app).lines[0].starts_with("GGUF"));
        assert_eq!(
            app.open_path(),
            Some(std::path::Path::new(&url)),
            "named by its URL"
        );
    }

    /// The sharded fixture, at `prefix` in the bucket.
    fn sharded(prefix: &str) -> Vec<(String, Vec<u8>)> {
        std::fs::read_dir(models().join("sharded"))
            .unwrap()
            .map(|entry| {
                let path = entry.unwrap().path();
                let name = path.file_name().unwrap().to_string_lossy().into_owned();
                let mut bytes = std::fs::read(&path).unwrap();
                if name.ends_with(".safetensors") {
                    bytes.resize(bytes.len() + PADDING, 0);
                }
                (format!("{prefix}{name}"), bytes)
            })
            .collect()
    }

    fn shards_read(app: &App) {
        assert_eq!(app.error_message(), None);
        assert_eq!(
            strings(&frame(app), "file"),
            [
                "model-00001-of-00002.safetensors",
                "model-00002-of-00002.safetensors",
                "model-00002-of-00002.safetensors"
            ]
        );
        let model = model_tab(app);
        assert!(
            says(&model, "3 tensors") && says(&model, "2 files"),
            "{model:?}"
        );
        assert!(meta(&model, "total_size").is_some());
    }

    /// A remote index names its shards beside its own URL: they are read there, by the
    /// first 64 KiB that holds each one's header, not beside a downloaded copy.
    #[cfg(feature = "http")]
    #[test]
    fn a_remote_index_reads_its_shards_beside_it() {
        let objects = sharded("org/m/resolve/main/");
        let s3 = FakeS3::serve("lake", objects.iter().cloned().collect());
        let url = format!(
            "{}/lake/org/m/resolve/main/model.safetensors.index.json?download=true",
            s3.endpoint
        );
        let app = open(&s3, &url);
        shards_read(&app);
        let headers: u64 = objects
            .iter()
            .filter(|(k, _)| k.ends_with(".safetensors"))
            .map(|(_, bytes)| {
                assert!(8 + header_len(bytes) < FIRST_SAFETENSORS_RANGE);
                FIRST_SAFETENSORS_RANGE.min(bytes.len() as u64)
            })
            .sum();
        let index = objects
            .iter()
            .find(|(k, _)| k.ends_with(".index.json"))
            .unwrap()
            .1
            .len() as u64;
        assert_eq!(s3.wire.count().bytes, index + headers);
    }

    /// A prefix holding a checkpoint, named on the command line, opens as the model
    /// rather than falling through to the Parquet scan.
    #[test]
    fn a_prefix_of_model_files_opens_as_the_model() {
        // The JSON beside the shards outnumbers them, as a tokenizer's does.
        let mut objects = sharded("ckpt/");
        for name in [
            "tokenizer.json",
            "tokenizer_config.json",
            "generation_config.json",
            "special_tokens_map.json",
        ] {
            objects.push((format!("ckpt/{name}"), b"{}".to_vec()));
        }
        let s3 = FakeS3::serve("lake", objects.into_iter().collect());
        let (mut app, rx) = app(&s3);
        let mut next = Some(AppEvent::OpenNamed(
            vec![PathBuf::from("s3://lake/ckpt/")],
            OpenOptions::default(),
        ));
        while let Some(event) = next.take().or_else(|| next_event(&app, &rx)) {
            next = app.event(event);
        }
        shards_read(&app);
        assert!(
            s3.wire.count().bytes < PADDING as u64,
            "{:?}",
            s3.wire.count()
        );
        let state = app.data_table_state.as_ref().unwrap();
        assert!(!state.has_notes(), "{:?}", state.notes());
    }

    /// A server that sends the whole file where a range was asked for: the open falls
    /// back to the download, saying why, and the downloaded file opens as the model.
    #[cfg(feature = "http")]
    #[test]
    fn a_server_without_ranges_falls_back_to_the_download() {
        use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
        let s3 = serve(&[("tiny.safetensors", padded("tiny.safetensors"))]);
        s3.whole_files();
        let (mut app, rx) = app(&s3);
        let dir = tempfile::tempdir().unwrap();
        let options = OpenOptions {
            temp_dir: Some(dir.path().to_path_buf()),
            ..OpenOptions::default()
        };
        let url = format!("{}/lake/tiny.safetensors", s3.endpoint);
        let mut next = Some(AppEvent::Open(vec![PathBuf::from(&url)], options));
        // The question holds the open, so the wait is for it rather than for quiet.
        while app.error_message().is_none() && !app.awaiting_open_confirmation() {
            let event = next
                .take()
                .or_else(|| next_event(&app, &rx))
                .expect("the open asks about the download");
            next = app.event(event);
        }
        assert_eq!(app.error_message(), None);
        assert!(
            app.confirmation_modal.message.contains("byte ranges"),
            "{}",
            app.confirmation_modal.message
        );
        let mut next = Some(AppEvent::Key(KeyEvent::new(
            KeyCode::Enter,
            KeyModifiers::NONE,
        )));
        while let Some(event) = next.take().or_else(|| next_event(&app, &rx)) {
            next = app.event(event);
        }
        assert_eq!(app.error_message(), None);
        assert_eq!(frame(&app).height(), 4);
        assert!(says(&model_tab(&app), "4 tensors"));
    }

    /// A hostile header over the network is refused by the lengths it states, before
    /// they are fetched.
    #[test]
    fn a_hostile_remote_header_is_an_error_after_a_few_bytes() {
        let mut claims_too_much = 50_000_000u64.to_le_bytes().to_vec();
        claims_too_much.extend_from_slice(b"{}");
        claims_too_much.resize(PADDING, b' ');
        let mut long_string = b"GGUF".to_vec();
        long_string.extend_from_slice(&3u32.to_le_bytes());
        long_string.extend_from_slice(&0u64.to_le_bytes());
        long_string.extend_from_slice(&1u64.to_le_bytes());
        long_string.extend_from_slice(&(u64::MAX - 3).to_le_bytes());
        long_string.resize(PADDING, 0);
        for (key, bytes, why) in [
            ("x.safetensors", claims_too_much, "claims"),
            ("x.gguf", long_string, "longer than datui reads"),
        ] {
            let s3 = serve(&[(key, bytes)]);
            let (mut app, rx) = app(&s3);
            let message = super::super::pump_open_until_error(
                &mut app,
                &rx,
                vec![PathBuf::from(format!("s3://lake/{key}"))],
                OpenOptions::default(),
            )
            .unwrap_or_else(|| panic!("{key} opens with an error"));
            assert!(message.contains(why), "{key}: {message}");
            assert!(
                s3.wire.count().bytes <= 256 * 1024,
                "{key}: {:?}",
                s3.wire.count()
            );
        }
    }

    /// A failed read names the URL without its password or signature.
    #[cfg(feature = "http")]
    #[test]
    fn a_failed_remote_read_does_not_show_credentials() {
        let s3 = serve(&[("other.safetensors", padded("tiny.safetensors"))]);
        let (mut app, rx) = app(&s3);
        let url = s3.endpoint.replacen("://", "://alice:hunter2@", 1)
            + "/lake/missing.safetensors?X-Amz-Signature=s3cr3tsig";
        let message = super::super::pump_open_until_error(
            &mut app,
            &rx,
            vec![PathBuf::from(&url)],
            OpenOptions::default(),
        )
        .expect("a missing file is an error");
        assert!(message.contains("missing.safetensors"), "{message}");
        assert!(!message.contains("hunter2"), "{message}");
        assert!(!message.contains("s3cr3tsig"), "{message}");
    }
}

//! NumPy arrays: each dtype, structured arrays, 2-D grids in either order, refusals,
//! and `.npz` archives listed on the home screen like a directory of arrays.
//!
//! Fixtures are written by `scripts/generate_sample_data.py` under
//! `tests/sample-data/numpy/`. Anything a test writes goes to `fixture_dir()`.

use crate::common::{self, drain_events, pump_open_until_loaded};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use datui::home::{HomeState, Row};
use datui::{App, AppEvent, InputMode, OpenOptions};
use polars::prelude::*;
use std::path::{Path, PathBuf};
use std::sync::mpsc;

fn numpy() -> PathBuf {
    common::ensure_sample_data();
    PathBuf::from("tests/sample-data/numpy")
}

/// Temporary files go to a scratch directory of the test's own, to be counted.
fn scratch() -> (OpenOptions, PathBuf) {
    let dir = common::fixture_dir().join(format!(
        "numpy-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    (
        OpenOptions {
            temp_dir: Some(dir.clone()),
            ..OpenOptions::default()
        },
        dir,
    )
}

fn open_with(path: PathBuf, options: OpenOptions) -> (App, mpsc::Receiver<AppEvent>) {
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path], options);
    (app, rx)
}

fn open(path: PathBuf) -> App {
    let (app, _rx) = open_with(path, scratch().0);
    assert_eq!(app.error_message(), None, "the file opens");
    app
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

fn names(df: &DataFrame) -> Vec<&str> {
    df.get_column_names().iter().map(|n| n.as_str()).collect()
}

fn listed(home: &HomeState) -> Vec<String> {
    home.visible()
        .iter()
        .filter_map(|r| match r {
            Row::Entry { entry, .. } => Some(entry.name.clone()),
            _ => None,
        })
        .collect()
}

fn key(app: &mut App, rx: &mpsc::Receiver<AppEvent>, code: KeyCode) {
    let mut next = app.event(&AppEvent::Key(KeyEvent::new(code, KeyModifiers::NONE)));
    while let Some(event) = next {
        next = app.event(&event);
    }
    drain_events(app, rx);
}

/// Handle events until the home screen lists the archive's arrays.
fn settle_home(app: &mut App, rx: &mpsc::Receiver<AppEvent>) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        while let Ok(event) = rx.try_recv() {
            let mut next = Some(event);
            while let Some(event) = next {
                next = app.event(&event);
            }
        }
        let listed = app
            .home
            .sections
            .iter()
            .any(|s| s.rows.iter().any(|r| r.table.is_some()));
        if !app.home.listing_in_flight && listed {
            let area = ratatui::layout::Rect::new(0, 0, 100, 30);
            let mut buf = ratatui::buffer::Buffer::empty(area);
            ratatui::widgets::Widget::render(&mut *app, area, &mut buf);
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the listing never came"
        );
        if let Ok(event) = rx.recv_timeout(std::time::Duration::from_millis(20)) {
            let mut next = Some(event);
            while let Some(event) = next {
                next = app.event(&event);
            }
        }
    }
}

fn files_in(dir: &Path) -> usize {
    std::fs::read_dir(dir).map_or(0, |d| d.count())
}

#[test]
fn a_vector_is_one_column_named_for_the_file() {
    let app = open(numpy().join("vector.npy"));
    let df = frame(&app);
    assert_eq!(names(&df), ["vector"]);
    assert_eq!(df.height(), 500);
    let values = df.column("vector").unwrap().f64().unwrap();
    assert_eq!(values.get(0), Some(0.0));
    assert_eq!(values.get(499), Some(124.75));
    let state = app.data_table_state.as_ref().unwrap();
    let detail = state.format_detail().expect("the NumPy tab");
    assert_eq!(detail.tab, "NumPy");
    assert!(detail.lines.contains(&"Shape: (500,)".to_string()));
}

#[test]
fn each_dtype_reads_as_its_type() {
    let df = frame(&open(numpy().join("dtypes.npy")));
    let dtype = |name: &str| df.column(name).unwrap().dtype().clone();
    assert_eq!(dtype("b1"), DataType::Boolean);
    assert_eq!(dtype("i1"), DataType::Int8);
    assert_eq!(dtype("u8"), DataType::UInt64);
    assert_eq!(dtype("f2"), DataType::Float32);
    assert_eq!(dtype("f8"), DataType::Float64);
    assert_eq!(dtype("c8"), DataType::Array(Box::new(DataType::Float32), 2));
    assert_eq!(
        dtype("c16"),
        DataType::Array(Box::new(DataType::Float64), 2)
    );
    assert_eq!(dtype("bytes"), DataType::String);
    assert_eq!(dtype("text"), DataType::String);
    assert_eq!(dtype("at"), DataType::Datetime(TimeUnit::Nanoseconds, None));
    assert_eq!(dtype("day"), DataType::Date);
    assert_eq!(
        dtype("second"),
        DataType::Datetime(TimeUnit::Milliseconds, None)
    );
    assert_eq!(dtype("took"), DataType::Duration(TimeUnit::Microseconds));

    // A big-endian field among little-endian ones.
    assert_eq!(
        df.column("big").unwrap().i32().unwrap().to_vec(),
        [Some(1), Some(2), Some(3)]
    );
    assert_eq!(
        df.column("b1")
            .unwrap()
            .bool()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        [Some(true), Some(false), Some(true)]
    );
    assert_eq!(
        df.column("f2").unwrap().f32().unwrap().to_vec(),
        [Some(0.5), Some(1.5), Some(2.5)]
    );
    let text = |name: &str| -> Vec<Option<String>> {
        df.column(name)
            .unwrap()
            .str()
            .unwrap()
            .iter()
            .map(|v| v.map(str::to_string))
            .collect()
    };
    assert_eq!(
        text("bytes"),
        [Some("ab".into()), Some("cdefg".into()), Some(String::new())]
    );
    assert_eq!(
        text("text"),
        [Some("hé".into()), Some("日本".into()), Some(String::new())]
    );
    // NaT is null; times keep their unit's precision.
    let at = df.column("at").unwrap().datetime().unwrap();
    assert_eq!(at.phys.get(0), Some(1_704_164_645_000_000_006));
    assert_eq!(at.phys.get(1), None);
    let day = df.column("day").unwrap().date().unwrap();
    assert_eq!(day.phys.to_vec(), [Some(19_782), Some(-1), None]);
    let second = df.column("second").unwrap().datetime().unwrap();
    assert_eq!(second.phys.get(0), Some(1_704_067_201_000));
    assert_eq!(second.phys.get(1), None);
    let took = df.column("took").unwrap().duration().unwrap();
    assert_eq!(took.phys.to_vec(), [Some(1500), Some(0), Some(-2)]);
    let c8 = df
        .column("c8")
        .unwrap()
        .array()
        .unwrap()
        .get_as_series(1)
        .unwrap();
    assert_eq!(c8.f32().unwrap().to_vec(), [Some(3.0), Some(-4.0)]);
}

#[test]
fn structured_arrays_are_a_column_per_field() {
    let df = frame(&open(numpy().join("trades.npy")));
    assert_eq!(names(&df), ["ts", "px", "qty"]);
    assert_eq!(df.height(), 500);
    assert_eq!(
        df.column("qty").unwrap().i32().unwrap().get(4),
        Some(1),
        "4 % 7 - 3"
    );

    // Padding between and after fields (align=True) is left out.
    let df = frame(&open(numpy().join("aligned.npy")));
    assert_eq!(names(&df), ["flag", "value", "id"]);
    assert_eq!(
        df.column("value").unwrap().f64().unwrap().to_vec(),
        [Some(1.25), Some(2.5), Some(3.75), Some(5.0)]
    );
    assert_eq!(
        df.column("id").unwrap().i16().unwrap().to_vec(),
        [Some(10), Some(20), Some(30), Some(40)]
    );

    // Fields at offsets, in an element larger than they are.
    let df = frame(&open(numpy().join("offsets.npy")));
    assert_eq!(names(&df), ["a", "b"]);
    assert_eq!(
        df.column("a").unwrap().i32().unwrap().to_vec(),
        [Some(7), Some(8), Some(9)]
    );
    assert_eq!(
        df.column("b").unwrap().f32().unwrap().to_vec(),
        [Some(0.5), Some(0.25), Some(0.125)]
    );

    // A subarray field is an Array column.
    let df = frame(&open(numpy().join("subarray.npy")));
    assert_eq!(
        df.column("px").unwrap().dtype(),
        &DataType::Array(Box::new(DataType::Float64), 10)
    );
    let last = df
        .column("px")
        .unwrap()
        .array()
        .unwrap()
        .get_as_series(4)
        .unwrap();
    assert_eq!(last.f64().unwrap().get(9), Some(24.5));
}

#[test]
fn a_grid_reads_the_same_in_either_order() {
    let c = frame(&open(numpy().join("grid.npy")));
    let f = frame(&open(numpy().join("grid_fortran.npy")));
    assert_eq!(names(&c), ["0", "1", "2", "3"]);
    assert_eq!(c.height(), 100);
    assert!(c.equals(&f), "C and Fortran order read alike");
    assert_eq!(c.column("3").unwrap().f32().unwrap().get(99), Some(399.0));
}

#[test]
fn more_dimensions_and_objects_are_refused() {
    let (app, _rx) = open_with(numpy().join("cube.npy"), scratch().0);
    let message = app.error_message().expect("refused");
    assert!(message.contains("shape (2, 3, 4)"), "{message}");
    let (app, _rx) = open_with(numpy().join("objects.npy"), scratch().0);
    let message = app.error_message().expect("refused");
    assert!(message.contains("does not unpickle"), "{message}");
}

/// An archive of several arrays lands on the home screen inside it, its arrays in the
/// order they were saved; Enter opens one and q comes back.
#[test]
fn an_archive_of_several_lands_on_its_arrays() {
    let (options, _) = scratch();
    let npz = numpy().join("run.npz");
    let (mut app, rx) = open_with(npz.clone(), options);
    settle_home(&mut app, &rx);
    assert_eq!(app.error_message(), None);
    assert_eq!(app.input_mode, InputMode::Home);
    assert_eq!(app.home.browsing.as_deref(), Some(npz.as_path()));
    assert_eq!(listed(&app.home), ["prices", "grid", "trades"]);

    let at = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, Row::Entry { entry, .. } if entry.name == "trades"))
        .unwrap();
    app.home.select(at);
    key(&mut app, &rx, KeyCode::Enter);
    assert_eq!(app.error_message(), None);
    assert_eq!(app.input_mode, InputMode::Normal);
    let df = frame(&app);
    assert_eq!(names(&df), ["ts", "px", "qty"]);
    assert_eq!(df.height(), 500);
    assert_eq!(app.open_path(), Some(npz.join("trades").as_path()));

    key(&mut app, &rx, KeyCode::Char('q'));
    settle_home(&mut app, &rx);
    assert_eq!(app.home.browsing.as_deref(), Some(npz.as_path()));
}

/// `--table` and a path inside the archive pick an array; a compressed one is read from
/// a temporary copy that goes with the dataset.
#[test]
fn table_picks_an_array_stored_or_compressed() {
    for archive in ["run.npz", "packed.npz"] {
        let (options, dir) = scratch();
        let (mut app, rx) = open_with(
            numpy().join(archive),
            OpenOptions {
                table: Some("grid".to_string()),
                ..options
            },
        );
        assert_eq!(app.error_message(), None, "{archive}");
        let df = frame(&app);
        assert_eq!(names(&df), ["0", "1", "2", "3"], "{archive}");
        assert_eq!(
            df.column("0").unwrap().f32().unwrap().get(1),
            Some(4.0),
            "{archive}"
        );
        let state = app.data_table_state.as_ref().unwrap();
        assert_eq!(
            state.format_detail().map(|d| d.tab),
            Some("NumPy"),
            "{archive}"
        );
        let copies = files_in(&dir);
        assert_eq!(copies, usize::from(archive == "packed.npz"), "{archive}");
        key(&mut app, &rx, KeyCode::Char('q'));
        drop(app);
        assert_eq!(
            files_in(&dir),
            0,
            "{archive}: the copy goes with the dataset"
        );
    }

    let app = open(numpy().join("packed.npz").join("prices"));
    assert_eq!(frame(&app).height(), 500);
    let (app, _rx) = open_with(
        numpy().join("run.npz"),
        OpenOptions {
            table: Some("nope".to_string()),
            ..scratch().0
        },
    );
    let message = app.error_message().expect("refused");
    assert!(
        message.contains("its tables: prices, grid, trades"),
        "{message}"
    );
}

#[test]
fn an_archive_of_one_opens_it() {
    for archive in ["single.npz", "single_packed.npz"] {
        let df = frame(&open(numpy().join(archive)));
        assert_eq!(names(&df), ["values"], "{archive}");
        assert_eq!(
            df.column("values").unwrap().i64().unwrap().get(9),
            Some(9),
            "{archive}"
        );
    }
}

/// The home screen counts an archive's arrays and lists them, with their columns.
#[test]
fn the_home_screen_lists_an_archives_arrays() {
    let npz = numpy().join("run.npz");
    let mut home = HomeState {
        browsing: Some(npz.clone()),
        ..HomeState::default()
    };
    home.rebuild(&[]);
    assert_eq!(listed(&home), ["prices", "grid", "trades"]);
    let mut entry = datui::discover::Entry::for_test(&npz, "run.npz");
    datui::discover::enrich(&mut entry);
    assert_eq!(entry.cost.tables, Some(3));
    let trades = datui::discover::table_row(&npz.join("trades")).unwrap();
    assert_eq!(trades.columns, ["ts", "px", "qty"]);
    let preview = datui::discover::schema_preview(&trades).unwrap();
    assert_eq!(preview[1], ("px".to_string(), DataType::Float64));
}

/// Copy as Python loads the array on screen with NumPy, named as datui names its
/// columns: a vector, a grid, a structured array and an archive's array.
#[test]
fn copy_as_python_loads_the_array_on_screen() {
    for (file, table) in [
        ("vector.npy", None),
        ("grid.npy", None),
        ("trades.npy", None),
        ("run.npz", Some("grid")),
    ] {
        let options = OpenOptions {
            table: table.map(str::to_string),
            ..scratch().0
        };
        let (mut app, rx) = open_with(numpy().join(file), options);
        assert_eq!(app.error_message(), None, "{file}");
        drain_events(&mut app, &rx);
        let script = app.python_script(app.data_table_state.as_ref().unwrap());
        assert!(script.contains("import numpy as np\n"), "{script}");
        assert!(script.contains("pl.from_numpy(np.load("), "{script}");
        let Some((rows, script)) = crate::run_python_script(&app) else {
            eprintln!("skipped: no .venv to run the scripts with");
            return;
        };
        assert_eq!(rows, crate::view_csv(&app), "{file}:\n{script}");
    }
}

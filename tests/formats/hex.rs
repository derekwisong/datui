//! The hex view: a file no reader takes opens there, `--hex` opens any file there,
//! and its keys move, go to an offset, find (across rows, with wildcards, cancelled)
//! and fix the bytes per row.

use super::*;
use datui::hex_view::Origin;

fn fresh() -> (App, mpsc::Receiver<AppEvent>, mpsc::Sender<AppEvent>) {
    let (tx, rx) = mpsc::channel();
    let app = App::new(tx.clone(), common::test_runtime());
    (app, rx, tx)
}

fn write(name: &str, bytes: &[u8]) -> PathBuf {
    let path = common::fixture_dir().join(name);
    std::fs::write(&path, bytes).unwrap();
    path
}

/// Open `path`, with `options`, and handle everything it leads to.
fn open(app: &mut App, rx: &mpsc::Receiver<AppEvent>, path: PathBuf, options: OpenOptions) {
    pump_open_until_loaded(app, rx, vec![path], options);
    drain_events(app, rx);
}

fn type_in(app: &mut App, text: &str) {
    for c in text.chars() {
        press(app, KeyCode::Char(c));
    }
}

fn ctrl(app: &mut App, c: char) -> Option<AppEvent> {
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char(c),
        KeyModifiers::CONTROL,
    )))
}

fn screen(app: &mut App, width: u16, height: u16) -> String {
    let area = Rect::new(0, 0, width, height);
    let mut buffer = Buffer::empty(area);
    app.render(area, &mut buffer);
    (0..height)
        .map(|y| {
            (0..width)
                .map(|x| buffer[(x, y)].symbol().to_string())
                .collect::<String>()
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn cursor(app: &App) -> u64 {
    app.hex.as_ref().unwrap().cursor
}

/// Records of 21 bytes, each starting with `SYNC`; `SY` and `NC` straddle the row
/// boundary at 16 in the second record.
fn records() -> Vec<u8> {
    let mut out = Vec::new();
    for i in 0..200u32 {
        out.extend(b"SYNC");
        out.extend(i.to_le_bytes());
        out.extend([0xde, (i % 7) as u8, 0xef]);
        out.extend([0u8; 10]);
    }
    out
}

#[test]
fn a_file_no_reader_takes_opens_as_hex_rather_than_failing() {
    let path = write("hex_fallback.weird", &records());
    let (mut app, rx, _tx) = fresh();
    open(&mut app, &rx, path, OpenOptions::default());
    assert!(app.error_message().is_none(), "{:?}", app.error_message());
    assert_eq!(app.input_mode, InputMode::Hex);
    let view = app.hex_view().unwrap();
    assert!(view.fallback);
    assert_eq!(view.origin, Origin::Launch);
    assert_eq!(view.len(), 21 * 200);
    let s = screen(&mut app, 80, 24);
    assert!(s.contains("Hex · hex_fallback.weird"), "{s}");
    assert!(s.contains("00000000  53 59 4e 43"), "{s}");
    assert!(s.contains("No reader matched this file"), "{s}");
    // `q` quits from a view the command line opened.
    assert!(matches!(
        press(&mut app, KeyCode::Char('q')),
        Some(AppEvent::Exit)
    ));
}

#[test]
fn hex_asks_for_any_file_and_record_size_lines_records_up() {
    let path = write("hex_asked.csv", b"a,b\n1,2\n");
    let (mut app, rx, _tx) = fresh();
    let options = OpenOptions {
        hex: true,
        record_size: Some(4),
        ..OpenOptions::default()
    };
    open(&mut app, &rx, path, options);
    let view = app.hex_view().expect("the hex view, not the table");
    assert!(!view.fallback);
    assert_eq!(view.record_size, Some(4));
    assert!(app.data_table_state.is_none());
    let s = screen(&mut app, 80, 24);
    assert!(s.contains("00000000  61 2c 62 0a"), "{s}");
    assert!(s.contains("00000004  31 2c 32 0a"), "{s}");
    assert!(s.contains("4 a row (fixed)"), "{s}");
}

#[test]
fn keys_move_go_to_an_offset_and_set_the_row_size() {
    let path = write("hex_keys.bin", &records());
    let (mut app, rx, _tx) = fresh();
    open(&mut app, &rx, path, OpenOptions::default());
    screen(&mut app, 80, 24);
    press(&mut app, KeyCode::Char('j'));
    assert_eq!(cursor(&app), 16);
    press(&mut app, KeyCode::Char('w'));
    assert_eq!(cursor(&app), 20);
    press(&mut app, KeyCode::Char('$'));
    assert_eq!(cursor(&app), 31);
    press(&mut app, KeyCode::Char('0'));
    assert_eq!(cursor(&app), 16);
    press(&mut app, KeyCode::Char('G'));
    assert_eq!(cursor(&app), 21 * 200 - 1);
    press(&mut app, KeyCode::Char('g'));
    assert_eq!(cursor(&app), 0);

    press(&mut app, KeyCode::Char(':'));
    type_in(&mut app, "0x2a");
    press(&mut app, KeyCode::Enter);
    assert_eq!(cursor(&app), 42);
    press(&mut app, KeyCode::Char(':'));
    type_in(&mut app, "e-1");
    press(&mut app, KeyCode::Enter);
    assert_eq!(cursor(&app), 21 * 200 - 1);
    press(&mut app, KeyCode::Char(':'));
    type_in(&mut app, "-4199");
    press(&mut app, KeyCode::Enter);
    assert_eq!(cursor(&app), 0);
    // Past the end: the prompt stays open and says why.
    press(&mut app, KeyCode::Char(':'));
    type_in(&mut app, "99999");
    press(&mut app, KeyCode::Enter);
    let view = app.hex.as_ref().unwrap();
    assert!(view.prompt.is_some());
    assert!(
        view.prompt_error
            .as_deref()
            .unwrap()
            .contains("past the end")
    );
    let s = screen(&mut app, 80, 24);
    assert!(s.contains("past the end"), "{s}");
    press(&mut app, KeyCode::Esc);
    assert!(app.hex.as_ref().unwrap().prompt.is_none());

    press(&mut app, KeyCode::Char('r'));
    type_in(&mut app, "21");
    press(&mut app, KeyCode::Enter);
    assert_eq!(app.hex.as_ref().unwrap().record_size, Some(21));
    let s = screen(&mut app, 120, 24);
    assert!(s.contains("00000015  53 59 4e 43  01 00 00 00"), "{s}");
    assert!(s.contains("0000002a  53 59 4e 43  02 00 00 00"), "{s}");
    press(&mut app, KeyCode::Char('#'));
    let s = screen(&mut app, 120, 24);
    assert!(s.contains("      21  53 59 4e 43"), "decimal offsets: {s}");
}

#[test]
fn a_find_spans_rows_takes_wildcards_and_guesses_the_stride() {
    let path = write("hex_find.bin", &records());
    let (mut app, rx, _tx) = fresh();
    open(&mut app, &rx, path, OpenOptions::default());
    screen(&mut app, 80, 24);
    // Record 1 is bytes 21 to 41: `de 01 ef` at 29 to 31, then zeros from 32, so
    // `01 ef 00` crosses the row boundary at 32.
    press(&mut app, KeyCode::Char(':'));
    type_in(&mut app, "20");
    press(&mut app, KeyCode::Enter);
    press(&mut app, KeyCode::Char('f'));
    type_in(&mut app, "01 ef 00");
    press(&mut app, KeyCode::Enter);
    drain_events(&mut app, &rx);
    assert_eq!(
        cursor(&app),
        30,
        "the match that starts in one row and ends in the next"
    );
    assert!(cursor(&app) / 16 != (cursor(&app) + 2) / 16);

    // A wildcard: `de ?? ef` with 3 in the middle is record 3.
    press(&mut app, KeyCode::Char('g'));
    press(&mut app, KeyCode::Char('f'));
    type_in(&mut app, "de 03 ef");
    press(&mut app, KeyCode::Enter);
    drain_events(&mut app, &rx);
    assert_eq!(cursor(&app), 3 * 21 + 8);
    press(&mut app, KeyCode::Char('g'));
    press(&mut app, KeyCode::Char('f'));
    type_in(&mut app, "de ?? ef");
    press(&mut app, KeyCode::Enter);
    drain_events(&mut app, &rx);
    assert_eq!(cursor(&app), 8);
    press(&mut app, KeyCode::Char('n'));
    drain_events(&mut app, &rx);
    assert_eq!(cursor(&app), 21 + 8);
    press(&mut app, KeyCode::Char('N'));
    drain_events(&mut app, &rx);
    assert_eq!(cursor(&app), 8);
    let found = app.hex.as_ref().unwrap().found.clone().unwrap();
    assert_eq!(found.stride, Some(21), "every record has one");
    let s = screen(&mut app, 100, 24);
    assert!(s.contains("every 21 bytes"), "{s}");
    assert!(s.contains("Use stride"), "{s}");
    press(&mut app, KeyCode::Char('R'));
    assert_eq!(app.hex.as_ref().unwrap().record_size, Some(21));

    // Text, and no match.
    press(&mut app, KeyCode::Char('f'));
    press(&mut app, KeyCode::Backspace);
    type_in(&mut app, "nowhere");
    press(&mut app, KeyCode::Enter);
    drain_events(&mut app, &rx);
    assert_eq!(app.flash_message(), Some("No match for nowhere"));
    assert_eq!(cursor(&app), 8, "the cursor stays where it was");
}

#[test]
fn esc_stops_a_find_on_a_large_file() {
    let path = common::fixture_dir().join("hex_sparse.bin");
    let file = std::fs::File::create(&path).unwrap();
    // Sparse: four gigabytes of zeros that take no disk.
    file.set_len(4 << 30).unwrap();
    drop(file);
    let (mut app, rx, _tx) = fresh();
    open(&mut app, &rx, path.clone(), OpenOptions::default());
    assert_eq!(app.hex_view().unwrap().len(), 4 << 30);
    let s = screen(&mut app, 80, 24);
    assert!(s.contains("4,294,967,296 bytes"), "{s}");
    press(&mut app, KeyCode::Char('f'));
    type_in(&mut app, "needle");
    press(&mut app, KeyCode::Enter);
    assert!(app.finding());
    assert!(app.is_busy(), "the keys wait on a find");
    press(&mut app, KeyCode::Esc);
    assert!(!app.finding());
    assert!(!app.is_busy());
    drain_events(&mut app, &rx);
    assert_eq!(app.flash_message(), Some("Find cancelled"));
    assert_eq!(
        app.input_mode,
        InputMode::Hex,
        "Esc stopped the find, not the view"
    );
    assert!(app.hex.as_ref().unwrap().found.is_none());
    std::fs::remove_file(path).unwrap();
}

#[test]
fn an_empty_file_and_a_one_byte_file_open() {
    let (mut app, rx, _tx) = fresh();
    open(
        &mut app,
        &rx,
        write("hex_empty.weird", b""),
        OpenOptions::default(),
    );
    assert_eq!(app.input_mode, InputMode::Hex);
    let s = screen(&mut app, 60, 20);
    assert!(s.contains("Empty file"), "{s}");
    for code in [
        KeyCode::Char('j'),
        KeyCode::Char('G'),
        KeyCode::Char('w'),
        KeyCode::Char('b'),
        KeyCode::Char('$'),
        KeyCode::Char('v'),
        KeyCode::Char('n'),
    ] {
        press(&mut app, code);
    }
    press(&mut app, KeyCode::Char(':'));
    type_in(&mut app, "0");
    press(&mut app, KeyCode::Enter);
    assert!(
        app.hex.as_ref().unwrap().prompt_error.as_deref() == Some("The file is empty"),
        "{:?}",
        app.hex.as_ref().unwrap().prompt_error
    );
    press(&mut app, KeyCode::Esc);
    press(&mut app, KeyCode::Char('f'));
    type_in(&mut app, "x");
    press(&mut app, KeyCode::Enter);
    drain_events(&mut app, &rx);
    assert_eq!(app.flash_message(), Some("No match for x"));

    let (mut app, rx, _tx) = fresh();
    open(
        &mut app,
        &rx,
        write("hex_one.weird", b"A"),
        OpenOptions::default(),
    );
    for code in [
        KeyCode::Char('l'),
        KeyCode::Char('j'),
        KeyCode::Char('G'),
        KeyCode::Char('w'),
        KeyCode::Char('b'),
    ] {
        press(&mut app, code);
        assert_eq!(cursor(&app), 0);
    }
    let s = screen(&mut app, 80, 24);
    assert!(s.contains("00000000  41"), "{s}");
    assert!(s.contains("100.0%"), "{s}");
}

#[test]
fn the_info_panel_shows_the_file_as_hex_and_esc_comes_back() {
    let path = write("hex_info.csv", b"a,b\n1,2\n3,4\n");
    let (mut app, rx, _tx) = fresh();
    open(&mut app, &rx, path, OpenOptions::default());
    assert!(app.data_table_state.is_some());
    press(&mut app, KeyCode::Char('i'));
    assert_eq!(app.input_mode, InputMode::Info);
    let s = screen(&mut app, 100, 30);
    assert!(s.contains("x  Hex"), "the panel offers x: {s}");
    press(&mut app, KeyCode::Char('x'));
    drain_events(&mut app, &rx);
    assert_eq!(app.input_mode, InputMode::Hex);
    assert_eq!(app.hex_view().unwrap().origin, Origin::Table);
    let s = screen(&mut app, 100, 30);
    assert!(s.contains("61 2c 62 0a"), "{s}");
    assert!(s.contains("Back"), "Esc says where it goes: {s}");
    press(&mut app, KeyCode::Esc);
    assert_eq!(app.input_mode, InputMode::Normal);
    assert!(app.hex.is_none());
    assert!(app.data_table_state.is_some(), "the table is still there");
}

#[test]
fn a_fallback_reads_the_file_with_a_spec_picked_with_b() {
    let spec = datui::formats::Spec::parse(
        r#"name = "acme.sync"
[records]
fields = [
  { name = "magic", type = "str", size = 4 },
  { name = "count", type = "u4" },
  { name = "tail", type = "bytes", size = 13 },
]
"#,
        None,
    )
    .unwrap();
    let (mut app, rx, _tx) = fresh();
    app.set_formats(datui::formats::Registry::of(vec![spec]));
    let path = write("hex_pick.weird", &records());
    open(&mut app, &rx, path, OpenOptions::default());
    assert_eq!(app.input_mode, InputMode::Hex);
    let s = screen(&mut app, 120, 24);
    assert!(s.contains("B reads it with a spec"), "{s}");
    press(&mut app, KeyCode::Char('B'));
    let s = screen(&mut app, 120, 24);
    assert!(s.contains("acme.sync"), "{s}");
    let next = press(&mut app, KeyCode::Enter).expect("an open");
    let mut next = Some(next);
    while let Some(event) = next.take() {
        next = app.event(&event);
    }
    drain_events(&mut app, &rx);
    assert!(app.error_message().is_none(), "{:?}", app.error_message());
    let state = app.data_table_state.as_ref().expect("the table");
    assert_eq!(state.num_rows(), 200);
    assert_eq!(state.format_read().unwrap().spec.name, "acme.sync");
    assert_eq!(app.input_mode, InputMode::Normal);
}

#[test]
fn ctrl_x_at_home_shows_a_file_s_bytes_and_esc_goes_back_home() {
    let dir = common::fixture_dir().join("hex_home");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("hex_home.csv");
    std::fs::write(&path, b"a,b\n1,2\n").unwrap();
    let (mut app, rx, _tx) = fresh();
    app.enter_home();
    press(&mut app, KeyCode::Char('~'));
    type_in(&mut app, &dir.display().to_string());
    press(&mut app, KeyCode::Enter);
    let tx = _tx.clone();
    pump_until(&mut app, &rx, &tx, |app| {
        app.home.visible().iter().any(|row| {
            matches!(row, datui::home::Row::Entry { entry, .. } if entry.path.ends_with("hex_home.csv"))
        })
    });
    assert_eq!(app.input_mode, InputMode::Home);
    for _ in 0..5 {
        let on_file = !app.home.selection_is_the_door()
            && app
                .home
                .selected_entry()
                .is_some_and(|entry| entry.path.ends_with("hex_home.csv"));
        if on_file {
            break;
        }
        press(&mut app, KeyCode::Down);
    }
    let selected = app
        .home
        .selected_entry()
        .map(|entry| entry.path)
        .unwrap_or_default();
    assert!(selected.ends_with("hex_home.csv"), "{selected:?}");
    ctrl(&mut app, 'x');
    drain_events(&mut app, &rx);
    assert_eq!(app.input_mode, InputMode::Hex);
    assert_eq!(app.hex_view().unwrap().origin, Origin::Home);
    press(&mut app, KeyCode::Esc);
    assert_eq!(app.input_mode, InputMode::Home);
}

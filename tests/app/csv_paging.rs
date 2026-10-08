//! Paging through CSV files whose windows are read from marks: short rows, blank
//! lines and quotes inside fields show the rows they hold, where they hold them.

use super::*;

const SCREEN: Rect = Rect::new(0, 0, 160, 50);

/// `text` written as `name` and opened, its first frames drawn.
fn open_csv(name: &str, text: &str, options: OpenOptions) -> (App, mpsc::Receiver<AppEvent>) {
    let path = common::fixture_dir().join(name);
    std::fs::write(&path, text).unwrap();
    open_path(path, options)
}

fn open_path(path: PathBuf, options: OpenOptions) -> (App, mpsc::Receiver<AppEvent>) {
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path], options);
    settle(&mut app, &rx);
    (app, rx)
}

/// Draw, read what the frame asks for, and handle the answers, as the run loop does.
fn settle(app: &mut App, rx: &mpsc::Receiver<AppEvent>) {
    for _ in 0..4 {
        app.render(SCREEN, &mut Buffer::empty(SCREEN));
        app.frame_painted();
        app.request_what_the_frame_needs();
        drain_events(app, rx);
    }
}

/// The view's first row and the `id` drawn on it.
fn first_id(app: &App) -> (usize, Option<i64>) {
    let state = app.data_table_state.as_ref().unwrap();
    let id = state.display_slice_df().and_then(|df| {
        df.column("id")
            .ok()
            .and_then(|c| c.get(0).ok())
            .and_then(|value| value.extract::<i64>())
    });
    (state.start_row(), id)
}

/// A first row shorter than the header opens, its missing fields null.
#[test]
fn a_short_first_row_opens() {
    let mut text = String::from("id,desc,price\n0,item\n");
    for i in 1..5_000 {
        text.push_str(&format!("{i},item {i},{i}.5\n"));
    }
    let (app, _rx) = open_csv("csv_paging_short.csv", &text, OpenOptions::default());
    assert_eq!(app.error_message(), None);
    assert_eq!(first_id(&app), (0, Some(0)));
}

/// A blank line after the header opens, as a row of nulls.
#[test]
fn a_blank_line_after_the_header_opens() {
    let mut text = String::from("id,desc,price\n\n");
    for i in 0..5_000 {
        text.push_str(&format!("{i},item {i},{i}.5\n"));
    }
    let (app, _rx) = open_csv("csv_paging_blank.csv", &text, OpenOptions::default());
    assert_eq!(app.error_message(), None);
    assert_eq!(first_id(&app), (0, None));
}

/// Inch marks (`24" monitor`) every so many rows: paging shows each row where it is.
/// Without `--ignore-errors` Polars may refuse the file, and says so; it never shows
/// a row in another's place.
#[test]
fn inch_marks_do_not_shift_the_rows_paged_to() {
    let mut text = String::from("id,desc,price\n");
    for i in 0..60_000 {
        if i % 5_000 == 17 {
            text.push_str(&format!("{i},24\" monitor,{i}\n"));
        } else {
            text.push_str(&format!("{i},item {i},{i}.5\n"));
        }
    }
    for ignore_errors in [false, true] {
        let (mut app, rx) = open_csv(
            &format!("csv_paging_inches_{ignore_errors}.csv"),
            &text,
            OpenOptions {
                ignore_errors,
                ..OpenOptions::default()
            },
        );
        for page in 0..60 {
            press_key(&mut app, KeyCode::PageDown, KeyModifiers::NONE);
            settle(&mut app, &rx);
            if app.error_message().is_some() {
                assert!(!ignore_errors, "page {page}: {:?}", app.error_message());
                break;
            }
            let (row, id) = first_id(&app);
            assert_eq!(
                id,
                Some(row as i64),
                "page {page}, ignore errors {ignore_errors}"
            );
        }
    }
}

/// A directory of one CSV pages as the file does.
#[test]
fn a_directory_of_one_csv_pages_in_place() {
    let dir = common::fixture_dir().join("csv_paging_dir");
    std::fs::create_dir_all(&dir).unwrap();
    let mut text = String::from("id,desc\n");
    for i in 0..20_000 {
        text.push_str(&format!("{i},item {i}\n"));
    }
    std::fs::write(dir.join("only.csv"), text).unwrap();
    let (mut app, rx) = open_path(dir, OpenOptions::default());
    for _ in 0..30 {
        press_key(&mut app, KeyCode::PageDown, KeyModifiers::NONE);
        settle(&mut app, &rx);
    }
    let (row, id) = first_id(&app);
    assert!(row > 0);
    assert_eq!(id, Some(row as i64));
}

/// An unclosed quote at row 300 and inch marks every 997 rows. With
/// `--ignore-errors` the file has no marks and opens as Polars' slice reads it,
/// failing as before. Without, a jump shows either Polars' error or, before the
/// unclosed quote, each row where it is.
#[test]
fn jumps_past_an_unclosed_quote_show_no_row_out_of_place() {
    let mut text = String::from("id,desc,n\n");
    for i in 0..20_000 {
        if i == 300 {
            text.push_str(&format!("{i},\"oops,{i}\n"));
        } else if i % 997 == 5 {
            text.push_str(&format!("{i},24\" tv,{i}\n"));
        } else {
            text.push_str(&format!("{i},item {i},{i}\n"));
        }
    }
    let ignoring = OpenOptions {
        ignore_errors: true,
        ..OpenOptions::default()
    };
    let (app, _rx) = open_csv("csv_paging_unclosed_ignored.csv", &text, ignoring);
    assert!(
        app.error_message().is_some(),
        "opens as Polars' slice reads it, failing"
    );

    let (mut app, rx) = open_csv("csv_paging_unclosed.csv", &text, OpenOptions::default());
    for target in [250usize, 290, 299, 300, 310, 1_000, 5_000, 9_000] {
        if app.error_message().is_some() {
            press_key(&mut app, KeyCode::Esc, KeyModifiers::NONE);
            settle(&mut app, &rx);
        }
        if let Some(state) = app.data_table_state.as_mut() {
            state.scroll_to_row_centered(target);
        }
        app.spawn_async_collect(datui::App::LOADING_BUFFER);
        settle(&mut app, &rx);
        if app.error_message().is_some() {
            continue;
        }
        let (row, id) = first_id(&app);
        if row < 300 {
            assert_eq!(id, Some(row as i64), "jump to {target}");
        }
    }
}

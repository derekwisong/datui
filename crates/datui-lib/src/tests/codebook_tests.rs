use crate::*;
use polars::prelude::{IntoLazy, df};

/// The codebook of a GHCN-like collection entry, as a config would carry it.
fn codebook() -> codebook::Codebook {
    let dataset: config::DatasetConfig = toml::from_str(
        r#"
name = "Weather"
url = "s3://weather/ghcn/"
codebook = "https://example.com/readme.txt"

[columns.ELEMENT]
description = "Element type"

[columns.ELEMENT.values]
AWDR = "Average daily wind direction (degrees)"
TMAX = "Maximum temperature (tenths of degrees C)"

[columns.DATA_VALUE]
description = "Data value for ELEMENT"
unit = "per ELEMENT"

[columns.Q_FLAG]
description = "Quality flag; blank is normal"

[columns.Q_FLAG.values]
"" = "did not fail any quality assurance check"
S = "failed spatial consistency check"
"#,
    )
    .unwrap();
    codebook::Codebook::of(&dataset).unwrap()
}

fn app() -> (App, std::sync::mpsc::Receiver<AppEvent>) {
    let (tx, rx) = std::sync::mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    let df = df!(
        "ELEMENT" => ["AWDR", "TMAX"],
        "DATA_VALUE" => [270i64, 312],
        "Q_FLAG" => [None, Some("S")],
    )
    .unwrap();
    let mut state = DataTableState::from_lazyframe(df.lazy(), &OpenOptions::default()).unwrap();
    // Collects the rows on screen, as the inspector's own tests do.
    state.set_column_order(vec![
        "ELEMENT".to_string(),
        "DATA_VALUE".to_string(),
        "Q_FLAG".to_string(),
    ]);
    app.data_table_state = Some(state);
    app.codebook = Some(std::sync::Arc::new(codebook()));
    draw(&mut app);
    (app, rx)
}

/// The screen, one line per row.
fn draw(app: &mut App) -> String {
    let area = Rect::new(0, 0, 120, 40);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    (0..area.height)
        .map(|y| {
            (0..area.width)
                .map(|x| buf[(x, y)].symbol())
                .collect::<String>()
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn press(app: &mut App, code: KeyCode) -> Option<AppEvent> {
    app.event(&AppEvent::Key(KeyEvent::new(code, KeyModifiers::NONE)))
}

#[test]
fn info_says_what_each_column_means_and_where_that_comes_from() {
    let (mut app, _rx) = app();
    press(&mut app, KeyCode::Char('i'));
    let screen = draw(&mut app);
    assert!(
        screen.contains("Documentation: https://example.com/readme.txt"),
        "{screen}"
    );
    assert!(screen.contains("About"), "{screen}");
    assert!(
        screen.contains("Data value for ELEMENT (per ELEMENT)"),
        "{screen}"
    );
    assert!(screen.contains("Quality flag; blank is normal"), "{screen}");
    // The selected column in full, with its codes.
    assert!(screen.contains("ELEMENT: Element type"), "{screen}");
    assert!(screen.contains("Codes: AWDR"), "{screen}");
    press(&mut app, KeyCode::Down);
    press(&mut app, KeyCode::Down);
    let screen = draw(&mut app);
    assert!(screen.contains("Codes: blank"), "{screen}");
}

#[test]
fn info_without_a_codebook_has_no_about_column() {
    let (mut app, _rx) = app();
    app.codebook = None;
    press(&mut app, KeyCode::Char('i'));
    let screen = draw(&mut app);
    assert!(!screen.contains("About"), "{screen}");
    assert!(!screen.contains("Documentation:"), "{screen}");
}

#[test]
fn the_inspector_names_the_code_under_the_cursor() {
    let (mut app, _rx) = app();
    press(&mut app, KeyCode::Char(' '));
    assert_eq!(app.input_mode, InputMode::Inspect);
    let screen = draw(&mut app);
    assert!(screen.contains("Element type"), "{screen}");
    assert!(
        screen.contains("AWDR = Average daily wind direction (degrees)"),
        "{screen}"
    );
    // A blank flag says it is the normal one.
    press(&mut app, KeyCode::End);
    let screen = draw(&mut app);
    assert!(
        screen.contains("blank = did not fail any quality assurance check"),
        "{screen}"
    );
}

use crate::*;
use polars::prelude::{IntoLazy, df};

/// The column notes of a GHCN-like catalog entry, as a catalog would carry them.
fn codebook() -> codebook::Codebook {
    let catalog = catalog::parse(
        r#"
[weather]
name = "Weather"
url = "s3://weather/ghcn/"
documentation = "https://example.com/readme.txt"

columns.ELEMENT.description = "Element type"
columns.DATA_VALUE = { description = "Data value for ELEMENT", unit = "per ELEMENT" }
columns.Q_FLAG.description = "Quality flag; blank is normal"

[weather.columns.ELEMENT.values]
AWDR = "Average daily wind direction (degrees)"
TMAX = "Maximum temperature (tenths of degrees C)"

[weather.columns.Q_FLAG.values]
"" = "did not fail any quality assurance check"
S = "failed spatial consistency check"
"#,
        "t",
        catalog::Origin::Listed,
        None,
    )
    .unwrap();
    codebook::Codebook::of(&catalog.datasets[0]).unwrap()
}

/// The bundled NOAA entry keeps the readme's source flags under S_FLAG, where they
/// belong, and only element codes under ELEMENT.
#[test]
fn the_noaa_source_flags_are_s_flags_not_elements() {
    let catalog = catalog::bundled();
    let noaa = catalog
        .datasets
        .iter()
        .find(|d| d.url.as_deref() == Some("s3://noaa-ghcn-pds/parquet/"))
        .expect("the NOAA entry");
    let book = codebook::Codebook::of(noaa).unwrap();
    let element = book.column("ELEMENT").unwrap();
    assert!(
        element.values.keys().all(|code| code.len() == 4),
        "{:?}",
        element.values.keys()
    );
    let source = book.column("S_FLAG").unwrap();
    // The readme's thirty-five: blank, the digits, and the letters.
    let codes = "0 1 2 6 7 A a B b C D d E F G H I K M f m N Q R r S s T U u W X Z z";
    for code in std::iter::once("").chain(codes.split(' ')) {
        assert!(source.values.contains_key(code), "S_FLAG lacks {code:?}");
    }
    assert_eq!(source.values.len(), 35);
    assert_eq!(
        source.legend_line(Some("C")).as_deref(),
        Some("C = Environment Canada")
    );
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
    app.info.codebook = Some(std::sync::Arc::new(codebook()));
    draw(&mut app);
    (app, rx)
}

/// The screen, one line per row.
fn draw(app: &mut App) -> String {
    let area = Rect::new(0, 0, 120, 40);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    crate::tests::buffer_text(&buf)
}

fn press(app: &mut App, code: KeyCode) -> Option<AppEvent> {
    app.event(AppEvent::Key(KeyEvent::new(code, KeyModifiers::NONE)))
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
    app.info.codebook = None;
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

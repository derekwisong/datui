use super::chart_prepare_tests::open;
use crate::*;
use std::sync::mpsc;

/// A view saved on a remote dataset under a query that renames its columns
/// records the URL and the columns as loaded: it matches the same URL as the
/// same file, and another file with the source columns by schema.
#[test]
fn a_view_on_a_queried_remote_dataset_matches_by_url_and_source_columns() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tmax.csv");
    std::fs::write(
        &path,
        "ID,DATE,DATA_VALUE\nUSW1,20240101,55\nUSW1,20240102,61\n",
    )
    .unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), crate::tests::test_runtime());
    open(&mut app, &rx, &tx, path);
    // Views of this test's own, so no other test's saved views show in the list.
    let config = crate::config::ConfigManager::with_dir(dir.path().join("config"));
    app.template_manager = TemplateManager::new(&config).unwrap().into();

    // Stand in for an S3 dataset: only the path decides how a view records it.
    let url = PathBuf::from("s3://noaa-ghcn-pds/parquet/by_year/YEAR=2024/ELEMENT=TMAX/");
    app.path = Some(url.clone());
    app.data_table_state.as_mut().unwrap().sql_query(
        "SELECT DATE AS day, DATA_VALUE / 10.0 AS high_c FROM df WHERE ID = 'USW1'".to_string(),
    );
    assert!(app.data_table_state.as_ref().unwrap().error().is_none());

    app.open_save_view_form();
    assert_eq!(
        app.template_modal.exact_path_input.value(),
        url.to_string_lossy(),
        "the URL, not the working directory joined to it"
    );
    assert_eq!(
        app.template_modal.relative_path_input.value(),
        "",
        "a URL has no relative form"
    );
    app.save_view_form();
    let saved = &app.template_manager.all_templates()[0].match_criteria;
    assert_eq!(saved.exact_path.as_deref(), Some(url.as_path()));
    assert_eq!(saved.relative_path, None);
    assert_eq!(
        saved.schema_columns.as_deref(),
        Some(
            &[
                "ID".to_string(),
                "DATE".to_string(),
                "DATA_VALUE".to_string()
            ][..]
        ),
        "the columns as loaded, not the query's output"
    );

    app.refresh_view_list();
    assert_eq!(
        app.template_modal.rows[0].reason,
        Some(template::MatchReason::SameFile)
    );

    // The next year's file, freshly opened: its columns are the source columns.
    let next = dir.path().join("tmax_2023.csv");
    std::fs::write(&next, "ID,DATE,DATA_VALUE\nUSW1,20230101,40\n").unwrap();
    open(&mut app, &rx, &tx, next);
    app.path = Some(PathBuf::from(
        "s3://noaa-ghcn-pds/parquet/by_year/YEAR=2023/ELEMENT=TMAX/",
    ));
    app.refresh_view_list();
    assert_eq!(
        app.template_modal.rows[0].reason,
        Some(template::MatchReason::SameColumns)
    );

    // `V` applies it there: the next year's rows under the saved query.
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('V'),
        KeyModifiers::NONE,
    )));
    let state = app.data_table_state.as_ref().unwrap();
    assert!(state.error().is_none(), "{:?}", state.error());
    assert!(app.active_template_id.is_some(), "the view is applied");
    let names: Vec<&str> = state.schema().iter_names().map(|n| n.as_str()).collect();
    assert_eq!(names, ["day", "high_c"]);
    // Once its rows are in, the bar says which view and why.
    super::chart_prepare_tests::pump(&mut app, &rx, &tx, |a| !a.is_busy());
    assert_eq!(
        app.flash_message(),
        Some("View \"ELEMENT=TMAX\" applied: same columns")
    );
}

/// Views of this test's own, so no other test's saved views show in the list.
fn own_views(app: &mut App, dir: &Path) {
    let config = crate::config::ConfigManager::with_dir(dir.join("config"));
    app.template_manager = TemplateManager::new(&config).unwrap().into();
}

/// Open `paths` with `options`, as the command line or the home screen does.
fn open_with(
    app: &mut App,
    rx: &mpsc::Receiver<AppEvent>,
    tx: &mpsc::Sender<AppEvent>,
    open: AppEvent,
) {
    app.input_mode = InputMode::Normal;
    if let Some(next) = app.event(&open) {
        let _ = tx.send(next);
    }
    super::chart_prepare_tests::pump(app, rx, tx, |a| {
        a.data_table_state.is_some() && !a.is_busy()
    });
}

fn press(app: &mut App, c: char) {
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char(c),
        KeyModifiers::NONE,
    )));
}

/// A view saved on one table of a database records the table: it is the same file to
/// that table however it is named (`shop.db/orders`, `shop.db --table orders`), and
/// to no other table of the file. A download names the database alone, so without
/// the table every table of it was the same file.
#[cfg(feature = "sqlite")]
#[test]
fn a_view_saved_on_one_table_fits_that_table_of_the_file_alone() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("shop.db");
    rusqlite::Connection::open(&db)
        .unwrap()
        .execute_batch(
            "CREATE TABLE orders (order_id INTEGER, total REAL);
             INSERT INTO orders VALUES (1, 9.5), (2, 3.0), (3, 7.25);
             CREATE TABLE customers (customer_id INTEGER, name TEXT);
             INSERT INTO customers VALUES (1, 'Ada'), (2, 'Lin');",
        )
        .unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), crate::tests::test_runtime());
    own_views(&mut app, dir.path());
    let table = |name: &str| OpenOptions {
        table: Some(name.to_string()),
        ..OpenOptions::default()
    };
    let as_place = |name: &str| AppEvent::Open(vec![db.join(name)], OpenOptions::default());
    let by_flag = |name: &str| AppEvent::Open(vec![db.clone()], table(name));
    let reason = |app: &mut App| {
        app.refresh_view_list();
        app.template_modal.rows[0].reason
    };

    open_with(&mut app, &rx, &tx, as_place("orders"));
    app.data_table_state
        .as_mut()
        .unwrap()
        .sort_by(vec!["total".to_string()], vec![true]);
    app.open_save_view_form();
    assert_eq!(app.template_modal.table.as_deref(), Some("orders"));
    app.save_view_form();
    let saved = &app.template_manager.all_templates()[0];
    assert_eq!(saved.match_criteria.table.as_deref(), Some("orders"));
    assert_eq!(reason(&mut app), Some(template::MatchReason::SameFile));

    open_with(&mut app, &rx, &tx, by_flag("orders"));
    assert_eq!(
        reason(&mut app),
        Some(template::MatchReason::SameFile),
        "--table names the same table"
    );
    open_with(&mut app, &rx, &tx, as_place("customers"));
    assert_eq!(reason(&mut app), None, "another table of the file");
    // V finds nothing that fits, so it opens the list rather than apply the view.
    press(&mut app, 'V');
    assert!(app.active_template_id.is_none());
    assert!(app.template_modal.active, "the list opens instead");
    app.template_modal.close();

    // Downloaded, the dataset is the database's URL whatever the table.
    let url = PathBuf::from("https://example.com/data/shop.db");
    open_with(&mut app, &rx, &tx, by_flag("orders"));
    app.path = Some(url.clone());
    app.open_save_view_form();
    app.template_modal.name_input.set_value("remote orders");
    app.save_view_form();
    let remote = app
        .template_manager
        .get_template_by_name("remote orders")
        .unwrap();
    assert_eq!(
        remote.match_criteria.exact_path.as_deref(),
        Some(url.as_path())
    );
    assert_eq!(remote.match_criteria.table.as_deref(), Some("orders"));
    open_with(&mut app, &rx, &tx, by_flag("customers"));
    app.path = Some(url);
    app.refresh_view_list();
    assert!(
        app.template_modal
            .rows
            .iter()
            .all(|row| row.reason.is_none()),
        "the URL's other table fits neither view"
    );

    // Applied on open, the view says which it is and why.
    app.app_config.templates.auto_apply = true;
    open_with(&mut app, &rx, &tx, by_flag("orders"));
    assert!(app.active_template_id.is_some(), "the view is applied");
    assert_eq!(
        app.flash_message(),
        Some("View \"orders\" applied: same file")
    );
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.get_sort_columns(), ["total".to_string()]);
}

/// A frame handed over from Python (`datui.view(frame)`) has no path: its views
/// are listed, saved and applied by its columns, as data piped in is.
#[test]
fn a_frame_from_python_matches_views_by_its_columns() {
    let dir = tempfile::tempdir().unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), crate::tests::test_runtime());
    own_views(&mut app, dir.path());
    let frame = |rows: i64| {
        let df = polars::df!(
            "city" => (0..rows).map(|i| format!("c{i}")).collect::<Vec<_>>(),
            "temp" => (0..rows).collect::<Vec<_>>(),
        )
        .unwrap();
        AppEvent::OpenLazyFrame(
            Box::new(polars::prelude::IntoLazy::lazy(df)),
            OpenOptions::default(),
        )
    };

    open_with(&mut app, &rx, &tx, frame(5));
    assert!(app.path.is_none(), "a frame has no path");
    press(&mut app, 'v');
    assert!(app.template_modal.active, "v opens the views list");
    app.data_table_state
        .as_mut()
        .unwrap()
        .sort_by(vec!["temp".to_string()], vec![true]);
    press(&mut app, 's');
    assert!(app.template_modal.schema_match_enabled);
    assert_eq!(app.template_modal.exact_path_input.value(), "");
    app.template_modal.name_input.set_value("warmest");
    app.save_view_form();
    assert_eq!(
        app.template_modal.rows[0].reason,
        Some(template::MatchReason::SameColumns)
    );
    app.template_modal.close();

    // The next frame with these columns: V applies the view and says why.
    open_with(&mut app, &rx, &tx, frame(8));
    assert!(app.active_template_id.is_none());
    press(&mut app, 'V');
    super::chart_prepare_tests::pump(&mut app, &rx, &tx, |a| !a.is_busy());
    assert!(app.active_template_id.is_some(), "V applies the view");
    assert_eq!(
        app.flash_message(),
        Some("View \"warmest\" applied: same columns")
    );

    // And auto-apply dresses a frame as it opens.
    app.app_config.templates.auto_apply = true;
    open_with(&mut app, &rx, &tx, frame(3));
    assert!(app.active_template_id.is_some(), "applied on open");
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.get_sort_columns(), ["temp".to_string()]);
}

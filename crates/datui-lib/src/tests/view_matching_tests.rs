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

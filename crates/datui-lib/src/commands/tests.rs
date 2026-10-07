use super::*;

#[test]
fn views_are_listed_removed_by_name_and_cleared() {
    let dir = tempfile::tempdir().unwrap();
    let config = ConfigManager::with_dir(dir.path().to_path_buf());
    let mut manager = ViewManager::new(&config).unwrap();
    for name in ["daily", "weekly"] {
        let criteria = MatchCriteria {
            exact_path: None,
            relative_path: None,
            path_pattern: None,
            filename_pattern: Some(format!("{name}_*.csv")),
            schema_columns: None,
            schema_types: None,
            table: None,
        };
        let settings = crate::view::ViewSettings {
            chart: None,
            sample: None,
            query: None,
            sql_query: None,
            fuzzy_query: None,
            filters: Vec::new(),
            sort_columns: Vec::new(),
            sort_descending: Vec::new(),
            sort_ascending: true,
            column_order: Vec::new(),
            locked_columns_count: 0,
            pivot: None,
            melt: None,
            reshape_source: None,
            columns: Vec::new(),
        };
        manager
            .create_view(name.to_string(), None, criteria, settings)
            .unwrap();
    }
    let (text, code) = views(&config, &ViewsAction::List);
    assert_eq!(code, 0);
    assert!(
        text.contains("daily") && text.contains("weekly_*.csv"),
        "{text}"
    );
    let (text, code) = views(
        &config,
        &ViewsAction::Rm {
            name: "nope".into(),
        },
    );
    assert_eq!(code, 1, "{text}");
    assert_eq!(
        views(
            &config,
            &ViewsAction::Rm {
                name: "daily".into()
            }
        )
        .1,
        0
    );
    let (text, _) = views(&config, &ViewsAction::List);
    assert!(!text.contains("daily") && text.contains("weekly"), "{text}");
    let (text, code) = views(&config, &ViewsAction::Clear);
    assert_eq!((text.as_str(), code), ("Removed 1 saved views\n", 0));
    assert_eq!(views(&config, &ViewsAction::List).0, "No saved views\n");
}

#[test]
fn cache_clear_says_what_it_did() {
    let (text, code) = cache(None, &CacheAction::Clear { recents: false });
    assert_eq!((text.as_str(), code), ("No cache to clear\n", 0));
}

#[test]
fn man_lists_writes_and_prints_pages() {
    let (list, code) = man(None, true, None, false);
    assert_eq!(code, 0);
    assert!(list.contains("datui-config(5)"), "{list}");
    let dir = tempfile::tempdir().unwrap();
    let (_, code) = man(None, false, Some(dir.path()), false);
    assert_eq!(code, 0);
    assert!(dir.path().join("man1/datui.1").exists());
    assert!(dir.path().join("man5/datui-config.5").exists());
    assert!(dir.path().join("man7/datui-keys.7").exists());
    let (page, code) = man(Some("keys"), false, None, false);
    assert_eq!(code, 0);
    assert!(page.contains(".TH DATUI\\-KEYS 7"), "{page}");
    // Without man(1), the page reads as text.
    let text = datui_cli::man::find("keys").unwrap().plain(80);
    assert!(text.starts_with("DATUI-KEYS(7)"), "{text}");
    assert!(text.contains("Ctrl+O"), "{text}");
    let (said, code) = man(Some("nope"), false, None, false);
    assert_eq!(code, 1);
    assert!(said.contains("datui man --list"), "{said}");
}

use super::*;

/// Old view JSON without sql_query or fuzzy_query deserializes; those fields default to None.
#[test]
fn test_settings_deserialize_without_sql_fuzzy() {
    let json = r#"{
            "query": "select a",
            "filters": [],
            "sort_columns": [],
            "sort_ascending": true,
            "column_order": ["a", "b"],
            "locked_columns_count": 0
        }"#;
    let settings: ViewSettings = serde_json::from_str(json).unwrap();
    assert_eq!(settings.query, Some("select a".to_string()));
    assert_eq!(settings.sql_query, None);
    assert_eq!(settings.fuzzy_query, None);
    assert!(settings.reshape_source.is_none());
}

/// A reshape's source is written only when there is one, and only what it holds, so
/// a view without one reads the same to an older datui.
#[test]
fn test_reshape_source_is_written_only_when_present() {
    let mut settings = a_view("t", no_criteria()).settings;
    let json = serde_json::to_string(&settings).unwrap();
    assert!(!json.contains("reshape_source"), "{json}");

    settings.reshape_source = Some(ReshapeSource {
        sql_query: Some("SELECT * FROM df".to_string()),
        ..ReshapeSource::default()
    });
    let json = serde_json::to_string(&settings).unwrap();
    assert!(
        json.contains(r#""reshape_source":{"sql_query":"SELECT * FROM df"}"#),
        "{json}"
    );
    let back: ViewSettings = serde_json::from_str(&json).unwrap();
    assert_eq!(
        back.reshape_source.and_then(|s| s.sql_query).as_deref(),
        Some("SELECT * FROM df")
    );
}

fn a_view(name: &str, criteria: MatchCriteria) -> SavedView {
    SavedView {
        id: name.to_string(),
        name: name.to_string(),
        description: None,
        created: SystemTime::now(),
        last_used: Some(SystemTime::now()),
        usage_count: 10,
        last_matched_file: None,
        match_criteria: criteria,
        settings: ViewSettings {
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
        },
    }
}

fn no_criteria() -> MatchCriteria {
    MatchCriteria {
        exact_path: None,
        relative_path: None,
        path_pattern: None,
        filename_pattern: None,
        schema_columns: None,
        schema_types: None,
        table: None,
    }
}

/// A view's path criteria fit only the table it was saved on; one saved before
/// tables were recorded fits any, as it always did.
#[test]
fn path_criteria_fit_only_the_saved_table() {
    let url = Path::new("https://example.com/shop.db");
    let schema = Schema::default();
    let on = |table| Dataset {
        path: url,
        table: Some(table),
    };
    let view = a_view(
        "orders",
        MatchCriteria {
            exact_path: Some(url.to_path_buf()),
            filename_pattern: Some("shop.db".to_string()),
            table: Some("orders".to_string()),
            ..no_criteria()
        },
    );
    assert_eq!(
        match_reason(&view, on("orders"), &schema),
        Some(MatchReason::SameFile)
    );
    assert_eq!(match_reason(&view, on("customers"), &schema), None);
    assert_eq!(match_reason(&view, url, &schema), None, "no table named");
    assert!(!filename_pattern_matches(
        &view.match_criteria,
        on("customers")
    ));

    let older = a_view(
        "older",
        MatchCriteria {
            exact_path: Some(url.to_path_buf()),
            ..no_criteria()
        },
    );
    assert_eq!(
        match_reason(&older, on("customers"), &schema),
        Some(MatchReason::SameFile)
    );
}

/// A table inside its file is spelled with the file's resolved path, so the place
/// a link or a relative path names matches the one the home screen lists.
#[cfg(unix)]
#[test]
fn a_table_inside_a_file_is_located_through_the_file() {
    let dir = tempfile::tempdir().unwrap();
    let real = dir.path().join("real");
    fs::create_dir(&real).unwrap();
    fs::write(real.join("shop.db"), b"").unwrap();
    let link = dir.path().join("link");
    std::os::unix::fs::symlink(&real, &link).unwrap();
    let resolved = crate::canonical::canonicalize(&real).unwrap();
    assert_eq!(
        exact_location(&link.join("shop.db").join("orders")),
        resolved.join("shop.db").join("orders")
    );
}

/// Usage and recency raise the score but are not a match: a well-used
/// view whose criteria fit nothing must never be what `V` applies.
#[test]
fn usage_alone_is_not_a_match() {
    use polars::prelude::DataType;
    let schema = Schema::from_iter([("a".into(), DataType::Int64)]);
    let path = Path::new("/data/other.parquet");

    let unrelated = a_view(
        "well used, fits nothing",
        MatchCriteria {
            filename_pattern: Some("sales_*.csv".to_string()),
            ..no_criteria()
        },
    );
    assert!(!match_reason(&unrelated, path, &schema).is_some());
    assert!(calculate_relevance(&unrelated, path.into(), &schema) > 0.0);

    let fits = a_view(
        "fits by schema",
        MatchCriteria {
            schema_columns: Some(vec!["a".to_string()]),
            ..no_criteria()
        },
    );
    assert!(match_reason(&fits, path, &schema).is_some());
}

/// Data piped in matches by its columns alone: no path or pattern fits `-`, even
/// a pattern of `*` or a view saved with that name as its path.
#[test]
fn stdin_matches_by_schema_only() {
    use polars::prelude::DataType;
    let schema = Schema::from_iter([("a".into(), DataType::Int64)]);
    let stdin = Path::new(crate::loading::stdin::PATH);
    let by_path = a_view(
        "every path",
        MatchCriteria {
            exact_path: Some(PathBuf::from("-")),
            relative_path: Some("-".to_string()),
            path_pattern: Some("*".to_string()),
            filename_pattern: Some("*".to_string()),
            ..no_criteria()
        },
    );
    assert_eq!(match_reason(&by_path, stdin, &schema), None);
    assert!(calculate_relevance(&by_path, stdin.into(), &schema) < 50.0);
    let by_schema = a_view(
        "by schema",
        MatchCriteria {
            schema_columns: Some(vec!["a".to_string()]),
            ..no_criteria()
        },
    );
    assert_eq!(
        match_reason(&by_schema, stdin, &schema),
        Some(MatchReason::SameColumns)
    );
}

/// A view's schema criterion asks for its columns to be present; a file
/// with extra columns still fits ("similar table"), a file missing one does not.
#[test]
fn schema_criterion_is_a_subset_test() {
    use polars::prelude::DataType;
    let schema = Schema::from_iter([
        ("a".into(), DataType::Int64),
        ("b".into(), DataType::String),
    ]);
    let view = a_view(
        "wants a and b",
        MatchCriteria {
            schema_columns: Some(vec!["a".into(), "b".into()]),
            ..no_criteria()
        },
    );
    assert!(match_reason(&view, Path::new("/x.parquet"), &schema).is_some());

    let narrower = Schema::from_iter([("a".into(), DataType::Int64)]);
    assert!(!match_reason(&view, Path::new("/x.parquet"), &narrower).is_some());
}

#[test]
fn test_matches_pattern() {
    assert!(matches_pattern("test.csv", "test.csv"));
    assert!(matches_pattern("test.csv", "*.csv"));
    assert!(matches_pattern("sales_2024.csv", "sales_*.csv"));
    assert!(matches_pattern(
        "/data/reports/sales.csv",
        "/data/reports/*.csv"
    ));
    assert!(!matches_pattern("test.txt", "*.csv"));
    assert!(!matches_pattern("sales.csv", "sales_*.csv"));
}

/// The list's annotation names the strongest criterion that fits: the
/// same file beats the same columns beats a pattern.
#[test]
fn match_reason_names_the_strongest_criterion() {
    use polars::prelude::DataType;
    let schema = Schema::from_iter([("a".into(), DataType::Int64)]);
    let path = Path::new("/data/sales_2024.csv");

    let by_path = a_view(
        "by path",
        MatchCriteria {
            exact_path: Some(path.to_path_buf()),
            schema_columns: Some(vec!["a".into()]),
            filename_pattern: Some("sales_*.csv".into()),
            ..no_criteria()
        },
    );
    assert_eq!(
        match_reason(&by_path, path, &schema),
        Some(MatchReason::SameFile)
    );

    let by_schema = a_view(
        "by schema",
        MatchCriteria {
            schema_columns: Some(vec!["a".into()]),
            filename_pattern: Some("sales_*.csv".into()),
            ..no_criteria()
        },
    );
    assert_eq!(
        match_reason(&by_schema, path, &schema),
        Some(MatchReason::SameColumns)
    );

    let by_pattern = a_view(
        "by pattern",
        MatchCriteria {
            filename_pattern: Some("sales_*.csv".into()),
            ..no_criteria()
        },
    );
    assert_eq!(
        match_reason(&by_pattern, path, &schema),
        Some(MatchReason::Glob)
    );

    let fits_nothing = a_view(
        "fits nothing",
        MatchCriteria {
            filename_pattern: Some("other_*.csv".into()),
            ..no_criteria()
        },
    );
    assert_eq!(match_reason(&fits_nothing, path, &schema), None);
    assert!(!match_reason(&fits_nothing, path, &schema).is_some());
}

/// A URL is recorded as written and matches itself, with or without the
/// trailing slash a directory's URL may carry; another URL is another file.
#[test]
fn a_remote_path_matches_the_same_url() {
    use polars::prelude::DataType;
    let schema = Schema::from_iter([("DATA_VALUE".into(), DataType::Int64)]);
    let url = Path::new("s3://noaa-ghcn-pds/parquet/by_year/YEAR=2024/ELEMENT=TMAX/");
    assert_eq!(exact_location(url), url);
    assert_eq!(relative_location(url), None);

    let view = a_view(
        "tmax",
        MatchCriteria {
            exact_path: Some(exact_location(url)),
            relative_path: relative_location(url),
            ..no_criteria()
        },
    );
    assert_eq!(
        match_reason(&view, url, &schema),
        Some(MatchReason::SameFile)
    );
    let unslashed = Path::new("s3://noaa-ghcn-pds/parquet/by_year/YEAR=2024/ELEMENT=TMAX");
    assert_eq!(
        match_reason(&view, unslashed, &schema),
        Some(MatchReason::SameFile)
    );
    let upper = Path::new("S3://noaa-ghcn-pds/parquet/by_year/YEAR=2024/ELEMENT=TMAX");
    assert_eq!(
        match_reason(&view, upper, &schema),
        Some(MatchReason::SameFile)
    );
    let key_case = Path::new("s3://noaa-ghcn-pds/parquet/by_year/YEAR=2024/element=TMAX/");
    assert_eq!(match_reason(&view, key_case, &schema), None);
    let other_year = Path::new("s3://noaa-ghcn-pds/parquet/by_year/YEAR=2023/ELEMENT=TMAX/");
    assert_eq!(match_reason(&view, other_year, &schema), None);
    assert!(calculate_relevance(&view, url.into(), &schema) >= 1000.0);
}

/// A local file opened by a relative path is the same file as its absolute,
/// resolved path, and has a path relative to the working directory.
#[test]
fn a_relative_local_path_matches_its_absolute_path() {
    let schema = Schema::default();
    let opened = Path::new("Cargo.toml");
    let absolute = crate::canonical::canonicalize(opened).unwrap();
    assert_eq!(exact_location(opened), absolute);
    assert_eq!(relative_location(opened).as_deref(), Some("Cargo.toml"));

    let by_exact = a_view(
        "exact",
        MatchCriteria {
            exact_path: Some(absolute.clone()),
            ..no_criteria()
        },
    );
    assert_eq!(
        match_reason(&by_exact, opened, &schema),
        Some(MatchReason::SameFile)
    );
    let by_relative = a_view(
        "relative",
        MatchCriteria {
            relative_path: Some("Cargo.toml".into()),
            ..no_criteria()
        },
    );
    assert_eq!(
        match_reason(&by_relative, opened, &schema),
        Some(MatchReason::SameFile)
    );
    // The save form suggests a pattern from the resolved directory.
    let by_pattern = a_view(
        "pattern",
        MatchCriteria {
            path_pattern: Some(format!(
                "{}{}*.toml",
                absolute.parent().unwrap().display(),
                std::path::MAIN_SEPARATOR
            )),
            ..no_criteria()
        },
    );
    assert_eq!(
        match_reason(&by_pattern, opened, &schema),
        Some(MatchReason::Glob)
    );
    assert!(calculate_relevance(&by_pattern, opened.into(), &schema) >= 50.0);
}

/// Views saved before URLs were told apart from local paths carry the working
/// directory joined to the URL, and the URL as the relative path. They still
/// load, with the URL as their exact path, and match it.
#[test]
fn a_view_saved_with_a_mangled_url_loads_with_the_url() {
    let dir = tempfile::tempdir().unwrap();
    let config = ConfigManager::with_dir(dir.path().to_path_buf());
    let views = dir.path().join("views");
    fs::create_dir_all(&views).unwrap();
    let json = r#"{
            "id": "old",
            "name": "old",
            "description": null,
            "created": 1790000000,
            "usage_count": 0,
            "match_criteria": {
                "exact_path": "/home/me/work/s3://noaa-ghcn-pds/parquet/by_year/YEAR=2024/ELEMENT=TMAX/",
                "relative_path": "s3://noaa-ghcn-pds/parquet/by_year/YEAR=2024/ELEMENT=TMAX",
                "filename_pattern": "ELEMENT=TMAX",
                "schema_columns": ["day", "high_c"]
            },
            "settings": {
                "sql_query": "SELECT 1 AS day, 2 AS high_c FROM df",
                "filters": [],
                "sort_columns": [],
                "sort_ascending": true,
                "column_order": [],
                "locked_columns_count": 0
            }
        }"#;
    fs::write(views.join("view_old.json"), json).unwrap();

    let manager = ViewManager::new(&config).unwrap();
    assert!(manager.broken_views.is_empty());
    let view = manager.get_view_by_id("old").unwrap();
    let url = "s3://noaa-ghcn-pds/parquet/by_year/YEAR=2024/ELEMENT=TMAX/";
    assert_eq!(
        view.match_criteria.exact_path.as_deref(),
        Some(Path::new(url))
    );
    assert_eq!(view.match_criteria.relative_path, None);
    assert_eq!(
        match_reason(view, Path::new(url), &Schema::default()),
        Some(MatchReason::SameFile)
    );
}

#[test]
fn unmangled_url_finds_the_url_behind_a_local_prefix() {
    assert_eq!(
        unmangled_url(Path::new("/work/gs://bucket/a.parquet")),
        Some(PathBuf::from("gs://bucket/a.parquet"))
    );
    assert_eq!(
        unmangled_url(Path::new(r"C:\work\https://example.com/a.csv")),
        Some(PathBuf::from("https://example.com/a.csv"))
    );
    assert_eq!(unmangled_url(Path::new("s3://bucket/a.parquet")), None);
    assert_eq!(unmangled_url(Path::new("/data/a.csv")), None);
    assert_eq!(unmangled_url(Path::new("/data/odd://name.csv")), None);
}

#[test]
fn test_pattern_specificity_bonus() {
    assert_eq!(pattern_specificity_bonus("test.csv"), 10.0);
    assert_eq!(pattern_specificity_bonus("*.csv"), 5.0);
    assert_eq!(pattern_specificity_bonus("sales_*.csv"), 5.0);
}

use color_eyre::Result;
use datui::config::ConfigManager;
use datui::filter_modal::{FilterOperator, FilterStatement, LogicalOperator};
use datui::template::{MatchCriteria, TemplateManager, TemplateSettings};
use std::path::PathBuf;
use std::time::SystemTime;

/// Create a unique temporary directory for testing.
fn create_test_temp_dir() -> Result<PathBuf> {
    let thread_id = std::thread::current().id();
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let temp_dir = std::env::temp_dir().join(format!(
        "datui_test_{}_{:?}_{}",
        std::process::id(),
        thread_id,
        timestamp
    ));
    std::fs::create_dir_all(&temp_dir)?;
    Ok(temp_dir)
}

#[test]
fn test_template_creation() -> Result<()> {
    let temp_dir = create_test_temp_dir()?;
    let config = ConfigManager::with_dir(temp_dir.clone());

    let mut manager = TemplateManager::new(&config)?;
    let match_criteria = MatchCriteria {
        exact_path: Some(PathBuf::from("/test/path.csv")),
        relative_path: None,
        path_pattern: None,
        filename_pattern: None,
        schema_columns: Some(vec!["col1".to_string(), "col2".to_string()]),
        schema_types: None,
        table: None,
    };

    let settings = TemplateSettings {
        query: Some("select a, b".to_string()),
        sql_query: None,
        fuzzy_query: None,
        filters: vec![FilterStatement {
            column: "col1".to_string(),
            operator: FilterOperator::Gt,
            value: "10".to_string(),
            logical_op: LogicalOperator::And,
        }],
        sort_columns: vec!["col1".to_string(), "col2".to_string()],
        sort_descending: Vec::new(),
        sort_ascending: false,
        column_order: vec!["col1".to_string(), "col2".to_string(), "col3".to_string()],
        locked_columns_count: 1,
        pivot: None,
        melt: None,
        reshape_source: None,
    };

    let template = manager.create_template(
        "test_template".to_string(),
        Some("Test description".to_string()),
        match_criteria,
        settings,
    )?;

    assert_eq!(template.name, "test_template");
    assert_eq!(template.description, Some("Test description".to_string()));
    assert_eq!(template.usage_count, 0);
    assert!(
        template
            .created
            .duration_since(SystemTime::UNIX_EPOCH)
            .is_ok()
    );

    manager.load_templates()?;
    assert!(manager.template_exists("test_template"));

    // Cleanup
    let _ = std::fs::remove_dir_all(&temp_dir);

    Ok(())
}

#[test]
fn test_template_serialization() -> Result<()> {
    let temp_dir = create_test_temp_dir()?;
    let config = ConfigManager::with_dir(temp_dir.clone());

    let mut manager = TemplateManager::new(&config)?;

    let match_criteria = MatchCriteria {
        exact_path: Some(PathBuf::from("/test/file.csv")),
        relative_path: None,
        path_pattern: None,
        filename_pattern: None,
        schema_columns: None,
        schema_types: None,
        table: None,
    };

    let settings = TemplateSettings {
        query: Some("select a".to_string()),
        sql_query: None,
        fuzzy_query: None,
        filters: Vec::new(),
        sort_columns: Vec::new(),
        sort_descending: Vec::new(),
        sort_ascending: true,
        column_order: vec!["a".to_string(), "b".to_string()],
        locked_columns_count: 0,
        pivot: None,
        melt: None,
        reshape_source: None,
    };

    let template = manager.create_template(
        "serialization_test".to_string(),
        None,
        match_criteria,
        settings,
    )?;

    manager.save_template(&template)?;
    manager.load_templates()?;

    let loaded = manager.get_template_by_name("serialization_test");
    assert!(loaded.is_some());
    let loaded = loaded.unwrap();
    assert_eq!(loaded.name, "serialization_test");
    assert_eq!(loaded.settings.query, Some("select a".to_string()));

    // Cleanup
    let _ = std::fs::remove_dir_all(&temp_dir);

    Ok(())
}

/// The suggested name comes from the state — the file stem, or the query's
/// first words — and numbers itself past a collision, never template0001.
#[test]
fn test_suggest_name_derives_from_state() -> Result<()> {
    let temp_dir = create_test_temp_dir()?;
    let config = ConfigManager::with_dir(temp_dir.clone());

    let mut manager = TemplateManager::new(&config)?;
    let path = PathBuf::from("/data/sales_2024.csv");

    assert_eq!(manager.suggest_name(Some(&path), None), "sales_2024");
    assert_eq!(
        manager.suggest_name(
            None,
            Some("select region, revenue from df where year == 2024")
        ),
        "select region, revenue from"
    );
    assert_eq!(manager.suggest_name(None, None), "view");

    // A taken name gets a number.
    let settings = TemplateSettings {
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
    };
    let criteria = MatchCriteria {
        exact_path: None,
        relative_path: None,
        path_pattern: None,
        filename_pattern: None,
        schema_columns: None,
        schema_types: None,
        table: None,
    };
    manager.create_template("sales_2024".to_string(), None, criteria, settings)?;
    assert_eq!(manager.suggest_name(Some(&path), None), "sales_2024 2");

    let _ = std::fs::remove_dir_all(&temp_dir);

    Ok(())
}

#[test]
fn test_template_relevance_exact_path() -> Result<()> {
    use polars::prelude::Schema;

    let temp_dir = create_test_temp_dir()?;
    let config = ConfigManager::with_dir(temp_dir.clone());

    let manager = TemplateManager::new(&config)?;

    let test_path = PathBuf::from("/test/exact.csv");
    let match_criteria = MatchCriteria {
        exact_path: Some(test_path.clone()),
        relative_path: None,
        path_pattern: None,
        filename_pattern: None,
        schema_columns: None,
        schema_types: None,
        table: None,
    };

    let settings = TemplateSettings {
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
    };

    let mut manager = manager;
    let _template = manager.create_template(
        "exact_path_test".to_string(),
        None,
        match_criteria,
        settings,
    )?;

    use polars::prelude::Field;
    let schema = Schema::from_iter([] as [Field; 0]);

    let relevant = manager.find_relevant_templates(&test_path, &schema);
    assert!(!relevant.is_empty());
    assert!(relevant[0].1 >= 1000.0);

    // Cleanup
    let _ = std::fs::remove_dir_all(&temp_dir);

    Ok(())
}

/// Template with sql_query and fuzzy_query round-trips correctly.
#[test]
fn test_template_serialization_with_sql_and_fuzzy() -> Result<()> {
    let temp_dir = create_test_temp_dir()?;
    let config = ConfigManager::with_dir(temp_dir.clone());

    let mut manager = TemplateManager::new(&config)?;
    let match_criteria = MatchCriteria {
        exact_path: Some(PathBuf::from("/test/file.csv")),
        relative_path: None,
        path_pattern: None,
        filename_pattern: None,
        schema_columns: None,
        schema_types: None,
        table: None,
    };

    let settings = TemplateSettings {
        query: None,
        sql_query: Some("SELECT * FROM df WHERE x > 0".to_string()),
        fuzzy_query: Some("foo bar".to_string()),
        filters: Vec::new(),
        sort_columns: Vec::new(),
        sort_descending: Vec::new(),
        sort_ascending: true,
        column_order: vec!["a".to_string(), "b".to_string()],
        locked_columns_count: 0,
        pivot: None,
        melt: None,
        reshape_source: None,
    };

    let template =
        manager.create_template("sql_fuzzy_test".to_string(), None, match_criteria, settings)?;

    manager.save_template(&template)?;
    manager.load_templates()?;

    let loaded = manager.get_template_by_name("sql_fuzzy_test").unwrap();
    assert_eq!(
        loaded.settings.sql_query,
        Some("SELECT * FROM df WHERE x > 0".to_string())
    );
    assert_eq!(loaded.settings.fuzzy_query, Some("foo bar".to_string()));

    let _ = std::fs::remove_dir_all(&temp_dir);
    Ok(())
}

// ---------------------------------------------------------------------------
// Several instances sharing one views directory.
// ---------------------------------------------------------------------------

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

fn plain_settings() -> TemplateSettings {
    TemplateSettings {
        query: None,
        sql_query: None,
        fuzzy_query: None,
        filters: Vec::new(),
        sort_columns: Vec::new(),
        sort_descending: Vec::new(),
        sort_ascending: false,
        column_order: Vec::new(),
        locked_columns_count: 0,
        pivot: None,
        melt: None,
        reshape_source: None,
    }
}

/// Two instances that have both read one view, and the view's id.
fn two_instances(dir: &std::path::Path) -> (TemplateManager, TemplateManager, String) {
    let config = ConfigManager::with_dir(dir.to_path_buf());
    let mut a = TemplateManager::new(&config).unwrap();
    let id = a
        .create_template("shared".into(), None, no_criteria(), plain_settings())
        .unwrap()
        .id;
    let b = TemplateManager::new(&config).unwrap();
    (a, b, id)
}

fn stored(dir: &std::path::Path) -> Vec<datui::template::Template> {
    let fresh = TemplateManager::new(&ConfigManager::with_dir(dir.to_path_buf())).unwrap();
    assert!(
        fresh.broken_templates.is_empty(),
        "a broken view was listed"
    );
    fresh.all_templates().to_vec()
}

#[derive(Clone, Copy, Debug)]
enum Op {
    /// Instance A renames the view.
    EditA,
    /// Instance B changes its description.
    EditB,
    ApplyA,
    ApplyB,
    DeleteB,
}

fn run(op: Op, a: &mut TemplateManager, b: &mut TemplateManager, id: &str) {
    let file = std::path::Path::new("/data/x.csv");
    let edit = |m: &mut TemplateManager, change: &dyn Fn(&mut datui::template::Template)| {
        if let Some(mut t) = m.get_template_by_id(id).cloned() {
            change(&mut t);
            // Fails only when the view was deleted, which the caller checks.
            let _ = m.update_template(&t);
        }
    };
    match op {
        Op::EditA => edit(a, &|t| t.name = "renamed".into()),
        Op::EditB => edit(b, &|t| t.description = Some("described".into())),
        Op::ApplyA => a.record_use(id, file).unwrap(),
        Op::ApplyB => b.record_use(id, file).unwrap(),
        Op::DeleteB => b.delete_template(id).unwrap(),
    }
}

fn orders(ops: &[Op]) -> Vec<Vec<Op>> {
    if ops.len() <= 1 {
        return vec![ops.to_vec()];
    }
    let mut all = Vec::new();
    for i in 0..ops.len() {
        let mut rest = ops.to_vec();
        let first = rest.remove(i);
        for mut tail in orders(&rest) {
            tail.insert(0, first);
            all.push(tail);
        }
    }
    all
}

/// Edits and applies in two instances, in every order: each instance's edit and every
/// use survive, whatever the other wrote since it read the view.
#[test]
fn edits_and_applies_in_two_instances_lose_nothing() {
    for order in orders(&[Op::EditA, Op::EditB, Op::ApplyA, Op::ApplyB]) {
        let dir = tempfile::tempdir().unwrap();
        let (mut a, mut b, id) = two_instances(dir.path());
        for &op in &order {
            run(op, &mut a, &mut b, &id);
        }
        let views = stored(dir.path());
        assert_eq!(views.len(), 1, "{order:?}");
        let view = &views[0];
        assert_eq!(view.name, "renamed", "{order:?}");
        assert_eq!(view.description.as_deref(), Some("described"), "{order:?}");
        assert_eq!(view.usage_count, 2, "{order:?}");
    }
}

/// A view deleted in one instance stays deleted, whatever the other does with its copy
/// afterwards, in every order.
#[test]
fn a_view_deleted_in_one_instance_stays_deleted() {
    for order in orders(&[Op::EditA, Op::ApplyA, Op::ApplyB, Op::DeleteB]) {
        let dir = tempfile::tempdir().unwrap();
        let (mut a, mut b, id) = two_instances(dir.path());
        let mut deleted = false;
        for &op in &order {
            run(op, &mut a, &mut b, &id);
            deleted |= matches!(op, Op::DeleteB);
            if deleted {
                assert!(stored(dir.path()).is_empty(), "{order:?} after {op:?}");
            }
        }
    }
}

/// An edit of a view deleted elsewhere says so, and the view leaves the editor too.
#[test]
fn editing_a_view_deleted_elsewhere_is_an_error() {
    let dir = tempfile::tempdir().unwrap();
    let (mut a, mut b, id) = two_instances(dir.path());
    b.delete_template(&id).unwrap();
    let mut edited = a.get_template_by_id(&id).cloned().unwrap();
    edited.name = "renamed".into();
    assert!(a.update_template(&edited).is_err());
    assert!(a.get_template_by_id(&id).is_none());
    assert!(stored(dir.path()).is_empty());
}

/// Readers never see a half-written view while two instances keep writing it.
#[test]
fn a_reader_never_sees_a_broken_view() {
    let dir = tempfile::tempdir().unwrap();
    let (a, b, id) = two_instances(dir.path());
    let done = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writers: Vec<_> = [a, b]
        .into_iter()
        .enumerate()
        .map(|(n, mut m)| {
            let id = id.clone();
            std::thread::spawn(move || {
                for i in 0..100 {
                    let mut t = m.get_template_by_id(&id).cloned().unwrap();
                    // A long value, so a torn write would be seen.
                    t.description = Some(format!("{n}-{i}-{}", "x".repeat(4096)));
                    m.update_template(&t).unwrap();
                    m.record_use(&id, std::path::Path::new("/data/x.csv"))
                        .unwrap();
                }
            })
        })
        .collect();
    let reader = {
        let dir = dir.path().to_path_buf();
        let done = done.clone();
        std::thread::spawn(move || {
            let mut reads = 0;
            while !done.load(std::sync::atomic::Ordering::Relaxed) || reads == 0 {
                assert_eq!(stored(&dir).len(), 1);
                reads += 1;
            }
        })
    };
    for writer in writers {
        writer.join().unwrap();
    }
    done.store(true, std::sync::atomic::Ordering::Relaxed);
    reader.join().unwrap();
    assert_eq!(stored(dir.path())[0].usage_count, 200);
}

/// A write killed before its rename leaves its temp file and the view as it was;
/// neither is listed as broken.
#[test]
fn a_write_killed_before_its_rename_breaks_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let (_a, _b, id) = two_instances(dir.path());
    let templates = dir.path().join("templates");
    std::fs::write(
        templates.join(format!("template_{id}.json.999.0.tmp")),
        "{\"id\": \"half",
    )
    .unwrap();
    let views = stored(dir.path());
    assert_eq!(views.len(), 1);
    assert_eq!(views[0].name, "shared");
}

use color_eyre::Result;
use datui::config::ConfigManager;
use datui::filter_modal::{FilterOperator, FilterStatement, LogicalOperator};
use datui::template::{MatchCriteria, TemplateManager, TemplateSettings};
use std::path::PathBuf;
use std::time::SystemTime;

#[test]
fn test_template_creation() -> Result<()> {
    // Create a temporary config manager for testing
    let temp_dir = std::env::temp_dir().join(format!("datui_test_{}", std::process::id()));
    std::fs::create_dir_all(&temp_dir)?;
    let config = ConfigManager::with_dir(temp_dir.clone());

    let mut manager = TemplateManager::new(&config)?;

    // Create a test template
    let match_criteria = MatchCriteria {
        exact_path: Some(PathBuf::from("/test/path.csv")),
        relative_path: None,
        path_pattern: None,
        filename_pattern: None,
        schema_columns: Some(vec!["col1".to_string(), "col2".to_string()]),
        schema_types: None,
    };

    let settings = TemplateSettings {
        query: Some("select a, b".to_string()),
        filters: vec![FilterStatement {
            column: "col1".to_string(),
            operator: FilterOperator::Gt,
            value: "10".to_string(),
            logical_op: LogicalOperator::And,
        }],
        sort_columns: vec!["col1".to_string(), "col2".to_string()],
        sort_ascending: false,
        column_order: vec!["col1".to_string(), "col2".to_string(), "col3".to_string()],
        locked_columns_count: 1,
    };

    let template = manager.create_template(
        "test_template".to_string(),
        Some("Test description".to_string()),
        match_criteria,
        settings,
    )?;

    // Verify template was created
    assert_eq!(template.name, "test_template");
    assert_eq!(template.description, Some("Test description".to_string()));
    assert_eq!(template.usage_count, 0);
    assert!(template
        .created
        .duration_since(SystemTime::UNIX_EPOCH)
        .is_ok());

    // Verify template can be loaded
    manager.load_templates()?;
    assert!(manager.template_exists("test_template"));

    // Cleanup
    let _ = std::fs::remove_dir_all(&temp_dir);

    Ok(())
}

#[test]
fn test_template_serialization() -> Result<()> {
    let temp_dir = std::env::temp_dir().join(format!("datui_test_{}", std::process::id()));
    std::fs::create_dir_all(&temp_dir)?;
    let config = ConfigManager::with_dir(temp_dir.clone());

    let mut manager = TemplateManager::new(&config)?;

    // Create a template
    let match_criteria = MatchCriteria {
        exact_path: Some(PathBuf::from("/test/file.csv")),
        relative_path: None,
        path_pattern: None,
        filename_pattern: None,
        schema_columns: None,
        schema_types: None,
    };

    let settings = TemplateSettings {
        query: Some("select a".to_string()),
        filters: Vec::new(),
        sort_columns: Vec::new(),
        sort_ascending: true,
        column_order: vec!["a".to_string(), "b".to_string()],
        locked_columns_count: 0,
    };

    let template = manager.create_template(
        "serialization_test".to_string(),
        None,
        match_criteria,
        settings,
    )?;

    // Save and reload
    manager.save_template(&template)?;
    manager.load_templates()?;

    // Verify template was loaded
    let loaded = manager.get_template_by_name("serialization_test");
    assert!(loaded.is_some());
    let loaded = loaded.unwrap();
    assert_eq!(loaded.name, "serialization_test");
    assert_eq!(loaded.settings.query, Some("select a".to_string()));

    // Cleanup
    let _ = std::fs::remove_dir_all(&temp_dir);

    Ok(())
}

#[test]
fn test_generate_next_template_name() -> Result<()> {
    let temp_dir = std::env::temp_dir().join(format!("datui_test_{}", std::process::id()));
    std::fs::create_dir_all(&temp_dir)?;
    let config = ConfigManager::with_dir(temp_dir.clone());

    let manager = TemplateManager::new(&config)?;

    // Should start with template0001
    let name1 = manager.generate_next_template_name();
    assert!(name1.starts_with("template"));

    // Cleanup
    let _ = std::fs::remove_dir_all(&temp_dir);

    Ok(())
}

#[test]
fn test_template_relevance_exact_path() -> Result<()> {
    use polars::prelude::Schema;

    let temp_dir = std::env::temp_dir().join(format!("datui_test_{}", std::process::id()));
    std::fs::create_dir_all(&temp_dir)?;
    let config = ConfigManager::with_dir(temp_dir.clone());

    let manager = TemplateManager::new(&config)?;

    // Create a template with exact path
    let test_path = PathBuf::from("/test/exact.csv");
    let match_criteria = MatchCriteria {
        exact_path: Some(test_path.clone()),
        relative_path: None,
        path_pattern: None,
        filename_pattern: None,
        schema_columns: None,
        schema_types: None,
    };

    let settings = TemplateSettings {
        query: None,
        filters: Vec::new(),
        sort_columns: Vec::new(),
        sort_ascending: true,
        column_order: Vec::new(),
        locked_columns_count: 0,
    };

    let mut manager = manager;
    let _template = manager.create_template(
        "exact_path_test".to_string(),
        None,
        match_criteria,
        settings,
    )?;

    // Create a test schema (empty schema)
    use polars::prelude::Field;
    let schema = Schema::from_iter([] as [Field; 0]);

    // Find relevant templates
    let relevant = manager.find_relevant_templates(&test_path, &schema);
    assert!(!relevant.is_empty());

    // Exact path match should have highest score (1000.0)
    assert!(relevant[0].1 >= 1000.0);

    // Cleanup
    let _ = std::fs::remove_dir_all(&temp_dir);

    Ok(())
}

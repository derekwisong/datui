//! `datui cache` and `datui views`: maintenance that opens no data.

use datui_cli::{CacheAction, ViewsAction};

use crate::cache::CacheManager;
use crate::config::ConfigManager;
use crate::template::{MatchCriteria, TemplateManager};

/// What `datui cache ACTION` prints, and its exit code.
pub fn cache(cache: Option<&CacheManager>, action: &CacheAction) -> (String, i32) {
    let CacheAction::Clear { recents } = action;
    let Some(cache) = cache else {
        return ("No cache to clear\n".into(), 0);
    };
    if *recents {
        cache.clear_recents();
        return ("Recently opened datasets forgotten\n".into(), 0);
    }
    match cache.clear_all() {
        Ok(()) => (format!("Cleared {}\n", cache.cache_dir().display()), 0),
        Err(e) => (format!("{e}\n"), 1),
    }
}

/// What `datui views ACTION` prints, and its exit code.
pub fn views(config: &ConfigManager, action: &ViewsAction) -> (String, i32) {
    let mut views = match TemplateManager::new(config) {
        Ok(views) => views,
        Err(e) => return (format!("{e}\n"), 1),
    };
    match action {
        ViewsAction::List => {
            let all = views.all_templates();
            if all.is_empty() {
                return ("No saved views\n".into(), 0);
            }
            let width = all
                .iter()
                .map(|v| v.name.chars().count())
                .max()
                .unwrap_or(0);
            let mut out = String::new();
            for view in all {
                out.push_str(&format!(
                    "{:<width$}  {}\n",
                    view.name,
                    matches(&view.match_criteria)
                ));
            }
            (out, 0)
        }
        ViewsAction::Rm { name } => {
            let Some(id) = views.get_template_by_name(name).map(|v| v.id.clone()) else {
                return (
                    format!("No saved view named \"{name}\"; `datui views list` lists them\n"),
                    1,
                );
            };
            match views.delete_template(&id) {
                Ok(()) => (format!("Removed {name}\n"), 0),
                Err(e) => (format!("{e}\n"), 1),
            }
        }
        ViewsAction::Clear => {
            let count = views.all_templates().len();
            match views.remove_all_templates() {
                Ok(()) => (format!("Removed {count} saved views\n"), 0),
                Err(e) => (format!("{e}\n"), 1),
            }
        }
    }
}

/// What files a view is matched to, in a few words.
fn matches(criteria: &MatchCriteria) -> String {
    let mut parts: Vec<String> = Vec::new();
    if let Some(path) = &criteria.exact_path {
        parts.push(path.display().to_string());
    } else if let Some(path) = &criteria.relative_path {
        parts.push(path.clone());
    }
    if let Some(pattern) = criteria
        .path_pattern
        .as_ref()
        .or(criteria.filename_pattern.as_ref())
    {
        parts.push(pattern.clone());
    }
    if let Some(columns) = &criteria.schema_columns {
        parts.push(format!("{} columns", columns.len()));
    }
    if parts.is_empty() {
        "any file".into()
    } else {
        parts.join(", ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn views_are_listed_removed_by_name_and_cleared() {
        let dir = tempfile::tempdir().unwrap();
        let config = ConfigManager::with_dir(dir.path().to_path_buf());
        let mut manager = TemplateManager::new(&config).unwrap();
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
            let settings = crate::template::TemplateSettings {
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
            manager
                .create_template(name.to_string(), None, criteria, settings)
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
}

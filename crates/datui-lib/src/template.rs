use color_eyre::Result;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashSet;
use std::fs;
use std::hash::{Hash, Hasher};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use polars::prelude::Schema;

use crate::config::ConfigManager;
use crate::filter_modal::FilterStatement;
use crate::pivot_melt_modal::{MeltSpec, PivotSpec};

// Custom serialization for SystemTime (convert to/from seconds since epoch)
mod time_serde {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::time::{SystemTime, UNIX_EPOCH};

    pub fn serialize<S>(time: &SystemTime, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let duration = time.duration_since(UNIX_EPOCH).map_err(|e| {
            serde::ser::Error::custom(format!("Failed to serialize SystemTime: {}", e))
        })?;
        duration.as_secs().serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<SystemTime, D::Error>
    where
        D: Deserializer<'de>,
    {
        let secs = u64::deserialize(deserializer)?;
        Ok(UNIX_EPOCH + std::time::Duration::from_secs(secs))
    }

    pub mod option {
        use super::*;

        pub fn serialize<S>(time: &Option<SystemTime>, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            match time {
                Some(time) => super::serialize(time, serializer),
                None => serializer.serialize_none(),
            }
        }

        pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<SystemTime>, D::Error>
        where
            D: Deserializer<'de>,
        {
            Option::<u64>::deserialize(deserializer)?
                .map(|secs| Ok(UNIX_EPOCH + std::time::Duration::from_secs(secs)))
                .transpose()
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Template {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
    #[serde(with = "time_serde")]
    pub created: SystemTime,
    #[serde(with = "time_serde::option")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub last_used: Option<SystemTime>,
    pub usage_count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_matched_file: Option<PathBuf>,
    pub match_criteria: MatchCriteria,
    pub settings: TemplateSettings,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatchCriteria {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exact_path: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub relative_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path_pattern: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filename_pattern: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_columns: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_types: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TemplateSettings {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub sql_query: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub fuzzy_query: Option<String>,
    pub filters: Vec<FilterStatement>,
    pub sort_columns: Vec<String>,
    pub sort_ascending: bool,
    pub column_order: Vec<String>,
    pub locked_columns_count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub pivot: Option<PivotSpec>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub melt: Option<MeltSpec>,
}

pub struct TemplateManager {
    config: ConfigManager,
    templates: Vec<Template>,
    pub(crate) templates_dir: PathBuf,
}

impl TemplateManager {
    pub fn new(config: &ConfigManager) -> Result<Self> {
        // Don't create directories on startup - be sensitive to constrained environments
        // Directories will be created lazily when actually needed (e.g., saving templates)
        let templates_dir = config.config_dir().join("templates");

        let mut manager = Self {
            config: config.clone(),
            templates: Vec::new(),
            templates_dir,
        };

        // Only try to load templates if the directory exists
        // Don't create it if it doesn't exist
        manager.load_templates()?;
        Ok(manager)
    }

    pub fn load_templates(&mut self) -> Result<()> {
        self.templates.clear();

        // Load all template files
        if !self.templates_dir.exists() {
            return Ok(());
        }

        let entries = fs::read_dir(&self.templates_dir)?;
        for entry in entries {
            let entry = entry?;
            let path = entry.path();

            if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("json") {
                if let Ok(content) = fs::read_to_string(&path) {
                    match serde_json::from_str::<Template>(&content) {
                        Ok(template) => {
                            self.templates.push(template);
                        }
                        Err(e) => {
                            eprintln!("Warning: Could not parse template file {:?}: {}", path, e);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    pub fn save_template(&self, template: &Template) -> Result<()> {
        // Ensure config directory exists first
        self.config.ensure_config_dir()?;

        // Always ensure templates directory exists before writing
        // Don't rely on existence checks - always create if needed
        // This handles cases where the directory might have been deleted
        // or where tests run in environments with different file system behavior
        fs::create_dir_all(&self.templates_dir)?;

        let filename = format!("template_{}.json", template.id);
        let file_path = self.templates_dir.join(filename);

        // Ensure the parent directory exists right before opening the file
        // Double-check for robustness, especially in CI/test environments
        if let Some(parent) = file_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let json = serde_json::to_string_pretty(template)?;

        // Use file locking to prevent race conditions
        use fs2::FileExt;
        let mut file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&file_path)?;

        file.lock_exclusive()?;
        file.write_all(json.as_bytes())?;
        file.flush()?;
        file.unlock()?;

        Ok(())
    }

    pub fn delete_template(&mut self, id: &str) -> Result<()> {
        let filename = format!("template_{}.json", id);
        let file_path = self.templates_dir.join(filename);

        if file_path.exists() {
            fs::remove_file(&file_path)?;
        }

        self.templates.retain(|t| t.id != id);
        Ok(())
    }

    pub fn find_relevant_templates(
        &self,
        file_path: &Path,
        schema: &Schema,
    ) -> Vec<(Template, f64)> {
        let mut results: Vec<(Template, f64)> = self
            .templates
            .iter()
            .map(|template| {
                let score = calculate_relevance(template, file_path, schema);
                (template.clone(), score)
            })
            .collect();

        // Sort by relevance score (highest first)
        results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        results
    }

    pub fn get_most_relevant(&self, file_path: &Path, schema: &Schema) -> Option<Template> {
        self.find_relevant_templates(file_path, schema)
            .into_iter()
            .next()
            .map(|(template, _)| template)
    }

    pub fn generate_next_template_name(&self) -> String {
        let mut max_num = 0;

        for template in &self.templates {
            if template.name.starts_with("template") {
                if let Some(num_str) = template.name.strip_prefix("template") {
                    if let Ok(num) = num_str.parse::<u32>() {
                        max_num = max_num.max(num);
                    }
                }
            }
        }

        format!("template{:04}", max_num + 1)
    }

    pub fn template_exists(&self, name: &str) -> bool {
        self.templates.iter().any(|t| t.name == name)
    }

    pub fn get_template_by_name(&self, name: &str) -> Option<&Template> {
        self.templates.iter().find(|t| t.name == name)
    }

    pub fn get_template_by_id(&self, id: &str) -> Option<&Template> {
        self.templates.iter().find(|t| t.id == id)
    }

    pub fn all_templates(&self) -> &[Template] {
        &self.templates
    }

    pub fn create_template(
        &mut self,
        name: String,
        description: Option<String>,
        match_criteria: MatchCriteria,
        settings: TemplateSettings,
    ) -> Result<Template> {
        // Generate unique ID based on name and timestamp
        let mut hasher = DefaultHasher::new();
        name.hash(&mut hasher);
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
            .hash(&mut hasher);
        let id = format!("{:016x}", hasher.finish());

        let template = Template {
            id,
            name,
            description,
            created: SystemTime::now(),
            last_used: None,
            usage_count: 0,
            last_matched_file: None,
            match_criteria,
            settings,
        };

        // Save the template
        self.save_template(&template)?;

        // Reload templates to include the new one
        self.load_templates()?;

        Ok(template)
    }

    pub fn update_template(&mut self, template: &Template) -> Result<()> {
        // Save the updated template
        self.save_template(template)?;

        // Update in-memory list
        if let Some(existing) = self.templates.iter_mut().find(|t| t.id == template.id) {
            *existing = template.clone();
        } else {
            // If not found, add it (shouldn't happen, but handle gracefully)
            self.templates.push(template.clone());
        }

        Ok(())
    }

    pub fn remove_all_templates(&mut self) -> Result<()> {
        // Delete all template files
        if self.templates_dir.exists() {
            for entry in fs::read_dir(&self.templates_dir)? {
                let entry = entry?;
                let path = entry.path();
                if path.is_file()
                    && path
                        .file_name()
                        .and_then(|n| n.to_str())
                        .map(|s| s.starts_with("template_") && s.ends_with(".json"))
                        .unwrap_or(false)
                {
                    fs::remove_file(&path)?;
                }
            }
        }

        // Clear in-memory list
        self.templates.clear();

        Ok(())
    }
}

fn calculate_relevance(template: &Template, file_path: &Path, schema: &Schema) -> f64 {
    let mut score = 0.0;

    // Check exact path (absolute) match
    let exact_path_match = template
        .match_criteria
        .exact_path
        .as_ref()
        .map(|exact| exact == file_path)
        .unwrap_or(false);

    // Check relative path match
    let relative_path_match = if let Some(relative_path) = &template.match_criteria.relative_path {
        // Calculate relative path from current working directory
        if let Ok(cwd) = std::env::current_dir() {
            if let Ok(rel_path) = file_path.strip_prefix(&cwd) {
                rel_path.to_string_lossy() == *relative_path
            } else {
                false
            }
        } else {
            false
        }
    } else {
        false
    };

    // Check for exact schema match
    let exact_schema_match = if let Some(required_cols) = &template.match_criteria.schema_columns {
        let file_cols: HashSet<&str> = schema.iter_names().map(|s| s.as_str()).collect();
        let required_cols_set: HashSet<&str> = required_cols.iter().map(|s| s.as_str()).collect();

        // All required columns present AND no extra columns (exact match)
        required_cols_set.is_subset(&file_cols) && file_cols.len() == required_cols_set.len()
    } else {
        false
    };

    // Exact path (absolute) + exact schema: highest priority (2000 points)
    if exact_path_match && exact_schema_match {
        return 2000.0;
    }

    // Exact path (absolute) only: very high priority (1000 points)
    if exact_path_match {
        return 1000.0;
    }

    // Relative path + exact schema: very high priority (1950 points)
    if relative_path_match && exact_schema_match {
        return 1950.0;
    }

    // Relative path only: very high priority (950 points)
    if relative_path_match {
        return 950.0;
    }

    // Exact schema only (without path matches): very high priority (900 points)
    if exact_schema_match {
        return 900.0;
    }

    // For non-exact matches, sum components
    // Path pattern match
    if let Some(pattern) = &template.match_criteria.path_pattern {
        if matches_pattern(file_path.to_str().unwrap_or(""), pattern) {
            score += 50.0;
            score += pattern_specificity_bonus(pattern);
        }
    }

    // Filename pattern match
    if let Some(pattern) = &template.match_criteria.filename_pattern {
        if let Some(filename) = file_path.file_name() {
            if let Some(filename_str) = filename.to_str() {
                if matches_pattern(filename_str, pattern) {
                    score += 30.0;
                    score += pattern_specificity_bonus(pattern);
                }
            }
        }
    }

    // Partial schema matching (only if not exact match)
    if let Some(required_cols) = &template.match_criteria.schema_columns {
        let file_cols: HashSet<&str> = schema.iter_names().map(|s| s.as_str()).collect();
        let matching_count = required_cols
            .iter()
            .filter(|col| file_cols.contains(col.as_str()))
            .count();
        score += (matching_count as f64) * 2.0; // 2 points per matching column

        // Optional: type matching bonus (if types are specified)
        // This would require comparing schema types, which is more complex
    }

    // Usage statistics
    score += (template.usage_count.min(10) as f64) * 1.0;
    if let Some(last_used) = template.last_used {
        if let Ok(duration) = SystemTime::now().duration_since(last_used) {
            let days_since = duration.as_secs() / 86400;
            if days_since <= 7 {
                score += 5.0;
            } else if days_since <= 30 {
                score += 2.0;
            }
        }
    }

    // Age penalty
    if let Ok(duration) = SystemTime::now().duration_since(template.created) {
        let months_old = (duration.as_secs() / (30 * 86400)) as f64;
        score -= months_old * 1.0;
    }

    score
}

fn pattern_specificity_bonus(pattern: &str) -> f64 {
    // More specific patterns (fewer wildcards) get higher bonuses
    let wildcard_count = pattern.matches('*').count() + pattern.matches('?').count();
    match wildcard_count {
        0 => 10.0, // No wildcards (most specific)
        1 => 5.0,  // One wildcard
        2 => 3.0,  // Two wildcards
        3 => 1.0,  // Three wildcards
        _ => 0.0,  // Many wildcards (less specific)
    }
}

fn matches_pattern(text: &str, pattern: &str) -> bool {
    // Simple glob-like pattern matching
    // Convert pattern to regex-like matching
    // Support: * (matches any sequence), ? (matches single char)

    // Simple implementation: convert * to .* and ? to . for regex
    let mut regex_pattern = String::new();
    for ch in pattern.chars() {
        match ch {
            '*' => regex_pattern.push_str(".*"),
            '?' => regex_pattern.push('.'),
            '.' | '(' | ')' | '[' | ']' | '{' | '}' | '\\' | '^' | '$' | '+' => {
                regex_pattern.push('\\');
                regex_pattern.push(ch);
            }
            _ => regex_pattern.push(ch),
        }
    }

    // Use simple string matching for now (full regex would require regex crate)
    // For simple cases: * matches anything, exact match otherwise
    if pattern == "*" {
        return true;
    }

    // Simple wildcard matching
    let pattern_parts: Vec<&str> = pattern.split('*').collect();

    if pattern_parts.len() == 1 {
        // No wildcards, exact match
        return text == pattern;
    }

    // Has wildcards - check if text matches pattern parts
    let mut text_pos = 0;
    for (i, part) in pattern_parts.iter().enumerate() {
        if part.is_empty() {
            continue;
        }

        if i == 0 {
            // First part must match start
            if !text.starts_with(part) {
                return false;
            }
            text_pos = part.len();
        } else if i == pattern_parts.len() - 1 {
            // Last part must match end
            return text[text_pos..].ends_with(part);
        } else {
            // Middle parts must appear in order
            if let Some(pos) = text[text_pos..].find(part) {
                text_pos += pos + part.len();
            } else {
                return false;
            }
        }
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Old template JSON without sql_query or fuzzy_query deserializes; those fields default to None.
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
        let settings: TemplateSettings = serde_json::from_str(json).unwrap();
        assert_eq!(settings.query, Some("select a".to_string()));
        assert_eq!(settings.sql_query, None);
        assert_eq!(settings.fuzzy_query, None);
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

    #[test]
    fn test_pattern_specificity_bonus() {
        assert_eq!(pattern_specificity_bonus("test.csv"), 10.0);
        assert_eq!(pattern_specificity_bonus("*.csv"), 5.0);
        assert_eq!(pattern_specificity_bonus("sales_*.csv"), 5.0);
    }
}

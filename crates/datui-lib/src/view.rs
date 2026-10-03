use color_eyre::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::collections::hash_map::DefaultHasher;
use std::fs;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use polars::prelude::Schema;

use crate::config::ConfigManager;
use crate::filter_modal::FilterStatement;
use crate::pivot_melt_modal::{MeltSpec, PivotSpec, ReshapeSource};

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
pub struct SavedView {
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
    pub settings: ViewSettings,
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
    /// The table of a file of tables the view was saved on (`orders` of `shop.db`,
    /// whether opened as `shop.db/orders` or with `--table orders`). Its path criteria
    /// fit only that table: a URL or standard input names the file alone.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table: Option<String>,
}

impl MatchCriteria {
    /// Repair the paths of a view saved before URLs were told apart from local paths:
    /// its exact path was the working directory joined to the URL, and its relative
    /// path the URL itself. Both become the URL, as the exact path.
    fn unmangle_urls(&mut self) {
        if let Some(url) = self.exact_path.as_deref().and_then(unmangled_url) {
            self.exact_path = Some(url);
        }
        if let Some(relative) = self.relative_path.take() {
            if crate::source::is_remote_url(Path::new(&relative)) {
                self.exact_path
                    .get_or_insert_with(|| PathBuf::from(&relative));
            } else {
                self.relative_path = Some(relative);
            }
        }
    }
}

/// The URL inside `<working directory>/s3://bucket/key`, or None when `path` is not
/// a URL behind a local prefix.
fn unmangled_url(path: &Path) -> Option<PathBuf> {
    let text = path.to_string_lossy();
    let scheme_end = text.find("://")?;
    let start = text[..scheme_end].rfind(['/', '\\'])? + 1;
    let url = PathBuf::from(&text[start..]);
    crate::source::is_remote_url(&url).then_some(url)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewSettings {
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
    /// Per entry of `sort_columns`, whether it runs descending. Empty in templates
    /// saved before per-column directions existed; `sort_ascending` then covers all.
    #[serde(default)]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub sort_descending: Vec<bool>,
    pub sort_ascending: bool,
    pub column_order: Vec<String>,
    pub locked_columns_count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub pivot: Option<PivotSpec>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub melt: Option<MeltSpec>,
    /// The query, filters and sort the pivot or melt ran over, replayed before it. With
    /// a reshape, `query`, `filters` and the sort are what ran on its result. Views
    /// saved before this existed have none: their reshape ran over the data as loaded.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub reshape_source: Option<ReshapeSource>,
}

impl ViewSettings {
    /// The per-column directions this template's sort runs. A template saved before
    /// per-column directions existed has none; `sort_ascending` then covers all.
    pub fn sort_directions(&self) -> Vec<bool> {
        if self.sort_descending.len() == self.sort_columns.len() {
            self.sort_descending.clone()
        } else {
            vec![!self.sort_ascending; self.sort_columns.len()]
        }
    }
}

#[derive(Debug, Clone)]
pub struct BrokenView {
    pub filename: String,
    pub error: String,
}

pub struct ViewManager {
    config: ConfigManager,
    views: Vec<SavedView>,
    pub(crate) views_dir: PathBuf,
    pub broken_views: Vec<BrokenView>,
}

/// The saved views, read on a worker from the moment `run` starts.
///
/// Reading them enumerates and parses a directory, which is a stall on a slow mount
/// and tens of milliseconds for a few thousand views; neither belongs in front of the
/// first frame. Nothing needs them until a dataset's schema is known (`--view` and
/// auto-apply meet it there) or the views list opens, so the first use waits for the
/// read if it is still going — a view the user asked for is never skipped by rows
/// shown without it. Derefs to the [`TemplateManager`].
pub struct Views {
    ready: std::cell::OnceCell<ViewManager>,
    pending: std::cell::RefCell<Option<std::sync::mpsc::Receiver<ViewManager>>>,
}

impl Views {
    /// Start reading the views on a worker.
    pub fn read_in_background() -> Self {
        let (tx, rx) = std::sync::mpsc::channel();
        let pending = std::thread::Builder::new()
            .name("datui-views".into())
            .spawn(move || {
                let _ = tx.send(ViewManager::load_or_empty());
            })
            .map(|_| rx)
            .ok();
        Self {
            ready: std::cell::OnceCell::new(),
            pending: std::cell::RefCell::new(pending),
        }
    }

    /// Views that arrive on `rx` whenever the test sends them: a reader as slow as the
    /// test likes.
    #[cfg(test)]
    pub(crate) fn waiting_on(rx: std::sync::mpsc::Receiver<ViewManager>) -> Self {
        Self {
            ready: std::cell::OnceCell::new(),
            pending: std::cell::RefCell::new(Some(rx)),
        }
    }

    /// Whether the read has been waited for yet.
    #[cfg(test)]
    pub(crate) fn is_read(&self) -> bool {
        self.ready.get().is_some()
    }

    fn manager(&self) -> &ViewManager {
        self.ready.get_or_init(|| {
            self.pending
                .borrow_mut()
                .take()
                .and_then(|rx| rx.recv().ok())
                // No worker, or it died: read them here rather than go without.
                .unwrap_or_else(ViewManager::load_or_empty)
        })
    }
}

impl From<ViewManager> for Views {
    fn from(manager: ViewManager) -> Self {
        Self {
            ready: std::cell::OnceCell::from(manager),
            pending: std::cell::RefCell::new(None),
        }
    }
}

impl std::ops::Deref for Views {
    type Target = ViewManager;

    fn deref(&self) -> &ViewManager {
        self.manager()
    }
}

impl std::ops::DerefMut for Views {
    fn deref_mut(&mut self) -> &mut ViewManager {
        self.manager();
        self.ready.get_mut().expect("read just now")
    }
}

impl ViewManager {
    /// The views in the config directory, or none: a directory that cannot be read
    /// falls back to a temporary one, as the app always has, so startup never fails
    /// on it.
    pub fn load_or_empty() -> Self {
        let config = ConfigManager::new(crate::APP_NAME).unwrap_or_else(|_| ConfigManager {
            config_dir: std::env::temp_dir().join(crate::APP_NAME).join("config"),
        });
        Self::new(&config).unwrap_or_else(|_| {
            let last_resort = ConfigManager {
                config_dir: std::env::temp_dir().join("datui_config"),
            };
            Self::new(&last_resort).unwrap_or_else(|_| Self::empty(&last_resort))
        })
    }

    /// Creates a template manager that loads templates from disk. Use `empty()` when
    /// config dirs are unavailable to avoid panicking on startup.
    pub fn new(config: &ConfigManager) -> Result<Self> {
        // Don't create directories on startup - be sensitive to constrained environments
        // Directories will be created lazily when actually needed (e.g., saving templates)
        let views_dir = config.config_dir().join("templates");

        let mut manager = Self {
            config: config.clone(),
            views: Vec::new(),
            views_dir,
            broken_views: Vec::new(),
        };

        // Only try to load templates if the directory exists
        // Don't create it if it doesn't exist
        manager.load_views()?;
        Ok(manager)
    }

    /// Creates an empty in-memory template manager (no disk load). Use when
    /// `new()` fails so the app can start without panicking; save may fail later.
    pub fn empty(config: &ConfigManager) -> Self {
        Self {
            config: config.clone(),
            views: Vec::new(),
            views_dir: config.config_dir().join("templates"),
            broken_views: Vec::new(),
        }
    }

    pub fn load_views(&mut self) -> Result<()> {
        self.views.clear();
        self.broken_views.clear();

        // Load all template files
        if !self.views_dir.exists() {
            return Ok(());
        }

        let entries = fs::read_dir(&self.views_dir)?;
        for entry in entries {
            let entry = entry?;
            let path = entry.path();

            if path.is_file()
                && path.extension().and_then(|s| s.to_str()) == Some("json")
                && let Ok(content) = fs::read_to_string(&path)
            {
                match serde_json::from_str::<SavedView>(&content) {
                    Ok(mut view) => {
                        view.match_criteria.unmangle_urls();
                        self.views.push(view);
                    }
                    Err(e) => {
                        let filename = path
                            .file_stem()
                            .and_then(|s| s.to_str())
                            .unwrap_or("unknown")
                            .to_string();
                        self.broken_views.push(BrokenView {
                            filename,
                            error: e.to_string(),
                        });
                    }
                }
            }
        }

        Ok(())
    }

    fn view_path(&self, id: &str) -> PathBuf {
        self.views_dir.join(format!("template_{id}.json"))
    }

    /// Run `work` holding the views' lock, which every write and delete takes, so
    /// another instance cannot land between reading a view and writing it back.
    fn locked<T>(&self, work: impl FnOnce() -> Result<T>) -> Result<T> {
        self.config.ensure_config_dir()?;
        fs::create_dir_all(&self.views_dir)?;
        let lock = crate::cache::lock_file(
            &self.views_dir.join("views.lock"),
            std::time::Duration::from_secs(2),
        )?
        .ok_or_else(|| color_eyre::eyre::eyre!("another datui is saving views; try again"))?;
        let result = work();
        drop(lock);
        result
    }

    /// The view as stored now, `None` when it is gone (deleted, perhaps by another
    /// instance). A file that does not parse is an error: it is not ours to replace.
    fn read_stored(&self, id: &str) -> Result<Option<SavedView>> {
        let text = match fs::read_to_string(self.view_path(id)) {
            Ok(text) => text,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let mut view: SavedView = serde_json::from_str(&text)?;
        view.match_criteria.unmangle_urls();
        Ok(Some(view))
    }

    fn write_stored(&self, view: &SavedView) -> Result<()> {
        let json = serde_json::to_string_pretty(view)?;
        crate::cache::atomic_write(&self.view_path(&view.id), json.as_bytes())?;
        Ok(())
    }

    /// Write `template` as it is, under the lock and by rename: a crash or a reader
    /// mid-write never meets a half-written view.
    pub fn save_view(&self, view: &SavedView) -> Result<()> {
        self.locked(|| self.write_stored(view))
    }

    pub fn delete_view(&mut self, id: &str) -> Result<()> {
        let file_path = self.view_path(id);
        if file_path.exists() {
            self.locked(|| match fs::remove_file(&file_path) {
                Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(e.into()),
                _ => Ok(()),
            })?;
        }

        self.views.retain(|t| t.id != id);
        Ok(())
    }

    /// Count a use of the view: its stored file is bumped, not this instance's copy
    /// written back, so an edit made by another instance since this one read the
    /// views is kept, and a view deleted elsewhere stays deleted.
    pub fn record_use(&mut self, id: &str, file: &Path) -> Result<()> {
        let stored = self.locked(|| {
            let Some(mut stored) = self.read_stored(id)? else {
                return Ok(None);
            };
            stored.last_used = Some(SystemTime::now());
            stored.usage_count += 1;
            stored.last_matched_file = Some(file.to_path_buf());
            self.write_stored(&stored)?;
            Ok(Some(stored))
        })?;
        self.adopt(id, stored);
        Ok(())
    }

    /// Replace this instance's copy of a view with the stored one, or drop it.
    fn adopt(&mut self, id: &str, stored: Option<SavedView>) {
        match stored {
            Some(stored) => match self.views.iter_mut().find(|t| t.id == id) {
                Some(existing) => *existing = stored,
                None => self.views.push(stored),
            },
            None => self.views.retain(|t| t.id != id),
        }
    }

    pub fn find_relevant_views<'a>(
        &self,
        dataset: impl Into<Dataset<'a>>,
        schema: &Schema,
    ) -> Vec<(SavedView, f64)> {
        let dataset = dataset.into();
        let mut results: Vec<(SavedView, f64)> = self
            .views
            .iter()
            .map(|view| {
                let score = calculate_relevance(view, dataset, schema);
                (view.clone(), score)
            })
            .collect();

        // Sort by relevance score (highest first)
        results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        results
    }

    /// The best template whose own criteria match this file — not merely the
    /// best-scored one. Scores mix in usage and recency, so with no gate the
    /// most-used template "matches" every dataset ever opened; `T` applying it
    /// silently is how templates lose the user's trust.
    pub fn get_most_relevant<'a>(
        &self,
        dataset: impl Into<Dataset<'a>>,
        schema: &Schema,
    ) -> Option<(SavedView, MatchReason)> {
        let dataset = dataset.into();
        self.find_relevant_views(dataset, schema)
            .into_iter()
            .find_map(|(view, _)| {
                let reason = match_reason(&view, dataset, schema)?;
                Some((view, reason))
            })
    }

    /// A name for a view saved from this state: the file stem, or failing
    /// that the query's first words — something the user will recognize in
    /// the list, never a serial number. Numbered past the first collision.
    pub fn suggest_name(&self, path: Option<&Path>, query: Option<&str>) -> String {
        let base = path
            .and_then(|p| p.file_stem())
            .and_then(|s| s.to_str())
            .map(str::to_string)
            .filter(|s| !s.trim().is_empty())
            .or_else(|| query.map(|q| q.split_whitespace().take(4).collect::<Vec<_>>().join(" ")))
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| "view".to_string());

        if !self.view_exists(&base) {
            return base;
        }
        (2..)
            .map(|n| format!("{base} {n}"))
            .find(|name| !self.view_exists(name))
            .expect("some numbered name is free")
    }

    pub fn view_exists(&self, name: &str) -> bool {
        self.views.iter().any(|t| t.name == name)
    }

    pub fn get_view_by_name(&self, name: &str) -> Option<&SavedView> {
        self.views.iter().find(|t| t.name == name)
    }

    pub fn get_view_by_id(&self, id: &str) -> Option<&SavedView> {
        self.views.iter().find(|t| t.id == id)
    }

    pub fn all_views(&self) -> &[SavedView] {
        &self.views
    }

    pub fn create_view(
        &mut self,
        name: String,
        description: Option<String>,
        match_criteria: MatchCriteria,
        settings: ViewSettings,
    ) -> Result<SavedView> {
        // Generate unique ID based on name and timestamp
        let mut hasher = DefaultHasher::new();
        name.hash(&mut hasher);
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
            .hash(&mut hasher);
        let id = format!("{:016x}", hasher.finish());

        let view = SavedView {
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
        self.save_view(&view)?;

        // Reload templates to include the new one
        self.load_views()?;

        Ok(view)
    }

    /// Save an edit of a view. Only what the edit changed from this instance's copy
    /// is written, over the view as stored now, so another instance's edit to other
    /// fields and its usage counts survive. A view deleted elsewhere is not brought
    /// back: that is an error, and the view leaves this instance too.
    pub fn update_view(&mut self, edited: &SavedView) -> Result<()> {
        let base = self.get_view_by_id(&edited.id).cloned();
        let stored = self.locked(|| {
            let Some(mut stored) = self.read_stored(&edited.id)? else {
                return Ok(None);
            };
            merge_edit(&mut stored, base.as_ref(), edited);
            self.write_stored(&stored)?;
            Ok(Some(stored))
        })?;
        let deleted = stored.is_none();
        self.adopt(&edited.id, stored);
        if deleted {
            return Err(color_eyre::eyre::eyre!(
                "view \"{}\" was deleted by another datui",
                edited.name
            ));
        }
        Ok(())
    }

    pub fn remove_all_views(&mut self) -> Result<()> {
        // Delete all template files
        if self.views_dir.exists() {
            self.locked(|| {
                for entry in fs::read_dir(&self.views_dir)? {
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
                Ok(())
            })?;
        }

        // Clear in-memory list
        self.views.clear();

        Ok(())
    }
}

/// Apply to `stored` the fields `edited` changed from `base`, the copy the edit began
/// from. Without a base every edited field is taken. Usage is never an edit.
fn merge_edit(stored: &mut SavedView, base: Option<&SavedView>, edited: &SavedView) {
    fn changed<T: Serialize>(base: Option<&T>, edited: &T) -> bool {
        base.is_none_or(|base| serde_json::to_value(base).ok() != serde_json::to_value(edited).ok())
    }
    if changed(base.map(|b| &b.name), &edited.name) {
        stored.name = edited.name.clone();
    }
    if changed(base.map(|b| &b.description), &edited.description) {
        stored.description = edited.description.clone();
    }
    if changed(base.map(|b| &b.match_criteria), &edited.match_criteria) {
        stored.match_criteria = edited.match_criteria.clone();
    }
    if changed(base.map(|b| &b.settings), &edited.settings) {
        stored.settings = edited.settings.clone();
    }
}

/// Why a view's criteria fit the open file, in the words the list annotates
/// rows with. The strongest reason wins: the same file beats the same columns
/// beats a glob.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatchReason {
    SameFile,
    SameColumns,
    Glob,
}

impl MatchReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            MatchReason::SameFile => "same file",
            MatchReason::SameColumns => "same columns",
            MatchReason::Glob => "glob",
        }
    }
}

/// What a view is matched against: where the dataset was read from, and which table
/// of it. Standard input's `-` is a path no path criterion fits, so data piped in or a
/// frame handed over is matched by its columns alone.
#[derive(Debug, Clone, Copy)]
pub struct Dataset<'a> {
    pub path: &'a Path,
    pub table: Option<&'a str>,
}

impl<'a> From<&'a Path> for Dataset<'a> {
    fn from(path: &'a Path) -> Self {
        Self { path, table: None }
    }
}

impl<'a> From<&'a PathBuf> for Dataset<'a> {
    fn from(path: &'a PathBuf) -> Self {
        Self::from(path.as_path())
    }
}

impl<'a> Dataset<'a> {
    /// The path the view's path criteria are compared with, or None when none can fit:
    /// the dataset has no path, or it is another table than the one the view was saved
    /// on. A view saved before tables were recorded fits any.
    fn place_for(self, criteria: &MatchCriteria) -> Option<&'a Path> {
        let table_fits = criteria
            .table
            .as_deref()
            .is_none_or(|saved| self.table == Some(saved));
        (table_fits && has_a_path(self.path)).then_some(self.path)
    }
}

/// A dataset's location as a view records it: a URL as written, a local path made
/// absolute and resolved. The save form offers this and matching compares with it,
/// so the two are spelled alike.
pub fn exact_location(path: &Path) -> PathBuf {
    if crate::source::is_remote_url(path) {
        return path.to_path_buf();
    }
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        match std::env::current_dir() {
            Ok(cwd) => cwd.join(path),
            Err(_) => return path.to_path_buf(),
        }
    };
    // Matching runs on the interface thread, where a stalled network mount must
    // not be touched; such a path is compared as spelled.
    if crate::home::is_network_path(&absolute) {
        return absolute;
    }
    crate::canonical::canonicalize(&absolute)
        .ok()
        .or_else(|| canonical_prefix(&absolute))
        .unwrap_or(absolute)
}

/// A path nothing on disk has, such as a table inside its file (`shop.db/orders`),
/// with the part that is there resolved: opened through a link or from the home
/// screen, it is spelled alike.
fn canonical_prefix(path: &Path) -> Option<PathBuf> {
    let (there, canonical) = path
        .ancestors()
        .skip(1)
        .take_while(|p| !p.as_os_str().is_empty())
        .find_map(|p| Some((p, crate::canonical::canonicalize(p).ok()?)))?;
    Some(canonical.join(path.strip_prefix(there).ok()?))
}

/// A local dataset's path relative to the working directory, when it is under it.
/// A URL has none: it names the same data wherever datui runs.
pub fn relative_location(path: &Path) -> Option<String> {
    if crate::source::is_remote_url(path) {
        return None;
    }
    let cwd = exact_location(&std::env::current_dir().ok()?);
    let relative = exact_location(path)
        .strip_prefix(&cwd)
        .ok()?
        .to_string_lossy()
        .into_owned();
    (!relative.is_empty()).then_some(relative)
}

/// Whether the view's exact path names the dataset at `file_path`. A URL compares as
/// text less any trailing slash, which is how a directory's URL may or may not end,
/// and with its scheme in either case, as datui opens it.
pub fn exact_path_matches<'a>(criteria: &MatchCriteria, dataset: impl Into<Dataset<'a>>) -> bool {
    let (Some(stored), Some(file_path)) = (
        criteria.exact_path.as_deref(),
        dataset.into().place_for(criteria),
    ) else {
        return false;
    };
    if crate::source::is_remote_url(stored) || crate::source::is_remote_url(file_path) {
        return url_key(stored) == url_key(file_path);
    }
    stored == file_path || stored == exact_location(file_path)
}

/// Whether `file_path` is a path a path criterion can fit: not standard input's `-`,
/// which views match by its columns alone.
fn has_a_path(file_path: &Path) -> bool {
    !crate::stdin::is_stdin(file_path)
}

fn url_key(path: &Path) -> String {
    let text = path.to_string_lossy();
    let text = text.trim_end_matches('/');
    match text.split_once("://") {
        Some((scheme, rest)) => format!("{}://{rest}", scheme.to_ascii_lowercase()),
        None => text.to_string(),
    }
}

/// Whether the view's relative path names the dataset at `file_path`.
pub fn relative_path_matches<'a>(
    criteria: &MatchCriteria,
    dataset: impl Into<Dataset<'a>>,
) -> bool {
    let Some(file_path) = dataset.into().place_for(criteria) else {
        return false;
    };
    criteria.relative_path.as_deref().is_some_and(|stored| {
        relative_location(file_path).is_some_and(|rel| Path::new(&rel) == Path::new(stored))
    })
}

/// Whether the view's path pattern fits `file_path`, as opened or as the save form
/// spells it: a file opened by a relative path or through a link is still under the
/// resolved directory its pattern was suggested from.
pub fn path_pattern_matches<'a>(criteria: &MatchCriteria, dataset: impl Into<Dataset<'a>>) -> bool {
    let Some(file_path) = dataset.into().place_for(criteria) else {
        return false;
    };
    criteria.path_pattern.as_deref().is_some_and(|pattern| {
        let fits = |p: &Path| {
            p.to_str()
                .is_some_and(|text| matches_pattern(text, pattern))
        };
        fits(file_path) || fits(&exact_location(file_path))
    })
}

/// Whether the view's filename pattern fits the name of `file_path`.
pub fn filename_pattern_matches<'a>(
    criteria: &MatchCriteria,
    dataset: impl Into<Dataset<'a>>,
) -> bool {
    let Some(file_path) = dataset.into().place_for(criteria) else {
        return false;
    };
    criteria.filename_pattern.as_deref().is_some_and(|pattern| {
        file_path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| matches_pattern(name, pattern))
    })
}

/// Whether the template's own criteria match this file: a path or pattern hit,
/// or every schema column the template asks for present. Distinct from the
/// relevance score, which also carries usage and recency and so is never zero
/// for a template that has been used — a ranking, not a claim of fit.
pub fn criteria_match<'a>(
    view: &SavedView,
    dataset: impl Into<Dataset<'a>>,
    schema: &Schema,
) -> bool {
    match_reason(view, dataset, schema).is_some()
}

/// The strongest criterion of the template's that fits this file, or None when
/// none does. This is the same test `criteria_match` gates on, kept in one
/// place so the list's "why it matches" annotation can never disagree with
/// what `V` and auto-apply do.
pub fn match_reason<'a>(
    view: &SavedView,
    dataset: impl Into<Dataset<'a>>,
    schema: &Schema,
) -> Option<MatchReason> {
    let dataset = dataset.into();
    let criteria = &view.match_criteria;
    if exact_path_matches(criteria, dataset) || relative_path_matches(criteria, dataset) {
        return Some(MatchReason::SameFile);
    }
    if let Some(required) = &criteria.schema_columns
        && !required.is_empty()
    {
        let file_cols: HashSet<&str> = schema.iter_names().map(|s| s.as_str()).collect();
        if required.iter().all(|col| file_cols.contains(col.as_str())) {
            return Some(MatchReason::SameColumns);
        }
    }
    if path_pattern_matches(criteria, dataset) || filename_pattern_matches(criteria, dataset) {
        return Some(MatchReason::Glob);
    }
    None
}

fn calculate_relevance(view: &SavedView, dataset: Dataset<'_>, schema: &Schema) -> f64 {
    let mut score = 0.0;

    let exact_path_match = exact_path_matches(&view.match_criteria, dataset);
    let relative_path_match = relative_path_matches(&view.match_criteria, dataset);

    // Check for exact schema match
    let exact_schema_match = if let Some(required_cols) = &view.match_criteria.schema_columns {
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
    if let Some(pattern) = &view.match_criteria.path_pattern
        && path_pattern_matches(&view.match_criteria, dataset)
    {
        score += 50.0;
        score += pattern_specificity_bonus(pattern);
    }

    // Filename pattern match
    if let Some(pattern) = &view.match_criteria.filename_pattern
        && filename_pattern_matches(&view.match_criteria, dataset)
    {
        score += 30.0;
        score += pattern_specificity_bonus(pattern);
    }

    // Partial schema matching (only if not exact match)
    if let Some(required_cols) = &view.match_criteria.schema_columns {
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
    score += (view.usage_count.min(10) as f64) * 1.0;
    if let Some(last_used) = view.last_used
        && let Ok(duration) = SystemTime::now().duration_since(last_used)
    {
        let days_since = duration.as_secs() / 86400;
        if days_since <= 7 {
            score += 5.0;
        } else if days_since <= 30 {
            score += 2.0;
        }
    }
    // No penalty for age since creation: a template is not worse for being old,
    // and last-used recency above already separates the live from the stale.
    // Charged anyway, a year-old template that fit showed a negative "score".

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

/// Glob-like matching: `*` matches any sequence; anything else matches itself.
/// (`?` gets no special treatment: it is rare in names, and a single-character
/// wildcard is not worth a second wildcard rule in a five-field matcher.)
fn matches_pattern(text: &str, pattern: &str) -> bool {
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
    /// template whose criteria fit nothing must never be what `T` applies.
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
        assert!(!criteria_match(&unrelated, path, &schema));
        assert!(calculate_relevance(&unrelated, path.into(), &schema) > 0.0);

        let fits = a_view(
            "fits by schema",
            MatchCriteria {
                schema_columns: Some(vec!["a".to_string()]),
                ..no_criteria()
            },
        );
        assert!(criteria_match(&fits, path, &schema));
    }

    /// Data piped in matches by its columns alone: no path or pattern fits `-`, even
    /// a pattern of `*` or a view saved with that name as its path.
    #[test]
    fn stdin_matches_by_schema_only() {
        use polars::prelude::DataType;
        let schema = Schema::from_iter([("a".into(), DataType::Int64)]);
        let stdin = Path::new(crate::stdin::PATH);
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

    /// A template's schema criterion asks for its columns to be present; a file
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
        assert!(criteria_match(&view, Path::new("/x.parquet"), &schema));

        let narrower = Schema::from_iter([("a".into(), DataType::Int64)]);
        assert!(!criteria_match(&view, Path::new("/x.parquet"), &narrower));
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
        assert!(!criteria_match(&fits_nothing, path, &schema));
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
        let views = dir.path().join("templates");
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
        fs::write(views.join("template_old.json"), json).unwrap();

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
}

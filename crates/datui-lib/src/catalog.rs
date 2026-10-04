//! Catalogs: named datasets, local or remote, one TOML file each.
//!
//! A catalog's top level holds `label` and `description`; every table in it is one
//! dataset, keyed by a short id. `catalog.toml` in the config directory is the user's
//! own, the one file datui writes (Ctrl+D on home); `catalogs = [...]` in the config
//! lists others; the `public` catalog is bundled. Each is a section of the home screen.
//!
//! Parsed with `toml_edit` rather than `toml`: it keeps the file's order, which is the
//! order the home screen lists, and the place of every key, which an error names.

use std::path::{Path, PathBuf};

use crate::config::{CloudConnectionConfig, expand_path, is_valid_source_id};

/// The bundled catalog's id.
pub const PUBLIC: &str = "public";
/// The id of the user's own catalog, `catalog.toml`.
pub const MINE: &str = "mine";
/// The user's own catalog, in the config directory.
pub const MINE_FILE: &str = "catalog.toml";
/// The label of `catalog.toml` when it gives none.
pub const MINE_LABEL: &str = "My datasets";

const BUNDLED: &str = include_str!("public_catalog.toml");

const DATASET_KEYS: &str = "name, path, url, auth, connection, description, publisher, \
     license, homepage, documentation, size, columns, bookmarks";
const COLUMN_KEYS: &str = "description, unit, values";
const AUTH_VALUES: &str = "auto or anonymous";

/// Where a catalog came from, which decides what datui may do to it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Origin {
    /// `catalog.toml`: Ctrl+D adds to it and forgets from it.
    Mine,
    /// A file named in `catalogs`: read, never written.
    Listed,
    /// The `public` catalog datui ships.
    Bundled,
}

/// One catalog file.
#[derive(Debug, Clone, PartialEq)]
pub struct Catalog {
    /// What `home.hide` names: `mine`, `public`, or a listed file's stem.
    pub id: String,
    /// The section's title.
    pub label: String,
    pub description: String,
    pub origin: Origin,
    /// The file it was read from; none for the bundled one.
    pub file: Option<PathBuf>,
    pub datasets: Vec<Dataset>,
}

/// One dataset of a catalog: a local `path` or a remote `url`, never both.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Dataset {
    /// The table's key: short, unique in its catalog.
    pub id: String,
    /// Where its table starts in the file, for errors.
    pub line: usize,
    /// The row's label.
    pub name: String,
    pub path: Option<String>,
    pub url: Option<String>,
    pub auth: Option<String>,
    pub connection: Option<String>,
    pub description: String,
    pub publisher: String,
    pub license: String,
    pub homepage: String,
    /// The publisher's documentation of the columns: the source of `columns`.
    pub documentation: String,
    /// About how many bytes an HTTP(S) file is, shown until it is measured.
    pub size: Option<u64>,
    /// What each column means, by its name as the data spells it, in file order.
    pub columns: Vec<(String, ColumnNote)>,
    /// Places inside a directory dataset to start from, by name, in file order.
    pub bookmarks: Vec<(String, String)>,
    /// The directory of the file that names it: a relative `path` is relative to it.
    pub base: Option<PathBuf>,
}

/// A column's note: what it means, its unit, and what its codes stand for.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ColumnNote {
    pub description: String,
    pub unit: String,
    /// Code to meaning, in file order. `""` is what a blank or null value means.
    pub values: Vec<(String, String)>,
}

/// A mistake in a catalog file: where, and what to do about it.
#[derive(Debug, Clone, PartialEq)]
pub struct CatalogError {
    pub line: Option<usize>,
    pub message: String,
}

impl CatalogError {
    fn at(line: usize, message: impl Into<String>) -> Self {
        Self {
            line: Some(line),
            message: message.into(),
        }
    }

    /// `file:line: message`, the way a compiler names a mistake.
    pub fn in_file(&self, file: &str) -> String {
        match self.line {
            Some(line) => format!("{file}:{line}: {}", self.message),
            None => format!("{file}: {}", self.message),
        }
    }
}

/// Where a dataset URL lives, as far as reading it is concerned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UrlPlace {
    /// S3, Google Cloud or Azure, with its connection kind.
    ObjectStore(&'static str),
    Http,
}

/// Where a dataset URL is, when datui can read it: a place in S3, Google Cloud or Azure
/// (a file or a directory), or a data file on a web server, which has no listing.
pub(crate) fn dataset_url_place(url: &str) -> Option<UrlPlace> {
    if url.chars().any(char::is_whitespace) {
        return None;
    }
    match crate::source::input_source(Path::new(url)) {
        crate::source::InputSource::Azure(_) => Some(UrlPlace::ObjectStore("azure")),
        crate::source::InputSource::S3(rest) | crate::source::InputSource::Gcs(rest) => {
            let host = rest.split('/').next().unwrap_or("");
            let kind = if url.to_ascii_lowercase().starts_with("s3") {
                "s3"
            } else {
                "gcs"
            };
            (!host.is_empty() && !host.contains('@')).then_some(UrlPlace::ObjectStore(kind))
        }
        crate::source::InputSource::Http(_) => {
            let (_, rest) = url.split_once("://")?;
            let (host, path) = rest.split_once('/')?;
            let path = path.split(['?', '#']).next().unwrap_or("");
            (!host.is_empty()
                && !host.contains('@')
                && crate::discover::is_data_file(Path::new(path)))
            .then_some(UrlPlace::Http)
        }
        crate::source::InputSource::Local(_) => None,
    }
}

/// Whether a dataset URL is in an object store, and so browsed as well as opened.
pub fn is_object_store_dataset(url: &str) -> bool {
    matches!(dataset_url_place(url), Some(UrlPlace::ObjectStore(_)))
}

/// The line `offset` is on, from 1.
fn line_of(text: &str, offset: usize) -> usize {
    text[..offset.min(text.len())].matches('\n').count() + 1
}

/// The line `key` of `table` is written on, or `fallback` when it has no place (a key
/// made by a dotted key further up).
fn key_line(text: &str, table: &dyn toml_edit::TableLike, key: &str, fallback: usize) -> usize {
    table
        .key(key)
        .and_then(toml_edit::Key::span)
        .map(|span| line_of(text, span.start))
        .unwrap_or(fallback)
}

/// A TOML syntax error, with the fix for the one mistake a catalog invites: a column
/// written as an inline table, then given a `[id.columns.X.values]` table of its own.
fn syntax_error(text: &str, error: &toml_edit::TomlError) -> CatalogError {
    let line = error.span().map(|span| line_of(text, span.start));
    let message = error.message().trim_end().to_string();
    let header = line
        .and_then(|l| text.lines().nth(l - 1))
        .map(str::trim)
        .unwrap_or("");
    let values_table = header.starts_with('[')
        && header.contains(".columns.")
        && header.trim_end_matches(']').ends_with(".values");
    let message = if values_table {
        let column = header
            .trim_matches(['[', ']'])
            .rsplit_once(".values")
            .and_then(|(head, _)| head.rsplit_once(".columns."))
            .map(|(_, column)| column.to_string())
            .unwrap_or_else(|| "X".to_string());
        format!(
            "{message}. {header} cannot add to a column written as an inline table \
             (columns.{column} = {{ ... }}). Write the column with dotted keys instead: \
             columns.{column}.description = \"...\""
        )
    } else {
        message
    };
    CatalogError { line, message }
}

fn string_of(item: &toml_edit::Item, what: &str, line: usize) -> Result<String, CatalogError> {
    item.as_str().map(str::to_string).ok_or_else(|| {
        CatalogError::at(
            line,
            format!("{what} must be a string, not {}", item.type_name()),
        )
    })
}

/// Read a catalog's text. `id` is what `home.hide` will name it; `file` is where a
/// relative `path` is anchored, and is named by errors.
pub fn parse(
    text: &str,
    id: &str,
    origin: Origin,
    file: Option<&Path>,
) -> Result<Catalog, CatalogError> {
    let doc = toml_edit::Document::parse(text).map_err(|e| syntax_error(text, &e))?;
    let root = doc.as_table();
    let base = file.and_then(Path::parent).map(Path::to_path_buf);
    let mut catalog = Catalog {
        id: id.to_string(),
        label: String::new(),
        description: String::new(),
        origin,
        file: file.map(Path::to_path_buf),
        datasets: Vec::new(),
    };
    for (key, item) in root.iter() {
        let line = key_line(text, root, key, 1);
        match key {
            "label" => catalog.label = string_of(item, "label", line)?.trim().to_string(),
            "description" => catalog.description = string_of(item, "description", line)?,
            _ => {
                let Some(table) = item.as_table_like() else {
                    return Err(CatalogError::at(
                        line,
                        format!(
                            "\"{key}\" is not a catalog key. The top level holds label, \
                             description, and one [id] table per dataset"
                        ),
                    ));
                };
                let mut dataset = dataset(text, key, table, line)?;
                dataset.base = base.clone();
                catalog.datasets.push(dataset);
            }
        }
    }
    if catalog.label.is_empty() {
        catalog.label = match origin {
            Origin::Mine => MINE_LABEL.to_string(),
            _ => id.to_string(),
        };
    }
    check_unique(&catalog)?;
    Ok(catalog)
}

/// One `[id]` table.
fn dataset(
    text: &str,
    id: &str,
    table: &dyn toml_edit::TableLike,
    line: usize,
) -> Result<Dataset, CatalogError> {
    let what = format!("[{id}]");
    if !is_valid_source_id(id) {
        return Err(CatalogError::at(
            line,
            format!(
                "{what}: a dataset's id is lowercase letters, digits and '-', up to 40 \
                 characters. Its label goes in name = \"...\""
            ),
        ));
    }
    let mut dataset = Dataset {
        id: id.to_string(),
        line,
        ..Default::default()
    };
    for (key, item) in table.iter() {
        let at = key_line(text, table, key, line);
        let field = format!("{what} {key}");
        match key {
            "name" => dataset.name = string_of(item, &field, at)?.trim().to_string(),
            "path" => dataset.path = Some(string_of(item, &field, at)?),
            "url" => dataset.url = Some(string_of(item, &field, at)?),
            "auth" => dataset.auth = Some(string_of(item, &field, at)?),
            "connection" => dataset.connection = Some(string_of(item, &field, at)?),
            "description" => dataset.description = string_of(item, &field, at)?,
            "publisher" => dataset.publisher = string_of(item, &field, at)?,
            "license" => dataset.license = string_of(item, &field, at)?,
            "homepage" => dataset.homepage = string_of(item, &field, at)?,
            "documentation" => dataset.documentation = string_of(item, &field, at)?,
            "size" => {
                let size = item.as_integer().filter(|n| *n >= 0).ok_or_else(|| {
                    CatalogError::at(at, format!("{field} must be a number of bytes"))
                })?;
                dataset.size = Some(size as u64);
            }
            "columns" => dataset.columns = columns(text, &what, item, at)?,
            "bookmarks" => {
                let Some(marks) = item.as_table_like() else {
                    return Err(CatalogError::at(
                        at,
                        format!("{field}: write each as bookmarks.\"Name\" = \"path/\""),
                    ));
                };
                for (name, place) in marks.iter() {
                    let at = key_line(text, marks, name, at);
                    let place = string_of(place, &format!("{what} bookmarks.\"{name}\""), at)?;
                    dataset.bookmarks.push((name.trim().to_string(), place));
                }
            }
            "codebook" => {
                return Err(CatalogError::at(
                    at,
                    format!("{field}: the link is documentation = \"https://...\" now"),
                ));
            }
            "suggested" => {
                return Err(CatalogError::at(
                    at,
                    format!("{field}: places are bookmarks.\"Name\" = \"path/\" now"),
                ));
            }
            _ => {
                return Err(CatalogError::at(
                    at,
                    format!("{what}: unknown key '{key}'. Expected one of: {DATASET_KEYS}"),
                ));
            }
        }
    }
    check_dataset(&dataset).map_err(|message| CatalogError::at(line, message))?;
    Ok(dataset)
}

fn columns(
    text: &str,
    what: &str,
    item: &toml_edit::Item,
    line: usize,
) -> Result<Vec<(String, ColumnNote)>, CatalogError> {
    let Some(table) = item.as_table_like() else {
        return Err(CatalogError::at(
            line,
            format!("{what} columns: write each as columns.NAME = {{ description = \"...\" }}"),
        ));
    };
    let mut out = Vec::new();
    for (column, item) in table.iter() {
        let at = key_line(text, table, column, line);
        let what = format!("{what} column \"{column}\"");
        let Some(fields) = item.as_table_like() else {
            return Err(CatalogError::at(
                at,
                format!("{what}: write it as columns.{column} = {{ description = \"...\" }}"),
            ));
        };
        let mut note = ColumnNote::default();
        for (key, item) in fields.iter() {
            let at = key_line(text, fields, key, at);
            match key {
                "description" => {
                    note.description = string_of(item, &format!("{what} description"), at)?
                        .trim()
                        .to_string();
                }
                "unit" => {
                    note.unit = string_of(item, &format!("{what} unit"), at)?
                        .trim()
                        .to_string();
                }
                "values" => {
                    let Some(values) = item.as_table_like() else {
                        return Err(CatalogError::at(
                            at,
                            format!("{what}: values is a table of code = \"meaning\""),
                        ));
                    };
                    for (code, meaning) in values.iter() {
                        let at = key_line(text, values, code, at);
                        let meaning = string_of(meaning, &format!("{what} value \"{code}\""), at)?;
                        if meaning.trim().is_empty() {
                            return Err(CatalogError::at(
                                at,
                                format!("{what}: value \"{code}\" has no meaning"),
                            ));
                        }
                        note.values.push((code.to_string(), meaning));
                    }
                }
                _ => {
                    return Err(CatalogError::at(
                        at,
                        format!("{what}: unknown key '{key}'. Expected one of: {COLUMN_KEYS}"),
                    ));
                }
            }
        }
        if note.description.is_empty() && note.unit.is_empty() && note.values.is_empty() {
            return Err(CatalogError::at(
                at,
                format!("{what}: says nothing. Give a description, unit or values"),
            ));
        }
        out.push((column.to_string(), note));
    }
    Ok(out)
}

/// The rules one dataset keeps on its own: where it is, how it is read, what its
/// documentation and bookmarks say. Connections are checked against the config in
/// [`Catalog::check_connections`].
fn check_dataset(dataset: &Dataset) -> Result<(), String> {
    let what = format!("[{}]", dataset.id);
    if dataset.name.is_empty() {
        return Err(format!("{what}: give it a name = \"...\" for its row"));
    }
    if !dataset.documentation.is_empty() && !dataset.documentation.starts_with("https://") {
        return Err(format!(
            "{what}: documentation \"{}\" is not an https:// link",
            dataset.documentation
        ));
    }
    if !dataset.homepage.is_empty()
        && !(dataset.homepage.starts_with("https://") || dataset.homepage.starts_with("http://"))
    {
        return Err(format!(
            "{what}: homepage \"{}\" is not an http(s):// link",
            dataset.homepage
        ));
    }
    let place = match (&dataset.path, &dataset.url) {
        (None, None) => return Err(format!("{what}: say where it is with path or url")),
        (Some(_), Some(_)) => {
            return Err(format!(
                "{what}: path and url both say where it is. Use one"
            ));
        }
        (Some(path), None) => {
            if path.trim().is_empty() {
                return Err(format!("{what}: path is blank"));
            }
            if path.contains("://") {
                return Err(format!("{what}: \"{path}\" is a URL. Use url = \"{path}\""));
            }
            for (field, set) in [
                ("auth", dataset.auth.is_some()),
                ("connection", dataset.connection.is_some()),
            ] {
                if set {
                    return Err(format!(
                        "{what}: {field} applies only to a url; a path is read as a file"
                    ));
                }
            }
            if dataset.size.is_some() {
                return Err(format!(
                    "{what}: size applies only to an http(s) url; a local file is measured"
                ));
            }
            None
        }
        (None, Some(url)) => Some(dataset_url_place(url).ok_or_else(|| {
            if crate::source::split_source_id(url).0.is_some() {
                format!(
                    "{what}: name the connection with connection = \"...\" rather than in \
                     the URL"
                )
            } else if url.starts_with("http://") || url.starts_with("https://") {
                format!(
                    "{what}: \"{url}\" is not a data file. An HTTP server has no listing, so \
                     a web URL must name a file datui reads, such as .csv or .parquet"
                )
            } else {
                format!("{what}: \"{url}\" is not an s3://, gs://, Azure or HTTP(S) URL")
            }
        })?),
    };
    if let Some(place) = place {
        if let Some(auth) = dataset.auth.as_deref()
            && !matches!(auth, "auto" | "anonymous")
        {
            return Err(format!(
                "{what}: auth \"{auth}\" is not valid. Expected {AUTH_VALUES}"
            ));
        }
        if place == UrlPlace::Http {
            if dataset.connection.is_some() {
                return Err(format!(
                    "{what}: connection applies only to s3://, gs:// and Azure URLs. A web \
                     URL is read with no login"
                ));
            }
        } else if dataset.size.is_some() {
            return Err(format!(
                "{what}: size applies only to an http(s) url; a store gives its own"
            ));
        }
        if dataset.auth.is_some() && dataset.connection.is_some() {
            return Err(format!(
                "{what}: auth and connection both say how to read it. Use one"
            ));
        }
    }
    if dataset.bookmarks.is_empty() {
        return Ok(());
    }
    if !matches!(place, None | Some(UrlPlace::ObjectStore(_))) {
        return Err(format!(
            "{what}: bookmarks apply only to a local path or an s3://, gs:// or Azure url"
        ));
    }
    let mut names = std::collections::HashSet::new();
    for (name, path) in &dataset.bookmarks {
        if name.is_empty() {
            return Err(format!("{what}: every bookmark needs a name"));
        }
        if !names.insert(name.as_str()) {
            return Err(format!(
                "{what} bookmark \"{name}\": the name is used twice"
            ));
        }
        let path = path.trim();
        if path.is_empty()
            || path.starts_with('/')
            || path.contains("://")
            || path.contains('\\')
            || path.split('/').any(|part| part == "..")
        {
            return Err(format!(
                "{what} bookmark \"{name}\": path \"{path}\" must be relative to the dataset \
                 and stay inside it"
            ));
        }
    }
    Ok(())
}

/// No two datasets of one catalog share a name or a location.
fn check_unique(catalog: &Catalog) -> Result<(), CatalogError> {
    let mut names = std::collections::HashMap::new();
    let mut places = std::collections::HashMap::new();
    for dataset in &catalog.datasets {
        if let Some(first) = names.insert(dataset.name.as_str(), dataset.id.as_str()) {
            return Err(CatalogError::at(
                dataset.line,
                format!(
                    "[{}]: name \"{}\" is [{first}]'s too. Give each its own",
                    dataset.id, dataset.name
                ),
            ));
        }
        if let Some(first) = places.insert(dataset.place_key(), dataset.id.as_str()) {
            return Err(CatalogError::at(
                dataset.line,
                format!(
                    "[{}]: \"{}\" is listed as [{first}] already",
                    dataset.id,
                    dataset.location_text()
                ),
            ));
        }
    }
    Ok(())
}

impl Dataset {
    /// The local path with `~` and `$VAR` expanded, anchored at the catalog's directory
    /// when it is relative.
    pub fn local_path(&self) -> Option<PathBuf> {
        let path = expand_path(self.path.as_deref()?);
        Some(match &self.base {
            Some(base) if path.is_relative() => base.join(path),
            _ => path,
        })
    }

    /// Where it is: the local path, expanded, or the URL.
    pub fn location(&self) -> PathBuf {
        self.local_path()
            .or_else(|| self.url.as_deref().map(PathBuf::from))
            .unwrap_or_default()
    }

    /// The path or URL as written.
    pub fn location_text(&self) -> &str {
        self.path.as_deref().or(self.url.as_deref()).unwrap_or("")
    }

    /// What two entries naming the same data have in common.
    pub fn place_key(&self) -> String {
        match (&self.path, &self.url) {
            (Some(_), _) => format!(
                "path:{}",
                crate::config::path_place(&self.local_path().unwrap_or_default()).display()
            ),
            (None, Some(url)) => url_key(url),
            (None, None) => String::new(),
        }
    }

    /// Where a bookmark is: its path under the dataset's.
    pub fn bookmark_location(&self, path: &str) -> PathBuf {
        let rel = path.trim().trim_start_matches("./");
        if let Some(local) = self.local_path() {
            return local.join(rel);
        }
        PathBuf::from(format!(
            "{}/{rel}",
            self.url.as_deref().unwrap_or("").trim_end_matches('/')
        ))
    }

    /// Whether a dataset in an object store, and how it is read: `auto`, `anonymous`,
    /// or a connection's name. `None` for a path or a web file.
    pub fn object_store_auth(&self) -> Option<crate::config::DatasetAuth> {
        let url = self.url.as_deref()?;
        if !is_object_store_dataset(url) {
            return None;
        }
        Some(match (self.connection.as_deref(), self.auth.as_deref()) {
            (Some(connection), _) => crate::config::DatasetAuth::Connection(connection.to_string()),
            (None, Some("anonymous")) => crate::config::DatasetAuth::Anonymous,
            _ => crate::config::DatasetAuth::Auto,
        })
    }

    /// Whether this is an HTTP(S) file.
    pub fn is_web_file(&self) -> bool {
        self.url
            .as_deref()
            .is_some_and(|url| dataset_url_place(url) == Some(UrlPlace::Http))
    }
}

impl Catalog {
    /// Every `connection` a dataset names is one of `connections`, of the URL's kind.
    pub fn check_connections(
        &self,
        connections: &[CloudConnectionConfig],
    ) -> Result<(), CatalogError> {
        for dataset in &self.datasets {
            let (Some(connection), Some(url)) =
                (dataset.connection.as_deref(), dataset.url.as_deref())
            else {
                continue;
            };
            let what = format!("[{}]", dataset.id);
            let fail = |message: String| CatalogError::at(dataset.line, message);
            let Some(configured) = connections.iter().find(|c| c.name == connection) else {
                let names: Vec<&str> = connections.iter().map(|c| c.name.as_str()).collect();
                return Err(fail(format!(
                    "{what}: no [[cloud.connections]] entry in the config is named \
                     \"{connection}\"{}",
                    if names.is_empty() {
                        String::new()
                    } else {
                        format!(". Connections: {}", names.join(", "))
                    }
                )));
            };
            let Some(UrlPlace::ObjectStore(kind)) = dataset_url_place(url) else {
                continue;
            };
            let connection_kind = configured.kind.as_deref().unwrap_or("");
            if connection_kind != kind {
                return Err(fail(format!(
                    "{what}: connection \"{connection}\" is kind = \"{connection_kind}\", \
                     which does not read {kind} URLs"
                )));
            }
            if let (Some(account), Some((url_account, _, _))) = (
                configured.account.as_deref(),
                crate::source::azure_parts(url),
            ) && !account.eq_ignore_ascii_case(&url_account)
            {
                return Err(fail(format!(
                    "{what}: connection \"{connection}\" signs in to account \"{account}\", \
                     but the URL is in \"{url_account}\""
                )));
            }
        }
        Ok(())
    }

    /// The file's name for errors: its path, `public` for the bundled one, or the
    /// file its id names.
    pub fn file_name(&self) -> String {
        match (&self.file, self.origin) {
            (Some(file), _) => file.display().to_string(),
            (None, Origin::Bundled) => PUBLIC.to_string(),
            (None, _) => format!("{}.toml", self.id),
        }
    }

    /// The dataset at `location`, by place.
    pub fn dataset_at(&self, location: &Path) -> Option<&Dataset> {
        let key = place_key_of(location);
        self.datasets.iter().find(|d| d.place_key() == key)
    }
}

/// The place key of a path or URL, as [`Dataset::place_key`] makes one.
pub fn place_key_of(location: &Path) -> String {
    let text = location.to_string_lossy();
    if matches!(
        crate::source::input_source(location),
        crate::source::InputSource::Local(_)
    ) {
        format!("path:{}", crate::config::path_place(location).display())
    } else {
        url_key(&text)
    }
}

/// A URL's place key: the place, whichever source a `s3://<id>@bucket` spelling names.
fn url_key(url: &str) -> String {
    let (_, plain) = crate::source::split_source_id(url);
    format!("url:{}", crate::source::canonical_cloud_place(&plain))
}

/// The bundled `public` catalog.
pub fn bundled() -> Catalog {
    static CATALOG: std::sync::OnceLock<Catalog> = std::sync::OnceLock::new();
    CATALOG
        .get_or_init(|| {
            parse(BUNDLED, PUBLIC, Origin::Bundled, None)
                .unwrap_or_else(|e| panic!("{}", e.in_file("public_catalog.toml")))
        })
        .clone()
}

/// The bundled catalog's text, comments and all.
pub fn bundled_text() -> &'static str {
    BUNDLED
}

/// Read the catalog in `file`. `None` when there is no such file.
pub fn read(file: &Path, id: &str, origin: Origin) -> color_eyre::Result<Option<Catalog>> {
    let text = match std::fs::read_to_string(file) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => {
            return Err(color_eyre::eyre::eyre!(
                "Failed to read catalog {}: {e}",
                file.display()
            ));
        }
    };
    parse(&text, id, origin, Some(file))
        .map(Some)
        .map_err(|e| color_eyre::eyre::eyre!("{}", e.in_file(&file.display().to_string())))
}

/// A listed catalog file's id: its file name without `.toml`.
pub fn id_of_file(file: &Path) -> String {
    file.file_stem()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_default()
}

/// A dataset id for `name`, not among `taken`: lowercase words joined by `-`.
pub fn id_for(name: &str, taken: &[&str]) -> String {
    let mut base = String::new();
    for c in name.chars() {
        if c.is_ascii_alphanumeric() {
            base.push(c.to_ascii_lowercase());
        } else if !base.ends_with('-') && !base.is_empty() {
            base.push('-');
        }
    }
    let mut base: String = base.trim_end_matches('-').chars().take(34).collect();
    base = base.trim_end_matches('-').to_string();
    if base.is_empty() {
        base = "dataset".to_string();
    }
    if !taken.contains(&base.as_str()) {
        return base;
    }
    (2..)
        .map(|n| format!("{base}-{n}"))
        .find(|id| !taken.contains(&id.as_str()))
        .expect("an unused id")
}

/// What Ctrl+D writes for a row.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct NewDataset {
    pub name: String,
    pub path: Option<String>,
    pub url: Option<String>,
    pub auth: Option<String>,
    pub connection: Option<String>,
    pub description: String,
    pub size: Option<u64>,
}

impl NewDataset {
    /// The dataset this would be in a catalog in `dir`.
    fn as_dataset(&self, dir: Option<&Path>) -> Dataset {
        Dataset {
            id: "new".to_string(),
            name: self.name.trim().to_string(),
            path: self.path.clone(),
            url: self.url.clone(),
            auth: self.auth.clone(),
            connection: self.connection.clone(),
            description: self.description.clone(),
            size: self.size,
            base: dir.map(Path::to_path_buf),
            ..Default::default()
        }
    }

    /// Why this cannot be a catalog entry, if it cannot.
    pub fn check(&self) -> Result<(), String> {
        check_dataset(&self.as_dataset(None))
            .map_err(|e| e.strip_prefix("[new]: ").map(str::to_string).unwrap_or(e))
    }

    /// Where it is, as a place key.
    pub fn place_key(&self) -> String {
        self.as_dataset(None).place_key()
    }
}

/// What a new `catalog.toml` starts with.
pub const MINE_TEMPLATE: &str = "\
# Your catalog: datasets and directories the home screen lists under its label.
# Ctrl+D on a home row adds it here, and on a row from this file forgets it. Your own
# edits, comments and layout are kept. `datui catalog check` checks the file.
#
# Each [table] is one dataset; its key is a short id (lowercase letters, digits, -).
#
#   [sales]
#   name = \"Sales\"
#   path = \"~/datasets/sales.parquet\"
#   description = \"Monthly sales\"
#   columns.amount = { description = \"Net of returns\", unit = \"USD\" }

label = \"My datasets\"
";

/// Write `text` to `file` whole: into a sibling, then renamed over it, so a reader
/// never sees half a file.
fn write_whole(file: &Path, text: &str) -> color_eyre::Result<()> {
    if let Some(dir) = file.parent() {
        std::fs::create_dir_all(dir)?;
    }
    // Its own temporary name, so two datui writing at once cannot share one.
    let mut temp = file.as_os_str().to_owned();
    temp.push(format!(".{}.tmp", std::process::id()));
    let temp = PathBuf::from(temp);
    std::fs::write(&temp, text)?;
    if let Err(e) = std::fs::rename(&temp, file) {
        let _ = std::fs::remove_file(&temp);
        return Err(e.into());
    }
    Ok(())
}

/// Append `dataset` to the catalog in `file`, creating the file when there is none.
/// Returns its id. The rest of the file is kept as written.
pub fn add(file: &Path, dataset: &NewDataset) -> color_eyre::Result<String> {
    let text = match std::fs::read_to_string(file) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => MINE_TEMPLATE.to_string(),
        Err(e) => return Err(e.into()),
    };
    // A file that does not read is not written over: the user's text is in it.
    let existing = parse(&text, MINE, Origin::Mine, Some(file))
        .map_err(|e| color_eyre::eyre::eyre!("{}", e.in_file(&file.display().to_string())))?;
    let candidate = dataset.as_dataset(file.parent());
    check_dataset(&candidate).map_err(|e| color_eyre::eyre::eyre!("{e}"))?;
    if let Some(there) = existing
        .datasets
        .iter()
        .find(|d| d.place_key() == candidate.place_key())
    {
        return Err(color_eyre::eyre::eyre!(
            "{} is in the catalog already, as {}",
            candidate.location_text(),
            there.name
        ));
    }
    // Names are unique in a catalog: a second `data.csv` is named by where it is.
    let mut name = candidate.name.clone();
    if existing.datasets.iter().any(|d| d.name == name) {
        name = candidate.location_text().to_string();
    }
    let mut doc: toml_edit::DocumentMut = text.parse()?;
    // The top-level keys are not ids either.
    let mut taken: Vec<String> = doc.iter().map(|(k, _)| k.to_string()).collect();
    taken.extend(["label".to_string(), "description".to_string()]);
    let taken: Vec<&str> = taken.iter().map(String::as_str).collect();
    let id = id_for(&dataset.name, &taken);
    let mut table = toml_edit::Table::new();
    table.insert("name", toml_edit::value(name.as_str()));
    for (key, value) in [
        ("path", &dataset.path),
        ("url", &dataset.url),
        ("auth", &dataset.auth),
        ("connection", &dataset.connection),
    ] {
        if let Some(value) = value {
            table.insert(key, toml_edit::value(value.as_str()));
        }
    }
    if let Some(size) = dataset.size {
        table.insert("size", toml_edit::value(size as i64));
    }
    if !dataset.description.is_empty() {
        table.insert(
            "description",
            toml_edit::value(dataset.description.as_str()),
        );
    }
    table.decor_mut().set_prefix("\n");
    doc.insert(&id, toml_edit::Item::Table(table));
    let mut out = doc.to_string();
    if !out.ends_with('\n') {
        out.push('\n');
    }
    // Never write a file the next start cannot read.
    parse(&out, MINE, Origin::Mine, Some(file)).map_err(|e| {
        color_eyre::eyre::eyre!("not added: {}", e.in_file(&file.display().to_string()))
    })?;
    write_whole(file, &out)?;
    Ok(id)
}

/// Remove the dataset `id` from the catalog in `file`: its table and every table under
/// it, and nothing else.
pub fn forget(file: &Path, id: &str) -> color_eyre::Result<()> {
    let text = std::fs::read_to_string(file)?;
    let mut doc: toml_edit::DocumentMut = text.parse()?;
    if doc.remove(id).is_none() {
        return Err(color_eyre::eyre::eyre!("{} has no [{id}]", file.display()));
    }
    write_whole(file, &doc.to_string())
}

/// Add each directory of `places` to the catalog in `file`, named by where it is, unless
/// the file lists it already: what Ctrl+D kept in the cache before 0.4.0. Returns how
/// many were added.
pub fn move_places(file: &Path, places: &[PathBuf]) -> color_eyre::Result<usize> {
    let mut added = 0;
    for place in places {
        let listed = match read(file, MINE, Origin::Mine)? {
            Some(catalog) => catalog.dataset_at(place).is_some(),
            None => false,
        };
        if listed {
            continue;
        }
        let text = crate::home::display_path(place);
        add(
            file,
            &NewDataset {
                name: text.clone(),
                path: Some(text),
                ..Default::default()
            },
        )?;
        added += 1;
    }
    Ok(added)
}

/// Columns padded to their widest cell, two spaces apart.
fn table(rows: &[Vec<String>]) -> String {
    let columns = rows.first().map(Vec::len).unwrap_or(0);
    let widths: Vec<usize> = (0..columns)
        .map(|c| rows.iter().map(|r| r[c].chars().count()).max().unwrap_or(0))
        .collect();
    let mut out = String::new();
    for row in rows {
        let mut line = String::new();
        for (c, cell) in row.iter().enumerate() {
            if c + 1 == row.len() {
                line.push_str(cell);
            } else {
                line.push_str(&format!("{cell:<w$}  ", w = widths[c]));
            }
        }
        out.push_str(line.trim_end());
        out.push('\n');
    }
    out
}

/// What `datui catalog ACTION` prints, and its exit code. `config` is the configuration
/// in effect, or why it could not be read.
pub fn command(
    action: &datui_cli::CatalogAction,
    config: color_eyre::Result<crate::config::AppConfig>,
) -> (String, i32) {
    use datui_cli::CatalogAction;
    use datui_cli::exit::{FAILURE, SUCCESS};
    match action {
        CatalogAction::Show { name } => {
            let config = match config {
                Ok(config) => config,
                Err(e) => return (format!("{e}\n"), FAILURE),
            };
            let catalogs = config.catalogs();
            let Some(name) = name else {
                let mut rows = vec![vec![
                    "ID".to_string(),
                    "LABEL".to_string(),
                    "DATASETS".to_string(),
                    "FILE".to_string(),
                ]];
                for catalog in &catalogs {
                    let hidden = if config.home.hide.contains(&catalog.id) {
                        " (hidden)"
                    } else {
                        ""
                    };
                    rows.push(vec![
                        catalog.id.clone(),
                        format!("{}{hidden}", catalog.label),
                        catalog.datasets.len().to_string(),
                        catalog
                            .file
                            .as_deref()
                            .map(|f| f.display().to_string())
                            .unwrap_or_else(|| "built in".to_string()),
                    ]);
                }
                return (table(&rows), SUCCESS);
            };
            if name == MINE && !catalogs.iter().any(|c| c.id == MINE) {
                return (
                    format!(
                        "No {MINE_FILE} yet. Ctrl+D on a home row, or datui config init, \
                         writes it\n"
                    ),
                    FAILURE,
                );
            }
            let Some(catalog) = catalogs.iter().find(|c| c.id == *name) else {
                let ids: Vec<&str> = catalogs.iter().map(|c| c.id.as_str()).collect();
                return (
                    format!("No catalog is named {name}. Catalogs: {}\n", ids.join(", ")),
                    FAILURE,
                );
            };
            match &catalog.file {
                None => (bundled_text().to_string(), SUCCESS),
                Some(file) => match std::fs::read_to_string(file) {
                    Ok(text) => (text, SUCCESS),
                    Err(e) => (format!("{}: {e}\n", file.display()), FAILURE),
                },
            }
        }
        CatalogAction::Check { file } => {
            let text = match std::fs::read_to_string(file) {
                Ok(text) => text,
                Err(e) => return (format!("{}: {e}\n", file.display()), FAILURE),
            };
            let name = file.display().to_string();
            let origin = if file.file_name().is_some_and(|n| n == MINE_FILE) {
                Origin::Mine
            } else {
                Origin::Listed
            };
            let parsed = parse(&text, &id_of_file(file), origin, Some(file)).and_then(|c| {
                // Connections are the config's: with no config to read, they go unchecked.
                match &config {
                    Ok(config) => c.check_connections(&config.cloud.connections).map(|()| c),
                    Err(_) => Ok(c),
                }
            });
            match parsed {
                Err(e) => (format!("{}\n", e.in_file(&name)), FAILURE),
                Ok(catalog) => {
                    let mut out = format!(
                        "{name}: {}, {} dataset{}\n",
                        catalog.label,
                        catalog.datasets.len(),
                        if catalog.datasets.len() == 1 { "" } else { "s" }
                    );
                    let rows: Vec<Vec<String>> = catalog
                        .datasets
                        .iter()
                        .map(|d| {
                            vec![
                                format!("  {}", d.id),
                                d.name.clone(),
                                d.location_text().to_string(),
                            ]
                        })
                        .collect();
                    out.push_str(&table(&rows));
                    (out, SUCCESS)
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn error_of(text: &str) -> String {
        parse(text, "t", Origin::Listed, None)
            .unwrap_err()
            .in_file("t.toml")
    }

    #[test]
    fn the_bundled_catalog_reads_in_file_order() {
        let catalog = bundled();
        assert_eq!(catalog.label, "Public datasets");
        assert_eq!(catalog.datasets[0].id, "nyc-flights");
        let noaa = catalog.datasets.iter().find(|d| d.id == "noaa").unwrap();
        assert_eq!(noaa.columns[0].0, "ID");
        let element = &noaa.columns.iter().find(|(c, _)| c == "ELEMENT").unwrap().1;
        assert_eq!(element.values[0].0, "PRCP");
        assert_eq!(noaa.bookmarks[0].0, "Daily highs, 2024");
    }

    #[test]
    fn a_values_table_after_an_inline_column_says_how_to_write_it() {
        let text = "[w]\nname = \"W\"\nurl = \"s3://b/w/\"\n\
                    columns.FLAG = { description = \"Flag\" }\n\n\
                    [w.columns.FLAG.values]\nS = \"spatial\"\n";
        let error = error_of(text);
        assert!(error.starts_with("t.toml:6: "), "{error}");
        assert!(
            error.contains("columns.FLAG.description = \"...\""),
            "{error}"
        );
        let dotted = "[w]\nname = \"W\"\nurl = \"s3://b/w/\"\n\
                      columns.FLAG.description = \"Flag\"\n\n\
                      [w.columns.FLAG.values]\nS = \"spatial\"\n";
        let catalog = parse(dotted, "t", Origin::Listed, None).unwrap();
        assert_eq!(
            catalog.datasets[0].columns[0].1.values,
            [("S".to_string(), "spatial".to_string())]
        );
    }

    #[test]
    fn a_mistake_is_named_at_its_line_with_the_fix() {
        assert_eq!(
            error_of("label = \"x\"\n\n[a]\nname = \"A\"\n"),
            "t.toml:3: [a]: say where it is with path or url"
        );
        let error = error_of("[a]\nname = \"A\"\npath = \"/x\"\ncodebook = \"https://x\"\n");
        assert!(error.starts_with("t.toml:4: "), "{error}");
        assert!(error.contains("documentation"), "{error}");
        let error = error_of("[\"A b\"]\nname = \"A\"\npath = \"/x\"\n");
        assert!(error.contains("lowercase"), "{error}");
        let error =
            error_of("[a]\nname = \"A\"\npath = \"/x\"\n[b]\nname = \"A\"\npath = \"/y\"\n");
        assert!(error.starts_with("t.toml:4: [b]: name \"A\""), "{error}");
        let error =
            error_of("[a]\nname = \"A\"\npath = \"/x\"\n[b]\nname = \"B\"\npath = \"/x/\"\n");
        assert!(error.contains("listed as [a]"), "{error}");
        let error = error_of(
            "[a]\nname = \"A\"\nurl = \"https://x.org/a.csv\"\nbookmarks.\"B\" = \"b/\"\n",
        );
        assert!(error.contains("bookmarks apply only"), "{error}");
        let error = error_of("[a]\nname = \"A\"\npath = \"/x\"\nbookmarks.\"Up\" = \"../b/\"\n");
        assert!(error.contains("stay inside"), "{error}");
        let error = error_of("stray = 1\n");
        assert!(error.starts_with("t.toml:1: "), "{error}");
    }

    #[test]
    fn ids_come_from_names_and_never_repeat() {
        assert_eq!(
            id_for("NOAA daily weather (GHCN-D)", &[]),
            "noaa-daily-weather-ghcn-d"
        );
        assert_eq!(id_for("~/Downloads", &[]), "downloads");
        assert_eq!(id_for("Sales", &["sales"]), "sales-2");
        assert_eq!(id_for("Sales", &["sales", "sales-2"]), "sales-3");
        assert_eq!(id_for("→", &[]), "dataset");
        assert!(is_valid_source_id(&id_for(&"x".repeat(80), &[])));
    }

    #[test]
    fn adding_and_forgetting_keep_the_rest_of_the_file() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join(MINE_FILE);
        let id = add(
            &file,
            &NewDataset {
                name: "Sales".into(),
                path: Some("/data/sales.parquet".into()),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(id, "sales");
        let mut text = std::fs::read_to_string(&file).unwrap();
        assert!(text.starts_with("# Your catalog"), "{text}");
        text.push_str("\n# kept\n[noaa]\nname = \"NOAA\"\nurl = \"s3://noaa-ghcn-pds/parquet/\"\ncolumns.ELEMENT.description = \"What\"\n\n[noaa.columns.ELEMENT.values]\nTMAX = \"High\"\n");
        std::fs::write(&file, &text).unwrap();
        let again = add(
            &file,
            &NewDataset {
                name: "Sales".into(),
                path: Some("/data/sales2.parquet".into()),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(again, "sales-2");
        forget(&file, "noaa").unwrap();
        let text = std::fs::read_to_string(&file).unwrap();
        assert!(!text.contains("noaa"), "{text}");
        assert!(text.contains("# Your catalog"), "{text}");
        let catalog = read(&file, MINE, Origin::Mine).unwrap().unwrap();
        let ids: Vec<&str> = catalog.datasets.iter().map(|d| d.id.as_str()).collect();
        assert_eq!(ids, ["sales", "sales-2"]);
        assert_eq!(catalog.label, MINE_LABEL);
    }

    #[test]
    fn remembered_places_move_into_the_catalog_once() {
        let cache_dir = tempfile::tempdir().unwrap();
        let config_dir = tempfile::tempdir().unwrap();
        let cache = crate::cache::CacheManager::with_dir(cache_dir.path().to_path_buf());
        let places = [PathBuf::from("/data/lake"), PathBuf::from("/mnt/nas/share")];
        cache.save_remembered_places(&places).unwrap();
        let taken = cache.load_remembered_places();
        assert_eq!(taken, places);
        let file = config_dir.path().join(MINE_FILE);
        assert_eq!(move_places(&file, &taken).unwrap(), 2);
        assert_eq!(move_places(&file, &taken).unwrap(), 0, "never twice");
        let catalog = read(&file, MINE, Origin::Mine).unwrap().unwrap();
        let paths: Vec<PathBuf> = catalog.datasets.iter().map(Dataset::location).collect();
        assert_eq!(paths, places);
    }

    #[test]
    fn show_prints_the_bundled_file_and_check_names_the_line() {
        use datui_cli::CatalogAction;
        let config = crate::config::AppConfig::default();
        let (text, code) = command(
            &CatalogAction::Show {
                name: Some(PUBLIC.into()),
            },
            Ok(config.clone()),
        );
        assert_eq!(code, 0);
        assert_eq!(text, BUNDLED);
        let (list, code) = command(&CatalogAction::Show { name: None }, Ok(config.clone()));
        assert_eq!(code, 0);
        assert!(
            list.contains("public") && list.contains("built in"),
            "{list}"
        );
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("team.toml");
        std::fs::write(
            &file,
            "label = \"Team\"\n\n[a]\nname = \"A\"\npath = \"a.csv\"\n",
        )
        .unwrap();
        let (text, code) = command(
            &CatalogAction::Check { file: file.clone() },
            Ok(config.clone()),
        );
        assert_eq!(code, 0, "{text}");
        assert!(text.contains("Team, 1 dataset"), "{text}");
        std::fs::write(
            &file,
            "[a]\nname = \"A\"\nurl = \"s3://b/a/\"\nconnection = \"lab\"\n",
        )
        .unwrap();
        let (text, code) = command(&CatalogAction::Check { file: file.clone() }, Ok(config));
        assert_eq!(code, 1);
        assert!(
            text.contains("team.toml:1: [a]: no [[cloud.connections]]"),
            "{text}"
        );
    }

    #[test]
    fn an_added_row_never_breaks_the_file() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join(MINE_FILE);
        let new = |name: &str, path: &str| NewDataset {
            name: name.into(),
            path: Some(path.into()),
            ..Default::default()
        };
        // Not the top level's keys.
        assert_eq!(
            add(&file, &new("Description", "/d")).unwrap(),
            "description-2"
        );
        assert_eq!(add(&file, &new("Label", "/l")).unwrap(), "label-2");
        // A name that is another's once trimmed is named by where it is.
        add(&file, &new("Sales", "/a.csv")).unwrap();
        add(&file, &new(" Sales ", "/b.csv")).unwrap();
        let catalog = read(&file, MINE, Origin::Mine).unwrap().unwrap();
        let names: Vec<&str> = catalog.datasets.iter().map(|d| d.name.as_str()).collect();
        assert_eq!(names, ["Description", "Label", "Sales", "/b.csv"]);
    }

    #[test]
    fn a_url_through_a_named_source_is_the_same_place() {
        assert_eq!(
            place_key_of(Path::new("s3://lab@bucket/dir/")),
            place_key_of(Path::new("s3://bucket/dir"))
        );
    }

    #[test]
    fn a_relative_path_is_relative_to_the_catalog() {
        let catalog = parse(
            "[a]\nname = \"A\"\npath = \"data/a.csv\"\n",
            "t",
            Origin::Listed,
            Some(Path::new("/team/t.toml")),
        )
        .unwrap();
        assert_eq!(
            catalog.datasets[0].local_path().unwrap(),
            Path::new("/team/data/a.csv")
        );
    }
}

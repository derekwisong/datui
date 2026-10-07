//! Input source detection for local paths vs remote URLs (S3, GCS, Azure, HTTP/HTTPS).

use std::path::{Path, PathBuf};

/// Which API a provider speaks. Not which company runs it: MinIO, Ceph, R2 and AWS
/// itself are all [`ProviderKind::S3`], and are told apart by their endpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ProviderKind {
    Gcs,
    #[default]
    S3,
    Azure,
}

impl ProviderKind {
    /// The URL scheme datui opens this provider's objects with.
    pub fn scheme(self) -> &'static str {
        match self {
            ProviderKind::Gcs => "gs",
            ProviderKind::S3 => "s3",
            ProviderKind::Azure => "abfss",
        }
    }

    /// The word for the API, as the config and the home screen write it.
    pub fn name(self) -> &'static str {
        match self {
            ProviderKind::Gcs => "gcs",
            ProviderKind::S3 => "s3",
            ProviderKind::Azure => "azure",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InputSource {
    Local(PathBuf),
    S3(String),
    Gcs(String),
    /// Azure Blob Storage, always as `abfss://container@account.dfs.core.windows.net/path`
    /// whichever of its forms it was written in.
    Azure(String),
    Http(String),
}

/// Classifies the path as local, S3, GCS, or HTTP/HTTPS using string parsing only (no filesystem calls).
pub fn input_source(path: &Path) -> InputSource {
    let s = path.as_os_str().to_string_lossy();
    if let Some(after_scheme) = s.find("://") {
        let prefix = s[..after_scheme].to_lowercase();
        let rest = s[after_scheme + 3..].to_string();
        if prefix == "s3" || prefix == "s3a" {
            return InputSource::S3(rest);
        }
        if prefix == "gs" || prefix == "gcs" {
            return InputSource::Gcs(rest);
        }
        if let Some((account, container, key)) = azure_parts(&s) {
            return InputSource::Azure(azure_url(&account, &container, &key));
        }
        if prefix == "http" || prefix == "https" {
            return InputSource::Http(s.to_string());
        }
    }
    InputSource::Local(path.to_path_buf())
}

/// Whether `path` is a URL datui reads rather than a path on this machine: whatever
/// [`input_source`] places remotely, and the Azure forms that name no account, which
/// are expanded when opened.
pub fn is_remote_url(path: &Path) -> bool {
    !matches!(input_source(path), InputSource::Local(_))
        || path
            .to_string_lossy()
            .split_once("://")
            .is_some_and(|(scheme, _)| is_azure_short_scheme(scheme))
}

/// Whether this build can open `path`: object stores need the `cloud` feature and web
/// URLs the `http` feature; a local path always opens.
pub fn opens_in_this_build(path: &Path) -> bool {
    match input_source(path) {
        InputSource::Local(_) if is_remote_url(path) => cfg!(feature = "cloud"),
        InputSource::Local(_) => true,
        InputSource::S3(_) | InputSource::Gcs(_) | InputSource::Azure(_) => {
            cfg!(feature = "cloud")
        }
        InputSource::Http(_) => cfg!(feature = "http"),
    }
}

/// `az`, `adl` and `azure`: Azure schemes that name a container but no account.
pub(crate) fn is_azure_short_scheme(scheme: &str) -> bool {
    matches!(scheme.to_ascii_lowercase().as_str(), "az" | "adl" | "azure")
}

/// The source an `s3://<id>@bucket/key` URL names, and the URL without it.
///
/// Only S3 URLs carry a source, and only for S3-compatible servers, whose bucket names
/// repeat from one endpoint to the next. Bucket names cannot contain `@`, so an `@` in
/// the first segment is always a source. Everything else comes back unchanged.
pub fn split_source_id(url: &str) -> (Option<&str>, std::borrow::Cow<'_, str>) {
    let Some((scheme, rest)) = url.split_once("://") else {
        return (None, url.into());
    };
    if !matches!(scheme.to_ascii_lowercase().as_str(), "s3" | "s3a") {
        return (None, url.into());
    }
    let first = rest.split('/').next().unwrap_or(rest);
    match first.split_once('@') {
        Some((id, _)) if !id.is_empty() => {
            let plain = format!("{scheme}://{}", &rest[id.len() + 1..]);
            (Some(id), plain.into())
        }
        _ => (None, url.into()),
    }
}

/// The account, container and path of an Azure Blob Storage URL.
///
/// Accepts the forms that name the account: `abfss://` and `abfs://`
/// (`container@account.dfs.core.windows.net/path`), and `https://` on the blob or dfs
/// endpoint (`account.blob.core.windows.net/container/path`). `az://container/path`
/// does not name the account, so it is not one of them. The path comes back without a
/// leading slash, and a trailing slash is kept, since it is what marks a directory.
pub fn azure_parts(url: &str) -> Option<(String, String, String)> {
    let (scheme, rest) = url.split_once("://")?;
    let scheme = scheme.to_ascii_lowercase();
    let (host_part, path) = match rest.split_once('/') {
        Some((host, path)) => (host, path),
        None => (rest, ""),
    };
    let account_of = |host: &str| {
        let host = host.to_ascii_lowercase();
        [".dfs.core.windows.net", ".blob.core.windows.net"]
            .iter()
            .find_map(|suffix| host.strip_suffix(suffix).map(str::to_string))
            .filter(|account| !account.is_empty() && !account.contains('.'))
    };
    match scheme.as_str() {
        "abfss" | "abfs" => {
            let (container, host) = host_part.split_once('@')?;
            let account = account_of(host)?;
            (!container.is_empty()).then(|| (account, container.to_string(), path.to_string()))
        }
        "https" | "http" => {
            let account = account_of(host_part)?;
            let (container, path) = match path.split_once('/') {
                Some((container, path)) => (container, path),
                None => (path, ""),
            };
            (!container.is_empty()).then(|| (account, container.to_string(), path.to_string()))
        }
        _ => None,
    }
}

/// The canonical URL for a place in Azure Blob Storage.
pub fn azure_url(account: &str, container: &str, path: &str) -> String {
    format!(
        "abfss://{container}@{account}.dfs.core.windows.net/{}",
        path.trim_start_matches('/')
    )
}

/// A cloud place in the form used for equality comparisons. Equivalent Azure URL
/// forms become `abfss://`, and a trailing slash does not distinguish a place.
pub(crate) fn canonical_cloud_place(url: &str) -> String {
    let canonical = match azure_parts(url) {
        Some((account, container, path)) => azure_url(&account, &container, &path),
        None => url.to_string(),
    };
    canonical.trim_end_matches('/').to_string()
}

/// A cloud location whose shape is a prefix or a glob rather than one object.
pub(crate) fn is_prefix_or_glob(url: &str) -> bool {
    url.ends_with('/') || url.contains('*')
}

/// The characters Polars expands a path on (`polars_io::path_utils::has_glob`).
pub(crate) fn has_glob_chars(path: &Path) -> bool {
    path.as_os_str().to_string_lossy().contains(['*', '?', '['])
}

/// Whether a path is a pattern to expand rather than a name: it carries a glob
/// character and nothing on disk has that name. An existing `d[1].csv` or `a*b.csv`
/// is that file; read as a glob, `d[1].csv` is `d1.csv` and `x?.csv` is every
/// two-letter name. This is the `glob` flag for every Polars scan of a local path.
pub(crate) fn expands_as_glob(path: &Path) -> bool {
    has_glob_chars(path) && std::fs::symlink_metadata(path).is_err()
}

/// A path for a Polars reader that always expands globs (its NDJSON scan has no
/// `glob` flag): an existing name with a glob character comes back escaped, so the
/// pattern matches that file alone.
pub(crate) fn polars_literal_path(
    path: &Path,
) -> polars::prelude::PolarsResult<polars::prelude::PlRefPath> {
    if !has_glob_chars(path) || expands_as_glob(path) {
        return polars::prelude::PlRefPath::try_from_path(path);
    }
    // Escaped as Polars will read it: on Windows that text has `/` separators and no
    // `\\?\` prefix, so neither a separator nor the prefix's `?` is touched.
    let text = polars::prelude::PlRefPath::try_from_path(path)?;
    Ok(polars::prelude::PlRefPath::new(
        escape_glob(text.as_str()).as_str(),
    ))
}

/// `text` as a glob that matches only itself: each glob character in brackets.
pub(crate) fn escape_glob(text: &str) -> String {
    let mut escaped = String::with_capacity(text.len() + 8);
    for c in text.chars() {
        if matches!(c, '*' | '?' | '[' | ']') {
            escaped.extend(['[', c, ']']);
        } else {
            escaped.push(c);
        }
    }
    escaped
}

/// True when the path names an object-store location datui scans in place, with range
/// requests, rather than downloads to a temporary file first: Parquet, or a prefix or
/// glob of it. A downloaded object reaches the schema phase under its display URL, and
/// this is what keeps it from being treated as a remote scan.
pub(crate) fn scans_in_place(path: &Path) -> bool {
    if !matches!(
        input_source(path),
        InputSource::S3(_) | InputSource::Gcs(_) | InputSource::Azure(_)
    ) {
        return false;
    }
    let url = path.to_string_lossy();
    let (_, ext) = url_path_extension(&url);
    !cloud_path_should_download(ext.as_deref(), is_prefix_or_glob(&url))
}

/// Returns the path segment and file extension for URL format inference.
/// For S3, path part is everything after `://` (bucket/key). For HTTP/HTTPS, path part is the URL path only (host stripped).
pub(crate) fn url_path_extension(url: &str) -> (String, Option<String>) {
    let path_part = if let Some(i) = url.find("://") {
        let scheme = url[..i].to_lowercase();
        let after = &url[i + 3..];
        if scheme == "http" || scheme == "https" {
            after
                .find('/')
                .map(|j| after[j + 1..].to_string())
                .unwrap_or_default()
        } else {
            after.to_string()
        }
    } else {
        String::new()
    };
    let last_segment = path_part.rsplit('/').next().unwrap_or(&path_part);
    let ext = std::path::Path::new(last_segment)
        .extension()
        .and_then(|e| e.to_str())
        .map(String::from);
    (path_part, ext)
}

/// The extension a downloaded copy of `url` should keep, so it opens as what it is:
/// `csv.gz` rather than `gz` for a compressed file, since a temporary `.gz` does not
/// say what is inside it.
#[cfg(any(feature = "http", feature = "cloud"))]
pub(crate) fn download_suffix(url: &str) -> Option<String> {
    let (path_part, ext) = url_path_extension(url);
    let ext = ext?;
    const COMPRESSION: [&str; 6] = ["gz", "zst", "bz2", "xz", "lz4", "zip"];
    if !COMPRESSION.iter().any(|c| ext.eq_ignore_ascii_case(c)) {
        return Some(ext);
    }
    let name = path_part.rsplit('/').next().unwrap_or(&path_part);
    let stem = &name[..name.len() - ext.len() - 1];
    match Path::new(stem).extension().and_then(|e| e.to_str()) {
        Some(inner) => Some(format!("{inner}.{ext}")),
        None => Some(ext),
    }
}

/// For S3/GCS: Polars can only scan Parquet directly. So we pass through only when the path is
/// Parquet or looks like a directory/glob (no extension, trailing slash, or *). All other paths
/// (e.g. .csv, .json, .gz, .csv.gz) must be downloaded first.
/// Returns true when the path should be downloaded to temp instead of passed to Polars.
pub(crate) fn cloud_path_should_download(ext: Option<&str>, is_glob: bool) -> bool {
    if is_glob {
        return false;
    }
    match ext {
        None => false,
        Some(e) => !e.eq_ignore_ascii_case("parquet"),
    }
}

#[cfg(test)]
mod tests;

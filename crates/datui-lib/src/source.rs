//! Input source detection for local paths vs remote URLs (S3, GCS, HTTP/HTTPS).

use std::path::{Path, PathBuf};

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
/// leading slash, and a trailing slash is kept, since it is what marks a folder.
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

/// A cloud location whose shape is a prefix or a glob rather than one object.
pub(crate) fn is_prefix_or_glob(url: &str) -> bool {
    url.ends_with('/') || url.contains('*')
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
mod tests {
    use super::*;

    #[test]
    fn input_source_local_path() {
        let p = PathBuf::from("/tmp/file.parquet");
        assert!(matches!(input_source(&p), InputSource::Local(_)));
        let p = PathBuf::from("relative.csv");
        assert!(matches!(input_source(&p), InputSource::Local(_)));
        let p = PathBuf::from(".");
        assert!(matches!(input_source(&p), InputSource::Local(_)));
    }

    #[test]
    fn input_source_s3() {
        let p = PathBuf::from("s3://bucket/key.parquet");
        match input_source(&p) {
            InputSource::S3(rest) => assert_eq!(rest, "bucket/key.parquet"),
            _ => panic!("expected S3"),
        }
        let p = PathBuf::from("S3://my-bucket/path/to/file.csv");
        match input_source(&p) {
            InputSource::S3(rest) => assert_eq!(rest, "my-bucket/path/to/file.csv"),
            _ => panic!("expected S3"),
        }
    }

    #[test]
    fn a_source_is_split_off_s3_urls_only() {
        assert_eq!(
            split_source_id("s3://onprem@sales/2024/q3.parquet"),
            (Some("onprem"), "s3://sales/2024/q3.parquet".into())
        );
        assert_eq!(
            split_source_id("s3://onprem@sales"),
            (Some("onprem"), "s3://sales".into())
        );
        assert_eq!(
            split_source_id("s3://sales/a@b.parquet"),
            (None, "s3://sales/a@b.parquet".into())
        );
        assert_eq!(
            split_source_id("gs://bucket/key"),
            (None, "gs://bucket/key".into())
        );
        assert_eq!(
            split_source_id("https://user@host/file.csv"),
            (None, "https://user@host/file.csv".into())
        );
        assert_eq!(split_source_id("s3://@sales"), (None, "s3://@sales".into()));
    }

    #[test]
    fn azure_urls_in_every_form_that_names_the_account_become_one() {
        let canonical = "abfss://datui-test@datalake001.dfs.core.windows.net/demo/fred/";
        for url in [
            canonical,
            "abfs://datui-test@datalake001.dfs.core.windows.net/demo/fred/",
            "https://datalake001.blob.core.windows.net/datui-test/demo/fred/",
            "https://DataLake001.dfs.core.windows.net/datui-test/demo/fred/",
        ] {
            assert_eq!(
                input_source(Path::new(url)),
                InputSource::Azure(canonical.to_string()),
                "{url}"
            );
        }
        assert_eq!(
            azure_parts("abfss://c@acct.dfs.core.windows.net"),
            Some(("acct".to_string(), "c".to_string(), String::new()))
        );
        // No account, or not Azure at all.
        assert_eq!(azure_parts("az://container/path"), None);
        assert!(matches!(
            input_source(Path::new("https://example.com/c/x.csv")),
            InputSource::Http(_)
        ));
        assert!(scans_in_place(Path::new(
            "abfss://c@acct.dfs.core.windows.net/x.parquet"
        )));
    }

    #[test]
    fn input_source_http() {
        let p = PathBuf::from("https://example.com/data.parquet");
        match input_source(&p) {
            InputSource::Http(u) => assert_eq!(u, "https://example.com/data.parquet"),
            _ => panic!("expected Http"),
        }
        let p = PathBuf::from("http://host/path/file.csv");
        match input_source(&p) {
            InputSource::Http(u) => assert_eq!(u, "http://host/path/file.csv"),
            _ => panic!("expected Http"),
        }
    }

    #[test]
    fn input_source_gcs() {
        let p = PathBuf::from("gs://my-bucket/path/file.parquet");
        match input_source(&p) {
            InputSource::Gcs(rest) => assert_eq!(rest, "my-bucket/path/file.parquet"),
            _ => panic!("expected Gcs"),
        }
        let p = PathBuf::from("gcs://bucket/key.parquet");
        match input_source(&p) {
            InputSource::Gcs(rest) => assert_eq!(rest, "bucket/key.parquet"),
            _ => panic!("expected Gcs"),
        }
    }

    #[test]
    fn input_source_unknown_scheme_stays_local() {
        let p = PathBuf::from("file:///tmp/foo.parquet");
        assert!(matches!(input_source(&p), InputSource::Local(_)));
    }

    #[test]
    fn url_path_extension_s3() {
        let (path, ext) = url_path_extension("s3://bucket/key.parquet");
        assert_eq!(path, "bucket/key.parquet");
        assert_eq!(ext.as_deref(), Some("parquet"));
        let (path, ext) = url_path_extension("s3://b/path/to/file.csv");
        assert_eq!(path, "b/path/to/file.csv");
        assert_eq!(ext.as_deref(), Some("csv"));
    }

    #[test]
    fn a_download_keeps_what_the_compressed_file_holds() {
        assert_eq!(
            download_suffix("s3://b/edge/100%.csv.gz").as_deref(),
            Some("csv.gz")
        );
        assert_eq!(
            download_suffix("https://x.com/a/log.json.zst").as_deref(),
            Some("json.zst")
        );
        assert_eq!(download_suffix("gs://b/data.csv").as_deref(), Some("csv"));
        assert_eq!(download_suffix("s3://b/archive.gz").as_deref(), Some("gz"));
        assert_eq!(download_suffix("s3://b/no-extension"), None);
    }

    #[test]
    fn url_path_extension_https() {
        let (path, ext) = url_path_extension("https://example.com/dir/file.parquet");
        assert_eq!(path, "dir/file.parquet");
        assert_eq!(ext.as_deref(), Some("parquet"));
        let (_, ext) = url_path_extension("https://x.com/file.csv.gz");
        assert_eq!(ext.as_deref(), Some("gz"));
    }

    #[test]
    fn only_parquet_prefixes_and_globs_are_scanned_in_place() {
        assert!(scans_in_place(Path::new("s3://bucket/obj.parquet")));
        assert!(scans_in_place(Path::new("gs://bucket/prefix/")));
        assert!(scans_in_place(Path::new("s3://bucket/year=*/*.parquet")));
        // Downloaded first, then opened as a local file: not a remote scan.
        assert!(!scans_in_place(Path::new("s3://bucket/data.csv")));
        assert!(!scans_in_place(Path::new("s3://bucket/data.csv.gz")));
        assert!(!scans_in_place(Path::new(
            "https://example.com/data.parquet"
        )));
        assert!(!scans_in_place(Path::new("/data/local.parquet")));
    }

    #[test]
    fn cloud_path_should_download() {
        assert!(super::cloud_path_should_download(Some("csv"), false));
        assert!(super::cloud_path_should_download(Some("gz"), false));
        assert!(super::cloud_path_should_download(Some("csv.gz"), false));
        assert!(!super::cloud_path_should_download(Some("parquet"), false));
        assert!(!super::cloud_path_should_download(None, false));
        assert!(!super::cloud_path_should_download(Some("csv"), true));
        assert!(!super::cloud_path_should_download(Some("parquet"), true));
    }
}

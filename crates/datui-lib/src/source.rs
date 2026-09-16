//! Input source detection for local paths vs remote URLs (S3, GCS, HTTP/HTTPS).

use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InputSource {
    Local(PathBuf),
    S3(String),
    Gcs(String),
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

/// A cloud location whose shape is a prefix or a glob rather than one object.
pub(crate) fn is_prefix_or_glob(url: &str) -> bool {
    url.ends_with('/') || url.contains('*')
}

/// True when the path names an object-store location datui scans in place, with range
/// requests, rather than downloads to a temporary file first: Parquet, or a prefix or
/// glob of it. A downloaded object reaches the schema phase under its display URL, and
/// this is what keeps it from being treated as a remote scan.
pub(crate) fn scans_in_place(path: &Path) -> bool {
    if !matches!(input_source(path), InputSource::S3(_) | InputSource::Gcs(_)) {
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

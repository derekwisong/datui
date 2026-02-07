//! Input source detection for local paths vs remote URLs (S3, GCS, HTTP/HTTPS).

use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum InputSource {
    Local(PathBuf),
    S3(String),
    Gcs(String),
    Http(String),
}

/// Classifies the path as local, S3, GCS, or HTTP/HTTPS using string parsing only (no filesystem calls).
pub(crate) fn input_source(path: &Path) -> InputSource {
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

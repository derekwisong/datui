//! Remote SafeTensors and GGUF files, read by their headers alone.
//!
//! A checkpoint is tens of GB and its header a few MB, so a remote model is not
//! downloaded: its header is fetched with ranged requests ([`model_files::RangeSource`])
//! over HTTP(S), or from S3, GCS or Azure through the same store a scan uses. A
//! SafeTensors index names its shards beside its own URL, and a prefix in an object
//! store is read as the model files it holds, as a directory on disk is.
//!
//! An HTTP server that sends the whole file where a range was asked for answers
//! [`RangeError::NoRanges`], and the open downloads the file instead.

use std::path::Path;

use polars::prelude::LazyFrame;

use crate::FileFormat;
use crate::model_files::{self, ModelSummary, RangeError, RangeSource, Remote};
use crate::source::{self, InputSource};

/// The model at `url`: one file, an index, or (in an object store) a prefix holding
/// model files. `stop` is asked before each request; the open's own stop flag.
pub(crate) fn read(
    url: &Path,
    format: FileFormat,
    cloud: &crate::config::CloudConfig,
    runtime: &tokio::runtime::Handle,
    stop: &dyn Fn() -> bool,
) -> Result<(LazyFrame, ModelSummary), RangeError> {
    #[cfg(not(feature = "cloud"))]
    let _ = (cloud, runtime);
    match source::input_source(url) {
        #[cfg(feature = "http")]
        InputSource::Http(url) => {
            let agent: ureq::Agent = ureq::Agent::config_builder()
                .timeout_global(Some(std::time::Duration::from_secs(120)))
                .build()
                .into();
            let open = |url: &str| -> Result<Box<dyn RangeSource>, RangeError> {
                Ok(Box::new(Http {
                    agent: agent.clone(),
                    url: url.to_string(),
                }))
            };
            let remote = Remote {
                open: &open,
                sibling: &http_sibling,
                stop,
            };
            model_files::read_remote_model(&[url], format, &remote)
        }
        #[cfg(feature = "cloud")]
        InputSource::S3(_) | InputSource::Gcs(_) | InputSource::Azure(_) => {
            read_cloud(url, format, cloud, runtime, stop)
        }
        _ => Err(RangeError::Failed(format!(
            "{} is not a URL datui reads headers from in this build",
            url.display()
        ))),
    }
}

/// The format a remote `url` is read as by its header, if it is a model: `--format`
/// when given, else its name. A prefix only with the format, which the listing gave.
pub(crate) fn model_format(url: &Path, format: Option<FileFormat>) -> Option<FileFormat> {
    let is_model = |f: &FileFormat| matches!(f, FileFormat::Safetensors | FileFormat::Gguf);
    let text = url.to_string_lossy();
    let reachable = match source::input_source(url) {
        InputSource::Http(_) => cfg!(feature = "http") && !text.ends_with('/'),
        InputSource::S3(_) | InputSource::Gcs(_) | InputSource::Azure(_) => {
            cfg!(feature = "cloud") && !text.contains('*')
        }
        InputSource::Local(_) => false,
    };
    if !reachable {
        return None;
    }
    if text.ends_with('/') {
        return format.filter(is_model);
    }
    if let Some(format) = format {
        return Some(format).filter(is_model);
    }
    let name = model_files::url_file_name(&text);
    if model_files::is_safetensors_index(Path::new(name)) {
        return Some(FileFormat::Safetensors);
    }
    let (_, ext) = source::url_path_extension(text.split(['?', '#']).next().unwrap_or(&text));
    ext.as_deref()
        .and_then(FileFormat::from_extension)
        .filter(is_model)
}

/// A file named beside `url` on the same server, its name percent-encoded. The query
/// is not carried over: a signed URL's signature is for its own file.
#[cfg(feature = "http")]
fn http_sibling(url: &str, name: &str) -> String {
    let base = url.split(['?', '#']).next().unwrap_or(url);
    let dir = base.rsplit_once('/').map_or(base, |(dir, _)| dir);
    let mut encoded = String::with_capacity(name.len());
    for byte in name.bytes() {
        if byte.is_ascii_alphanumeric() || b"-._~".contains(&byte) {
            encoded.push(byte as char);
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
    }
    format!("{dir}/{encoded}")
}

/// One file on an HTTP server, read by `Range` requests.
#[cfg(feature = "http")]
struct Http {
    agent: ureq::Agent,
    url: String,
}

#[cfg(feature = "http")]
impl RangeSource for Http {
    fn get(&mut self, start: u64, end: u64) -> Result<(Vec<u8>, u64), RangeError> {
        use std::io::Read;
        let failed = |e: &dyn std::fmt::Display| {
            RangeError::Failed(format!("Could not read {}: {e}", self.url))
        };
        // Identity, so a length is the file's and not a compressed body's.
        let response = self
            .agent
            .get(&self.url)
            .header("Range", format!("bytes={start}-{}", end.saturating_sub(1)))
            .header("Accept-Encoding", "identity")
            .call()
            .map_err(|e| failed(&e))?;
        let header = |name: &str| {
            response
                .headers()
                .get(name)
                .and_then(|v| v.to_str().ok())
                .map(str::to_string)
        };
        let len = match response.status().as_u16() {
            206 => {
                // `bytes a-b/len`: the range must start where it was asked to, and the
                // whole length must be known for the header's bounds to mean anything.
                let range = header("Content-Range").unwrap_or_default();
                let parsed = range
                    .strip_prefix("bytes ")
                    .and_then(|r| r.split_once('/'))
                    .and_then(|(span, len)| {
                        let (from, _) = span.split_once('-')?;
                        Some((
                            from.trim().parse::<u64>().ok()?,
                            len.trim().parse::<u64>().ok()?,
                        ))
                    });
                match parsed {
                    Some((from, len)) if from == start => len,
                    Some(_) => return Err(failed(&format!("the server sent {range:?}"))),
                    None => return Err(RangeError::NoRanges),
                }
            }
            // The whole file. Kept when it is no more than was asked for anyway.
            200 => match header("Content-Length").and_then(|v| v.parse::<u64>().ok()) {
                Some(len) if start == 0 && len <= end => len,
                _ => return Err(RangeError::NoRanges),
            },
            status => return Err(failed(&format!("the server answered {status}"))),
        };
        let mut body = Vec::new();
        // One byte past what was asked: a server that sends more is caught by the
        // length check rather than read to its end.
        response
            .into_body()
            .into_reader()
            .take(end.saturating_sub(start) + 1)
            .read_to_end(&mut body)
            .map_err(|e| failed(&e))?;
        Ok((body, len))
    }
}

/// One object in a store, read by ranged `GET`s through the store a scan uses.
#[cfg(feature = "cloud")]
struct Object {
    store: std::sync::Arc<dyn object_store::ObjectStore>,
    path: object_store::path::Path,
    runtime: tokio::runtime::Handle,
    url: String,
}

#[cfg(feature = "cloud")]
impl RangeSource for Object {
    fn get(&mut self, start: u64, end: u64) -> Result<(Vec<u8>, u64), RangeError> {
        let (store, path) = (self.store.clone(), self.path.clone());
        crate::wait_on_runtime(&self.runtime, async move {
            let got = store
                .get_opts(
                    &path,
                    object_store::GetOptions {
                        range: Some((start..end).into()),
                        ..Default::default()
                    },
                )
                .await?;
            let len = got.meta.size;
            let bytes = got.bytes().await?;
            Ok::<_, object_store::Error>((bytes.to_vec(), len))
        })
        .ok_or_else(|| RangeError::Failed("cancelled".to_string()))?
        .map_err(|e| RangeError::Failed(format!("Could not read {}: {e}", self.url)))
    }
}

/// A model in an object store: one object, an index, or a prefix of model files.
#[cfg(feature = "cloud")]
fn read_cloud(
    url: &Path,
    format: FileFormat,
    cloud: &crate::config::CloudConfig,
    runtime: &tokio::runtime::Handle,
    stop: &dyn Fn() -> bool,
) -> Result<(LazyFrame, ModelSummary), RangeError> {
    // One store for every file: shards and a prefix's files are in its bucket.
    let (full, _, store) = crate::App::cloud_store_for(url, cloud, runtime)?;
    let open = |url: &str| -> Result<Box<dyn RangeSource>, RangeError> {
        let (_, key) = crate::App::cloud_bucket_and_key(url)?;
        Ok(Box::new(Object {
            store: store.clone(),
            path: crate::cloud_browse::object_path(&key),
            runtime: runtime.clone(),
            url: url.to_string(),
        }))
    };
    let urls = if url.to_string_lossy().ends_with('/') {
        list_prefix(&full, &store, runtime, format)?
    } else {
        vec![full]
    };
    let sibling = |url: &str, name: &str| {
        let dir = url.rsplit_once('/').map_or(url, |(dir, _)| dir);
        format!("{dir}/{name}")
    };
    let remote = Remote {
        open: &open,
        sibling: &sibling,
        stop,
    };
    model_files::read_remote_model(&urls, format, &remote)
}

/// The files of `format` directly under a prefix, in name order: a SafeTensors index
/// among them, which names the same shards and is read once with them.
#[cfg(feature = "cloud")]
fn list_prefix(
    prefix_url: &str,
    store: &std::sync::Arc<dyn object_store::ObjectStore>,
    runtime: &tokio::runtime::Handle,
    format: FileFormat,
) -> Result<Vec<String>, RangeError> {
    let (_, key) = crate::App::cloud_bucket_and_key(prefix_url)?;
    let key = key.trim_matches('/').to_string();
    let store = store.clone();
    let listed = crate::wait_on_runtime(runtime, async move {
        let prefix = (!key.is_empty()).then(|| crate::cloud_browse::object_path(&key));
        store.list_with_delimiter(prefix.as_ref()).await
    })
    .ok_or_else(|| RangeError::Failed("cancelled".to_string()))?
    .map_err(|e| RangeError::Failed(format!("Could not list {prefix_url}: {e}")))?;
    let base = format!("{}/", prefix_url.trim_end_matches('/'));
    let mut urls: Vec<String> = listed
        .objects
        .iter()
        .filter_map(|meta| meta.location.filename())
        .filter(|name| crate::discover::data_format(Path::new(name)) == Some(format))
        .map(|name| format!("{base}{name}"))
        .collect();
    urls.sort();
    if urls.is_empty() {
        return Err(RangeError::Failed(format!(
            "{prefix_url} holds no {} files",
            format.name()
        )));
    }
    Ok(urls)
}

#[cfg(all(test, feature = "http"))]
mod tests {
    use super::*;

    #[cfg(feature = "cloud")]
    #[test]
    fn a_remote_model_is_known_by_its_name_or_its_format() {
        let format = |url: &str, given| model_format(Path::new(url), given);
        assert_eq!(
            format("s3://b/m/model.safetensors", None),
            Some(FileFormat::Safetensors)
        );
        assert_eq!(
            format("gs://b/m/model.safetensors.index.json", None),
            Some(FileFormat::Safetensors)
        );
        assert_eq!(
            format("https://h/m/tiny.gguf?download=true", None),
            Some(FileFormat::Gguf)
        );
        assert_eq!(
            format("s3://b/m/", Some(FileFormat::Safetensors)),
            Some(FileFormat::Safetensors)
        );
        // Not a model, or nothing says it is one.
        assert_eq!(format("s3://b/m/", None), None);
        assert_eq!(format("s3://b/data.csv", None), None);
        assert_eq!(format("s3://b/x.safetensors", Some(FileFormat::Csv)), None);
        assert_eq!(format("s3://b/*.safetensors", None), None);
        assert_eq!(format("https://h/models/", Some(FileFormat::Gguf)), None);
        assert_eq!(format("/local/model.safetensors", None), None);
    }

    #[test]
    fn a_shard_is_named_beside_its_index() {
        assert_eq!(
            http_sibling(
                "https://h/org/m/resolve/main/model.safetensors.index.json?download=true",
                "model-00001-of-00002.safetensors"
            ),
            "https://h/org/m/resolve/main/model-00001-of-00002.safetensors"
        );
        assert_eq!(
            http_sibling("http://h/a/index.json", "a b?#.safetensors"),
            "http://h/a/a%20b%3F%23.safetensors"
        );
    }
}

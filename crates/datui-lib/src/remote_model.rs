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
//!
//! [`fetch_small`] reads a small remote file whole the same ways: a `--format` URL.

use std::path::Path;

use polars::prelude::LazyFrame;

use crate::FileFormat;
use crate::error_display::file_message;
use crate::model_files::{self, ModelSummary, RangeError, RangeSource, Remote};
use crate::source::{self, InputSource};

/// A remote model read: its table, its summary, and what the read noticed.
pub(crate) struct Read {
    pub lf: LazyFrame,
    pub summary: ModelSummary,
    pub notes: Vec<crate::notes::Note>,
}

/// The model at `url`: one file, an index, or (in an object store) a prefix holding
/// model files. `stop` is asked before each request; the open's own stop flag.
pub(crate) fn read(
    url: &Path,
    format: FileFormat,
    cloud: &crate::config::CloudConfig,
    runtime: &tokio::runtime::Handle,
    stop: &(dyn Fn() -> bool + Sync),
) -> Result<Read, RangeError> {
    read_from(url, format, cloud, runtime, stop).map_err(|e| match e {
        RangeError::Failed(what) => RangeError::Failed(file_message(url, &what)),
        e => e,
    })
}

fn read_from(
    url: &Path,
    format: FileFormat,
    cloud: &crate::config::CloudConfig,
    runtime: &tokio::runtime::Handle,
    stop: &(dyn Fn() -> bool + Sync),
) -> Result<Read, RangeError> {
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
                Ok(Box::new(Http::new(agent.clone(), url)))
            };
            let remote = Remote {
                open: &open,
                sibling: &http_sibling,
                stop,
            };
            let (lf, summary) = model_files::read_remote_model(&[url], format, &remote)?;
            Ok(Read {
                lf,
                summary,
                notes: Vec::new(),
            })
        }
        #[cfg(feature = "cloud")]
        InputSource::S3(_) | InputSource::Gcs(_) | InputSource::Azure(_) => {
            read_cloud(url, format, cloud, runtime, stop)
        }
        _ => Err(RangeError::Failed(
            "not a URL datui reads model headers from in this build".to_string(),
        )),
    }
}

/// The remote file at `url`, whole, or `None` when it is over `cap` bytes: over
/// HTTP(S) a GET read no further than a byte past `cap`, from a store a ranged GET of
/// as much.
pub(crate) fn fetch_small(
    url: &Path,
    cap: u64,
    cloud: &crate::config::CloudConfig,
    runtime: &tokio::runtime::Handle,
    stop: &(dyn Fn() -> bool + Sync),
) -> Result<Option<Vec<u8>>, String> {
    #[cfg(not(feature = "cloud"))]
    let _ = (cloud, runtime);
    let named = url.display().to_string();
    if stop() {
        return Err(file_message(url, "stopped"));
    }
    match source::input_source(url) {
        #[cfg(feature = "http")]
        InputSource::Http(url) => {
            use std::io::Read;
            let agent: ureq::Agent = ureq::Agent::config_builder()
                .timeout_global(Some(std::time::Duration::from_secs(60)))
                .build()
                .into();
            // A range, so a server sends no more than is read.
            match Http::new(agent.clone(), &url).get(0, cap + 1) {
                Ok((body, len)) => return Ok((len <= cap).then_some(body)),
                Err(RangeError::Failed(message)) => return Err(message),
                Err(RangeError::NoRanges) => {}
            }
            // A server without ranges: the body as it comes, no further than the cap.
            let named = Path::new(&named);
            let response = agent
                .get(&url)
                .header("Accept-Encoding", "identity")
                .call()
                .map_err(|e| file_message(named, &crate::error_display::http_message(&e)))?;
            let mut body = Vec::new();
            response
                .into_body()
                .into_reader()
                .take(cap + 1)
                .read_to_end(&mut body)
                .map_err(|e| file_message(named, &download_stopped(&e)))?;
            Ok((body.len() as u64 <= cap).then_some(body))
        }
        // `az://container/key` names no account and is expanded by the store's setup.
        #[cfg(feature = "cloud")]
        src if source::is_remote_url(url) && !matches!(src, InputSource::Http(_)) => {
            let said = |e: color_eyre::Report| {
                file_message(
                    url,
                    &crate::error_display::user_message_from_report(&e, None),
                )
            };
            let (full, _, store) =
                crate::App::cloud_store_for(url, cloud, runtime).map_err(said)?;
            let (_, key) = crate::App::cloud_bucket_and_key(&full).map_err(said)?;
            let mut object = Object {
                store,
                path: crate::cloud_browse::object_path(&key),
                runtime: runtime.clone(),
                url: named.clone(),
            };
            match object.get(0, cap + 1) {
                Ok((bytes, len)) => Ok((len <= cap).then_some(bytes)),
                Err(RangeError::Failed(message)) => Err(message),
                Err(RangeError::NoRanges) => Ok(None),
            }
        }
        _ => Err(file_message(url, "not a URL datui reads in this build")),
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
    /// Where requests go: the URL given, then wherever its first answer came from.
    url: String,
    /// What errors call the file: the URL given.
    named: String,
}

#[cfg(feature = "http")]
impl Http {
    fn new(agent: ureq::Agent, url: &str) -> Self {
        Self {
            agent,
            url: url.to_string(),
            named: url.to_string(),
        }
    }
}

#[cfg(feature = "http")]
impl RangeSource for Http {
    fn get(&mut self, start: u64, end: u64) -> Result<(Vec<u8>, u64), RangeError> {
        use std::io::Read;
        use ureq::ResponseExt;
        let named = self.named.clone();
        let failed = |what: &str| RangeError::Failed(file_message(Path::new(&named), what));
        // Identity, so a length is the file's and not a compressed body's.
        let response = self
            .agent
            .get(&self.url)
            .header("Range", format!("bytes={start}-{}", end.saturating_sub(1)))
            .header("Accept-Encoding", "identity")
            .call()
            .map_err(|e| failed(&crate::error_display::http_message(&e)))?;
        let landed = response.get_uri().to_string();
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
                    Some(_) => {
                        return Err(failed(&format!(
                            "the server sent the range \"{range}\", not the one asked for"
                        )));
                    }
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
            .map_err(|e| failed(&download_stopped(&e)))?;
        // A redirect (Hugging Face sends each file to its CDN) is followed once: the
        // ranges after the first go straight to where it led.
        self.url = landed;
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
        .map_err(|e| {
            RangeError::Failed(file_message(
                Path::new(&self.url),
                &crate::error_display::store_message(&e),
            ))
        })
    }
}

/// A model in an object store: one object, an index, or a prefix of model files.
#[cfg(feature = "cloud")]
fn read_cloud(
    url: &Path,
    format: FileFormat,
    cloud: &crate::config::CloudConfig,
    runtime: &tokio::runtime::Handle,
    stop: &(dyn Fn() -> bool + Sync),
) -> Result<Read, RangeError> {
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
    let (urls, cut_short) = if url.to_string_lossy().ends_with('/') {
        list_prefix(&full, &store, runtime, format, MAX_LISTED)?
    } else {
        (vec![full.clone()], false)
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
    let (lf, summary) = model_files::read_remote_model(&urls, format, &remote)?;
    let notes = cut_short
        .then(|| crate::notes::Note {
            summary: format!(
                "listing stopped at {} objects {} later model files not read",
                crate::numfmt::group_chrome(MAX_LISTED),
                crate::glyphs::get().middot
            ),
            scope: format!("of {full}"),
            read_as_text: None,
            passed_over: None,
        })
        .into_iter()
        .collect();
    Ok(Read { lf, summary, notes })
}

/// The most objects a prefix's listing reads before it stops: far more than any
/// checkpoint's shards, and a bounded cost for a prefix that holds a whole bucket.
#[cfg(feature = "cloud")]
const MAX_LISTED: usize = 10_000;

/// The files of `format` directly under a prefix, in name order — a SafeTensors index
/// among them, which names the same shards and is read once with them — from the
/// first `cap` objects under it, and whether there were more.
#[cfg(feature = "cloud")]
fn list_prefix(
    prefix_url: &str,
    store: &std::sync::Arc<dyn object_store::ObjectStore>,
    runtime: &tokio::runtime::Handle,
    format: FileFormat,
    cap: usize,
) -> Result<(Vec<String>, bool), RangeError> {
    use futures::StreamExt;
    let (_, key) = crate::App::cloud_bucket_and_key(prefix_url)?;
    let key = key.trim_matches('/').to_string();
    let store = store.clone();
    // The objects a page at a time, stopped at the cap: a listing with a delimiter
    // reads every page before it answers.
    let (listed, more) = crate::wait_on_runtime(runtime, async move {
        let prefix = (!key.is_empty()).then(|| crate::cloud_browse::object_path(&key));
        let mut stream = store.list(prefix.as_ref());
        let mut listed = Vec::new();
        while let Some(meta) = stream.next().await {
            if listed.len() == cap {
                return Ok((listed, true));
            }
            let meta = meta?;
            // Directly under the prefix: the parts after it are only the name.
            let below = meta
                .location
                .prefix_match(&prefix.clone().unwrap_or_default())
                .map_or(0, Iterator::count);
            listed.push((below == 1).then(|| meta.location.filename().map(str::to_string)));
        }
        Ok::<_, object_store::Error>((listed, false))
    })
    .ok_or_else(|| RangeError::Failed("cancelled".to_string()))?
    .map_err(|e| {
        RangeError::Failed(file_message(
            Path::new(prefix_url),
            &crate::error_display::store_message(&e),
        ))
    })?;
    let base = format!("{}/", prefix_url.trim_end_matches('/'));
    let mut urls: Vec<String> = listed
        .into_iter()
        .flatten()
        .flatten()
        .filter(|name| crate::discover::data_format(Path::new(name)) == Some(format))
        .map(|name| format!("{base}{name}"))
        .collect();
    urls.sort();
    if urls.is_empty() {
        let among = match more {
            true => format!(
                " among the first {} objects",
                crate::numfmt::group_chrome(cap)
            ),
            false => String::new(),
        };
        return Err(RangeError::Failed(file_message(
            Path::new(prefix_url),
            &format!("no {} files{among}", format.name()),
        )));
    }
    Ok((urls, more))
}

/// A body that stopped partway, said plainly.
#[cfg(feature = "http")]
fn download_stopped(e: &std::io::Error) -> String {
    format!(
        "the download stopped. {}",
        crate::error_display::user_message_from_io(e, None)
    )
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

    /// A server on a local port that answers each request with `answer(request)`, one
    /// connection per request, and keeps every request it was sent.
    fn scripted(
        answer: impl Fn(&str) -> Vec<u8> + Send + 'static,
    ) -> (String, std::sync::Arc<std::sync::Mutex<Vec<String>>>) {
        use std::io::{BufRead, BufReader, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let kept = seen.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { return };
                let mut request = String::new();
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                loop {
                    let mut line = String::new();
                    if reader.read_line(&mut line).unwrap_or(0) == 0 || line == "\r\n" {
                        break;
                    }
                    request.push_str(&line);
                }
                // Kept before the answer goes out: the client may check what was
                // sent as soon as it has its answer.
                let reply = answer(&request);
                kept.lock().unwrap().push(request);
                let _ = stream.write_all(&reply);
            }
        });
        (base, seen)
    }

    /// A ranged answer from `bytes` for the request's `Range`, as a server sends it.
    fn partial(request: &str, bytes: &[u8]) -> Vec<u8> {
        let range = request
            .lines()
            .find_map(|l| {
                l.strip_prefix("range: bytes=")
                    .or(l.strip_prefix("Range: bytes="))
            })
            .unwrap();
        let (a, b) = range.trim().split_once('-').unwrap();
        let (a, b): (usize, usize) = (a.parse().unwrap(), b.parse().unwrap());
        let b = b.min(bytes.len() - 1);
        let mut out = format!(
            "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes {a}-{b}/{}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            bytes.len(),
            b + 1 - a
        )
        .into_bytes();
        out.extend_from_slice(&bytes[a..=b]);
        out
    }

    fn agent() -> ureq::Agent {
        ureq::Agent::config_builder()
            .timeout_global(Some(std::time::Duration::from_secs(10)))
            .build()
            .into()
    }

    /// A redirect is followed once, and the ranges after it go where it led. Every
    /// request asks for the file as it is, not compressed.
    #[test]
    fn a_redirect_is_followed_once() {
        let bytes: Vec<u8> = (0..64).collect();
        let (base, seen) = scripted(move |request| {
            if request.starts_with("GET /resolve/") {
                b"HTTP/1.1 302 Found\r\nLocation: /cdn/m.gguf?sig=x\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_vec()
            } else {
                partial(request, &bytes)
            }
        });
        let mut http = Http::new(agent(), &format!("{base}/resolve/m.gguf"));
        assert_eq!(http.get(0, 8).unwrap(), ((0..8).collect(), 64));
        assert_eq!(http.get(8, 16).unwrap(), ((8..16).collect(), 64));
        let seen = seen.lock().unwrap();
        let lines: Vec<&str> = seen.iter().map(|r| r.lines().next().unwrap()).collect();
        assert_eq!(
            lines,
            [
                "GET /resolve/m.gguf HTTP/1.1",
                "GET /cdn/m.gguf?sig=x HTTP/1.1",
                "GET /cdn/m.gguf?sig=x HTTP/1.1"
            ]
        );
        assert!(
            seen.iter()
                .all(|r| r.to_ascii_lowercase().contains("accept-encoding: identity")),
            "{seen:?}"
        );
    }

    /// What a server answers in place of the range asked for: another range is an
    /// error, the whole file or an unknown length is a download.
    #[test]
    fn a_server_that_does_not_send_the_range_asked_for() {
        let cases: [(&[u8], Option<RangeError>); 4] = [
            (
                b"HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 4-11/64\r\nContent-Length: 8\r\nConnection: close\r\n\r\n01234567",
                None,
            ),
            (
                b"HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 0-7/*\r\nContent-Length: 8\r\nConnection: close\r\n\r\n01234567",
                Some(RangeError::NoRanges),
            ),
            (
                b"HTTP/1.1 200 OK\r\nContent-Length: 64\r\nConnection: close\r\n\r\n0123456789012345678901234567890123456789012345678901234567890123",
                Some(RangeError::NoRanges),
            ),
            (
                b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n8\r\n01234567\r\n0\r\n\r\n",
                Some(RangeError::NoRanges),
            ),
        ];
        for (answer, want) in cases {
            let (base, _) = scripted(move |_| answer.to_vec());
            let got = Http::new(agent(), &format!("{base}/m.gguf")).get(0, 8);
            match want {
                Some(want) => assert_eq!(got, Err(want)),
                None => assert!(
                    matches!(got, Err(RangeError::Failed(ref m)) if m.contains("4-11")),
                    "{got:?}"
                ),
            }
        }
    }

    /// Every way a remote model, or a format spec fetched for an open, is refused
    /// names the file in the one shape: the URL asked for, or the shard that failed.
    #[test]
    fn errors_name_the_file() {
        let shard = |json: &str| crate::model_files::tests::safetensors_bytes(json, 0);
        let index = br#"{"weight_map":{"a":"bad.safetensors"}}"#.to_vec();
        let (base, _) = scripted(move |request| {
            let path = request.split_whitespace().nth(1).unwrap_or_default();
            match path {
                "/missing.gguf" => {
                    b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                        .to_vec()
                }
                "/denied.gguf" => {
                    b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                        .to_vec()
                }
                "/elsewhere.gguf" => b"HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 4-11/64\r\nContent-Length: 8\r\nConnection: close\r\n\r\n01234567".to_vec(),
                "/m/model.safetensors.index.json" => partial(request, &index),
                "/m/bad.safetensors" => partial(request, &shard("{nope")),
                _ => partial(request, b"GGML\x03\0\0\0"),
            }
        });
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let reading = |name: &str, format| {
            let url = format!("{base}/{name}");
            match read(
                Path::new(&url),
                format,
                &Default::default(),
                runtime.handle(),
                &|| false,
            ) {
                Err(RangeError::Failed(message)) => message,
                Err(e) => panic!("{name}: {e:?}"),
                Ok(_) => panic!("{name} opens"),
            }
        };
        let gguf = FileFormat::Gguf;
        for (name, format, file, says) in [
            ("missing.gguf", gguf, "missing.gguf", "No file there (404)"),
            ("denied.gguf", gguf, "denied.gguf", "refused it (403)"),
            (
                "elsewhere.gguf",
                gguf,
                "elsewhere.gguf",
                "not the one asked for",
            ),
            ("magic.gguf", gguf, "magic.gguf", "does not start with GGUF"),
            (
                "m/model.safetensors.index.json",
                FileFormat::Safetensors,
                "m/bad.safetensors",
                "not valid",
            ),
        ] {
            let message = reading(name, format);
            eprintln!("{message}");
            let path = format!("{base}/{file}");
            crate::readers::bad_input::assert_shape(&message, Path::new(&path));
            assert!(message.contains(says), "{name}: {message}");
        }
        let url = format!("{base}/missing.gguf");
        let fetched = fetch_small(
            Path::new(&url),
            1024,
            &Default::default(),
            runtime.handle(),
            &|| false,
        );
        let message = fetched.expect_err("a 404 is refused");
        crate::readers::bad_input::assert_shape(&message, Path::new(&url));
    }

    /// A sharded checkpoint over HTTP: the index, then each shard in one request of
    /// its first 64 KiB, several under way at once, the table in the index's order.
    #[test]
    fn shards_over_http_take_one_request_each() {
        let n = crate::model_files::SHARD_READS * 2;
        let names: Vec<String> = (1..=n)
            .map(|i| format!("model-{i:05}-of-{n:05}.safetensors"))
            .collect();
        let map: Vec<String> = names
            .iter()
            .enumerate()
            .map(|(i, name)| format!(r#""t{i}":"{name}""#))
            .collect();
        let index = format!(r#"{{"weight_map":{{{}}}}}"#, map.join(","));
        let shards: Vec<Vec<u8>> = (0..n)
            .map(|i| {
                crate::model_files::tests::safetensors_bytes(
                    &format!(r#"{{"t{i}":{{"dtype":"F32","shape":[1],"data_offsets":[0,4]}}}}"#),
                    4,
                )
            })
            .collect();
        let served_names = names.clone();
        let (base, seen) = scripted(move |request| {
            let path = request.split_whitespace().nth(1).unwrap_or_default();
            let file = path.rsplit('/').next().unwrap_or_default();
            match served_names.iter().position(|name| name == file) {
                Some(i) => partial(request, &shards[i]),
                None => partial(request, index.as_bytes()),
            }
        });
        let url = format!("{base}/m/model.safetensors.index.json");
        let read = read(
            Path::new(&url),
            FileFormat::Safetensors,
            &Default::default(),
            &tokio::runtime::Runtime::new().unwrap().handle().clone(),
            &|| false,
        )
        .unwrap();
        assert_eq!(read.summary.tensors, n);
        let df = read.lf.collect().unwrap();
        let files: Vec<&str> = df
            .column("file")
            .unwrap()
            .str()
            .unwrap()
            .iter()
            .flatten()
            .collect();
        assert_eq!(files, names);
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 1 + n, "the index, and one request a shard");
        let first = format!(
            "bytes=0-{}",
            crate::model_files::FIRST_SAFETENSORS_RANGE - 1
        );
        assert!(
            seen.iter()
                .filter(|r| r.contains(".safetensors HTTP"))
                .all(|r| r.to_ascii_lowercase().contains(&first)),
            "{seen:?}"
        );
    }
}

/// A prefix of model files in a store, listed to a cap.
#[cfg(all(test, feature = "cloud"))]
mod listing {
    use super::*;
    use object_store::{ObjectStoreExt, PutPayload, memory::InMemory, path::Path as Key};

    fn store(keys: &[&str]) -> std::sync::Arc<dyn object_store::ObjectStore> {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let store = InMemory::new();
        runtime.block_on(async {
            for key in keys {
                store
                    .put(&Key::from(*key), PutPayload::from_static(b"x"))
                    .await
                    .unwrap();
            }
        });
        std::sync::Arc::new(store)
    }

    /// The files of the format directly under the prefix, in name order; a listing past
    /// the cap stops there and says so, and one that found none says where it looked.
    #[test]
    fn a_prefix_is_listed_to_a_cap() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let handle = runtime.handle().clone();
        let store = store(&[
            "m/config.json",
            "m/model-00002-of-00002.safetensors",
            "m/model-00001-of-00002.safetensors",
            "m/original/consolidated.safetensors",
            "n/model.safetensors",
        ]);
        let list = |cap| list_prefix("s3://b/m/", &store, &handle, FileFormat::Safetensors, cap);
        assert_eq!(
            list(100).unwrap(),
            (
                vec![
                    "s3://b/m/model-00001-of-00002.safetensors".to_string(),
                    "s3://b/m/model-00002-of-00002.safetensors".to_string(),
                ],
                false
            )
        );
        let (urls, cut_short) = list(3).unwrap();
        assert!(cut_short, "{urls:?}");
        assert!(urls.len() <= 2, "{urls:?}");
        let err = list_prefix("s3://b/m/", &store, &handle, FileFormat::Gguf, 2).unwrap_err();
        assert!(
            matches!(err, RangeError::Failed(ref m) if m.ends_with(&format!("{} files among the first 2 objects.", FileFormat::Gguf.name()))),
            "{err:?}"
        );
    }
}

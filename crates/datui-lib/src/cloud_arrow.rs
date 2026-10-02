//! Arrow in S3, GCS or Azure: one object, or a prefix of them.
//!
//! Polars scans an IPC file in a store where it is, by range, from its footer. A
//! stream has no footer, so it is read whole: each one is converted to an IPC file in
//! the temp directory as it downloads, never kept as downloaded beside the copy. The
//! listing picks the objects (a Hugging Face cache's or DatasetDict's one split, as on
//! disk), and the first bytes of each say which kind it is.

use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use color_eyre::{Result, eyre::eyre};
use object_store::{ObjectStore, ObjectStoreExt};

use crate::download::TempDownload;
use crate::ipc_stream::{Merge, Part};
use crate::unfinished::Writer;
use crate::{App, FileFormat, OpenOptions};

/// One Arrow object an open reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Object {
    pub url: String,
    pub size: u64,
    /// An IPC stream, which is downloaded; an IPC file is scanned where it is.
    pub stream: bool,
}

/// Objects by name or URL, with their sizes.
type Listing = Vec<(String, u64)>;

/// Objects whose first bytes are asked for at once.
const PEEKS: usize = 8;

/// The Arrow objects `url` names, in name order: the object itself, or those directly
/// under the prefix, of one split when the prefix is a Hugging Face cache or
/// DatasetDict. Each is told stream or IPC file by its first bytes. `options` comes
/// back with the split chosen.
pub(crate) fn list(
    url: &str,
    options: &OpenOptions,
    cloud: &crate::config::CloudConfig,
    runtime: &tokio::runtime::Handle,
) -> Result<(Vec<Object>, OpenOptions)> {
    let (full, _, store) = App::cloud_store_for(Path::new(url), cloud, runtime)?;
    let (_, key) = App::cloud_bucket_and_key(&full)?;
    let key = key.trim_matches('/').to_string();
    let base = full.trim_end_matches('/').to_string();
    let (objects, options) = if url.ends_with('/') || key.is_empty() {
        let names = names_under(&store, &key, url, runtime)?;
        let (base, names, options) = if names
            .iter()
            .any(|(name, _)| name == crate::hf_splits::DATASET_DICT)
        {
            dict_split(&store, &key, &base, url, options, runtime)?
        } else {
            let (names, options) = one_split(names, options, url)?;
            (format!("{base}/"), names, options)
        };
        let objects = names
            .into_iter()
            .map(|(name, size)| (format!("{base}{name}"), size))
            .collect();
        (objects, options)
    } else {
        let path = crate::cloud_browse::object_path(&key);
        let store = store.clone();
        let meta = crate::wait_on_runtime(runtime, async move { store.head(&path).await })
            .ok_or_else(|| eyre!("Looking at {url} was cancelled."))?
            .map_err(|e| eyre!("Could not read {url}: {e}"))?;
        (vec![(full.clone(), meta.size)], options.clone())
    };
    Ok((peek(&store, objects, runtime)?, options))
}

/// The split of the DatasetDict at `key` an open reads: its subdirectory's Arrow
/// files, under the URL they are named by, and `options` with the split chosen.
fn dict_split(
    store: &Arc<dyn ObjectStore>,
    key: &str,
    base: &str,
    url: &str,
    options: &OpenOptions,
    runtime: &tokio::runtime::Handle,
) -> Result<(String, Listing, OpenOptions)> {
    let text = get_text(store, &join(key, crate::hf_splits::DATASET_DICT), runtime)?;
    let splits = crate::hf_splits::dict_splits(&text)
        .ok_or_else(|| eyre!("{url}: its dataset_dict.json names no splits"))?;
    let listed: Vec<&str> = splits.iter().map(String::as_str).collect();
    let picked = crate::hf_splits::pick(&listed, options.table.as_deref())
        .map_err(|e| eyre!("{url}: {e}"))?;
    let split = picked.split.clone().unwrap_or_default();
    let names = names_under(store, &join(key, &split), url, runtime)?;
    let inner = OpenOptions {
        table: None,
        ..options.clone()
    };
    let (names, inner) = one_split(names, &inner, url)?;
    let options = OpenOptions {
        splits: Some(Arc::new(crate::hf_splits::Splits {
            caches: inner.splits.as_ref().map_or(0, |s| s.caches),
            ..picked
        })),
        ..options.clone()
    };
    Ok((format!("{base}/{split}/"), names, options))
}

/// `key` and `name` as one key.
fn join(key: &str, name: &str) -> String {
    if key.is_empty() {
        name.to_string()
    } else {
        format!("{key}/{name}")
    }
}

/// The names and sizes of the objects directly under `key`, in name order.
fn names_under(
    store: &Arc<dyn ObjectStore>,
    key: &str,
    url: &str,
    runtime: &tokio::runtime::Handle,
) -> Result<Vec<(String, u64)>> {
    let store = store.clone();
    let prefix = (!key.is_empty()).then(|| crate::cloud_browse::object_path(key));
    let listed = crate::wait_on_runtime(runtime, async move {
        store.list_with_delimiter(prefix.as_ref()).await
    })
    .ok_or_else(|| eyre!("Listing {url} was cancelled."))?
    .map_err(|e| eyre!("Could not list {url}: {e}"))?;
    let mut names: Vec<(String, u64)> = listed
        .objects
        .iter()
        .filter_map(|meta| Some((meta.location.filename()?.to_string(), meta.size)))
        .collect();
    names.sort();
    Ok(names)
}

/// The Arrow files of `names`, of one split when they are a Hugging Face cache's, and
/// `options` with the split chosen.
fn one_split(
    mut names: Vec<(String, u64)>,
    options: &OpenOptions,
    url: &str,
) -> Result<(Vec<(String, u64)>, OpenOptions)> {
    let hugging_face = names
        .iter()
        .any(|(name, _)| crate::discover::is_hugging_face_metadata(name));
    names.retain(|(name, _)| {
        !crate::discover::is_bookkeeping(name)
            && crate::discover::data_format(Path::new(name)) == Some(FileFormat::Arrow)
    });
    if names.is_empty() {
        return Err(eyre!("{url} holds no Arrow files"));
    }
    let mut options = options.clone();
    if hugging_face {
        let listed: Vec<&str> = names.iter().map(|(name, _)| name.as_str()).collect();
        let (chosen, splits) = crate::hf_splits::choose(&listed, options.table.as_deref())
            .map_err(|e| eyre!("{url}: {e}"))?;
        names = chosen.into_iter().map(|i| names[i].clone()).collect();
        options.splits = Some(Arc::new(splits));
    }
    Ok((names, options))
}

/// A small object's text: a DatasetDict's `dataset_dict.json`.
fn get_text(
    store: &Arc<dyn ObjectStore>,
    key: &str,
    runtime: &tokio::runtime::Handle,
) -> Result<String> {
    let store = store.clone();
    let path = crate::cloud_browse::object_path(key);
    let bytes = crate::wait_on_runtime(
        runtime,
        async move { store.get(&path).await?.bytes().await },
    )
    .ok_or_else(|| eyre!("Reading {key} was cancelled."))?
    .map_err(|e| eyre!("Could not read {key}: {e}"))?;
    Ok(String::from_utf8_lossy(&bytes).into_owned())
}

/// Each object told stream or IPC file by its first six bytes, a few at a time.
fn peek(
    store: &Arc<dyn ObjectStore>,
    objects: Vec<(String, u64)>,
    runtime: &tokio::runtime::Handle,
) -> Result<Vec<Object>> {
    use futures::StreamExt;
    let store = store.clone();
    let heads = crate::wait_on_runtime(runtime, async move {
        futures::stream::iter(objects)
            .map(|(url, size)| {
                let store = store.clone();
                async move {
                    let (_, key) = App::cloud_bucket_and_key(&url)?;
                    let path = crate::cloud_browse::object_path(&key);
                    let head = store
                        .get_range(&path, 0..size.min(6))
                        .await
                        .map_err(|e| eyre!("Could not read {url}: {e}"))?;
                    let stream = !crate::ipc_stream::is_ipc_file_head(&head);
                    Ok::<_, color_eyre::Report>(Object { url, size, stream })
                }
            })
            .buffered(PEEKS)
            .collect::<Vec<_>>()
            .await
    })
    .ok_or_else(|| eyre!("Looking at the Arrow files was cancelled."))?;
    heads.into_iter().collect()
}

/// The bytes of the streams among `objects`: what the download reads.
pub(crate) fn stream_bytes(objects: &[Object]) -> u64 {
    objects.iter().filter(|o| o.stream).map(|o| o.size).sum()
}

/// Each object's place when they are all IPC files: read where they are, nothing
/// downloaded.
pub(crate) fn in_place(objects: &[Object]) -> Option<Vec<Part>> {
    objects.iter().all(|o| !o.stream).then(|| {
        objects
            .iter()
            .map(|o| Part::InPlace(PathBuf::from(&o.url)))
            .collect()
    })
}

/// Convert the streams among `objects` into one IPC file through `writer`, each as it
/// downloads, with nothing of it kept but the converted rows. The IPC files are left
/// where they are, in their place among the streams.
pub(crate) fn download(
    objects: &[Object],
    options: &OpenOptions,
    cloud: &crate::config::CloudConfig,
    runtime: &tokio::runtime::Handle,
    writer: &Writer,
) -> Result<(TempDownload, Vec<Part>)> {
    crate::ipc_stream::has_room(stream_bytes(objects), options.temp_dir.as_deref())?;
    let mut merge = Merge::create(options.temp_dir.as_deref(), writer)?;
    let read = AtomicU64::new(0);
    let mut parts = Vec::with_capacity(objects.len());
    for object in objects {
        if !object.stream {
            parts.push(Part::InPlace(PathBuf::from(&object.url)));
            continue;
        }
        let body = Body::open(&object.url, cloud, runtime, writer)?;
        parts.push(merge.append(body, Path::new(&object.url), 0, &read)?);
    }
    Ok((merge.finish()?, parts))
}

/// An object's bytes as they arrive, read on this thread: the stream fed to the
/// conversion. A few chunks are queued at a time; see [`crate::download::stream_into`].
struct Body {
    chunks: std::sync::mpsc::Receiver<std::result::Result<Vec<u8>, String>>,
    chunk: Vec<u8>,
    at: usize,
}

impl Body {
    fn open(
        url: &str,
        cloud: &crate::config::CloudConfig,
        runtime: &tokio::runtime::Handle,
        writer: &Writer,
    ) -> Result<Body> {
        use crate::download::StreamError;
        let (_, key) = App::cloud_bucket_and_key(url)?;
        let (_, _, store) = App::cloud_store_for(Path::new(url), cloud, runtime)?;
        let path = crate::cloud_browse::object_path(&key);
        let open = async move {
            let got = store.get(&path).await.map_err(|e| e.to_string())?;
            let len = got.range.end - got.range.start;
            Ok((got.into_stream(), Some(len)))
        };
        let (tx, chunks) = std::sync::mpsc::sync_channel(crate::download::QUEUED_CHUNKS);
        let runtime = runtime.clone();
        let stop = {
            let writer = writer.clone();
            move || writer.stopped()
        };
        let url = url.to_string();
        std::thread::Builder::new()
            .name("datui-arrow-download".to_string())
            .spawn(move || {
                let sent = crate::download::stream_into(&runtime, open, stop, |chunk| {
                    tx.send(Ok(chunk.to_vec()))
                        .map_err(|_| eyre!("the conversion stopped"))
                });
                let message = match sent {
                    Ok(_) => return,
                    Err(StreamError::Open(e)) => {
                        format!("Could not read {url}. Check credentials and URL: {e}")
                    }
                    Err(StreamError::Read(e)) => format!("Could not read {url}: {e}"),
                    Err(StreamError::Short { expected, got }) => {
                        format!("Could not read {url}: it ended after {got} of {expected} bytes")
                    }
                    Err(StreamError::Write(e)) => e.to_string(),
                    Err(StreamError::Cut) => format!("Downloading {url} was cancelled."),
                };
                let _ = tx.send(Err(message));
            })?;
        Ok(Body {
            chunks,
            chunk: Vec::new(),
            at: 0,
        })
    }
}

impl Read for Body {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        while self.at == self.chunk.len() {
            match self.chunks.recv() {
                Ok(Ok(chunk)) => {
                    self.chunk = chunk;
                    self.at = 0;
                }
                Ok(Err(message)) => return Err(std::io::Error::other(message)),
                // The object ended, all of it here.
                Err(_) => return Ok(0),
            }
        }
        let n = buf.len().min(self.chunk.len() - self.at);
        buf[..n].copy_from_slice(&self.chunk[self.at..self.at + n]);
        self.at += n;
        Ok(n)
    }
}

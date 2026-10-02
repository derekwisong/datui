//! A prefix of Arrow files in S3, GCS or Azure.
//!
//! Polars scans neither an Arrow stream nor a store's IPC files as one table, so a
//! single remote `.arrow` is downloaded, and a prefix of them is too: the listing picks
//! the files (a Hugging Face cache's one split, as on disk), and each is downloaded and
//! appended to one IPC file in the temp directory, then let go.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use color_eyre::{Result, eyre::eyre};

use crate::download::TempDownload;
use crate::unfinished::Writer;
use crate::{App, FileFormat, OpenOptions};

/// The Arrow files directly under the prefix `url`, by URL and size, in name order,
/// and `options` with the split chosen when the prefix is a Hugging Face cache.
pub(crate) fn list(
    url: &str,
    options: &OpenOptions,
    cloud: &crate::config::CloudConfig,
    runtime: &tokio::runtime::Handle,
) -> Result<(Vec<(String, u64)>, OpenOptions)> {
    let (full, _, store) = App::cloud_store_for(Path::new(url), cloud, runtime)?;
    let (_, key) = App::cloud_bucket_and_key(&full)?;
    let key = key.trim_matches('/').to_string();
    let listed = crate::wait_on_runtime(runtime, async move {
        let prefix = (!key.is_empty()).then(|| crate::cloud_browse::object_path(&key));
        store.list_with_delimiter(prefix.as_ref()).await
    })
    .ok_or_else(|| eyre!("Listing {url} was cancelled."))?
    .map_err(|e| eyre!("Could not list {url}: {e}"))?;
    let mut objects: Vec<(String, u64)> = listed
        .objects
        .iter()
        .filter_map(|meta| Some((meta.location.filename()?.to_string(), meta.size)))
        .collect();
    objects.sort();
    let hugging_face = objects
        .iter()
        .any(|(name, _)| crate::discover::is_hugging_face_metadata(name));
    objects.retain(|(name, _)| {
        !crate::discover::is_bookkeeping(name)
            && crate::discover::data_format(Path::new(name)) == Some(FileFormat::Arrow)
    });
    if objects.is_empty() {
        return Err(eyre!("{url} holds no Arrow files"));
    }
    let mut options = options.clone();
    if hugging_face {
        let names: Vec<&str> = objects.iter().map(|(name, _)| name.as_str()).collect();
        let (chosen, splits) = crate::hf_splits::choose(&names, options.table.as_deref())
            .map_err(|e| eyre!("{url}: {e}"))?;
        objects = chosen.into_iter().map(|i| objects[i].clone()).collect();
        options.splits = Some(Arc::new(splits));
    }
    let base = format!("{}/", full.trim_end_matches('/'));
    let objects = objects
        .into_iter()
        .map(|(name, size)| (format!("{base}{name}"), size))
        .collect();
    Ok((objects, options))
}

/// Download `objects` through `writer`: one as it is, more into one IPC file, each let
/// go once it is appended.
pub(crate) fn download(
    objects: &[(String, u64)],
    options: &OpenOptions,
    cloud: &crate::config::CloudConfig,
    runtime: &tokio::runtime::Handle,
    writer: &Writer,
) -> Result<TempDownload> {
    let fetch = |url: &str| App::download_cloud_to_temp(url, cloud, options, runtime, writer);
    if let [(url, _)] = objects {
        return fetch(url);
    }
    let size = objects.iter().map(|(_, size)| size).sum();
    crate::ipc_stream::has_room(size, options.temp_dir.as_deref())?;
    let mut merge = crate::ipc_stream::Merge::create(options.temp_dir.as_deref(), writer)?;
    let read = AtomicU64::new(0);
    for (url, _) in objects {
        let object = fetch(url)?;
        merge.append(object.path(), Path::new(url), &read)?;
    }
    merge.finish()
}

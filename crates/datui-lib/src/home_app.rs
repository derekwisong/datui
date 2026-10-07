//! The home screen's work: listing, probing remote places, measuring and
//! classifying rows, previews, search, catalogs and the cloud sources, and the
//! answers its workers send back.

use crate::background::{CacheWrites, OwedAnswer};
use crate::cache::CacheManager;
use crate::cli::FileFormat;
use crate::open_options::OpenOptions;
#[cfg(feature = "cloud")]
use crate::wait_on_runtime;
use crate::{
    APP_NAME, App, AppEvent, InputMode, catalog, config, discover, home, loading, source, widgets,
};
use color_eyre::Result;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// Rows measured per background pass. Small enough that a slow filesystem shows
/// progress rather than a long silence.
const MEASURE_BATCH: usize = 12;

/// Rows a probe measures while it is already reading a remote directory.
const PROBE_MEASURE_LIMIT: usize = 24;

/// Rows one classification pass looks into.
///
/// A cap on work in flight rather than a budget spent per directory: what gets looked
/// into is what is on screen, and the next pass is chosen from the viewport as it is
/// when the previous one lands. Sized like [`MEASURE_BATCH`], for the same reason —
/// on a share that answers in milliseconds per row, a screenful arriving in pieces
/// reads as filling in, and one long silence reads as broken.
pub(crate) const CLASSIFY_BATCH: usize = 16;

/// Probes allowed at once. A probe of a share that has gone away holds its thread
/// until the process exits, so the number of them has to be bounded.
pub(crate) const MAX_CONCURRENT_PROBES: usize = 4;

/// A number for each walk the home search starts, so scorings of one are never taken
/// for another's, even when the two walked the same place.
fn next_search_epoch() -> u64 {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

/// A source as a home-screen row, with the last run's buckets when they still apply.
#[cfg(feature = "cloud")]
pub(crate) fn home_cloud_source(
    source: &crate::cloud_sources::Source,
    cached: Option<&crate::cache::CloudListing>,
    listing: bool,
) -> home::CloudSource {
    let mut details: Vec<(String, String)> = vec![
        ("source".to_string(), source.id.clone()),
        (
            "api".to_string(),
            match source.kind {
                crate::cloud_browse::ProviderKind::S3 => "s3",
                crate::cloud_browse::ProviderKind::Gcs => "gcs",
                crate::cloud_browse::ProviderKind::Azure => "azure",
            }
            .to_string(),
        ),
    ];
    if let Some(endpoint) = &source.s3.endpoint {
        details.push(("endpoint".to_string(), endpoint.clone()));
    }
    if let Some(region) = &source.s3.region {
        details.push(("region".to_string(), region.clone()));
    }
    if let Some(project) = &source.project {
        details.push(("project".to_string(), project.clone()));
    }
    if let Some(profile) = &source.profile {
        details.push(("profile".to_string(), profile.clone()));
    }
    if let Some(configuration) = &source.gcloud {
        details.push(("configuration".to_string(), configuration.clone()));
    }
    if source.s3.virtual_hosted.is_some() {
        let style = if source.s3.virtual_hosted_style() {
            "virtual-hosted"
        } else {
            "path-style"
        };
        details.push(("addressing".to_string(), style.to_string()));
    }
    details.push(("login".to_string(), source.origin.clone()));

    let short = source.problem.as_deref().map(|problem| {
        if problem.starts_with("not signed in") {
            "not signed in"
        } else if problem.starts_with("unsupported login") {
            "unsupported login"
        } else {
            "not configured"
        }
    });
    // A source that cannot list says what to do about it where the row has room: the
    // count already carries the short problem, and the login it would use is moot
    // (#547 D5).
    let note = match (&source.problem, short) {
        (Some(problem), Some(short)) => problem
            .strip_prefix(short)
            .map(|rest| rest.trim_start_matches([':', ' ']))
            .filter(|rest| !rest.is_empty())
            .unwrap_or(problem)
            .to_string(),
        _ => [source.detail(), Some(source.origin.clone())]
            .into_iter()
            .flatten()
            .filter(|n| !n.is_empty())
            .collect::<Vec<_>>()
            .join(&format!(" {} ", crate::glyphs::get().middot)),
    };
    let mut names: Vec<String> = cached.map(|c| c.buckets.clone()).unwrap_or_default();
    for bucket in &source.buckets {
        if !names.contains(bucket) {
            names.push(bucket.clone());
        }
    }
    let status = match (&source.problem, short) {
        (Some(problem), Some(short)) => home::CloudStatus::Failed {
            short: short.to_string(),
            detail: problem.clone(),
        },
        _ if cached.is_some() => home::CloudStatus::Listed,
        _ if listing => home::CloudStatus::Listing,
        _ => home::CloudStatus::Unlisted,
    };
    home::CloudSource {
        id: source.id.clone(),
        label: source.label.clone(),
        api: match source.kind {
            crate::cloud_browse::ProviderKind::S3 => "s3",
            crate::cloud_browse::ProviderKind::Gcs => "gcs",
            crate::cloud_browse::ProviderKind::Azure => "azure",
        }
        .to_string(),
        note,
        buckets: names
            .iter()
            .map(|b| PathBuf::from(source.bucket_url(b)))
            .collect(),
        refreshing: listing && cached.is_some() && source.problem.is_none(),
        // A source that failed before any request has nothing to ask.
        asked: listing || source.problem.is_some(),
        listed_at: cached
            .map(|c| std::time::UNIX_EPOCH + std::time::Duration::from_secs(c.listed_at)),
        status,
        details,
        place_details: Default::default(),
    }
}

/// A listing error as a word for the row and the full message for the details pane.
#[cfg(feature = "cloud")]
fn summarize_cloud_failure(error: &str) -> (String, String) {
    let lower = error.to_lowercase();
    // A missing tool is already as short as it gets: `needs the AWS CLI`.
    if let Some(start) = lower.find("needs ") {
        return (error[start..].to_string(), error.to_string());
    }
    let short = if lower.contains("403")
        || lower.contains("forbidden")
        || lower.contains("accessdenied")
        || lower.contains("access denied")
    {
        "403"
    } else if lower.contains("401")
        || lower.contains("unauthorized")
        || lower.contains("credential")
        || lower.contains("invalidaccesskeyid")
        || lower.contains("expired")
        || lower.contains("sso")
        || lower.contains("az login")
    {
        "not logged in"
    } else if lower.contains("unsupported login") {
        "unsupported login"
    } else if lower.contains("no gcp project") {
        "no project"
    } else if lower.contains("is not set") {
        "not configured"
    } else if lower.contains("timed out")
        || lower.contains("timeout")
        || lower.contains("connection")
        || lower.contains("dns")
        || lower.contains("resolve")
    {
        "unavailable"
    } else {
        "error"
    };
    (short.to_string(), error.to_string())
}

impl App {
    /// Schema for a home-screen entry, read from Parquet metadata and memoised for
    /// the session. `None` means "not knowable without a scan", which the UI reports
    /// rather than papering over.
    pub fn home_schema(&mut self, entry: &discover::Entry) -> Option<discover::SchemaPreview> {
        // A schema preview reads a local file. Nothing in an object store is read before
        // it is opened: asking would only come back empty, again on every rebuild.
        if home::is_cloud_place(&entry.path) || home::is_object_store_url(&entry.path) {
            return None;
        }
        if let Some(cached) = self.home_schema_cache.get(&entry.path) {
            return cached.clone();
        }
        // Reading a schema opens a file, so it is requested rather than done here.
        // Until it arrives the preview says so; it never blocks the frame.
        self.request_home_schema(entry.clone());
        None
    }

    /// What a home-screen worker owes in place of its answer if it panics.
    fn owed_answer(&mut self, instead: AppEvent) -> OwedAnswer {
        OwedAnswer {
            tx: self.events.clone(),
            #[cfg(test)]
            dies: self
                .home_worker_dies
                .as_mut()
                .is_some_and(|dies| dies(&instead)),
            instead: Some(instead),
        }
    }

    /// List the directory the `~` prompt is typing, when it is not the one listed. A
    /// URL is listed from what the screen already knows; a local directory is read on
    /// a worker.
    pub(crate) fn list_the_typed_directory(&mut self) {
        if !self.home.path_input_active {
            return;
        }
        let dir = home::typed_dir(&self.home.path_input).to_string();
        if self
            .home
            .path_listing
            .as_ref()
            .is_some_and(|l| l.dir == dir)
        {
            return;
        }
        if home::typed_dir_is_url(&dir) {
            self.home.path_listing = Some(home::names_under(&dir, self.home.known_urls()));
            return;
        }
        // Read off the UI thread: a typed path is where a dead mount gets named.
        let tx = self.events.clone();
        let owed = self.owed_answer(AppEvent::HomePathListed {
            listing: Box::new(home::PathListing {
                dir: dir.clone(),
                names: Vec::new(),
                failed: true,
            }),
        });
        std::thread::spawn(move || {
            owed.run(|| {
                let listing = home::list_typed_dir(&dir);
                let _ = tx.send(AppEvent::HomePathListed {
                    listing: Box::new(listing),
                });
            })
        });
    }

    /// Complete the path being typed, on a worker.
    pub(crate) fn request_path_completion(&mut self) {
        let typed = self.home.path_input.clone();
        if typed.is_empty() {
            return;
        }
        let generation = self.home_generation;
        let tx = self.events.clone();
        std::thread::spawn(move || {
            let (completed, candidates) = home::complete_path(&typed);
            let _ = tx.send(AppEvent::HomePathCompleted {
                generation,
                typed,
                completed,
                candidates,
            });
        });
    }

    /// The first rows of a home-screen file for its `ROWS` preview, read on a worker
    /// the way its open reads them. `None` until they land, and for a row that is not
    /// previewed. `screen_height` sizes the page to the one the table will ask for.
    pub fn home_preview_rows(
        &mut self,
        entry: &discover::Entry,
        screen_height: u16,
    ) -> Option<Arc<crate::home_preview::PreviewRows>> {
        let max = self.app_config.home.preview_max.bytes();
        if !crate::home_preview::previewable(entry, max) {
            return None;
        }
        let stamp = crate::home_preview::Stamp::of_entry(entry);
        if let Some(known) = self.home_previews.rows(&entry.path, stamp) {
            return known;
        }
        if self.home_previews.inflight.is_none() {
            self.request_home_preview(entry.path.clone(), stamp, screen_height);
        }
        None
    }

    /// Whether `entry` is one the preview reads, before its rows are in.
    pub fn home_preview_pending(&self, path: &Path) -> bool {
        self.home_previews.reading(path)
    }

    /// Read a file's first page on a worker, through the open's own scan and schema
    /// read, so the open can install what it built.
    fn request_home_preview(
        &mut self,
        path: PathBuf,
        stamp: crate::home_preview::Stamp,
        screen_height: u16,
    ) {
        self.home_previews.inflight = Some(path.clone());
        self.reads.previews += 1;
        let tx = self.events.clone();
        let cloud = self.app_config.cloud.clone();
        let formats = self.formats.clone();
        let runtime = self.runtime.clone();
        let cache = self.cache.clone();
        let writes = self.cache_writes.clone();
        // The table's rows: the screen less the title, the header and the control bar.
        let visible = (screen_height as usize).saturating_sub(3).max(1);
        let owed = self.owed_answer(AppEvent::HomePreviewReady {
            path: path.clone(),
            stamp,
            read_at: None,
            rows: None,
            prepared: crate::home_preview::Handoff::default(),
        });
        self.runtime.spawn_blocking(move || {
            owed.run(|| {
                let began = std::time::Instant::now();
                let read_at = crate::home_preview::Stamp::of_file(&path);
                let read = Self::read_home_preview(
                    &path, &cloud, &formats, &runtime, cache, writes, visible,
                );
                log::debug!(
                    target: "datui",
                    "home preview of {}: {:.1?}",
                    path.display(),
                    began.elapsed()
                );
                let (rows, prepared) = match read {
                    Some((rows, prepared)) => (Some(Arc::new(rows)), Some(Box::new(prepared))),
                    None => (None, None),
                };
                let _ = tx.send(AppEvent::HomePreviewReady {
                    path,
                    stamp,
                    read_at,
                    rows,
                    prepared: Arc::new(Mutex::new(prepared)),
                });
            })
        });
    }

    /// What the open of `path` from the home screen reads first: its scan, its schema
    /// and the page the table asks for when `visible` rows show. Built by the open's
    /// own steps with the options the home screen opens a file with, so the dataset is
    /// the one the open would build.
    fn read_home_preview(
        path: &Path,
        cloud: &crate::config::CloudConfig,
        formats: &crate::formats::Registry,
        runtime: &tokio::runtime::Handle,
        cache: CacheManager,
        writes: CacheWrites,
        visible: usize,
    ) -> Option<(
        crate::home_preview::PreviewRows,
        crate::home_preview::Prepared,
    )> {
        let paths = [path.to_path_buf()];
        let scanned = Self::scan_for_open(
            cloud,
            formats,
            &paths,
            OpenOptions::default(),
            Some(path.to_path_buf()),
        )
        .ok()?;
        let loading::LoadAnswer::Scanned { lf, path, options } = scanned else {
            return None;
        };
        let progress = Arc::<crate::schema_union::FooterProgress>::default();
        let report = crate::measurements::OpenReport {
            progress: progress.clone(),
            meter: Arc::new(crate::measurements::Meter::default()),
            remembered: Some(cache),
            writes,
        };
        let read = Self::read_schema_for_open(
            *lf,
            path,
            options,
            cloud,
            runtime,
            &report,
            loading::Made::default(),
        )
        .ok()?;
        let loading::LoadAnswer::SchemaRead {
            mut state,
            options,
            debug_label,
            ..
        } = read
        else {
            return None;
        };
        // Planned as the table plans its first page, so the page is the one it wants.
        state.visible_rows = visible;
        let began = std::time::Instant::now();
        let request = state.prepare_async_collect(None)?;
        let df = crate::statistics::collect_lazy(request.lf, request.polars_streaming).ok()?;
        let result = request.plan.fit(df);
        let rows = crate::home_preview::PreviewRows::from_frame(result.rows());
        state.measurements().read_page(began.elapsed(), Some(1));
        state.apply_async_collect(result);
        Some((
            rows,
            crate::home_preview::Prepared {
                state,
                options,
                debug_label,
                progress,
            },
        ))
    }

    /// Whether a schema read is currently out for this path.
    pub fn home_schema_pending(&self, path: &Path) -> bool {
        self.home_schema_inflight.iter().any(|p| p == path)
    }

    /// Read the selected dataset's schema on a worker.
    fn request_home_schema(&mut self, entry: discover::Entry) {
        if self.home_schema_inflight.contains(&entry.path) {
            return;
        }
        self.home_schema_inflight.push(entry.path.clone());

        let generation = self.home_generation;
        let tx = self.events.clone();
        // Remembered as having none, so the preview is not asked for again.
        let owed = self.owed_answer(AppEvent::HomeSchemaReady {
            generation,
            path: entry.path.clone(),
            preview: None,
        });
        self.runtime.spawn_blocking(move || {
            owed.run(|| {
                let preview = discover::schema_preview(&entry);
                let _ = tx.send(AppEvent::HomeSchemaReady {
                    generation,
                    path: entry.path,
                    preview,
                });
            })
        });
    }

    /// Start listing any network roots that have not answered yet.
    ///
    /// Nothing here waits on the result. A share that has gone away leaves its thread
    /// blocked in the kernel — on a `hard` NFS mount that is uninterruptible and the
    /// thread never returns — so the task is abandoned rather than joined, exactly as
    /// an abandoned dataset load is.
    pub(crate) fn spawn_home_probes(&mut self) {
        self.stop_listings_left_behind();
        for root in self.home.pending_probes() {
            if self.home_probes_inflight.contains(&root) {
                // Left and come back to before its next page: it goes on.
                if let Some(cancelled) = self.home_listing_cancels.get(&root) {
                    cancelled.store(false, std::sync::atomic::Ordering::Relaxed);
                }
                continue;
            }
            // Each probe of an unreachable share costs a thread that will never come
            // back. A handful is a rounding error; an unbounded number, on a machine
            // with a page of dead mounts, is not.
            //
            // Except the directory browsed into, which is the whole screen and has
            // nothing else to show. Held behind the cap, it waited on roots the user
            // had left — a few slow bucket listings kept a share's directory on a
            // spinner long after it could have been read. One more thread per
            // directory the user opens is bounded by the user.
            let browsed = self.home.browsing.as_ref() == Some(&root);
            if !browsed && self.home_probes_inflight.len() >= MAX_CONCURRENT_PROBES {
                continue;
            }
            self.home_probes_inflight.push(root.clone());
            let tx = self.events.clone();
            let cache = self.cache.clone();
            let owed = self.owed_answer(AppEvent::HomeProbeFailed {
                root: root.clone(),
                message: "Could not read it; see the log".to_string(),
            });
            #[cfg(feature = "cloud")]
            let cloud = self.app_config.cloud.clone();
            #[cfg(feature = "cloud")]
            let runtime = self.runtime.clone();
            #[cfg(feature = "cloud")]
            let cancelled = {
                let flag = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
                self.home_listing_cancels.insert(root.clone(), flag.clone());
                flag
            };
            // A detached OS thread, not the runtime's blocking pool. A thread wedged
            // on an unreachable `hard` mount never returns, and the pool is shared with
            // the work that actually loads data — a few dead shares must not eat into
            // the capacity that opening a dataset depends on.
            std::thread::spawn(move || {
                owed.run(|| {
                    // A bucket or a prefix inside one. It looks like a network root to
                    // everything above, and it is, but it is read with an object-store
                    // listing rather than `read_dir` — which on a `gs://` path fails, which
                    // is why descending into a bucket used to show nothing at all.
                    //
                    // Deliberately metadata-only. A delimited listing returns names, sizes
                    // and modification times for one level, and nothing here reads an
                    // object's contents: no footers, no schemas, no row counts. Those are
                    // what a local listing fills in for free from bytes already on the
                    // machine, and what would cost a ranged read per row against an object
                    // store somebody pays egress on.
                    #[cfg(feature = "cloud")]
                    if let Some((id, account)) = home::cloud_account(&root) {
                        let listed = wait_on_runtime(&runtime, async move {
                            crate::cloud_browse::list_account(&id, &account, &cloud).await
                        });
                        match listed {
                            Some(Ok(rows)) => {
                                let _ = tx.send(AppEvent::HomeProbeReady {
                                    root,
                                    rows: Some(rows),
                                    cut_short: false,
                                });
                            }
                            Some(Err(message)) => {
                                log::warn!(
                                    target: "datui::cloud",
                                    "listing {} failed: {message}",
                                    root.display()
                                );
                                let _ = tx.send(AppEvent::HomeProbeFailed { root, message });
                            }
                            None => {
                                let _ = tx.send(AppEvent::HomeProbeReady {
                                    root,
                                    rows: None,
                                    cut_short: false,
                                });
                            }
                        }
                        return;
                    }
                    #[cfg(feature = "cloud")]
                    if crate::cloud_browse::split_bucket_url(&root.to_string_lossy()).is_some()
                        || source::azure_parts(&root.to_string_lossy()).is_some()
                    {
                        let url = root.to_string_lossy().into_owned();
                        // Each page's rows are drawn as they come, and leaving the place
                        // stops the listing before its next page.
                        let watch = crate::cloud_browse::Watch {
                            progress: Some(std::sync::Arc::new({
                                let (tx, root) = (tx.clone(), root.clone());
                                move |page: &[crate::discover::Entry]| {
                                    let _ = tx.send(AppEvent::HomeProbeProgress {
                                        root: root.clone(),
                                        rows: page.to_vec(),
                                    });
                                }
                            })),
                            cancelled,
                            names_from: None,
                        };
                        let listed = wait_on_runtime(&runtime, async move {
                            crate::cloud_browse::list_objects_watched(&url, &cloud, &watch).await
                        });
                        // A refused listing says why, rather than reading as a place that
                        // stopped answering.
                        match listed {
                            Some(Err(message)) => {
                                log::warn!(
                                    target: "datui::cloud",
                                    "listing {} failed: {message}",
                                    root.display()
                                );
                                let _ = tx.send(AppEvent::HomeProbeFailed { root, message });
                            }
                            Some(Ok(level)) if level.cancelled => {
                                let _ = tx.send(AppEvent::HomeProbeCancelled { root });
                            }
                            other => {
                                let (rows, cut_short) = match other {
                                    Some(Ok(level)) => (Some(level.rows), level.truncated),
                                    _ => (None, false),
                                };
                                let _ = tx.send(AppEvent::HomeProbeReady {
                                    root,
                                    rows,
                                    cut_short,
                                });
                            }
                        }
                        return;
                    }
                    // Nothing to list a bucket with, and `read_dir` on its URL would
                    // only call it unavailable.
                    #[cfg(not(feature = "cloud"))]
                    if source::is_remote_url(&root) {
                        let message = "cloud support not in this build".to_string();
                        let _ = tx.send(AppEvent::HomeProbeFailed { root, message });
                        return;
                    }
                    let mut cut_short = false;
                    let rows = if std::fs::read_dir(&root).is_ok() {
                        // What has been read shows while the rest is read: a share can take
                        // seconds over a directory of thousands.
                        let scan = crate::discover::scan_dir_progressive(&root, |read| {
                            let _ = tx.send(AppEvent::HomeProbeProgress {
                                root: root.clone(),
                                rows: read.to_vec(),
                            });
                        });
                        cut_short = scan.truncated;
                        let mut rows = scan.entries;
                        // Measuring happens here too: it is the same remote filesystem,
                        // and this thread is already the one allowed to block on it.
                        for row in rows.iter_mut().take(PROBE_MEASURE_LIMIT) {
                            crate::discover::enrich(row);
                        }
                        // Remote datasets are measured nowhere else, so this is the only
                        // chance to remember them. Without it a remote row is blank on
                        // every run, which is exactly backwards: the hardest things to
                        // reach are the ones most worth remembering.
                        let mounts = crate::locality::Mounts::current();
                        for row in rows.iter_mut() {
                            row.cost.source = Some(mounts.describe(&row.path).fstype);
                        }
                        let facts: Vec<_> = rows.iter().filter_map(home::facts_for).collect();
                        cache.record_dataset_facts(&facts);
                        Some(rows)
                    } else {
                        None
                    };
                    let _ = tx.send(AppEvent::HomeProbeReady {
                        root,
                        rows,
                        cut_short,
                    });
                })
            });
        }
    }

    /// Stop the cloud listings of places no longer on screen: the one browsed, or the
    /// roots of the home listing. One left and come back to is listed again.
    fn stop_listings_left_behind(&mut self) {
        let home = &self.home;
        for (root, cancelled) in &self.home_listing_cancels {
            let wanted = match &home.browsing {
                Some(dir) => dir == root,
                None => home
                    .sections
                    .iter()
                    .any(|s| s.remote_root.as_ref() == Some(root)),
            };
            if !wanted {
                cancelled.store(true, std::sync::atomic::Ordering::Relaxed);
            }
        }
        if let Some((dir, _, cancelled)) = &self.home_narrowing
            && home.browsing.as_ref() != Some(dir)
        {
            cancelled.store(true, std::sync::atomic::Ordering::Relaxed);
            self.home_narrowing = None;
        }
        if self
            .home
            .narrowed
            .as_ref()
            .is_some_and(|n| self.home.browsing.as_ref() != Some(&n.dir))
        {
            self.home.narrowed = None;
        }
    }

    /// In a cloud directory cut short at the cap, ask the server for the names the
    /// filter starts, so a name past the first few thousand can still be found. Nothing
    /// asked when the filter is empty or what is held already answers it.
    #[cfg(feature = "cloud")]
    pub(crate) fn narrow_cloud_listing(&mut self) {
        let dir = self.home.browsing.clone();
        let prefix = dir.as_ref().and_then(|dir| {
            if !self.home.probes.cut_short(dir) {
                return None;
            }
            let rows = self.home.probes.listed(dir)?;
            let names: Vec<&str> = rows.iter().map(|row| row.name.as_str()).collect();
            crate::cloud_browse::narrowing_prefix(&self.home.filter, &names)
        });
        let (Some(dir), Some(prefix)) = (dir, prefix) else {
            if let Some((_, _, cancelled)) = self.home_narrowing.take() {
                cancelled.store(true, std::sync::atomic::Ordering::Relaxed);
            }
            if self.home.narrowed.take().is_some() {
                self.home_refresh();
            }
            return;
        };
        // Everything under a shorter prefix is everything under this one too.
        if self.home.narrowed.as_ref().is_some_and(|n| {
            n.dir == dir && (n.prefix == prefix || (!n.truncated && prefix.starts_with(&n.prefix)))
        }) {
            return;
        }
        if let Some((d, p, _)) = &self.home_narrowing
            && *d == dir
            && *p == prefix
        {
            return;
        }
        if let Some((_, _, cancelled)) = self.home_narrowing.take() {
            cancelled.store(true, std::sync::atomic::Ordering::Relaxed);
        }
        let cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));
        self.home_narrowing = Some((dir.clone(), prefix.clone(), cancelled.clone()));
        let tx = self.events.clone();
        let owed = self.owed_answer(AppEvent::HomeNarrowed {
            dir: dir.clone(),
            prefix: prefix.clone(),
            listed: None,
        });
        let cloud = self.app_config.cloud.clone();
        let runtime = self.runtime.clone();
        std::thread::spawn(move || {
            owed.run(|| {
                let url = dir.to_string_lossy().into_owned();
                let watch = crate::cloud_browse::Watch {
                    progress: None,
                    cancelled,
                    names_from: Some(prefix.clone()),
                };
                let listed = wait_on_runtime(&runtime, async move {
                    crate::cloud_browse::list_objects_watched(&url, &cloud, &watch).await
                });
                let listed = match listed {
                    Some(Ok(level)) if !level.cancelled => Some((level.rows, level.truncated)),
                    _ => None,
                };
                let _ = tx.send(AppEvent::HomeNarrowed {
                    dir,
                    prefix,
                    listed,
                });
            })
        });
    }

    /// Find the cloud sources this machine and the config describe, and list their
    /// buckets when `[cloud] list_on_start` asks. Once per session; Ctrl+R asks again.
    #[cfg(feature = "cloud")]
    fn spawn_cloud_discovery(&mut self) {
        if self.cloud_discovery_started {
            return;
        }
        self.cloud_discovery_started = true;
        let list = self.app_config.cloud.list_on_start;
        self.list_cloud_sources(None, list);
    }

    /// List the source being browsed, when it has not been asked this session.
    /// Entering a source is the request to list it.
    #[cfg(feature = "cloud")]
    fn list_browsed_cloud_source(&mut self) {
        let Some(id) = self
            .home
            .browsing
            .as_deref()
            .and_then(home::cloud_source_id)
        else {
            return;
        };
        let Some(source) = self.home.cloud.iter_mut().find(|s| s.id == id) else {
            return;
        };
        if source.asked {
            return;
        }
        source.begin_listing();
        self.list_cloud_sources(Some(id), true);
    }

    /// Send the rows of every source, or list the buckets of the one named.
    ///
    /// The rows go out first, filled from the last run's listing when the source still
    /// points at the same place, so the home screen has its counts before any request
    /// is made. With `list`, the sources are then listed side by side, a few at a
    /// time, and each result is sent the moment it arrives. Without it nothing leaves
    /// the machine: no request, and no credential command.
    ///
    /// Runs on the runtime rather than a detached thread. Unlike a probe of a dead
    /// `hard` mount, an HTTP request cannot wedge forever: every call here is bounded
    /// by a global timeout, so the task is guaranteed to end.
    #[cfg(feature = "cloud")]
    fn list_cloud_sources(&mut self, only: Option<String>, list: bool) {
        let tx = self.events.clone();
        let cloud = self.app_config.cloud.clone();
        let cache = self.cache.clone();
        self.runtime.spawn(async move {
            let mut hidden = cache.load_hidden_cloud_sources();
            hidden.extend(cloud.hide.iter().cloned());
            let cached_for = |source: &crate::cloud_sources::Source| {
                cache.cloud_listing(&source.id, &source.fingerprint())
            };
            // Looked for again, and kept for the opens and listings that follow.
            let found = crate::cloud_sources::rediscover(&cloud).to_vec();
            // Shown or not: a bucket under Recent opens with the login that listed it
            // whatever `discover` says. Not a hidden source, which may be hidden for a
            // login that no longer works; the default login opens its buckets instead.
            // Only with the rows, so an old listing never overrides one made since.
            if only.is_none() {
                for source in found.iter().filter(|s| !hidden.contains(&s.id)) {
                    if let Some(cached) = cached_for(source) {
                        crate::cloud_sources::remember_listed(source, &cached.buckets);
                    }
                }
            }
            let sources: Vec<crate::cloud_sources::Source> =
                crate::cloud_sources::on_home(found, &cloud)
                    .into_iter()
                    .filter(|s| !hidden.contains(&s.id))
                    .collect();

            match &only {
                None => {
                    let rows = sources
                        .iter()
                        .map(|source| home_cloud_source(source, cached_for(source).as_ref(), list))
                        .collect();
                    let _ = tx.send(AppEvent::HomeCloudSources { sources: rows });
                }
                // Gone since its row was drawn: a profile removed, a source hidden
                // elsewhere. Said, so the row does not wait on an answer never coming.
                Some(id) if !sources.iter().any(|s| &s.id == id) => {
                    let _ = tx.send(AppEvent::HomeCloudListed {
                        id: id.clone(),
                        buckets: Vec::new(),
                        details: Vec::new(),
                        failure: Some((
                            "not found".to_string(),
                            format!("{id} is gone or hidden. Ctrl+R at the top looks again."),
                        )),
                        listed_at: std::time::SystemTime::now(),
                    });
                    return;
                }
                Some(_) => {}
            }
            if !list {
                return;
            }

            // Enough to keep one slow endpoint from delaying the rest, few enough that a
            // long list of sources does not open a connection storm.
            const LISTING_AT_ONCE: usize = 4;
            let permits = std::sync::Arc::new(tokio::sync::Semaphore::new(LISTING_AT_ONCE));
            let mut listings = tokio::task::JoinSet::new();
            for source in sources
                .into_iter()
                .filter(|s| only.as_ref().is_none_or(|id| &s.id == id))
            {
                let permits = permits.clone();
                listings.spawn(async move {
                    let _permit = permits.acquire_owned().await;
                    let result = crate::cloud_browse::list_first_level(&source).await;
                    (source, result)
                });
            }
            while let Some(joined) = listings.join_next().await {
                let Ok((source, result)) = joined else {
                    continue;
                };
                let listed_at = std::time::SystemTime::now();
                // Buckets named in the config are shown whether or not the login can
                // list them; that is what naming them is for.
                let mut names = source.buckets.clone();
                let mut details = Vec::new();
                let failure = match result {
                    Ok(listed) => {
                        for item in listed {
                            crate::cloud_sources::remember_bucket(&source, &item.name);
                            if !item.details.is_empty() {
                                details.push((item.place.clone(), item.details));
                            }
                            let name = item.name;
                            if !names.contains(&name) {
                                names.push(name);
                            }
                        }
                        cache.save_cloud_listing(
                            &source.id,
                            crate::cache::CloudListing {
                                fingerprint: source.fingerprint(),
                                buckets: names.clone(),
                                listed_at: listed_at
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .map(|d| d.as_secs())
                                    .unwrap_or(0),
                            },
                        );
                        None
                    }
                    Err(e) => {
                        log::warn!(target: "datui::cloud", "listing {} failed: {e}", source.id);
                        Some(summarize_cloud_failure(&e))
                    }
                };
                let _ = tx.send(AppEvent::HomeCloudListed {
                    id: source.id.clone(),
                    buckets: names
                        .iter()
                        .map(|b| PathBuf::from(source.bucket_url(b)))
                        .collect(),
                    details,
                    failure,
                    listed_at,
                });
            }
        });
    }

    /// Ask again for what is on screen, ignoring what is cached: the buckets of the
    /// source being browsed, the contents of the bucket or directory being browsed, or
    /// every source's buckets from the home listing.
    pub(crate) fn home_reload(&mut self) {
        #[cfg(feature = "cloud")]
        {
            let browsing = self.home.browsing.clone();
            match browsing.as_deref().and_then(home::cloud_source_id) {
                Some(id) => {
                    if let Some(source) = self.home.cloud.iter_mut().find(|s| s.id == id) {
                        source.begin_listing();
                    }
                    self.list_cloud_sources(Some(id), true);
                }
                None if browsing.is_none() && !self.home.cloud.is_empty() => {
                    for source in &mut self.home.cloud {
                        source.begin_listing();
                    }
                    self.list_cloud_sources(None, true);
                }
                None => {}
            }
        }
        if let Some(dir) = self.home.browsing.clone() {
            // Its answer, not a listing still coming in.
            if self.home.probes.settled(&dir) {
                self.home.probes.forget(&dir);
            }
        }
        // A peek that failed is asked again: Ctrl+R is the request to try. So is a web
        // file that was not there.
        self.home.peek_failed.clear();
        for path in std::mem::take(&mut self.home.web_gone).into_keys() {
            self.home.sized.remove(&path);
        }
        self.home.status = None;
        self.home_refresh();
    }

    /// Start the recursive search below the working directory, if it is wanted and
    /// not already running.
    ///
    /// Triggered by typing rather than by opening the home screen: typing is the
    /// signal that someone is looking for something. Launching datui, pressing Enter
    /// on a recent dataset and leaving costs no walk at all.
    pub(crate) fn spawn_home_search(&mut self) {
        if self.home_search_inflight || self.home.search.done {
            return;
        }
        let config = self.app_config.home.search.clone();
        if !config.enabled {
            return;
        }
        let Some(root) =
            crate::search::search_root(self.home.browsing.as_ref(), self.home.network_check)
        else {
            return;
        };

        self.home.search.reset();
        self.home.search.root = Some(root.clone());
        self.home.search.running = true;
        self.home.search.epoch = next_search_epoch();
        self.home.search_limit = config.max_results;
        self.home_search_inflight = true;

        let generation = self.home_generation;
        self.home_search_generation = generation;
        let tx = self.events.clone();
        let formats = self.formats.clone();
        // Ended, with what the batches already found kept.
        let owed = self.owed_answer(AppEvent::HomeSearchDone {
            generation,
            root: root.clone(),
            scanned: 0,
            limited: Some(crate::glyphs::dotted("partial · failed")),
        });
        // A detached thread for the same reason the probes use one: the walk touches
        // a filesystem, and nothing that touches a filesystem may run where a stall
        // would stop the screen from drawing.
        std::thread::spawn(move || {
            owed.run(|| {
                let walk_root = root.clone();
                let batch_tx = tx.clone();
                let batch_gen = generation;
                let batch_root = root.clone();
                let outcome = crate::search::walk_with_specs(
                    &walk_root,
                    &config,
                    &formats,
                    move |found, outcome| {
                        // Sent even when empty: it carries the progress count, and it is the
                        // only place the walk learns that nobody is listening any more.
                        batch_tx
                            .send(AppEvent::HomeSearchBatch {
                                generation: batch_gen,
                                root: batch_root.clone(),
                                found,
                                scanned: outcome.scanned,
                            })
                            // A closed channel means the app is gone; stop walking.
                            .is_ok()
                    },
                );
                let _ = tx.send(AppEvent::HomeSearchDone {
                    generation,
                    root,
                    scanned: outcome.scanned,
                    limited: outcome.note().map(crate::glyphs::dotted),
                });
            })
        });
    }

    /// Score the filter against the search's files on a worker, when a scoring is owed.
    ///
    /// Asked after every event. Over a tree of tens of thousands of files the scoring
    /// is what held each keystroke's echo back, so it runs where a stall cannot hold
    /// the screen, one at a time; each answer asks for the next if the filter moved on.
    pub(crate) fn home_score_search(&mut self) {
        if self.input_mode != InputMode::Home {
            return;
        }
        let Some(job) = self.home.score_job() else {
            return;
        };
        let epoch = job.epoch;
        let tx = self.events.clone();
        // A worker that dies answers with nothing.
        let owed = self.owed_answer(AppEvent::HomeSearchScored {
            epoch,
            matches: None,
        });
        self.runtime.spawn_blocking(move || {
            owed.run(move || {
                let matches =
                    crate::search::score(&job.results, &job.query, job.base.as_ref(), job.limit);
                let _ = tx.send(AppEvent::HomeSearchScored {
                    epoch,
                    matches: Some(Box::new(matches)),
                });
            })
        });
    }

    /// Rebuild the home listing from the filesystem.
    pub(crate) fn home_refresh(&mut self) {
        self.home_refresh_owed = false;
        // Every way into a source comes through here: Enter, Backspace up from a
        // bucket, a jump, and rows arriving while the source is already open.
        #[cfg(feature = "cloud")]
        self.list_browsed_cloud_source();
        // Somewhere else now, a listing of where the user was is pages for nobody.
        self.stop_listings_left_behind();
        self.home_generation = self.home_generation.wrapping_add(1);
        let generation = self.home_generation;

        self.move_remembered_places();
        let mut catalogs = home::catalogs(&self.app_config);
        // Hidden with Delete on its heading: the catalog that comes with datui only,
        // never a user's own `examples.toml`.
        if self.cache.examples_hidden() {
            catalogs.retain(|c| c.origin != crate::catalog::Origin::Bundled);
        }
        self.home.set_catalogs(catalogs);
        let mut request = home::ListingRequest {
            // Filled in on the worker, from the cache and the desktop's recents: files
            // all the same, and the first frame does not wait on a file.
            recents: Vec::new(),
            desktop_dirs: Vec::new(),
            browsing: self.home.browsing.clone(),
            probes: self.home.probes.clone(),
            narrowed: self.home.narrowed.clone(),
            network_check: self.home.network_check,
            cloud: self.home.cloud.clone(),
            catalogs: self.home.catalogs.clone(),
            known: Default::default(),
            formats: self.formats.clone(),
        };
        let read_folds = std::mem::take(&mut self.home.folds_owed);
        let desktop = self.app_config.home.desktop_recents;
        let cache = self.cache.clone();
        let writes = self.cache_writes.clone();

        self.home.listing_in_flight = true;
        let tx = self.events.clone();
        let owed = self.owed_answer(AppEvent::HomeListingFailed);
        self.runtime.spawn_blocking(move || {
            owed.run(move || {
                // After the dataset just left is in the recents with its shape.
                writes.settle();
                // Ranked by frecency; the newest is where the cursor lands, so the
                // last file is still one Enter away.
                let (recents, visits) = cache.load_recents_with_visits();
                let newest = recents.first().cloned();
                request.recents = crate::cache::by_frecency(recents, &visits);
                request.known = cache.load_dataset_facts();
                if desktop {
                    request.desktop_dirs = home::desktop_recent_dirs();
                }
                let listing = home::build_listing(&request);
                let mut visits = visits;
                listing.alias_visits(&mut visits);
                // A record shown is a record used: the ones eviction keeps.
                let shown: Vec<PathBuf> = listing
                    .sections
                    .iter()
                    .flat_map(|s| s.rows.iter().chain(&s.door))
                    .flat_map(|row| [row.path.clone(), home::index_key(&row.path)])
                    .filter(|key| request.known.contains_key(key))
                    .collect();
                cache.touch_dataset_facts(shown.iter().map(PathBuf::as_path));
                let _ = tx.send(AppEvent::HomeListingReady {
                    generation,
                    listing: Box::new(listing),
                    known: request.known,
                    visits,
                    newest,
                    folds: read_folds.then(|| cache.load_folds()),
                });
            })
        });
    }

    /// Ask the worker to measure rows that are on screen and not yet known.
    ///
    /// Reading a Parquet footer opens a file. That is the call that blocks on a FIFO,
    /// a device node, a wedged mount or a failing disk, so it never happens on the
    /// thread that draws.
    pub(crate) fn request_home_measurements(&mut self) {
        if self.home.measure_in_flight {
            return;
        }
        let wanted = self.home.unmeasured_visible(MEASURE_BATCH);
        if wanted.is_empty() {
            return;
        }

        self.home.measure_in_flight = true;
        let tx = self.events.clone();
        let cache = self.cache.clone();
        self.runtime.spawn_blocking(move || {
            home::look_into_batch(wanted, &cache, |path, m| {
                let _ = tx.send(AppEvent::HomeMeasured {
                    measured: vec![(path, m)],
                    done: false,
                });
            });
            let _ = tx.send(AppEvent::HomeMeasured {
                measured: Vec::new(),
                done: true,
            });
        });
    }

    /// Ask an HTTP(S) server what the file under the cursor weighs, once a session, when
    /// nothing has measured it: its row shows a catalog's `~33 MB` until the answer
    /// lands, and the answer is kept with what datui measured, for the next listing.
    #[cfg(feature = "http")]
    pub(crate) fn size_selected_web_file(&mut self) {
        if !self.head_web_rows {
            return;
        }
        let Some(entry) = self.home.selected_entry().cloned() else {
            return;
        };
        if entry.size.is_some()
            || !matches!(
                source::input_source(&entry.path),
                source::InputSource::Http(_)
            )
            || !self.home.sized.insert(entry.path.clone())
        {
            return;
        }
        let tx = self.events.clone();
        let cache = self.cache.clone();
        self.runtime.spawn_blocking(move || {
            let size = match Self::fetch_remote_size_http(&entry.path.to_string_lossy()) {
                Ok(Some(size)) => size,
                Ok(None) => return,
                Err(gone) => {
                    let _ = tx.send(AppEvent::HomeWebGone {
                        path: entry.path,
                        gone,
                    });
                    return;
                }
            };
            let key = home::index_key(&entry.path);
            let mut facts = cache.dataset_facts(&key).unwrap_or_default();
            facts.size = size;
            cache.record_dataset_facts(&[(key, facts)]);
            let measured = home::Measured {
                rows: entry.rows,
                cols: entry.cols,
                cols_sampled: entry.cols_sampled,
                size: Some(size),
                columns: entry.columns.clone(),
                cost: entry.cost.clone(),
                kind: None,
                holds: entry.holds.clone(),
            };
            let _ = tx.send(AppEvent::HomeSized {
                path: entry.path,
                measured,
            });
        });
    }

    /// Ask a worker what the rows on screen are.
    ///
    /// Classifying a row reads the directory it names, which on a share is a round
    /// trip and on a wedged mount never returns, so it happens here for the rows on
    /// screen rather than while the listing is built.
    ///
    /// A detached thread, not the runtime's blocking pool, for the reason the probes
    /// give: a thread stuck on an unreachable `hard` mount never comes back. One at a
    /// time, so a share that has stopped answering costs one thread.
    ///
    /// This also measures remote rows, which [`HomeState::unmeasured_visible`] leaves
    /// alone: every row a probe returns is `Unknown`, so this pass is the only one that
    /// can, and it does so on the thread already reading that filesystem.
    pub(crate) fn request_home_classifications(&mut self) {
        if self.home.classify_in_flight {
            return;
        }
        let wanted = self.home.unclassified_visible(CLASSIFY_BATCH);
        if wanted.is_empty() {
            return;
        }

        self.home.classify_in_flight = true;
        let tx = self.events.clone();
        let cache = self.cache.clone();
        std::thread::spawn(move || {
            home::look_into_batch(wanted, &cache, |path, m| {
                let _ = tx.send(AppEvent::HomeClassified {
                    measured: vec![(path, m)],
                    done: false,
                });
            });
            let _ = tx.send(AppEvent::HomeClassified {
                measured: Vec::new(),
                done: true,
            });
        });
    }

    pub fn enter_home(&mut self) {
        self.pause_indexing();
        if self.return_from_quality_evidence(false) {
            self.analysis_modal.close();
        }
        // The view modal keys and renders off its own `active`, not the input
        // mode, so left open here it would come back as a zombie over the next
        // dataset opened.
        self.view_modal.close();
        self.inspector_modal.close();
        self.stop_find();
        self.hex = None;
        // A count of the dataset being left is read for nobody.
        self.stop_value_count();
        self.export_counts = None;
        self.abandon_load();
        // Nobody is watching the file any more.
        if let Some(state) = self.data_table_state.as_mut() {
            state.stop_following();
        }
        self.home.status = None;
        // The search that found the dataset comes back, selected: the next character
        // typed starts a new one, and `~` opens the path prompt.
        self.home.filter_selected = !self.home.filter.is_empty();
        self.home.folds_owed = true;
        self.home_refresh();
        if let Some(open_path) = self.path.clone() {
            let target =
                crate::canonical::canonicalize(&open_path).unwrap_or_else(|_| open_path.clone());
            if let Some(idx) = self.home.visible().iter().position(|row| match row {
                home::Row::Entry { entry, .. } => {
                    crate::canonical::canonicalize(&entry.path)
                        .unwrap_or_else(|_| entry.path.clone())
                        == target
                }
                // Not the door: its path is the directory's, so an open file whose
                // directory is being browsed would put the cursor on the row that
                // opens the whole directory rather than on the file itself.
                home::Row::Header { .. }
                | home::Row::Place { .. }
                | home::Row::More { .. }
                | home::Row::Hidden { .. }
                | home::Row::Door { .. } => false,
            }) {
                self.home.selected = idx;
            }
        }
        self.input_mode = InputMode::Home;
    }

    /// Esc backs out one layer of context at a time: the filter, then the directory
    /// descended into, then back to the data that was open. At the top level it does
    /// nothing. It used to quit there, which made a reflexive Esc close the program
    /// while the same key one level down merely went up; Ctrl+C quits, from anywhere.
    pub(crate) fn home_escape(&mut self) -> Option<AppEvent> {
        if !self.home.filter.is_empty() {
            self.home.filter.clear();
            self.home.sync_search_section();
            // On a dataset, as at launch, not on the first section's header.
            self.home.select_first_entry();
            return None;
        }
        if self.home.browsing.is_some() {
            if self.home.below_browse_start() {
                self.home_ascend();
            } else {
                // Climbing past where the browse began would take Esc somewhere the
                // user never was; it returns to the listing they started from instead.
                self.home_leave_browsing(None);
            }
            return None;
        }
        if self.data_table_state.is_some() {
            self.input_mode = InputMode::Normal;
            // Said on arrival: Esc pressed once too often to clear the home screen lands
            // here, and the keys typed next act on the table (#547 D14).
            let name = self
                .path
                .as_deref()
                .and_then(|p| p.file_name())
                .map(|n| n.to_string_lossy().into_owned());
            if let Some(name) = name {
                self.flash_note(format!("Back to {name}"));
            }
        }
        None
    }

    /// Drop the highlighted dataset from the recents list.
    ///
    /// Only from the Recent section: a row under a directory is a file on disk, and
    /// forgetting it there would either do nothing or imply a deletion datui is not
    /// going to perform.
    pub(crate) fn home_forget_selected(&mut self) {
        // A catalog's heading: Delete hides the one that comes with datui, until the
        // cache is cleared. A catalog of the user's is hidden by its id in the config.
        if let Some(catalog) = self.home.selected_catalog() {
            if catalog.origin == crate::catalog::Origin::Bundled {
                let message = format!(
                    "Hide {}? It comes back after datui cache clear.",
                    catalog.label
                );
                self.pending_hide_examples = true;
                self.confirmation_modal.show_destructive(message, "Hide");
            } else {
                self.home.status = Some(format!(
                    "[home] hide = [\"{}\"] in config.toml hides it",
                    catalog.id
                ));
            }
            return;
        }
        // A place row stands for every recent under it. Forgetting them all is one
        // keystroke from forgetting one, so it asks first, the way Shift+Delete does.
        if let Some(home::Row::Place { path, held, .. }) = self.home.selected_row() {
            self.pending_forget_place = Some(path.clone());
            self.confirmation_modal.show(format!(
                "Forget {held} recently opened {} under {}?",
                if held == 1 { "dataset" } else { "datasets" },
                home::display_path(&path)
            ));
            return;
        }
        // A row of catalog.toml's own section goes from the file, as Ctrl+D on it does.
        // The same place under Recent is a recent, and Delete forgets only that.
        let in_mine = self
            .home
            .selected_section()
            .and_then(|i| self.home.sections.get(i))
            .is_some_and(|s| s.origin == Some("catalog.toml"));
        if in_mine
            && let Some((location, _)) = self.home_row_for_catalog()
            && let Some((id, name)) = self.mine_entry_at(&location)
        {
            self.home_forget_from_catalog(&id, &name);
            return;
        }
        let section_title = self
            .home
            .selected_section()
            .and_then(|i| self.home.sections.get(i))
            .map(|s| s.title.clone())
            .unwrap_or_default();
        if self.home.browsing.is_none()
            && section_title == home::HomeState::CLOUD_SECTION
            && let Some(id) = self
                .home
                .selected_entry()
                .and_then(|e| home::cloud_source_id(&e.path))
        {
            self.cache.hide_cloud_source(&id);
            self.home.cloud.retain(|s| s.id != id);
            self.home_refresh();
            return;
        }
        let in_recents = section_title == "Recent";
        if !in_recents {
            self.home.status = Some("Only recents and catalog.toml rows can be forgotten".into());
            return;
        }
        let Some(entry) = self.home.selected_entry() else {
            return;
        };
        self.cache.forget_recent(&entry.path);
        // Nothing to say: the row going is the answer.
        self.home.status = None;
        self.home_refresh();
    }

    /// The dataset or directory the highlighted row stands for, as Ctrl+D adds it: its
    /// location, and the name its row shows. A heading stands for the directory its
    /// section lists. A table inside a file, a cloud source and the rows that are not
    /// places have none.
    fn home_row_for_catalog(&self) -> Option<(PathBuf, String)> {
        match self.home.selected_row()? {
            home::Row::Place { path, .. } => {
                let name = home::display_path(&path);
                Some((path, name))
            }
            home::Row::Door { entry, .. } => {
                // A local door's trailing slash goes; a URL keeps its `//`, which
                // `components` would fold into a local path.
                let path: PathBuf = if matches!(
                    source::input_source(&entry.path),
                    source::InputSource::Local(_)
                ) {
                    entry.path.components().collect()
                } else {
                    entry.path.clone()
                };
                Some((path.clone(), home::display_path(&path)))
            }
            home::Row::Entry { entry, .. } => (entry.table.is_none()
                && !home::is_cloud_place(&entry.path)
                && crate::members::split(&entry.path).is_none())
            .then(|| (entry.path.clone(), entry.name.clone())),
            home::Row::Header { section, .. } => {
                let root = self.home.sections.get(section)?.root.clone()?;
                let name = home::display_path(&root);
                Some((root, name))
            }
            home::Row::More { .. } | home::Row::Hidden { .. } => None,
        }
    }

    /// `catalog.toml`'s path: beside the config file read, else in the config directory.
    fn mine_catalog_file(&self) -> Option<PathBuf> {
        let dir = match &self.app_config.catalog_dir {
            Some(dir) => dir.clone(),
            None => config::ConfigManager::new(APP_NAME)
                .ok()?
                .config_dir()
                .to_path_buf(),
        };
        Some(dir.join(catalog::MINE_FILE))
    }

    /// The id and name of the `catalog.toml` entry at `location`, when there is one.
    fn mine_entry_at(&self, location: &Path) -> Option<(String, String)> {
        self.app_config
            .read_catalogs
            .iter()
            .find(|c| c.origin == catalog::Origin::Mine)?
            .dataset_at(location)
            .map(|d| (d.id.clone(), d.name.clone()))
    }

    /// Read the catalogs again after `catalog.toml` changed, and list again.
    fn reload_catalogs(&mut self) -> Result<(), String> {
        let dir = self
            .mine_catalog_file()
            .and_then(|f| f.parent().map(Path::to_path_buf));
        self.app_config
            .read_catalog_files(dir.as_deref())
            .map_err(|e| e.to_string())?;
        // The open dataset's notes and Documentation tab follow the file.
        let shown = home::catalogs(&self.app_config);
        let path = self.path.clone();
        self.codebook = path.as_deref().and_then(|p| home::codebook_for(&shown, p));
        self.catalog_entry = path
            .as_deref()
            .and_then(|p| home::catalog_entry_for(&shown, p));
        self.open_info_documentation();
        self.home_refresh();
        Ok(())
    }

    /// What Ctrl+D writes for a row at `location` named `name`: a copy of what another
    /// catalog says of it, or the place as it is.
    fn new_catalog_dataset(&self, location: &Path, name: &str) -> catalog::NewDataset {
        if let Some((_, shown)) = self.home.catalog_dataset(location) {
            let entry = &shown.entry;
            return catalog::NewDataset {
                name: entry.name.clone(),
                path: entry.local_path().map(|p| home::display_path(&p)),
                url: entry.url.clone(),
                auth: entry.auth.clone(),
                connection: entry.connection.clone(),
                description: entry.description.clone(),
                size: entry.size,
            };
        }
        let mut new = catalog::NewDataset {
            name: name.to_string(),
            ..Default::default()
        };
        if matches!(
            source::input_source(location),
            source::InputSource::Local(_)
        ) {
            let absolute = if location.is_relative() {
                std::env::current_dir()
                    .map(|cwd| cwd.join(location))
                    .unwrap_or_else(|_| location.to_path_buf())
            } else {
                location.to_path_buf()
            };
            new.path = Some(home::display_path(&absolute));
            return new;
        }
        // A store reached through a named source: the source becomes the connection
        // when it is one of the config's, and the URL loses it.
        let text = location.to_string_lossy();
        let (id, plain) = source::split_source_id(&text);
        new.url = Some(plain.into_owned());
        if let Some(id) = id {
            new.connection = Some(id.to_string());
        }
        new
    }

    /// Ctrl+D: add the row under the cursor to `catalog.toml`, or forget it from there.
    pub(crate) fn home_toggle_catalog(&mut self) {
        let Some((location, name)) = self.home_row_for_catalog() else {
            self.home.status = Some("Move to a dataset or directory to add it".into());
            return;
        };
        if let Some((id, name)) = self.mine_entry_at(&location) {
            self.home_forget_from_catalog(&id, &name);
            return;
        }
        let Some(file) = self.mine_catalog_file() else {
            self.home.status = Some("No config directory to keep catalog.toml in".into());
            return;
        };
        let new = self.new_catalog_dataset(&location, &name);
        // A source found on the machine, not one of the config's connections, cannot
        // be named in a catalog: without it the URL would be read elsewhere.
        if let Some(connection) = &new.connection
            && !self
                .app_config
                .cloud
                .connections
                .iter()
                .any(|c| c.name == *connection)
        {
            self.home.status = Some(format!(
                "Not added: {connection} is not a [[cloud.connections]] entry in the config"
            ));
            return;
        }
        if let Err(why) = new.check() {
            self.home.status = Some(format!("Not added: {why}"));
            return;
        }
        let label = self
            .app_config
            .read_catalogs
            .iter()
            .find(|c| c.origin == catalog::Origin::Mine)
            .map(|c| c.label.clone())
            .unwrap_or_else(|| catalog::MINE_LABEL.to_string());
        match catalog::add(&file, &new) {
            Ok(_) => match self.reload_catalogs() {
                Ok(()) => self.flash_note(format!("Added {} to {label}", new.name)),
                Err(e) => self.error_modal.show(e),
            },
            Err(e) => self.error_modal.show(e.to_string()),
        }
    }

    /// Remove the entry `id` from `catalog.toml`.
    fn home_forget_from_catalog(&mut self, id: &str, name: &str) {
        let Some(file) = self.mine_catalog_file() else {
            return;
        };
        match catalog::forget(&file, id) {
            Ok(()) => match self.reload_catalogs() {
                Ok(()) => self.flash_note(format!("Forgot {name}")),
                Err(e) => self.error_modal.show(e),
            },
            Err(e) => self.error_modal.show(e.to_string()),
        }
    }

    /// Before 0.4.0 Ctrl+D kept directories in the cache. Once, they move into
    /// `catalog.toml`, where Ctrl+D keeps them now, and the cache's list goes.
    fn move_remembered_places(&mut self) {
        if std::mem::replace(&mut self.remembered_moved, true) {
            return;
        }
        let places = self.cache.load_remembered_places();
        if places.is_empty() {
            return;
        }
        let Some(file) = self.mine_catalog_file() else {
            return;
        };
        // The cache's list goes only once every place is in catalog.toml: a failure
        // leaves it for the next run.
        match catalog::move_places(&file, &places) {
            Ok(_) => self.cache.clear_remembered_places(),
            Err(e) => {
                log::warn!(target: "datui", "moving remembered places into catalog.toml: {e:#}")
            }
        }
        let dir = file.parent().map(Path::to_path_buf);
        if let Err(e) = self.app_config.read_catalog_files(dir.as_deref()) {
            log::warn!(target: "datui", "reading catalog.toml: {e:#}");
        }
    }

    /// Ctrl+E: the Documentation view of the row under the cursor: the catalog dataset
    /// it is or is inside, and what the format spec that reads it says.
    pub(crate) fn home_open_documentation(&mut self) {
        let Some((path, doc)) = self.home_documented_row() else {
            self.home.status =
                Some("Ctrl+E shows what a catalog or a format spec says of a row".into());
            return;
        };
        let measured = self
            .home
            .selected_entry()
            .filter(|e| {
                e.path == path
                    && doc
                        .catalog
                        .as_ref()
                        .is_some_and(|(_, entry)| entry.location() == path)
            })
            .and_then(|e| e.size);
        self.documentation.open(doc, measured);
        self.documentation.links_open = self.local_desktop;
    }

    /// What Ctrl+E documents for the row under the cursor, with the row's path: the
    /// catalog dataset it is, or is inside, and what the format spec that reads a file
    /// says of it, when the spec documents anything.
    pub(crate) fn home_documented_row(
        &self,
    ) -> Option<(PathBuf, widgets::documentation::Documented)> {
        let row = self.home.selected_row();
        let (path, file) = match &row {
            Some(home::Row::Entry { entry, .. }) | Some(home::Row::Door { entry, .. }) => {
                (Some(entry.path.clone()), Some(*entry))
            }
            Some(home::Row::Place { path, .. }) => (Some(path.clone()), None),
            Some(home::Row::Header { section, .. }) => (
                self.home
                    .sections
                    .get(*section)
                    .and_then(|s| s.root.clone()),
                None,
            ),
            _ => (None, None),
        };
        let path = path?;
        let catalog = home::catalog_entry_for(&self.home.catalogs, &path);
        let spec = file
            .filter(|e| e.kind == discover::EntryKind::File)
            .and_then(|e| e.format_spec.as_deref())
            .and_then(|name| self.home.formats.get(name))
            .and_then(|spec| spec.docs())
            .map(std::sync::Arc::new);
        // A record type's row (`day.ord/add`) documents its file.
        let name = file
            .map(|e| {
                let text = e.path.to_string_lossy();
                text.strip_suffix(e.name.as_str())
                    .filter(|_| e.table.is_some())
                    .map(|file| file.trim_end_matches(std::path::is_separator))
                    .and_then(|file| Path::new(file).file_name())
                    .map_or_else(|| e.name.clone(), |n| n.to_string_lossy().into_owned())
            })
            .unwrap_or_default();
        let doc = widgets::documentation::Documented::new(catalog, spec, name)?;
        Some((path, doc))
    }

    /// What Ctrl+D does on the row under the cursor, as the footer names it: add it to
    /// `catalog.toml`, or forget it from there; `None` on a row it cannot add.
    /// Whether Delete on the selected row hides a catalog: the heading of the one
    /// that comes with datui.
    pub(crate) fn home_hides_catalog(&self) -> bool {
        self.home
            .selected_catalog()
            .is_some_and(|c| c.origin == crate::catalog::Origin::Bundled)
    }

    pub(crate) fn home_catalog_action(&self) -> Option<&'static str> {
        let (location, _) = self.home_row_for_catalog()?;
        Some(if self.mine_entry_at(&location).is_some() {
            "Forget"
        } else {
            "Add"
        })
    }

    /// Collapse or expand the section the cursor is in.
    ///
    /// Collapsing moves the cursor to the header, so the section the user just folded
    /// is what stays selected rather than whatever row happens to fall into place.
    /// Fold or unfold the section whose header is highlighted.
    pub(crate) fn home_toggle_fold(&mut self) {
        if let Some(section) = self.home.selected_section() {
            self.home.toggle_collapsed(section);
            self.home.clamp_selection();
            self.cache.save_folds(&self.home.folds);
        }
    }

    pub(crate) fn home_collapse(&mut self, collapse: bool) {
        // The listing browsed into is the whole screen. It never folds, and the fold
        // must not be remembered for its path either — see `set_collapsed`.
        if self.home.browsing.is_some() {
            return;
        }
        let Some(section) = self.home.selected_section() else {
            return;
        };
        if collapse && !self.home.is_collapsed(section) {
            self.home.set_collapsed(section, true);
            if let Some(idx) = self
                .home
                .visible()
                .iter()
                .position(|row| row.section() == section)
            {
                self.home.selected = idx;
            }
        } else if !collapse {
            self.home.set_collapsed(section, false);
        }
        self.home.clamp_selection();
        self.cache.save_folds(&self.home.folds);
    }

    /// Step out of a directory that was descended into.
    pub(crate) fn home_ascend(&mut self) {
        let Some(current) = self.home.browsing.clone() else {
            return;
        };
        let parent = self.home.parent_of(&current);
        self.home_leave_browsing(parent);
    }

    /// Move the browse up to `to`, or back to the root listing when `None`.
    fn home_leave_browsing(&mut self, to: Option<PathBuf>) {
        // Whatever the last place said about itself, it said about that place. "these
        // are the files under it" is wrong the moment "it" is somewhere else.
        self.home.status = None;
        let from = std::mem::replace(&mut self.home.browsing, to);
        // Backspace can climb above where the browse began; the start follows, so a
        // later Esc still has a place to stop.
        if !self.home.below_browse_start() {
            self.home.browse_start = self.home.browsing.clone();
        }
        // The filter, search and row the user left here, if they were here; the
        // listing lands later, and the cursor goes back to that row when it does.
        // Somewhere new, a search of the old place no longer answers the question.
        self.home.come_back(from);
        self.home.sync_search_section();
        self.home.selected = 0;
        self.home_refresh();
        if !self.home.filter.is_empty() {
            self.spawn_home_search();
        }
    }

    /// Whether a peek's answer changes anything a row draws: its kind, or anything in
    /// `holds` (the details pane draws the whole line, and `truncated` alone turns a
    /// cloud row's `dir` into `dir+`). Each kept answer rebuilds the listing on the UI
    /// thread, so the rest are replaced by "a directory, nothing to say": the directory
    /// still has to come back to leave `peeking`.
    #[cfg(feature = "cloud")]
    pub(crate) fn peek_tells_a_row_something(
        answer: &(discover::EntryKind, discover::Holds),
    ) -> bool {
        answer.0 != discover::EntryKind::Directory || !answer.1.is_empty()
    }

    /// Look inside the cloud directories the cursor is on or near, so the ones that are
    /// datasets say `hive` or `multi` and open as one. One small listing request per
    /// directory, and each directory is peeked at once per session.
    ///
    /// A directory the listing takes for `multi` costs a little more: up to three ranged
    /// reads of a few kilobytes each, to ask the footers whether its files are really
    /// one table. Nothing else reads an object, and nothing reads a whole one.
    ///
    /// Driven by the cursor rather than by the listing. It used to take the first
    /// forty-eight directories of each listing, once: a bucket of two hundred prefixes
    /// had forty-eight labelled and the rest reading `dir` for the session however long
    /// you spent on them, and paging straight past those forty-eight spent the requests
    /// on rows nobody saw. The budget is the same shape as the local classify pass now —
    /// what is on screen, a batch at a time, the highlighted row first.
    #[cfg(feature = "cloud")]
    pub(crate) fn peek_cloud_directories(&mut self) {
        const PEEKS_AT_ONCE: usize = 4;
        let directories = self.home.cloud_directories_to_peek(PEEKS_AT_ONCE);
        if directories.is_empty() {
            return;
        }
        // Out, not answered. A second pass before these land must not ask again, and an
        // answer written here instead would be a claim — `dir` on a row that has a
        // count, and "never again this session" staked on a request that may fail.
        for directory in &directories {
            self.home.peeking.insert(directory.clone());
        }
        let tx = self.events.clone();
        let cloud = self.app_config.cloud.clone();
        self.runtime.spawn(async move {
            let permits = Arc::new(tokio::sync::Semaphore::new(PEEKS_AT_ONCE));
            let mut peeks = tokio::task::JoinSet::new();
            // By task, so a peek that panics is still sent back, as failed, and does
            // not stay in `peeking` spinning for good.
            let mut asked = std::collections::HashMap::new();
            for directory in directories {
                let (permits, cloud) = (permits.clone(), cloud.clone());
                let task_directory = directory.clone();
                let task = peeks.spawn(async move {
                    let directory = task_directory;
                    let _permit = permits.acquire_owned().await;
                    let kind =
                        crate::cloud_browse::peek_kind(&directory.to_string_lossy(), &cloud).await;
                    (directory, kind)
                });
                asked.insert(task.id(), directory);
            }
            // Sent a few at a time: the labels fill in as they are found, without a
            // rebuild per directory.
            //
            // Every directory asked about is sent back, including the ones whose peek
            // decided nothing and the ones whose request failed. That is what takes
            // them out of `peeking` and what holds the one-request-per-directory promise
            // — and an answer of "a directory, and nothing to say about it" is a real
            // answer once the request has been made, which is what it was not while it
            // was being written before the request.
            let mut found = Vec::new();
            let mut failed = Vec::new();
            while let Some(joined) = peeks.join_next_with_id().await {
                match joined {
                    Ok((_, (directory, Ok(answer)))) => {
                        let answer = Some(answer)
                            .filter(Self::peek_tells_a_row_something)
                            .unwrap_or((discover::EntryKind::Directory, Default::default()));
                        found.push((directory, answer));
                    }
                    Ok((_, (directory, Err(_)))) => failed.push(directory),
                    Err(error) => failed.extend(asked.remove(&error.id())),
                }
                if found.len() + failed.len() >= PEEKS_AT_ONCE {
                    let _ = tx.send(AppEvent::HomeCloudKinds {
                        kinds: std::mem::take(&mut found),
                        failed: std::mem::take(&mut failed),
                    });
                }
            }
            if !found.is_empty() || !failed.is_empty() {
                let _ = tx.send(AppEvent::HomeCloudKinds {
                    kinds: found,
                    failed,
                });
            }
        });
    }

    /// Browse into a directory or bucket, local or remote.
    pub(crate) fn home_browse_into(&mut self, path: PathBuf) {
        self.home.leave_mark();
        if self.home.browsing.is_none() {
            self.home.browse_start = Some(path.clone());
        } else if self.home.browse_start.is_none() {
            self.home.browse_start = self.home.browsing.clone();
        }
        self.home.browsing = Some(path);
        // "Below here" now means somewhere else. Whatever the last walk found
        // describes a different place, and a fresh one starts on the next
        // keystroke. The status line goes with them: "these are the files under it"
        // is about wherever "it" was. A caller with something to say about the place
        // it is going says it after this returns.
        self.home.status = None;
        self.home.search.reset();
        self.home.filter.clear();
        self.home.sync_search_section();
        self.home.selected = 0;
        self.home_refresh();
    }

    /// Whether the highlighted row is the `(all files)` row: the one that opens the
    /// directory being browsed, and so is already inside it.
    ///
    /// `Enter` on it opens the directory whatever the label says, and → on it would
    /// descend into where it already is.
    /// Asked of the row's variant rather than of a flag on the entry it carries: the
    /// door is a `Row::Door` now, so this is one match instead of a clone.
    fn selection_opens_the_whole_directory(&self) -> bool {
        self.home.selection_is_the_door()
    }

    /// The highlighted row, when → goes inside it.
    ///
    /// Every directory, whatever its label. A label describes what is directly inside; it
    /// no longer decides what can be reached, so the exception list this used to carry —
    /// hive, multi and the three lake markers — is gone, and with it the directories that
    /// had no way in because datui did not recognize how they were stored. What is left
    /// out is what is not a directory: a file, a section header, and the row that opens
    /// the directory you are already in.
    ///
    /// Local or remote. The split this used to carry — remote only — was never about
    /// where the directory was: a cloud prefix simply could not be descended into until
    /// there was a listing to descend with.
    pub(crate) fn selected_directory_to_enter(&self) -> Option<PathBuf> {
        // A place under `RECENT` is a directory to go inside, and → is one of its two
        // doors. It has no entry to ask about, so it is answered before one is looked for.
        if let Some(home::Row::Place { path, .. }) = self.home.selected_row() {
            return home::place_is_browsable(&path).then_some(path);
        }
        let entry = self.home.selected_entry()?;
        if self.selection_opens_the_whole_directory() || self.home.missing.contains(&entry.path) {
            return None;
        }
        // A SQLite database lists its tables, however many it has.
        if entry.cost.tables.is_some() {
            return Some(entry.path.clone());
        }
        (!matches!(
            entry.kind,
            discover::EntryKind::File | discover::EntryKind::Other
        ))
        .then_some(entry.path.clone())
    }

    /// Why a prefix in an object store cannot be read as one table, when it cannot.
    ///
    /// Every cloud path is scanned as Parquet — the directory-format dispatch is local
    /// only — so a prefix of anything else comes back "Could not read from S3. Check
    /// credentials and URL", which is a false statement about a login that is fine.
    /// What the prefix holds is already counted and on screen, so saying so costs no
    /// request. #275 phase 4 is where these read.
    ///
    /// `None` for a prefix that may yet be Parquet: one holding Parquet, and one
    /// holding no data files at all, whose data may be a level down.
    /// Why Enter on a bucket directory's `(all files)` row reads nothing, by the rule
    /// Enter itself applies: a hive root or a directory of one table is read through
    /// its files, and one with a reader for what it holds is read with that.
    #[cfg(feature = "cloud")]
    pub(crate) fn why_a_door_reads_nothing(entry: &discover::Entry) -> Option<String> {
        if !home::is_object_store_url(&entry.path)
            || matches!(
                entry.kind,
                discover::EntryKind::Hive | discover::EntryKind::MultiFile
            )
            || Self::cloud_prefix_format(&entry.holds).is_some()
        {
            return None;
        }
        Self::why_a_cloud_prefix_cannot_be_read(&entry.holds)
    }

    #[cfg(feature = "cloud")]
    pub(crate) fn why_a_cloud_prefix_cannot_be_read(holds: &discover::Holds) -> Option<String> {
        let reads_parquet =
            |name: &str| crate::FileFormat::from_name(name) == Some(crate::FileFormat::Parquet);
        if holds.formats.iter().any(|(name, _)| reads_parquet(name)) {
            return None;
        }
        match holds.formats.as_slice() {
            // Data files, none of them Parquet. `label()` says `mixed` for more than
            // one format, which is a word rather than a count, so the line is spelled
            // out from the formats themselves.
            [] => {
                // Nothing datui has a reader for. Only a refusal when there is also
                // nothing below: a prefix of sub-prefixes may hold Parquet a level
                // down, and nothing here has looked.
                (holds.not_read > 0 && holds.directories == 0).then(|| {
                    "this prefix holds nothing datui can read — datui reads a directory in \
                     an object store as Parquet only."
                        .to_string()
                })
            }
            formats => {
                let held = formats
                    .iter()
                    .map(|(name, count)| format!("{count} {name}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                Some(format!(
                    "this prefix holds {held} — datui reads a directory in an object store \
                     as Parquet only. Open one of the files below instead."
                ))
            }
        }
    }

    /// The reader a prefix in an object store calls for, from what its listing counted.
    ///
    /// The commonest format, which is the same rule a directory on disk follows — and
    /// `rank_formats` is the same order, so a prefix and the directory it mirrors pick
    /// the same reader. `None` when nothing there has a multi-file reader, which is where
    /// the refusal that names what is there belongs.
    ///
    /// Parquet included and returned as itself: the cloud branches compare against it
    /// and take their own path, which is the one every cloud dataset took before any of
    /// this, and the only one with hive partitioning behind it.
    #[cfg(feature = "cloud")]
    pub(crate) fn cloud_prefix_format(
        holds: &discover::Holds,
    ) -> Option<(FileFormat, Vec<(FileFormat, usize)>)> {
        // A saved DatasetDict: its splits are Arrow, read one at a time.
        if holds.dataset_dict {
            return Some((FileFormat::Arrow, Vec::new()));
        }
        // A model's weights beside its config and tokenizer JSON: the prefix is the
        // model, as a directory on disk is, and the JSON is not data passed over.
        if let Some((name, _)) = holds.model_weights() {
            return FileFormat::from_name(name).map(|format| (format, Vec::new()));
        }
        let (name, _) = holds.formats.first()?;
        // A GPS log is read whole from disk; a bucket's logs are opened one at a time,
        // as its text files are.
        let format = FileFormat::from_name(name)
            .filter(|f| f.reads_many_files() && !f.reads_into() && !f.is_lines())?;
        // And what taking the commonest passes over. The local read reports its own —
        // it is the pass that decides — but here Polars does the listing and never sees
        // the other formats, so the note has to be written from the listing on screen.
        let left_out = holds
            .formats
            .iter()
            .skip(1)
            .filter_map(|(name, n)| FileFormat::from_name(name).map(|f| (f, *n)))
            .collect();
        Some((format, left_out))
    }

    /// Open the highlighted entry: toggle a section, descend into a directory, or
    /// load a dataset.
    pub(crate) fn home_open_selected(&mut self) -> Option<AppEvent> {
        match self.home.selected_row() {
            // Into the directory or prefix the recents under it live in: the way back
            // to a place found by hand, now that recents no longer make roots.
            Some(home::Row::Place { path, .. }) => {
                if home::place_is_browsable(&path) {
                    self.home_browse_into(path);
                } else {
                    self.home.status = Some(
                        "An HTTP server has no listing to browse. Open a file under it".into(),
                    );
                }
                return None;
            }
            // The rest of `RECENT`, for the session.
            Some(home::Row::More { .. }) => {
                self.home.recent_expanded = true;
                return None;
            }
            // What Ctrl+A shows. The cursor goes to the first of them, where the row
            // that stood for them was.
            Some(home::Row::Hidden { .. }) => {
                self.home.hide_unreadable = false;
                if let Some(idx) = self.home.visible().iter().position(|row| {
                    matches!(row, home::Row::Entry { entry, .. }
                        if entry.hidden_by_default())
                }) {
                    self.home.selected = idx;
                }
                return None;
            }
            _ => {}
        }
        if self.home.selection_is_header() {
            self.home_toggle_fold();
            return None;
        }
        let entry = self.home.selected_entry()?.clone();
        // A collection's local dataset that is not there: said here, where it was named.
        if self.home.missing.contains(&entry.path) {
            self.home.status = Some(format!(
                "{} does not exist",
                home::display_path(&entry.path)
            ));
            return None;
        }
        // A place a collection suggests is a starting point: Enter opens it as one
        // table rather than stepping inside. → still goes in.
        if entry.kind != discover::EntryKind::File && self.home.bookmark(&entry.path).is_some() {
            #[cfg(feature = "cloud")]
            let reader = if home::is_object_store_url(&entry.path)
                && !matches!(
                    entry.kind,
                    discover::EntryKind::Hive | discover::EntryKind::MultiFile
                ) {
                Self::cloud_prefix_format(&entry.holds)
            } else {
                None
            };
            #[cfg(not(feature = "cloud"))]
            let reader = None;
            let directory = home::directory_dataset_url(&entry.path);
            return Some(self.home_open_directory_as(directory, true, None, reader));
        }
        // The `(all files)` row opens the directory it names, whatever the directory is
        // labelled. That is the whole of what it is for: the label describes, and this
        // row is the promise that the description cannot lock you out. Sent straight to
        // the open, because `open_what_it_is` would read the label back and send a
        // `dir` row inside the directory it is already in.
        if self.selection_opens_the_whole_directory() {
            // A lake table is not a directory of Parquet files however much it looks like
            // one: reading it as one counts tombstoned rows, every rewritten version
            // and both sides of a compaction. So the read is labelled rather than
            // refused. Refusing it left a directory the user could see and could not read
            // at all — this row is the promise that no label locks you out, and a
            // refusal here is that promise broken on the one directory that needed it.
            // Until datui reads the log, its files are what there is, and what makes
            // that honest is that nothing about it is silent: a note in the panel, a
            // chip in the control bar, and `Enter` on the row one level up still goes
            // inside and says datui does not read the table itself yet.
            let lake = entry.kind.lake_name();
            // A prefix in an object store used to be scanned as Parquet whatever was
            // in it — every cloud path returns before the directory-format dispatch is
            // reached — so a prefix of CSV answered "Could not read from S3. Check
            // credentials and URL", a false statement about the user's login. What the
            // prefix holds was counted by the listing and is on screen, so the reader
            // is picked from it, which costs no request. Only a prefix the listing
            // already calls a dataset is left alone: a hive root is read through its
            // partitions, and one stray `manifest.csv` beside them is not what it
            // holds — but it is the only thing in `formats`.
            #[cfg(feature = "cloud")]
            let reader = if home::is_object_store_url(&entry.path)
                && !matches!(
                    entry.kind,
                    discover::EntryKind::Hive | discover::EntryKind::MultiFile
                ) {
                let reader = Self::cloud_prefix_format(&entry.holds);
                // Nothing here datui has a reader for. The listing is on screen, so the
                // refusal names what is there rather than blaming the connection.
                if reader.is_none()
                    && let Some(what) = Self::why_a_cloud_prefix_cannot_be_read(&entry.holds)
                {
                    self.home.status = Some(what);
                    return None;
                }
                reader
            } else {
                None
            };
            #[cfg(not(feature = "cloud"))]
            let reader = None;
            // `hive: true` says read this as one, which is the whole of what the row
            // promises — it is also what carries partition columns through, for a
            // directory the dispatch sends down the hive route. The cloud route returns
            // before the dispatch is reached.
            let directory = home::directory_dataset_url(&entry.path);
            return Some(self.home_open_directory_as(directory, true, lake, reader));
        }
        // A row nothing has looked at is looked at before it is opened, rather than
        // opened as whatever it turns out to be. `EntryKind::Unknown` is offered as
        // openable, so without this a lake root reached this way is read as one table:
        // #237 through the door #249 leaves open.
        let mut entry = entry;
        if entry.kind == discover::EntryKind::Unknown {
            if self.looking_could_block(&entry.path) {
                return Some(AppEvent::ClassifyThenOpen {
                    path: entry.path,
                    jump: false,
                });
            }
            if entry.path.is_dir() {
                entry.kind = discover::classify_directory(&entry.path);
            }
        }
        // A database of several tables lists them rather than opening; one not yet
        // measured is opened, and the open lands on its tables the same way.
        if entry.enter_lists_tables() {
            self.home_browse_into(entry.path);
            return None;
        }
        self.open_what_it_is(entry.path, entry.kind, false)
    }

    /// Browse into `path` as a jump, from wherever the user was.
    ///
    /// Unlike `home_browse_into`, the browse *starts* here: Esc comes back from here to
    /// the listing rather than up through whatever the path happens to sit under.
    pub(crate) fn home_jump_into(&mut self, path: PathBuf) {
        // A new browse: Esc comes back from here to the listing, so only the listing's
        // mark is still a way back.
        self.home.trail.retain(|mark| mark.place.is_none());
        if self.home.browsing.is_none() {
            self.home.leave_mark();
        }
        self.home.browse_start = Some(path.clone());
        self.home.browsing = Some(path);
        self.home.status = None;
        self.home.search.reset();
        self.home.filter.clear();
        self.home.sync_search_section();
        self.home.selected = 0;
        self.home_refresh();
    }

    /// Load a path from the home screen.
    ///
    /// The recent entry is recorded by the `Open` handler, which every open goes
    /// through, so this does not record one itself.
    pub(crate) fn home_open_path(&mut self, path: PathBuf, hive: bool) -> AppEvent {
        self.home_open_directory(path, hive, None)
    }

    /// As [`Self::home_open_path`], and carrying whether the directory being read is a
    /// lake table whose plain files this read is, so the dataset can say so.
    fn home_open_directory(
        &mut self,
        path: PathBuf,
        hive: bool,
        lake: Option<&'static str>,
    ) -> AppEvent {
        self.home_open_directory_as(path, hive, lake, None)
    }

    /// As [`Self::home_open_directory`], naming the reader to use.
    ///
    /// For a prefix in an object store, where nothing downstream reads the listing: the
    /// cloud branches scan before the directory-format dispatch is reached, so the format
    /// the listing counted has to travel with the open or the scan falls back to
    /// Parquet, which is what it always did.
    fn home_open_directory_as(
        &mut self,
        path: PathBuf,
        hive: bool,
        lake: Option<&'static str>,
        reader: Option<(FileFormat, Vec<(FileFormat, usize)>)>,
    ) -> AppEvent {
        let (format, left_out) = match reader {
            Some((format, left_out)) => (Some(format), left_out),
            None => (None, Vec::new()),
        };
        // A directory of partitions is only meaningful read as one hive dataset. Told
        // rather than stat'ed: the caller already knows what this is, and on a share that
        // has gone away a `stat` here would freeze the thread reading the keys — the same
        // reason the size below is left to the `Open` handler.
        let options = OpenOptions {
            hive,
            read_as_plain_files_of: lake,
            format,
            left_out,
            ..self.open_defaults()
        };
        self.input_mode = InputMode::Normal;
        // Chosen here, so a failure is reported here.
        self.announce_open(true, "Scanning input".to_string(), 10);
        // A frame is drawn between this keypress and the `Open` that carries it out,
        // and it is the one the user is looking at when they press Enter — so it says
        // which file, not just that something is happening. `Open` fills in the size a
        // frame later; stat'ing here would put a possibly-dead mount on this thread.
        self.name_what_is_loading(path.clone());
        AppEvent::Open(vec![path], options)
    }

    /// The home screen's worker answers.
    pub(crate) fn home_event(&mut self, event: &AppEvent) -> Option<AppEvent> {
        match event {
            AppEvent::HomeListingReady {
                generation,
                listing,
                known,
                visits,
                newest,
                folds,
            } => {
                // Only the current listing's answer clears the flag: a stale one landing
                // first said nothing was in flight while the listing for where the user
                // is still ran. Every refresh asks again, so the newest always answers.
                if *generation == self.home_generation {
                    self.home.listing_in_flight = false;
                }
                // Read fresh from the cache, so true whichever listing carried them:
                // the facts fill in rows the recursive search finds the same way, and
                // only the first listing after entering home carries the folds.
                self.home.known = known.clone();
                self.home.set_visits(visits.clone());
                self.home.newest_recent = newest.clone();
                if let Some(folds) = folds {
                    self.home.folds = folds.clone();
                }
                // A listing from a superseded request describes somewhere the user has
                // already left.
                if *generation != self.home_generation {
                    return None;
                }
                self.home.apply_listing((**listing).clone());
                // Probes are chosen from the sections, so they can only be started
                // once those exist — asking before the listing lands finds nothing.
                self.spawn_home_probes();
                #[cfg(feature = "cloud")]
                self.spawn_cloud_discovery();
                self.request_home_measurements();
                self.request_home_classifications();
                None
            }
            AppEvent::HomeListingFailed => {
                // The rows already listed stay. The panic is flashed as a raw worker's.
                self.home.listing_in_flight = false;
                None
            }
            AppEvent::HomeMeasured { measured, done } => {
                for (path, m) in measured {
                    self.home.enriched.insert(path.clone(), m.clone());
                }
                self.home.apply_measurements();
                if *done {
                    self.home.measure_in_flight = false;
                    self.request_home_measurements();
                }
                None
            }
            AppEvent::HomeSized { path, measured } => {
                // Only the size: a measurement that landed meanwhile keeps the rest.
                match self.home.enriched.get_mut(path) {
                    Some(known) => known.size = measured.size,
                    None => {
                        self.home.enriched.insert(path.clone(), measured.clone());
                    }
                }
                self.home.apply_measurements();
                None
            }
            AppEvent::HomeWebGone { path, gone } => {
                self.home.web_gone.insert(path.clone(), gone.clone());
                None
            }
            AppEvent::HomeClassified { measured, done } => {
                // Kept even when the listing has been rebuilt since it was asked for. A
                // probe or a cloud peek landing rebuilds it, and a Recent section full of
                // buckets lands several in a row: dropping the answer each time left a
                // share's rows unlabeled for as long as the cloud kept answering.
                for (path, m) in measured {
                    self.home.enriched.insert(path.clone(), m.clone());
                }
                // Nothing re-sorts. `apply_measurements` writes the kind into the row
                // where it already is, which is the whole reason a kind is allowed to
                // arrive after the row was drawn: a listing that reshuffled itself
                // under the cursor while it filled in would be worse than a late
                // label.
                self.home.apply_measurements();
                // The next batch is chosen from the viewport as it is now, so a page
                // that scrolled past four hundred rows while this one was out asks
                // about the forty it landed on, not the four hundred it left behind.
                if *done {
                    self.home.classify_in_flight = false;
                    self.request_home_classifications();
                }
                None
            }
            AppEvent::HomePathListed { listing } => {
                // Kept only for the directory still being typed: a listing for one the
                // user has typed past would offer names from somewhere else.
                if self.home.path_input_active
                    && home::typed_dir(&self.home.path_input) == listing.dir
                {
                    self.home.path_listing = Some((**listing).clone());
                    if self.home.path_pick.is_none() {
                        self.home.pick_first_path();
                    }
                }
                None
            }
            AppEvent::HomePathCompleted {
                generation,
                typed,
                completed,
                candidates,
            } => {
                // Discard if the user has typed since asking: completing onto a
                // different string would scramble what they are in the middle of.
                if *generation != self.home_generation || &self.home.path_input != typed {
                    return None;
                }
                if *candidates == 0 {
                    self.home.status = Some("No such path".to_string());
                } else {
                    self.home.status = None;
                    if *candidates > 1 {
                        self.flash_note(format!("{candidates} matches"));
                    }
                    self.home.path_input = completed.clone();
                    self.home.pick_first_path();
                }
                None
            }
            AppEvent::HomePreviewReady {
                path,
                stamp,
                read_at,
                rows,
                prepared,
            } => {
                let prepared = prepared.lock().ok().and_then(|mut p| p.take());
                // The columns came with the rows: the pane lists them, for a CSV too.
                if let Some(prepared) = &prepared {
                    let schema = prepared
                        .state
                        .schema()
                        .iter()
                        .map(|(name, dtype)| (name.to_string(), dtype.clone()))
                        .collect();
                    self.home_schema_cache.insert(path.clone(), Some(schema));
                }
                let prepared = prepared.filter(|_| read_at.is_some());
                self.home_previews.landed(
                    path.clone(),
                    *stamp,
                    read_at.unwrap_or(*stamp),
                    rows.clone(),
                    prepared,
                );
                None
            }
            AppEvent::HomeSchemaReady {
                generation,
                path,
                preview,
            } => {
                self.home_schema_inflight.retain(|p| p != path);
                // A preview's columns are not taken back by a metadata read that had none.
                let known = self
                    .home_schema_cache
                    .get(path)
                    .is_some_and(Option::is_some);
                if *generation == self.home_generation && (preview.is_some() || !known) {
                    self.home_schema_cache.insert(path.clone(), preview.clone());
                }
                None
            }
            AppEvent::HomeSearchBatch {
                generation,
                root,
                found,
                scanned,
            } => {
                // Results from a walk that a later navigation superseded describe a
                // place the user has left. The walk is abandoned, not cancelled, so
                // late batches are expected rather than exceptional. A refresh of the
                // same place supersedes nothing: its end dropped kept it running.
                if *generation == self.home_search_generation {
                    self.home.search_batch(root, found.clone(), *scanned);
                }
                None
            }
            AppEvent::HomeSearchScored { epoch, matches } => {
                // A scoring that died is not asked again: the next would die the same
                // way, and the matches already listed stand.
                if let Some(matches) = matches {
                    self.home.search_scored(*epoch, (**matches).clone());
                }
                None
            }
            AppEvent::HomeSearchDone {
                generation,
                root,
                scanned,
                limited,
            } => {
                if *generation == self.home_search_generation {
                    self.home.search_finished(root, *scanned, limited.clone());
                }
                self.home_search_inflight = false;
                // A walk abandoned by a browse held up the one the filter now asks for.
                if !self.home.filter.is_empty() && self.home.search.root.is_none() {
                    self.spawn_home_search();
                }
                None
            }
            #[cfg(feature = "cloud")]
            AppEvent::HomeCloudSources { sources } => {
                self.home.cloud = sources.clone();
                self.home_refresh();
                None
            }
            #[cfg(feature = "cloud")]
            AppEvent::HomeCloudListed {
                id,
                buckets,
                details,
                failure,
                listed_at,
            } => {
                if let Some(source) = self.home.cloud.iter_mut().find(|s| &s.id == id) {
                    source.refreshing = false;
                    for (place, lines) in details {
                        source.place_details.insert(place.clone(), lines.clone());
                    }
                    match failure {
                        // A refresh that failed keeps what the last one found: stale
                        // buckets are more use than none, and the row says it failed.
                        Some((short, detail)) => {
                            for bucket in buckets {
                                if !source.buckets.contains(bucket) {
                                    source.buckets.push(bucket.clone());
                                }
                            }
                            source.status = home::CloudStatus::Failed {
                                short: short.clone(),
                                detail: detail.clone(),
                            };
                        }
                        None => {
                            source.buckets = buckets.clone();
                            source.status = home::CloudStatus::Listed;
                            source.listed_at = Some(*listed_at);
                        }
                    }
                }
                self.home_refresh();
                None
            }
            AppEvent::HomeNarrowed {
                dir,
                prefix,
                listed,
            } => {
                // Only the request out now: one replaced by a later key may still
                // answer, after the later one, and put back the shorter prefix.
                let asked = self
                    .home_narrowing
                    .as_ref()
                    .is_some_and(|(d, p, _)| d == dir && p == prefix);
                if asked {
                    self.home_narrowing = None;
                }
                // Only while it is still where the user is and what the filter asks.
                let wanted = asked
                    && self.home.browsing.as_ref() == Some(dir)
                    && !self.home.filter.is_empty();
                if let (Some((rows, truncated)), true) = (listed, wanted) {
                    self.home.narrowed = Some(home::Narrowed {
                        dir: dir.clone(),
                        prefix: prefix.clone(),
                        rows: rows.clone(),
                        truncated: *truncated,
                    });
                    self.home_refresh();
                }
                None
            }
            AppEvent::HomeProbeCancelled { root } => {
                self.home_probes_inflight.retain(|p| p != root);
                self.home_listing_cancels.remove(root);
                self.home.probes.stopped(root);
                // Come back to after it had stopped: listed afresh.
                if self.home.browsing.as_ref() == Some(root) {
                    self.home_refresh();
                }
                None
            }
            AppEvent::HomeProbeFailed { root, message } => {
                self.home_probes_inflight.retain(|p| p != root);
                self.home_listing_cancels.remove(root);
                self.home.probe_failed(root.clone(), Some(message.clone()));
                self.home_refresh();
                None
            }
            AppEvent::HomeProbeProgress { root, rows } => {
                // Only while that listing is still out: a late batch must not paint
                // over the whole answer.
                if self.home_probes_inflight.contains(root) && !self.home.probes.settled(root) {
                    self.home.probes.read(root, rows);
                    // Listed once a frame, however many batches came in it.
                    self.home_refresh_owed = true;
                }
                None
            }
            AppEvent::HomeProbeReady {
                root,
                rows,
                cut_short,
            } => {
                // Give the slot back. The cap exists to bound threads wedged on a dead
                // `hard` mount, which never send this event and so keep their slot for
                // good — a probe that answered is not one of those. Without this the
                // list only grows, and after MAX_CONCURRENT_PROBES roots no further
                // root is ever probed for the rest of the session.
                self.home_probes_inflight.retain(|p| p != root);
                self.home_listing_cancels.remove(root);
                let landed = rows.is_some();
                match rows {
                    Some(rows) => self
                        .home
                        .probe_ready(root.clone(), rows.clone(), *cut_short),
                    None => self.home.probe_failed(root.clone(), None),
                }
                // A filter typed while it was listing asks the server too.
                #[cfg(feature = "cloud")]
                if *cut_short && !self.home.filter.is_empty() {
                    self.narrow_cloud_listing();
                }
                // An account read with its keys because the sign-in has no data role
                // says so beside the account.
                #[cfg(feature = "cloud")]
                if let Some((account, _, _)) = source::azure_parts(&root.to_string_lossy())
                    && crate::azure::remembered_key(&account).is_some()
                {
                    for source in &mut self.home.cloud {
                        let place = source
                            .buckets
                            .iter()
                            .find(|b| home::cloud_account(b).is_some_and(|(_, a)| a == account))
                            .cloned();
                        if let Some(place) = place {
                            let lines = source.place_details.entry(place).or_default();
                            if !lines.iter().any(|(k, _)| k == "access") {
                                lines.push(("access".to_string(), "access key".to_string()));
                            }
                        }
                    }
                }
                // Rebuild so the listing picks the result up; the probe is the only
                // thing that ever reads a remote root.
                self.home_refresh();
                // And then ask about the rows it brought. After the rebuild, never
                // before: the picker reads `visible()`, which is written by the
                // rebuild, so a peek asked between `probe_ready` and here looks at the
                // previous listing and finds nothing in it to ask about.
                //
                // Asked here at all because a listing that lands while the cursor is
                // already where it will stay may draw no further frame, and the frame
                // is what otherwise notices.
                #[cfg(feature = "cloud")]
                if landed {
                    self.peek_cloud_directories();
                }
                #[cfg(not(feature = "cloud"))]
                let _ = landed;
                None
            }
            AppEvent::HomeCloudKinds { kinds, failed } => {
                for directory in failed {
                    self.home.peeking.remove(directory);
                    self.home.peek_failed.insert(directory.clone());
                }
                let roots: Vec<PathBuf> = self
                    .home
                    .probes
                    .answered()
                    .map(|(root, _)| root.clone())
                    .collect();
                for (directory, kind) in kinds {
                    // Answered: out of the in-flight set and into the one the rows are
                    // labelled from. Every directory asked about comes back, so nothing
                    // stays in `peeking` and nothing is asked twice.
                    self.home.peeking.remove(directory);
                    self.home
                        .cloud_kinds
                        .insert(directory.clone(), kind.clone());
                }
                for root in roots {
                    self.home.apply_cloud_kinds(&root);
                }
                self.home_refresh();
                None
            }
            _ => unreachable!("not a home event"),
        }
    }
}

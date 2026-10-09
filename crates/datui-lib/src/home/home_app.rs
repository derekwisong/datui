//! The home screen's work: listing, probing remote places, measuring and
//! classifying rows, previews, search, catalogs and the cloud sources, and the
//! answers its workers send back.

use crate::app::background::{CacheWrites, OwedAnswer};
use crate::app::feedback::Confirm;
use crate::cache::CacheManager;
use crate::cli::FileFormat;
use crate::loading::open_options::OpenOptions;
#[cfg(feature = "cloud")]
use crate::wait_on_runtime;
use crate::{
    APP_NAME, App, AppEvent, InputMode, cloud::source, config, home, home::catalog, home::discover,
    loading, widgets,
};
use color_eyre::Result;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// The home screen's work in flight and what it keeps for the session: probes, listings,
/// search, previews and schemas.
#[derive(Default)]
pub struct HomeApp {
    /// Network roots being listed off-thread, so a probe is not started twice. Never
    /// removed for a root that does not answer: its thread is lost, and a retry would
    /// lose another.
    pub(crate) probes_inflight: Vec<PathBuf>,
    /// When each listing out last sent news (began, or a page): one silent past
    /// [`PROBE_PATIENCE`] is shown not answering rather than spun for all session.
    pub(crate) probe_heard: HashMap<PathBuf, std::time::Instant>,
    /// The wait before a silent listing is said not to answer, for tests.
    #[cfg(test)]
    pub(crate) probe_patience: Option<std::time::Duration>,
    /// Held by a test, a lock every probe waits on before listing: a share that does
    /// not answer.
    #[cfg(test)]
    pub(crate) probe_gate: Option<Arc<Mutex<()>>>,
    /// Each cloud listing's stop flag, by place: leaving the place stops it before its
    /// next page.
    pub(crate) listing_cancels: HashMap<PathBuf, Arc<std::sync::atomic::AtomicBool>>,
    /// The listing a filter asked of a cut-short cloud directory: where, the name
    /// prefix, and its stop flag.
    pub(crate) narrowing: Option<(PathBuf, String, Arc<std::sync::atomic::AtomicBool>)>,
    /// Cloud discovery has started. It costs a request per provider, so it runs once
    /// per session.
    #[cfg(feature = "cloud")]
    pub(crate) cloud_discovery_started: bool,
    /// A recursive search below the working directory is out. One at a time: a second
    /// would only compete for the disk.
    pub(crate) search_inflight: bool,
    /// The home generation the walk started in. Its batches stay its own across
    /// refreshes; the root decides whether they still apply.
    pub(crate) search_generation: u64,
    /// Why the last open failed, shown at home when the error is dismissed with nothing
    /// to fall back to.
    pub(crate) last_load_error: Option<String>,
    /// The screen's height when the last frame had room for the selected file's first
    /// rows; `None` when it had none, and nothing is read for them.
    pub preview_room: Option<u16>,
    /// The schema read out, if any: one at a time.
    pub(crate) schema_inflight: Option<PathBuf>,
    /// Invalidates listings and measurements from a request the user has moved past.
    pub(crate) generation: u64,
    /// Places whose listing sent rows since the last frame; their sections are built
    /// again from the rows before the next.
    pub(crate) pages_owed: Vec<PathBuf>,
    /// Directories the `~` prompt asked a worker to list, until each answers: typing
    /// within one directory asks once.
    pub(crate) path_listings_out: std::collections::HashSet<String>,
    /// The cache's dataset index has been read this session; later listings use it
    /// as kept, without a scan of the cache.
    pub(crate) facts_read: bool,
    /// The dataset left for home, whose open may have recorded facts the index lacks.
    pub(crate) left: Option<PathBuf>,
    /// Index records already dated as used this session: each is dated once.
    pub(crate) facts_dated: Arc<Mutex<std::collections::HashSet<PathBuf>>>,
    /// Schema previews, memoized for the session only (persisted, they would go stale).
    pub(crate) schema_cache: HashMap<PathBuf, Option<discover::SchemaPreview>>,
    /// The home screen's `ROWS` previews, and the dataset the newest one built.
    pub previews: crate::home::home_preview::Previews,
    /// The pre-0.4.0 Ctrl+D directories in the cache have been moved to `catalog.toml`.
    pub(crate) remembered_moved: bool,
    /// Which home workers panic before starting, for tests. The jobs' own is
    /// [`Jobs::worker_dies`].
    #[cfg(test)]
    pub(crate) worker_dies: Option<crate::HomeWorkerDies>,
    /// Whether a browser opened here appears in front of the user: `o` on a doc link
    /// is offered only then (`link_open::local_desktop`).
    pub local_desktop: bool,
    /// The reads of data started this session, by kind.
    pub reads: crate::home::home_preview::ReadCounts,
}

/// Rows measured per background pass: small, so a slow filesystem shows progress.
const MEASURE_BATCH: usize = 12;

/// Rows a probe measures while it is already reading a remote directory.
const PROBE_MEASURE_LIMIT: usize = 24;

/// Rows one classification pass looks into: a cap on work in flight, the next pass
/// chosen from the viewport when this one lands. Small like [`MEASURE_BATCH`], so a
/// screen fills in rather than going silent.
pub(crate) const CLASSIFY_BATCH: usize = 16;

/// How long answers wait to share an event: a batch of local files lands as one, and a
/// slow share still shows its rows a few times a second.
const ANSWER_EVERY: std::time::Duration = std::time::Duration::from_millis(250);

/// Look into `rows` on this thread, sending what was learned as `answer` events, the
/// last one `done`. One event per batch where the batch is quick: each event is a pass
/// of the event loop, and a frame.
fn look_and_answer(
    rows: Vec<discover::Entry>,
    cache: &CacheManager,
    known: &home::Known,
    tx: &std::sync::mpsc::Sender<AppEvent>,
    answer: impl Fn(Vec<(PathBuf, home::Measured)>, bool) -> AppEvent,
) {
    let mut held = Vec::new();
    let mut since = std::time::Instant::now();
    home::look_into_batch(rows, cache, known, |path, measured| {
        // A row's later answer (its count after its kind) replaces one not yet sent.
        match held.iter().position(|(held, _)| *held == path) {
            Some(at) => held[at].1 = measured,
            None => held.push((path, measured)),
        }
        if since.elapsed() >= ANSWER_EVERY {
            let _ = tx.send(answer(std::mem::take(&mut held), false));
            since = std::time::Instant::now();
        }
    });
    let _ = tx.send(answer(held, true));
}

/// How long a listing goes without a page or an answer before it is shown not
/// answering: a dead share never answers, and its spinner would turn all session.
const PROBE_PATIENCE: std::time::Duration = std::time::Duration::from_secs(30);

/// Probes allowed at once: a probe of a gone share holds its thread until exit.
pub(crate) const MAX_CONCURRENT_PROBES: usize = 4;

/// A number per home search walk, so one walk's scorings are never taken for
/// another's over the same place.
fn next_search_epoch() -> u64 {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

/// A source as a home-screen row, with the last run's buckets when they still apply.
#[cfg(feature = "cloud")]
pub(crate) fn home_cloud_source(
    source: &crate::cloud::cloud_sources::Source,
    cached: Option<&crate::cache::CloudListing>,
    listing: bool,
) -> home::CloudSource {
    let mut details: Vec<(String, String)> = vec![
        ("source".to_string(), source.id.clone()),
        ("api".to_string(), source.kind.name().to_string()),
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
    // A source that cannot list says what to do where the row has room; the count
    // carries the short problem.
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
        api: source.kind,
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
    /// Schema for a home entry, from Parquet metadata or the preview's read, memoized for
    /// the session. `None` until read, or when not knowable without a scan, and the UI
    /// says so. Asked for by [`Self::request_home_schema`], never by a frame.
    pub fn home_schema(&self, entry: &discover::Entry) -> Option<discover::SchemaPreview> {
        self.home_app
            .schema_cache
            .get(&entry.path)
            .cloned()
            .flatten()
    }

    /// What a home-screen worker owes in place of its answer if it panics.
    fn owed_answer(&mut self, instead: AppEvent) -> OwedAnswer {
        OwedAnswer {
            tx: self.events.clone(),
            #[cfg(test)]
            dies: self
                .home_app
                .worker_dies
                .as_mut()
                .is_some_and(|dies| dies(&instead)),
            instead: Some(instead),
        }
    }

    /// List the directory the `~` prompt is typing, if not already listed: a URL from
    /// what the screen knows, a local directory on a worker.
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
        if !self.home_app.path_listings_out.insert(dir.clone()) {
            return;
        }
        // Off the UI thread: a typed path may name a dead mount.
        let tx = self.events.clone();
        let owed = self.owed_answer(AppEvent::HomePathListed {
            listing: Box::new(home::PathListing {
                dir: dir.clone(),
                names: Vec::new(),
                failed: true,
                matched: Default::default(),
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
        let generation = self.home_app.generation;
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

    /// A home file's first rows for its `ROWS` preview, read on a worker as its open
    /// reads them. `None` until they land or when not previewed. Asked for by
    /// [`Self::request_selected_preview`], never by a frame.
    pub fn home_preview_rows(
        &self,
        entry: &discover::Entry,
    ) -> Option<Arc<crate::home::home_preview::PreviewRows>> {
        let stamp = crate::home::home_preview::Stamp::of_entry(entry);
        self.home_app.previews.rows(&entry.path, stamp).flatten()
    }

    /// Read the highlighted file's first rows when the last frame had room to show them
    /// and nothing has: one read at a time, for the row the cursor is on when the last
    /// one lands. `preview_room` is the screen's height, which sizes the page to the
    /// table's.
    pub(crate) fn request_selected_preview(&mut self) {
        let Some(screen_height) = self.home_app.preview_room else {
            return;
        };
        if self.home_app.previews.inflight.is_some() || self.home.path_input_active {
            return;
        }
        let Some(entry) = self.home.selected_entry() else {
            return;
        };
        let max = self.app_config.home.preview_max.bytes();
        if !crate::home::home_preview::previewable(entry, max) {
            return;
        }
        let stamp = crate::home::home_preview::Stamp::of_entry(entry);
        if self.home_app.previews.rows(&entry.path, stamp).is_some() {
            return;
        }
        let path = entry.path.clone();
        self.request_home_preview(path, stamp, screen_height);
    }

    /// Whether `entry` is one the preview reads, before its rows are in.
    pub fn home_preview_pending(&self, path: &Path) -> bool {
        self.home_app.previews.reading(path)
    }

    /// Read a file's first page on a worker through the open's own scan and schema read,
    /// so the open can install the result.
    fn request_home_preview(
        &mut self,
        path: PathBuf,
        stamp: crate::home::home_preview::Stamp,
        screen_height: u16,
    ) {
        self.home_app.previews.inflight = Some(path.clone());
        self.home_app.reads.previews += 1;
        let tx = self.events.clone();
        let cloud = self.app_config.cloud.clone();
        let formats = self.formats.clone();
        let runtime = self.runtime.clone();
        let cache = self.cache.clone();
        let writes = self.cache_writes.clone();
        // The table's rows: the screen less the title, the header and the footer.
        let visible = (screen_height as usize).saturating_sub(3).max(1);
        let owed = self.owed_answer(AppEvent::HomePreviewReady {
            path: path.clone(),
            stamp,
            read_at: None,
            rows: None,
            prepared: crate::home::home_preview::Handoff::default(),
        });
        self.runtime.spawn_blocking(move || {
            owed.run(|| {
                let began = std::time::Instant::now();
                let read_at = crate::home::home_preview::Stamp::of_file(&path);
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

    /// What opening `path` from home reads first: scan, schema and the page for
    /// `visible` rows, built by the open's own steps and options so the dataset is the
    /// one the open would build.
    fn read_home_preview(
        path: &Path,
        cloud: &crate::config::CloudConfig,
        formats: &crate::formats::Registry,
        runtime: &tokio::runtime::Handle,
        cache: CacheManager,
        writes: CacheWrites,
        visible: usize,
    ) -> Option<(
        crate::home::home_preview::PreviewRows,
        crate::home::home_preview::Prepared,
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
        let progress = Arc::<crate::formats::schema_union::FooterProgress>::default();
        let report = crate::loading::measurements::OpenReport {
            progress: progress.clone(),
            meter: Arc::new(crate::loading::measurements::Meter::default()),
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
        let df =
            crate::analysis::statistics::collect_lazy(request.lf, request.polars_streaming).ok()?;
        let result = request.plan.fit(df);
        let rows = crate::home::home_preview::PreviewRows::from_frame(result.rows());
        state.measurements().read_page(began.elapsed(), Some(1));
        state.apply_async_collect(result);
        Some((
            rows,
            crate::home::home_preview::Prepared {
                state,
                options,
                debug_label,
                progress,
            },
        ))
    }

    /// Whether a schema read is currently out for this path.
    pub fn home_schema_pending(&self, path: &Path) -> bool {
        self.home_app.schema_inflight.as_deref() == Some(path)
    }

    /// Read the highlighted dataset's schema on a worker, when nothing has and no read
    /// is out: one at a time, for the row the cursor is on when the last one lands, so
    /// a held arrow key reads where it stops rather than every row it passes. A file
    /// the preview reads gets its columns from that read instead.
    pub(crate) fn request_home_schema(&mut self) {
        if self.home_app.schema_inflight.is_some() || self.home.path_input_active {
            return;
        }
        let Some(entry) = self.home.selected_entry() else {
            return;
        };
        // Nothing in an object store is read before it is opened.
        // A file whose first rows are shown brings its columns with them.
        let previewed = self.home_app.preview_room.is_some()
            && crate::home::home_preview::previewable(
                entry,
                self.app_config.home.preview_max.bytes(),
            );
        if home::is_cloud_place(&entry.path)
            || home::is_object_store_url(&entry.path)
            || self.home_app.schema_cache.contains_key(&entry.path)
            || previewed
        {
            return;
        }
        let entry = entry.clone();
        let network = (self.home.network_check)(&entry.path);
        self.home_app.schema_inflight = Some(entry.path.clone());
        self.home_app.reads.schemas += 1;
        let tx = self.events.clone();
        // Remembered as none on failure, so it is not asked again.
        let owed = self.owed_answer(AppEvent::HomeSchemaReady {
            path: entry.path.clone(),
            preview: None,
        });
        let read = move || {
            owed.run(|| {
                let preview = discover::schema_preview(&entry);
                let _ = tx.send(AppEvent::HomeSchemaReady {
                    path: entry.path,
                    preview,
                });
            })
        };
        // A share that stops answering holds its thread: never one of the pool that loads
        // data.
        if network {
            std::thread::spawn(read);
        } else {
            self.runtime.spawn_blocking(read);
        }
    }

    /// Start listing network roots that have not answered yet. Never waits: a gone
    /// share (a `hard` NFS mount) blocks its thread uninterruptibly, so the task is
    /// abandoned, not joined.
    pub(crate) fn spawn_home_probes(&mut self) {
        self.stop_listings_left_behind();
        for root in self.home.pending_probes() {
            if self.home_app.probes_inflight.contains(&root) {
                // Left and returned to before its next page: it goes on.
                if let Some(cancelled) = self.home_app.listing_cancels.get(&root) {
                    cancelled.store(false, std::sync::atomic::Ordering::Relaxed);
                }
                continue;
            }
            // Each probe of an unreachable share costs a thread forever, so they are capped.
            // The browsed directory is exempt: it is the whole screen, and the user bounds how
            // many they open.
            let browsed = self.home.browsing.as_ref() == Some(&root);
            if !browsed && self.home_app.probes_inflight.len() >= MAX_CONCURRENT_PROBES {
                continue;
            }
            self.home_app.probes_inflight.push(root.clone());
            (self.home_app.probe_heard).insert(root.clone(), std::time::Instant::now());
            #[cfg(test)]
            let gate = self.home_app.probe_gate.clone();
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
                self.home_app
                    .listing_cancels
                    .insert(root.clone(), flag.clone());
                flag
            };
            // A detached thread, not the runtime's blocking pool: a thread wedged on a dead
            // mount never returns, and must not eat the pool that loads data.
            std::thread::spawn(move || {
                owed.run(|| {
                    #[cfg(test)]
                    if let Some(gate) = gate {
                        let _held = gate.lock();
                    }
                    // A bucket or prefix: listed with an object-store listing, not `read_dir` (which
                    // fails on `gs://`). Metadata only: names, sizes and times for one level; no
                    // footers, schemas or counts, which would cost a paid ranged read per row.
                    #[cfg(feature = "cloud")]
                    if let Some((id, account)) = home::cloud_account(&root) {
                        let listed = wait_on_runtime(&runtime, async move {
                            crate::cloud::cloud_browse::list_account(&id, &account, &cloud).await
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
                    if crate::cloud::cloud_browse::split_bucket_url(&root.to_string_lossy())
                        .is_some()
                        || source::azure_parts(&root.to_string_lossy()).is_some()
                    {
                        let url = root.to_string_lossy().into_owned();
                        // Each page's rows are drawn as they come; leaving the place stops the listing.
                        let watch = crate::cloud::cloud_browse::Watch {
                            progress: Some(std::sync::Arc::new({
                                let (tx, root) = (tx.clone(), root.clone());
                                move |page: &[crate::home::discover::Entry]| {
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
                            crate::cloud::cloud_browse::list_objects_watched(&url, &cloud, &watch)
                                .await
                        });
                        // A refused listing says why, rather than reading as a place that stopped
                        // answering.
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
                    // No cloud feature: `read_dir` on a bucket URL would only say unavailable.
                    #[cfg(not(feature = "cloud"))]
                    if source::is_remote_url(&root) {
                        let message = "cloud support not in this build".to_string();
                        let _ = tx.send(AppEvent::HomeProbeFailed { root, message });
                        return;
                    }
                    let mut cut_short = false;
                    let rows = if std::fs::read_dir(&root).is_ok() {
                        // What has been read shows while the rest is read (seconds for thousands of files).
                        let scan = crate::home::discover::scan_dir_progressive(&root, |read| {
                            let _ = tx.send(AppEvent::HomeProbeProgress {
                                root: root.clone(),
                                rows: read.to_vec(),
                            });
                        });
                        cut_short = scan.truncated;
                        let mut rows = scan.entries;
                        // Measured here too: this thread is already the one allowed to block on the share.
                        for row in rows.iter_mut().take(PROBE_MEASURE_LIMIT) {
                            crate::home::discover::enrich(row);
                        }
                        // Remote datasets are measured nowhere else, so remember them here; otherwise a
                        // remote row is blank on every run.
                        let mounts = crate::home::locality::Mounts::current();
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

    /// Stop cloud listings of places no longer on screen (the browsed one, or home's
    /// roots). A place left and revisited is listed again.
    fn stop_listings_left_behind(&mut self) {
        let home = &self.home;
        for (root, cancelled) in &self.home_app.listing_cancels {
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
        if let Some((dir, _, cancelled)) = &self.home_app.narrowing
            && home.browsing.as_ref() != Some(dir)
        {
            cancelled.store(true, std::sync::atomic::Ordering::Relaxed);
            self.home_app.narrowing = None;
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

    /// In a cloud directory cut short at the cap, ask the server for names starting with
    /// the filter, so names past the cap can be found. Nothing when the filter is empty
    /// or what is held answers it.
    #[cfg(feature = "cloud")]
    pub(crate) fn narrow_cloud_listing(&mut self) {
        let dir = self.home.browsing.clone();
        let prefix = dir.as_ref().and_then(|dir| {
            if !self.home.probes.cut_short(dir) {
                return None;
            }
            let rows = self.home.probes.listed(dir)?;
            let names: Vec<&str> = rows.iter().map(|row| row.name.as_str()).collect();
            crate::cloud::cloud_browse::narrowing_prefix(&self.home.filter, &names)
        });
        let (Some(dir), Some(prefix)) = (dir, prefix) else {
            if let Some((_, _, cancelled)) = self.home_app.narrowing.take() {
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
        if let Some((d, p, _)) = &self.home_app.narrowing
            && *d == dir
            && *p == prefix
        {
            return;
        }
        if let Some((_, _, cancelled)) = self.home_app.narrowing.take() {
            cancelled.store(true, std::sync::atomic::Ordering::Relaxed);
        }
        let cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));
        self.home_app.narrowing = Some((dir.clone(), prefix.clone(), cancelled.clone()));
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
                let watch = crate::cloud::cloud_browse::Watch {
                    progress: None,
                    cancelled,
                    names_from: Some(prefix.clone()),
                };
                let listed = wait_on_runtime(&runtime, async move {
                    crate::cloud::cloud_browse::list_objects_watched(&url, &cloud, &watch).await
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
        if self.home_app.cloud_discovery_started {
            return;
        }
        self.home_app.cloud_discovery_started = true;
        let list = self.app_config.cloud.list_on_start;
        self.list_cloud_sources(None, list);
    }

    /// List the browsed source if not asked this session; entering a source asks.
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

    /// Send every source's rows, or list the named one's buckets. Rows go first, filled
    /// from the last run's listing when still valid. With `list`, sources are listed a
    /// few at a time and each result sent on arrival; without, nothing leaves the
    /// machine. On the runtime, not a detached thread: every call has a global timeout.
    #[cfg(feature = "cloud")]
    fn list_cloud_sources(&mut self, only: Option<String>, list: bool) {
        let tx = self.events.clone();
        let cloud = self.app_config.cloud.clone();
        let cache = self.cache.clone();
        self.runtime.spawn(async move {
            let mut hidden = cache.load_hidden_cloud_sources();
            hidden.extend(cloud.hide.iter().cloned());
            let cached_for = |source: &crate::cloud::cloud_sources::Source| {
                cache.cloud_listing(&source.id, &source.fingerprint())
            };
            // Looked for again, and kept for the opens and listings that follow.
            let found = crate::cloud::cloud_sources::rediscover(&cloud).to_vec();
            // A bucket under Recent opens with the login that listed it, shown or not, unless
            // its source is hidden (perhaps for a dead login; the default opens it then).
            // Only with the rows, so an old listing never overrides a newer one.
            if only.is_none() {
                for source in found.iter().filter(|s| !hidden.contains(&s.id)) {
                    if let Some(cached) = cached_for(source) {
                        crate::cloud::cloud_sources::remember_listed(source, &cached.buckets);
                    }
                }
            }
            let sources: Vec<crate::cloud::cloud_sources::Source> =
                crate::cloud::cloud_sources::on_home(found, &cloud)
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
                // Gone since its row was drawn (a profile removed, a source hidden): said, so the
                // row does not wait forever.
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

            // One slow endpoint does not delay the rest, and many sources do not storm.
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
                    let result = crate::cloud::cloud_browse::list_first_level(&source).await;
                    (source, result)
                });
            }
            while let Some(joined) = listings.join_next().await {
                let Ok((source, result)) = joined else {
                    continue;
                };
                let listed_at = std::time::SystemTime::now();
                // Buckets named in the config show whether or not the login can list them.
                let mut names = source.buckets.clone();
                let mut details = Vec::new();
                let failure = match result {
                    Ok(listed) => {
                        for item in listed {
                            crate::cloud::cloud_sources::remember_bucket(&source, &item.name);
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

    /// Ask again for what is on screen, bypassing the cache: the browsed source's
    /// buckets, the browsed bucket or directory, or every source's buckets.
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
        // A listing that went silent is waited on again: its thread is still out, and a
        // second on the same share would stick as it has.
        for place in self.home.probes.silent_places() {
            self.home.probes.forget(&place);
            (self.home_app.probe_heard).insert(place, std::time::Instant::now());
        }
        if let Some(dir) = self.home.browsing.clone() {
            // Its answer, not a listing still coming in.
            if self.home.probes.settled(&dir) {
                self.home.probes.forget(&dir);
            }
        }
        // Ctrl+R retries failed peeks and missing web files too.
        self.home.peek_failed.clear();
        for path in std::mem::take(&mut self.home.web_gone).into_keys() {
            self.home.sized.remove(&path);
        }
        self.home.status = None;
        self.home_refresh();
    }

    /// Start the recursive search below the working directory, if wanted and not
    /// running. Triggered by typing, not by opening home, so opening a recent costs no
    /// walk.
    pub(crate) fn spawn_home_search(&mut self) {
        if self.home_app.search_inflight || self.home.search.done {
            return;
        }
        let config = self.app_config.home.search.clone();
        if !config.enabled {
            return;
        }
        let Some(root) =
            crate::home::search::search_root(self.home.browsing.as_ref(), self.home.network_check)
        else {
            return;
        };

        self.home.search.reset();
        self.home.search.root = Some(root.clone());
        self.home.search.running = true;
        self.home.search.epoch = next_search_epoch();
        self.home.search_limit = config.max_results;
        self.home_app.search_inflight = true;

        let generation = self.home_app.generation;
        self.home_app.search_generation = generation;
        let tx = self.events.clone();
        let formats = self.formats.clone();
        let known = self.home.known.clone();
        // Ended, with what the batches already found kept.
        let owed = self.owed_answer(AppEvent::HomeSearchDone {
            generation,
            root: root.clone(),
            scanned: 0,
            limited: Some(crate::glyphs::dotted("partial · failed")),
        });
        // A detached thread, as for probes: a filesystem stall must not stop drawing.
        std::thread::spawn(move || {
            owed.run(|| {
                let walk_root = root.clone();
                let batch_tx = tx.clone();
                let batch_gen = generation;
                let batch_root = root.clone();
                let outcome = crate::home::search::walk_recalling(
                    &walk_root,
                    &config,
                    &formats,
                    &known,
                    move |found, outcome| {
                        // Sent even when empty: it carries progress, and a failed send tells the walk
                        // nobody listens.
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

    /// Score the filter against the search's files on a worker, when owed. Asked after
    /// every event; over tens of thousands of files scoring held back keystroke echo.
    /// One at a time; each answer asks for the next if the filter moved.
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
                let matches = crate::home::search::score(
                    &job.results,
                    &job.query,
                    job.base.as_ref(),
                    job.limit,
                );
                let _ = tx.send(AppEvent::HomeSearchScored {
                    epoch,
                    matches: Some(Box::new(matches)),
                });
            })
        });
    }

    /// Rebuild the home listing from the filesystem.
    pub(crate) fn home_refresh(&mut self) {
        // Every way into a source comes through here: Enter, Backspace up from a bucket,
        // a jump, and rows arriving while it is open.
        #[cfg(feature = "cloud")]
        self.list_browsed_cloud_source();
        // Listings of where the user was are pages for nobody.
        self.stop_listings_left_behind();
        self.home_app.generation = self.home_app.generation.wrapping_add(1);
        let generation = self.home_app.generation;

        self.move_remembered_places();
        let mut catalogs = home::catalogs(&self.app_config);
        // Hidden with Delete on its heading: only the bundled catalog, never a user's
        // `examples.toml`.
        if self.cache.examples_hidden() {
            catalogs.retain(|c| c.origin != crate::home::catalog::Origin::Bundled);
        }
        self.home.set_catalogs(catalogs);
        let mut request = home::ListingRequest {
            // Filled on the worker from the cache and the desktop's recents, so the first frame
            // waits on no file.
            recents: Vec::new(),
            desktop_dirs: Vec::new(),
            browsing: self.home.browsing.clone(),
            probes: self.home.probes.clone(),
            narrowed: self.home.narrowed.clone(),
            network_check: self.home.network_check,
            cloud: self.home.cloud.clone(),
            catalogs: self.home.catalogs.clone(),
            known: self.home.known.clone(),
            formats: self.formats.clone(),
        };
        let read_folds = std::mem::take(&mut self.home.folds_owed);
        let desktop = self.app_config.home.desktop_recents;
        let cache = self.cache.clone();
        let writes = self.cache_writes.clone();
        // The index is read until a listing brings it; then kept, with this session's
        // measurements beside it, and only the dataset just left read again.
        let read_facts = !self.home_app.facts_read;
        let left = self.home_app.left.take().filter(|_| !read_facts);
        let dated = self.home_app.facts_dated.clone();

        self.home.listing_in_flight = true;
        let tx = self.events.clone();
        let owed = self.owed_answer(AppEvent::HomeListingFailed);
        self.runtime.spawn_blocking(move || {
            owed.run(move || {
                // After the dataset just left is in the recents with its shape.
                writes.settle();
                // Ranked by frecency; the cursor lands on the newest, one Enter from the last file.
                let (recents, visits) = cache.load_recents_with_visits();
                let newest = recents.first().cloned();
                request.recents = crate::cache::by_frecency(recents, &visits);
                if read_facts {
                    request.known = Arc::new(cache.load_dataset_facts());
                }
                // Read, which dates them: they are shown.
                let learned: Vec<(PathBuf, crate::cache::DatasetFacts)> = (left.iter())
                    .flat_map(|path| [path.clone(), home::index_key(path)])
                    .filter_map(|key| Some((key.clone(), cache.dataset_facts(&key)?)))
                    .collect();
                if desktop {
                    request.desktop_dirs = home::desktop_recent_dirs();
                }
                let mut listing = home::build_listing(&request);
                listing.learn(&learned, request.network_check);
                let mut visits = visits;
                listing.alias_visits(&mut visits);
                // A record shown is a record used: the ones eviction keeps. Each is dated once
                // a session, not at every listing.
                let shown: Vec<PathBuf> = {
                    let mut dated = dated.lock().unwrap_or_else(|e| e.into_inner());
                    (listing.sections.iter())
                        .flat_map(|s| s.rows.iter().chain(&s.door))
                        .flat_map(|row| [row.path.clone(), home::index_key(&row.path)])
                        .filter(|key| request.known.contains_key(key) && dated.insert(key.clone()))
                        .collect()
                };
                cache.touch_dataset_facts(shown.iter().map(PathBuf::as_path));
                let known = read_facts.then(|| request.known.clone());
                // Not held past the answer: the screen adds to the index in place.
                drop(request);
                let _ = tx.send(AppEvent::HomeListingReady {
                    generation,
                    listing: Box::new(listing),
                    known,
                    learned,
                    visits,
                    newest,
                    folds: read_folds.then(|| cache.load_folds()),
                });
            })
        });
    }

    /// Ask the worker to measure on-screen rows not yet known. Opening a footer can
    /// block (a FIFO, a device, a wedged mount, a failing disk), so never on the draw
    /// thread.
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
        let known = self.home.known.clone();
        let owed = self.owed_answer(AppEvent::HomeMeasured {
            measured: Vec::new(),
            done: true,
        });
        self.runtime.spawn_blocking(move || {
            owed.run(|| {
                look_and_answer(wanted, &cache, &known, &tx, |measured, done| {
                    AppEvent::HomeMeasured { measured, done }
                })
            })
        });
    }

    /// Ask an HTTP(S) server the size of the file under the cursor, once a session,
    /// when unmeasured. The row shows the catalog's `~33 MB` until then; the answer is
    /// kept for the next listing.
    #[cfg(feature = "http")]
    pub(crate) fn size_selected_web_file(&mut self) {
        if !self.info.head_web_rows {
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
                modified: None,
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

    /// Ask a worker what the rows on screen are. Classifying reads the named directory
    /// (a round trip on a share, forever on a wedged mount), so only on-screen rows,
    /// on a detached thread, one pass at a time. It also measures remote rows, which
    /// [`crate::home::HomeState::unmeasured_visible`] leaves alone since probes return them
    /// `Unknown`.
    pub(crate) fn request_home_classifications(&mut self) {
        // One pass per filesystem: a pass stuck on a share that stopped answering holds
        // up that share's rows, which would stick too, and nothing else.
        let mounts = crate::home::locality::Mounts::cached();
        let place = |entry: &discover::Entry| mounts.mount_point_for(&entry.path);
        loop {
            let wanted = self
                .home
                .unclassified_visible_where(CLASSIFY_BATCH, |entry| {
                    !self.home.classifying.contains(&place(entry))
                });
            let Some(first) = wanted.first() else {
                return;
            };
            let pass = place(first);
            let wanted: Vec<discover::Entry> = (wanted.into_iter())
                .filter(|entry| place(entry) == pass)
                .collect();
            self.home.classifying.insert(pass.clone());
            let tx = self.events.clone();
            let cache = self.cache.clone();
            let known = self.home.known.clone();
            let owed = self.owed_answer(AppEvent::HomeClassified {
                pass: pass.clone(),
                measured: Vec::new(),
                done: true,
            });
            std::thread::spawn(move || {
                owed.run(|| {
                    look_and_answer(wanted, &cache, &known, &tx, |measured, done| {
                        AppEvent::HomeClassified {
                            pass: pass.clone(),
                            measured,
                            done,
                        }
                    })
                })
            });
        }
    }

    pub fn enter_home(&mut self) {
        self.counting.pause_indexing();
        if self.return_from_quality_evidence(false) {
            self.analysis_modal.close();
        }
        self.close_overlays();
        self.stop_find();
        // A count of the dataset being left is read for nobody.
        self.stop_value_count();
        self.export_modal.forget_counts();
        self.abandon_load();
        // Nobody is watching the file any more.
        if let Some(state) = self.data_table_state.as_mut() {
            state.stop_following();
        }
        self.home.status = None;
        // The search that found the dataset comes back selected: the next character starts
        // a new one, and `~` opens the path prompt.
        self.home.filter_selected = !self.home.filter.is_empty();
        self.home.folds_owed = true;
        self.home_app.left = self.path.clone();
        self.home_refresh();
        if let Some(open_path) = self.path.clone() {
            // Rows are compared as listed (from canonical roots and canonical recents), so
            // only the open path is resolved, once, and not on a share that may not answer.
            let resolved = (!home::is_remote_path(&open_path))
                .then(|| crate::canonical::canonicalize(&open_path).ok())
                .flatten();
            if let Some(idx) = self.home.position(|row| match row {
                home::Row::Entry { entry, .. } => {
                    entry.path == open_path || resolved.as_ref() == Some(&entry.path)
                }
                // Not the door: its path is the directory's, and would take the cursor from the
                // file to the whole-directory row.
                home::Row::Header { .. }
                | home::Row::Place { .. }
                | home::Row::More { .. }
                | home::Row::Hidden { .. }
                | home::Row::Up { .. }
                | home::Row::Door { .. } => false,
            }) {
                self.home.selected = idx;
            }
        }
        self.input_mode = InputMode::Home;
    }

    /// Esc backs out one layer: the filter, the directory descended into, then back to
    /// the open data. Nothing at the top level; Ctrl+C quits.
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
                // Not past where the browse began: back to the listing it started from.
                self.home_leave_browsing(None);
            }
            return None;
        }
        if self.data_table_state.is_some() {
            self.show_table();
            // Said on arrival: one Esc too many lands here, and the next keys act on the table.
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

    /// Drop the highlighted dataset from the recents list. Only from Recent: elsewhere
    /// a row is a file on disk, and forgetting it would imply a deletion.
    pub(crate) fn home_forget_selected(&mut self) {
        // A catalog's heading: Delete hides the bundled one until the cache is cleared; a
        // user's catalog is hidden by id in the config.
        if let Some(catalog) = self.home.selected_catalog() {
            if catalog.origin == crate::home::catalog::Origin::Bundled {
                let message = format!(
                    "Hide {}? It comes back after datui cache clear.",
                    catalog.label
                );
                self.confirmation_modal
                    .show_destructive(message, "Hide", Confirm::HideExamples);
            } else {
                self.home.status = Some(format!(
                    "[home] hide = [\"{}\"] in config.toml hides it",
                    catalog.id
                ));
            }
            return;
        }
        // A place row stands for every recent under it, so forgetting them asks first, as
        // Shift+Delete does.
        if let Some(home::Row::Place { path, held, .. }) = self.home.selected_row() {
            let message = format!(
                "Forget {held} recently opened {} under {}?",
                if held == 1 { "dataset" } else { "datasets" },
                home::display_path(&path)
            );
            self.confirmation_modal
                .show(message, Confirm::ForgetPlace(path.clone()));
            return;
        }
        // A row in catalog.toml's own section is removed from the file, as Ctrl+D does; the
        // same place under Recent is only forgotten as a recent.
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

    /// The location and shown name Ctrl+D adds for the highlighted row; a heading stands
    /// for its section's directory. Tables inside files, cloud sources and non-place
    /// rows have none.
    fn home_row_for_catalog(&self) -> Option<(PathBuf, String)> {
        match self.home.selected_row()? {
            home::Row::Place { path, .. } => {
                let name = home::display_path(&path);
                Some((path, name))
            }
            home::Row::Door { entry, .. } => {
                // A local door's trailing slash goes; a URL keeps its `//`, which `components`
                // would fold.
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
            // A table inside a file is listed with its `table`: no stat asks it again, as
            // this is asked every frame for the footer.
            home::Row::Entry { entry, .. } => (entry.table.is_none()
                && !home::is_cloud_place(&entry.path))
            .then(|| (entry.path.clone(), entry.name.clone())),
            home::Row::Header { section, .. } => {
                let root = self.home.sections.get(section)?.root.clone()?;
                let name = home::display_path(&root);
                Some((root, name))
            }
            home::Row::More { .. } | home::Row::Hidden { .. } | home::Row::Up { .. } => None,
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
        self.info
            .follow_catalogs(&self.app_config, self.path.as_deref());
        self.open_info_documentation();
        self.home_refresh();
        Ok(())
    }

    /// What Ctrl+D writes for `location` named `name`: a copy of another catalog's
    /// entry, or the place as is.
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
        // A store reached through a configured source: the source becomes the connection
        // and leaves the URL.
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
        // A source found on the machine but not configured cannot be named in a catalog:
        // the URL would be read elsewhere.
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

    /// Move pre-0.4.0 Ctrl+D directories from the cache into `catalog.toml`, once.
    fn move_remembered_places(&mut self) {
        if std::mem::replace(&mut self.home_app.remembered_moved, true) {
            return;
        }
        let places = self.cache.load_remembered_places();
        if places.is_empty() {
            return;
        }
        let Some(file) = self.mine_catalog_file() else {
            return;
        };
        // The cache's list goes only once all are in catalog.toml; a failure retries next
        // run.
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

    /// Ctrl+E: the Documentation view of the row under the cursor.
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
        self.info.documentation.open(doc, measured);
        self.info.documentation.links_open = self.home_app.local_desktop;
    }

    /// What Ctrl+E documents for the row under the cursor, with its path: the catalog
    /// dataset it is or is inside, and its format spec's docs, if any.
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

    /// Whether Delete on the selected row hides a catalog: the bundled one's heading.
    pub(crate) fn home_hides_catalog(&self) -> bool {
        self.home
            .selected_catalog()
            .is_some_and(|c| c.origin == crate::home::catalog::Origin::Bundled)
    }

    /// What Ctrl+D does on the row under the cursor, as the footer names it: add to
    /// `catalog.toml` or forget from it; `None` where it cannot add.
    pub(crate) fn home_catalog_action(&self) -> Option<&'static str> {
        let (location, _) = self.home_row_for_catalog()?;
        Some(if self.mine_entry_at(&location).is_some() {
            "Forget"
        } else {
            "Add"
        })
    }

    /// Fold or unfold the section under the cursor; folding moves the cursor to its
    /// header.
    pub(crate) fn home_toggle_fold(&mut self) {
        if let Some(section) = self.home.selected_section() {
            self.home.toggle_collapsed(section);
            self.home.clamp_selection();
            self.cache.save_folds(&self.home.folds);
        }
    }

    pub(crate) fn home_collapse(&mut self, collapse: bool) {
        // The browsed listing is the whole screen: it never folds, and no fold is
        // remembered for its path (see `set_collapsed`).
        if self.home.browsing.is_some() {
            return;
        }
        let Some(section) = self.home.selected_section() else {
            return;
        };
        // → on a section's more row shows it whole; ← on a row its cut would hide cuts it
        // back. Elsewhere they fold.
        if collapse && self.home.cut_again(section) {
            return;
        }
        if !collapse && matches!(self.home.selected_row(), Some(home::Row::More { .. })) {
            self.home.show_all(section);
            return;
        }
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
        // What the last place said of itself no longer applies.
        self.home.status = None;
        let from = std::mem::replace(&mut self.home.browsing, to);
        // Backspace can climb above the browse start; the start follows so Esc has a place
        // to stop.
        if !self.home.below_browse_start() {
            self.home.browse_start = self.home.browsing.clone();
        }
        // The filter, search and row left here come back; the cursor returns to that row
        // when the listing lands. A search of the old place no longer applies.
        self.home.come_back(from);
        self.home.sync_search_section();
        self.home.selected = 0;
        self.home_refresh();
        if !self.home.filter.is_empty() {
            self.spawn_home_search();
        }
    }

    /// Whether a peek's answer changes what a row draws: its kind, or anything in
    /// `holds` (`truncated` turns `dir` into `dir+`). Kept answers rebuild the listing
    /// on the UI thread, so the rest become "a directory, nothing to say", still sent
    /// to clear `peeking`.
    #[cfg(feature = "cloud")]
    pub(crate) fn peek_tells_a_row_something(
        answer: &(discover::EntryKind, discover::Holds),
    ) -> bool {
        answer.0 != discover::EntryKind::Directory || !answer.1.is_empty()
    }

    /// Peek inside cloud directories at or near the cursor so datasets show `hive` or
    /// `multi` and open as one: one small listing per directory, once a session, plus
    /// up to three few-KiB footer reads for a `multi` candidate. On-screen rows a batch
    /// at a time, the highlighted first.
    #[cfg(feature = "cloud")]
    pub(crate) fn peek_cloud_directories(&mut self) {
        const PEEKS_AT_ONCE: usize = 4;
        let directories = self.home.cloud_directories_to_peek(PEEKS_AT_ONCE);
        if directories.is_empty() {
            return;
        }
        // Marked out, not answered: a second pass must not ask again, and writing an
        // answer now would claim one before the request is made.
        for directory in &directories {
            self.home.peeking.insert(directory.clone());
        }
        let tx = self.events.clone();
        let cloud = self.app_config.cloud.clone();
        self.runtime.spawn(async move {
            let permits = Arc::new(tokio::sync::Semaphore::new(PEEKS_AT_ONCE));
            let mut peeks = tokio::task::JoinSet::new();
            // By task, so a panicking peek is sent back as failed rather than spinning
            // forever.
            let mut asked = std::collections::HashMap::new();
            for directory in directories {
                let (permits, cloud) = (permits.clone(), cloud.clone());
                let task_directory = directory.clone();
                let task = peeks.spawn(async move {
                    let directory = task_directory;
                    let _permit = permits.acquire_owned().await;
                    let kind =
                        crate::cloud::cloud_browse::peek_kind(&directory.to_string_lossy(), &cloud)
                            .await;
                    (directory, kind)
                });
                asked.insert(task.id(), directory);
            }
            // Sent a few at a time so labels fill in without a rebuild per directory. Every
            // directory asked is sent back, undecided and failed ones too: that clears
            // `peeking` and keeps one request per directory.
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
        // What the last walk found and the status line describe a different place; a fresh
        // walk starts on the next keystroke. A caller with news of the new place says it
        // after this returns.
        self.home.status = None;
        self.home.search.reset();
        self.home.filter.clear();
        self.home.sync_search_section();
        self.home.selected = 0;
        self.home_refresh();
    }

    /// Whether the highlighted row is the `(all files)` door, which opens the browsed
    /// directory and so is already inside it.
    fn selection_opens_the_whole_directory(&self) -> bool {
        self.home.selection_is_the_door()
    }

    /// The highlighted row when → goes inside it: any directory, local or remote,
    /// whatever its label. Files, headers and the door are left out.
    pub(crate) fn selected_directory_to_enter(&self) -> Option<PathBuf> {
        // A place under `RECENT` has no entry, so it is answered first.
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

    /// Why Enter on a bucket directory's `(all files)` row reads nothing, by Enter's
    /// own rule: a hive root or one-table directory reads through its files, and one
    /// with a reader for its contents reads with that.
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

    /// Why an object-store prefix cannot be read as one table, from its listed
    /// contents (no request). `None` when it may yet be Parquet: it holds Parquet,
    /// or no data files and perhaps data a level down.
    #[cfg(feature = "cloud")]
    pub(crate) fn why_a_cloud_prefix_cannot_be_read(holds: &discover::Holds) -> Option<String> {
        let reads_parquet =
            |name: &str| crate::FileFormat::from_name(name) == Some(crate::FileFormat::Parquet);
        if holds.formats.iter().any(|(name, _)| reads_parquet(name)) {
            return None;
        }
        match holds.formats.as_slice() {
            // Data files, none Parquet. `label()` says `mixed` for several formats, so the
            // line spells them out.
            [] => {
                // Nothing readable. A refusal only if nothing is below: sub-prefixes may hold
                // Parquet a level down.
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

    /// The reader an object-store prefix calls for, from its listing's counts: the
    /// commonest format, ranked by `rank_formats` as on disk, Parquet included. `None`
    /// when nothing there has a multi-file reader, where the refusal belongs.
    #[cfg(feature = "cloud")]
    pub(crate) fn cloud_prefix_format(
        holds: &discover::Holds,
    ) -> Option<(FileFormat, Vec<(FileFormat, usize)>)> {
        // A saved DatasetDict: its splits are Arrow, read one at a time.
        if holds.dataset_dict {
            return Some((FileFormat::Arrow, Vec::new()));
        }
        // Model weights beside config and tokenizer JSON: the prefix is the model, and the
        // JSON is not data passed over.
        if let Some((name, _)) = holds.model_weights() {
            return FileFormat::from_name(name).map(|format| (format, Vec::new()));
        }
        let (name, _) = holds.formats.first()?;
        // A GPS log is read whole from disk; a bucket's logs open one at a time, as its
        // text files do.
        let format = FileFormat::from_name(name)
            .filter(|f| f.reads_many_files() && !f.reads_into() && !f.is_lines())?;
        // What taking the commonest passes over. Polars lists the prefix and never sees
        // other formats, so the note comes from the listing on screen.
        let left_out = holds
            .formats
            .iter()
            .skip(1)
            .filter_map(|(name, n)| FileFormat::from_name(name).map(|f| (f, *n)))
            .collect();
        Some((format, left_out))
    }

    /// Open the highlighted entry: toggle a section, descend, or load a dataset.
    pub(crate) fn home_open_selected(&mut self) -> Option<AppEvent> {
        match self.home.selected_row() {
            // Into the directory or prefix the recents under it live in.
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
            // The rest of `RECENT`, or of a directory, for the session.
            Some(home::Row::More { section, .. }) => {
                self.home.show_all(section);
                return None;
            }
            // Up a level: as Backspace while browsing, and above a root at the listing.
            Some(home::Row::Up { section }) => {
                if self.home.browsing.is_some() {
                    self.home_ascend();
                } else if let Some(parent) = self
                    .home
                    .sections
                    .get(section)
                    .and_then(|s| s.root.as_deref())
                    .and_then(|root| self.home.parent_of(root))
                {
                    self.home_browse_into(parent);
                }
                return None;
            }
            // What Ctrl+A shows; the cursor goes to the first, where the hidden row stood.
            Some(home::Row::Hidden { .. }) => {
                self.home.hide_unreadable = false;
                if let Some(idx) = self.home.position(|row| {
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
        // A place a collection suggests opens as one table on Enter; → still goes in.
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
        // The `(all files)` row opens its directory whatever the label: no label can lock
        // the user out. Sent straight to the open, since `open_what_it_is` would read the
        // label and step inside again.
        if self.selection_opens_the_whole_directory() {
            // A lake table read as Parquet counts tombstoned rows and every rewritten version,
            // so the read is labeled, not refused: a note in the panel and a footer chip say
            // so, and Enter one level up explains datui does not read the table itself yet.
            let lake = entry.kind.lake_name();
            // Pick an object-store prefix's reader from its listed contents (no request), so a
            // CSV prefix is not scanned as Parquet and blamed on credentials. A prefix the
            // listing already calls a dataset is left alone: a hive root reads through its
            // partitions despite a stray `manifest.csv`.
            #[cfg(feature = "cloud")]
            let reader = if home::is_object_store_url(&entry.path)
                && !matches!(
                    entry.kind,
                    discover::EntryKind::Hive | discover::EntryKind::MultiFile
                ) {
                let reader = Self::cloud_prefix_format(&entry.holds);
                // Nothing readable: the refusal names what is there rather than blaming the
                // connection.
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
            // `hive: true` reads it as one and carries partition columns through the hive
            // route. The cloud route returns before the dispatch.
            let directory = home::directory_dataset_url(&entry.path);
            return Some(self.home_open_directory_as(directory, true, lake, reader));
        }
        // A row nothing has looked at is looked at first: `Unknown` is offered as openable,
        // and a lake root would otherwise read as one table. On a worker: the directory is
        // read, and any mount can stall.
        if entry.kind == discover::EntryKind::Unknown
            && !home::is_cloud_place(&entry.path)
            && matches!(
                source::input_source(&entry.path),
                source::InputSource::Local(_)
            )
        {
            return Some(AppEvent::ClassifyThenOpen {
                path: entry.path,
                jump: false,
            });
        }
        // A database of several tables lists them; one not yet measured opens and lands on
        // its tables the same way.
        if entry.enter_lists_tables() {
            self.home_browse_into(entry.path);
            return None;
        }
        self.open_what_it_is(entry.path, entry.kind, false)
    }

    /// Browse into `path` as a jump: the browse starts here, so Esc returns to the
    /// listing rather than up through the path's parents.
    pub(crate) fn home_jump_into(&mut self, path: PathBuf) {
        // Only the listing's mark is still a way back.
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

    /// Load a path from the home screen. The `Open` handler records the recent.
    pub(crate) fn home_open_path(&mut self, path: PathBuf, hive: bool) -> AppEvent {
        self.home_open_directory(path, hive, None)
    }

    /// As [`Self::home_open_path`], noting whether the directory is a lake table whose
    /// plain files are read, so the dataset can say so.
    fn home_open_directory(
        &mut self,
        path: PathBuf,
        hive: bool,
        lake: Option<&'static str>,
    ) -> AppEvent {
        self.home_open_directory_as(path, hive, lake, None)
    }

    /// As [`Self::home_open_directory`], naming the reader. For an object-store prefix
    /// the cloud scan runs before the format dispatch, so the listing's format travels
    /// with the open or the scan falls back to Parquet.
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
        // Told, not stat'ed: the caller knows, and a stat on a gone share would freeze the
        // key thread (also why `Open` fills in the size).
        let options = OpenOptions {
            hive,
            read_as_plain_files_of: lake,
            format,
            left_out,
            ..self.open_defaults()
        };
        self.show_table();
        // Chosen here, so a failure is reported here.
        self.announce_open(true, "Scanning input".to_string(), 10);
        // The frame drawn before `Open` runs says which file; `Open` fills in the size a
        // frame later, off this thread.
        self.name_what_is_loading(path.clone());
        AppEvent::Open(vec![path], options)
    }

    /// Before a frame: build again the sections whose listings sent rows, and say which
    /// listings out have gone silent past the wait.
    pub(crate) fn take_listing_news(&mut self) {
        for root in std::mem::take(&mut self.home_app.pages_owed) {
            self.home.relist_remote(&root);
        }
        #[cfg(test)]
        let patience = self.home_app.probe_patience.unwrap_or(PROBE_PATIENCE);
        #[cfg(not(test))]
        let patience = PROBE_PATIENCE;
        let now = std::time::Instant::now();
        let silent: Vec<PathBuf> = (self.home_app.probe_heard.iter())
            .filter(|(root, heard)| {
                now.duration_since(**heard) >= patience
                    && self.home_app.probes_inflight.contains(root)
                    && !self.home.probes.settled(root)
            })
            .map(|(root, _)| root.clone())
            .collect();
        for root in silent {
            self.home.probes.go_silent(&root);
            self.home.relist_remote(&root);
        }
    }

    /// The home screen's worker answers.
    pub(crate) fn home_event(&mut self, event: AppEvent) -> Option<AppEvent> {
        match event {
            AppEvent::HomeListingReady {
                generation,
                listing,
                known,
                learned,
                visits,
                newest,
                folds,
            } => {
                // Only the current listing's answer clears the flag; a stale one landing first
                // would say nothing is in flight.
                if generation == self.home_app.generation {
                    self.home.listing_in_flight = false;
                }
                // Read fresh from the cache, so true whichever listing carried them; only the
                // first listing after entering home carries the folds.
                if let Some(known) = known {
                    self.home.known = known;
                    self.home_app.facts_read = true;
                }
                if !learned.is_empty() {
                    std::sync::Arc::make_mut(&mut self.home.known).extend(learned);
                }
                self.home.set_visits(visits);
                self.home.newest_recent = newest;
                if let Some(folds) = folds {
                    self.home.folds = folds;
                }
                // A superseded listing describes somewhere the user has left.
                if generation != self.home_app.generation {
                    return None;
                }
                self.home.apply_listing(*listing);
                // Probes are chosen from the sections, so only once they exist.
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
                    self.home.record_measurement(path, m);
                }
                // A batch answers one file per event; they are folded in at the next frame
                // (`begin_frame`), not one list per file.
                if done {
                    self.home.measure_in_flight = false;
                    self.request_home_measurements();
                }
                None
            }
            AppEvent::HomeSized { path, measured } => {
                self.home.record_size(path, measured);
                self.home.apply_new_measurements();
                None
            }
            AppEvent::HomeWebGone { path, gone } => {
                self.home.web_gone.insert(path, gone);
                None
            }
            AppEvent::HomeClassified {
                pass,
                measured,
                done,
            } => {
                // Kept even if the listing was rebuilt since: probes and peeks rebuild it often,
                // and dropping answers would leave rows unlabeled.
                for (path, m) in measured {
                    self.home.record_measurement(path, m);
                }
                // Nothing re-sorts: kinds are written into rows in place, so the listing never
                // reshuffles under the cursor. Folded in at the next frame, as measurements.
                // The next batch comes from the viewport as it is now, not the rows scrolled past.
                if done {
                    self.home.classifying.remove(&pass);
                    self.request_home_classifications();
                }
                None
            }
            AppEvent::HomePathListed { listing } => {
                self.home_app.path_listings_out.remove(&listing.dir);
                // Kept only for the directory still being typed.
                if self.home.path_input_active
                    && home::typed_dir(&self.home.path_input) == listing.dir
                {
                    self.home.path_listing = Some(*listing);
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
                // Discard if the user typed since asking: completing would scramble their input.
                if generation != self.home_app.generation || self.home.path_input != typed {
                    return None;
                }
                if candidates == 0 {
                    self.home.status = Some("No such path".to_string());
                } else {
                    self.home.status = None;
                    if candidates > 1 {
                        self.flash_note(format!("{candidates} matches"));
                    }
                    self.home.path_input = completed;
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
                    self.home_app
                        .schema_cache
                        .insert(path.clone(), Some(schema));
                }
                let prepared = prepared.filter(|_| read_at.is_some());
                self.home_app.previews.landed(
                    path,
                    stamp,
                    read_at.unwrap_or(stamp),
                    rows,
                    prepared,
                );
                None
            }
            AppEvent::HomeSchemaReady { path, preview } => {
                if self.home_app.schema_inflight.as_ref() == Some(&path) {
                    self.home_app.schema_inflight = None;
                }
                // Kept whatever was listed since: a schema is the file's, and a finished read is
                // never thrown away. A preview's columns are not taken back by a metadata read
                // that had none.
                let known = self
                    .home_app
                    .schema_cache
                    .get(&path)
                    .is_some_and(Option::is_some);
                if preview.is_some() || !known {
                    self.home_app.schema_cache.insert(path, preview);
                }
                None
            }
            AppEvent::HomeSearchBatch {
                generation,
                root,
                found,
                scanned,
            } => {
                // Batches from a walk a later navigation superseded describe a place left behind;
                // the walk is abandoned, not cancelled, so late batches are expected. A refresh of
                // the same place supersedes nothing.
                if generation == self.home_app.search_generation {
                    self.home.search_batch(&root, found, scanned);
                }
                None
            }
            AppEvent::HomeSearchScored { epoch, matches } => {
                // A scoring that died is not retried (it would die again); listed matches stand.
                if let Some(matches) = matches {
                    self.home.search_scored(epoch, *matches);
                }
                None
            }
            AppEvent::HomeSearchDone {
                generation,
                root,
                scanned,
                limited,
            } => {
                if generation == self.home_app.search_generation {
                    self.home.search_finished(&root, scanned, limited);
                }
                self.home_app.search_inflight = false;
                // A walk abandoned by a browse held up the one the filter now asks for.
                if !self.home.filter.is_empty() && self.home.search.root.is_none() {
                    self.spawn_home_search();
                }
                None
            }
            #[cfg(feature = "cloud")]
            AppEvent::HomeCloudSources { sources } => {
                // Only the sources' rows change: nothing else is listed again.
                self.home.set_cloud(sources);
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
                if let Some(source) = self.home.cloud.iter_mut().find(|s| s.id == id) {
                    source.refreshing = false;
                    for (place, lines) in details {
                        source.place_details.insert(place, lines);
                    }
                    match failure {
                        // A failed refresh keeps the last buckets, and the row says it failed.
                        Some((short, detail)) => {
                            for bucket in buckets {
                                if !source.buckets.contains(&bucket) {
                                    source.buckets.push(bucket);
                                }
                            }
                            source.status = home::CloudStatus::Failed { short, detail };
                        }
                        None => {
                            source.buckets = buckets;
                            source.status = home::CloudStatus::Listed;
                            source.listed_at = Some(listed_at);
                        }
                    }
                }
                let cloud = std::mem::take(&mut self.home.cloud);
                self.home.set_cloud(cloud);
                None
            }
            AppEvent::HomeNarrowed {
                dir,
                prefix,
                listed,
            } => {
                // Only the current request: a replaced one may answer late with a shorter prefix.
                let asked = self
                    .home_app
                    .narrowing
                    .as_ref()
                    .is_some_and(|(d, p, _)| *d == dir && *p == prefix);
                if asked {
                    self.home_app.narrowing = None;
                }
                // Only while it is still where the user is and what the filter asks.
                let wanted = asked
                    && self.home.browsing.as_ref() == Some(&dir)
                    && !self.home.filter.is_empty();
                if let (Some((rows, truncated)), true) = (listed, wanted) {
                    self.home.narrowed = Some(home::Narrowed {
                        dir: dir.clone(),
                        prefix,
                        rows,
                        truncated,
                    });
                    self.home.relist_remote(&dir);
                }
                None
            }
            AppEvent::HomeProbeCancelled { root } => {
                self.home_app.probes_inflight.retain(|p| p != &root);
                self.home_app.probe_heard.remove(&root);
                self.home_app.listing_cancels.remove(&root);
                self.home.probes.stopped(&root);
                // Come back to after it had stopped: listed afresh.
                if self.home.browsing.as_ref() == Some(&root) {
                    self.home_refresh();
                }
                None
            }
            AppEvent::HomeProbeFailed { root, message } => {
                self.home_app.probes_inflight.retain(|p| p != &root);
                self.home_app.probe_heard.remove(&root);
                self.home_app.listing_cancels.remove(&root);
                self.home.probe_failed(root, Some(message));
                self.home_refresh();
                None
            }
            AppEvent::HomeProbeProgress { root, rows } => {
                // Only while that listing is out: a late batch must not paint over the answer.
                if self.home_app.probes_inflight.contains(&root)
                    && self.home.probes.takes_pages(&root)
                {
                    self.home.probes.read(&root, &rows);
                    (self.home_app.probe_heard).insert(root.clone(), std::time::Instant::now());
                    // Listed once a frame, however many batches came in it.
                    if !self.home_app.pages_owed.contains(&root) {
                        self.home_app.pages_owed.push(root);
                    }
                }
                None
            }
            AppEvent::HomeProbeReady {
                root,
                rows,
                cut_short,
            } => {
                // Free the slot. The cap bounds threads wedged on dead mounts, which never send
                // this; without freeing, probing stops after MAX_CONCURRENT_PROBES roots.
                self.home_app.probes_inflight.retain(|p| p != &root);
                self.home_app.probe_heard.remove(&root);
                self.home_app.listing_cancels.remove(&root);
                let landed = rows.is_some();
                match rows {
                    Some(rows) => self.home.probe_ready(root.clone(), rows, cut_short),
                    None => self.home.probe_failed(root.clone(), None),
                }
                // A filter typed while it was listing asks the server too.
                #[cfg(feature = "cloud")]
                if cut_short && !self.home.filter.is_empty() {
                    self.narrow_cloud_listing();
                }
                // An account read with its keys (the sign-in has no data role) says so.
                #[cfg(feature = "cloud")]
                if let Some((account, _, _)) = source::azure_parts(&root.to_string_lossy())
                    && crate::cloud::azure::remembered_key(&account).is_some()
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
                // Rebuild so the listing picks up the result.
                self.home_refresh();
                // Then peek at its rows: after the rebuild, which writes the `visible()` the picker
                // reads. Here, because a listing landing under a still cursor may draw no frame to
                // notice.
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
                    self.home.peeking.remove(&directory);
                    self.home.peek_failed.insert(directory);
                }
                for (directory, kind) in kinds {
                    // Answered: out of `peeking` into the set rows are labeled from; every directory
                    // comes back, so none is asked twice.
                    self.home.peeking.remove(&directory);
                    self.home.cloud_kinds.insert(directory, kind);
                }
                // Into the rows as listed: nothing is read again for a label.
                self.home.take_cloud_kinds();
                None
            }
            _ => unreachable!("not a home event"),
        }
    }
}

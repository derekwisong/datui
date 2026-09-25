//! The home screen: datui's answer to "I want to look at my data", before you have
//! had to answer "where is it, exactly".
//!
//! # Roots
//!
//! Code lives in your working directory; the interesting datasets usually do not.
//! They are on a mount, a NAS, a scratch volume. So the home screen is built around
//! *roots* — places to look — gathered from two sources, neither of which requires
//! maintaining a catalogue:
//!
//! 1. **Configured** — `[data] directories`, a `PATH`-shaped list of places.
//! 2. **The working directory** — free, and right for local exports and fixtures.
//!
//! What bridges "code here, data there" with no configuration at all is `RECENT`:
//! every dataset you have opened, grouped under the directory or prefix it lives in.
//! Opening `/mnt/data/sales/` once puts `/mnt/data/` on the screen as a place row,
//! and `Enter` on that row browses it. It is derived state, so it costs nothing to be
//! wrong and nothing to throw away.

use crate::discover::{self, Entry, EntryKind};
use std::path::{Path, PathBuf};

/// Where a root came from. Shown subtly in the UI so the list is explicable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RootOrigin {
    Cwd,
    Configured,
    /// Derived from the desktop's own recently-used list.
    Desktop,
}

impl RootOrigin {
    pub fn note(self) -> &'static str {
        match self {
            RootOrigin::Cwd => "current directory",
            RootOrigin::Configured => "configured",
            RootOrigin::Desktop => "opened elsewhere",
        }
    }
}

/// Directories holding data files that the desktop has recorded you opening.
///
/// Reads `recently-used.xbel`, the freedesktop standard that file managers and GTK
/// applications write. It is here to solve one problem: a fresh install has no
/// recents of its own, so it has nowhere to point you.
///
/// **Only the directories are used, never the files.** That distinction is the whole
/// design. The list contains whatever you last opened anywhere on the machine, which
/// is frequently something you would not want appearing on a screen you demo — a
/// bank export, a password vault dump. Surfacing `~/Downloads` as a place to look is
/// useful; listing what is in it, unbidden, is not datui's business.
pub fn desktop_recent_dirs() -> Vec<PathBuf> {
    let Some(data_dir) = dirs::data_dir() else {
        return Vec::new();
    };
    let path = data_dir.join("recently-used.xbel");
    let Ok(contents) = std::fs::read_to_string(&path) else {
        return Vec::new();
    };
    dirs_from_xbel(&contents)
}

/// Extract directories of data files from XBEL content.
///
/// Split out from the filesystem read so it can be tested directly. Scans for
/// `href="file://…"` rather than parsing XML: the attribute is all that is needed,
/// and a hand-rolled scan avoids taking an XML dependency for one file.
pub fn dirs_from_xbel(contents: &str) -> Vec<PathBuf> {
    const PREFIX: &str = "href=\"file://";
    let mut dirs: Vec<PathBuf> = Vec::new();

    for chunk in contents.split(PREFIX).skip(1) {
        let Some(end) = chunk.find('"') else { continue };
        let decoded = percent_decode(&chunk[..end]);
        let file = PathBuf::from(decoded);
        // Only files datui could actually open, and only ones still present.
        if !crate::discover::is_data_file(&file) || !file.is_file() {
            continue;
        }
        let Some(parent) = file.parent() else {
            continue;
        };
        if parent.as_os_str().is_empty() {
            continue;
        }
        let parent = parent.to_path_buf();
        if !dirs.contains(&parent) {
            dirs.push(parent);
        }
    }

    dirs
}

/// Decode `%20`-style escapes in a file URI.
fn percent_decode(raw: &str) -> String {
    let bytes = raw.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).ok();
            if let Some(byte) = hex.and_then(|h| u8::from_str_radix(h, 16).ok()) {
                out.push(byte);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// What to call a place inside an object store: a bucket, or a prefix within one.
///
/// `None` for anything that is not an object-store URL, and for objects themselves,
/// which are named by their own kind like any other file.
///
/// Worth the few lines. A bucket labelled `dir` is not wrong so much as unhelpful: the
/// word that tells you what you are looking at is the one the service uses for it, and
/// the distinction between a bucket and a prefix is exactly the one that decides whether
/// stepping out of it leaves the store.
pub fn object_place_label(path: &Path) -> Option<&'static str> {
    if cloud_source_id(path).is_some() {
        return Some("source");
    }
    if cloud_account(path).is_some() {
        return Some("account");
    }
    let text = path.to_string_lossy();
    if let Some((_, _, key)) = crate::source::azure_parts(&text) {
        return Some(if key.trim_matches('/').is_empty() {
            "container"
        } else {
            "prefix"
        });
    }
    let (scheme, rest) = text.split_once("://")?;
    if !matches!(scheme, "s3" | "s3a" | "gs" | "gcs") {
        return None;
    }
    let rest = rest.trim_end_matches('/');
    if rest.is_empty() {
        return None;
    }
    Some(if rest.contains('/') {
        "prefix"
    } else {
        "bucket"
    })
}

/// How a cloud source is addressed on the home screen: `cloud://<id>`. Not a URL any
/// library reads; it names the level above a source's buckets, which no real URL can.
pub const CLOUD_PLACE: &str = "cloud://";

/// The place for one cloud source.
pub fn cloud_place(id: &str) -> PathBuf {
    PathBuf::from(format!("{CLOUD_PLACE}{id}"))
}

/// The source ID of a `cloud://<id>` place.
pub fn cloud_source_id(path: &Path) -> Option<String> {
    let text = path.to_string_lossy();
    let id = text.strip_prefix(CLOUD_PLACE)?.trim_end_matches('/');
    (!id.is_empty() && !id.contains('/')).then(|| id.to_string())
}

/// The source ID and account of a `cloud://<id>/<account>` place: an Azure storage
/// account, which has no URL of its own.
pub fn cloud_account(path: &Path) -> Option<(String, String)> {
    let text = path.to_string_lossy();
    let rest = text.strip_prefix(CLOUD_PLACE)?.trim_end_matches('/');
    let (id, account) = rest.split_once('/')?;
    (!id.is_empty() && !account.is_empty() && !account.contains('/'))
        .then(|| (id.to_string(), account.to_string()))
}

/// Whether `url` is `root` or inside it, for URLs of any provider.
fn within(url: &str, root: &str) -> bool {
    #[cfg(feature = "cloud")]
    {
        crate::cloud_sources::is_within(url, root)
    }
    #[cfg(not(feature = "cloud"))]
    {
        let (url, root) = (url.trim_end_matches('/'), root.trim_end_matches('/'));
        url == root || url.strip_prefix(root).is_some_and(|r| r.starts_with('/'))
    }
}

/// What is left of `url` below `root`, which it is [`within`].
fn within_rest(url: &str, root: &str) -> String {
    let canonical = |u: &str| match crate::source::azure_parts(u) {
        Some((account, container, path)) => crate::source::azure_url(&account, &container, &path),
        None => u.to_string(),
    };
    let (url, root) = (canonical(url), canonical(root));
    url.strip_prefix(root.trim_end_matches('/'))
        .unwrap_or("")
        .to_string()
}

/// Whether `path` is a place in an object store: `s3://`, `gs://`, or Azure.
pub fn is_object_store_url(path: &Path) -> bool {
    let text = path.to_string_lossy();
    let scheme = text
        .split_once("://")
        .map(|(s, _)| s.to_ascii_lowercase())
        .unwrap_or_default();
    matches!(scheme.as_str(), "s3" | "s3a" | "gs" | "gcs")
        || crate::source::azure_parts(&text).is_some()
}

/// The URL that opens a cloud folder as one dataset: with its trailing slash, which is
/// what makes it a prefix to scan rather than an object to fetch.
pub fn folder_dataset_url(path: &Path) -> PathBuf {
    let text = path.to_string_lossy();
    if text.ends_with('/') {
        path.to_path_buf()
    } else {
        PathBuf::from(format!("{text}/"))
    }
}

/// A row that opens the folder being browsed as one table, whatever its label says.
///
/// The second of the two doors. A label describes what is directly inside a folder; it
/// does not decide what the folder can give you, so every folder carries this row and
/// the worst a wrong label can cost is one keystroke. It used to be offered in a bucket
/// only, and there only for the two kinds the listing had already called a dataset —
/// which is the same judgement twice, and left a local folder of separate tables with
/// no way to read them together at all.
///
/// Built from the listing already on screen, so it costs nothing to look at.
///
/// Not behind `feature = "cloud"`, though it was while the row belonged to a bucket.
/// Since it is offered in every folder, the gate left a `--no-default-features` build
/// with no second door at all, local folders included, while the help text and three
/// doc pages described it unconditionally. Only the remote classifier needs the gate.
fn whole_folder_row(dir: &Path, rows: &[Entry], remote: bool) -> Option<Entry> {
    // Not a folder: a `cloud://<id>/<account>` place stands for an Azure storage
    // account, whose children are containers and which has no URL to open.
    if cloud_account(dir).is_some() {
        return None;
    }
    let folders: Vec<String> = rows
        .iter()
        .filter(|r| r.kind != EntryKind::File)
        .map(|r| format!("{}/", r.name))
        .collect();
    let objects: Vec<(String, u64)> = rows
        .iter()
        .filter(|r| r.kind == EntryKind::File)
        .map(|r| (r.path.to_string_lossy().into_owned(), r.size.unwrap_or(1)))
        .collect();
    // Each route asked in its own vocabulary. Feeding a local listing to the cloud
    // classifier got two answers wrong in opposite directions: `scan_dir` drops dotted
    // names, so `.hoodie` never reached it and a local Hudi table came back
    // `MultiFile` — the door then read its tombstones, two keystrokes after the row
    // above said datui does not read Hudi tables. And the cloud Iceberg rule is the
    // looser of the two on purpose, name-shape only, so a plain folder holding `data/`
    // beside `metadata/` was refused as a lake table it is not.
    // Nothing remote is read here, which is the rule this whole branch is built on:
    // the call that freezes the interface is a listing of a share that has stopped
    // answering, and `look_at_directory` is a `read_dir` plus a `metadata` per entry.
    // An object store was never going to be read anyway — `read_dir` on an `s3://`
    // URL asks the working directory about a file called `s3:` — and a mount is not
    // read because the rows in hand came from the probe that already paid for it.
    let (kind, holds) = if remote || is_object_store_url(dir) {
        #[cfg(feature = "cloud")]
        {
            crate::cloud_browse::look_at_listing(&dir.to_string_lossy(), &folders, &objects)
        }
        // Without the cloud feature there is no remote classifier to ask, and reading
        // the share here is the one thing this branch exists to avoid. The door is
        // still offered — that is the whole of what it promises — and carries no kind,
        // which costs it the lake check and nothing else: its label is suppressed
        // either way, because the row is about the folder rather than in it.
        #[cfg(not(feature = "cloud"))]
        {
            let _ = (&folders, &objects);
            (EntryKind::Unknown, Default::default())
        }
    } else {
        crate::discover::look_at_directory(dir)
    };
    // Nothing in it to open. An empty folder is the one place a second door leads
    // nowhere, and a row promising to read nothing is worse than no row. A folder
    // holding only a `_SUCCESS` is that folder too.
    //
    // Asked of what the folder holds and not only of what the listing showed, because
    // the two differ on the folder that most needs the door: Spark and GBIF write part
    // files with no extension, no name in there says data, so nothing is listed — and a
    // guard on the rows alone made that folder a dead end, nothing listed and no way to
    // read it, though the open reads it by its bytes perfectly well. Files nothing
    // could name count here for that reason, and so do subfolders, whose data is a
    // level down.
    let nothing_to_open =
        rows.is_empty() && holds.formats.is_empty() && holds.folders == 0 && holds.not_read == 0;
    if nothing_to_open {
        return None;
    }
    // What the row says it opens, not whether it opens: a hive folder is read through
    // its partitions and everything else through its files.
    let what = if kind == EntryKind::Hive {
        "all partitions"
    } else {
        "all files"
    };
    // Named the way the section title above it names the same place, or the two
    // disagree about the folder you are standing in. A source id is not part of the
    // name — `s3://lab@bucket` is titled `bucket` — and an Azure container is named by
    // container, not by the long URL its last component happens to be.
    //
    // `file_name` rather than splitting on `/` for the rest: at the filesystem root
    // there is no last component and the row was named `" (all files)"`, and on Windows
    // the separator is not the one a split would look for.
    let text = dir.to_string_lossy();
    let name = if let Some((_, container, key)) = crate::source::azure_parts(&text) {
        let leaf = key.trim_matches('/').rsplit('/').next().unwrap_or("");
        if leaf.is_empty() {
            container
        } else {
            leaf.to_string()
        }
    } else {
        let (_, plain) = crate::source::split_source_id(&text);
        std::path::Path::new(plain.as_ref())
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| plain.into_owned())
    };
    let mut entry = Entry::directory(&folder_dataset_url(dir));
    entry.kind = kind;
    // What the listing you are looking at holds. Not the same tally as the folder's own
    // row upstairs: that one was counted from one page of a peek and may say `100+`,
    // and this one is counted from rows a listing has already dropped its markers from,
    // so it reports fewer skipped. Two views of one folder, each true of what it saw.
    entry.holds = holds;
    entry.name = format!("{name} ({what})");
    entry.opens_whole_folder = true;
    Some(entry)
}

/// Whether `path` is one of datui's own `cloud://` places rather than a real location.
pub fn is_cloud_place(path: &Path) -> bool {
    path.to_string_lossy().starts_with(CLOUD_PLACE)
}

/// Whether `path` is the root of a bucket: `s3://bucket`, `s3://<id>@bucket`,
/// `gs://bucket`, with no prefix.
fn is_bucket_root(path: &Path) -> bool {
    let text = path.to_string_lossy();
    if let Some((_, _, key)) = crate::source::azure_parts(&text) {
        return key.trim_matches('/').is_empty();
    }
    let Some((scheme, rest)) = text.split_once("://") else {
        return false;
    };
    matches!(scheme, "s3" | "s3a" | "gs" | "gcs") && {
        let rest = rest.trim_end_matches('/');
        !rest.is_empty() && !rest.contains('/')
    }
}

/// The location one level up from `path`, or `None` at the top.
///
/// A URL's top is its bucket or host: `Path::parent` would turn `gs://bucket` into `gs:`.
pub fn parent_location(path: &Path) -> Option<PathBuf> {
    if !matches!(
        crate::source::input_source(path),
        crate::source::InputSource::Local(_)
    ) {
        let s = path.to_string_lossy();
        let (scheme, rest) = s.split_once("://")?;
        let (up, _) = rest.trim_end_matches('/').rsplit_once('/')?;
        return Some(PathBuf::from(format!("{scheme}://{up}")));
    }
    path.parent()
        .filter(|p| !p.as_os_str().is_empty() && *p != path)
        .map(Path::to_path_buf)
}

/// Whether `path` is somewhere reading it could block: an object-store or HTTP URL,
/// or a directory on a network filesystem.
///
/// This is the predicate the home screen uses to decide what it may touch on the
/// interface thread. It answers from the string and the mount table alone, never by
/// reaching for the thing itself.
pub fn is_remote_path(path: &Path) -> bool {
    is_cloud_place(path)
        || !matches!(
            crate::source::input_source(path),
            crate::source::InputSource::Local(_)
        )
        || is_network_path(path)
}

/// Whether `path` sits on a network filesystem, according to the mount table.
///
/// Takes the longest mount point that is a prefix of the path. Returns false wherever
/// the mount table is unavailable or unparseable, so this is a hint and never a gate.
///
/// The table comes from [`crate::locality::Mounts::cached`] rather than from a fresh
/// read, because this is asked per row: once by `annotate` for every row of a
/// listing, and again by `unmeasured_visible` for every row on every frame that draws
/// one. Five thousand rows on a network share meant five thousand reads of
/// `/proc/self/mountinfo` per frame — a hundred milliseconds on the thread that
/// draws, which is the whole of why browsing a large remote directory crawled.
pub fn is_network_path(path: &Path) -> bool {
    crate::locality::Mounts::cached().is_network(path)
}

/// The mount-table logic, separated from reading `/proc` so it can be tested against
/// a fixture — the interesting cases (an NFS share shadowing an autofs entry at the
/// same path) are awkward to arrange on a real machine.
#[doc(hidden)]
pub fn network_fs_for_test(mountinfo: &str, path: &Path) -> bool {
    crate::locality::Mounts::parse(mountinfo).is_network(path)
}

/// A place datui will look, and whether it can currently be read.
#[derive(Debug, Clone)]
pub struct Root {
    pub path: PathBuf,
    pub origin: RootOrigin,
    /// True when the root is on a network filesystem.
    pub network: bool,
    /// False when the directory cannot be read — an unmounted NAS, a deleted
    /// scratch dir. Shown rather than hidden: "the mount is down" is information.
    pub available: bool,
}

/// A titled group of rows on the home screen.
#[derive(Debug, Clone)]
pub struct Section {
    pub title: String,
    /// How the section is doing, at the far end of the rule: the filesystem it is on,
    /// `first 5000` for a listing cut short, what a search covered. Never why the
    /// section exists; that is `origin`.
    pub subtitle: Option<String>,
    /// Why a path-titled section is here — `current directory`, `configured` — as a
    /// chip beside the count, where the eye is. It used to share the note slot at the
    /// far right with the state, and a directory derived from a recent was drawn
    /// exactly like a configured one with only that word to tell them apart.
    pub origin: Option<&'static str>,
    pub rows: Vec<Entry>,
    /// The row that opens the folder this section lists, as one table. Its own row,
    /// not one of `rows`.
    ///
    /// Kept apart because its path *is* the folder's — with a trailing slash, which
    /// `PathBuf` compares and hashes away — so as a row among the others it was the
    /// same key as the folder's row one level up in every path-keyed map. That cost a
    /// real bug once: measuring the door wrote a kind-less measurement into the
    /// folder's slot, the folder upstairs was then taken for already looked into, and
    /// it kept `Unknown` — no label, no `holds` line, no place in the count — for the
    /// rest of the session. A guard per walker would have to be added again by every
    /// walker written after it, so the collision is gone instead: nothing that walks
    /// `rows` or matches [`Row::Entry`] can reach the door.
    pub door: Option<Entry>,
    /// Set when a root could not be read, so the UI can say why it is empty.
    pub unavailable: bool,
    /// What to say instead of the bare word "unavailable".
    ///
    /// A share that has stopped answering has nothing to add: "unavailable" is the
    /// whole story. A bucket listing that was refused does — "403, no
    /// storage.buckets.list access" tells the user what to change, and an empty section
    /// that does not say why tells them nothing.
    pub unavailable_note: Option<String>,
    /// Starts folded unless the user has opened it. For the places that are context
    /// rather than the reason you came: directories promoted from recents, and the
    /// desktop's list of where you have been.
    pub folded_by_default: bool,
    /// The remote root whose background probe fills this section in.
    pub remote_root: Option<PathBuf>,
    /// The probe had not answered when this listing was built, so the rows are not in
    /// yet. Shown, not hidden: an empty section here means "wait", not "nothing".
    pub waiting: bool,
    /// Rows are shown under the place each lives in, with a row for the place itself.
    /// Set on `RECENT`, whose rows come from anywhere; a directory's rows all live in
    /// the directory the title names.
    pub grouped_by_place: bool,
    /// What the dataset index remembers each place to be, for the place rows of a
    /// grouped section. Filled from the cache when the listing is built, never by
    /// reading a place.
    pub place_labels: std::collections::HashMap<PathBuf, String>,
}

/// Where a cloud source's listing stands.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum CloudStatus {
    /// Asked, and no answer yet.
    #[default]
    Listing,
    /// Listed, now or on an earlier run.
    Listed,
    /// The listing was refused or never answered. `short` goes on the row; `detail`
    /// says what happened and how to fix it, in the details pane.
    Failed { short: String, detail: String },
}

/// One cloud source as the home screen shows it: a row under `CLOUD`, and the list of
/// buckets inside it.
///
/// Held apart from [`Section`] because it survives a rebuild. A listing is rebuilt
/// whenever a probe answers or a measurement lands, and re-enumerating buckets each time
/// would be a billed network round trip per keystroke.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CloudSource {
    /// The source ID, as in `[[cloud.sources]]` and `s3://<id>@bucket`.
    pub id: String,
    /// The row's name.
    pub label: String,
    /// The API spoken: `s3`, `gcs`, `azure`, or `public` for public datasets.
    pub api: String,
    /// The account, endpoint or project, and where the login came from.
    pub note: String,
    /// Bucket URLs, most useful first: `s3://bucket`, `s3://<id>@bucket`, `gs://bucket`.
    pub buckets: Vec<PathBuf>,
    pub status: CloudStatus,
    /// When the buckets were listed, when they were.
    pub listed_at: Option<std::time::SystemTime>,
    /// A listing is out for buckets already on screen from an earlier run.
    pub refreshing: bool,
    /// `key  value` lines for the details pane: endpoint, region, login.
    pub details: Vec<(String, String)>,
    /// Lines for the details pane of places inside the source: an Azure account's
    /// subscription, region and namespace.
    pub place_details: std::collections::HashMap<PathBuf, Vec<(String, String)>>,
    /// Names for places that are not named by their URL: a public dataset.
    pub names: std::collections::HashMap<PathBuf, String>,
}

impl CloudSource {
    /// Whether this source's first level is public datasets rather than buckets.
    pub fn is_public(&self) -> bool {
        self.api == "public"
    }

    /// The row for one of this source's places.
    fn entry(&self, place: &Path) -> Entry {
        let mut entry = bucket_entry(place);
        if let Some(name) = self.names.get(place) {
            entry.name = name.clone();
        }
        entry
    }

    /// The dataset `path` is in, when this is a public source: its place as listed.
    fn dataset_of(&self, path: &Path) -> Option<&PathBuf> {
        if !self.is_public() {
            return None;
        }
        let text = path.to_string_lossy();
        self.buckets
            .iter()
            .filter(|place| within(&text, &place.to_string_lossy()))
            // The innermost, when one dataset is inside another.
            .max_by_key(|place| place.as_os_str().len())
    }
    /// What the row says instead of a size: the bucket count, or why there is none.
    pub fn count_text(&self) -> String {
        match &self.status {
            CloudStatus::Failed { short, .. } if self.buckets.is_empty() => short.clone(),
            CloudStatus::Listing if self.buckets.is_empty() => String::new(),
            _ => {
                let (one, many) = if self.api == "azure" {
                    ("account", "accounts")
                } else if self.api == "gcs" {
                    ("project", "projects")
                } else if self.is_public() {
                    ("dataset", "datasets")
                } else {
                    ("bucket", "buckets")
                };
                match self.buckets.len() {
                    0 => format!("no {many}"),
                    1 => format!("1 {one}"),
                    n => format!("{n} {many}"),
                }
            }
        }
    }

    /// Whether the row should show a spinner.
    pub fn busy(&self) -> bool {
        self.refreshing || (self.status == CloudStatus::Listing && self.buckets.is_empty())
    }

    /// Whether the row reports a failure.
    pub fn failed(&self) -> bool {
        matches!(self.status, CloudStatus::Failed { .. })
    }
}

/// What measuring a dataset yielded: rows, columns, and total size, each absent when
/// it cannot be known without reading the data.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Measured {
    pub rows: Option<usize>,
    pub cols: Option<usize>,
    /// Whether `cols` is a floor rather than a total. See [`crate::discover::Entry`].
    pub cols_sampled: bool,
    pub size: Option<u64>,
    /// Column names, when the format gave them up for free.
    pub columns: Vec<String>,
    /// What opening it costs: compression, layout, partitioning.
    ///
    /// Carried here for the same reason the row count is. Without it, everything a
    /// footer said beyond `rows` and `cols` was read, recorded in the cache, and then
    /// dropped on the way to the screen -- so a hive dataset measured the ordinary
    /// way showed no partitions, and a compressed file no codec.
    pub cost: crate::discover::Cost,
    /// What the footers said it is, when that differs from what its filenames
    /// suggested: a folder whose files turn out to be separate tables is a directory,
    /// not a dataset. `None` when measuring did not change what it is, which is the
    /// ordinary case. See [`crate::discover::enrich`].
    pub kind: Option<crate::discover::EntryKind>,
    /// What one listing of it found, which is what the row's label says. Carried for
    /// the same reason `cost` is: the classify pass is the only thing that counts a
    /// local folder, and a count that stops here never reaches the screen.
    pub holds: crate::discover::Holds,
}

/// How rows are ordered within each section.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum SortMode {
    /// Recency under Recent, name under a directory — what each section is naturally
    /// ordered by.
    #[default]
    Natural,
    /// Largest first: the question is "what is big in here".
    Size,
    /// Most recently changed first: the question is "what moved".
    Modified,
    /// Most rows first.
    Rows,
}

impl SortMode {
    /// What this mode is doing *here*.
    ///
    /// The default orders each section by whatever suits it — recency for a list of
    /// things you opened, name for a directory you are reading. Labelling that
    /// "natural" names the idea rather than the behaviour, and leaves the user to
    /// guess which of the two they are looking at. So the label follows the cursor.
    pub fn label_in(self, section_is_recency_ordered: bool) -> &'static str {
        match self {
            SortMode::Natural if section_is_recency_ordered => "recent",
            SortMode::Natural => "name",
            SortMode::Size => "size",
            SortMode::Modified => "modified",
            SortMode::Rows => "rows",
        }
    }

    pub fn next(self) -> Self {
        match self {
            SortMode::Natural => SortMode::Size,
            SortMode::Size => SortMode::Modified,
            SortMode::Modified => SortMode::Rows,
            SortMode::Rows => SortMode::Natural,
        }
    }
}

/// One line of the home screen. Headers are selectable so a section can be
/// collapsed and expanded from the keyboard.
///
/// A place and the `more` row are rows of the view, not entries. An [`Entry`]'s kind
/// decides whether the probe passes look into it, whether it is measured and whether
/// what was learned is cached, and none of those may ever happen to a place: it is
/// drawn from what is already known and never causes a directory read. Being a
/// variant here rather than an [`EntryKind`] keeps it outside all four by construction.
#[derive(Debug, Clone)]
pub enum Row<'a> {
    Header {
        section: usize,
        /// Rows this section holds under the current filter.
        matches: usize,
        collapsed: bool,
    },
    Entry {
        section: usize,
        entry: &'a Entry,
        /// Drawn two cells in, under the place row above it.
        nested: bool,
    },
    /// The directory or prefix the entries below it live in, under `RECENT`.
    Place {
        section: usize,
        path: PathBuf,
        /// What the place itself was last found to be, from the dataset index: `hive`,
        /// `12 parquet`. Only when the index has a record for it; never from a read.
        label: Option<String>,
        /// The filesystem it is on, or the object store's scheme.
        source: Option<String>,
        /// How many recents live there, whether or not the filter shows them.
        held: usize,
    },
    /// The door that opens the folder being browsed as one table. See [`Section::door`].
    ///
    /// Not an `Entry` row, deliberately: it carries the folder's own path, so anything
    /// that keys a map by row path would write the door's answer into the folder's slot.
    Door { section: usize, entry: &'a Entry },
    /// What the cap on `RECENT` is hiding: `… 13 more in 5 places`.
    More {
        section: usize,
        hidden: usize,
        places: usize,
    },
}

impl Row<'_> {
    pub fn section(&self) -> usize {
        match self {
            Row::Header { section, .. }
            | Row::Entry { section, .. }
            | Row::Door { section, .. }
            | Row::Place { section, .. }
            | Row::More { section, .. } => *section,
        }
    }
}

/// The place a recent lives in: its directory, or its prefix in an object store.
///
/// A bare bucket or host, which has nothing above it, is its own place.
pub fn place_of(path: &Path) -> PathBuf {
    parent_location(path).unwrap_or_else(|| path.to_path_buf())
}

/// Whether a place can be listed: a directory, or a prefix in an object store.
///
/// An HTTP server has no listing to give — the only thing datui can do with a URL on
/// one is fetch the file it names — so the place a URL recent lives in is a heading
/// and not a door. Offering `Enter` on it led to "Listing https://…" and then
/// `unreachable`, which is the probe reporting truthfully on a `read_dir` of a URL.
pub fn place_is_browsable(path: &Path) -> bool {
    is_cloud_place(path)
        || is_object_store_url(path)
        || matches!(
            crate::source::input_source(path),
            crate::source::InputSource::Local(_)
        )
}

/// What a row is, apart from where it sits: enough to find it again after the rows
/// have been rebuilt or the cap has moved.
///
/// The cursor is an index into [`HomeState::visible`], and the rows behind that index
/// change under it whenever a listing lands or the terminal changes height. Keeping the
/// index kept the cursor on whatever row fell into its place, which for a place row —
/// the thing a user arrows onto and then presses `Enter` — meant opening the dataset
/// beneath it instead.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowKey {
    Header(String),
    Entry(PathBuf),
    /// The door, by the folder it opens. Its own variant for the same reason the row
    /// is: keyed as an `Entry` it would put the cursor on the folder's row instead.
    Door(PathBuf),
    Place(PathBuf),
    More(String),
}

/// Home screen state.
#[derive(Debug)]
pub struct HomeState {
    pub sections: Vec<Section>,
    /// Fuzzy filter over every row in every section.
    pub filter: String,
    /// Index into the flattened list of currently visible rows.
    pub selected: usize,
    /// First row of the last frame drawn, as an index into [`HomeState::visible`].
    ///
    /// Written by the renderer, which is the only place that knows how tall the list
    /// is, and read by [`HomeState::unclassified_visible`] so that what gets looked
    /// into is what is being looked at. Zero until a frame has been drawn, which is
    /// the list scrolled to the top and so a safe place to start.
    pub scroll: usize,
    /// How many rows the last frame had room for. See [`HomeState::scroll`].
    pub view_height: usize,
    /// True while the user is typing a path directly.
    pub path_input_active: bool,
    pub path_input: String,
    /// Directory the user has descended into, if any. `None` means the root listing.
    pub browsing: Option<PathBuf>,
    /// Where the current browse began: the directory entered from the root listing or
    /// jumped to by path. Esc climbs back to here and then to the listing, so it never
    /// wanders above the place the user started from. `None` treats `browsing` itself
    /// as the start.
    pub browse_start: Option<PathBuf>,
    /// Transient message (e.g. a path that does not exist).
    pub status: Option<String>,
    /// How a path is judged to be network-backed. Swappable so the "never touch a
    /// remote path on this thread" rule can be tested without a remote.
    pub network_check: fn(&Path) -> bool,
    /// Network roots whose listing has come back, keyed by path.
    pub probed: std::collections::HashMap<PathBuf, Vec<Entry>>,
    /// Network roots that did not answer.
    pub unreachable: std::collections::HashSet<PathBuf>,
    /// Why a cloud listing was refused, when the service said.
    pub probe_errors: std::collections::HashMap<PathBuf, String>,
    /// What cloud folders turned out to hold when peeked into: `hive` or `multi`.
    /// Kept for the session, so a folder is peeked at once however often it is listed.
    pub cloud_kinds: std::collections::HashMap<PathBuf, (EntryKind, crate::discover::Holds)>,
    /// How rows are ordered inside each section.
    pub sort: SortMode,
    /// True while a listing is being built on a worker. The previous listing stays on
    /// screen meanwhile, so a refresh never blanks the view.
    pub listing_in_flight: bool,
    /// True while a measurement batch is out, so only one is in flight at a time.
    pub measure_in_flight: bool,
    /// True while a classification batch is out. One at a time, and the next batch is
    /// chosen from the viewport as it is then — which is what keeps paging quickly
    /// from queueing a classification for every row it passed over.
    pub classify_in_flight: bool,
    /// Set while rows on screen are still unmeasured, so the main loop knows to draw
    /// another frame and measure the next batch.
    pub pending_enrich: bool,
    /// The same, for rows on screen nothing has looked into yet.
    pub pending_classify: bool,
    /// Row and column counts already read, keyed by path. Reading a Parquet footer
    /// is cheap; reading several hundred of them is not, so results are kept for the
    /// session and each dataset is measured once.
    pub enriched: std::collections::HashMap<PathBuf, Measured>,
    /// Sections the user has folded or opened, by title, `true` meaning folded. A
    /// section not listed here takes its own default. Keyed by title rather than
    /// index so the state survives a rebuild, which reorders and renumbers sections,
    /// and kept in the cache so it survives a restart. Use
    /// [`HomeState::toggle_collapsed`] and [`HomeState::set_collapsed`] rather than
    /// touching this directly.
    pub folds: std::collections::HashMap<String, bool>,
    /// Datasets found by walking below the working directory.
    pub search: SearchState,
    /// Cloud sources discovered on this machine or named in the config, with their
    /// buckets. Empty on a machine with no cloud credentials, which is the common case
    /// and not a failure.
    pub cloud: Vec<CloudSource>,
    /// When the current wait for a remote listing began, for the elapsed time on screen.
    pub waiting_since: Option<std::time::Instant>,
    /// `RECENT` shows every place, however many rows that takes. Set by `Enter` on the
    /// `… N more` row, for the session.
    pub recent_expanded: bool,
}

/// The result of one recursive walk below the working directory.
///
/// Held apart from `sections` because it outlives them: a listing is rebuilt whenever
/// a probe answers or a measurement lands, and re-walking the tree each time would be
/// exactly the per-keystroke cost this feature exists to avoid.
#[derive(Debug, Clone, Default)]
pub struct SearchState {
    /// Where the walk started. `None` means no search has been asked for yet.
    pub root: Option<PathBuf>,
    /// Every dataset found so far, unfiltered. The filter runs over this in memory.
    pub results: Vec<Entry>,
    /// Directory entries examined, for the progress note.
    pub scanned: usize,
    /// A walk is out. Results may still be arriving.
    pub running: bool,
    /// The walk has finished, successfully or against a limit.
    pub done: bool,
    /// Why the walk stopped short, when it did.
    pub limited: Option<String>,
}

impl SearchState {
    /// Forget everything, because the place being searched has changed.
    pub fn reset(&mut self) {
        *self = Self::default();
    }
}

impl Default for HomeState {
    fn default() -> Self {
        Self {
            sections: Vec::new(),
            cloud: Vec::new(),
            filter: String::new(),
            selected: 0,
            scroll: 0,
            view_height: 0,
            path_input_active: false,
            path_input: String::new(),
            browsing: None,
            browse_start: None,
            status: None,
            network_check: is_remote_path,
            sort: SortMode::default(),
            listing_in_flight: false,
            measure_in_flight: false,
            classify_in_flight: false,
            pending_classify: false,
            probed: std::collections::HashMap::new(),
            unreachable: std::collections::HashSet::new(),
            probe_errors: std::collections::HashMap::new(),
            cloud_kinds: std::collections::HashMap::new(),
            pending_enrich: false,
            waiting_since: None,
            enriched: std::collections::HashMap::new(),
            folds: std::collections::HashMap::new(),
            search: SearchState::default(),
            recent_expanded: false,
        }
    }
}

/// Everything [`build_listing`] needs, gathered on the interface thread from state it
/// already has, so the worker never reaches back into the app.
#[derive(Debug, Clone)]
pub struct ListingRequest {
    pub config_dirs: Vec<PathBuf>,
    pub recents: Vec<PathBuf>,
    pub desktop_dirs: Vec<PathBuf>,
    pub browsing: Option<PathBuf>,
    pub probed: std::collections::HashMap<PathBuf, Vec<Entry>>,
    pub unreachable: std::collections::HashSet<PathBuf>,
    /// Why a cloud listing was refused.
    pub probe_errors: std::collections::HashMap<PathBuf, String>,
    pub network_check: fn(&Path) -> bool,
    /// Cloud sources and the buckets already enumerated for them.
    pub cloud: Vec<CloudSource>,
    /// What datui measured on a previous run. A row whose size and modification time
    /// still match is filled in from here, so the screen has counts and column names
    /// before anything has been read this time.
    pub known: std::collections::HashMap<PathBuf, crate::cache::DatasetFacts>,
}

/// What a listing pass produced.
#[derive(Debug, Clone, Default)]
pub struct Listing {
    pub sections: Vec<Section>,
}

/// Find out what a row is, and then what is in it.
///
/// One pass, because the two questions are asked of the same filesystem and the
/// thread that asks is already there. Classifying a row nothing has looked into is
/// the [`crate::discover::classify_directory`] call the listing did not make;
/// measuring is what [`crate::discover::enrich`] has always done, and it does nothing
/// for a row that turns out to be a plain directory.
pub fn look_into(entry: &Entry) -> Entry {
    let mut probe = entry.clone();
    if probe.kind == EntryKind::Unknown && probe.path.is_dir() {
        let (kind, holds) = discover::look_at_directory(&probe.path);
        probe.kind = kind;
        probe.holds = holds;
    }
    discover::enrich(&mut probe);
    probe.size = probe.size.or(entry.size);
    probe.modified = probe.modified.or(entry.modified);
    probe
}

/// Look into a batch of rows, and remember what was learned.
///
/// What both background passes do — the one that measures rows on screen and the one
/// that classifies them — because the difference between them is which rows they pick,
/// not what is done to one. Runs on a worker; see [`look_into`] for why never here.
pub fn look_into_batch(
    rows: Vec<Entry>,
    cache: &crate::cache::CacheManager,
) -> Vec<(PathBuf, Measured)> {
    let looked_at: Vec<(Entry, Entry)> = rows
        .into_iter()
        .map(|entry| (look_into(&entry), entry))
        .collect();

    // Remember what was learned, so the next run has it before reading anything.
    // Purely a cache: every record carries the size and mtime it came from and
    // invalidates itself when those change.
    let facts: Vec<_> = looked_at
        .iter()
        .filter_map(|(probe, _)| facts_for(probe))
        .collect();
    cache.record_dataset_facts(&facts);

    looked_at
        .into_iter()
        .map(|(probe, entry)| (entry.path.clone(), measured_from(&probe, &entry)))
        .collect()
}

/// Fold a measured probe into the record kept for a row.
pub fn measured_from(probe: &Entry, original: &Entry) -> Measured {
    Measured {
        rows: probe.rows,
        cols: probe.cols,
        cols_sampled: probe.cols_sampled,
        size: probe.size.or(original.size),
        columns: probe.columns.clone(),
        kind: (probe.kind != original.kind).then_some(probe.kind),
        holds: probe.holds.clone(),
        // The source is resolved from the live mount table on every listing, so only
        // what the file said about itself is carried forward.
        cost: crate::discover::Cost {
            source: None,
            ..probe.cost.clone()
        },
    }
}

/// A row a completed probe already produced for this exact path, if any.
fn probed_entry(
    probed: &std::collections::HashMap<PathBuf, Vec<Entry>>,
    path: &Path,
) -> Option<Entry> {
    probed.values().flatten().find(|e| e.path == path).cloned()
}

/// Build the home listing.
///
/// A free function taking everything it needs, so it can run on a worker thread. It
/// is the only place the home screen touches the filesystem, and it must never be
/// called from the thread that draws — a directory on a wedged mount, a FIFO, a
/// failing disk all block here, and none of them can be enumerated in advance.
pub fn build_listing(request: &ListingRequest) -> Listing {
    let ListingRequest {
        config_dirs,
        recents,
        desktop_dirs,
        browsing,
        probed,
        unreachable,
        probe_errors,
        network_check,
        cloud,
        known,
    } = request;
    let network_check = *network_check;
    // One read of the mount table for the whole listing. It is a kernel-generated
    // file, so consulting it cannot block on the filesystem it describes -- which is
    // the entire reason it is safe to ask about a share that has stopped answering.
    let mounts = crate::locality::Mounts::current();
    let mut sections: Vec<Section> = Vec::new();

    // Inside a cloud source: its buckets, and nothing else.
    if let Some(id) = browsing.as_deref().and_then(cloud_source_id) {
        let source = cloud.iter().find(|s| s.id == id);
        let rows = source
            .map(|s| s.buckets.iter().map(|b| s.entry(b)).collect())
            .unwrap_or_default();
        let failure = source.and_then(|s| match &s.status {
            CloudStatus::Failed { short, .. } => Some(short.clone()),
            _ => None,
        });
        sections.push(Section {
            title: source.map(|s| s.label.clone()).unwrap_or(id),
            subtitle: source.map(|s| s.note.clone()).filter(|n| !n.is_empty()),
            origin: None,
            rows,
            unavailable: source.is_none() || failure.is_some(),
            unavailable_note: if source.is_none() {
                Some("source not found".to_string())
            } else {
                failure
            },
            folded_by_default: false,
            remote_root: None,
            waiting: source.is_some_and(|s| s.busy()),
            grouped_by_place: false,
            door: None,
            place_labels: Default::default(),
        });
        annotate(&mut sections, known, network_check, &mounts);
        return Listing { sections };
    }

    // Descended into a directory: show only that.
    if let Some(dir) = browsing.clone() {
        // A remote directory is never read here. Listing an object store or a share
        // that has stopped answering is the call that freezes the interface, so the
        // rows come from whatever the background probe returned and the section is
        // empty until it does. This is the same rule the root listing below follows;
        // it was missing here, which is why descending into a bucket showed nothing
        // and kept showing nothing.
        let remote = network_check(&dir);
        let rows = if remote {
            probed.get(&dir).cloned().unwrap_or_default()
        } else {
            discover::scan_dir(&dir)
        };
        let unavailable = remote && unreachable.contains(&dir);
        // The first row inside any folder opens the whole of it, since `Enter` on the
        // rows below opens one file. The other door.
        let door = whole_folder_row(&dir, &rows, remote);
        sections.push(Section {
            // The URL without a source ID: the title bar's trail already says which
            // source, and `s3://lab@data` is not a name anyone would write. An Azure
            // account or container is titled by name, not by its long URL.
            title: {
                let text = dir.to_string_lossy();
                if let Some(name) = cloud.iter().find_map(|s| s.names.get(&dir)) {
                    name.clone()
                } else if let Some((_, account)) = cloud_account(&dir) {
                    account
                } else if let Some((_, container, key)) = crate::source::azure_parts(&text) {
                    format!("{container}/{}", key.trim_matches('/'))
                        .trim_end_matches('/')
                        .to_string()
                } else {
                    match crate::source::split_source_id(&text) {
                        (Some(_), plain) => plain.into_owned(),
                        (None, _) => display_path(&dir),
                    }
                }
            },
            subtitle: None,
            origin: None,
            rows,
            unavailable,
            // A browsed remote place that did not answer has nothing to add; one whose
            // listing was refused says why.
            unavailable_note: probe_errors.get(&dir).cloned(),
            folded_by_default: false,
            // Its wait is drawn in place of the whole list; see `awaiting_listing`.
            remote_root: None,
            waiting: false,
            grouped_by_place: false,
            door,
            place_labels: Default::default(),
        });
        annotate(&mut sections, known, network_check, &mounts);
        return Listing { sections };
    }

    // Recents that still exist, most recent first.
    let recent_rows: Vec<Entry> = recents
        .iter()
        // `exists()` stats the path, so a remote entry is taken on trust and
        // dropped later only if its probe says it is gone.
        .filter(|p| network_check(p) || p.exists())
        // No display cap. The store already bounds this, the header states the
        // count, and the section folds — an invisible limit would just hide recents
        // with nothing to say it had.
        .map(|p| {
            // A probe of the containing root has already classified and measured
            // this; reuse it, so the same dataset does not read as `hive` under
            // its directory and `dir` under Recent.
            if let Some(known) = probed_entry(probed, p) {
                return known;
            }
            entry_for_path(p, network_check(p))
        })
        .collect();
    if !recent_rows.is_empty() {
        let place_labels = place_labels(&recent_rows, known, network_check);
        sections.push(Section {
            title: HomeState::RECENT_SECTION.to_string(),
            subtitle: None,
            origin: None,
            rows: recent_rows,
            unavailable: false,
            unavailable_note: None,
            folded_by_default: false,
            remote_root: None,
            waiting: false,
            // Every trace of recent use lives here. The directories recents live in
            // used to be sections of their own, titled by path and drawn exactly like
            // a configured directory, with `recent` at the far end of the rule the
            // only thing saying why they were there. Now they are rows of this one.
            grouped_by_place: true,
            door: None,
            place_labels,
        });
    }

    // Desktop-derived places are collected rather than expanded — see below.
    let mut elsewhere: Vec<Entry> = Vec::new();

    let roots = HomeState::roots_with(config_dirs, desktop_dirs, network_check);
    let mut root_sections: Vec<(RootOrigin, Section)> = Vec::new();
    for root in roots {
        // A place the desktop mentioned is listed as a directory to step into,
        // never expanded. Its contents are whatever you last opened anywhere on
        // the machine, which is regularly something you would not want appearing
        // on a screen you are sharing. Naming the place is useful; showing what
        // is in it, unasked, is not datui's business. Pressing Enter is the ask.
        if root.origin == RootOrigin::Desktop {
            if root.available {
                let mut entry = Entry::directory(&root.path);
                // Show the place, not just its leaf: "~/Downloads" says more
                // than "Downloads" when the section has no path of its own.
                entry.name = display_path(&root.path);
                elsewhere.push(entry);
            }
            continue;
        }

        // A remote root is listed from whatever its background probe returned,
        // and left empty until then. Scanning it here is the thing that freezes
        // datui on a slow or absent network.
        // A local root is listed here; a remote one only reports what its background
        // probe already returned. Truncation is knowable for the local scan, which is
        // where a directory big enough to hit the cap realistically lives.
        let mut truncated = false;
        let rows = if root.network {
            probed.get(&root.path).cloned().unwrap_or_default()
        } else if root.available {
            let scan = discover::scan_dir_bounded(&root.path);
            truncated = scan.truncated;
            scan.entries
        } else {
            Vec::new()
        };
        // A root that cannot be *read* stays: a network share that has stopped
        // answering is the case the section heading exists to report, and silently
        // dropping it is the worst answer.
        let unreachable = root.network && unreachable.contains(&root.path);
        let waiting = root.network && !unreachable && !probed.contains_key(&root.path);
        // A network root is worth flagging: it is the one that will be slow, and
        // the one that can stop answering.
        // Naming the filesystem rather than saying "network" costs one word and says
        // considerably more: nfs4, cifs and fuse.sshfs fail in different ways, and
        // none of them behaves like the tmpfs someone staged a dataset on.
        // Name the filesystem when the mount table agrees this is remote. When it
        // does not -- an unmounted automount, a path judged remote some other way --
        // fall back to the plain word, because losing the warning to gain a more
        // precise label is the wrong trade.
        let described = mounts.describe(&root.path);
        let fstype = if described.network() {
            described.fstype
        } else {
            "network".to_string()
        };
        // Say when the list is a prefix. A directory cut off at the cap otherwise
        // looks exactly like one that happens to hold that many things.
        let mut state: Vec<String> = Vec::new();
        if truncated {
            state.push(format!("first {}", discover::MAX_ENTRIES_PER_DIR));
        }
        if root.network {
            state.push(fstype);
        }
        root_sections.push((
            root.origin,
            Section {
                title: display_path(&root.path),
                subtitle: (!state.is_empty()).then(|| state.join(" · ")),
                origin: Some(root.origin.note()),
                rows,
                unavailable: !root.available || unreachable,
                unavailable_note: None,
                folded_by_default: false,
                remote_root: root.network.then(|| root.path.clone()),
                waiting,
                grouped_by_place: false,
                door: None,
                place_labels: Default::default(),
            },
        ));
    }

    // The order is by why you came, not by where the rows come from: what you
    // opened last, where you are standing, the object stores your credentials
    // reach, then the places you configured. Cloud sits high because credentials
    // on a machine are a deliberate signal, and a bucket is the one place no
    // directory listing can ever reach.
    let (cwd_sections, rest): (Vec<_>, Vec<_>) = root_sections
        .into_iter()
        .partition(|(origin, _)| *origin == RootOrigin::Cwd);
    sections.extend(cwd_sections.into_iter().map(|(_, s)| s));

    // One section for every cloud source, each a row to step into. A section per
    // source stopped scaling at a handful: ten sources were ten headings, and the
    // configured directories below them went off the screen. Buckets are one level
    // down, listed once per session, never expanded here.
    if !cloud.is_empty() {
        sections.push(Section {
            title: HomeState::CLOUD_SECTION.to_string(),
            subtitle: None,
            origin: None,
            rows: cloud.iter().map(source_entry).collect(),
            unavailable: false,
            unavailable_note: None,
            folded_by_default: false,
            remote_root: None,
            waiting: false,
            grouped_by_place: false,
            door: None,
            place_labels: Default::default(),
        });
    }

    // Configured places, in the order configured.
    sections.extend(rest.into_iter().map(|(_, s)| s));

    if !elsewhere.is_empty() {
        sections.push(Section {
            title: "Elsewhere".to_string(),
            // The title says what these are; a note repeating it said nothing.
            subtitle: None,
            origin: None,
            rows: elsewhere,
            unavailable: false,
            unavailable_note: None,
            // Places to look, not datasets: folded until asked for.
            folded_by_default: true,
            remote_root: None,
            waiting: false,
            grouped_by_place: false,
            door: None,
            place_labels: Default::default(),
        });
    }

    // Fill in whatever was measured before and still matches. `scan_dir` already
    // stat'ed every row, so verifying the fingerprint costs nothing.
    //
    // The mount table is read once for the whole listing rather than per row.
    // Resolving a path against it is string work, and it is a kernel-generated file,
    // so nothing here can block on a filesystem that has stopped answering.
    annotate(&mut sections, known, network_check, &mounts);

    Listing { sections }
}

/// The key a record about `path` is filed under in the dataset index.
///
/// An open records what it learned under the URL it resolved to: `s3://bucket/x` for
/// `s3://lab@bucket/x`, and the one `abfss://` spelling for every way an Azure path can
/// be written. A recent is stored as it was typed. The two have to meet, or a dataset
/// opened through a named source shows nothing under RECENT however often it is opened.
pub fn index_key(path: &Path) -> PathBuf {
    let text = path.to_string_lossy();
    if let Some((account, container, key)) = crate::source::azure_parts(&text) {
        return PathBuf::from(crate::source::azure_url(&account, &container, &key));
    }
    match crate::source::split_source_id(&text) {
        (Some(_), plain) => PathBuf::from(plain.into_owned()),
        (None, _) => path.to_path_buf(),
    }
}

/// A record about `path`, under the path itself or the key an open files it under.
fn known_facts<'a>(
    known: &'a std::collections::HashMap<PathBuf, crate::cache::DatasetFacts>,
    path: &Path,
) -> Option<&'a crate::cache::DatasetFacts> {
    known.get(path).or_else(|| known.get(&index_key(path)))
}

/// What the dataset index remembers each of these rows' places to be.
///
/// A place that was itself measured on some earlier listing — as a row of its own
/// parent, or as a dataset opened whole — has a label there, and the place row can
/// carry it: `bitcoin/  2 parquet`. Only from a record this build's classifier would
/// have written, and only a label that says something: `dir` is what every folder
/// with no data files in it says, and a place holds recents, so it says nothing.
///
/// A local place is held to the same fingerprint `apply_known_facts` asks of a folder:
/// its mtime, which moves when a file is added or removed. A record of `12 parquet`
/// for a folder that has since lost ten would otherwise sit two rows above the live
/// listing calling it `2 parquet`. A remote place cannot be stat'ed and is taken as
/// recorded, as its rows are.
fn place_labels(
    rows: &[Entry],
    known: &std::collections::HashMap<PathBuf, crate::cache::DatasetFacts>,
    network_check: fn(&Path) -> bool,
) -> std::collections::HashMap<PathBuf, String> {
    let mut labels = std::collections::HashMap::new();
    for row in rows {
        let place = place_of(&row.path);
        if labels.contains_key(&place) {
            continue;
        }
        let Some(facts) = known_facts(known, &place) else {
            continue;
        };
        if facts.classified_by != crate::discover::CLASSIFIER_VERSION {
            continue;
        }
        if !network_check(&place) {
            let same_mtime = std::fs::metadata(&place)
                .and_then(|m| m.modified())
                .ok()
                .and_then(|m| m.duration_since(std::time::UNIX_EPOCH).ok())
                .is_some_and(|d| d.as_secs() == facts.mtime);
            if !same_mtime {
                continue;
            }
        }
        let Some(kind) = facts.kind else {
            continue;
        };
        let mut probe = Entry::directory(&place);
        probe.kind = kind;
        probe.holds = facts.holds.clone();
        let label = probe.label();
        if !label.is_empty() && !label.starts_with("dir") {
            labels.insert(place, label.into_owned());
        }
    }
    labels
}

/// Apply a cached measurement to a row.
///
/// For a local row the fingerprint is checked: both size and modification time must
/// still agree, so a dataset that has changed invalidates itself. `scan_dir` already
/// stat'ed the row, so this costs nothing.
///
/// A remote row has no fingerprint to check, because checking it means a `stat` on a
/// path that may not answer. Its cached facts are used as-is. That is the right
/// trade: this is a cache of what a dataset looked like, the entry was written from a
/// real read, and a stale row count is a far better answer than an empty one for the
/// datasets that are hardest to reach and most worth remembering.
/// Fill every row in with what is already known about it, and with where it lives.
///
/// Called from each of `build_listing`'s exits. Having one function rather than a
/// loop at each return is the difference between adding a new exit and adding a new
/// exit whose rows silently lack half their facts.
fn annotate(
    sections: &mut [Section],
    known: &std::collections::HashMap<PathBuf, crate::cache::DatasetFacts>,
    network_check: fn(&Path) -> bool,
    mounts: &crate::locality::Mounts,
) {
    for section in sections {
        for row in &mut section.rows {
            if is_cloud_place(&row.path) {
                row.cost.source = Some("cloud".to_string());
                continue;
            }
            apply_known_facts(row, known, network_check(&row.path));
            row.cost.source = Some(mounts.describe(&row.path).fstype);
        }
    }
}

fn apply_known_facts(
    row: &mut Entry,
    known: &std::collections::HashMap<PathBuf, crate::cache::DatasetFacts>,
    remote: bool,
) {
    let Some(facts) = known_facts(known, &row.path) else {
        return;
    };

    // What a previous run found this folder to be. A listing no longer looks into a
    // folder at all — that is a `read_dir` apiece, a round trip apiece on a share —
    // so a row arrives `Unknown`, and this is the only thing that can answer for it
    // without reading the directory again.
    //
    // Only for folders a previous run *measured*, which is narrower than it sounds:
    // `facts_for` needs a size, and a folder only has one once its files were totalled
    // or sampled. A plain directory has nothing recorded and is classified again every
    // session — one `read_dir`, which is the cheap end of this. What it does cover is
    // the expensive end: a folder whose files were read and found to be separate
    // tables stays a directory, rather than being offered as one dataset again until
    // its footers have been read a second time.
    //
    // Tested before the fingerprint below rather than after, because that fingerprint
    // is a file's: a listing gives a directory no size, so `same_bytes` is never true
    // for one. A directory's own mtime is what it has, and it moves when a file is
    // added or removed, which is when this answer could change. Nothing here writes a
    // size back onto the row, and nothing should: the fingerprint below would then be
    // comparing a cached size with itself.
    //
    // Gated on the classifier, because a kind is a judgement where everything else
    // here is a measurement. `is_one_table`'s answer is the most version-sensitive
    // judgement datui makes — #234 introduced it and #243 changed what it runs over —
    // so a build that decided differently does not get to speak here.
    if !remote
        && matches!(row.kind, EntryKind::Unknown | EntryKind::MultiFile)
        && facts.classified_by == crate::discover::CLASSIFIER_VERSION
        && let Some(kind) = facts.kind
    {
        let same_mtime = row
            .modified
            .and_then(|m| m.duration_since(std::time::UNIX_EPOCH).ok())
            .is_some_and(|d| d.as_secs() == facts.mtime);
        if same_mtime {
            row.kind = kind;
            // What it holds comes back with the kind. They are one answer: a row given
            // its kind from the cache is never looked into again, so a count left
            // behind is left behind for the session — the row says `dir` about a folder
            // of fifteen Parquet files, and `enrich` goes on to describe it by whatever
            // is in its subfolders.
            if row.holds.is_empty() {
                row.holds = facts.holds.clone();
            }
            // The kind and the count, and nothing measured. Both of those come from
            // the folder's *names*, which is what a directory's mtime is a fingerprint
            // for: it moves when an entry is added, removed or renamed. What the
            // footers said — the width, the size, the column names — can change with
            // no entry added or removed at all, by one file being rewritten in place,
            // and a directory mtime cannot see that. `same_bytes` below is the
            // fingerprint for those, it is a file's, and a folder has no size to offer
            // it; so a folder of separate tables shows its width in the session that
            // measured it and not after. That is the honest end of a weak key, and
            // #275 phase 6 gives it a real one by verifying under the cursor.
        }
    }

    if !remote {
        let same_bytes = row.size.map(|s| s == facts.size).unwrap_or(false)
            && row
                .modified
                .and_then(|m| m.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_secs() == facts.mtime)
                .unwrap_or(false);
        if !same_bytes {
            return;
        }
    }

    row.rows = facts.rows;
    row.cols = facts.cols;
    row.cols_sampled = facts.cols_sampled;
    if !facts.columns.is_empty() {
        row.columns = facts.columns.clone();
    }
    // The source is filled in from the live mount table afterwards, so what is
    // restored here is only what the file itself said about itself.
    let source = row.cost.source.take();
    row.cost = facts.cost.clone();
    row.cost.source = source;
    if remote {
        // A remote row was never stat'ed, so these are all it has. A record with no
        // size to give — one object's, whose open read its footer and nothing else —
        // gives none rather than a zero.
        if facts.size > 0 {
            row.size = row.size.or(Some(facts.size));
        }
        // What it was last seen to be, rather than what its name suggests. Guessing
        // here is how the same dataset ends up reading `hive` in one section and
        // something else in another.
        // Only from a build that classified the way this one does: a Delta root
        // measured before lake tables were recognized is recorded as `multifile`, and
        // restoring that opens it as one table again.
        if row.kind == EntryKind::Unknown
            && facts.classified_by == crate::discover::CLASSIFIER_VERSION
            && let Some(kind) = facts.kind
        {
            row.kind = kind;
            // What it holds comes back with the kind. They are one answer: a row given
            // its kind from the cache is never looked into again, so without this it
            // says `dir` about a folder of fifteen Parquet files for the rest of the
            // session — and `enrich` describes it by whatever is in its subfolders.
            if row.holds.is_empty() {
                row.holds = facts.holds.clone();
            }
        }
    }
}

/// The record to keep for a row that has just been measured.
pub fn facts_for(entry: &Entry) -> Option<(PathBuf, crate::cache::DatasetFacts)> {
    let size = entry.size?;
    let mtime = entry
        .modified?
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?
        .as_secs();
    if entry.rows.is_none() && entry.columns.is_empty() && entry.cost == Default::default() {
        return None; // Nothing learned worth keeping.
    }
    Some((
        entry.path.clone(),
        crate::cache::DatasetFacts {
            mtime,
            size,
            rows: entry.rows,
            cols: entry.cols,
            cols_sampled: entry.cols_sampled,
            columns: entry.columns.clone(),
            kind: Some(entry.kind),
            holds: entry.holds.clone(),
            classified_by: crate::discover::CLASSIFIER_VERSION,
            // The source is where it is *now*, not where it was when measured: a
            // path can move between mounts, and a stale answer to "will this be
            // slow" is worse than no answer.
            cost: crate::discover::Cost {
                source: None,
                ..entry.cost.clone()
            },
        },
    ))
}

/// How well an entry answers the filter, by name or by column.
///
/// Searching column names is what turns a list of files into something you can ask a
/// question of: "which of these has a `customer_id`?" is the question a data person
/// actually has, and the answer is already in the Parquet footer datui read to get
/// the row count. A name match always outranks a column match, so typing a dataset's
/// name still finds the dataset.
///
/// Higher is better, as in fzf — see [`crate::fuzzy`] for why datui scores the way
/// that program does.
pub fn match_score(filter: &str, entry: &Entry) -> Option<i32> {
    if let Some(m) = crate::fuzzy::best_match(filter, &entry.name) {
        return Some(m.score);
    }
    if filter.is_empty() {
        return Some(0);
    }
    // Ranked below every name match, so column hits are an addition to what the
    // filter did rather than a dilution of it. Column matching is a substring test,
    // which has no score of its own worth comparing.
    matching_column(filter, entry).map(|_| -COLUMN_MATCH_PENALTY)
}

/// Distance by which a column match sits below any name match.
///
/// Larger than any score a name match can reach, so the two never interleave.
const COLUMN_MATCH_PENALTY: i32 = 1_000_000;

/// The first column of `entry` that contains `filter`, case-insensitively.
///
/// Substring rather than subsequence: a column name is short and specific, and a
/// fuzzy match over dozens of them matches nearly everything.
pub fn matching_column<'a>(filter: &str, entry: &'a Entry) -> Option<&'a str> {
    if filter.is_empty() {
        return None;
    }
    let needle = filter.to_lowercase();
    entry
        .columns
        .iter()
        .find(|c| c.to_lowercase().contains(&needle))
        .map(|c| c.as_str())
}

/// Character positions in `haystack` that `needle` matched, for highlighting.
///
/// Taken from the same alignment that produced the score, so the marks are always on
/// the characters that were actually scored.
pub fn fuzzy_positions(needle: &str, haystack: &str) -> Vec<usize> {
    crate::fuzzy::best_match(needle, haystack)
        .map(|m| m.positions)
        .unwrap_or_default()
}

/// Character positions of the first case-insensitive occurrence of `needle`.
///
/// Column matching is a substring test, not a subsequence one, so highlighting it has
/// to be too — otherwise the marks land on letters that had nothing to do with why
/// the row is on screen.
pub fn substring_positions(needle: &str, haystack: &str) -> Vec<usize> {
    if needle.is_empty() {
        return Vec::new();
    }
    let hay: Vec<char> = haystack.to_lowercase().chars().collect();
    let need: Vec<char> = needle.to_lowercase().chars().collect();
    if need.len() > hay.len() {
        return Vec::new();
    }
    for start in 0..=(hay.len() - need.len()) {
        if hay[start..start + need.len()] == need[..] {
            return (start..start + need.len()).collect();
        }
    }
    Vec::new()
}

/// Whether and how well `needle` matches `haystack`, higher being better.
///
/// A thin name over [`crate::fuzzy::best_match`]. Everything that ranks or highlights
/// goes through that one function, which is what keeps the two from drifting apart.
pub fn fuzzy_score(needle: &str, haystack: &str) -> Option<i32> {
    crate::fuzzy::best_match(needle, haystack).map(|m| m.score)
}

impl HomeState {
    /// Gather roots from the working directory, the config and the desktop.
    ///
    /// Order matters and is deliberate: where you are first, then the places you
    /// configured, then the directories the desktop says you have opened data from.
    /// Duplicates collapse to their highest-priority origin. Recents do not make
    /// roots: the place a recent lives in is a row of `RECENT`.
    pub fn roots(config_dirs: &[PathBuf], desktop_dirs: &[PathBuf]) -> Vec<Root> {
        Self::roots_with(config_dirs, desktop_dirs, is_remote_path)
    }

    /// As [`HomeState::roots`], with the network test injected.
    pub fn roots_with(
        config_dirs: &[PathBuf],
        desktop_dirs: &[PathBuf],
        is_network: fn(&Path) -> bool,
    ) -> Vec<Root> {
        let mut roots: Vec<Root> = Vec::new();
        let mut seen: Vec<PathBuf> = Vec::new();

        let push =
            |path: PathBuf, origin: RootOrigin, roots: &mut Vec<Root>, seen: &mut Vec<PathBuf>| {
                // The network test reads only the mount table, so it is safe on a
                // path that would otherwise block.
                let network = is_network(&path);

                // Canonicalising and listing both touch the filesystem. On an
                // unreachable NFS share those block — for seconds on a `soft` mount,
                // and indefinitely and uninterruptibly on a `hard` one, which is the
                // default. Nothing on the interface thread may do that, so a remote
                // root is taken at face value and probed in the background instead.
                let key = if network {
                    path.clone()
                } else {
                    path.canonicalize().unwrap_or_else(|_| path.clone())
                };
                if seen.contains(&key) {
                    return;
                }
                seen.push(key);

                let available = if network {
                    true // unknown until probed; assumed present so it is listed
                } else {
                    std::fs::read_dir(&path).is_ok()
                };
                roots.push(Root {
                    path,
                    origin,
                    available,
                    network,
                });
            };

        // Where you are comes first. Standing in a directory is the strongest
        // statement of what you are working on right now — stronger than a directory
        // configured months ago.
        if let Ok(cwd) = std::env::current_dir() {
            push(cwd, RootOrigin::Cwd, &mut roots, &mut seen);
        }

        for dir in config_dirs {
            push(dir.clone(), RootOrigin::Configured, &mut roots, &mut seen);
        }

        // Last, and weakest: places the desktop says you have opened data from. Only
        // useful before datui has recents of its own.
        for dir in desktop_dirs {
            push(dir.clone(), RootOrigin::Desktop, &mut roots, &mut seen);
        }

        roots
    }

    /// Build the home listing.
    ///
    /// Recents lead, because for data on a mount the thing you want is almost always
    /// something you have opened before. Roots follow, each scanned one level deep.
    pub fn rebuild(&mut self, config_dirs: &[PathBuf], recents: &[PathBuf]) {
        self.rebuild_with(config_dirs, recents, &[])
    }

    /// As [`HomeState::rebuild`], plus directories derived from the desktop's own
    /// recently-used list.
    pub fn rebuild_with(
        &mut self,
        config_dirs: &[PathBuf],
        recents: &[PathBuf],
        desktop_dirs: &[PathBuf],
    ) {
        let request = ListingRequest {
            config_dirs: config_dirs.to_vec(),
            recents: recents.to_vec(),
            desktop_dirs: desktop_dirs.to_vec(),
            browsing: self.browsing.clone(),
            probed: self.probed.clone(),
            unreachable: self.unreachable.clone(),
            probe_errors: self.probe_errors.clone(),
            network_check: self.network_check,
            cloud: self.cloud.clone(),
            // The synchronous path is for tests and library callers; it consults no
            // cache, so what it produces is exactly what is on disk right now.
            known: Default::default(),
        };
        let listing = build_listing(&request);
        self.apply_listing(listing);
    }

    /// Install a listing built elsewhere, keeping the cursor on whatever it was on.
    pub fn apply_listing(&mut self, listing: Listing) {
        let previous = self.selected_key();
        self.sections = listing.sections;
        // A rebuild replaces every section, and search results outlive rebuilds —
        // they came from a walk, not from this listing. Put them back.
        self.sync_search_section();
        // So does what this session has looked into. A rebuild is cheap because it
        // reads names; a kind and a row count cost round trips, and rebuilding often
        // — a probe answers, a bucket is discovered — must not throw them away and
        // ask for them again.
        self.apply_measurements();

        // Keep the cursor on the same row across a refresh; landing back at the top
        // every time a background result arrives makes the screen unusable.
        if !self.reselect(previous) {
            self.select_first_entry();
        }
        self.follow_selection();
    }

    /// What the cursor is on, as something that survives the rows changing.
    pub fn selected_key(&self) -> Option<RowKey> {
        let title = |section: usize| self.sections.get(section).map(|s| s.title.clone());
        Some(match self.selected_row()? {
            Row::Header { section, .. } => RowKey::Header(title(section)?),
            Row::More { section, .. } => RowKey::More(title(section)?),
            Row::Entry { entry, .. } => RowKey::Entry(entry.path.clone()),
            Row::Door { entry, .. } => RowKey::Door(entry.path.clone()),
            Row::Place { path, .. } => RowKey::Place(path),
        })
    }

    /// Put the cursor back on the row `key` names, if it is still on screen. Says
    /// whether the cursor was placed by the key; when it was not, the cursor is
    /// clamped, so a cursor left past the end by rows disappearing is never left there.
    ///
    /// A row the cap has just hidden is still there, behind the `more` row that now
    /// stands for it, so the cursor goes to that row rather than to whatever fell
    /// into its index in the section below — and that counts as placed.
    pub fn reselect(&mut self, key: Option<RowKey>) -> bool {
        let Some(key) = key else {
            self.clamp_selection();
            return false;
        };
        let rows = self.visible();
        let found = rows.iter().position(|row| match (row, &key) {
            (Row::Entry { entry, .. }, RowKey::Entry(path)) => entry.path == *path,
            (Row::Door { entry, .. }, RowKey::Door(path)) => entry.path == *path,
            (Row::Place { path, .. }, RowKey::Place(wanted)) => path == wanted,
            (Row::Header { section, .. }, RowKey::Header(title))
            | (Row::More { section, .. }, RowKey::More(title)) => self
                .sections
                .get(*section)
                .is_some_and(|s| s.title == *title),
            _ => false,
        });
        if let Some(idx) = found {
            self.selected = idx;
            return true;
        }
        let behind_the_cap = match &key {
            RowKey::Entry(path) | RowKey::Place(path) => rows.iter().position(|row| {
                matches!(row, Row::More { section, .. }
                if self.sections.get(*section).is_some_and(|s| {
                    s.grouped_by_place
                        && s.rows.iter().any(|r| r.path == *path || place_of(&r.path) == *path)
                }))
            }),
            _ => None,
        };
        match behind_the_cap {
            Some(idx) => {
                self.selected = idx;
                true
            }
            None => {
                self.clamp_selection();
                false
            }
        }
    }

    /// Tell the listing how tall the list is, keeping the cursor on the row it was on.
    ///
    /// The cap on `RECENT` is a share of this height, so a shorter terminal takes rows
    /// out from under the cursor and a taller one puts rows in above it. The renderer
    /// calls this every frame; only a change in height does any work.
    pub fn set_view_height(&mut self, height: usize) {
        if height == self.view_height {
            return;
        }
        let key = self.selected_key();
        self.view_height = height;
        self.reselect(key);
    }

    /// Put the viewport where the next frame will put it, without waiting for it.
    ///
    /// The renderer settles `scroll` from `selected` every frame, but the pass that
    /// looks into rows is asked for when a listing lands, which is before that frame
    /// is drawn — and a `scroll` left over from the listing just replaced points into
    /// a different set of rows entirely. Browsing into a directory selects row 0 while
    /// `scroll` still says four hundred, and the first batch is spent on rows nobody
    /// is looking at.
    fn follow_selection(&mut self) {
        self.scroll = self
            .selected
            .saturating_sub(self.view_height.saturating_sub(3).max(1));
    }

    /// Whether a section is folded: what the user last chose for it, else its default.
    ///
    /// Never while browsing. The listing of the place browsed into is the whole
    /// screen, and folding it leaves nothing. The fold memory is keyed by title, and
    /// a directory's title is its path, so a fold remembered for a section that used
    /// to be titled by that path — the directories recents were promoted to, before
    /// they became place rows — would otherwise fold the listing the place row leads
    /// to, which is the one place the user has just asked to see.
    fn section_folded(&self, section: &Section) -> bool {
        if self.browsing.is_some() {
            return false;
        }
        self.folds
            .get(&section.title)
            .copied()
            .unwrap_or(section.folded_by_default)
    }

    /// Whether a section is collapsed.
    pub fn is_collapsed(&self, section: usize) -> bool {
        self.sections
            .get(section)
            .is_some_and(|s| self.section_folded(s))
    }

    /// Collapse or expand a section.
    pub fn toggle_collapsed(&mut self, section: usize) {
        let folded = self.is_collapsed(section);
        self.set_collapsed(section, !folded);
    }

    /// Nothing is remembered while browsing: the listing browsed into is never drawn
    /// folded (see `section_folded`), and a fold written for it would be a fold for
    /// its path, which is the title the same directory has as a configured or current
    /// directory section on the root listing.
    pub fn set_collapsed(&mut self, section: usize, collapsed: bool) {
        if self.browsing.is_some() {
            return;
        }
        let Some(title) = self.sections.get(section).map(|s| s.title.clone()) else {
            return;
        };
        self.folds.insert(title, collapsed);
    }

    /// Move the cursor to the next (`delta` > 0) or previous section header,
    /// wrapping. The way past a long section to the one you came for.
    pub fn jump_section(&mut self, delta: isize) {
        let rows = self.visible();
        let headers: Vec<usize> = rows
            .iter()
            .enumerate()
            .filter(|(_, r)| matches!(r, Row::Header { .. }))
            .map(|(i, _)| i)
            .collect();
        if headers.is_empty() {
            return;
        }
        let current = self.selected;
        self.selected = if delta > 0 {
            headers
                .iter()
                .copied()
                .find(|&h| h > current)
                .unwrap_or(headers[0])
        } else {
            headers
                .iter()
                .rev()
                .copied()
                .find(|&h| h < current)
                .unwrap_or(*headers.last().unwrap())
        };
    }

    /// Whether the listing holds anything openable at all, folded or not.
    ///
    /// A lake table counts. datui cannot read one as a table yet, so it is not a dataset
    /// — but it is somewhere to go, and a warehouse directory of fifty `delta` rows with
    /// "No datasets here." printed underneath them is plainly wrong.
    ///
    /// So does a row nothing has looked into, for the same reason and more sharply: in
    /// a fresh listing that is every folder in it, and any of them may turn out to be a
    /// dataset. This is [`EntryKind::is_dataset`] rather than
    /// [`EntryKind::is_known_dataset`] on purpose — the question is whether there is
    /// anywhere to go, not how many datasets there are, which is what the control bar's
    /// count asks and answers differently.
    pub fn has_any_dataset(&self) -> bool {
        self.sections
            .iter()
            .flat_map(|s| s.rows.iter())
            .any(|e| e.kind.is_dataset() || e.kind.is_lake_table())
    }

    /// Lines currently on screen: a header per non-empty section, followed by its
    /// matching rows unless it is collapsed.
    ///
    /// Results stay grouped even while filtering. Ranking them across sections would
    /// read better as a hit list, but it costs the one thing the grouping is for —
    /// seeing *where* a dataset lives — and a name on its own rarely says that.
    /// Title of the section holding recursive search results.
    ///
    /// A constant because collapse state is keyed by title, and because the renderer
    /// and the tests both need to name it.
    pub const SEARCH_SECTION: &'static str = "Found";

    /// Title of the section listing cloud sources.
    pub const CLOUD_SECTION: &'static str = "Cloud";

    /// Title of the section listing what has been opened, grouped by place.
    pub const RECENT_SECTION: &'static str = "Recent";

    /// The source a place belongs to: `cloud://<id>` itself, a bucket named with a
    /// source (`s3://<id>@bucket`), or a bucket some source listed.
    pub fn cloud_source_of(&self, path: &Path) -> Option<&CloudSource> {
        if let Some(id) = cloud_source_id(path) {
            return self.cloud.iter().find(|s| s.id == id);
        }
        if let Some((source, _)) = self.dataset_of(path) {
            return Some(source);
        }
        if let Some((id, _)) = cloud_account(path) {
            return self.cloud.iter().find(|s| s.id == id);
        }
        let text = path.to_string_lossy();
        if let Some((account, _, _)) = crate::source::azure_parts(&text) {
            return self.azure_account_place(&account).and_then(|place| {
                cloud_account(&place).and_then(|(id, _)| self.cloud.iter().find(|s| s.id == id))
            });
        }
        if let (Some(id), _) = crate::source::split_source_id(&text) {
            return self.cloud.iter().find(|s| s.id == id);
        }
        if let Some(project) =
            Self::google_bucket_root(path).and_then(|b| self.project_of_bucket(&b))
        {
            return cloud_account(&project)
                .and_then(|(id, _)| self.cloud.iter().find(|s| s.id == id));
        }
        let (_, plain) = crate::source::split_source_id(&text);
        let (scheme, rest) = plain.split_once("://")?;
        let bucket = rest.split('/').next()?;
        let root = PathBuf::from(format!("{scheme}://{bucket}"));
        self.cloud.iter().find(|s| s.buckets.contains(&root))
    }

    /// One level up from `path`. A bucket's parent is the source that lists it, so
    /// Backspace from a bucket returns to its source rather than to the home listing.
    pub fn parent_of(&self, path: &Path) -> Option<PathBuf> {
        if cloud_source_id(path).is_some() {
            return None;
        }
        // Out of a dataset's root is back to the datasets, not up into a bucket that
        // may not be listable at all.
        if let Some((source, place)) = self.dataset_of(path) {
            let text = path.to_string_lossy();
            if text.trim_end_matches('/') == place.to_string_lossy().trim_end_matches('/') {
                return Some(cloud_place(&source.id));
            }
            let up = self.parent_within(path)?;
            // The dataset's own place, as listed, so its listing is found again.
            return Some(
                if up.to_string_lossy().trim_end_matches('/')
                    == place.to_string_lossy().trim_end_matches('/')
                {
                    place.clone()
                } else {
                    up
                },
            );
        }
        if let Some((id, _)) = cloud_account(path) {
            return Some(cloud_place(&id));
        }
        let text = path.to_string_lossy();
        if let Some((account, container, key)) = crate::source::azure_parts(&text) {
            let key = key.trim_matches('/');
            if key.is_empty() {
                return self.azure_account_place(&account);
            }
            let up = key.rsplit_once('/').map(|(up, _)| up).unwrap_or("");
            let up = if up.is_empty() {
                String::new()
            } else {
                format!("{up}/")
            };
            return Some(PathBuf::from(crate::source::azure_url(
                &account, &container, &up,
            )));
        }
        if is_bucket_root(path) {
            // A Google bucket's parent is the project it was listed under.
            if let Some(project) = self.project_of_bucket(path) {
                return Some(project);
            }
            return self.cloud_source_of(path).map(|s| cloud_place(&s.id));
        }
        parent_location(path)
    }

    /// One level up inside a bucket or container, whatever the provider.
    fn parent_within(&self, path: &Path) -> Option<PathBuf> {
        let text = path.to_string_lossy();
        if let Some((account, container, key)) = crate::source::azure_parts(&text) {
            let key = key.trim_matches('/');
            let up = key.rsplit_once('/').map(|(up, _)| up).unwrap_or("");
            let up = if up.is_empty() {
                String::new()
            } else {
                format!("{up}/")
            };
            return Some(PathBuf::from(crate::source::azure_url(
                &account, &container, &up,
            )));
        }
        parent_location(path)
    }

    /// The public source and dataset `path` is in.
    fn dataset_of(&self, path: &Path) -> Option<(&CloudSource, &PathBuf)> {
        self.cloud
            .iter()
            .find_map(|source| source.dataset_of(path).map(|place| (source, place)))
    }

    /// The place of an Azure account, from whichever source lists it.
    fn azure_account_place(&self, account: &str) -> Option<PathBuf> {
        self.cloud
            .iter()
            .flat_map(|s| s.buckets.iter())
            .find(|place| cloud_account(place).is_some_and(|(_, a)| a == account))
            .cloned()
    }

    /// What to call a place a source names itself: a public dataset.
    pub fn place_kind(&self, path: &Path) -> Option<&'static str> {
        if let Some((id, _)) = cloud_account(path) {
            return self
                .cloud
                .iter()
                .any(|s| s.id == id && s.api == "gcs")
                .then_some("project");
        }
        self.cloud
            .iter()
            .any(|s| s.is_public() && s.names.contains_key(path))
            .then_some("dataset")
    }

    /// The project place a Google bucket was listed under, when it was.
    fn project_of_bucket(&self, bucket_root: &Path) -> Option<PathBuf> {
        let root = bucket_root.to_string_lossy();
        let root = root.trim_end_matches('/');
        self.probed
            .iter()
            .filter(|(place, _)| cloud_account(place).is_some())
            .find(|(_, rows)| {
                rows.iter()
                    .any(|row| row.path.to_string_lossy().trim_end_matches('/') == root)
            })
            .map(|(place, _)| place.clone())
    }

    /// The bucket root of a Google URL: `gs://bucket`.
    fn google_bucket_root(path: &Path) -> Option<PathBuf> {
        let text = path.to_string_lossy();
        let rest = text
            .strip_prefix("gs://")
            .or_else(|| text.strip_prefix("gcs://"))?;
        let bucket = rest.split('/').next().filter(|b| !b.is_empty())?;
        Some(PathBuf::from(format!("gs://{bucket}")))
    }

    /// Details-pane lines for a place a cloud source listed, when it has any.
    pub fn place_details(&self, path: &Path) -> Option<&[(String, String)]> {
        self.cloud
            .iter()
            .find_map(|s| s.place_details.get(path))
            .map(Vec::as_slice)
    }

    /// The location as the title bar names it. Cloud places read as a trail through
    /// the source's label, since neither `cloud://<id>` nor `s3://<id>@bucket` is
    /// something to show a person.
    pub fn location_label(&self, path: &Path) -> String {
        let sep = crate::glyphs::get().trail;
        if let Some(source) = self.cloud_source_of(path) {
            let mut parts = vec!["cloud".to_string(), source.label.clone()];
            let text = path.to_string_lossy();
            if let Some(place) = source.dataset_of(path) {
                parts.push(source.entry(place).name);
                let rest = within_rest(&text, &place.to_string_lossy());
                parts.extend(
                    rest.split('/')
                        .filter(|p| !p.is_empty())
                        .map(str::to_string),
                );
            } else if let Some((_, account)) = cloud_account(path) {
                parts.push(account);
            } else if let Some((account, container, key)) = crate::source::azure_parts(&text) {
                parts.push(account);
                parts.push(container);
                parts.extend(key.split('/').filter(|p| !p.is_empty()).map(str::to_string));
            } else if cloud_source_id(path).is_none() {
                if let Some((_, project)) = Self::google_bucket_root(path)
                    .and_then(|b| self.project_of_bucket(&b))
                    .as_deref()
                    .and_then(cloud_account)
                {
                    parts.push(project);
                }
                let (_, plain) = crate::source::split_source_id(&text);
                if let Some((_, rest)) = plain.split_once("://") {
                    parts.extend(
                        rest.split('/')
                            .filter(|p| !p.is_empty())
                            .map(str::to_string),
                    );
                }
            }
            return parts.join(&format!(" {sep} "));
        }
        display_path(path)
    }

    /// Put the current search results into `sections`, or take them out.
    ///
    /// Called after every rebuild and every batch of results. The section only exists
    /// while there is a filter: with none, every row matches, and twenty thousand
    /// matches is not a home screen.
    pub fn sync_search_section(&mut self) {
        self.sections.retain(|s| s.title != Self::SEARCH_SECTION);

        if self.filter.is_empty() {
            return;
        }
        // Bucket names already listed, from every source. Nothing is fetched for this:
        // a search that went to the network per keystroke would be a bill per keystroke.
        let cloud_rows: Vec<Entry> = if self.browsing.is_none() {
            self.cloud
                .iter()
                .flat_map(|source| {
                    source.buckets.iter().map(move |bucket| {
                        let mut entry = source.entry(bucket);
                        entry.name = format!(
                            "{} {} {}",
                            source.label,
                            crate::glyphs::get().trail,
                            entry.name
                        );
                        entry.cost.source = Some(source.api.clone());
                        entry
                    })
                })
                .filter(|e| match_score(&self.filter, e).is_some())
                .collect()
        } else {
            Vec::new()
        };
        let local =
            self.search.root.is_some() && (!self.search.results.is_empty() || self.search.running);
        if !local {
            if !cloud_rows.is_empty() {
                let subtitle = format!("cloud · {} names", cloud_rows.len());
                self.sections.push(Section {
                    title: Self::SEARCH_SECTION.to_string(),
                    subtitle: Some(subtitle),
                    origin: None,
                    rows: cloud_rows,
                    unavailable: false,
                    unavailable_note: None,
                    folded_by_default: false,
                    remote_root: None,
                    waiting: false,
                    grouped_by_place: false,
                    door: None,
                    place_labels: Default::default(),
                });
            }
            return;
        }

        // A dataset already on screen under the directory it lives in should not
        // appear a second time under the search. The search is for what you could
        // not otherwise see.
        let listed: std::collections::HashSet<&PathBuf> = self
            .sections
            .iter()
            .flat_map(|s| s.rows.iter().map(|r| &r.path))
            .collect();

        let mut rows: Vec<Entry> = self
            .search
            .results
            .iter()
            .filter(|e| !listed.contains(&e.path))
            .cloned()
            .collect();
        rows.extend(cloud_rows);

        if rows.is_empty() && !self.search.running {
            return;
        }

        let root = self.search.root.clone().unwrap_or_default();
        let mut subtitle = display_path(&root);
        if self.search.running {
            subtitle = format!("{subtitle} · searching {}", self.search.scanned);
        } else if let Some(limit) = &self.search.limited {
            subtitle = format!("{subtitle} · {limit} · {} searched", self.search.scanned);
        } else {
            subtitle = format!("{subtitle} · {} searched", self.search.scanned);
        }

        self.sections.push(Section {
            title: Self::SEARCH_SECTION.to_string(),
            subtitle: Some(subtitle),
            origin: None,
            rows,
            unavailable: false,
            unavailable_note: None,
            folded_by_default: false,
            remote_root: None,
            waiting: false,
            grouped_by_place: false,
            door: None,
            place_labels: Default::default(),
        });
    }

    /// Fold a batch of search results in, keeping the list free of duplicates.
    pub fn search_batch(&mut self, root: &Path, mut found: Vec<Entry>, scanned: usize) {
        // A batch from a walk the user has already moved on from is dropped: the
        // walk is abandoned rather than cancelled, so late results are normal.
        if self.search.root.as_deref() != Some(root) {
            return;
        }
        self.search.scanned = scanned;
        self.search.results.append(&mut found);
        self.sync_search_section();
    }

    /// Record that the walk under `root` has finished.
    pub fn search_finished(&mut self, root: &Path, scanned: usize, limited: Option<String>) {
        if self.search.root.as_deref() != Some(root) {
            return;
        }
        self.search.running = false;
        self.search.done = true;
        self.search.scanned = scanned;
        self.search.limited = limited;
        self.sync_search_section();
    }

    pub fn visible(&self) -> Vec<Row<'_>> {
        self.rows(true)
    }

    /// Every row a section would show, with the cap on `RECENT` lifted.
    ///
    /// For counting what is listed. The header says thirty and the `more` row says
    /// twenty-seven more, so the control bar must not say three.
    pub fn listed(&self) -> Vec<Row<'_>> {
        self.rows(false)
    }

    fn rows(&self, capped: bool) -> Vec<Row<'_>> {
        let mut out: Vec<Row<'_>> = Vec::new();
        for (si, section) in self.sections.iter().enumerate() {
            let mut matched: Vec<(&Entry, i32)> = section
                .rows
                .iter()
                .filter_map(|row| match_score(&self.filter, row).map(|s| (row, s)))
                .collect();

            // A section with nothing to show is dropped, unless it is standing in for
            // a root the user named or is currently in, where its absence would be
            // more confusing than an empty heading, or its rows are still on the way.
            //
            // A door is something to show. A folder of part files written with no
            // extension lists nothing — no name in it says data — and the door is the
            // only way to read it; dropped here, the whole section went with it and the
            // folder was a dead end that the open could have read.
            let keep_empty = section.unavailable || section.waiting || section.origin.is_some();
            let has_door = section.door.is_some() && self.filter.is_empty();
            if matched.is_empty() && !has_door && !(keep_empty && self.filter.is_empty()) {
                continue;
            }

            // Within a section, rank by match quality; without a filter every score is
            // equal and the curated order is preserved. Ties go to the shorter name,
            // which is fzf's default tiebreak and the reason `sales` prefers
            // `sales.csv` over `sales_by_region_and_quarter.csv`.
            if !self.filter.is_empty() {
                matched.sort_by(|(a, sa), (b, sb)| {
                    sb.cmp(sa).then_with(|| a.name.len().cmp(&b.name.len()))
                });
            }

            // An explicit sort overrides both. Rows with nothing to sort by go last
            // rather than sorting as zero, so "biggest first" does not begin with a
            // page of datasets whose size is simply unknown.
            match self.sort {
                SortMode::Natural => {}
                SortMode::Size => {
                    matched.sort_by_key(|(e, _)| std::cmp::Reverse(e.size.unwrap_or(0)));
                }
                SortMode::Rows => {
                    matched.sort_by_key(|(e, _)| std::cmp::Reverse(e.rows.unwrap_or(0)));
                }
                SortMode::Modified => {
                    matched.sort_by_key(|(e, _)| {
                        std::cmp::Reverse(
                            e.modified
                                .and_then(|m| m.duration_since(std::time::UNIX_EPOCH).ok())
                                .map(|d| d.as_secs())
                                .unwrap_or(0),
                        )
                    });
                }
            }

            let collapsed = self.section_folded(section);
            out.push(Row::Header {
                section: si,
                // What the section holds, which the door is not: it is a way to open
                // the folder those rows are in, so counting it would make a folder of
                // three files say four. It is not among `rows`, so nothing here has to
                // take it back out.
                matches: matched.len(),
                collapsed,
            });
            if collapsed {
                continue;
            }
            // First inside the folder, before the rows and whatever the sort, because
            // being the first row inside a folder is the whole of what it is.
            //
            // Not while a filter is on. Its name carries the words `all files`, which a
            // fuzzy filter matches for most of the alphabet — `sal` found it beside
            // `sales.parquet` — so it steps out of the way and comes back when the
            // filter is cleared.
            if let Some(door) = section.door.as_ref()
                && self.filter.is_empty()
            {
                out.push(Row::Door {
                    section: si,
                    entry: door,
                });
            }
            if section.grouped_by_place {
                out.extend(self.rows_by_place(si, section, &matched, capped));
            } else {
                out.extend(matched.into_iter().map(|(entry, _)| Row::Entry {
                    section: si,
                    entry,
                    nested: false,
                }));
            }
        }
        out
    }

    /// A grouped section's rows under the place each lives in, and what the cap hides.
    ///
    /// Places come in the order of their newest row in the section, which for `RECENT`
    /// is the order the rows already have. Within a place the rows keep the order
    /// `matched` gave them — recency, or the sort or match rank in effect — so a sort
    /// orders each place and never flattens the section.
    ///
    /// Whole places are shown, newest first, until they have used a third of the
    /// list's height, and always at least one; what is left is one `… N more in M
    /// places` row. The fraction is a judgment. If it proves wrong in use the answer
    /// is a `[data] recent_rows` setting, not a different fraction. A filter shows
    /// every match, and the `more` row goes with the cap: a match that is hidden is
    /// not a match.
    fn rows_by_place<'a>(
        &self,
        si: usize,
        section: &'a Section,
        matched: &[(&'a Entry, i32)],
        capped: bool,
    ) -> Vec<Row<'a>> {
        let mut order: Vec<PathBuf> = Vec::new();
        for row in &section.rows {
            let place = place_of(&row.path);
            if !order.contains(&place) {
                order.push(place);
            }
        }
        let groups: Vec<(PathBuf, Vec<&'a Entry>)> = order
            .into_iter()
            .filter_map(|place| {
                let rows: Vec<&'a Entry> = matched
                    .iter()
                    .filter(|(entry, _)| place_of(&entry.path) == place)
                    .map(|(entry, _)| *entry)
                    .collect();
                (!rows.is_empty()).then_some((place, rows))
            })
            .collect();

        // Before the first frame there is no height to budget against, and a listing
        // built for a caller with no screen is asked for whole.
        let capped =
            capped && !self.recent_expanded && self.filter.is_empty() && self.view_height > 0;
        let budget = self.view_height / 3;
        let mut out: Vec<Row<'a>> = Vec::new();
        let mut used = 0usize;
        let mut shown = 0usize;
        for (place, rows) in &groups {
            let cost = 1 + rows.len();
            if capped && shown > 0 && used + cost > budget {
                break;
            }
            out.push(Row::Place {
                section: si,
                path: place.clone(),
                label: section.place_labels.get(place).cloned(),
                // Every row in a place is on the filesystem the place is, so the first
                // speaks for it. Filled in by `annotate` from the mount table.
                source: rows[0].cost.source.clone(),
                held: section
                    .rows
                    .iter()
                    .filter(|row| place_of(&row.path) == *place)
                    .count(),
            });
            out.extend(rows.iter().map(|entry| Row::Entry {
                section: si,
                entry,
                nested: true,
            }));
            used += cost;
            shown += 1;
        }
        if shown < groups.len() {
            out.push(Row::More {
                section: si,
                hidden: groups[shown..].iter().map(|(_, rows)| rows.len()).sum(),
                places: groups.len() - shown,
            });
        }
        out
    }

    /// The highlighted row, whatever it is.
    pub fn selected_row(&self) -> Option<Row<'_>> {
        self.visible().into_iter().nth(self.selected)
    }

    /// The highlighted row, when it is a dataset rather than a section header.
    ///
    /// The door counts: it is something to open, and every caller here wants what the
    /// cursor is on. What it must not be is a row in a path-keyed map, which is why it
    /// is [`Row::Door`] and not an entry among the section's rows.
    pub fn selected_entry(&self) -> Option<Entry> {
        match self.visible().get(self.selected) {
            Some(Row::Entry { entry, .. }) | Some(Row::Door { entry, .. }) => {
                Some((*entry).clone())
            }
            _ => None,
        }
    }

    /// Whether the cursor is on the door rather than on something in the folder.
    pub fn selection_is_the_door(&self) -> bool {
        matches!(self.visible().get(self.selected), Some(Row::Door { .. }))
    }

    /// The recents that live in `place`: what `Delete` on its row forgets.
    pub fn recents_in(&self, place: &Path) -> Vec<PathBuf> {
        self.sections
            .iter()
            .filter(|s| s.grouped_by_place)
            .flat_map(|s| s.rows.iter())
            .filter(|row| place_of(&row.path) == place)
            .map(|row| row.path.clone())
            .collect()
    }

    /// The section the highlighted row belongs to.
    pub fn selected_section(&self) -> Option<usize> {
        self.visible().get(self.selected).map(|r| r.section())
    }

    /// Whether the highlighted row is a section header.
    pub fn selection_is_header(&self) -> bool {
        matches!(self.visible().get(self.selected), Some(Row::Header { .. }))
    }

    /// Remote roots that have neither answered nor been written off.
    ///
    /// The caller probes these off the interface thread; nothing here may touch them.
    pub fn pending_probes(&self) -> Vec<PathBuf> {
        let check = self.network_check;
        let mut out = Vec::new();
        // Read from the section, not its subtitle: a share's subtitle names its
        // filesystem, and matching on the word "network" meant NFS roots were never
        // listed at all.
        for root in self.sections.iter().filter_map(|s| s.remote_root.as_ref()) {
            if !self.probed.contains_key(root)
                && !self.unreachable.contains(root)
                && !out.contains(root)
            {
                out.push(root.clone());
            }
        }
        // Descended into a remote directory — a bucket, a prefix, a share. It is the
        // only thing on screen and its rows can come from nowhere but a probe, so it
        // is not covered by the section scan above, which only looks at roots.
        if let Some(dir) = &self.browsing
            && check(dir)
            && cloud_source_id(dir).is_none()
            && !self.probed.contains_key(dir)
            && !self.unreachable.contains(dir)
            && !out.contains(dir)
        {
            out.push(dir.clone());
        }
        out
    }

    /// Whether the browsed directory is strictly below where the browse began, so Esc
    /// still has a level to climb before it returns to the listing.
    pub fn below_browse_start(&self) -> bool {
        let (Some(dir), Some(start)) = (&self.browsing, &self.browse_start) else {
            return false;
        };
        if dir == start {
            return false;
        }
        // Up through parents rather than a path prefix: a bucket sits below its cloud
        // source, and `s3://bucket` does not start with `cloud://<id>`.
        let mut current = self.parent_of(dir);
        let mut steps = 0;
        while let Some(place) = current {
            if &place == start {
                return true;
            }
            steps += 1;
            if steps > 64 {
                break;
            }
            current = self.parent_of(&place);
        }
        false
    }

    /// Whether any section on screen is still waiting for its rows.
    pub fn sections_waiting(&self) -> bool {
        self.sections.iter().any(|s| s.waiting)
    }

    /// The remote location being browsed, while its listing has not come back.
    pub fn awaiting_listing(&self) -> Option<&Path> {
        let dir = self.browsing.as_deref()?;
        if cloud_source_id(dir).is_some() {
            return self
                .cloud_source_of(dir)
                .is_some_and(|s| s.status == CloudStatus::Listing && s.buckets.is_empty())
                .then_some(dir);
        }
        ((self.network_check)(dir)
            && !self.probed.contains_key(dir)
            && !self.unreachable.contains(dir))
        .then_some(dir)
    }

    /// Record what a probe found. An empty listing is still an answer.
    pub fn probe_ready(&mut self, root: PathBuf, rows: Vec<Entry>) {
        self.unreachable.remove(&root);
        self.probe_errors.remove(&root);
        self.probed.insert(root.clone(), rows);
        self.apply_cloud_kinds(&root);
    }

    /// Label the rows of a cloud listing with what peeking inside them found.
    pub fn apply_cloud_kinds(&mut self, root: &Path) {
        let Some(rows) = self.probed.get_mut(root) else {
            return;
        };
        for row in rows.iter_mut() {
            if row.kind == EntryKind::Directory
                && let Some((kind, holds)) = self.cloud_kinds.get(&row.path)
            {
                row.kind = *kind;
                // The label is what the peek counted, not the kind it decided: a prefix
                // of twelve Parquet objects reads `12 parquet` in a bucket for the same
                // reason it does on disk. Only when there is something to say — a claim
                // staked before the answer arrives carries no count, and must not erase
                // one the row already has.
                if !holds.is_empty() {
                    row.holds = holds.clone();
                }
            }
        }
    }

    /// The folders of a cloud listing not yet peeked into, at most `limit`, in the order
    /// they are listed.
    pub fn cloud_folders_to_peek(&self, root: &Path, limit: usize) -> Vec<PathBuf> {
        if !is_object_store_url(root) {
            return Vec::new();
        }
        self.probed
            .get(root)
            .into_iter()
            .flatten()
            .filter(|row| {
                row.kind == EntryKind::Directory && !self.cloud_kinds.contains_key(&row.path)
            })
            .take(limit)
            .map(|row| row.path.clone())
            .collect()
    }

    /// Record that a probe could not read the root.
    pub fn probe_failed(&mut self, root: PathBuf) {
        self.probed.remove(&root);
        self.unreachable.insert(root);
    }

    /// Measure a batch of rows on the calling thread.
    ///
    /// For tests and library callers that know their paths are safe. **The application
    /// never calls this**: reading a footer opens a file, which blocks on a FIFO, a
    /// device node, a wedged mount or a failing disk. In the app the interface thread
    /// only ever decides *what* to measure, via [`HomeState::unmeasured_visible`], and
    /// a worker does the reading.
    pub fn measure_now(&mut self, limit: usize) -> bool {
        let wanted = self.unmeasured_visible(limit);
        let more = self.unmeasured_visible(limit + 1).len() > wanted.len();
        for entry in wanted {
            let mut probe = entry.clone();
            discover::enrich(&mut probe);
            self.enriched
                .insert(entry.path.clone(), measured_from(&probe, &entry));
        }
        self.apply_measurements();
        more
    }

    /// Rows on screen that have not been measured yet, up to `limit`.
    ///
    /// The interface thread decides *what* is worth measuring — it knows what is
    /// visible — and a worker does the reading.
    pub fn unmeasured_visible(&self, limit: usize) -> Vec<Entry> {
        let mut out = Vec::new();
        for row in self.visible() {
            let Row::Entry { entry, .. } = row else {
                continue;
            };
            if entry.rows.is_some() || self.enriched.contains_key(&entry.path) {
                continue;
            }
            // What a row *is* settles it before where it lives does, because the kind
            // is already in hand and the mount table is a lookup. This runs once per
            // row on every frame that draws the home screen, and a directory of six
            // thousand partitions is every one of those rows: asking the cheap
            // question first is the difference between a free frame and a scan.
            if matches!(entry.kind, EntryKind::Directory | EntryKind::Unknown)
                || entry.kind.is_lake_table()
            {
                continue;
            }
            // Remote rows are measured by their root's probe, which already reads that
            // filesystem. Measuring them here too would put a second thread on a share
            // that may never answer, and a wedged thread is never reclaimed.
            if (self.network_check)(&entry.path) {
                continue;
            }
            out.push(entry.clone());
            if out.len() >= limit {
                break;
            }
        }
        out
    }

    /// Look into a batch of rows on the calling thread.
    ///
    /// For tests and library callers, exactly as [`HomeState::measure_now`] is, and
    /// for the same reason the application never calls it: `read_dir` on a wedged
    /// mount blocks, and the thread that draws must never be the one it blocks.
    pub fn classify_now(&mut self, limit: usize) -> bool {
        let wanted = self.unclassified_visible(limit);
        let more = self.unclassified_visible(limit + 1).len() > wanted.len();
        for entry in wanted {
            let probe = look_into(&entry);
            self.enriched
                .insert(entry.path.clone(), measured_from(&probe, &entry));
        }
        self.apply_measurements();
        more
    }

    /// Rows on or near the screen that nothing has looked into yet, up to `limit`.
    ///
    /// [`HomeState::unmeasured_visible`]'s sibling, and deliberately a different
    /// shape. Measuring walks the list from the top, which is affordable because
    /// every row wants a count eventually and a listing of a few hundred gets there.
    /// Classifying cannot work that way: a share holding six thousand date
    /// partitions would spend ten seconds reaching the row you scrolled to, and the
    /// rows on screen are the only ones anybody is reading. So this asks the
    /// viewport, plus a screen either side so arrowing off the edge does not wait for
    /// a round trip.
    ///
    /// The highlighted row comes first, then the rest of the screen, then the screen
    /// below, then the screen above: when the batch is smaller than the window, the
    /// buffer is what goes without.
    ///
    /// The highlighted row first because it is the one about to be acted on. → goes
    /// inside a folder that holds one dataset and folds the section otherwise, and the
    /// control bar offers the key on the same test, so both read better for the row
    /// being looked into in the first pass rather than the third.
    pub fn unclassified_visible(&self, limit: usize) -> Vec<Entry> {
        if limit == 0 {
            return Vec::new();
        }
        let rows = self.visible();
        // Before the first frame there is no height to go on. The top of the list is
        // where the viewport is about to be, and a batch's worth of it is the most
        // that pass could use anyway.
        let height = if self.view_height == 0 {
            limit
        } else {
            self.view_height
        };
        let top = self.scroll.min(rows.len());
        let ahead = top.saturating_add(2 * height).min(rows.len());
        let behind = top.saturating_sub(height);

        let mut out: Vec<Entry> = Vec::new();
        let order = std::iter::once(self.selected)
            .chain(top..ahead)
            .chain(behind..top);
        for row in order.filter_map(|i| rows.get(i)) {
            let Row::Entry { entry, .. } = row else {
                continue;
            };
            if entry.kind != EntryKind::Unknown {
                continue;
            }
            // Already looked into, even if the look settled nothing — a path that has
            // gone away, or a remote row that turned out not to be a directory. Asking
            // again every frame would be a `stat` per frame on the one filesystem
            // where that costs a round trip.
            if self.enriched.contains_key(&entry.path) {
                continue;
            }
            // A place in an object store is peeked into by listing it, not by reading
            // it: `read_dir` on an `s3://` URL asks the working directory about a file
            // called `s3:` and truthfully finds nothing. See
            // [`HomeState::cloud_folders_to_peek`], which is that path.
            if is_object_store_url(&entry.path) || is_cloud_place(&entry.path) {
                continue;
            }
            // The same dataset can be listed under its directory and again under
            // Recent, and looking into it twice would cost the round trip twice.
            if out.iter().any(|e| e.path == entry.path) {
                continue;
            }
            out.push((*entry).clone());
            if out.len() >= limit {
                break;
            }
        }
        out
    }

    /// Fold known measurements into the rows currently listed.
    pub fn apply_measurements(&mut self) {
        for section in &mut self.sections {
            // The door as well, whose path is the folder's: it *reads* that slot on
            // purpose, so stepping into a folder the listing above already measured
            // shows those numbers instead of a blank. Reading was never the problem —
            // writing was, and nothing writes the door's own answer anywhere now.
            for row in section.rows.iter_mut().chain(section.door.iter_mut()) {
                if let Some(m) = self.enriched.get(&row.path) {
                    row.rows = m.rows;
                    row.cols = m.cols;
                    row.cols_sampled = m.cols_sampled;
                    if let Some(kind) = m.kind {
                        row.kind = kind;
                    }
                    if m.size.is_some() {
                        row.size = m.size;
                    }
                    if !m.columns.is_empty() {
                        row.columns = m.columns.clone();
                    }
                    if !m.holds.is_empty() {
                        row.holds = m.holds.clone();
                    }
                    // Keep the source, which came from the mount table just now; take
                    // everything else, which came from the file.
                    let source = row.cost.source.take();
                    row.cost = m.cost.clone();
                    row.cost.source = source;
                }
            }
        }
    }

    /// Put the cursor on the first dataset rather than the first header, so the
    /// preview pane has something to show without a keypress.
    pub fn select_first_entry(&mut self) {
        let rows = self.visible();
        self.selected = rows
            .iter()
            .position(|r| matches!(r, Row::Entry { .. } | Row::Door { .. }))
            .unwrap_or(0);
    }

    pub fn clamp_selection(&mut self) {
        let n = self.visible().len();
        if n == 0 {
            self.selected = 0;
        } else if self.selected >= n {
            self.selected = n - 1;
        }
    }

    pub fn move_selection(&mut self, delta: isize) {
        let n = self.visible().len();
        if n == 0 {
            return;
        }
        let cur = self.selected as isize;
        let next = (cur + delta).rem_euclid(n as isize);
        self.selected = next as usize;
    }
}

/// The row for a cloud source under `CLOUD`.
fn source_entry(source: &CloudSource) -> Entry {
    Entry {
        path: cloud_place(&source.id),
        kind: EntryKind::Directory,
        name: source.label.clone(),
        size: None,
        modified: source.listed_at,
        rows: None,
        cols: None,
        cols_sampled: false,
        columns: Vec::new(),
        cost: Default::default(),
        holds: Default::default(),
        opens_whole_folder: false,
    }
}

/// The row for one bucket.
fn bucket_entry(url: &Path) -> Entry {
    let mut entry = Entry::directory(url);
    // The bucket name, not the last path segment of a URL, which for `gs://name` is the
    // whole thing anyway but reads as an accident. A source ID is not part of the name.
    let text = url.to_string_lossy();
    let (_, plain) = crate::source::split_source_id(&text);
    entry.name = plain
        .rsplit('/')
        .find(|part| !part.is_empty())
        .unwrap_or("")
        .to_string();
    entry
}

/// Build an entry for a path that is already known (a recent), classifying it.
fn entry_for_path(path: &Path, remote: bool) -> Entry {
    let mut holds = discover::Holds::default();
    // Classifying reads the directory, and stat'ing gives size and mtime. Both touch
    // the filesystem, so a remote entry is listed by name alone until its probe lands.
    let kind = if remote {
        // A name is all there is to go on without reading the path. An extension
        // settles it; anything else stays Unknown rather than being called a plain
        // directory, which would contradict the same dataset listed under its root as
        // `hive` once that root's probe lands.
        //
        // An extension datui has no reader for settles it too. `s3://bucket/data.dat`
        // is certainly not a prefix, and calling it Unknown sent → into an empty
        // listing with nothing to say why (#283). Excluding Unknown from what → enters
        // was the other way to fix that, and it is the label deciding access one
        // indirection along — every row on a share is Unknown before anything has
        // looked into it. So the row is named instead. A trailing slash is a prefix
        // whatever is in the name, which is what `exports/` and `2024.01.15/` are.
        let named = path.to_string_lossy();
        let dotted = !named.ends_with('/')
            && named
                .rsplit('/')
                .next()
                .is_some_and(|last| last.trim_start_matches('.').contains('.'));
        if discover::is_data_file(path) || dotted {
            EntryKind::File
        } else {
            EntryKind::Unknown
        }
    } else if path.is_dir() {
        let (kind, found) = discover::look_at_directory(path);
        holds = found;
        kind
    } else {
        EntryKind::File
    };
    let mut entry = Entry {
        path: path.to_path_buf(),
        kind,
        name: path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| path.to_string_lossy().into_owned()),
        size: None,
        modified: None,
        rows: None,
        cols: None,
        cols_sampled: false,
        columns: Vec::new(),
        cost: Default::default(),
        holds,
        opens_whole_folder: false,
    };
    if !remote && let Ok(meta) = std::fs::metadata(path) {
        if meta.is_file() {
            entry.size = Some(meta.len());
        }
        entry.modified = meta.modified().ok();
    }
    entry
}

/// Abbreviate a path with `~` for display.
pub fn display_path(path: &Path) -> String {
    if let Some(home) = dirs::home_dir()
        && let Ok(rest) = path.strip_prefix(&home)
    {
        if rest.as_os_str().is_empty() {
            return "~".to_string();
        }
        return format!("~/{}", rest.display());
    }
    path.display().to_string()
}

/// Complete a partially typed path against the directory it names.
///
/// Returns the longest unambiguous extension of `typed`, and how many candidates
/// there were. Reading a directory can block, so this is only ever called from a
/// worker — never in response to a keystroke on the interface thread.
pub fn complete_path(typed: &str) -> (String, usize) {
    let expanded = expand_user_path(typed);
    let typed_ends_in_sep = typed.ends_with('/');

    let (dir, prefix) = if typed_ends_in_sep {
        (expanded.clone(), String::new())
    } else {
        match (expanded.parent(), expanded.file_name()) {
            (Some(parent), Some(name)) => {
                (parent.to_path_buf(), name.to_string_lossy().into_owned())
            }
            _ => (expanded.clone(), String::new()),
        }
    };

    let Ok(entries) = std::fs::read_dir(&dir) else {
        return (typed.to_string(), 0);
    };

    let mut names: Vec<String> = entries
        .flatten()
        .filter_map(|e| {
            let name = e.file_name().to_string_lossy().into_owned();
            // A leading dot is only offered when it was asked for; otherwise every
            // completion in a home directory is dotfiles.
            if name.starts_with('.') && !prefix.starts_with('.') {
                return None;
            }
            name.starts_with(&prefix).then_some(name)
        })
        .collect();
    if names.is_empty() {
        return (typed.to_string(), 0);
    }
    names.sort();

    // The longest prefix every candidate agrees on: completing further would be
    // guessing between them.
    let shared = names
        .iter()
        .skip(1)
        .fold(names[0].clone(), |acc, name| common_prefix(&acc, name));

    let mut completed = typed.to_string();
    completed.truncate(typed.len() - prefix.len());
    completed.push_str(&shared);

    // A single directory gets its separator, so the next Tab descends into it.
    if names.len() == 1 && dir.join(&shared).is_dir() && !completed.ends_with('/') {
        completed.push('/');
    }
    (completed, names.len())
}

fn common_prefix(a: &str, b: &str) -> String {
    a.chars()
        .zip(b.chars())
        .take_while(|(x, y)| x == y)
        .map(|(x, _)| x)
        .collect()
}

/// Expand `~` and `$VAR` in a path the user typed.
pub fn expand_user_path(raw: &str) -> PathBuf {
    crate::config::expand_config_path(raw)
}

#[cfg(test)]
mod holds_flow_tests {
    use super::*;

    fn counted(n: usize) -> crate::discover::Holds {
        crate::discover::Holds {
            formats: vec![("parquet".to_string(), n)],
            ..Default::default()
        }
    }

    /// The claim `peek_cloud_folders` stakes before its answers arrive, so a rebuild in
    /// the meantime does not ask the store again: a `Directory` that counted nothing.
    fn in_flight() -> (EntryKind, crate::discover::Holds) {
        (EntryKind::Directory, crate::discover::Holds::default())
    }

    #[test]
    fn a_claim_staked_before_a_peek_lands_keeps_the_count_a_row_already_has() {
        let root = std::path::PathBuf::from("s3://bucket/warehouse");
        let path = root.join("orders");
        let mut row = Entry::for_test(&path, "orders");
        row.kind = EntryKind::Directory;
        // Restored from the facts cache on the way in, which is the only reason a
        // remote row has a count before anything peeked at it.
        row.holds = counted(15);

        let mut home = HomeState::default();
        home.probed.insert(root.clone(), vec![row]);
        home.cloud_kinds.insert(path, in_flight());
        home.apply_cloud_kinds(&root);

        assert_eq!(
            home.probed[&root][0].holds.label(),
            "15 parquet",
            "the placeholder erased a count the row already had"
        );
    }

    #[test]
    fn a_peeks_answer_replaces_the_count_a_row_had() {
        let root = std::path::PathBuf::from("s3://bucket/warehouse");
        let path = root.join("orders");
        let mut row = Entry::for_test(&path, "orders");
        row.kind = EntryKind::Directory;
        row.holds = counted(15);

        let mut home = HomeState::default();
        home.probed.insert(root.clone(), vec![row]);
        home.cloud_kinds
            .insert(path, (EntryKind::MultiFile, counted(40)));
        home.apply_cloud_kinds(&root);

        assert_eq!(home.probed[&root][0].holds.label(), "40 parquet");
        assert_eq!(home.probed[&root][0].kind, EntryKind::MultiFile);
    }

    #[test]
    fn a_peek_answers_only_the_rows_that_asked() {
        let root = std::path::PathBuf::from("s3://bucket/warehouse");
        // A row the listing already settled. Its path is in `cloud_kinds` — a peek was
        // answered for it once — and it must not be read back over the top of a kind
        // the listing was surer of.
        let settled = root.join("sales");
        let mut row = Entry::for_test(&settled, "sales");
        row.kind = EntryKind::Hive;
        row.holds = counted(40);

        let mut home = HomeState::default();
        home.probed.insert(root.clone(), vec![row]);
        home.cloud_kinds
            .insert(settled, (EntryKind::Directory, counted(1)));
        home.apply_cloud_kinds(&root);

        assert_eq!(home.probed[&root][0].kind, EntryKind::Hive);
        assert_eq!(home.probed[&root][0].holds.label(), "40 parquet");
    }

    #[test]
    fn only_folders_nothing_has_looked_into_are_queued_for_a_peek() {
        let root = std::path::PathBuf::from("s3://bucket/warehouse");
        let mut home = HomeState::default();
        let rows: Vec<Entry> = ["a", "b", "c", "d", "e"]
            .iter()
            .map(|n| {
                let mut row = Entry::for_test(&root.join(n), n);
                row.kind = EntryKind::Directory;
                row
            })
            .collect();
        home.probed.insert(root.clone(), rows);
        // One already answered, so four are left to ask about.
        home.cloud_kinds
            .insert(root.join("b"), (EntryKind::MultiFile, counted(3)));

        let asked = home.cloud_folders_to_peek(&root, 3);
        assert_eq!(asked.len(), 3, "the budget is a budget");
        assert!(
            !asked.contains(&root.join("b")),
            "a folder already looked into is not asked again"
        );
    }

    #[test]
    fn a_measurement_that_counted_nothing_keeps_the_count_a_row_already_has() {
        let path = std::path::PathBuf::from("/data/warehouse/orders");
        let mut row = Entry::for_test(&path, "orders");
        row.kind = EntryKind::Directory;
        row.holds = counted(15);

        let mut home = HomeState::default();
        home.sections.push(Section {
            title: "Here".to_string(),
            subtitle: None,
            origin: None,
            rows: vec![row],
            unavailable: false,
            unavailable_note: None,
            folded_by_default: false,
            remote_root: None,
            waiting: false,
            grouped_by_place: false,
            door: None,
            place_labels: Default::default(),
        });
        // A measurement of a file carries no `holds`, and the same struct measures both.
        home.enriched.insert(
            path,
            Measured {
                kind: Some(EntryKind::Directory),
                ..Default::default()
            },
        );
        home.apply_measurements();

        assert_eq!(
            home.sections[0].rows[0].holds.label(),
            "15 parquet",
            "a measurement with nothing to say erased the label"
        );
    }
}

#[cfg(test)]
mod known_facts_tests {
    use super::*;
    use crate::cache::DatasetFacts;

    /// What a folder holds comes back with its kind, on both routes.
    ///
    /// A row given a kind from the cache is never looked into again — `look_into` only
    /// classifies an `Unknown`, and `unclassified_visible` skips anything else. So a
    /// count left behind is left behind for the session: the row says `dir` about a
    /// folder of fifteen Parquet files, and `enrich` goes on to describe it by whatever
    /// is in its subfolders.
    #[test]
    fn what_a_folder_holds_is_restored_beside_its_kind() {
        let holds = crate::discover::Holds {
            formats: vec![("parquet".to_string(), 15)],
            ..Default::default()
        };
        for (path, remote) in [
            (
                std::path::PathBuf::from("s3://bucket/warehouse/orders"),
                true,
            ),
            (std::path::PathBuf::from("/data/warehouse/orders"), false),
        ] {
            let facts = DatasetFacts {
                mtime: 0,
                size: 4096,
                // A row count a folder's record has no business carrying, to prove
                // the gate below still turns it away.
                rows: Some(999),
                cols: Some(72),
                cols_sampled: false,
                columns: vec!["lat".to_string()],
                // A folder of separate tables: the kind the footers settled on, its
                // width, and no row count, because a sum over them is not a number.
                kind: Some(EntryKind::Directory),
                classified_by: crate::discover::CLASSIFIER_VERSION,
                holds: holds.clone(),
                cost: Default::default(),
            };
            let mut row = Entry::directory(&path);
            row.kind = EntryKind::Unknown;
            row.modified = Some(std::time::UNIX_EPOCH);
            // No size, which is what a listing gives a directory — and what makes the
            // byte fingerprint below unable to speak for one.
            assert_eq!(row.size, None);
            let index = std::collections::HashMap::from([(path.clone(), facts)]);

            apply_known_facts(&mut row, &index, remote);
            assert_eq!(row.kind, EntryKind::Directory, "{path:?}");
            assert_eq!(row.label(), "15 parquet", "{path:?}");
            // And nothing the footers said. A directory's mtime moves when an entry
            // is added, removed or renamed; a file rewritten in place moves nothing,
            // and the width, the size and the column names all change with it. Only
            // locally — a remote row is never measured here at all, so the record is
            // all it will ever have and it takes the whole of it.
            if !remote {
                assert_eq!(row.rows, None, "{path:?}");
                assert_eq!(row.cols, None, "{path:?}");
                assert_eq!(row.size, None, "{path:?}");
                assert!(row.columns.is_empty(), "{path:?}");
            }
        }
    }

    /// A dataset's own counts are not restored beside its kind. `unmeasured_visible`
    /// skips a row that already has a row count, so restoring one would stop a `hive`
    /// or a `multi` folder ever being measured again — and its size and its codec,
    /// which nothing else fills in, would be blank for the rest of the session.
    #[test]
    fn a_datasets_counts_are_measured_rather_than_restored() {
        let path = std::path::PathBuf::from("/data/warehouse/events");
        let facts = DatasetFacts {
            mtime: 0,
            size: 4096,
            rows: Some(1_200_000),
            cols: Some(58),
            cols_sampled: false,
            columns: vec!["ts".to_string()],
            kind: Some(EntryKind::MultiFile),
            classified_by: crate::discover::CLASSIFIER_VERSION,
            holds: crate::discover::Holds {
                formats: vec![("parquet".to_string(), 15)],
                ..Default::default()
            },
            cost: Default::default(),
        };
        let mut row = Entry::directory(&path);
        row.kind = EntryKind::Unknown;
        row.modified = Some(std::time::UNIX_EPOCH);
        let index = std::collections::HashMap::from([(path.clone(), facts)]);

        apply_known_facts(&mut row, &index, false);
        assert_eq!(row.kind, EntryKind::MultiFile, "the kind comes back");
        assert_eq!(row.label(), "15 parquet", "and what it holds");
        assert_eq!(
            row.rows, None,
            "but not the count: the measuring pass skips a row that has one"
        );
        assert_eq!(row.cols, None);
    }

    /// A kind recorded by a build that classified differently is not restored.
    ///
    /// A remote row was never stat'ed, so its cached kind is all it has and is restored
    /// rather than re-derived. That makes it a way for an answer this build would not
    /// give to come back: a Delta root measured before lake tables were recognized was
    /// recorded as `multifile`, and restoring that opens it as one table again — #237
    /// read back off disk. Everything else in the record is a measurement rather than a
    /// judgement, and survives.
    #[test]
    fn a_kind_from_an_older_classifier_is_not_restored() {
        let remote = std::path::PathBuf::from("s3://bucket/warehouse/orders");
        let facts = |classified_by| DatasetFacts {
            mtime: 0,
            size: 4096,
            rows: Some(1_000),
            cols: Some(7),
            cols_sampled: false,
            columns: vec!["id".into(), "amount".into()],
            kind: Some(EntryKind::MultiFile),
            classified_by,
            cost: Default::default(),
            holds: Default::default(),
        };
        let unprobed = || {
            let mut row = Entry::directory(&remote);
            row.kind = EntryKind::Unknown;
            row
        };

        let index = |classified_by| {
            std::collections::HashMap::from([(remote.clone(), facts(classified_by))])
        };

        let mut row = unprobed();
        apply_known_facts(&mut row, &index(crate::discover::CLASSIFIER_VERSION), true);
        assert_eq!(
            row.kind,
            EntryKind::MultiFile,
            "this build's own answer comes back"
        );

        let mut row = unprobed();
        apply_known_facts(&mut row, &index(0), true);
        assert_eq!(
            row.kind,
            EntryKind::Unknown,
            "an older build's does not: it may be a lake table this one would recognize"
        );
        assert_eq!(
            row.rows,
            Some(1_000),
            "but what it measured is still measured"
        );
        assert_eq!(row.columns, vec!["id".to_string(), "amount".to_string()]);
    }
}

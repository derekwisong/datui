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
//! 1. **Configured** — `[home] directories`, a `PATH`-shaped list of places.
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
    /// Kept with Ctrl+D on the home screen: `[home] directories` without editing it.
    Remembered,
    /// Derived from the desktop's own recently-used list.
    Desktop,
}

impl RootOrigin {
    pub fn note(self) -> &'static str {
        match self {
            RootOrigin::Cwd => "current directory",
            RootOrigin::Configured => "configured",
            RootOrigin::Remembered => "remembered",
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

/// What to call the top of a place in an object store: a source, an account, a bucket
/// or a container.
///
/// `None` for anything else, including a directory inside a bucket: that is labelled by
/// what it holds, like a local one, and a spinner stands in until it has been looked
/// into. `prefix` said nothing a user could act on.
///
/// Worth the few lines. A bucket labelled `dir` is not wrong so much as unhelpful: the
/// word that tells you what you are looking at is the one the service uses for it, and
/// it is exactly what decides whether stepping out of it leaves the store.
pub fn object_place_label(path: &Path) -> Option<&'static str> {
    if cloud_source_id(path).is_some() {
        return Some("source");
    }
    if cloud_account(path).is_some() {
        return Some("account");
    }
    let text = path.to_string_lossy();
    if let Some((_, _, key)) = crate::source::azure_parts(&text) {
        return key.trim_matches('/').is_empty().then_some("container");
    }
    let (scheme, rest) = text.split_once("://")?;
    if !matches!(scheme, "s3" | "s3a" | "gs" | "gcs") {
        return None;
    }
    let rest = rest.trim_end_matches('/');
    if rest.is_empty() {
        return None;
    }
    (!rest.contains('/')).then_some("bucket")
}

/// A directory in an object store that has not said what it holds yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloudLook {
    /// Not asked about: `…`.
    Waiting,
    /// Its peek is out, or its answer has not reached the row: a spinner.
    Looking,
    /// Its peek failed: `?` until Ctrl+R.
    Failed,
}

/// How a cloud source is addressed on the home screen: `cloud://<id>`. Not a URL any
/// library reads; it names the level above a source's buckets, which no real URL can.
pub const CLOUD_PLACE: &str = "cloud://";

/// Lines kept between the cursor and the edge of the list while it can scroll.
const SCROLL_MARGIN: usize = 2;

/// The first line of a list `height` lines tall that keeps line `selected` on
/// screen, given the list started at `top`.
///
/// The view stays put while the cursor moves inside it, and scrolls only as far as
/// keeps the cursor [`SCROLL_MARGIN`] lines from an edge. It never starts so low
/// that the last line rises above the bottom: folding, filtering or a taller
/// terminal shows more rows rather than empty space.
pub(crate) fn settle_top(top: usize, selected: usize, height: usize, total: usize) -> usize {
    if height == 0 {
        return top.min(selected);
    }
    // A short list keeps the cursor reachable at every line.
    let margin = SCROLL_MARGIN.min((height - 1) / 2);
    let top = if selected < top + margin {
        selected.saturating_sub(margin)
    } else if selected + margin >= top + height {
        selected + margin + 1 - height
    } else {
        top
    };
    top.min(total.saturating_sub(height))
}

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

/// Whether two locations are one place, however a trailing slash or an Azure URL is
/// spelled.
fn same_place(a: &Path, b: &Path) -> bool {
    let (a, b) = (a.to_string_lossy(), b.to_string_lossy());
    within(&a, &b) && within(&b, &a)
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

/// The URL that opens a cloud directory as one dataset: with its trailing slash, which is
/// what makes it a prefix to scan rather than an object to fetch.
pub fn directory_dataset_url(path: &Path) -> PathBuf {
    let text = path.to_string_lossy();
    if text.ends_with('/') {
        path.to_path_buf()
    } else {
        PathBuf::from(format!("{text}/"))
    }
}

/// A row that opens the directory being browsed as one table, whatever its label says.
///
/// The second of the two doors. A label describes what is directly inside a directory; it
/// does not decide what the directory can give you, so every directory carries this row
/// and the worst a wrong label can cost is one keystroke. It used to be offered in a
/// bucket only, and there only for the two kinds the listing had already called a dataset
/// — which is the same judgement twice, and left a local directory of separate tables
/// with no way to read them together at all.
///
/// Built from the listing already on screen, so it costs nothing to look at.
///
/// Not behind `feature = "cloud"`, though it was while the row belonged to a bucket.
/// Since it is offered in every directory, the gate left a `--no-default-features` build
/// with no second door at all, local directories included, while the help text and three
/// doc pages described it unconditionally. Only the remote classifier needs the gate.
fn whole_directory_row(dir: &Path, rows: &[Entry], remote: bool) -> Option<Entry> {
    // Not a directory: a `cloud://<id>/<account>` place stands for an Azure storage
    // account, whose children are containers and which has no URL to open.
    if cloud_account(dir).is_some() {
        return None;
    }
    let directories: Vec<String> = rows
        .iter()
        .filter(|r| !matches!(r.kind, EntryKind::File | EntryKind::Other))
        .map(|r| format!("{}/", r.name))
        .collect();
    let objects: Vec<(String, u64)> = rows
        .iter()
        .filter(|r| matches!(r.kind, EntryKind::File | EntryKind::Other))
        .map(|r| (r.path.to_string_lossy().into_owned(), r.size.unwrap_or(1)))
        .collect();
    // Each route asked in its own vocabulary. Feeding a local listing to the cloud
    // classifier got two answers wrong in opposite directions: `scan_dir` drops dotted
    // names, so `.hoodie` never reached it and a local Hudi table came back
    // `MultiFile` — the door then read its tombstones, two keystrokes after the row
    // above said datui does not read Hudi tables. And the cloud Iceberg rule is the
    // looser of the two on purpose, name-shape only, so a plain directory holding `data/`
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
            crate::cloud_browse::look_at_listing(&dir.to_string_lossy(), &directories, &objects)
        }
        // Without the cloud feature there is no remote classifier to ask, and reading
        // the share here is the one thing this branch exists to avoid. The door is
        // still offered — that is the whole of what it promises — and carries no kind,
        // which costs it the lake check and nothing else: its label is suppressed
        // either way, because the row is about the directory rather than in it.
        #[cfg(not(feature = "cloud"))]
        {
            let _ = (&directories, &objects);
            (EntryKind::Unknown, Default::default())
        }
    } else {
        crate::discover::look_at_directory(dir)
    };
    // Nothing in it to open. An empty directory is the one place a second door leads
    // nowhere, and a row promising to read nothing is worse than no row. A directory
    // holding only a `_SUCCESS` is that directory too.
    //
    // Asked of what the directory holds and not only of what the listing showed, because
    // the two differ on the directory that most needs the door: Spark and GBIF write part
    // files with no extension, no name in there says data, so nothing is listed — and a
    // guard on the rows alone made that directory a dead end, nothing listed and no way
    // to read it, though the open reads it by its bytes perfectly well. Files with no
    // extension count here for that reason, and so do subdirectories, whose data is a
    // level down. A file whose extension no reader takes does not: a directory of notes
    // has nothing for the door to read.
    let openable_row = rows.iter().any(|r| r.kind != EntryKind::Other);
    if !openable_row && holds_nothing_to_open(&holds) {
        return None;
    }
    let mut entry = Entry::directory(&directory_dataset_url(dir));
    entry.kind = kind;
    // What the listing you are looking at holds. Not the same tally as the directory's
    // own row upstairs: that one was counted from one page of a peek and may say `100+`,
    // and this one is counted from rows a listing has already dropped its markers from,
    // so it reports fewer skipped. Two views of one directory, each true of what it saw.
    entry.holds = holds;
    entry.opens_whole_directory = true;
    entry.name = door_name(&entry, rows);
    Some(entry)
}

/// The directory a door opens, named the way the section title above it names the
/// same place, or the two disagree about the directory you are standing in. A source id
/// is not part of the name — `s3://lab@bucket` is titled `bucket` — and an Azure
/// container is named by container, not by the long URL its last component happens to be.
///
/// `file_name` rather than splitting on `/` for the rest: at the filesystem root there
/// is no last component and the row was named `" (all files)"`, and on Windows the
/// separator is not the one a split would look for.
fn door_base_name(dir: &Path) -> String {
    let text = dir.to_string_lossy();
    if let Some((_, container, key)) = crate::source::azure_parts(&text) {
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
    }
}

/// What a directory's door opens, from the kind and the tally already in hand.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DoorKind {
    /// `key=value` partitions: one table, read with its partition columns.
    Hive,
    /// Files of one format that read as one table.
    OneSchema,
    /// Files of one format whose footers or headers disagree: the read is a union.
    SchemasDiffer,
    /// One data file, beside whatever else.
    Single,
    /// A Delta, Iceberg or Hudi root, whose files are not its rows.
    Lake,
    /// More than one format, files beside subdirectories, or only subdirectories.
    Mixed,
    /// Nothing has said what is here.
    Unknown,
}

/// Which [`DoorKind`] a door is.
///
/// A directory the footers or headers turned down as one table is `Directory` with one
/// format left in its tally, and that is the only route to one: the listing alone calls
/// such a directory `MultiFile`.
pub fn door_kind(door: &Entry) -> DoorKind {
    let holds = &door.holds;
    match door.kind {
        EntryKind::Hive => DoorKind::Hive,
        EntryKind::MultiFile => DoorKind::OneSchema,
        k if k.is_lake_table() => DoorKind::Lake,
        EntryKind::Unknown => DoorKind::Unknown,
        _ => {
            let Some(format) = holds.one_format() else {
                return DoorKind::Mixed;
            };
            if holds.directories > 0 {
                return DoorKind::Mixed;
            }
            let files = holds.data_files();
            if files == 1 {
                return DoorKind::Single;
            }
            let mostly_data = files * 2 >= files + holds.not_read + holds.unnamed;
            let reads_many = crate::FileFormat::from_name(format)
                .is_some_and(crate::FileFormat::reads_many_files);
            if mostly_data && reads_many {
                DoorKind::SchemasDiffer
            } else {
                DoorKind::Mixed
            }
        }
    }
}

/// Whether stepping into a directory puts the cursor on its door: only when the door
/// opens the directory as the one dataset its name says, which is what `Enter` on the
/// directory's own row one level up opens too. Anywhere else the first `Enter` would
/// start a combined read nobody asked for.
pub fn door_lands(door: &Entry) -> bool {
    matches!(door_kind(door), DoorKind::Hive | DoorKind::OneSchema)
}

/// A format as prose names it, by the name the listing counted: `Parquet`, `CSV`.
fn format_title(name: &str) -> String {
    crate::FileFormat::from_name(name)
        .map_or_else(|| name.to_ascii_uppercase(), |f| f.title().to_string())
}

/// The partition keys a hive door names: the layout the footers pass found, else the
/// `key=value` names in the listing on screen, which in a bucket are the levels seen so
/// far.
fn door_keys(door: &Entry, rows: &[Entry]) -> Vec<String> {
    if let Some(layout) = door.cost.partitions.as_ref()
        && !layout.keys.is_empty()
    {
        return layout.keys.clone();
    }
    let mut keys: Vec<String> = Vec::new();
    for row in rows {
        if let Some((key, _)) = row.name.split_once('=')
            && !key.is_empty()
            && !keys.iter().any(|k| k == key)
        {
            keys.push(key.to_string());
        }
    }
    keys
}

/// The door's name: the directory, and what `Enter` on it opens.
pub fn door_name(door: &Entry, rows: &[Entry]) -> String {
    let name = door_base_name(&door.path);
    let holds = &door.holds;
    let more = if holds.truncated { "+" } else { "" };
    let files = |format: &str| {
        let count = holds.data_files();
        let word = if count == 1 { "file" } else { "files" };
        format!("{count}{more} {} {word}", format_title(format))
    };
    let what = match door_kind(door) {
        DoorKind::Hive => {
            let keys = door_keys(door, rows);
            if keys.is_empty() {
                "hive table".to_string()
            } else {
                format!("hive table: {}", keys.join(", "))
            }
        }
        DoorKind::OneSchema => match (holds.model_weights(), holds.one_format()) {
            (Some((format, count)), _) => {
                let word = if count == 1 { "file" } else { "files" };
                format!("model, {count}{more} {} {word}", format_title(format))
            }
            (None, Some(format)) => format!("{}, one schema", files(format)),
            (None, None) => "one table".to_string(),
        },
        DoorKind::SchemasDiffer => {
            format!(
                "{}, schemas differ",
                files(holds.one_format().unwrap_or(""))
            )
        }
        DoorKind::Single => files(holds.one_format().unwrap_or("")),
        DoorKind::Lake => format!(
            "{} files, not the table",
            door.kind.lake_name().unwrap_or_default()
        ),
        DoorKind::Mixed => "all files, mixed".to_string(),
        DoorKind::Unknown => "all files".to_string(),
    };
    format!("{name} ({what})")
}

/// What `Enter` on a door that is not one table reads, and what it leaves out, for the
/// details pane. The local open reads the commonest format's files directly inside; a
/// directory of Parquet with subdirectories, or with no files of its own, is scanned
/// whole for Parquet instead. A prefix in an object store is scanned whole in its
/// commonest format.
pub fn door_reads(door: &Entry) -> Option<(String, Option<String>)> {
    if !matches!(door_kind(door), DoorKind::Mixed | DoorKind::Single) {
        return None;
    }
    let holds = &door.holds;
    let more = if holds.truncated { "+" } else { "" };
    let plain = holds.directories.saturating_sub(holds.partitions);
    let directories = |n: usize| {
        let word = if n == 1 { "directory" } else { "directories" };
        format!("{n}{more} {word}")
    };
    let Some((format, count)) = holds.formats.first() else {
        return (holds.directories > 0).then(|| ("every Parquet file below".to_string(), None));
    };
    // A prefix in an object store is scanned whole, subdirectories included.
    let remote = is_object_store_url(&door.path);
    let below = remote || holds.partitions > 0 || (holds.formats.len() == 1 && format == "parquet");
    let below = below && holds.directories > 0;
    let reads = if below && remote {
        format!("every {format} file below")
    } else if below {
        "every Parquet file below".to_string()
    } else {
        format!("{count}{more} {format}")
    };
    let mut skips: Vec<String> = holds
        .formats
        .iter()
        .skip(1)
        .map(|(name, n)| format!("{n}{more} {name}"))
        .collect();
    if !below && plain > 0 {
        skips.push(directories(plain));
    }
    Some((reads, (!skips.is_empty()).then(|| skips.join(", "))))
}

/// List a file a format spec's glob names as data, under the spec's name. Its name is
/// all that is asked: the listing reads nothing more for it.
pub fn name_by_spec(formats: &crate::formats::Registry, rows: &mut [Entry]) {
    if formats.is_empty() {
        return;
    }
    let mut named = false;
    for row in rows
        .iter_mut()
        .filter(|r| r.kind == EntryKind::Other && r.format_spec.is_none())
    {
        if let Some(spec) = formats.by_glob(&row.path, false).first() {
            row.kind = EntryKind::File;
            row.format_spec = Some(spec.name.clone());
            named = true;
        }
    }
    // A file read as several variants is a place too: → lists them.
    for row in rows
        .iter_mut()
        .filter(|r| r.kind == EntryKind::File && r.format_spec.is_some() && r.table.is_none())
    {
        if let Some(spec) = formats.variants_of(&row.path) {
            row.cost.tables = Some(spec.records.variants.len());
        }
    }
    // Delimited text a delimited spec's glob names keeps its place and gains the name.
    for row in rows.iter_mut().filter(|r| {
        r.kind == EntryKind::File
            && r.format_spec.is_none()
            && discover::data_format(&r.path).is_some_and(|f| f.separator().is_some())
    }) {
        if let Some(spec) = formats
            .by_glob(&row.path, false)
            .into_iter()
            .find(|s| s.is_delimited())
        {
            row.format_spec = Some(spec.name.clone());
        }
    }
    // Data sorts first, and these rows are data now.
    if named {
        discover::sort_entries(rows);
    }
}

/// Whether a directory holds nothing a `(all files)` row could read.
///
/// The door's own test, named so the details pane can ask it too: the pane tells the user
/// where the whole of a directory can be read, and on a directory with no door that is a
/// promise nothing keeps. One function, or the two drift and the sentence outlives the
/// row it points at.
pub fn holds_nothing_to_open(holds: &discover::Holds) -> bool {
    holds.formats.is_empty() && holds.directories == 0 && holds.unnamed == 0
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
    /// The directory a path-titled section lists: a root, or the directory browsed.
    /// The title is abbreviated for display and cannot be turned back into a path.
    pub root: Option<PathBuf>,
    pub rows: Vec<Entry>,
    /// The row that opens the directory this section lists, as one table. Its own row,
    /// not one of `rows`.
    ///
    /// Kept apart because its path *is* the directory's — with a trailing slash, which
    /// `PathBuf` compares and hashes away — so as a row among the others it was the same
    /// key as the directory's row one level up in every path-keyed map. That cost a real
    /// bug once: measuring the door wrote a kind-less measurement into the directory's
    /// slot, the directory upstairs was then taken for already looked into, and it kept
    /// `Unknown` — no label, no `holds` line, no place in the count — for the rest of the
    /// session. A guard per walker would have to be added again by every walker written
    /// after it, so the collision is gone instead: nothing that walks `rows` or matches
    /// [`Row::Entry`] can reach the door.
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
    /// Not asked, and not listed on an earlier run. Its rows, if any, are the buckets
    /// named in the config. Entering the source or Ctrl+R lists it.
    Unlisted,
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
    /// The source ID, as in `[[cloud.connections]]` and `s3://<id>@bucket`.
    pub id: String,
    /// The row's name.
    pub label: String,
    /// The API spoken: `s3`, `gcs` or `azure`.
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
    /// Listed, or being listed, this session. Entering a source that is not lists it.
    pub asked: bool,
    /// `key  value` lines for the details pane: endpoint, region, login.
    pub details: Vec<(String, String)>,
    /// Lines for the details pane of places inside the source: an Azure account's
    /// subscription, region and namespace.
    pub place_details: std::collections::HashMap<PathBuf, Vec<(String, String)>>,
}

impl CloudSource {
    /// What the row says instead of a size: the bucket count, or why there is none.
    pub fn count_text(&self) -> String {
        match &self.status {
            CloudStatus::Failed { short, .. } if self.buckets.is_empty() => short.clone(),
            CloudStatus::Listing if self.buckets.is_empty() => String::new(),
            CloudStatus::Unlisted if self.buckets.is_empty() => "not listed".to_string(),
            _ => {
                let (one, many) = if self.api == "azure" {
                    ("account", "accounts")
                } else if self.api == "gcs" {
                    ("project", "projects")
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

    /// Mark a listing as out: a spinner in place of the count when there are no buckets
    /// to show yet, beside it when there are.
    pub fn begin_listing(&mut self) {
        self.asked = true;
        if self.status == CloudStatus::Unlisted && self.buckets.is_empty() {
            self.status = CloudStatus::Listing;
        } else {
            self.refreshing = true;
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

/// A `[[sources]]` collection as the home screen shows it: a section of named datasets,
/// local and remote alike.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Collection {
    /// The collection's name, as in `[[sources]]` and `[home] hide`.
    pub name: String,
    /// The section's title.
    pub label: String,
    /// The built-in catalog, rather than a collection from the config.
    pub builtin: bool,
    pub datasets: Vec<CollectionDataset>,
}

/// One dataset of a [`Collection`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CollectionDataset {
    /// The row's name.
    pub name: String,
    /// The local path with `~` and `$VAR` expanded, or the URL.
    pub location: PathBuf,
    /// `key  value` lines for the details pane.
    pub details: Vec<(String, String)>,
    /// What the collection says a remote file weighs, before it is downloaded.
    pub size: Option<u64>,
}

impl Collection {
    /// A configured collection as the home screen shows it.
    pub fn from_config(source: &crate::config::SourceConfig, builtin: bool) -> Self {
        Self {
            name: source.name.clone(),
            label: source.label().to_string(),
            builtin,
            datasets: source
                .datasets
                .iter()
                .map(|dataset| {
                    let location = dataset
                        .local_path()
                        .or_else(|| dataset.url.as_deref().map(PathBuf::from))
                        .unwrap_or_default();
                    let mut details: Vec<(String, String)> = [
                        ("about", &dataset.description),
                        ("publisher", &dataset.publisher),
                        ("license", &dataset.license),
                        ("homepage", &dataset.homepage),
                    ]
                    .into_iter()
                    .filter(|(_, value)| !value.is_empty())
                    .map(|(key, value)| (key.to_string(), value.clone()))
                    .collect();
                    match &dataset.url {
                        None => details.push(("path".to_string(), display_path(&location))),
                        Some(url) => {
                            details.push(("url".to_string(), url.clone()));
                            // How it is read: what `auth` and `connection` say, in words.
                            let login = match (&dataset.connection, dataset.auth.as_deref()) {
                                (Some(connection), _) => connection.clone(),
                                (None, Some("anonymous")) => "none".to_string(),
                                _ if !crate::config::is_object_store_dataset(url) => {
                                    "none".to_string()
                                }
                                _ => "auto".to_string(),
                            };
                            details.push(("login".to_string(), login));
                        }
                    }
                    CollectionDataset {
                        name: dataset.name.clone(),
                        location,
                        details,
                        size: dataset.size,
                    }
                })
                .collect(),
        }
    }
}

/// The collections the home screen shows, in order.
///
/// The built-in catalog keeps only what this build can open: every dataset in it is
/// remote, and a build without `cloud` or `http` would list rows that only fail.
/// A configured collection is shown whole; the user named those, and opening one
/// says why it cannot be read.
pub fn collections(config: &crate::config::AppConfig) -> Vec<Collection> {
    config
        .shown_collections()
        .iter()
        .filter_map(|source| {
            let builtin = !config.sources.iter().any(|s| s.name == source.name);
            let mut collection = Collection::from_config(source, builtin);
            if builtin {
                collection
                    .datasets
                    .retain(|d| crate::source::opens_in_this_build(&d.location));
                if collection.datasets.is_empty() {
                    return None;
                }
            }
            Some(collection)
        })
        .collect()
}

/// The row for one dataset of a collection. Nothing is read to make it but a local
/// path's own directory entry; a remote dataset is named by its URL alone.
fn collection_entry(
    dataset: &CollectionDataset,
    network_check: fn(&Path) -> bool,
    missing: &mut std::collections::HashSet<PathBuf>,
) -> Entry {
    let path = &dataset.location;
    let local = matches!(
        crate::source::input_source(path),
        crate::source::InputSource::Local(_)
    );
    let mut entry = if is_object_store_url(path) {
        if names_a_file(path) {
            entry_for_path(path, true)
        } else {
            Entry::directory(path)
        }
    } else if !local || network_check(path) {
        entry_for_path(path, true)
    } else if path.exists() {
        entry_for_path(path, false)
    } else {
        missing.insert(path.clone());
        let mut entry = entry_for_path(path, true);
        entry.kind = EntryKind::Unknown;
        entry
    };
    entry.name = dataset.name.clone();
    // A remote file is named, not stat'ed: its size is the collection's word for it.
    entry.size = entry.size.or(dataset.size);
    entry
}

/// A collection's section.
fn collection_section(
    collection: &Collection,
    network_check: fn(&Path) -> bool,
    missing: &mut std::collections::HashSet<PathBuf>,
) -> Section {
    Section {
        title: collection.label.clone(),
        subtitle: None,
        origin: Some(if collection.builtin {
            "built in"
        } else {
            "configured"
        }),
        root: None,
        rows: collection
            .datasets
            .iter()
            .map(|dataset| collection_entry(dataset, network_check, missing))
            .collect(),
        unavailable: false,
        unavailable_note: None,
        folded_by_default: false,
        remote_root: None,
        waiting: false,
        grouped_by_place: false,
        door: None,
        place_labels: Default::default(),
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
    /// What the footers said it is, when that differs from what its filenames suggested:
    /// a directory whose files turn out to be separate tables is a plain directory, not a
    /// dataset. `None` when measuring did not change what it is, which is the ordinary
    /// case. See [`crate::discover::enrich`].
    pub kind: Option<crate::discover::EntryKind>,
    /// What one listing of it found, which is what the row's label says. Carried for
    /// the same reason `cost` is: the classify pass is the only thing that counts a
    /// local directory, and a count that stops here never reaches the screen.
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
    /// The door that opens the directory being browsed as one table. See
    /// [`Section::door`].
    ///
    /// Not an `Entry` row, deliberately: it carries the directory's own path, so anything
    /// that keys a map by row path would write the door's answer into the directory's
    /// slot.
    Door { section: usize, entry: &'a Entry },
    /// What the cap on `RECENT` is hiding: `… 13 more in 5 places`.
    More {
        section: usize,
        hidden: usize,
        places: usize,
    },
    /// The last row inside a browsed directory whose files datui cannot read are
    /// hidden: `… 10 files with no reader`. Without it a directory of notes looks
    /// empty, or broken. `Enter` shows them, as `Ctrl+A` does.
    Hidden { section: usize, count: usize },
}

impl Row<'_> {
    pub fn section(&self) -> usize {
        match self {
            Row::Header { section, .. }
            | Row::Entry { section, .. }
            | Row::Door { section, .. }
            | Row::Place { section, .. }
            | Row::More { section, .. }
            | Row::Hidden { section, .. } => *section,
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
    /// The door, by the directory it opens. Its own variant for the same reason the row
    /// is: keyed as an `Entry` it would put the cursor on the directory's row instead.
    Door(PathBuf),
    Place(PathBuf),
    More(String),
    Hidden(String),
}

/// Home screen state.
#[derive(Debug)]
pub struct HomeState {
    pub sections: Vec<Section>,
    /// Fuzzy filter over every row in every section.
    pub filter: String,
    /// The most search matches listed under `Found`: `[home.search] max_results`.
    pub search_limit: usize,
    /// The filter `Found`'s rows were scored for, and the score of each of its first
    /// rows, in order. Listing a thousand matches scored each of them again on every
    /// pass over the rows, several per frame.
    pub found_scores: Option<(String, Vec<i32>)>,
    /// Leave out files datui has no reader for. `Ctrl+A` flips it; they are hidden by
    /// default.
    pub hide_unreadable: bool,
    /// The format specs on the search path: a file one of them names by its glob is
    /// listed as data, under the spec's name.
    pub formats: std::sync::Arc<crate::formats::Registry>,
    /// The lake table being browsed, and its format. Said on its heading for as long as
    /// the browse lasts, since it is a fact about the directory rather than an event.
    pub lake_here: Option<(PathBuf, &'static str)>,
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
    /// What the `~` prompt lists: the directory being typed and the names in it.
    pub path_listing: Option<PathListing>,
    /// The name ↑↓ put the cursor on, among those the last segment matches.
    pub path_pick: Option<usize>,
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
    /// How often and how lately each recent was opened: ranks matches (#547 M9).
    pub visits: std::collections::HashMap<PathBuf, crate::cache::Visits>,
    /// The recent opened last. Recent is ranked by frecency, and the cursor lands here
    /// so the last file is still one Enter away.
    pub newest_recent: Option<PathBuf>,
    /// Network roots whose listing has come back, keyed by path.
    pub probed: std::collections::HashMap<PathBuf, Vec<Entry>>,
    /// Network roots that did not answer.
    pub unreachable: std::collections::HashSet<PathBuf>,
    /// The rows of network directories still being listed, read so far.
    pub listing_so_far: std::collections::HashMap<PathBuf, Vec<Entry>>,
    /// Network directories whose listing stopped at [`discover::MAX_ENTRIES_PER_DIR`].
    pub cut_short: std::collections::HashSet<PathBuf>,
    /// The names a filter asked the server for, in a cloud directory cut short.
    pub narrowed: Option<Narrowed>,
    /// Why a cloud listing was refused, when the service said.
    pub probe_errors: std::collections::HashMap<PathBuf, String>,
    /// What cloud directories turned out to hold when peeked into: `hive` or `multi`.
    /// Kept for the session, so a directory is peeked at once however often it is listed.
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
    /// The same, for cloud directories on screen nothing has peeked into yet.
    pub pending_peek: bool,
    /// Cloud directories with a peek out. Their own set rather than a claim written into
    /// [`Self::cloud_kinds`]: a claim is an answer, and writing one before the request
    /// comes back put `dir` on a row that had a count and staked "never again this
    /// session" on a request that might fail.
    pub peeking: std::collections::HashSet<PathBuf>,
    /// Cloud directories whose peek failed: not asked again until Ctrl+R, and labelled
    /// `?` rather than `dir`, which would claim there is no data inside.
    pub peek_failed: std::collections::HashSet<PathBuf>,
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
    /// The saved folds are to be read again, with the next listing: entering the home
    /// screen asks for them, and they arrive with the rows they fold.
    pub folds_owed: bool,
    /// Datasets found by walking below the working directory.
    pub search: SearchState,
    /// What datui measured on previous runs, keyed by path — the same index the
    /// listing was annotated from, kept here so rows the recursive search finds
    /// can be filled in the same way (columns are what the filter matches on).
    pub known: std::collections::HashMap<PathBuf, crate::cache::DatasetFacts>,
    /// Cloud sources discovered on this machine or named in the config, with their
    /// buckets. Empty on a machine with no cloud credentials, which is the common case
    /// and not a failure.
    pub cloud: Vec<CloudSource>,
    /// The `[[sources]]` collections shown, each a section of its own.
    pub collections: Vec<Collection>,
    /// Local datasets of a collection that the last listing found missing.
    pub missing: std::collections::HashSet<PathBuf>,
    /// When the current wait for a remote listing began, for the elapsed time on screen.
    pub waiting_since: Option<std::time::Instant>,
    /// `RECENT` shows every place, however many rows that takes. Set by `Enter` on the
    /// `… N more` row, for the session.
    pub recent_expanded: bool,
    /// The listings the user went inside from, outermost first: where to put the
    /// cursor back on the way out. See [`HomeState::leave_mark`].
    pub trail: Vec<Mark>,
    /// The row the cursor goes back to once the listing being returned to lands.
    /// Held across listings while rows are still arriving, since the row may not be in
    /// the first one; dropped as soon as the user moves the cursor.
    pub returning: Option<RowKey>,
    /// How far down the list the row being returned to was when the user left it, so
    /// it comes back on the same line rather than wherever the scroll falls.
    pub returning_line: Option<usize>,
    /// The cursor is where [`HomeState::select_first_entry`] put it, and the user has not
    /// moved it since: a door the footers turn down afterwards takes it to the first row.
    pub landing: bool,
}

/// Where the cursor was in a listing the user went inside from.
///
/// By row identity, not index: the listing returned to is rebuilt on a worker and
/// lands later, and may have changed in the meantime.
#[derive(Debug, Clone)]
pub struct Mark {
    /// The listing left: the place browsed, or `None` for the root listing.
    pub place: Option<PathBuf>,
    pub key: Option<RowKey>,
    /// The filter typed there, which entering cleared.
    pub filter: String,
    /// The search below that place, when it had finished or not started. A walk still
    /// running is dropped by its generation once the user leaves, so it is started
    /// again rather than kept.
    pub search: Option<SearchState>,
    /// Rows between the top of the list and the cursor.
    pub line: usize,
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
    /// Which walk this is, so a scoring of an earlier walk's files is never taken for
    /// this one's.
    pub epoch: u64,
    /// Every data file found so far, unfiltered, in the batches they arrived in. Shared
    /// with the worker that scores the filter against them, so handing it over copies
    /// nothing.
    pub results: Vec<std::sync::Arc<[Entry]>>,
    /// How many files `results` holds.
    pub indexed: usize,
    /// What the filter matched, as last scored: possibly for an older filter, or for
    /// fewer files than are in now, while a scoring is out.
    pub matches: Option<crate::search::Matches>,
    /// A scoring is out on a worker.
    pub scoring: bool,
    /// Directory entries examined, for the progress note.
    pub scanned: usize,
    /// A walk is out. Results may still be arriving.
    pub running: bool,
    /// The walk has finished, successfully or against a limit.
    pub done: bool,
    /// Why the walk stopped short, when it did.
    pub limited: Option<String>,
}

/// Below this many files to look at, the filter is scored where it is typed: it takes
/// a millisecond or two, and the list answers in the same frame as the key.
const SCORE_INLINE_MAX: usize = 2_000;

/// Match score a unit of frecency is worth, up to ten units: a file opened every day
/// outranks one whose name matches a little better, never one that matches far better.
const FRECENCY_LIFT: f64 = 3.0;

/// What a worker needs to score the filter against a walk's files.
#[derive(Debug, Clone)]
pub struct ScoreJob {
    pub epoch: u64,
    pub results: Vec<std::sync::Arc<[Entry]>>,
    pub query: String,
    pub base: Option<crate::search::Matches>,
    pub limit: usize,
}

impl SearchState {
    /// Forget everything, because the place being searched has changed.
    pub fn reset(&mut self) {
        *self = Self::default();
    }

    /// Set the files found, all at once: what a walk would have handed over in batches.
    pub fn set_results(&mut self, results: Vec<Entry>) {
        self.indexed = results.len();
        self.results = vec![results.into()];
        self.matches = None;
    }

    /// Every file found, in the order found.
    pub fn files(&self) -> impl Iterator<Item = &Entry> {
        self.results.iter().flat_map(|batch| batch.iter())
    }

    /// Whether the matches in hand are for `query` over every file found.
    fn scored_for(&self, query: &str) -> bool {
        self.matches
            .as_ref()
            .is_some_and(|m| m.query == query && m.upto == self.indexed)
    }

    /// The matches to narrow from for `query`, and how many files scoring it will look at.
    fn base_for(&self, query: &str) -> (Option<&crate::search::Matches>, usize) {
        match self.matches.as_ref() {
            Some(m) if m.narrows_to(query) && m.upto <= self.indexed => {
                (Some(m), m.ids.len() + self.indexed - m.upto)
            }
            _ => (None, self.indexed),
        }
    }
}

impl Default for HomeState {
    fn default() -> Self {
        Self {
            sections: Vec::new(),
            cloud: Vec::new(),
            collections: Vec::new(),
            missing: Default::default(),
            filter: String::new(),
            search_limit: crate::config::SearchConfig::default().max_results,
            found_scores: None,
            hide_unreadable: true,
            formats: Default::default(),
            lake_here: None,
            selected: 0,
            scroll: 0,
            view_height: 0,
            path_input_active: false,
            path_input: String::new(),
            path_listing: None,
            path_pick: None,
            browsing: None,
            browse_start: None,
            status: None,
            network_check: is_remote_path,
            visits: Default::default(),
            newest_recent: None,
            sort: SortMode::default(),
            listing_in_flight: false,
            measure_in_flight: false,
            classify_in_flight: false,
            pending_classify: false,
            pending_peek: false,
            peeking: std::collections::HashSet::new(),
            probed: std::collections::HashMap::new(),
            unreachable: std::collections::HashSet::new(),
            listing_so_far: std::collections::HashMap::new(),
            cut_short: std::collections::HashSet::new(),
            narrowed: None,
            probe_errors: std::collections::HashMap::new(),
            cloud_kinds: std::collections::HashMap::new(),
            peek_failed: std::collections::HashSet::new(),
            pending_enrich: false,
            waiting_since: None,
            enriched: std::collections::HashMap::new(),
            folds: std::collections::HashMap::new(),
            folds_owed: false,
            search: SearchState::default(),
            known: Default::default(),
            recent_expanded: false,
            trail: Vec::new(),
            returning: None,
            returning_line: None,
            landing: false,
        }
    }
}

/// Everything [`build_listing`] needs, gathered on the interface thread from state it
/// already has, so the worker never reaches back into the app.
#[derive(Debug, Clone)]
pub struct ListingRequest {
    pub config_dirs: Vec<PathBuf>,
    /// Directories kept with Ctrl+D, listed after the configured ones.
    pub remembered_dirs: Vec<PathBuf>,
    pub recents: Vec<PathBuf>,
    pub desktop_dirs: Vec<PathBuf>,
    pub browsing: Option<PathBuf>,
    pub probed: std::collections::HashMap<PathBuf, Vec<Entry>>,
    pub unreachable: std::collections::HashSet<PathBuf>,
    /// Rows of network directories still being listed. See [`HomeState::listing_so_far`].
    pub listing_so_far: std::collections::HashMap<PathBuf, Vec<Entry>>,
    /// See [`HomeState::cut_short`].
    pub cut_short: std::collections::HashSet<PathBuf>,
    /// See [`HomeState::narrowed`].
    pub narrowed: Option<Narrowed>,
    /// Why a cloud listing was refused.
    pub probe_errors: std::collections::HashMap<PathBuf, String>,
    pub network_check: fn(&Path) -> bool,
    /// Cloud sources and the buckets already enumerated for them.
    pub cloud: Vec<CloudSource>,
    /// The `[[sources]]` collections to list.
    pub collections: Vec<Collection>,
    /// What datui measured on a previous run. A row whose size and modification time
    /// still match is filled in from here, so the screen has counts and column names
    /// before anything has been read this time.
    pub known: std::collections::HashMap<PathBuf, crate::cache::DatasetFacts>,
    /// The format specs on the search path: a file one reads as several variants is a
    /// place whose rows are its variants.
    pub formats: std::sync::Arc<crate::formats::Registry>,
}

/// What a listing pass produced.
#[derive(Debug, Clone, Default)]
pub struct Listing {
    pub sections: Vec<Section>,
    /// Local datasets of a collection that do not exist.
    pub missing: std::collections::HashSet<PathBuf>,
}

/// Find out what a row is, and then what is in it.
///
/// One pass, because the two questions are asked of the same filesystem and the
/// thread that asks is already there. Classifying a row nothing has looked into is
/// the [`crate::discover::classify_directory`] call the listing did not make;
/// measuring is what [`crate::discover::enrich`] has always done, and it does nothing
/// for a row that turns out to be a plain directory.
pub fn look_into(entry: &Entry) -> Entry {
    look_into_as(entry, &crate::schema_union::ReadAs::default())
}

/// As [`look_into`], reading each file the way the open that follows will read it.
///
/// For the command line, which has the user's own reader settings in hand before it
/// looks. The listing passes have none and take the defaults, which is what an open
/// from the home screen is made with.
pub fn look_into_as(entry: &Entry, as_read: &crate::schema_union::ReadAs) -> Entry {
    let mut probe = classify_row(entry);
    measure_row(&mut probe, entry, as_read);
    probe
}

/// The first half of [`look_into`]: what a row nothing has looked into is.
fn classify_row(entry: &Entry) -> Entry {
    let mut probe = entry.clone();
    if probe.kind == EntryKind::Unknown && probe.path.is_dir() {
        let (kind, holds) = discover::look_at_directory(&probe.path);
        probe.kind = kind;
        probe.holds = holds;
    }
    probe
}

/// The second half of [`look_into`]: what is in it, from the files themselves.
fn measure_row(probe: &mut Entry, entry: &Entry, as_read: &crate::schema_union::ReadAs) {
    discover::enrich_as(probe, as_read);
    probe.size = probe.size.or(entry.size);
    probe.modified = probe.modified.or(entry.modified);
}

/// Look into a batch of rows, handing each answer to `each` as it arrives, and
/// remember what was learned.
///
/// What both background passes do — the one that measures rows on screen and the one
/// that classifies them — because the difference between them is which rows they pick,
/// not what is done to one. Runs on a worker; see [`look_into`] for why never here.
///
/// Every row is classified before any is measured. A kind costs one directory read and
/// a count can cost sixty-four footers, so a batch that did both a row at a time kept
/// the last row's label waiting on every footer above it.
pub fn look_into_batch(
    rows: Vec<Entry>,
    cache: &crate::cache::CacheManager,
    mut each: impl FnMut(PathBuf, Measured),
) {
    let as_read = crate::schema_union::ReadAs::default();
    let classified: Vec<(Entry, Entry)> = rows
        .into_iter()
        .map(|entry| {
            let probe = classify_row(&entry);
            if probe.kind != entry.kind {
                each(entry.path.clone(), measured_from(&probe, &entry));
            }
            (probe, entry)
        })
        .collect();

    let mut facts = Vec::new();
    for (mut probe, entry) in classified {
        measure_row(&mut probe, &entry, &as_read);
        facts.extend(facts_for(&probe));
        each(entry.path.clone(), measured_from(&probe, &entry));
    }
    // Remember what was learned, so the next run has it before reading anything.
    // Purely a cache: every record carries the size and mtime it came from and
    // invalidates itself when those change.
    cache.record_dataset_facts(&facts);
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
/// What a cloud directory cut short at the cap holds under one name prefix, asked of
/// the server because a filter was typed there.
#[derive(Debug, Clone)]
pub struct Narrowed {
    pub dir: PathBuf,
    /// The start of every name asked for (`STATION=USW`).
    pub prefix: String,
    pub rows: Vec<Entry>,
    /// These stopped at the cap too.
    pub truncated: bool,
}

pub fn build_listing(request: &ListingRequest) -> Listing {
    let ListingRequest {
        config_dirs,
        remembered_dirs,
        recents,
        desktop_dirs,
        browsing,
        probed,
        unreachable,
        listing_so_far,
        cut_short,
        narrowed,
        probe_errors,
        network_check,
        cloud,
        collections,
        known,
        formats,
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
            .map(|s| s.buckets.iter().map(|b| bucket_entry(b)).collect())
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
            root: None,
        });
        annotate(&mut sections, known, network_check, &mounts);
        return Listing {
            sections,
            ..Default::default()
        };
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
        // A remote listing still being read shows what it has, and says so.
        let so_far = remote && !probed.contains_key(&dir) && listing_so_far.contains_key(&dir);
        // A SQLite database is a place too, whose rows are its tables.
        let database = !remote && dir.is_file();
        let (mut rows, truncated) = if remote {
            let rows = probed
                .get(&dir)
                .or_else(|| listing_so_far.get(&dir))
                .cloned()
                .unwrap_or_default();
            (rows, cut_short.contains(&dir))
        } else if database {
            let tables = discover::database_rows(&dir);
            let rows = if tables.is_empty() {
                discover::variant_rows(&dir, formats)
            } else {
                tables
            };
            (rows, false)
        } else {
            let scan = discover::scan_dir_bounded(&dir);
            // A Hugging Face cache's splits, before the files they are made of.
            let mut rows = discover::split_rows(&dir);
            rows.extend(scan.entries);
            (rows, scan.truncated)
        };
        // A level cut short holds the names a filter asked the server for, beside the
        // first of the rest.
        let narrowed = narrowed
            .as_ref()
            .filter(|n| remote && truncated && n.dir == dir);
        if let Some(narrowed) = narrowed {
            let listed: std::collections::HashSet<PathBuf> =
                rows.iter().map(|row| row.path.clone()).collect();
            rows.extend(
                narrowed
                    .rows
                    .iter()
                    .filter(|row| !listed.contains(&row.path))
                    .cloned(),
            );
        }
        // A directory cut off at the cap otherwise looks exactly like one that happens
        // to hold that many things.
        let subtitle = if so_far {
            Some(format!(
                "{} so far",
                crate::numfmt::group_chrome(rows.len())
            ))
        } else if truncated {
            let first = format!(
                "first {}",
                crate::numfmt::group_chrome(discover::MAX_ENTRIES_PER_DIR)
            );
            Some(match narrowed {
                Some(n) => format!(
                    "{first} + {}{} {}*",
                    crate::numfmt::group_chrome(n.rows.len()),
                    if n.truncated { "+" } else { "" },
                    n.prefix
                ),
                None => first,
            })
        } else {
            None
        };
        let unavailable = remote && unreachable.contains(&dir);
        // The first row inside any directory opens the whole of it, since `Enter` on the
        // rows below opens one file. The other door.
        let mut door = (!database)
            .then(|| whole_directory_row(&dir, &rows, remote))
            .flatten();
        // What an earlier run's footers made of this directory, as its row upstairs is
        // given: without it a directory of separate tables is a dataset inside and a
        // place to look into one level up. Its own mtime is the fingerprint, as there.
        if !remote
            && let Some(door) = door.as_mut()
            && let Ok(meta) = std::fs::metadata(&dir)
        {
            door.modified = meta.modified().ok();
            apply_known_facts(door, known, false);
            door.modified = None;
            door.name = door_name(door, &rows);
        }
        sections.push(Section {
            // The URL without a source ID: the title bar's trail already says which
            // source, and `s3://lab@data` is not a name anyone would write. An Azure
            // account or container is titled by name, not by its long URL.
            title: {
                let text = dir.to_string_lossy();
                if let Some(dataset) = collections
                    .iter()
                    .flat_map(|c| c.datasets.iter())
                    .find(|d| is_object_store_url(&d.location) && same_place(&d.location, &dir))
                {
                    dataset.name.clone()
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
            subtitle,
            origin: None,
            root: Some(dir.clone()),
            rows,
            unavailable,
            // A browsed remote place that did not answer has nothing to add; one whose
            // listing was refused says why.
            unavailable_note: probe_errors.get(&dir).cloned(),
            folded_by_default: false,
            // Its wait is drawn in place of the whole list until rows arrive (see
            // `awaiting_listing`), and on the heading once they do.
            remote_root: None,
            waiting: so_far,
            grouped_by_place: false,
            door,
            place_labels: Default::default(),
        });
        annotate(&mut sections, known, network_check, &mounts);
        return Listing {
            sections,
            ..Default::default()
        };
    }

    // Recents that still exist, most recent first.
    let recent_rows: Vec<Entry> = recents
        .iter()
        // `exists()` stats the path, so a remote entry is taken on trust and
        // dropped later only if its probe says it is gone.
        .filter(|p| {
            network_check(p)
                || p.exists()
                || crate::members::split(p).is_some()
                || crate::members::split_variant(p, formats).is_some()
                || crate::hf_splits::split_place(p).is_some()
        })
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
            if let Some(variant) = discover::variant_row(p, formats) {
                return variant;
            }
            if !network_check(p)
                && let Some(split) = discover::split_row(p)
            {
                return split;
            }
            let mut entry = entry_for_path(p, network_check(p));
            // A dataset opened from a collection comes back under the collection's name
            // for it, not its URL's last segment (#547 D12).
            if let Some(dataset) = collections
                .iter()
                .flat_map(|c| &c.datasets)
                .find(|d| d.location == *p)
            {
                entry.name = dataset.name.clone();
                entry.size = entry.size.or(dataset.size);
            }
            entry
        })
        .collect();

    // Desktop-derived places are collected rather than expanded — see below.
    let mut elsewhere: Vec<Entry> = Vec::new();

    let roots = HomeState::roots_from(config_dirs, remembered_dirs, desktop_dirs, network_check);
    let mut root_sections: Vec<(RootOrigin, Section)> = Vec::new();
    // What the current-directory section is about to show, for the RECENT dedupe
    // below: where it is, and the names it lists.
    let mut cwd_listing: Option<(PathBuf, std::collections::HashSet<std::ffi::OsString>)> = None;
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
            truncated = cut_short.contains(&root.path);
            probed
                .get(&root.path)
                .or_else(|| listing_so_far.get(&root.path))
                .cloned()
                .unwrap_or_default()
        } else if root.available {
            let scan = discover::scan_dir_bounded(&root.path);
            truncated = scan.truncated;
            scan.entries
        } else {
            Vec::new()
        };
        if root.origin == RootOrigin::Cwd {
            // Compared canonically, because the recents store canonicalizes what it
            // keeps. A network cwd is taken as spelled: canonicalizing it is the
            // stat on a mount that may never answer, which nothing here may make.
            let key = if root.network {
                root.path.clone()
            } else {
                crate::canonical::canonicalize(&root.path).unwrap_or_else(|_| root.path.clone())
            };
            let names = rows
                .iter()
                .filter_map(|row| row.path.file_name().map(|n| n.to_os_string()))
                .collect();
            cwd_listing = Some((key, names));
        }
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
            state.push(format!(
                "first {}",
                crate::numfmt::group_chrome(discover::MAX_ENTRIES_PER_DIR)
            ));
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
                root: Some(root.path.clone()),
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

    // The current directory's datasets are listed in a section of their own
    // directly below RECENT, with facts scanned this pass — so a RECENT place for
    // the same directory repeated those rows, and the first screen after opening a
    // few local files said everything twice. A recent is dropped only when the
    // current-directory section really lists it: decided by what the sections
    // contain rather than by the place's path alone, so a recent the scan did not
    // surface — a hidden file, a directory cut off at the scan cap — is on screen
    // nowhere else and stays under RECENT, keeping its place row alive when it was
    // all the place held. A deleted recent never gets this far: the `exists`
    // filter above already dropped it. Recents in any other directory are
    // untouched, and a browsed directory needs no twin of this because browsing
    // returned above with only that directory's section and no RECENT at all.
    let recent_rows: Vec<Entry> = match &cwd_listing {
        None => recent_rows,
        Some((cwd, names)) => recent_rows
            .into_iter()
            .filter(|row| {
                let place = place_of(&row.path);
                // A local place is resolved before comparing, as the store resolves
                // what it keeps; a remote one is compared as written, because
                // canonicalizing it would stat a mount that may never answer.
                let place = if network_check(&place) {
                    place
                } else {
                    crate::canonical::canonicalize(&place).unwrap_or(place)
                };
                place != *cwd || !row.path.file_name().is_some_and(|n| names.contains(n))
            })
            .collect(),
    };
    if !recent_rows.is_empty() {
        let place_labels = place_labels(&recent_rows, known, network_check);
        sections.push(Section {
            title: HomeState::RECENT_SECTION.to_string(),
            subtitle: None,
            origin: None,
            root: None,
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
            root: None,
        });
    }

    // Collections from the config, in the order defined: named datasets rather than
    // directories, so they sit with the stores, above the directories to look through.
    let mut missing = std::collections::HashSet::new();
    let (builtin, configured): (Vec<&Collection>, Vec<&Collection>) =
        collections.iter().partition(|c| c.builtin);
    for collection in configured {
        sections.push(collection_section(collection, network_check, &mut missing));
    }

    // Configured places in the order configured, then remembered ones in the order kept.
    sections.extend(rest.into_iter().map(|(_, s)| s));

    // The built-in catalog is for when there is nothing of your own yet, so it comes
    // after everything that is.
    for collection in builtin {
        sections.push(collection_section(collection, network_check, &mut missing));
    }

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
            root: None,
        });
    }

    // Fill in whatever was measured before and still matches. `scan_dir` already
    // stat'ed every row, so verifying the fingerprint costs nothing.
    //
    // The mount table is read once for the whole listing rather than per row.
    // Resolving a path against it is string work, and it is a kernel-generated file,
    // so nothing here can block on a filesystem that has stopped answering.
    annotate(&mut sections, known, network_check, &mounts);

    Listing { sections, missing }
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
/// have written, and only a label that says something: `dir` is what every directory
/// with no data files in it says, and a place holds recents, so it says nothing.
///
/// A local place is held to the same fingerprint `apply_known_facts` asks of a directory:
/// its mtime, which moves when a file is added or removed. A record of `12 parquet`
/// for a directory that has since lost ten would otherwise sit two rows above the live
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
        // The door reads where its directory is; without this it drew the unknown
        // place's glyph on local disk (#547 D10).
        if let Some(door) = section.door.as_mut()
            && !is_cloud_place(&door.path)
        {
            door.cost.source = Some(mounts.describe(&door.path).fstype);
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

    // What a previous run found this directory to be. A listing no longer looks into a
    // directory at all — that is a `read_dir` apiece, a round trip apiece on a share —
    // so a row arrives `Unknown`, and this is the only thing that can answer for it
    // without reading the directory again.
    //
    // Only for directories a previous run *measured*, which is narrower than it sounds:
    // `facts_for` needs a size, and a directory only has one once its files were totalled
    // or sampled. A plain directory has nothing recorded and is classified again every
    // session — one `read_dir`, which is the cheap end of this. What it does cover is
    // the expensive end: a directory whose files were read and found to be separate
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
            // its kind from the cache is never looked into again, so a count left behind
            // is left behind for the session — the row says `dir` about a directory of
            // fifteen Parquet files, and `enrich` goes on to describe it by whatever is
            // in its subdirectories.
            if row.holds.is_empty() {
                row.holds = facts.holds.clone();
            }
            // The kind and the count, and nothing measured. Both of those come from
            // the directory's *names*, which is what its mtime is a fingerprint
            // for: it moves when an entry is added, removed or renamed. What the
            // footers said — the width, the size, the column names — can change with
            // no entry added or removed at all, by one file being rewritten in place,
            // and a directory mtime cannot see that. `same_bytes` below is the
            // fingerprint for those, it is a file's, and a directory has no size to offer
            // it; so a directory of separate tables shows its width in the session that
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
            // says `dir` about a directory of fifteen Parquet files for the rest of the
            // session — and `enrich` describes it by whatever is in its subdirectories.
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
        Self::roots_from(config_dirs, &[], desktop_dirs, is_network)
    }

    /// As [`HomeState::roots_with`], plus the directories kept with Ctrl+D. They come
    /// after the configured ones: a place written into the config is the more
    /// deliberate choice, and a directory in both is listed as configured.
    pub fn roots_from(
        config_dirs: &[PathBuf],
        remembered_dirs: &[PathBuf],
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
                    crate::canonical::canonicalize(&path).unwrap_or_else(|_| path.clone())
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

        for dir in remembered_dirs {
            push(dir.clone(), RootOrigin::Remembered, &mut roots, &mut seen);
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
            remembered_dirs: Vec::new(),
            recents: recents.to_vec(),
            desktop_dirs: desktop_dirs.to_vec(),
            browsing: self.browsing.clone(),
            probed: self.probed.clone(),
            unreachable: self.unreachable.clone(),
            listing_so_far: self.listing_so_far.clone(),
            cut_short: self.cut_short.clone(),
            narrowed: self.narrowed.clone(),
            probe_errors: self.probe_errors.clone(),
            network_check: self.network_check,
            cloud: self.cloud.clone(),
            collections: self.collections.clone(),
            // The synchronous path is for tests and library callers; it consults no
            // cache, so what it produces is exactly what is on disk right now.
            known: Default::default(),
            formats: self.formats.clone(),
        };
        let listing = build_listing(&request);
        self.apply_listing(listing);
    }

    /// Install a listing built elsewhere, keeping the cursor on whatever it was on.
    pub fn apply_listing(&mut self, listing: Listing) {
        let returning = self.returning.take();
        let previous = returning.clone().or_else(|| self.selected_key());
        // Rows landing above the cursor move the list, not the cursor.
        let line = self.selected.saturating_sub(self.scroll);
        let mut listing = listing;
        for section in &mut listing.sections {
            name_by_spec(&self.formats, &mut section.rows);
        }
        self.sections = listing.sections;
        self.missing = listing.missing;
        // Browsing, the first section is the directory browsed.
        if let (Some(browsing), Some((dir, format))) = (&self.browsing, &self.lake_here)
            && browsing == dir
            && let Some(section) = self.sections.first_mut()
        {
            let note = format!("{} · not read as a table", format.to_ascii_lowercase());
            section.subtitle = Some(match section.subtitle.take() {
                Some(state) => format!("{note} · {state}"),
                None => note,
            });
        }
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
        let placed = self.reselect(previous);
        // A row returned to is where the user left it, not a landing a late footer may
        // still move.
        if placed && returning.is_some() {
            self.landing = false;
        }
        if !placed {
            self.select_first_entry();
            // The row being returned to may be in a later listing: a remote place
            // still answering, or a search still walking.
            if self.rows_still_arriving() {
                self.returning = returning;
            }
        } else if returning.is_some() {
            self.scroll_to_returning_line();
        } else {
            self.scroll = self.selected.saturating_sub(line);
        }
        self.follow_selection();
    }

    /// Remember where the cursor is before going inside something, so leaving comes
    /// back to it. Call before `browsing` changes.
    pub fn leave_mark(&mut self) {
        let mark = Mark {
            place: self.browsing.clone(),
            key: self.selected_key(),
            filter: self.filter.clone(),
            // A scoring out now answers while the user is elsewhere and is dropped, so
            // the copy kept asks again when it comes back.
            search: (!self.search.running).then(|| SearchState {
                scoring: false,
                ..self.search.clone()
            }),
            line: self.selected.saturating_sub(self.scroll),
        };
        // A place already on the trail is being entered again from elsewhere; its
        // old mark describes a visit that is over.
        self.trail.retain(|m| m.place != mark.place);
        self.trail.push(mark);
    }

    /// Come back to the place now browsed, from `from`: the filter and search it had,
    /// and the cursor on the row it was on once the listing lands. Call after
    /// `browsing` is set.
    ///
    /// A place never entered from — Backspace above where the browse began — puts the
    /// cursor on the place just left instead, which is the row that leads back to it.
    pub fn come_back(&mut self, from: Option<PathBuf>) {
        let to = self.browsing.clone();
        let mark = self
            .trail
            .iter()
            .rposition(|m| m.place == to)
            .map(|at| self.trail.split_off(at).remove(0));
        match mark {
            Some(mark) => {
                self.filter = mark.filter;
                self.search = mark.search.unwrap_or_default();
                self.returning = mark.key;
                self.returning_line = Some(mark.line);
            }
            None => {
                self.filter.clear();
                self.search.reset();
                self.returning = from.map(RowKey::Entry);
                self.returning_line = None;
            }
        }
    }

    /// Whether rows may yet arrive without the user asking: a remote listing still
    /// answering, or a search still walking.
    fn rows_still_arriving(&self) -> bool {
        self.listing_in_flight
            || self.sections_waiting()
            || self.awaiting_listing().is_some()
            || self.search.running
    }

    /// Put the cursor on the row being returned to, if it has arrived.
    fn settle_return(&mut self) {
        let Some(key) = self.returning.clone() else {
            return;
        };
        if let Some(idx) = self.row_of(&key) {
            self.selected = idx;
            self.returning = None;
            self.landing = false;
            self.scroll_to_returning_line();
            self.follow_selection();
        } else if !self.rows_still_arriving() {
            self.returning = None;
        }
    }

    /// Put the row just returned to on the line it was left on, when that is known.
    fn scroll_to_returning_line(&mut self) {
        if let Some(line) = self.returning_line.take() {
            self.scroll = self.selected.saturating_sub(line);
        }
    }

    /// What the cursor is on, as something that survives the rows changing.
    pub fn selected_key(&self) -> Option<RowKey> {
        let title = |section: usize| self.sections.get(section).map(|s| s.title.clone());
        Some(match self.selected_row()? {
            Row::Header { section, .. } => RowKey::Header(title(section)?),
            Row::More { section, .. } => RowKey::More(title(section)?),
            Row::Hidden { section, .. } => RowKey::Hidden(title(section)?),
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
        match self.row_of(&key) {
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

    /// Where the row `key` names is on screen, or the `more` row hiding it.
    ///
    /// A row the cap has just hidden is still there, behind the `more` row that now
    /// stands for it, so that row is the answer rather than whatever fell into its
    /// index in the section below.
    fn row_of(&self, key: &RowKey) -> Option<usize> {
        let rows = self.visible();
        let found = rows.iter().position(|row| match (row, key) {
            (Row::Entry { entry, .. }, RowKey::Entry(path)) => entry.path == *path,
            (Row::Door { entry, .. }, RowKey::Door(path)) => entry.path == *path,
            (Row::Place { path, .. }, RowKey::Place(wanted)) => path == wanted,
            (Row::Header { section, .. }, RowKey::Header(title))
            | (Row::More { section, .. }, RowKey::More(title))
            | (Row::Hidden { section, .. }, RowKey::Hidden(title)) => self
                .sections
                .get(*section)
                .is_some_and(|s| s.title == *title),
            _ => false,
        });
        if found.is_some() {
            return found;
        }
        match key {
            RowKey::Entry(path) | RowKey::Place(path) => rows.iter().position(|row| {
                matches!(row, Row::More { section, .. }
                if self.sections.get(*section).is_some_and(|s| {
                    s.grouped_by_place
                        && s.rows.iter().any(|r| r.path == *path || place_of(&r.path) == *path)
                }))
            }),
            _ => None,
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
        let rows = self.visible().len();
        self.scroll = settle_top(self.scroll, self.selected, self.view_height, rows);
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
        self.returning = None;
        self.landing = false;
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
    /// So does a row nothing has looked into, for the same reason and more sharply: in a
    /// fresh listing that is every directory in it, and any of them may turn out to be a
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
        // Out of a remote dataset's root is back to the collection it is listed in, not
        // up into a bucket that may not be listable at all.
        if let Some((_, dataset)) = self.remote_dataset_of(path) {
            let place = &dataset.location;
            if same_place(path, place) {
                return None;
            }
            let up = self.parent_within(path)?;
            // The dataset's own place, as listed, so its listing is found again.
            return Some(if same_place(&up, place) {
                place.clone()
            } else {
                up
            });
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

    /// The remote collection dataset `path` is in: the innermost, when one dataset is
    /// inside another, and the first listed of two that are the same place.
    fn remote_dataset_of(&self, path: &Path) -> Option<(&Collection, &CollectionDataset)> {
        if !is_object_store_url(path) {
            return None;
        }
        let text = path.to_string_lossy();
        self.collections
            .iter()
            .flat_map(|c| c.datasets.iter().map(move |d| (c, d)))
            .filter(|(_, d)| {
                is_object_store_url(&d.location) && within(&text, &d.location.to_string_lossy())
            })
            .rev()
            .max_by_key(|(_, d)| d.location.to_string_lossy().trim_end_matches('/').len())
    }

    /// The collection dataset listed at `path` itself.
    pub fn collection_dataset(&self, path: &Path) -> Option<(&Collection, &CollectionDataset)> {
        self.collections
            .iter()
            .flat_map(|c| c.datasets.iter().map(move |d| (c, d)))
            .find(|(_, d)| d.location == path || same_place(&d.location, path))
    }

    /// The place of an Azure account, from whichever source lists it.
    fn azure_account_place(&self, account: &str) -> Option<PathBuf> {
        self.cloud
            .iter()
            .flat_map(|s| s.buckets.iter())
            .find(|place| cloud_account(place).is_some_and(|(_, a)| a == account))
            .cloned()
    }

    /// Where a directory in an object store is in being looked into, while its row has
    /// no label of its own yet. `None` once the row says what it holds, or when it is not
    /// such a directory.
    pub fn cloud_look(&self, entry: &Entry) -> Option<CloudLook> {
        if entry.kind != EntryKind::Directory
            || !entry.holds.is_empty()
            || !is_object_store_url(&entry.path)
            || is_cloud_place(&entry.path)
            || object_place_label(&entry.path).is_some()
        {
            return None;
        }
        if self.peeking.contains(&entry.path) {
            return Some(CloudLook::Looking);
        }
        if self.peek_failed.contains(&entry.path) {
            return Some(CloudLook::Failed);
        }
        match self.cloud_kinds.get(&entry.path) {
            None => Some(CloudLook::Waiting),
            // Answered with something this row does not show yet: the listing it was
            // drawn from is being rebuilt. `dir` in the meantime would claim no data.
            Some((kind, holds)) if *kind != EntryKind::Directory || !holds.is_empty() => {
                Some(CloudLook::Looking)
            }
            Some(_) => None,
        }
    }

    /// What to call a place a source or collection names itself: a remote dataset of a
    /// collection, or a local one that is missing.
    pub fn place_kind(&self, path: &Path) -> Option<&'static str> {
        if self.missing.contains(path) {
            return Some("missing");
        }
        if let Some((id, _)) = cloud_account(path) {
            return self
                .cloud
                .iter()
                .any(|s| s.id == id && s.api == "gcs")
                .then_some("project");
        }
        (is_object_store_url(path)
            && self
                .collection_dataset(path)
                .is_some_and(|(_, d)| is_object_store_url(&d.location)))
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

    /// Details-pane lines for a place a cloud source listed or a collection names, when
    /// it has any.
    pub fn place_details(&self, path: &Path) -> Option<&[(String, String)]> {
        self.cloud
            .iter()
            .find_map(|s| s.place_details.get(path))
            .or_else(|| self.collection_dataset(path).map(|(_, d)| &d.details))
            .map(Vec::as_slice)
    }

    /// The location as the title bar names it. Cloud places read as a trail through
    /// the source's label, since neither `cloud://<id>` nor `s3://<id>@bucket` is
    /// something to show a person.
    pub fn location_label(&self, path: &Path) -> String {
        let sep = crate::glyphs::get().trail;
        // Inside a remote dataset of a collection: the collection, the dataset's name,
        // and the way down from it.
        if let Some((collection, dataset)) = self.remote_dataset_of(path) {
            let text = path.to_string_lossy();
            let rest = within_rest(&text, &dataset.location.to_string_lossy());
            let mut parts = vec![collection.label.clone(), dataset.name.clone()];
            parts.extend(
                rest.split('/')
                    .filter(|p| !p.is_empty())
                    .map(str::to_string),
            );
            return parts.join(&format!(" {sep} "));
        }
        if let Some(source) = self.cloud_source_of(path) {
            let mut parts = vec!["cloud".to_string(), source.label.clone()];
            let text = path.to_string_lossy();
            if let Some((_, account)) = cloud_account(path) {
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
        self.found_scores = None;

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
                        let mut entry = bucket_entry(bucket);
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
        self.score_search_inline();
        let local = self.search.root.is_some()
            && (self.search.indexed > 0 || self.search.running || self.search.limited.is_some());
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
                    root: None,
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

        // The best matches as last scored. Those for an older filter, while a scoring is
        // out, are scored again here, once, so nothing that no longer matches shows
        // meanwhile and the passes over the rows need not score them each time.
        let matches = self.search.matches.as_ref();
        let kept: Vec<(&Entry, i32)> = matches
            .map(|m| {
                let fresh = m.query == self.filter;
                m.top
                    .iter()
                    .zip(m.scores.iter().copied())
                    .filter(|(e, _)| !listed.contains(&e.path))
                    .filter_map(|(e, score)| {
                        if fresh {
                            Some((e, score))
                        } else {
                            match_score(&self.filter, e).map(|s| (e, s))
                        }
                    })
                    .collect()
            })
            .unwrap_or_default();
        let found_scores = Some((
            self.filter.clone(),
            kept.iter().map(|&(_, s)| s).collect::<Vec<i32>>(),
        ));
        let mut rows: Vec<Entry> = kept.into_iter().map(|(e, _)| e.clone()).collect();
        rows.extend(cloud_rows);

        // An empty result is said when it is not the whole answer: a walk that stopped
        // short may have missed the file, and "no match" there is something people act on.
        let partial = self.search.limited.is_some();
        let scored = self.search.scored_for(&self.filter);
        if rows.is_empty() && !self.search.running && !partial && scored {
            return;
        }

        let subtitle = self.found_subtitle(rows.is_empty());

        self.found_scores = found_scores;
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
            root: None,
        });
    }

    /// What `Found`'s rule says: where the search looked, how many matched, and how far
    /// the walk got.
    fn found_subtitle(&self, empty: bool) -> String {
        let root = self.search.root.clone().unwrap_or_default();
        let dot = crate::glyphs::get().middot;
        let files = crate::numfmt::group_chrome(self.search.indexed);
        let files = if self.search.indexed == 1 {
            format!("{files} file")
        } else {
            format!("{files} files")
        };
        let subtitle = display_path(&root);
        // How many matched, when more matched than are listed. Last, since the rule
        // cuts a long note from the start and the place is what it can best spare.
        let counted = self
            .search
            .matches
            .as_ref()
            .filter(|m| m.query == self.filter && m.ids.len() > m.top.len())
            .map(|m| {
                format!(
                    " {dot} {} of {} matches",
                    crate::numfmt::group_chrome(m.top.len()),
                    crate::numfmt::group_chrome(m.ids.len())
                )
            })
            .unwrap_or_default();
        if self.search.running {
            format!(
                "{subtitle} {dot} searching {}{counted}",
                crate::numfmt::group_chrome(self.search.scanned)
            )
        } else if !self.search.scored_for(&self.filter) {
            format!("{subtitle} {dot} matching {files}")
        } else if empty {
            match &self.search.limited {
                Some(limit) => format!("{subtitle} {dot} no match in {files} {dot} {limit}"),
                None => format!("{subtitle} {dot} no match in {files}"),
            }
        } else {
            let searched = crate::numfmt::group_chrome(self.search.scanned);
            match &self.search.limited {
                Some(limit) => {
                    format!("{subtitle} {dot} {limit} {dot} {searched} searched{counted}")
                }
                None => format!("{subtitle} {dot} {searched} searched{counted}"),
            }
        }
    }

    /// Fold a batch of search results in, keeping the list free of duplicates.
    pub fn search_batch(&mut self, root: &Path, mut found: Vec<Entry>, scanned: usize) {
        // A batch from a walk the user has already moved on from is dropped: the
        // walk is abandoned rather than cancelled, so late results are normal.
        if self.search.root.as_deref() != Some(root) {
            return;
        }
        // What earlier runs measured, so a found row carries its shape and column
        // names like a listed one — the filter matches on column names, and without
        // this the promise that "customer_id finds every dataset with that column"
        // stopped at the rows already on screen. The strict (non-remote) fingerprint
        // gates it: same size and mtime, or nothing is said.
        for row in &mut found {
            apply_known_facts(row, &self.known, false);
        }
        self.search.scanned = scanned;
        let start = self.search.indexed;
        let batch: std::sync::Arc<[Entry]> = found.into();
        if !batch.is_empty() {
            self.search.indexed += batch.len();
            self.search.results.push(batch.clone());
        }
        // Matches for the filter typed are carried forward over the new files alone. A
        // whole scoring per batch, over hundreds of batches, is what kept typed keys
        // waiting while a large walk ran.
        let limit = self.search_limit;
        let changed = match self.search.matches.as_mut() {
            Some(m) if m.query == self.filter && m.upto == start => m.extend(&batch, start, limit),
            _ => {
                let before = self.search.matches.as_ref().map(|m| m.upto);
                self.score_search_inline();
                self.search.matches.as_ref().map(|m| m.upto) != before
            }
        };
        // `Found` is rebuilt only when what it lists changed; otherwise only the progress
        // on its rule moves.
        let found_listed = self
            .sections
            .iter()
            .position(|s| s.title == Self::SEARCH_SECTION);
        match found_listed {
            Some(at) if !changed => {
                let empty = self.sections[at].rows.is_empty();
                self.sections[at].subtitle = Some(self.found_subtitle(empty));
            }
            _ => self.sync_search_section(),
        }
        self.settle_return();
    }

    /// Score the filter against the files found, here and now, when that is cheap.
    fn score_search_inline(&mut self) {
        if self.filter.is_empty() || self.search.scored_for(&self.filter) {
            return;
        }
        let (base, looks_at) = self.search.base_for(&self.filter);
        if looks_at > SCORE_INLINE_MAX {
            return;
        }
        let scored =
            crate::search::score(&self.search.results, &self.filter, base, self.search_limit);
        self.search.matches = Some(scored);
    }

    /// The scoring a worker should do next, if one is owed: the filter has changed, or
    /// files arrived since the last. One at a time; the next is asked for when it answers.
    pub fn score_job(&mut self) -> Option<ScoreJob> {
        if self.filter.is_empty() || self.search.scoring || self.search.scored_for(&self.filter) {
            return None;
        }
        let base = self.search.base_for(&self.filter).0.cloned();
        self.search.scoring = true;
        Some(ScoreJob {
            epoch: self.search.epoch,
            results: self.search.results.clone(),
            query: self.filter.clone(),
            base,
            limit: self.search_limit,
        })
    }

    /// A worker's scoring is in.
    pub fn search_scored(&mut self, epoch: u64, scored: crate::search::Matches) {
        if epoch != self.search.epoch {
            return;
        }
        self.search.scoring = false;
        // Batches may have carried the same filter's matches further meanwhile.
        if self
            .search
            .matches
            .as_ref()
            .is_some_and(|m| m.query == scored.query && m.upto >= scored.upto)
        {
            return;
        }
        self.search.matches = Some(scored);
        self.sync_search_section();
        // Typing put the cursor on the first row there was; if that was nothing, the
        // first match is where it belongs now.
        if !matches!(self.selected_row(), Some(Row::Entry { .. })) {
            self.select_first_entry();
        }
        self.settle_return();
    }

    /// Record that the walk under `root` has finished.
    pub fn search_finished(&mut self, root: &Path, scanned: usize, limited: Option<String>) {
        if self.search.root.as_deref() != Some(root) {
            return;
        }
        self.search.running = false;
        self.search.done = true;
        // Never fewer than the batches reported: a walk that died says nothing of its own.
        self.search.scanned = self.search.scanned.max(scanned);
        self.search.limited = limited;
        self.sync_search_section();
        self.settle_return();
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
            let scored = self
                .found_scores
                .as_ref()
                .filter(|(query, _)| section.title == Self::SEARCH_SECTION && *query == self.filter)
                .map(|(_, scores)| scores.as_slice())
                .unwrap_or_default();
            let mut matched: Vec<(&Entry, i32)> = section
                .rows
                .iter()
                .enumerate()
                .filter(|(_, row)| !(self.hide_unreadable && row.hidden_by_default()))
                .filter_map(|(i, row)| match scored.get(i) {
                    Some(&score) => Some((row, score)),
                    None => match_score(&self.filter, row).map(|s| (row, s)),
                })
                .collect();

            // A section with nothing to show is dropped, unless it is standing in for
            // a root the user named or is currently in, where its absence would be
            // more confusing than an empty heading, or its rows are still on the way.
            //
            // A door is something to show. A directory of part files written with no
            // extension lists nothing — no name in it says data — and the door is the
            // only way to read it; dropped here, the whole section went with it and the
            // directory was a dead end that the open could have read.
            let keep_empty = section.unavailable || section.waiting || section.origin.is_some();
            let has_door = section.door.is_some() && self.filter.is_empty();
            // Only inside a directory, where the listing is the whole screen and an
            // empty one needs saying why. The root listing's sections leave them out
            // quietly, as they always have.
            let hidden =
                if self.browsing.is_some() && self.hide_unreadable && self.filter.is_empty() {
                    section
                        .rows
                        .iter()
                        .filter(|row| row.hidden_by_default())
                        .count()
                } else {
                    0
                };
            // `Found` is only put in empty when it has something to say: a walk that
            // stopped short and matched nothing.
            let says_why = section.title == Self::SEARCH_SECTION;
            if matched.is_empty()
                && !has_door
                && hidden == 0
                && !says_why
                && !(keep_empty && self.filter.is_empty())
            {
                continue;
            }

            // Within a section, rank by match quality; without a filter every score is
            // equal and the curated order is preserved. Ties go to the shorter name,
            // which is fzf's default tiebreak and the reason `sales` prefers
            // `sales.csv` over `sales_by_region_and_quarter.csv`.
            //
            // An often-opened row is lifted by its frecency, up to a few characters'
            // worth of match: of two files `sales` finds, the one opened most comes
            // first (#547 M9).
            if !self.filter.is_empty() {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or_default();
                let lifted = |entry: &Entry, score: i32| {
                    let frecency = self
                        .visits
                        .get(&entry.path)
                        .map_or(0.0, |v| v.frecency(now));
                    score.saturating_add((frecency.min(10.0) * FRECENCY_LIFT) as i32)
                };
                matched.sort_by(|(a, sa), (b, sb)| {
                    lifted(b, *sb)
                        .cmp(&lifted(a, *sa))
                        .then_with(|| a.name.len().cmp(&b.name.len()))
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
                // What the section holds, which the door is not: it is a way to open the
                // directory those rows are in, so counting it would make a directory of
                // three files say four. It is not among `rows`, so nothing here has to
                // take it back out.
                matches: matched.len(),
                collapsed,
            });
            if collapsed {
                continue;
            }
            // First inside the directory, before the rows and whatever the sort, because
            // being the first row inside a directory is the whole of what it is.
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
            if hidden > 0 {
                out.push(Row::Hidden {
                    section: si,
                    count: hidden,
                });
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
    /// is a `[home] recent_rows` setting, not a different fraction. A filter shows
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

    /// The names the `~` prompt offers: those in the directory being typed that its
    /// last segment matches, best first. Hidden names only when a dot is typed.
    pub fn path_candidates(&self) -> Vec<&PathName> {
        let Some(listing) = self
            .path_listing
            .as_ref()
            .filter(|l| l.dir == typed_dir(&self.path_input))
        else {
            return Vec::new();
        };
        let segment = &self.path_input[listing.dir.len()..];
        let mut matched: Vec<(&PathName, i32)> = listing
            .names
            .iter()
            .filter(|n| !n.name.starts_with('.') || segment.starts_with('.'))
            .filter_map(|n| {
                if segment.is_empty() {
                    return Some((n, 0));
                }
                // A name that starts with what is typed first, as a shell completes;
                // then the rest the fuzzy match finds.
                let prefix = n.name.starts_with(segment) as i32 * 1_000_000;
                fuzzy_score(segment, &n.name).map(|score| (n, prefix + score))
            })
            .collect();
        matched.sort_by(|(a, sa), (b, sb)| sb.cmp(sa).then_with(|| a.name.cmp(&b.name)));
        matched.into_iter().map(|(n, _)| n).collect()
    }

    /// The path the picked candidate names, with its separator when it is a directory.
    pub fn picked_path(&self) -> Option<String> {
        let pick = self.path_pick?;
        let name = *self.path_candidates().get(pick)?;
        let dir = typed_dir(&self.path_input);
        let mut path = format!("{dir}{}", name.name);
        if name.dir {
            path.push(separator_in(dir));
        }
        Some(path)
    }

    /// What Tab makes of the typed path: the one candidate whole, or the longest
    /// start every candidate shares. `None` when it would add nothing.
    pub fn path_completion(&self) -> Option<String> {
        let dir = typed_dir(&self.path_input);
        let segment = &self.path_input[dir.len()..];
        let candidates = self.path_candidates();
        match candidates.as_slice() {
            [] => None,
            [one] => {
                let mut path = format!("{dir}{}", one.name);
                if one.dir {
                    path.push(separator_in(dir));
                }
                Some(path)
            }
            many => {
                let starting: Vec<&str> = many
                    .iter()
                    .map(|n| n.name.as_str())
                    .filter(|n| n.starts_with(segment))
                    .collect();
                let first = starting.first()?;
                let shared = starting
                    .iter()
                    .skip(1)
                    .fold(first.to_string(), |acc, n| common_prefix(&acc, n));
                (shared.len() > segment.len()).then(|| format!("{dir}{shared}"))
            }
        }
    }

    /// Every URL the screen already knows: collections, buckets, what has been listed
    /// and what the index remembers. What `s3://` completes from.
    pub fn known_urls(&self) -> Vec<String> {
        let mut urls: Vec<String> = Vec::new();
        let mut add = |path: &Path| {
            let text = path.to_string_lossy();
            if text.contains("://") && !is_cloud_place(path) {
                urls.push(text.into_owned());
            }
        };
        for collection in &self.collections {
            for dataset in &collection.datasets {
                add(&dataset.location);
            }
        }
        for source in &self.cloud {
            for bucket in &source.buckets {
                add(bucket);
            }
        }
        for (root, rows) in &self.probed {
            add(root);
            for row in rows {
                add(&row.path);
            }
        }
        for path in self.known.keys() {
            add(path);
        }
        for section in &self.sections {
            for row in &section.rows {
                add(&row.path);
            }
        }
        urls
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

    /// Whether the cursor is on the door rather than on something in the directory.
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
        self.listing_so_far.remove(&root);
        self.cut_short.remove(&root);
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

    /// The cloud directories on or near the screen that nothing has peeked into, at most
    /// `limit`, the highlighted row first.
    ///
    /// [`Self::unclassified_visible`]'s cloud twin, and deliberately the same shape: a
    /// peek is a request, and the rows worth spending one on are the rows somebody is
    /// looking at. It used to take the first forty-eight directories of each listing,
    /// once per session — so a bucket of two hundred prefixes had its first forty-eight
    /// labelled and the rest reading `dir` for good, however long you spent on them,
    /// while paging straight past the first forty-eight spent forty-eight requests on
    /// rows nobody saw. The cap and its never-again claim both go: the bound is what the
    /// cursor rests on.
    pub fn cloud_directories_to_peek(&self, limit: usize) -> Vec<PathBuf> {
        if limit == 0 {
            return Vec::new();
        }
        let rows = self.visible();
        let height = if self.view_height == 0 {
            limit
        } else {
            self.view_height
        };
        let top = self.scroll.min(rows.len());
        let ahead = top.saturating_add(2 * height).min(rows.len());
        let behind = top.saturating_sub(height);

        let mut out: Vec<PathBuf> = Vec::new();
        let order = std::iter::once(self.selected)
            .chain(top..ahead)
            .chain(behind..top);
        for row in order.filter_map(|i| rows.get(i)) {
            let Row::Entry { entry, .. } = row else {
                continue;
            };
            // A directory in an object store, by its URL: `read_dir` on an `s3://` path
            // asks the working directory about a file called `s3:` and truthfully finds
            // nothing, which is why these have a pass of their own.
            if !is_object_store_url(&entry.path) || is_cloud_place(&entry.path) {
                continue;
            }
            if !matches!(entry.kind, EntryKind::Directory | EntryKind::Unknown) {
                continue;
            }
            // A collection's dataset is listed by name at the top, and nothing is asked
            // of its store until it is opened or entered.
            if self.browsing.is_none() && self.collection_dataset(&entry.path).is_some() {
                continue;
            }
            // Asked and answered, or asked and still out.
            if self.cloud_kinds.contains_key(&entry.path)
                || self.peeking.contains(&entry.path)
                || self.peek_failed.contains(&entry.path)
            {
                continue;
            }
            if out.contains(&entry.path) {
                continue;
            }
            out.push(entry.path.clone());
            if out.len() >= limit {
                break;
            }
        }
        out
    }

    /// Record that a probe could not read the root.
    pub fn probe_failed(&mut self, root: PathBuf) {
        self.probed.remove(&root);
        self.listing_so_far.remove(&root);
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
            if matches!(
                entry.kind,
                EntryKind::Directory | EntryKind::Unknown | EntryKind::Other
            ) || entry.kind.is_lake_table()
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
    /// inside a directory that holds one dataset and folds the section otherwise, and the
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
            if entry.kind != EntryKind::Unknown || self.missing.contains(&entry.path) {
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
            // [`HomeState::cloud_directories_to_peek`], which is that path.
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
            // The door as well, whose path is the directory's: it *reads* that slot on
            // purpose, so stepping into a directory the listing above already measured
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
            // The door's name says what it opens, and a measurement can change that: the
            // footers turn a directory of files down as one table, or name its keys.
            if let Some(door) = section.door.as_mut() {
                door.name = door_name(door, &section.rows);
            }
        }
        // Landed on a door the footers have since turned down, and not moved: the cursor
        // goes where it would have landed had they been read first.
        if self.landing
            && let Some(Row::Door { entry, .. }) = self.visible().get(self.selected)
            && !door_lands(entry)
        {
            self.selected = self.landing_row();
            self.follow_selection();
        }
    }

    /// Put the cursor on the first dataset rather than the first header, so the
    /// preview pane has something to show without a keypress.
    ///
    /// Inside a directory that is the door when the door opens the directory as one
    /// dataset (see [`door_lands`]), and the first thing in it otherwise.
    pub fn select_first_entry(&mut self) {
        self.returning = None;
        self.landing = true;
        self.selected = self.landing_row();
    }

    /// Where [`HomeState::select_first_entry`] puts the cursor.
    fn landing_row(&self) -> usize {
        let rows = self.visible();
        // Recent is ranked by frecency, and the last file opened is still one Enter away.
        if self.filter.is_empty()
            && let Some(newest) = self.newest_recent.as_ref()
            && let Some(at) = rows.iter().position(|r| {
                matches!(r, Row::Entry { section, entry, .. }
                    if entry.path == *newest
                        && self.sections[*section].title == Self::RECENT_SECTION)
            })
        {
            return at;
        }
        let first = rows
            .iter()
            .position(|r| matches!(r, Row::Entry { .. } | Row::Door { .. }));
        let first = match first.and_then(|i| rows.get(i)) {
            Some(Row::Door { entry, .. }) if !door_lands(entry) => rows
                .iter()
                .position(|r| matches!(r, Row::Entry { .. } | Row::Hidden { .. }))
                .or(first),
            _ => first,
        };
        // A directory of files datui cannot open: the row that says so.
        first
            .or_else(|| rows.iter().position(|r| matches!(r, Row::Hidden { .. })))
            .unwrap_or(0)
    }

    pub fn clamp_selection(&mut self) {
        let n = self.visible().len();
        if n == 0 {
            self.selected = 0;
        } else if self.selected >= n {
            self.selected = n - 1;
        }
    }

    /// Put the selection on row `index` of what is listed, as a click does.
    pub fn select(&mut self, index: usize) {
        if index < self.visible().len() {
            self.returning = None;
            self.landing = false;
            self.selected = index;
        }
    }

    pub fn move_selection(&mut self, delta: isize) {
        self.returning = None;
        self.landing = false;
        let n = self.visible().len();
        if n == 0 {
            return;
        }
        let cur = self.selected as isize;
        let next = (cur + delta).rem_euclid(n as isize);
        self.selected = next as usize;
    }

    /// Move the selection `delta` rows, stopping at the first and last rather than
    /// wrapping. A step of one wraps, which is a quick way round; a jump of ten that
    /// wrapped landed somewhere near the top with nothing to say it had gone round.
    pub fn page_selection(&mut self, delta: isize) {
        self.returning = None;
        self.landing = false;
        let n = self.visible().len();
        if n == 0 {
            return;
        }
        // Saturating, so Home and End are a page of `isize::MIN` or `isize::MAX`.
        let next = (self.selected as isize)
            .saturating_add(delta)
            .clamp(0, n as isize - 1);
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
        opens_whole_directory: false,
        format_spec: None,
        table: None,
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

/// Whether a remote path's name says it is a file: a data extension, or any dot in its
/// last segment. A trailing slash is a prefix whatever the name says.
pub fn names_a_file(path: &Path) -> bool {
    let named = path.to_string_lossy();
    // `file_name` rather than a split on `/`, which on Windows took the whole path as
    // its last segment and called `C:\Users\RUNNER~1\…\.tmp\orders` a file.
    let dotted = !named.ends_with('/')
        && path
            .file_name()
            .map(|last| last.to_string_lossy())
            .is_some_and(|last| last.trim_start_matches('.').contains('.'));
    discover::is_data_file(path) || dotted
}

/// Build an entry for a path that is already known (a recent), classifying it.
fn entry_for_path(path: &Path, remote: bool) -> Entry {
    // A table inside a SQLite database, which nothing on disk is named.
    if !remote && let Some(table) = discover::table_row(path) {
        return table;
    }
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
        if names_a_file(path) {
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
        opens_whole_directory: false,
        format_spec: None,
        table: None,
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
        // The platform's separator, so Windows reads `~\data\a.csv` rather than a
        // mix of the two.
        return format!("~{}{}", std::path::MAIN_SEPARATOR, rest.display());
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
    // `\` is a separator on Windows too, and what a Windows user types: `C:\data\`
    // completed the name `data` in `C:\` instead of listing inside it.
    let is_separator = |c: char| c == '/' || (cfg!(windows) && c == '\\');
    let typed_ends_in_sep = typed.ends_with(is_separator);

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

    // A single directory gets its separator, so the next Tab descends into it: the one
    // already being typed, so `C:\Users\` does not become `C:\Users/`.
    if names.len() == 1 && dir.join(&shared).is_dir() && !completed.ends_with(is_separator) {
        let separator = typed
            .chars()
            .rev()
            .find(|c| is_separator(*c))
            .unwrap_or(std::path::MAIN_SEPARATOR);
        completed.push(separator);
    }
    (completed, names.len())
}

/// One name in the directory the `~` prompt is typing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathName {
    pub name: String,
    /// A directory, prefix or bucket: completed with a separator, and gone into.
    pub dir: bool,
}

/// What the `~` prompt lists: the directory part of what is typed, and what is in it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PathListing {
    /// The typed text up to and including its last separator, as typed.
    pub dir: String,
    pub names: Vec<PathName>,
    /// Reading the directory failed: there is nothing to list, and the prompt says so.
    pub failed: bool,
}

/// Names a typed directory lists at most. A prompt is for finding one name, and a
/// directory of a hundred thousand is typed into, not scrolled.
const PATH_LISTING_MAX: usize = 5_000;

/// The directory part of a typed path: everything up to and including its last
/// separator. For a URL, at least its scheme (`s3://`), so the buckets are what is
/// listed under it.
pub fn typed_dir(typed: &str) -> &str {
    let is_separator = |c: char| c == '/' || (cfg!(windows) && c == '\\');
    let floor = typed.find("://").map_or(0, |at| at + 3);
    match typed[floor..].rfind(is_separator) {
        Some(at) => &typed[..floor + at + 1],
        None => &typed[..floor],
    }
}

/// The separator a directory completed under `dir` ends with: a URL's `/`, or the one
/// the user has been typing, so `C:\Users\` does not become `C:\Users/`.
fn separator_in(dir: &str) -> char {
    if typed_dir_is_url(dir) {
        return '/';
    }
    dir.chars()
        .rev()
        .find(|c| *c == '/' || (cfg!(windows) && *c == '\\'))
        .unwrap_or(std::path::MAIN_SEPARATOR)
}

/// Whether a typed directory is a URL, listed from what datui already knows rather
/// than read.
pub fn typed_dir_is_url(dir: &str) -> bool {
    dir.contains("://")
}

/// What a local directory typed at `~` holds, for the prompt's list. Reads the
/// directory, so it runs on a worker. Nothing typed lists the working directory.
pub fn list_typed_dir(dir: &str) -> PathListing {
    let path = if dir.is_empty() {
        PathBuf::from(".")
    } else {
        expand_user_path(dir)
    };
    let Ok(entries) = std::fs::read_dir(&path) else {
        return PathListing {
            dir: dir.to_string(),
            names: Vec::new(),
            failed: true,
        };
    };
    let mut names: Vec<PathName> = entries
        .flatten()
        .take(PATH_LISTING_MAX)
        .map(|e| {
            let name = e.file_name().to_string_lossy().into_owned();
            // A link to a directory is one to go into.
            let dir = e.file_type().is_ok_and(|t| t.is_dir())
                || (e.file_type().is_ok_and(|t| t.is_symlink()) && e.path().is_dir());
            PathName { name, dir }
        })
        .collect();
    names.sort_by(|a, b| a.name.cmp(&b.name));
    PathListing {
        dir: dir.to_string(),
        names,
        failed: false,
    }
}

/// The names one level below `dir` among `urls`: how `s3://`, `gs://` and `az://`
/// complete, from buckets, prefixes and datasets datui has already listed, opened or
/// been given by a collection. Nothing is asked of the store.
pub fn names_under(dir: &str, urls: impl IntoIterator<Item = String>) -> PathListing {
    let mut names: Vec<PathName> = Vec::new();
    for url in urls {
        // An Azure URL is known in its full form; `az://container/` is how one is typed.
        let forms = match crate::source::azure_parts(&url) {
            Some((_, container, key)) => vec![url.clone(), format!("az://{container}/{key}")],
            None => vec![url],
        };
        for form in forms {
            let Some(rest) = form.strip_prefix(dir) else {
                continue;
            };
            let (name, more) = match rest.split_once('/') {
                Some((name, more)) => (name, Some(more)),
                None => (rest, None),
            };
            if name.is_empty() {
                continue;
            }
            // Something below it, or a trailing slash: a bucket or a prefix. A last
            // segment with no extension is taken for one too, as a recent is.
            let is_dir = more.is_some() || !names_a_file(Path::new(&form));
            match names.iter_mut().find(|n| n.name == name) {
                Some(known) => known.dir |= is_dir,
                None => names.push(PathName {
                    name: name.to_string(),
                    dir: is_dir,
                }),
            }
        }
    }
    names.sort_by(|a, b| a.name.cmp(&b.name));
    PathListing {
        dir: dir.to_string(),
        names,
        failed: false,
    }
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

    /// A path under the home directory is written the way it is typed back: `~\` on
    /// Windows, and `~\` typed at the prompt expands.
    #[cfg(windows)]
    #[test]
    fn a_windows_home_path_is_shown_and_typed_with_backslashes() {
        let home = dirs::home_dir().unwrap();
        let path = home.join("data").join("a.csv");
        let shown = display_path(&path);
        assert_eq!(shown, r"~\data\a.csv");
        assert_eq!(expand_user_path(&shown), path);
    }

    /// The last segment of a Windows path is after its last `\`, so a dot higher up
    /// does not make a directory a file.
    #[cfg(windows)]
    #[test]
    fn a_dot_above_a_windows_recent_does_not_make_it_a_file() {
        let path = Path::new(r"C:\Users\RUNNER~1\AppData\Local\Temp\.tmpAzMMTE\orders");
        assert_eq!(entry_for_path(path, true).kind, EntryKind::Unknown);
        let file = Path::new(r"C:\Users\RUNNER~1\AppData\Local\Temp\.tmpAzMMTE\a.parquet");
        assert_eq!(entry_for_path(file, true).kind, EntryKind::File);
    }

    fn counted(n: usize) -> crate::discover::Holds {
        crate::discover::Holds {
            formats: vec![("parquet".to_string(), n)],
            ..Default::default()
        }
    }

    /// The claim `peek_cloud_directories` stakes before its answers arrive, so a rebuild
    /// in the meantime does not ask the store again: a `Directory` that counted nothing.
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
    fn a_cloud_directory_waits_then_looks_then_answers() {
        let path = std::path::PathBuf::from("gs://pitscope/seasons");
        let mut row = Entry::for_test(&path, "seasons");
        row.kind = EntryKind::Directory;
        let mut home = HomeState::default();

        assert_eq!(
            home.cloud_look(&row),
            Some(CloudLook::Waiting),
            "not asked yet"
        );
        home.peeking.insert(path.clone());
        assert_eq!(
            home.cloud_look(&row),
            Some(CloudLook::Looking),
            "being looked into"
        );
        home.peeking.remove(&path);

        // Answered with a count the drawn row does not carry yet: still looking, not
        // `dir`, which would claim there is no data inside.
        home.cloud_kinds
            .insert(path.clone(), (EntryKind::Directory, counted(12)));
        assert_eq!(home.cloud_look(&row), Some(CloudLook::Looking));
        home.cloud_kinds
            .insert(path.clone(), (EntryKind::Hive, Default::default()));
        assert_eq!(home.cloud_look(&row), Some(CloudLook::Looking));

        // Answered with nothing to count: `dir` is the truth.
        home.cloud_kinds.insert(path.clone(), in_flight());
        assert_eq!(home.cloud_look(&row), None, "answered");

        // A failed peek is not an answer, and is not asked again until Ctrl+R. The
        // picker reads the listing, so the row has to be on it.
        let mut home = HomeState {
            network_check: |_| true,
            ..Default::default()
        };
        let root = std::path::PathBuf::from("gs://pitscope");
        home.probe_ready(root.clone(), vec![row.clone()]);
        home.browsing = Some(root);
        home.rebuild(&[], &[]);
        assert_eq!(
            home.cloud_directories_to_peek(4),
            std::slice::from_ref(&path)
        );
        home.peek_failed.insert(path.clone());
        assert_eq!(home.cloud_look(&row), Some(CloudLook::Failed));
        assert!(home.cloud_directories_to_peek(4).is_empty());

        // A row that already says what it holds, a bucket, and a local directory never
        // wait on a peek.
        let mut counted_row = Entry::for_test(&path.join("x"), "x");
        counted_row.kind = EntryKind::Directory;
        counted_row.holds = counted(3);
        assert_eq!(home.cloud_look(&counted_row), None);
        let mut bucket = Entry::for_test(std::path::Path::new("gs://pitscope"), "pitscope");
        bucket.kind = EntryKind::Directory;
        assert_eq!(home.cloud_look(&bucket), None);
        let mut local = Entry::for_test(std::path::Path::new("/data/seasons"), "seasons");
        local.kind = EntryKind::Directory;
        assert_eq!(home.cloud_look(&local), None);
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

    /// A peek is a request, so it is spent on the row the cursor is on.
    ///
    /// The picker took the first forty-eight directories of each listing, once per
    /// session: a bucket of two hundred prefixes had forty-eight labelled and the rest
    /// reading `dir` however long you spent on them, and paging straight past those
    /// forty-eight spent every request on rows nobody saw.
    #[test]
    fn a_peek_goes_to_the_row_the_cursor_is_on_and_is_never_asked_twice() {
        let root = std::path::PathBuf::from("s3://bucket/warehouse");
        let mut home = HomeState {
            network_check: |_| true,
            ..Default::default()
        };
        let rows: Vec<Entry> = ["a", "b", "c", "d", "e"]
            .iter()
            .map(|n| {
                let mut row = Entry::for_test(&root.join(n), n);
                row.kind = EntryKind::Directory;
                row
            })
            .collect();
        home.probe_ready(root.clone(), rows);
        home.browsing = Some(root.clone());
        home.rebuild(&[], &[]);
        // One already answered, and one with a request already out.
        home.cloud_kinds
            .insert(root.join("b"), (EntryKind::MultiFile, counted(3)));
        home.peeking.insert(root.join("c"));

        // The cursor on `e`, the last row: it is asked about first, though four rows
        // above it have never been looked into. That is the whole change.
        home.selected = home
            .visible()
            .iter()
            .position(|r| matches!(r, Row::Entry { entry, .. } if entry.name == "e"))
            .expect("the row is listed");
        let asked = home.cloud_directories_to_peek(3);
        assert_eq!(
            asked.first(),
            Some(&root.join("e")),
            "the highlighted row is the one about to be acted on"
        );
        assert_eq!(asked.len(), 3, "the budget is a budget");
        assert!(
            !asked.contains(&root.join("b")),
            "a directory already looked into is not asked again"
        );
        assert!(
            !asked.contains(&root.join("c")),
            "nor one with a request already out"
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
            root: None,
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
mod look_into_batch_tests {
    use super::*;
    use polars::prelude::*;

    /// Every row in a batch is labeled before any is measured: a kind is one directory
    /// read, and a count can be sixty-four footers.
    #[test]
    fn every_kind_is_sent_before_any_count() {
        let dir = tempfile::tempdir().unwrap();
        let cache_dir = tempfile::tempdir().unwrap();
        let cache = crate::cache::CacheManager::with_dir(cache_dir.path().to_path_buf());
        let mut rows = Vec::new();
        for name in ["a", "b"] {
            let partition = dir.path().join(name).join("year=2024");
            std::fs::create_dir_all(&partition).unwrap();
            let mut frame = df!("x" => [1i32, 2, 3]).unwrap();
            let file = std::fs::File::create(partition.join("part.parquet")).unwrap();
            ParquetWriter::new(file).finish(&mut frame).unwrap();
            rows.push(Entry::new(dir.path().join(name), EntryKind::Unknown));
        }

        let mut sent = Vec::new();
        look_into_batch(rows, &cache, |path, m| {
            let name = path.file_name().unwrap().to_string_lossy().into_owned();
            sent.push((name, m.kind, m.rows));
        });

        let hive = Some(EntryKind::Hive);
        assert_eq!(
            sent,
            vec![
                ("a".to_string(), hive, None),
                ("b".to_string(), hive, None),
                ("a".to_string(), hive, Some(3)),
                ("b".to_string(), hive, Some(3)),
            ]
        );
    }
}

#[cfg(test)]
mod known_facts_tests {
    use super::*;
    use crate::cache::DatasetFacts;

    /// What a directory holds comes back with its kind, on both routes.
    ///
    /// A row given a kind from the cache is never looked into again — `look_into` only
    /// classifies an `Unknown`, and `unclassified_visible` skips anything else. So a
    /// count left behind is left behind for the session: the row says `dir` about a
    /// directory of fifteen Parquet files, and `enrich` goes on to describe it by
    /// whatever is in its subdirectories.
    #[test]
    fn what_a_directory_holds_is_restored_beside_its_kind() {
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
                // A row count a directory's record has no business carrying, to prove
                // the gate below still turns it away.
                rows: Some(999),
                cols: Some(72),
                cols_sampled: false,
                columns: vec!["lat".to_string()],
                // A directory of separate tables: the kind the footers settled on, its
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
    /// or a `multi` directory ever being measured again — and its size and its codec,
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

#[cfg(test)]
mod build_feature_tests {
    use super::*;

    /// The built-in catalog lists only what this build can open; with neither `cloud`
    /// nor `http` the section is gone rather than a list of failures.
    #[test]
    fn the_builtin_catalog_lists_only_what_this_build_opens() {
        let urls: Vec<String> = collections(&crate::config::AppConfig::default())
            .into_iter()
            .filter(|c| c.builtin)
            .flat_map(|c| c.datasets)
            .map(|d| d.location.to_string_lossy().into_owned())
            .collect();
        let web = urls.iter().filter(|u| u.starts_with("https://")).count();
        let stores = urls
            .iter()
            .filter(|u| is_object_store_url(Path::new(u)))
            .count();
        assert_eq!(web + stores, urls.len(), "{urls:?}");
        assert_eq!(web > 0, cfg!(feature = "http"), "{urls:?}");
        assert_eq!(stores > 0, cfg!(feature = "cloud"), "{urls:?}");
    }

    /// A configured collection stays whole whatever the build: the user named it, and
    /// opening a dataset it cannot read says why.
    #[test]
    fn a_configured_collection_is_shown_whole() {
        let mut config = crate::config::AppConfig::default();
        let mine: crate::config::SourceConfig = toml::from_str(
            r#"
            name = "mine"
            label = "Mine"
            [[datasets]]
            name = "Bucket"
            url = "s3://bucket/prefix/"
            [[datasets]]
            name = "Web"
            url = "https://example.com/data.csv"
            "#,
        )
        .unwrap();
        config.sources = vec![mine];
        let shown = collections(&config);
        let mine = shown.iter().find(|c| c.name == "mine").unwrap();
        assert_eq!(mine.datasets.len(), 2);
    }
}

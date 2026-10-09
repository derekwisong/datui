//! The home screen: pick a dataset without first knowing where it is.
//!
//! It lists *roots* (places to look) and *catalogs* (named datasets and
//! directories):
//!
//! 1. **The working directory.**
//! 2. **Catalogs**: `catalog.toml` (Ctrl+D adds to it), the files `catalogs` lists, and
//!    the bundled `public` catalog. A directory in one is a row to step into.
//!
//! `RECENT` lists every dataset opened, grouped under the directory or prefix it
//! lives in; `Enter` on such a place row browses it. It is derived state, cheap to
//! rebuild or discard.

pub mod catalog;
pub mod codebook;
pub mod discover;
pub mod fuzzy;
pub(crate) mod home_app;
pub(crate) mod home_keys;
pub mod home_preview;
pub mod locality;
pub mod search;

use crate::home::discover::{Entry, EntryKind};
use std::path::{Path, PathBuf};

/// Where a root came from, shown subtly in the UI.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RootOrigin {
    Cwd,
    /// Derived from the desktop's own recently-used list.
    Desktop,
}

impl RootOrigin {
    pub fn note(self) -> &'static str {
        match self {
            RootOrigin::Cwd => "current directory",
            RootOrigin::Desktop => "opened elsewhere",
        }
    }
}

/// Directories holding data files the desktop recorded you opening
/// (`recently-used.xbel`), so a fresh install has places to suggest. Only the
/// directories are used, never the files: the list holds whatever was opened
/// anywhere, which may be private.
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

/// Extract directories of data files from XBEL content. Scans for
/// `href="file://…"` rather than parsing XML, to avoid an XML dependency.
pub fn dirs_from_xbel(contents: &str) -> Vec<PathBuf> {
    const PREFIX: &str = "href=\"file://";
    let mut dirs: Vec<PathBuf> = Vec::new();

    for chunk in contents.split(PREFIX).skip(1) {
        let Some(end) = chunk.find('"') else { continue };
        let decoded = percent_decode(&chunk[..end]);
        let file = PathBuf::from(decoded);
        // Only files datui can open that still exist.
        if !crate::home::discover::is_data_file(&file) || !file.is_file() {
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

/// The service's word for the top of an object-store place: a source, account,
/// bucket or container. `None` otherwise, including directories inside a bucket,
/// which are labeled by their contents like local ones.
pub fn object_place_label(path: &Path) -> Option<&'static str> {
    if cloud_source_id(path).is_some() {
        return Some("source");
    }
    if cloud_account(path).is_some() {
        return Some("account");
    }
    let text = path.to_string_lossy();
    if let Some((_, _, key)) = crate::cloud::source::azure_parts(&text) {
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

/// What a home row is called, decided once for the list and the pane beside it.
#[derive(Debug, Default, PartialEq)]
pub struct RowLabel {
    /// The list's word beside the name: a count, a kind, the curated word, a source id.
    pub short: String,
    /// The pane's `kind` line, which has the room to say it in words.
    pub words: String,
    /// `short` is the word a source or catalog gives the place.
    pub curated: bool,
    /// `short` is the source id the path names, or that the source is gone.
    pub source: bool,
    /// The source the path names has left the config.
    pub missing_source: bool,
}

/// What `entry` is called. `look` is a bucket directory's look-up state, drawn at
/// spinner `frame`; `known_sources` are the sources a URL can name, `None` where
/// the trail already names it.
pub fn describe(
    entry: &Entry,
    place_kind: Option<&'static str>,
    look: Option<CloudLook>,
    frame: usize,
    known_sources: Option<&[crate::config::CloudConnectionConfig]>,
) -> RowLabel {
    // The door is an action: labels, curated words and source ids describe the
    // directory, not the door.
    if entry.opens_whole_directory {
        return RowLabel::default();
    }
    let g = crate::glyphs::get();
    // A source's word for a place it names (`dataset`, `project`), or `missing` for an
    // absent catalog dataset, ahead of any count so the row stays marked curated.
    let curated =
        place_kind.filter(|_| matches!(entry.kind, EntryKind::Directory | EntryKind::Unknown));
    let look_glyph = look.map(|look| match look {
        CloudLook::Waiting => g.ellipsis,
        CloudLook::Looking => g.spinner[frame % g.spinner.len()],
        CloudLook::Failed => "?",
    });
    let short = match curated {
        Some(word) => word.to_string(),
        // An uncounted directory: a bucket's own word, else that it is being looked into.
        None if entry.kind == EntryKind::Directory && entry.holds.formats.is_empty() => {
            object_place_label(&entry.path)
                .or(look_glyph)
                .map(str::to_string)
                .unwrap_or_else(|| entry.label().into_owned())
        }
        None => entry.label().into_owned(),
    };
    // Two stores can hold the same bucket and key, so a row named by source says
    // which, or that the source has left the config.
    let path_text = entry.path.to_string_lossy();
    let named = crate::cloud::source::split_source_id(&path_text).0;
    let missing_source = named
        .is_some_and(|id| known_sources.is_some_and(|known| !known.iter().any(|k| k.name == id)));
    let (short, source) = match (named, known_sources) {
        (Some(id), Some(_)) if short.is_empty() && missing_source => {
            (format!("source not found: {id}"), true)
        }
        (Some(id), Some(_)) if short.is_empty() => (id.to_string(), true),
        // Nothing known yet: an ellipsis claims nothing.
        _ if short.is_empty() && entry.kind == EntryKind::Unknown => {
            (g.ellipsis.to_string(), false)
        }
        _ => (short, false),
    };
    let words = match (&entry.table, entry.kind) {
        (Some(table), _) => {
            let of = match (&entry.format_spec, table.format) {
                (Some(spec), _) => spec.clone(),
                (None, Some(format)) => format.name().to_string(),
                (None, None) => String::new(),
            };
            format!("{of} {}", table.kind).trim_start().to_string()
        }
        (None, _) if curated.is_some() => curated.unwrap_or_default().to_string(),
        (None, EntryKind::File) => match (
            &entry.format_spec,
            crate::FileFormat::from_path(&entry.path),
        ) {
            (Some(spec), _) => format!("{spec} file"),
            (None, Some(format)) => format!("{} file", format.name()),
            // Named nothing, and found by its bytes to be data.
            (None, None) => "data file".to_string(),
        },
        (None, EntryKind::Hive) => "hive table".to_string(),
        (None, EntryKind::MultiFile) => "multi-file table".to_string(),
        (None, kind) if kind.is_lake_table() => {
            format!(
                "{} table",
                kind.lake_name().unwrap_or_default().to_lowercase()
            )
        }
        (None, EntryKind::Directory) => match look {
            Some(CloudLook::Failed) => format!("? {} listing failed, Ctrl+R retries", g.middot),
            // Nothing to say yet; the row's spinner shows it is being looked into.
            Some(_) => String::new(),
            None => object_place_label(&entry.path)
                .unwrap_or("directory")
                .to_string(),
        },
        _ => String::new(),
    };
    RowLabel {
        short,
        words,
        curated: curated.is_some(),
        source,
        missing_source,
    }
}

/// How a cloud source is addressed on home: `cloud://<id>`, naming the level above
/// its buckets, which no real URL can.
pub const CLOUD_PLACE: &str = "cloud://";

/// Lines kept between the cursor and the edge of the list while it can scroll.
const SCROLL_MARGIN: usize = 2;

/// The first line of a `height`-line list that keeps `selected` on screen, given it
/// started at `top`. Scrolls only enough to keep the cursor [`SCROLL_MARGIN`] lines
/// from an edge, and never leaves empty space below the last line.
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
        crate::cloud::cloud_sources::is_within(url, root)
    }
    #[cfg(not(feature = "cloud"))]
    {
        let (url, root) = (url.trim_end_matches('/'), root.trim_end_matches('/'));
        url == root || url.strip_prefix(root).is_some_and(|r| r.starts_with('/'))
    }
}

/// Whether two locations are one place, however a trailing slash, an Azure URL or a
/// local path's separators and drive letter are spelled.
fn same_place(a: &Path, b: &Path) -> bool {
    place_key(a) == place_key(b)
}

/// A location as [`same_place`] compares it.
fn place_key(path: &Path) -> String {
    let text = path.to_string_lossy();
    // A catalog may write `C:/data/x.csv` where a listing or a recent has
    // `C:\data\x.csv`; compare local paths by their components.
    if !text.contains("://") {
        return crate::config::path_place(path).display().to_string();
    }
    #[cfg(feature = "cloud")]
    {
        crate::cloud::source::canonical_cloud_place(&text)
    }
    #[cfg(not(feature = "cloud"))]
    {
        text.trim_end_matches('/').to_string()
    }
}

/// The catalogs' datasets and bookmarks indexed by place, for per-row lookups.
#[derive(Debug, Default)]
pub struct CatalogPlaces {
    /// Catalog and dataset, by [`place_key`]: the first listed of two at one place.
    datasets: std::collections::HashMap<String, (usize, usize)>,
    /// Catalog, dataset and bookmark, by [`place_key`].
    bookmarks: std::collections::HashMap<String, (usize, usize, usize)>,
    /// How many datasets and bookmarks the catalogs held; catalogs set other than by
    /// [`HomeState::set_catalogs`] are scanned instead.
    counted: (usize, usize),
}

impl CatalogPlaces {
    fn of(catalogs: &[ShownCatalog]) -> Self {
        let mut places = CatalogPlaces {
            counted: Self::count(catalogs),
            ..Default::default()
        };
        for (c, catalog) in catalogs.iter().enumerate() {
            for (d, dataset) in catalog.datasets.iter().enumerate() {
                places
                    .datasets
                    .entry(place_key(&dataset.location))
                    .or_insert((c, d));
                for (b, (_, place)) in dataset.bookmarks.iter().enumerate() {
                    places
                        .bookmarks
                        .entry(place_key(place))
                        .or_insert((c, d, b));
                }
            }
        }
        places
    }

    fn count(catalogs: &[ShownCatalog]) -> (usize, usize) {
        let datasets = catalogs.iter().flat_map(|c| c.datasets.iter());
        (
            datasets.clone().count(),
            datasets.map(|d| d.bookmarks.len()).sum(),
        )
    }

    /// Whether the index answers for `catalogs`: a lookup it misses is a miss.
    fn indexes(&self, catalogs: &[ShownCatalog]) -> bool {
        self.counted == Self::count(catalogs)
    }
}

/// What is left of `url` below `root`, which it is [`within`].
fn within_rest(url: &str, root: &str) -> String {
    let canonical = |u: &str| match crate::cloud::source::azure_parts(u) {
        Some((account, container, path)) => {
            crate::cloud::source::azure_url(&account, &container, &path)
        }
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
        || crate::cloud::source::azure_parts(&text).is_some()
}

/// The URL that opens a cloud directory as one dataset: with a trailing slash, so
/// it is scanned as a prefix rather than fetched as an object.
pub fn directory_dataset_url(path: &Path) -> PathBuf {
    let text = path.to_string_lossy();
    if text.ends_with('/') {
        path.to_path_buf()
    } else {
        PathBuf::from(format!("{text}/"))
    }
}

/// The `(all files)` row: opens the browsed directory as one table whatever its
/// label, so a wrong label costs at most a keystroke. Offered in every directory,
/// local or remote, and built from the listing on screen.
fn whole_directory_row(dir: &Path, rows: &[Entry], remote: bool) -> Option<Entry> {
    // A `cloud://<id>/<account>` place is an Azure account: its children are
    // containers, with no URL to open.
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
    // Each route classifies in its own terms: the listing drops dotted names (so a
    // local `.hoodie` would be missed by the cloud classifier), and the cloud Iceberg
    // rule is looser by design. Nothing remote is read here: a listing of a dead share
    // freezes the UI, so local classification uses rows the probe already read.
    let (kind, holds) = if remote || is_object_store_url(dir) {
        #[cfg(feature = "cloud")]
        {
            crate::cloud::cloud_browse::look_at_listing(
                &dir.to_string_lossy(),
                &directories,
                &objects,
            )
        }
        // Without the cloud feature there is no remote classifier, and reading the share
        // is what this avoids: the door is offered without a kind (losing only the lake
        // check; its label is suppressed anyway).
        #[cfg(not(feature = "cloud"))]
        {
            let _ = (&directories, &objects);
            (EntryKind::Unknown, Default::default())
        }
    } else {
        crate::home::discover::look_at_directory(dir)
    };
    // No door into a directory with nothing to open (empty, or only `_SUCCESS`).
    // Judged by what the directory holds, not only the listed rows: Spark and GBIF
    // part files have no extension and list nothing, yet open by their bytes.
    // Extensionless files and subdirectories count; files no reader takes do not.
    let openable_row = rows.iter().any(|r| r.kind != EntryKind::Other);
    if !openable_row && holds_nothing_to_open(&holds) {
        return None;
    }
    let mut entry = Entry::directory(&directory_dataset_url(dir));
    entry.kind = kind;
    // This listing's tally; it may differ from the directory's own row upstairs,
    // counted from one peek page.
    entry.holds = holds;
    entry.opens_whole_directory = true;
    entry.name = door_name(&entry, rows);
    Some(entry)
}

/// The directory a door opens, named as the section title names it: no source id
/// (`s3://lab@bucket` is `bucket`), and an Azure container by its name.
/// `file_name` rather than splitting on `/`, for the filesystem root and Windows.
fn door_base_name(dir: &Path) -> String {
    let text = dir.to_string_lossy();
    if let Some((_, container, key)) = crate::cloud::source::azure_parts(&text) {
        let leaf = key.trim_matches('/').rsplit('/').next().unwrap_or("");
        if leaf.is_empty() {
            container
        } else {
            leaf.to_string()
        }
    } else {
        let (_, plain) = crate::cloud::source::split_source_id(&text);
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

/// Which [`DoorKind`] a door is. A directory the footers or headers turned down as
/// one table is `Directory` with one format in its tally; the listing alone calls
/// it `MultiFile`.
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
/// opens the one dataset the directory's own `Enter` opens; elsewhere it would start
/// an unasked combined read.
pub fn door_lands(door: &Entry) -> bool {
    matches!(door_kind(door), DoorKind::Hive | DoorKind::OneSchema)
}

/// A format as prose names it, by the name the listing counted: `Parquet`, `CSV`.
fn format_title(name: &str) -> String {
    crate::FileFormat::from_name(name)
        .map_or_else(|| name.to_ascii_uppercase(), |f| f.title().to_string())
}

/// The partition keys a hive door names: from the footer pass's layout, else the
/// `key=value` names listed (in a bucket, the levels seen so far).
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

/// What `Enter` on a door that is not one table reads and leaves out, for the
/// details pane. Locally: the commonest format's files directly inside, or a whole
/// Parquet scan when there are subdirectories or no own files. In an object store:
/// the prefix scanned whole in its commonest format.
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

/// Name files a format spec's glob matches as data, under the spec's name, from the
/// name alone. Files a spec's magic matches were named by the scan's sniff.
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
            discover::name_spec_file(row, spec);
            named = true;
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

/// Whether a directory holds nothing a `(all files)` row could read. Shared by the
/// door and the details pane so they agree.
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
    if let Some((_, _, key)) = crate::cloud::source::azure_parts(&text) {
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

/// The location one level up from `path`, or `None` at the top. A URL's top is its
/// bucket or host (`Path::parent` would make `gs://bucket` into `gs:`).
pub fn parent_location(path: &Path) -> Option<PathBuf> {
    if !matches!(
        crate::cloud::source::input_source(path),
        crate::cloud::source::InputSource::Local(_)
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

/// Whether reading `path` could block: an object-store or HTTP URL, or a network
/// filesystem. Decides what home may touch on the UI thread; answered from the
/// string and mount table alone.
pub fn is_remote_path(path: &Path) -> bool {
    is_cloud_place(path)
        || !matches!(
            crate::cloud::source::input_source(path),
            crate::cloud::source::InputSource::Local(_)
        )
        || is_network_path(path)
}

/// Whether `path` is on a filesystem a call can hang on, by the mount table (longest
/// matching mount point): a network one, or any FUSE one. False where the table is
/// unavailable: a hint, never a gate. Uses
/// [`crate::home::locality::Mounts::cached`], since this is asked per row per frame.
pub fn is_network_path(path: &Path) -> bool {
    crate::home::locality::Mounts::cached().could_block(path)
}

/// The mount-table logic against a fixture, for tests (e.g. an NFS share shadowing
/// an autofs entry at the same path).
#[doc(hidden)]
pub fn network_fs_for_test(mountinfo: &str, path: &Path) -> bool {
    crate::home::locality::Mounts::parse(mountinfo).is_network(path)
}

/// A place datui will look, and whether it can currently be read.
#[derive(Debug, Clone)]
pub struct Root {
    pub path: PathBuf,
    pub origin: RootOrigin,
    /// True when the root is on a network filesystem.
    pub network: bool,
    /// False when the directory cannot be read (an unmounted NAS, a deleted scratch
    /// dir); shown, not hidden.
    pub available: bool,
}

/// A titled group of rows on the home screen.
#[derive(Debug, Clone, Default)]
pub struct Section {
    pub title: String,
    /// The section's state at the far end of the rule: its filesystem, `first 5000` for
    /// a cut listing, what a search covered. Why it exists is `origin`.
    pub subtitle: Option<String>,
    /// Why a path-titled section is here (`current directory`, `configured`), as a chip
    /// beside the count.
    pub origin: Option<&'static str>,
    /// The directory a path-titled section lists (a root, or the browsed directory);
    /// the title is abbreviated and cannot be turned back into a path.
    pub root: Option<PathBuf>,
    pub rows: Vec<Entry>,
    /// The row opening this section's directory as one table, kept out of `rows`: its
    /// path is the directory's own, so among the rows it would collide in every
    /// path-keyed map with the directory's row one level up. Nothing that walks `rows`
    /// or matches [`Row::Entry`] can reach it.
    pub door: Option<Entry>,
    /// Set when a root could not be read, so the UI can say why it is empty.
    pub unavailable: bool,
    /// What to say instead of "unavailable", when there is more to say (a refused bucket
    /// listing's reason and fix).
    pub unavailable_note: Option<String>,
    /// Starts folded unless opened: places that are context rather than the reason you
    /// came (directories promoted from recents, the desktop's list).
    pub folded_by_default: bool,
    /// The remote root whose background probe fills this section in.
    pub remote_root: Option<PathBuf>,
    /// The probe had not answered when this listing was built: empty means "wait".
    pub waiting: bool,
    /// Rows are grouped under the place each lives in, with a place row. Set on
    /// `RECENT`, whose rows come from anywhere.
    pub grouped_by_place: bool,
    /// What the dataset index remembers each place to be, for a grouped section's place
    /// rows; from the cache, never a read.
    pub place_labels: std::collections::HashMap<PathBuf, String>,
}

impl Section {
    /// A section of `rows` under `title`, everything else at its default.
    pub fn titled(title: impl Into<String>, rows: Vec<Entry>) -> Self {
        Section {
            title: title.into(),
            rows,
            ..Default::default()
        }
    }
}

/// Where a cloud source's listing stands.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum CloudStatus {
    /// Asked, and no answer yet.
    #[default]
    Listing,
    /// Not asked and not listed before; its rows, if any, are the config's named
    /// buckets. Entering it or Ctrl+R lists it.
    Unlisted,
    /// Listed, now or on an earlier run.
    Listed,
    /// The listing was refused or never answered: `short` for the row, `detail` (what
    /// happened, how to fix) for the details pane.
    Failed { short: String, detail: String },
}

/// One cloud source on home: a row under `CLOUD` and its bucket list. Kept apart
/// from [`Section`] to survive rebuilds: re-listing buckets would be a billed round
/// trip per keystroke.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CloudSource {
    /// The source ID, as in `[[cloud.connections]]` and `s3://<id>@bucket`.
    pub id: String,
    /// The row's name.
    pub label: String,
    /// The API spoken.
    pub api: crate::cloud::source::ProviderKind,
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
                let (one, many) = match self.api {
                    crate::cloud::source::ProviderKind::Azure => ("account", "accounts"),
                    crate::cloud::source::ProviderKind::Gcs => ("project", "projects"),
                    crate::cloud::source::ProviderKind::S3 => ("bucket", "buckets"),
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

/// A catalog as the home screen shows it: a section of named datasets, local and remote
/// alike.
#[derive(Debug, Clone, PartialEq)]
pub struct ShownCatalog {
    /// The catalog's id, as `[home] hide` names it.
    pub id: String,
    /// The section's title.
    pub label: String,
    /// `catalog.toml`, a listed file, or the bundled catalog.
    pub origin: crate::home::catalog::Origin,
    /// What the catalog says it is, for its heading's details.
    pub description: String,
    /// The file it was read from; none for the bundled one.
    pub file: Option<PathBuf>,
    pub datasets: Vec<ShownDataset>,
    /// Left out for a mistake: the one line its section says instead of rows.
    pub broken: Option<String>,
}

/// One dataset of a [`ShownCatalog`].
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ShownDataset {
    /// The row's name.
    pub name: String,
    /// The local path with `~` and `$VAR` expanded, or the URL.
    pub location: PathBuf,
    /// `key  value` lines for the details pane.
    pub details: Vec<(String, String)>,
    /// What the catalog says a remote file weighs, until it is measured.
    pub size: Option<u64>,
    /// What its columns mean, when the catalog says.
    pub codebook: Option<std::sync::Arc<crate::home::codebook::Codebook>>,
    /// Places inside it to start from, by name, listed under its row.
    pub bookmarks: Vec<(String, PathBuf)>,
    /// The entry as its catalog writes it: what the Documentation view shows.
    pub entry: std::sync::Arc<crate::home::catalog::Dataset>,
}

impl ShownCatalog {
    /// A catalog as the home screen shows it.
    pub fn from_catalog(catalog: &crate::home::catalog::Catalog) -> Self {
        Self {
            id: catalog.id.clone(),
            label: catalog.label.clone(),
            origin: catalog.origin,
            description: catalog.description.clone(),
            file: catalog.file.clone(),
            datasets: catalog
                .datasets
                .iter()
                .map(|dataset| {
                    let location = dataset.location();
                    let mut details: Vec<(String, String)> = [
                        ("about", &dataset.description),
                        ("publisher", &dataset.publisher),
                        ("license", &dataset.license),
                        ("homepage", &dataset.homepage),
                        ("documentation", &dataset.documentation),
                    ]
                    .into_iter()
                    .filter(|(_, value)| !value.is_empty())
                    .map(|(key, value)| (key.to_string(), value.clone()))
                    .collect();
                    match &dataset.url {
                        None => details.push(("path".to_string(), display_path(&location))),
                        Some(url) => {
                            details.push(("url".to_string(), url.clone()));
                            details.push(("login".to_string(), login_of(dataset)));
                        }
                    }
                    ShownDataset {
                        name: dataset.name.clone(),
                        location,
                        details,
                        size: dataset.size,
                        codebook: crate::home::codebook::Codebook::of(dataset)
                            .map(std::sync::Arc::new),
                        bookmarks: dataset
                            .bookmarks
                            .iter()
                            .map(|(name, path)| (name.clone(), dataset.bookmark_location(path)))
                            .collect(),
                        entry: std::sync::Arc::new(dataset.clone()),
                    }
                })
                .collect(),
            broken: None,
        }
    }

    /// A catalog file left out for a mistake, as a section that says what is wrong.
    pub fn from_broken(broken: &crate::home::catalog::Broken) -> Self {
        Self {
            id: broken.id.clone(),
            label: broken.id.clone(),
            origin: broken.origin,
            description: String::new(),
            file: None,
            datasets: Vec::new(),
            broken: Some(broken.callout()),
        }
    }

    /// The chip beside the section's title: where its datasets are written. Each is
    /// one of [`CATALOG_ORIGINS`].
    pub fn origin_note(&self) -> &'static str {
        match self.origin {
            crate::home::catalog::Origin::Mine => "catalog.toml",
            crate::home::catalog::Origin::Listed | crate::home::catalog::Origin::Folder => {
                "catalog"
            }
            crate::home::catalog::Origin::Bundled => BUNDLED_ORIGIN,
        }
    }
}

/// The chip on the bundled catalog's section.
pub const BUNDLED_ORIGIN: &str = "comes with datui";

/// The chips a catalog's section carries, and nothing else does.
pub const CATALOG_ORIGINS: [&str; 3] = ["catalog.toml", "catalog", BUNDLED_ORIGIN];

/// Whether a section's origin chip says it is a catalog.
pub fn is_catalog_origin(origin: &str) -> bool {
    CATALOG_ORIGINS.contains(&origin)
}

/// How a catalog URL is read, in words: what `auth` and `connection` say.
pub fn login_of(dataset: &crate::home::catalog::Dataset) -> String {
    match dataset.object_store_auth() {
        Some(crate::config::DatasetAuth::Connection(connection)) => connection,
        Some(crate::config::DatasetAuth::Anonymous) | None => "none".to_string(),
        Some(crate::config::DatasetAuth::Auto) => "auto".to_string(),
    }
}

/// The catalogs home shows, in order. The bundled catalog keeps only what this
/// build can open (all its datasets are remote); a user's catalog is shown whole.
/// An empty catalog has no section.
pub fn catalogs(config: &crate::config::AppConfig) -> Vec<ShownCatalog> {
    let mut out: Vec<ShownCatalog> = config
        .shown_catalogs()
        .iter()
        .filter_map(|catalog| {
            let mut shown = ShownCatalog::from_catalog(catalog);
            if catalog.origin == crate::home::catalog::Origin::Bundled {
                shown
                    .datasets
                    .retain(|d| crate::cloud::source::opens_in_this_build(&d.location));
            }
            (!shown.datasets.is_empty()).then_some(shown)
        })
        .collect();
    // A broken file's section says so, before the bundled catalog, unless it is hidden.
    let at = out
        .iter()
        .position(|c| c.origin == crate::home::catalog::Origin::Bundled)
        .unwrap_or(out.len());
    let broken: Vec<ShownCatalog> = config
        .broken_catalogs
        .iter()
        .filter(|b| !config.home.hide.contains(&b.id))
        .map(ShownCatalog::from_broken)
        .collect();
    out.splice(at..at, broken);
    out
}

/// The row for one catalog dataset. Reads nothing but a local path's directory
/// entry; a remote dataset is named by its URL.
fn catalog_entry(
    dataset: &ShownDataset,
    network_check: fn(&Path) -> bool,
    missing: &mut std::collections::HashSet<PathBuf>,
) -> Entry {
    let path = &dataset.location;
    let local = matches!(
        crate::cloud::source::input_source(path),
        crate::cloud::source::InputSource::Local(_)
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
    entry
}

/// The column notes of the catalog dataset `path` is or is inside (the innermost),
/// among datasets with notes.
pub fn codebook_for(
    catalogs: &[ShownCatalog],
    path: &Path,
) -> Option<std::sync::Arc<crate::home::codebook::Codebook>> {
    let text = path.to_string_lossy();
    catalogs
        .iter()
        .flat_map(|c| c.datasets.iter())
        .filter(|d| d.codebook.is_some())
        .filter(|d| d.location == path || within(&text, &d.location.to_string_lossy()))
        .max_by_key(|d| d.location.to_string_lossy().trim_end_matches('/').len())
        .and_then(|d| d.codebook.clone())
}

/// The catalog entry `path` is or is inside, with its catalog's label: the innermost,
/// or the first listed of two at one place.
pub fn catalog_entry_for(
    catalogs: &[ShownCatalog],
    path: &Path,
) -> Option<(String, std::sync::Arc<crate::home::catalog::Dataset>)> {
    let text = path.to_string_lossy();
    catalogs
        .iter()
        .flat_map(|c| c.datasets.iter().map(move |d| (c, d)))
        .filter(|(_, d)| {
            d.location == path
                || same_place(&d.location, path)
                || within(&text, &d.location.to_string_lossy())
        })
        .rev()
        .max_by_key(|(_, d)| d.location.to_string_lossy().trim_end_matches('/').len())
        .map(|(c, d)| (c.label.clone(), d.entry.clone()))
}

/// The row for a bookmark inside one of a catalog's datasets.
fn bookmark_entry(name: &str, place: &Path, network_check: fn(&Path) -> bool) -> Entry {
    let local = matches!(
        crate::cloud::source::input_source(place),
        crate::cloud::source::InputSource::Local(_)
    );
    let mut entry = if is_object_store_url(place) && !names_a_file(place) {
        Entry::directory(place)
    } else {
        entry_for_path(place, !local || network_check(place) || !place.exists())
    };
    entry.name = name.to_string();
    entry
}

/// A catalog's section.
fn catalog_section(
    catalog: &ShownCatalog,
    network_check: fn(&Path) -> bool,
    missing: &mut std::collections::HashSet<PathBuf>,
) -> Section {
    // Each dataset, and under it its bookmarks.
    let rows = catalog
        .datasets
        .iter()
        .flat_map(|dataset| {
            std::iter::once(catalog_entry(dataset, network_check, missing)).chain(
                dataset
                    .bookmarks
                    .iter()
                    .map(|(name, place)| bookmark_entry(name, place, network_check)),
            )
        })
        .collect();
    Section {
        origin: Some(catalog.origin_note()),
        unavailable: catalog.broken.is_some(),
        unavailable_note: catalog.broken.clone(),
        ..Section::titled(catalog.label.clone(), rows)
    }
}

/// What measuring a dataset yielded; each part absent when unknowable without
/// reading the data.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Measured {
    pub rows: Option<usize>,
    pub cols: Option<usize>,
    /// Whether `cols` is a floor rather than a total. See [`crate::home::discover::Entry`].
    pub cols_sampled: bool,
    pub size: Option<u64>,
    /// When it last changed, from the stat a listing leaves to the rows shown.
    pub modified: Option<std::time::SystemTime>,
    /// Column names, when the format gave them up for free.
    pub columns: Vec<String>,
    /// What opening it costs: compression, layout, partitioning, carried to the screen
    /// with the row count.
    pub cost: crate::home::discover::Cost,
    /// What the footers said it is when that differs from its filenames (separate tables
    /// make a plain directory); usually `None`. See [`crate::home::discover::enrich`].
    pub kind: Option<crate::home::discover::EntryKind>,
    /// What one listing found, which the row's label says; only the classify pass counts
    /// a local directory.
    pub holds: crate::home::discover::Holds,
}

/// How rows are ordered within each section.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum SortMode {
    /// Each section's natural order: recency under Recent, name under a directory.
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
    /// What this mode does in a section, since `Natural` means recency or name depending
    /// on where the cursor is.
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

/// One line of the home screen; headers are selectable to fold. Places and `more`
/// rows are view rows, not entries: an [`Entry`]'s kind triggers probes,
/// measurement and caching, none of which may happen to a place.
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
        /// How it answers the filter, for the marks the row is drawn with.
        hit: Hit,
    },
    /// The directory or prefix the entries below it live in, under `RECENT`.
    Place {
        section: usize,
        path: PathBuf,
        /// What the place was last found to be (`hive`, `12 parquet`), from the dataset
        /// index only, never a read.
        label: Option<String>,
        /// The filesystem it is on, or the object store's scheme.
        source: Option<String>,
        /// How many recents live there, whether or not the filter shows them.
        held: usize,
    },
    /// The door opening the browsed directory as one table; see [`Section::door`]. Not
    /// an `Entry` row, since it carries the directory's own path.
    Door { section: usize, entry: &'a Entry },
    /// What a cap is hiding: `RECENT`'s, `… 13 more in 5 places`, or a directory's
    /// at the root listing, `… 4,958 more` (`places` is 0).
    More {
        section: usize,
        hidden: usize,
        places: usize,
        /// Sorted by rows with some hidden rows still being measured, so the order may
        /// change.
        measuring: bool,
    },
    /// The last row of a browsed directory hiding unreadable files (`… 10 files with no
    /// reader`), so a directory of notes does not look empty. `Enter` shows them, as
    /// `Ctrl+A` does.
    Hidden { section: usize, count: usize },
    /// The way up, first in a directory's section: `..`; Enter goes to the parent, as
    /// Backspace does.
    Up { section: usize },
}

impl Row<'_> {
    pub fn section(&self) -> usize {
        match self {
            Row::Header { section, .. }
            | Row::Entry { section, .. }
            | Row::Door { section, .. }
            | Row::Place { section, .. }
            | Row::More { section, .. }
            | Row::Hidden { section, .. }
            | Row::Up { section } => *section,
        }
    }
}

/// How a row answers the filter: its score, and whether its name or a column matched.
/// Scored when rows are listed; the matched characters are found when drawn, for the
/// rows on screen rather than every row of thousands.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Hit {
    pub score: i32,
    /// Index into the entry's `columns` of the matching column, when the name did not
    /// match.
    pub column: Option<usize>,
}

impl Hit {
    /// The column that matched, when the name did not.
    pub fn column_of<'a>(&self, entry: &'a Entry) -> Option<&'a str> {
        self.column
            .and_then(|i| entry.columns.get(i))
            .map(String::as_str)
    }

    /// Character positions to mark for `filter`: in the name, or in the matched
    /// column's name. From the same alignment that scored it.
    pub fn positions(&self, filter: &str, entry: &Entry) -> Vec<usize> {
        match self.column_of(entry) {
            Some(column) => substring_positions(filter, column),
            None => fuzzy_positions(filter, &entry.name),
        }
    }
}

/// [`HomeState::visible`]'s rows, cached until their inputs change: building scores
/// every row and sorts each section, and every pass reads them. `HomeState`'s own
/// row-changing methods drop it; fields set elsewhere (filter, sort, folds) are
/// compared on every read.
#[derive(Debug, Default)]
pub struct RowsCache {
    built: std::cell::RefCell<Option<View>>,
    builds: std::cell::Cell<usize>,
    /// The last build's hits, kept for the next build when only some rows changed
    /// (measured, or `Found` replaced): see [`HomeState::rows_changed`].
    rescored: std::cell::RefCell<Option<Hits>>,
}

/// Every row's match against the filter, by section and row index; `None` where the row
/// does not match. A section with no hits is scored at the next build.
#[derive(Debug, Clone)]
struct Hits {
    filter: String,
    sections: Vec<Option<Vec<Option<Hit>>>>,
}

/// The rows as last built, and what they were built from.
#[derive(Debug)]
struct View {
    key: ViewKey,
    slots: Vec<Slot>,
    /// Which slots are section headers, in order: few, so the list's lines are found
    /// from them without walking every row.
    headers: Vec<usize>,
    hits: Hits,
    /// See [`HomeState::has_any_dataset`].
    has_dataset: bool,
}

/// The rows' inputs not changed only through [`HomeState`]'s methods, plus the
/// sections' shape, so a stale index is never read.
#[derive(Debug, PartialEq)]
struct ViewKey {
    filter: String,
    sort: SortMode,
    hide_unreadable: bool,
    recent_expanded: bool,
    shown_whole: std::collections::HashSet<PathBuf>,
    view_height: usize,
    browsing: Option<PathBuf>,
    folds: std::collections::HashMap<String, bool>,
    shape: Vec<(usize, bool)>,
}

impl ViewKey {
    fn of(home: &HomeState) -> Self {
        ViewKey {
            filter: home.filter.clone(),
            sort: home.sort,
            hide_unreadable: home.hide_unreadable,
            recent_expanded: home.recent_expanded,
            shown_whole: home.shown_whole.clone(),
            view_height: home.view_height,
            browsing: home.browsing.clone(),
            folds: home.folds.clone(),
            shape: (home.sections.iter())
                .map(|s| (s.rows.len(), s.door.is_some()))
                .collect(),
        }
    }

    /// Whether `home` would make this key, compared without cloning.
    fn matches(&self, home: &HomeState) -> bool {
        self.filter == home.filter
            && self.sort == home.sort
            && self.hide_unreadable == home.hide_unreadable
            && self.recent_expanded == home.recent_expanded
            && self.shown_whole == home.shown_whole
            && self.view_height == home.view_height
            && self.browsing == home.browsing
            && self.folds == home.folds
            && self.shape.iter().copied().eq(home
                .sections
                .iter()
                .map(|s| (s.rows.len(), s.door.is_some())))
    }
}

/// A built row, by index into the sections for the rows that are entries.
#[derive(Debug)]
enum Slot {
    /// A row that holds no entry, as it is drawn.
    Plain(Row<'static>),
    Entry {
        section: usize,
        index: usize,
        nested: bool,
        hit: Hit,
    },
    Door {
        section: usize,
    },
}

/// Where the listed rows fall in the list, given its section headers: with `spaced`, a
/// blank line comes before every header but the first. Answers in a few steps for
/// any row or line, so a frame never walks thousands of rows to find its own.
#[derive(Debug, Clone)]
pub struct ListLines {
    headers: Vec<usize>,
    rows: usize,
    spaced: bool,
}

impl ListLines {
    /// How many rows are listed.
    pub fn rows(&self) -> usize {
        self.rows
    }

    /// How many of them are section headers.
    pub fn headers(&self) -> usize {
        self.headers.len()
    }

    /// The line row `row` is drawn on.
    pub fn line_of(&self, row: usize) -> usize {
        if !self.spaced {
            return row;
        }
        // Headers at or above the row, but the first, each bring a blank line.
        let above = self.headers.partition_point(|&h| h <= row);
        let first_at_top = self.headers.first() == Some(&0);
        row + above.saturating_sub(usize::from(first_at_top))
    }

    /// Lines the whole list takes.
    pub fn total(&self) -> usize {
        match self.rows {
            0 => 0,
            n => self.line_of(n - 1) + 1,
        }
    }

    /// The first row drawn on `line` or below it.
    pub fn first_row_from(&self, line: usize) -> usize {
        let (mut lo, mut hi) = (0, self.rows);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if self.line_of(mid) < line {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }

    /// The row drawn on `line`; `None` for a blank line or past the end.
    pub fn row_on(&self, line: usize) -> Option<usize> {
        let row = self.first_row_from(line);
        (row < self.rows && self.line_of(row) == line).then_some(row)
    }
}

/// The place a recent lives in: its directory or object-store prefix; a bare bucket
/// or host is its own place.
pub fn place_of(path: &Path) -> PathBuf {
    parent_location(path).unwrap_or_else(|| path.to_path_buf())
}

/// Whether a place can be listed: a directory or an object-store prefix. An HTTP
/// server has no listing, so a URL recent's place is a heading, not a door.
pub fn place_is_browsable(path: &Path) -> bool {
    is_cloud_place(path)
        || is_object_store_url(path)
        || matches!(
            crate::cloud::source::input_source(path),
            crate::cloud::source::InputSource::Local(_)
        )
}

/// A row's identity apart from its index, to find it again after a rebuild or a cap
/// change: the cursor's index points at different rows whenever a listing lands or
/// the terminal resizes, which would make `Enter` open the wrong row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowKey {
    Header(String),
    Entry(PathBuf),
    /// The door, by the directory it opens; keyed as an `Entry` it would land on the
    /// directory's row.
    Door(PathBuf),
    Place(PathBuf),
    More(String),
    Hidden(String),
    Up(String),
}

/// Home screen state.
#[derive(Debug)]
pub struct HomeState {
    pub sections: Vec<Section>,
    /// Fuzzy filter over every row in every section.
    pub filter: String,
    /// The filter kept from before a dataset opened, shown selected: the next character
    /// replaces it and `~` opens the path prompt. Any other key keeps it.
    pub filter_selected: bool,
    /// The most search matches listed under `Found`: `[home.search] max_results`.
    pub search_limit: usize,
    /// Hide files datui has no reader for (default); `Ctrl+A` flips it.
    pub hide_unreadable: bool,
    /// The format specs on the search path: files one names by glob list as data under
    /// its name.
    pub formats: std::sync::Arc<crate::formats::Registry>,
    /// The lake table being browsed and its format, said on its heading for the whole
    /// browse.
    pub lake_here: Option<(PathBuf, &'static str)>,
    /// Index into the flattened list of currently visible rows.
    pub selected: usize,
    /// First row of the last frame, as an index into [`HomeState::visible`]. Written by
    /// the renderer (which knows the list's height) and read by
    /// [`HomeState::unclassified_visible`], so what is looked into is what is on
    /// screen. Zero until the first frame.
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
    /// Where the browse began (entered from the root listing or jumped to). Esc climbs
    /// back to here, then to the listing, never above. `None`: `browsing` is the start.
    pub browse_start: Option<PathBuf>,
    /// Transient message (e.g. a path that does not exist).
    pub status: Option<String>,
    /// How a path is judged network-backed; swappable so the never-touch-remote rule is
    /// testable without a remote.
    pub network_check: fn(&Path) -> bool,
    /// How often and how lately each recent was opened, ranking matches. Set with
    /// [`HomeState::set_visits`], which relists.
    pub visits: std::collections::HashMap<PathBuf, crate::cache::Visits>,
    /// The recent opened last: the cursor lands here, one Enter from the last file.
    pub newest_recent: Option<PathBuf>,
    /// Where the listing of each network root and remote directory is.
    pub probes: Probes,
    /// The names a filter asked the server for, in a cloud directory cut short.
    pub narrowed: Option<Narrowed>,
    /// What peeked cloud directories hold (`hive`, `multi`), kept for the session so each
    /// is peeked once.
    pub cloud_kinds: std::collections::HashMap<PathBuf, (EntryKind, crate::home::discover::Holds)>,
    /// How rows are ordered inside each section.
    pub sort: SortMode,
    /// A listing is being built on a worker; the previous one stays on screen.
    pub listing_in_flight: bool,
    /// True while a measurement batch is out, so only one is in flight at a time.
    pub measure_in_flight: bool,
    /// The filesystems (by mount point) with a classification pass out. One pass per
    /// filesystem, each chosen from the viewport then, so fast paging does not queue
    /// every row it passed and a share that stopped answering stops only its own rows.
    pub classifying: std::collections::HashSet<PathBuf>,
    /// Cloud directories with a peek out, kept apart from [`Self::cloud_kinds`] so no
    /// answer is claimed before the request returns.
    pub peeking: std::collections::HashSet<PathBuf>,
    /// Cloud directories whose peek failed: not asked again until Ctrl+R, labeled `?`
    /// (not `dir`, which would claim no data inside).
    pub peek_failed: std::collections::HashSet<PathBuf>,
    /// Row and column counts already read, by path, so each dataset is measured once a
    /// session.
    pub enriched: std::collections::HashMap<PathBuf, Measured>,
    /// Paths recorded in `enriched` since the rows last took them in.
    pub unapplied: std::collections::HashSet<PathBuf>,
    /// Sections folded (`true`) or opened by the user, by title so it survives rebuilds
    /// that renumber sections, and cached across restarts. Unlisted sections take their
    /// default. Use [`HomeState::toggle_collapsed`] and [`HomeState::set_collapsed`].
    pub folds: std::collections::HashMap<String, bool>,
    /// The saved folds are to be read again with the next listing, arriving with the
    /// rows they fold.
    pub folds_owed: bool,
    /// Datasets found by walking below the working directory.
    pub search: SearchState,
    /// What earlier runs measured, by path: the index the listing was annotated from,
    /// kept so search results fill in the same way (the filter matches columns). Read
    /// once a session; what this session measures is in [`Self::enriched`].
    pub known: Known,
    /// Cloud sources found on this machine or in the config, with their buckets. Empty
    /// without cloud credentials, which is normal.
    pub cloud: Vec<CloudSource>,
    /// The catalogs shown, a section each. Set with [`HomeState::set_catalogs`], which
    /// indexes their places.
    pub catalogs: Vec<ShownCatalog>,
    /// HTTP(S) catalog files whose size was asked for this session (a HEAD).
    pub sized: std::collections::HashSet<PathBuf>,
    /// HTTP(S) catalog files a HEAD showed unavailable (missing, no server); retried on
    /// Ctrl+R.
    pub web_gone: std::collections::HashMap<PathBuf, crate::error_display::HttpGone>,
    /// Local datasets of a catalog that the last listing found missing.
    pub missing: std::collections::HashSet<PathBuf>,
    /// When the current wait for a remote listing began, for the elapsed time on screen.
    pub waiting_since: Option<std::time::Instant>,
    /// `RECENT` shows every place, for the session: `Enter` on its `… N more` row.
    pub recent_expanded: bool,
    /// Directory sections shown whole rather than cut, by directory, for the session.
    /// See [`HomeState::show_all`].
    pub shown_whole: std::collections::HashSet<PathBuf>,
    /// The listings entered from, outermost first, to restore the cursor on the way out.
    /// See [`HomeState::leave_mark`].
    pub trail: Vec<Mark>,
    /// The row the cursor returns to once the returned-to listing lands; held across
    /// listings while rows arrive, dropped when the user moves.
    pub returning: Option<RowKey>,
    /// How far down the returned-to row was, so it comes back on the same line.
    pub returning_line: Option<usize>,
    /// The cursor is where [`HomeState::select_first_entry`] put it and has not moved: a
    /// door the footers later turn down sends it to the first row.
    pub landing: bool,
    /// The rows as last listed. See [`RowsCache`].
    pub rows_cache: RowsCache,
    /// The catalogs' places. See [`HomeState::set_catalogs`].
    pub catalog_places: CatalogPlaces,
}

/// Where the cursor was in a listing the user went inside from, by row identity:
/// the listing is rebuilt on a worker and may change.
#[derive(Debug, Clone)]
pub struct Mark {
    /// The listing left: the place browsed, or `None` for the root listing.
    pub place: Option<PathBuf>,
    pub key: Option<RowKey>,
    /// The filter typed there, which entering cleared.
    pub filter: String,
    /// The search below that place, if finished or not started; a running walk is
    /// dropped on leaving and restarted.
    pub search: Option<SearchState>,
    /// Rows between the top of the list and the cursor.
    pub line: usize,
}

/// One recursive walk below the working directory. Kept apart from `sections`,
/// which rebuild often: re-walking each time would cost per keystroke.
#[derive(Debug, Clone, Default)]
pub struct SearchState {
    /// Where the walk started. `None` means no search has been asked for yet.
    pub root: Option<PathBuf>,
    /// Which walk this is, so an earlier walk's scoring is never taken for this one's.
    pub epoch: u64,
    /// Every data file found so far, unfiltered, in arrival batches; shared with the
    /// scoring worker without copying.
    pub results: Vec<std::sync::Arc<[Entry]>>,
    /// How many files `results` holds.
    pub indexed: usize,
    /// The last scored matches, possibly for an older filter or fewer files while a
    /// scoring is out.
    pub matches: Option<crate::home::search::Matches>,
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

/// Below this many files the filter is scored inline: a millisecond or two, answered
/// in the key's frame.
const SCORE_INLINE_MAX: usize = 2_000;

/// Match score per unit of frecency, up to ten units: a daily file outranks a
/// slightly better name match, never a far better one.
const FRECENCY_LIFT: f64 = 3.0;

/// What a worker needs to score the filter against a walk's files.
#[derive(Debug, Clone)]
pub struct ScoreJob {
    pub epoch: u64,
    pub results: Vec<std::sync::Arc<[Entry]>>,
    pub query: String,
    pub base: Option<crate::home::search::Matches>,
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
    fn base_for(&self, query: &str) -> (Option<&crate::home::search::Matches>, usize) {
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
            catalogs: Vec::new(),
            sized: std::collections::HashSet::new(),
            web_gone: Default::default(),
            missing: Default::default(),
            filter: String::new(),
            search_limit: crate::config::SearchConfig::default().max_results,
            rows_cache: RowsCache::default(),
            catalog_places: CatalogPlaces::default(),
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
            filter_selected: false,
            browsing: None,
            browse_start: None,
            status: None,
            network_check: is_remote_path,
            visits: Default::default(),
            newest_recent: None,
            sort: SortMode::default(),
            listing_in_flight: false,
            measure_in_flight: false,
            classifying: Default::default(),
            peeking: std::collections::HashSet::new(),
            probes: Probes::default(),
            narrowed: None,
            cloud_kinds: std::collections::HashMap::new(),
            peek_failed: std::collections::HashSet::new(),
            waiting_since: None,
            enriched: std::collections::HashMap::new(),
            unapplied: Default::default(),
            folds: std::collections::HashMap::new(),
            folds_owed: false,
            search: SearchState::default(),
            known: Default::default(),
            recent_expanded: false,
            shown_whole: Default::default(),
            trail: Vec::new(),
            returning: None,
            returning_line: None,
            landing: false,
        }
    }
}

/// The dataset index as read from the cache, by path; shared, never copied, with the
/// listing workers.
pub type Known = std::sync::Arc<std::collections::HashMap<PathBuf, crate::cache::DatasetFacts>>;

/// Everything [`build_listing`] needs, gathered on the UI thread so the worker never
/// reaches into the app.
#[derive(Debug, Clone)]
pub struct ListingRequest {
    pub recents: Vec<PathBuf>,
    pub desktop_dirs: Vec<PathBuf>,
    pub browsing: Option<PathBuf>,
    /// See [`HomeState::probes`].
    pub probes: Probes,
    /// See [`HomeState::narrowed`].
    pub narrowed: Option<Narrowed>,
    pub network_check: fn(&Path) -> bool,
    /// Cloud sources and the buckets already enumerated for them.
    pub cloud: Vec<CloudSource>,
    /// The catalogs to list.
    pub catalogs: Vec<ShownCatalog>,
    /// What earlier runs measured: a row whose size and mtime still match is filled in
    /// from here before anything is read.
    pub known: Known,
    /// The format specs on the search path: a file one reads as several variants is a
    /// place listing its variants.
    pub formats: std::sync::Arc<crate::formats::Registry>,
}

/// What a listing pass produced.
#[derive(Debug, Clone, Default)]
pub struct Listing {
    pub sections: Vec<Section>,
    /// Local datasets of a catalog that do not exist.
    pub missing: std::collections::HashSet<PathBuf>,
}

impl Listing {
    /// Fill in rows from records read since the index was: the dataset just left,
    /// whose open recorded what it found.
    pub fn learn(
        &mut self,
        learned: &[(PathBuf, crate::cache::DatasetFacts)],
        network_check: fn(&Path) -> bool,
    ) {
        if learned.is_empty() {
            return;
        }
        let learned: std::collections::HashMap<PathBuf, crate::cache::DatasetFacts> =
            learned.iter().cloned().collect();
        for row in self.sections.iter_mut().flat_map(|s| s.rows.iter_mut()) {
            if known_facts(&learned, &row.path).is_some() {
                apply_known_facts(row, &learned, network_check(&row.path));
            }
        }
    }

    /// Add each row's own path to `visits` where its canonical path has visits: recents
    /// are keyed canonically, catalogs spell paths as written (`/var/…` vs
    /// `/private/var/…` on macOS, short names on Windows). Canonicalizing touches the
    /// filesystem, so this runs on the listing worker, only for local rows named like a
    /// visited file.
    pub fn alias_visits(
        &self,
        visits: &mut std::collections::HashMap<PathBuf, crate::cache::Visits>,
    ) {
        let names: std::collections::HashSet<std::ffi::OsString> = visits
            .keys()
            .filter_map(|p| p.file_name().map(|n| n.to_os_string()))
            .collect();
        let mut aliases = Vec::new();
        for row in self.sections.iter().flat_map(|s| &s.rows) {
            let path = &row.path;
            if visits.contains_key(path)
                || row.table.is_some()
                || !path.file_name().is_some_and(|n| names.contains(n))
                || is_network_path(path)
            {
                continue;
            }
            if let Some(v) = crate::canonical::canonicalize(path)
                .ok()
                .and_then(|canonical| visits.get(&canonical))
            {
                aliases.push((path.clone(), *v));
            }
        }
        visits.extend(aliases);
    }
}

/// Find out what a row is, then what is in it, in one pass on the same filesystem
/// (`measure_row` does nothing for a plain directory), reading files as the following
/// open will: the command line passes the user's reader settings; listing passes use
/// the defaults, as a home open does.
pub fn look_into_as(entry: &Entry, as_read: &crate::formats::schema_union::ReadAs) -> Entry {
    let mut probe = classify_row(entry);
    measure_row(&mut probe, entry, as_read, None);
    probe
}

/// The first half of [`look_into_as`]: what a row nothing has looked into is.
fn classify_row(entry: &Entry) -> Entry {
    let mut probe = entry.clone();
    if probe.kind == EntryKind::Unknown && probe.path.is_dir() {
        let (kind, holds) = discover::look_at_directory(&probe.path);
        probe.kind = kind;
        probe.holds = holds;
    }
    probe
}

/// A row as far as a stat and the dataset index tell, before anything is read: its
/// size and mtime when its listing left them out, then what an earlier run recorded
/// for it at that size and mtime. Whether it is there.
fn stat_and_recall(
    probe: &mut Entry,
    known: &std::collections::HashMap<PathBuf, crate::cache::DatasetFacts>,
) -> bool {
    if !discover::unstated(probe) {
        return true;
    }
    if !discover::stat_row(probe) {
        return false;
    }
    apply_known_facts(probe, known, false);
    true
}

/// The second half of [`look_into_as`]: what is in it, from the files or from what an
/// open kept in `remembered`.
fn measure_row(
    probe: &mut Entry,
    entry: &Entry,
    as_read: &crate::formats::schema_union::ReadAs,
    remembered: Option<&crate::cache::CacheManager>,
) {
    discover::enrich_with(probe, as_read, remembered);
    probe.size = probe.size.or(entry.size);
    probe.modified = probe.modified.or(entry.modified);
}

/// How far a look into a row goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reads {
    /// Its stat, what the index recalls, and its files: a row on screen.
    Files,
    /// Its stat and what the index recalls: a row a sort by size or time needs.
    StatOnly,
}

/// Look into a batch of rows on a worker, passing each answer to `each` and caching
/// what was learned. Shared by the measure and classify passes. Every row is
/// classified before any is measured: a kind is one directory read, a count up to
/// sixty-four footers.
pub fn look_into_batch(
    rows: Vec<Entry>,
    cache: &crate::cache::CacheManager,
    known: &std::collections::HashMap<PathBuf, crate::cache::DatasetFacts>,
    reads: Reads,
    mut each: impl FnMut(PathBuf, Measured),
) {
    let as_read = crate::formats::schema_union::ReadAs::default();
    let classified: Vec<(Entry, Entry)> = rows
        .into_iter()
        .map(|entry| {
            let mut probe = entry.clone();
            // Gone since it was listed: said as measured, so it is not asked again.
            if !stat_and_recall(&mut probe, known) {
                return (probe, entry);
            }
            // A kind the index restored is not looked for again.
            let probe = classify_row(&probe);
            if probe.kind != entry.kind || probe.modified != entry.modified {
                each(entry.path.clone(), measured_from(&probe, &entry));
            }
            (probe, entry)
        })
        .collect();

    let mut facts = Vec::new();
    for (mut probe, entry) in classified {
        // What the index recalled is not read again, nor recorded again.
        if reads == Reads::Files && probe.rows.is_none() && probe.columns.is_empty() {
            measure_row(&mut probe, &entry, &as_read, Some(cache));
            facts.extend(facts_for(&probe));
        }
        each(entry.path.clone(), measured_from(&probe, &entry));
    }
    // Cache what was learned; each record carries its size and mtime and invalidates
    // itself when they change.
    cache.record_dataset_facts(&facts);
}

/// Fold a measured probe into the record kept for a row.
pub fn measured_from(probe: &Entry, original: &Entry) -> Measured {
    Measured {
        rows: probe.rows,
        cols: probe.cols,
        cols_sampled: probe.cols_sampled,
        size: probe.size.or(original.size),
        modified: probe.modified.or(original.modified),
        columns: probe.columns.clone(),
        kind: (probe.kind != original.kind).then_some(probe.kind),
        holds: probe.holds.clone(),
        // The source is resolved from the live mount table on every listing; only what the
        // file said of itself carries forward.
        cost: crate::home::discover::Cost {
            source: None,
            ..probe.cost.clone()
        },
    }
}

/// Where the listing of one network root or remote directory stands; read off the UI
/// thread (see [`HomeState::pending_probes`]).
#[derive(Debug, Clone)]
pub enum Probe {
    /// Still being read: the rows so far, in the order they came.
    Listing(Vec<Entry>),
    /// Answered. `cut_short`: the listing stopped at [`discover::MAX_ENTRIES_PER_DIR`].
    Listed {
        rows: std::sync::Arc<[Entry]>,
        cut_short: bool,
    },
    /// Did not answer, with why when the service said.
    Unreachable(Option<String>),
    /// Still out, but nothing has come for longer than a listing is waited on: the rows
    /// so far stand, the spinner stops, and Ctrl+R waits again. A page revives it.
    Silent(Vec<Entry>),
}

/// What a section says of a place whose listing stopped answering.
pub const NOT_ANSWERING: &str = "not answering · Ctrl+R retries";

/// Every probe, by the place it lists.
#[derive(Debug, Clone, Default)]
pub struct Probes(std::collections::HashMap<PathBuf, Probe>);

impl Probes {
    /// The rows of a listing that has answered.
    pub fn listed(&self, place: &Path) -> Option<&[Entry]> {
        match self.0.get(place)? {
            Probe::Listed { rows, .. } => Some(rows),
            _ => None,
        }
    }

    /// The rows read so far of a listing still going on.
    pub fn so_far(&self, place: &Path) -> Option<&[Entry]> {
        match self.0.get(place)? {
            Probe::Listing(rows) => Some(rows),
            _ => None,
        }
    }

    /// Answered, written off, or gone silent: nothing more to wait for.
    pub fn settled(&self, place: &Path) -> bool {
        matches!(
            self.0.get(place),
            Some(Probe::Listed { .. } | Probe::Unreachable(_) | Probe::Silent(_))
        )
    }

    /// Whether a listing still out has gone without news past the wait.
    pub fn silent(&self, place: &Path) -> bool {
        matches!(self.0.get(place), Some(Probe::Silent(_)))
    }

    /// The places whose listings went silent.
    pub fn silent_places(&self) -> Vec<PathBuf> {
        (self.0.iter())
            .filter(|(_, probe)| matches!(probe, Probe::Silent(_)))
            .map(|(place, _)| place.clone())
            .collect()
    }

    /// Whether pages of a listing of `place` are still news: nothing has settled it, or
    /// it only went silent.
    pub fn takes_pages(&self, place: &Path) -> bool {
        matches!(
            self.0.get(place),
            None | Some(Probe::Listing(_) | Probe::Silent(_))
        )
    }

    /// Say the listing of `place` has gone without news too long, keeping its rows.
    pub fn go_silent(&mut self, place: &Path) {
        let rows = match self.0.remove(place) {
            Some(Probe::Listing(rows) | Probe::Silent(rows)) => rows,
            None => Vec::new(),
            Some(settled) => {
                self.0.insert(place.to_path_buf(), settled);
                return;
            }
        };
        self.0.insert(place.to_path_buf(), Probe::Silent(rows));
    }

    pub fn cut_short(&self, place: &Path) -> bool {
        matches!(
            self.0.get(place),
            Some(Probe::Listed {
                cut_short: true,
                ..
            })
        )
    }

    pub fn unreachable(&self, place: &Path) -> bool {
        matches!(self.0.get(place), Some(Probe::Unreachable(_)))
    }

    /// Why the listing was refused, when the service said.
    pub fn error(&self, place: &Path) -> Option<&str> {
        match self.0.get(place)? {
            Probe::Unreachable(why) => why.as_deref(),
            _ => None,
        }
    }

    /// What the place lists now: its answer, the rows so far in the finished order
    /// (bucket directories above objects, or [`discover::sort_entries`]), or nothing.
    fn rows(&self, place: &Path) -> Vec<Entry> {
        match self.0.get(place) {
            Some(Probe::Listed { rows, .. }) => rows.to_vec(),
            Some(Probe::Listing(rows) | Probe::Silent(rows)) => {
                let mut rows = rows.clone();
                if is_object_store_url(place) {
                    rows.sort_by_key(|row| row.kind != EntryKind::Directory);
                } else {
                    discover::sort_entries(&mut rows);
                }
                rows
            }
            _ => Vec::new(),
        }
    }

    /// The places answered, with their rows.
    pub fn answered(&self) -> impl Iterator<Item = (&PathBuf, &[Entry])> {
        self.0.iter().filter_map(|(place, probe)| match probe {
            Probe::Listed { rows, .. } => Some((place, &rows[..])),
            _ => None,
        })
    }

    /// A row an answered probe produced for this exact path, if any.
    fn entry(&self, path: &Path) -> Option<Entry> {
        self.answered()
            .flat_map(|(_, rows)| rows.iter())
            .find(|e| e.path == path)
            .cloned()
    }

    /// Rows read since the last batch, while the listing is still out.
    pub fn read(&mut self, place: &Path, rows: &[Entry]) {
        let probe = (self.0)
            .entry(place.to_path_buf())
            .or_insert(Probe::Listing(Vec::new()));
        // A page from a listing that went silent: it is answering again.
        if let Probe::Silent(so_far) = probe {
            *probe = Probe::Listing(std::mem::take(so_far));
        }
        if let Probe::Listing(so_far) = probe {
            so_far.extend_from_slice(rows);
        }
    }

    pub fn insert(&mut self, place: PathBuf, probe: Probe) {
        self.0.insert(place, probe);
    }

    /// Forget a place's listing, so it is asked for again.
    pub fn forget(&mut self, place: &Path) {
        self.0.remove(place);
    }

    /// Forget a listing still being read: it was stopped.
    pub fn stopped(&mut self, place: &Path) {
        if let Some(Probe::Listing(_) | Probe::Silent(_)) = self.0.get(place) {
            self.0.remove(place);
        }
    }

    fn listed_mut(&mut self, place: &Path) -> Option<&mut std::sync::Arc<[Entry]>> {
        match self.0.get_mut(place)? {
            Probe::Listed { rows, .. } => Some(rows),
            _ => None,
        }
    }
}

/// What a cut-short cloud directory holds under one name prefix, asked of the server
/// for a typed filter.
#[derive(Debug, Clone)]
pub struct Narrowed {
    pub dir: PathBuf,
    /// The start of every name asked for (`STATION=USW`).
    pub prefix: String,
    pub rows: Vec<Entry>,
    /// These stopped at the cap too.
    pub truncated: bool,
}

/// Build the home listing. A free function so it runs on a worker: it is home's only
/// filesystem access, and a wedged mount, FIFO or failing disk blocks here, so never
/// call it from the drawing thread.
pub fn build_listing(request: &ListingRequest) -> Listing {
    let ListingRequest {
        recents,
        desktop_dirs,
        browsing,
        probes,
        narrowed,
        network_check,
        cloud,
        catalogs,
        known,
        formats,
    } = request;
    let network_check = *network_check;
    // One read of the mount table for the listing: a kernel-generated file, so it cannot
    // block on a share that stopped answering.
    let mounts = crate::home::locality::Mounts::current();
    let mut sections: Vec<Section> = Vec::new();

    // Inside a cloud source: its buckets, and nothing else.
    if let Some(id) = browsing.as_deref().and_then(cloud_source_id) {
        sections.push(source_section(&id, cloud));
        annotate(&mut sections, known, network_check, &mounts);
        return Listing {
            sections,
            ..Default::default()
        };
    }

    // Descended into a directory: show only that.
    if let Some(dir) = browsing.clone() {
        let remote = network_check(&dir);
        sections.push(browsed_section(
            &dir,
            remote,
            &Listed {
                probes,
                narrowed: narrowed.as_ref(),
                catalogs,
                known,
                formats,
            },
        ));
        annotate(&mut sections, known, network_check, &mounts);
        return Listing {
            sections,
            ..Default::default()
        };
    }

    // Recents that still exist, most recent first.
    let recent_rows: Vec<Entry> = recents
        .iter()
        // `exists()` stats, so a remote entry is trusted and dropped only if its probe says
        // it is gone.
        .filter(|p| {
            network_check(p)
                || p.exists()
                || crate::formats::members::split(p).is_some()
                || crate::formats::members::split_variant(p, formats).is_some()
                || crate::formats::hf_splits::split_place(p).is_some()
        })
        // No display cap: the store bounds it, the header counts it, and the section folds.
        .map(|p| {
            // Reuse the containing root probe's classification, so a dataset reads the same
            // under its directory and under Recent.
            if let Some(known) = probes.entry(p) {
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
            if !network_check(p) {
                discover::name_unlisted_file(&mut entry, formats);
            }
            // A dataset opened from a catalog keeps the catalog's name, not its URL's last
            // segment.
            if let Some(dataset) = catalogs
                .iter()
                .flat_map(|c| &c.datasets)
                .find(|d| d.location == *p)
            {
                entry.name = dataset.name.clone();
            }
            entry
        })
        .collect();

    // Desktop-derived places are collected rather than expanded — see below.
    let mut elsewhere: Vec<Entry> = Vec::new();

    let roots = HomeState::roots_with(desktop_dirs, network_check);
    let mut root_sections: Vec<(RootOrigin, Section)> = Vec::new();
    // Where the current-directory section is and the names it lists, for the RECENT
    // dedupe below.
    let mut cwd_listing: Option<(PathBuf, std::collections::HashSet<std::ffi::OsString>)> = None;
    for root in roots {
        // A desktop place is a directory to step into, never expanded: its contents may be
        // private (whatever was opened anywhere). Enter is the ask.
        if root.origin == RootOrigin::Desktop {
            if root.available {
                let mut entry = Entry::directory(&root.path);
                // The full place ("~/Downloads"), since the section has no path of its own.
                entry.name = display_path(&root.path);
                elsewhere.push(entry);
            }
            continue;
        }

        // A local root is scanned here (truncation is known for it); a remote one shows
        // only what its background probe returned, since scanning it can freeze datui.
        let mut truncated = false;
        let rows = if root.network {
            truncated = probes.cut_short(&root.path);
            probes.rows(&root.path)
        } else if root.available {
            let scan = discover::scan_dir_specs(&root.path, formats);
            truncated = scan.truncated;
            scan.entries
        } else {
            Vec::new()
        };
        if root.origin == RootOrigin::Cwd {
            // Compared canonically, as recents are stored; a network cwd as spelled, since
            // canonicalizing would stat a mount that may not answer.
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
        // An unreadable root stays: a dead share is what the section heading reports.
        let unreachable = root.network && probes.unreachable(&root.path);
        let silent = root.network && probes.silent(&root.path);
        let waiting = root.network && !probes.settled(&root.path);
        // Flag a network root, the one that will be slow or stop answering, by its
        // filesystem (nfs4, cifs, fuse.sshfs fail differently) when the mount table agrees
        // it is remote; otherwise just "network", rather than lose the warning.
        let described = mounts.describe(&root.path);
        let fstype = if described.network() {
            described.fstype
        } else {
            "network".to_string()
        };
        // Say when the list is a prefix: a directory cut at the cap otherwise looks like one
        // holding exactly that many.
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
                subtitle: (!state.is_empty()).then(|| crate::glyphs::dotted(&state.join(" · "))),
                origin: Some(root.origin.note()),
                root: Some(root.path.clone()),
                unavailable: !root.available || unreachable || silent,
                unavailable_note: silent.then(|| NOT_ANSWERING.to_string()),
                remote_root: root.network.then(|| root.path.clone()),
                waiting,
                ..Section::titled(display_path(&root.path), rows)
            },
        ));
    }

    // The current directory's section sits right below RECENT, so a recent it already
    // lists is dropped from RECENT. Decided by what that section contains, not by path:
    // a recent the scan did not surface (hidden, past the cap) stays under RECENT.
    // Browsing returned above with no RECENT at all.
    let recent_rows: Vec<Entry> = match &cwd_listing {
        None => recent_rows,
        Some((cwd, names)) => recent_rows
            .into_iter()
            .filter(|row| {
                let place = place_of(&row.path);
                // A local place is resolved before comparing, as the store resolves; a remote one
                // is compared as written, to avoid a stat.
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
            // Every trace of recent use lives here, the places as rows of this section.
            grouped_by_place: true,
            place_labels,
            ..Section::titled(HomeState::RECENT_SECTION, recent_rows)
        });
    }

    // Ordered by why you came: what you opened last, where you are, the object stores
    // your credentials reach (deliberate setup, unreachable by directory listing), then
    // the catalogs.
    sections.extend(root_sections.into_iter().map(|(_, s)| s));

    // One section for all cloud sources, each a row to step into; buckets are listed
    // one level down, once per session.
    sections.extend(cloud_section(cloud));

    // Catalogs in order: yours, the listed files, then the bundled one.
    let mut missing = std::collections::HashSet::new();
    for catalog in catalogs {
        let mut section = catalog_section(catalog, network_check, &mut missing);
        // A catalog's local file is named by a spec whose glob matches; nothing is read.
        for row in section.rows.iter_mut().filter(|r| {
            r.kind == EntryKind::File
                && r.format_spec.is_none()
                && r.table.is_none()
                && !network_check(&r.path)
                && !discover::is_data_file(&r.path)
        }) {
            if let Some(spec) = formats.by_glob(&row.path, false).first() {
                discover::name_spec_file(row, spec);
            }
        }
        sections.push(section);
    }

    if !elsewhere.is_empty() {
        sections.push(Section {
            // Places to look, not datasets: folded until asked for.
            folded_by_default: true,
            ..Section::titled("Elsewhere", elsewhere)
        });
    }

    // Fill in earlier measurements that still match: the listing already stat'ed every
    // row, and the mount table is read once and resolved as strings, so nothing blocks.
    annotate(&mut sections, known, network_check, &mounts);

    Listing { sections, missing }
}

/// The section of a cloud source's buckets, browsed into. Built from what the source
/// listing said, so on any thread.
fn source_section(id: &str, cloud: &[CloudSource]) -> Section {
    let source = cloud.iter().find(|s| s.id == id);
    let rows = source
        .map(|s| s.buckets.iter().map(|b| bucket_entry(b)).collect())
        .unwrap_or_default();
    let failure = source.and_then(|s| match &s.status {
        CloudStatus::Failed { short, .. } => Some(short.clone()),
        _ => None,
    });
    Section {
        subtitle: source.map(|s| s.note.clone()).filter(|n| !n.is_empty()),
        unavailable: source.is_none() || failure.is_some(),
        unavailable_note: if source.is_none() {
            Some("source not found".to_string())
        } else {
            failure
        },
        waiting: source.is_some_and(|s| s.busy()),
        ..Section::titled(
            source.map_or_else(|| id.to_string(), |s| s.label.clone()),
            rows,
        )
    }
}

/// The listing's section of cloud sources, when there are any.
fn cloud_section(cloud: &[CloudSource]) -> Option<Section> {
    (!cloud.is_empty()).then(|| {
        Section::titled(
            HomeState::CLOUD_SECTION.to_string(),
            cloud.iter().map(source_entry).collect(),
        )
    })
}

/// What a listing is built from beside the places themselves, borrowed so a
/// remote place's section can be built again on the UI thread as its pages arrive.
pub(crate) struct Listed<'a> {
    pub probes: &'a Probes,
    pub narrowed: Option<&'a Narrowed>,
    pub catalogs: &'a [ShownCatalog],
    pub known: &'a std::collections::HashMap<PathBuf, crate::cache::DatasetFacts>,
    pub formats: &'a crate::formats::Registry,
}

/// The section of a browsed directory. A `remote` one is built from its probe alone,
/// touching nothing, so it can be built on any thread; a local one is read here.
fn browsed_section(dir: &Path, remote: bool, listed: &Listed) -> Section {
    let Listed {
        probes,
        narrowed,
        catalogs,
        known,
        formats,
    } = *listed;
    let dir = dir.to_path_buf();

    // A remote directory is never read here (that freezes the UI): rows come from the
    // background probe, and the section is empty until it answers.
    // A remote listing still being read shows what it has, and says so.
    let so_far = remote && probes.so_far(&dir).is_some();
    // A SQLite database is a place too, whose rows are its tables.
    let database = !remote && dir.is_file();
    let (mut rows, truncated) = if remote {
        (probes.rows(&dir), probes.cut_short(&dir))
    } else if database {
        let tables = discover::database_rows(&dir);
        let rows = if tables.is_empty() {
            discover::variant_rows(&dir, formats)
        } else {
            tables
        };
        (rows, false)
    } else {
        let scan = discover::scan_dir_specs(&dir, formats);
        // A Hugging Face cache's splits, before the files they are made of.
        let mut rows = discover::split_rows(&dir);
        rows.extend(scan.entries);
        (rows, scan.truncated)
    };
    // A cut-short level holds the names a filter asked the server for, beside the first
    // of the rest.
    let narrowed = narrowed.filter(|n| remote && truncated && n.dir == dir);
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
    // Otherwise a directory cut at the cap looks like one holding exactly that many.
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
    let unavailable = remote && (probes.unreachable(&dir) || probes.silent(&dir));
    // The first row inside any directory opens all of it, since `Enter` below opens one
    // file: the other door.
    let mut door = (!database)
        .then(|| whole_directory_row(&dir, &rows, remote))
        .flatten();
    // What an earlier run's footers made of this directory, as for its row upstairs
    // (fingerprinted by its mtime), so a directory of separate tables reads the same in
    // both places.
    if !remote
        && let Some(door) = door.as_mut()
        && let Ok(meta) = std::fs::metadata(&dir)
    {
        door.modified = meta.modified().ok();
        apply_known_facts(door, known, false);
        door.modified = None;
        door.name = door_name(door, &rows);
    }
    // The URL without a source id (the trail names the source); an Azure account or
    // container by name, not its long URL.
    let title = {
        let text = dir.to_string_lossy();
        if let Some(dataset) = catalogs
            .iter()
            .flat_map(|c| c.datasets.iter())
            .find(|d| is_object_store_url(&d.location) && same_place(&d.location, &dir))
        {
            dataset.name.clone()
        } else if let Some((_, account)) = cloud_account(&dir) {
            account
        } else if let Some((_, container, key)) = crate::cloud::source::azure_parts(&text) {
            format!("{container}/{}", key.trim_matches('/'))
                .trim_end_matches('/')
                .to_string()
        } else {
            match crate::cloud::source::split_source_id(&text) {
                (Some(_), plain) => plain.into_owned(),
                (None, _) => display_path(&dir),
            }
        }
    };
    Section {
        subtitle,
        root: Some(dir.clone()),
        unavailable,
        // A browsed remote place that did not answer has nothing to add; a refused listing
        // says why, and one gone quiet says so.
        unavailable_note: probes
            .error(&dir)
            .map(str::to_string)
            .or_else(|| probes.silent(&dir).then(|| NOT_ANSWERING.to_string())),
        // Its wait replaces the whole list until rows arrive (`awaiting_listing`), then sits
        // on the heading; no `remote_root`.
        waiting: so_far,
        door,
        ..Section::titled(title, rows)
    }
}

/// The key a record about `path` is filed under in the dataset index. Opens record
/// under the resolved URL (`s3://bucket/x` for `s3://lab@bucket/x`, one `abfss://`
/// spelling for Azure) while recents are stored as typed; both must meet here.
pub fn index_key(path: &Path) -> PathBuf {
    let text = path.to_string_lossy();
    if let Some((account, container, key)) = crate::cloud::source::azure_parts(&text) {
        return PathBuf::from(crate::cloud::source::azure_url(&account, &container, &key));
    }
    match crate::cloud::source::split_source_id(&text) {
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

/// What the dataset index remembers each row's place to be (`bitcoin/  2 parquet`),
/// from records this build's classifier would have written, skipping `dir`. A local
/// place must still match its recorded mtime, as `apply_known_facts` requires; a
/// remote place is taken as recorded.
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
        if facts.classified_by != crate::home::discover::CLASSIFIER_VERSION {
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

/// Fill every row in with what is already known about it and where it lives. Called
/// from each of `build_listing`'s exits, so no exit's rows miss facts.
fn annotate(
    sections: &mut [Section],
    known: &std::collections::HashMap<PathBuf, crate::cache::DatasetFacts>,
    network_check: fn(&Path) -> bool,
    mounts: &crate::home::locality::Mounts,
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
        // The door reads where its directory is (the local glyph on local disk).
        if let Some(door) = section.door.as_mut()
            && !is_cloud_place(&door.path)
        {
            door.cost.source = Some(mounts.describe(&door.path).fstype);
        }
    }
}

/// What of a row decides where the list puts it, or whether it lists it at all: a
/// measurement that changes none of this leaves the list as built.
#[derive(PartialEq)]
struct Standing {
    hidden: bool,
    dataset: bool,
    /// The value the sort orders by, when it orders by something measured.
    sorted_by: Option<u64>,
}

impl Standing {
    fn of(row: &Entry, sort: SortMode) -> Self {
        Standing {
            hidden: row.hidden_by_default(),
            dataset: row.kind.is_dataset() || row.kind.is_lake_table(),
            sorted_by: match sort {
                SortMode::Size => Some(row.size.unwrap_or(0)),
                SortMode::Rows => Some(row.rows.unwrap_or(0) as u64),
                SortMode::Modified => Some(
                    (row.modified)
                        .and_then(|m| m.duration_since(std::time::UNIX_EPOCH).ok())
                        .map_or(0, |d| d.as_secs()),
                ),
                SortMode::Natural => None,
            },
        }
    }
}

/// Write a measurement into its row.
fn fold_measured(row: &mut Entry, m: &Measured) {
    row.measured = true;
    row.rows = m.rows;
    row.cols = m.cols;
    row.cols_sampled = m.cols_sampled;
    if let Some(kind) = m.kind {
        row.kind = kind;
    }
    if m.size.is_some() {
        row.size = m.size;
    }
    if m.modified.is_some() {
        row.modified = m.modified;
    }
    if !m.columns.is_empty() && row.columns != m.columns {
        row.columns.clone_from(&m.columns);
    }
    if !m.holds.is_empty() && row.holds != m.holds {
        row.holds.clone_from(&m.holds);
    }
    // Keep the source (from the mount table); take everything else (from the file).
    take_cost(row, &m.cost);
}

/// Take a measured or remembered row cost, keeping what came from elsewhere: its
/// location (mount table) and a spec file's variant count (an older record lacks
/// it, which would leave no tables to list).
fn take_cost(row: &mut Entry, cost: &discover::Cost) {
    let source = row.cost.source.take();
    let variants = row.cost.tables.filter(|_| row.format_spec.is_some());
    row.cost = cost.clone();
    row.cost.source = source;
    if variants.is_some() {
        row.cost.tables = variants;
    }
}

/// Apply a cached measurement to a row. A local row must still match its size and
/// mtime (the listing already stat'ed it). A remote row cannot be stat'ed safely, so
/// its cached facts are used as is: a stale count beats none for hard-to-reach data.
fn apply_known_facts(
    row: &mut Entry,
    known: &std::collections::HashMap<PathBuf, crate::cache::DatasetFacts>,
    remote: bool,
) {
    let Some(facts) = known_facts(known, &row.path) else {
        return;
    };

    // What an earlier run found this directory to be: listings no longer read
    // directories, so rows arrive `Unknown`. Only directories a run measured (with a
    // size) have a record; what it saves is re-reading footers that found separate
    // tables. Checked before the file fingerprint below, which never matches a
    // directory: the directory's mtime is its fingerprint, moving as files come and go.
    // Gated on the classifier version, since `is_one_table` is version-sensitive.
    if !remote
        && matches!(row.kind, EntryKind::Unknown | EntryKind::MultiFile)
        && facts.classified_by == crate::home::discover::CLASSIFIER_VERSION
        && let Some(kind) = facts.kind
    {
        let same_mtime = row
            .modified
            .and_then(|m| m.duration_since(std::time::UNIX_EPOCH).ok())
            .is_some_and(|d| d.as_secs() == facts.mtime);
        if same_mtime {
            row.kind = kind;
            // The holdings come back with the kind: a row given its kind from the cache is
            // never looked into again, so a missing count would stay missing all session.
            if row.holds.is_empty() {
                row.holds = facts.holds.clone();
            }
            // Only kind and count: both come from the directory's names, which its mtime
            // fingerprints. Footer facts (width, size, columns) can change by a file rewritten
            // in place, which a directory mtime cannot see, so they are not restored.
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
    // The source comes from the live mount table afterwards; only what the file said of
    // itself is restored.
    take_cost(row, &facts.cost);
    if remote {
        // A remote row was never stat'ed, so these are all it has; a record with no size
        // (one object's footer read) gives none rather than zero.
        if facts.size > 0 {
            row.size = row.size.or(Some(facts.size));
        }
        // What it was last seen to be, not what its name suggests, so a dataset reads the
        // same in every section. Only from this classifier version: an older record could
        // call a Delta root `multifile`.
        if row.kind == EntryKind::Unknown
            && facts.classified_by == crate::home::discover::CLASSIFIER_VERSION
            && let Some(kind) = facts.kind
        {
            row.kind = kind;
            // The holdings come back with the kind, or the row would say `dir` all session.
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
    // Where it lives is not learned: it is never recorded.
    let cost = crate::home::discover::Cost {
        source: None,
        ..entry.cost.clone()
    };
    if entry.rows.is_none() && entry.columns.is_empty() && cost == Default::default() {
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
            classified_by: crate::home::discover::CLASSIFIER_VERSION,
            // The source is where it is now: paths move between mounts.
            cost: crate::home::discover::Cost {
                source: None,
                ..entry.cost.clone()
            },
        },
    ))
}

/// How well an entry answers the filter, by name or by column (the footer's column
/// names: "which has a `customer_id`?"). A name match always outranks a column
/// match. Higher is better, as in fzf; see [`crate::home::fuzzy`].
pub fn match_score(filter: &str, entry: &Entry) -> Option<i32> {
    match crate::home::fuzzy::best_match(filter, &entry.name) {
        Some(m) => Some(m.score),
        // Below every name match; a column match is a substring test with no score of its
        // own.
        None => matching_column(filter, entry).map(|_| -COLUMN_MATCH_PENALTY),
    }
}

/// [`match_score`], with what the row is marked by when drawn.
pub fn match_hit(filter: &str, entry: &Entry) -> Option<Hit> {
    Needle::new(filter).hit(entry)
}

/// A filter prepared once for scoring a whole listing: lowered once, not once per row
/// and column.
struct Needle<'a> {
    filter: &'a str,
    lower: String,
}

impl<'a> Needle<'a> {
    fn new(filter: &'a str) -> Self {
        Needle {
            filter,
            lower: filter.to_lowercase(),
        }
    }

    fn hit(&self, entry: &Entry) -> Option<Hit> {
        let named = crate::home::fuzzy::best_match_with(self.filter, &entry.name, |score, _| Hit {
            score,
            column: None,
        });
        if named.is_some() {
            return named;
        }
        Some(Hit {
            score: -COLUMN_MATCH_PENALTY,
            column: Some(self.column(entry)?),
        })
    }

    /// The first column of `entry` containing the filter, case-insensitively.
    fn column(&self, entry: &Entry) -> Option<usize> {
        if self.filter.is_empty() {
            return None;
        }
        (entry.columns.iter()).position(|c| contains_folded(c, &self.lower))
    }
}

/// Whether `haystack` contains `lower` (already lowercase), ignoring case; without
/// allocating when both are ASCII.
fn contains_folded(haystack: &str, lower: &str) -> bool {
    if haystack.is_ascii() && lower.is_ascii() {
        let (hay, needle) = (haystack.as_bytes(), lower.as_bytes());
        return needle.is_empty()
            || hay
                .windows(needle.len())
                .any(|w| w.eq_ignore_ascii_case(needle));
    }
    haystack.to_lowercase().contains(lower)
}

/// How far a column match sits below any name match: more than any name score, so
/// they never interleave.
const COLUMN_MATCH_PENALTY: i32 = 1_000_000;

/// The first column of `entry` containing `filter`, case-insensitively. Substring,
/// not subsequence: fuzzy matching dozens of names matches nearly everything.
pub fn matching_column<'a>(filter: &str, entry: &'a Entry) -> Option<&'a str> {
    (Needle::new(filter).column(entry)).map(|i| entry.columns[i].as_str())
}

/// Character positions in `haystack` that `needle` matched, from the same alignment
/// that scored it.
pub fn fuzzy_positions(needle: &str, haystack: &str) -> Vec<usize> {
    crate::home::fuzzy::best_match(needle, haystack)
        .map(|m| m.positions)
        .unwrap_or_default()
}

/// Character positions of the first case-insensitive occurrence of `needle`, since
/// column matching is a substring test.
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

/// Whether and how well `needle` matches `haystack` (higher is better), through
/// [`crate::home::fuzzy::best_match`] like every ranking and highlight.
pub fn fuzzy_score(needle: &str, haystack: &str) -> Option<i32> {
    crate::home::fuzzy::best_match(needle, haystack).map(|m| m.score)
}

impl HomeState {
    /// Roots from the working directory, then the desktop's data directories;
    /// duplicates collapse to the first. Recents' places are `RECENT` rows, not roots.
    pub fn roots(desktop_dirs: &[PathBuf]) -> Vec<Root> {
        Self::roots_with(desktop_dirs, is_remote_path)
    }

    /// As [`HomeState::roots`], with the network test injected.
    pub fn roots_with(desktop_dirs: &[PathBuf], is_network: fn(&Path) -> bool) -> Vec<Root> {
        let mut roots: Vec<Root> = Vec::new();
        let mut seen: Vec<PathBuf> = Vec::new();

        let push =
            |path: PathBuf, origin: RootOrigin, roots: &mut Vec<Root>, seen: &mut Vec<PathBuf>| {
                // The network test reads only the mount table, safe on a blocking path.
                let network = is_network(&path);

                // Canonicalizing and listing touch the filesystem and block on a dead NFS share
                // (indefinitely on a `hard` mount): a remote root is taken as is and probed in the
                // background.
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

        // Last, and weakest: places the desktop says you have opened data from. Only
        // useful before datui has recents of its own.
        for dir in desktop_dirs {
            push(dir.clone(), RootOrigin::Desktop, &mut roots, &mut seen);
        }

        roots
    }

    /// Build the home listing: recents first (on a mount, the dataset you want was
    /// usually opened before), then roots scanned one level deep.
    pub fn rebuild(&mut self, recents: &[PathBuf]) {
        self.rebuild_with(recents, &[])
    }

    /// As [`HomeState::rebuild`], plus directories derived from the desktop's own
    /// recently-used list.
    pub fn rebuild_with(&mut self, recents: &[PathBuf], desktop_dirs: &[PathBuf]) {
        let request = ListingRequest {
            recents: recents.to_vec(),
            desktop_dirs: desktop_dirs.to_vec(),
            browsing: self.browsing.clone(),
            probes: self.probes.clone(),
            narrowed: self.narrowed.clone(),
            network_check: self.network_check,
            cloud: self.cloud.clone(),
            catalogs: self.catalogs.clone(),
            // The synchronous path (tests, library callers) uses no cache: exactly what is on
            // disk now.
            known: Default::default(),
            formats: self.formats.clone(),
        };
        let listing = build_listing(&request);
        self.apply_listing(listing);
    }

    /// Install a listing built elsewhere, keeping the cursor on whatever it was on.
    pub fn apply_listing(&mut self, listing: Listing) {
        let mut listing = listing;
        for section in &mut listing.sections {
            name_by_spec(&self.formats, &mut section.rows);
        }
        self.missing = listing.missing;
        self.replace_sections(|home| home.sections = listing.sections);
    }

    /// Build again, from its probe alone, the section showing the remote place `root`:
    /// the browsed directory or a network root. Pages of a listing land several times a
    /// second; this touches no file and leaves every other section as it is.
    pub fn relist_remote(&mut self, root: &Path) {
        let browsed = self.browsing.as_deref() == Some(root)
            && (self.network_check)(root)
            && cloud_source_id(root).is_none();
        let at = (self.sections.iter()).position(|s| {
            s.root.as_deref() == Some(root) && (browsed || s.remote_root.as_deref() == Some(root))
        });
        let Some(at) = at else {
            return;
        };
        let mut section = if browsed {
            browsed_section(
                root,
                true,
                &Listed {
                    probes: &self.probes,
                    narrowed: self.narrowed.as_ref(),
                    catalogs: &self.catalogs,
                    known: &self.known,
                    formats: &self.formats,
                },
            )
        } else {
            Section::titled("", self.probes.rows(root))
        };
        let mounts = crate::home::locality::Mounts::cached();
        annotate(
            std::slice::from_mut(&mut section),
            &self.known,
            self.network_check,
            &mounts,
        );
        name_by_spec(&self.formats, &mut section.rows);
        let silent = self.probes.silent(root);
        let waiting = !self.probes.settled(root);
        self.replace_sections(|home| {
            if browsed {
                home.sections[at] = section;
                return;
            }
            // A root keeps its heading; its rows and its wait are the probe's.
            let shown = &mut home.sections[at];
            shown.rows = section.rows;
            shown.waiting = waiting;
            if silent {
                shown.unavailable = true;
                shown.unavailable_note = Some(NOT_ANSWERING.to_string());
            }
        });
    }

    /// Show `cloud` as the cloud sources: the Cloud section, or the browsed source's
    /// buckets, built again in place. Nothing else changes, so nothing is read.
    pub fn set_cloud(&mut self, cloud: Vec<CloudSource>) {
        self.cloud = cloud;
        let mounts = crate::home::locality::Mounts::cached();
        let annotated = |home: &Self, mut section: Section| {
            annotate(
                std::slice::from_mut(&mut section),
                &home.known,
                home.network_check,
                &mounts,
            );
            section
        };
        match self.browsing.as_deref().map(cloud_source_id) {
            Some(Some(id)) => {
                let section = annotated(self, source_section(&id, &self.cloud));
                self.replace_sections(|home| home.sections = vec![section]);
            }
            Some(None) => {}
            None => {
                let section = cloud_section(&self.cloud).map(|s| annotated(self, s));
                self.replace_sections(|home| {
                    let at = (home.sections.iter()).position(|s| s.title == Self::CLOUD_SECTION);
                    match (at, section) {
                        (Some(at), Some(section)) => home.sections[at] = section,
                        (Some(at), None) => {
                            home.sections.remove(at);
                        }
                        // Below the places, above the catalogs, as a listing puts it.
                        (None, Some(section)) => {
                            let at = (home.sections.iter())
                                .position(|s| {
                                    s.origin.is_some_and(is_catalog_origin)
                                        || s.title == "Elsewhere"
                                        || s.title == Self::SEARCH_SECTION
                                })
                                .unwrap_or(home.sections.len());
                            home.sections.insert(at, section);
                        }
                        (None, None) => {}
                    }
                });
            }
        }
    }

    /// Change the sections through `change`, then put back what rides on them: the search
    /// results, this session's measurements, and the cursor on whatever it was on.
    fn replace_sections(&mut self, change: impl FnOnce(&mut Self)) {
        let returning = self.returning.take();
        let previous = returning.clone().or_else(|| self.selected_key());
        // Rows landing above the cursor move the list, not the cursor.
        let line = self.selected.saturating_sub(self.scroll);
        change(self);
        self.changed();
        // Browsing, the first section is the directory browsed.
        if let (Some(browsing), Some((dir, format))) = (&self.browsing, &self.lake_here)
            && browsing == dir
            && let Some(section) = self.sections.first_mut()
        {
            let note = crate::glyphs::dotted(&format!(
                "{} · not read as a table",
                format.to_ascii_lowercase()
            ));
            section.subtitle = Some(match section.subtitle.take() {
                Some(state) => crate::glyphs::dotted(&format!("{note} · {state}")),
                None => note,
            });
        }
        // Search results outlive rebuilds (they came from a walk): put them back.
        self.sync_search_section();
        // So does what this session looked into: rebuilds read names cheaply, and kinds and
        // counts cost round trips.
        self.apply_measurements();

        // Keep the cursor on its row across refreshes, not back at the top on every result.
        let placed = self.reselect(previous);
        // A returned-to row is where the user left it, not a landing a late footer may
        // move.
        if placed && returning.is_some() {
            self.landing = false;
        }
        if !placed {
            self.select_first_entry();
            // The returned-to row may be in a later listing (a remote place still answering, a
            // search still walking).
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

    /// Remember the cursor before going inside something, so leaving returns to it. Call
    /// before `browsing` changes.
    pub fn leave_mark(&mut self) {
        let mark = Mark {
            place: self.browsing.clone(),
            key: self.selected_key(),
            filter: self.filter.clone(),
            // A scoring out now would answer while away and be dropped; the kept copy asks
            // again on return.
            search: (!self.search.running).then(|| SearchState {
                scoring: false,
                ..self.search.clone()
            }),
            line: self.selected.saturating_sub(self.scroll),
        };
        // A place already on the trail is being re-entered from elsewhere; its old mark is
        // over.
        self.trail.retain(|m| m.place != mark.place);
        self.trail.push(mark);
    }

    /// Return to the place now browsed, from `from`: its filter, search, and the cursor
    /// on its row once the listing lands. Call after `browsing` is set. A place never
    /// entered from (Backspace above the browse start) puts the cursor on the place just
    /// left.
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
        if let Some(idx) = self.place_key(&key) {
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
            Row::Up { section } => RowKey::Up(title(section)?),
            Row::Entry { entry, .. } => RowKey::Entry(entry.path.clone()),
            Row::Door { entry, .. } => RowKey::Door(entry.path.clone()),
            Row::Place { path, .. } => RowKey::Place(path),
        })
    }

    /// Put the cursor back on the row `key` names, if still listed; true if placed. A
    /// row the cap now hides counts as placed on its `more` row. Otherwise the cursor
    /// is clamped so it never sits past the end.
    pub fn reselect(&mut self, key: Option<RowKey>) -> bool {
        let Some(key) = key else {
            self.clamp_selection();
            return false;
        };
        match self.place_key(&key) {
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

    /// Where the row `key` names is on screen. A row a directory's cut hides makes that
    /// directory show whole; a row `RECENT`'s cap hides answers with its `more` row.
    fn place_key(&mut self, key: &RowKey) -> Option<usize> {
        if let Some(found) = self.listed(key) {
            return Some(found);
        }
        if let RowKey::Entry(path) = key
            && let Some(root) = self
                .sections
                .iter()
                .find(|s| !s.grouped_by_place && s.rows.iter().any(|r| r.path == *path))
                .and_then(|s| s.root.clone())
            && self.shown_whole.insert(root.clone())
        {
            match self.listed(key) {
                Some(found) => return Some(found),
                None => {
                    self.shown_whole.remove(&root);
                }
            }
        }
        self.behind_recent_cap(key)
    }

    /// Where the row `key` names is on screen, as it is listed now.
    fn listed(&self, key: &RowKey) -> Option<usize> {
        self.position(|row| match (row, key) {
            (Row::Entry { entry, .. }, RowKey::Entry(path)) => entry.path == *path,
            (Row::Door { entry, .. }, RowKey::Door(path)) => entry.path == *path,
            (Row::Place { path, .. }, RowKey::Place(wanted)) => path == wanted,
            (Row::Header { section, .. }, RowKey::Header(title))
            | (Row::More { section, .. }, RowKey::More(title))
            | (Row::Hidden { section, .. }, RowKey::Hidden(title))
            | (Row::Up { section }, RowKey::Up(title)) => self
                .sections
                .get(*section)
                .is_some_and(|s| s.title == *title),
            _ => false,
        })
    }

    /// The `more` row of `RECENT` when its cap hides the row `key` names.
    fn behind_recent_cap(&self, key: &RowKey) -> Option<usize> {
        let (RowKey::Entry(path) | RowKey::Place(path)) = key else {
            return None;
        };
        self.position(|row| {
            matches!(row, Row::More { section, .. }
            if self.sections.get(*section).is_some_and(|s| {
                s.grouped_by_place
                    && s.rows
                        .iter()
                        .any(|r| r.path == *path || place_of(&r.path) == *path)
            }))
        })
    }

    /// Tell the listing the list's height, keeping the cursor on its row: `RECENT`'s cap
    /// is a share of the height. Called every frame; only a change does work.
    pub fn set_view_height(&mut self, height: usize) {
        if height == self.view_height {
            return;
        }
        let key = self.selected_key();
        self.view_height = height;
        self.reselect(key);
    }

    /// Settle the viewport now, as the next frame will: the look-into pass is asked for
    /// when a listing lands, before that frame, and a stale `scroll` from the replaced
    /// listing would spend the batch on rows nobody sees.
    fn follow_selection(&mut self) {
        let rows = self.row_count();
        self.scroll = settle_top(self.scroll, self.selected, self.view_height, rows);
    }

    /// Whether a section is folded: the user's last choice, else its default. Never
    /// while browsing: the browsed listing is the whole screen, and a fold remembered
    /// for a section once titled by that path must not hide it.
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

    /// Fold or unfold `section`, remembered by title. Nothing is remembered while
    /// browsing: the browsed listing never folds, and its path is also a root section's
    /// title.
    pub fn set_collapsed(&mut self, section: usize, collapsed: bool) {
        if self.browsing.is_some() {
            return;
        }
        let Some(title) = self.sections.get(section).map(|s| s.title.clone()) else {
            return;
        };
        self.folds.insert(title, collapsed);
    }

    /// Move the cursor to the next (`delta` > 0) or previous section header, wrapping.
    pub fn jump_section(&mut self, delta: isize) {
        self.returning = None;
        self.landing = false;
        let headers = self.view().headers.clone();
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

    /// Whether the listing holds anywhere to go, folded or not: datasets, lake tables
    /// (not readable as tables, but places), and rows not yet looked into, which may be
    /// datasets. Hence [`EntryKind::is_dataset`], not [`EntryKind::is_known_dataset`].
    pub fn has_any_dataset(&self) -> bool {
        self.view().has_dataset
    }

    /// Title of the section of recursive search results; fold state is keyed by title.
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
        if let Some((account, _, _)) = crate::cloud::source::azure_parts(&text) {
            return self.azure_account_place(&account).and_then(|place| {
                cloud_account(&place).and_then(|(id, _)| self.cloud.iter().find(|s| s.id == id))
            });
        }
        if let (Some(id), _) = crate::cloud::source::split_source_id(&text) {
            return self.cloud.iter().find(|s| s.id == id);
        }
        if let Some(project) =
            Self::google_bucket_root(path).and_then(|b| self.project_of_bucket(&b))
        {
            return cloud_account(&project)
                .and_then(|(id, _)| self.cloud.iter().find(|s| s.id == id));
        }
        let (_, plain) = crate::cloud::source::split_source_id(&text);
        let (scheme, rest) = plain.split_once("://")?;
        let bucket = rest.split('/').next()?;
        let root = PathBuf::from(format!("{scheme}://{bucket}"));
        self.cloud.iter().find(|s| s.buckets.contains(&root))
    }

    /// One level up from `path`. A bucket's parent is its source, so Backspace from a
    /// bucket returns to the source.
    pub fn parent_of(&self, path: &Path) -> Option<PathBuf> {
        if cloud_source_id(path).is_some() {
            return None;
        }
        // Out of a remote dataset's root goes back to its catalog, not into a bucket that
        // may not be listable.
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
        if let Some((account, container, key)) = crate::cloud::source::azure_parts(&text) {
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
            return Some(PathBuf::from(crate::cloud::source::azure_url(
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
        if let Some((account, container, key)) = crate::cloud::source::azure_parts(&text) {
            let key = key.trim_matches('/');
            let up = key.rsplit_once('/').map(|(up, _)| up).unwrap_or("");
            let up = if up.is_empty() {
                String::new()
            } else {
                format!("{up}/")
            };
            return Some(PathBuf::from(crate::cloud::source::azure_url(
                &account, &container, &up,
            )));
        }
        parent_location(path)
    }

    /// The remote catalog dataset `path` is in: the innermost, or the first listed of two
    /// at the same place.
    fn remote_dataset_of(&self, path: &Path) -> Option<(&ShownCatalog, &ShownDataset)> {
        if !is_object_store_url(path) {
            return None;
        }
        let text = path.to_string_lossy();
        self.catalogs
            .iter()
            .flat_map(|c| c.datasets.iter().map(move |d| (c, d)))
            .filter(|(_, d)| {
                is_object_store_url(&d.location) && within(&text, &d.location.to_string_lossy())
            })
            .rev()
            .max_by_key(|(_, d)| d.location.to_string_lossy().trim_end_matches('/').len())
    }

    /// The catalog dataset listed at `path` itself.
    pub fn catalog_dataset(&self, path: &Path) -> Option<(&ShownCatalog, &ShownDataset)> {
        let places = &self.catalog_places;
        if places.indexes(&self.catalogs) {
            let key = place_key(path);
            let &(c, d) = places.datasets.get(&key)?;
            let catalog = self.catalogs.get(c)?;
            if let Some(dataset) = catalog.datasets.get(d)
                && place_key(&dataset.location) == key
            {
                return Some((catalog, dataset));
            }
        }
        self.catalogs
            .iter()
            .flat_map(|c| c.datasets.iter().map(move |d| (c, d)))
            .find(|(_, d)| same_place(&d.location, path))
    }

    /// The dataset a bookmark is listed under, and the bookmark's name.
    pub fn bookmark(&self, path: &Path) -> Option<(&ShownDataset, &str)> {
        let places = &self.catalog_places;
        if places.indexes(&self.catalogs) {
            let key = place_key(path);
            let &(c, d, b) = places.bookmarks.get(&key)?;
            if let Some(dataset) = self.catalogs.get(c).and_then(|c| c.datasets.get(d))
                && let Some((name, place)) = dataset.bookmarks.get(b)
                && place_key(place) == key
            {
                return Some((dataset, name.as_str()));
            }
        }
        self.catalogs
            .iter()
            .flat_map(|c| c.datasets.iter())
            .find_map(|d| {
                d.bookmarks
                    .iter()
                    .find(|(_, place)| same_place(place, path))
                    .map(|(name, _)| (d, name.as_str()))
            })
    }

    /// What a catalog says an unmeasured HTTP(S) file weighs, shown as `~33 MB`.
    pub fn size_hint(&self, path: &Path) -> Option<u64> {
        self.catalog_dataset(path).and_then(|(_, d)| d.size)
    }

    /// The place of an Azure account, from whichever source lists it.
    fn azure_account_place(&self, account: &str) -> Option<PathBuf> {
        self.cloud
            .iter()
            .flat_map(|s| s.buckets.iter())
            .find(|place| cloud_account(place).is_some_and(|(_, a)| a == account))
            .cloned()
    }

    /// How far an object-store directory is in being looked into, while its row has no
    /// label. `None` once labeled, or for other rows.
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
            // Answered, but the row is from a listing being rebuilt: `dir` meanwhile would claim
            // no data.
            Some((kind, holds)) if *kind != EntryKind::Directory || !holds.is_empty() => {
                Some(CloudLook::Looking)
            }
            Some(_) => None,
        }
    }

    /// What a source or catalog calls a place: a catalog's remote dataset, or `missing`
    /// for an absent local one.
    pub fn place_kind(&self, path: &Path) -> Option<&'static str> {
        if self.missing.contains(path) {
            return Some("missing");
        }
        if let Some((id, _)) = cloud_account(path) {
            return self
                .cloud
                .iter()
                .any(|s| s.id == id && s.api == crate::cloud::source::ProviderKind::Gcs)
                .then_some("project");
        }
        // A bookmark inside a catalog dataset opens whole, as a dataset does.
        (is_object_store_url(path)
            && (self
                .catalog_dataset(path)
                .is_some_and(|(_, d)| is_object_store_url(&d.location))
                || (self.browsing.is_none() && self.bookmark(path).is_some())))
        .then_some("dataset")
    }

    /// The project place a Google bucket was listed under, when it was.
    fn project_of_bucket(&self, bucket_root: &Path) -> Option<PathBuf> {
        let root = bucket_root.to_string_lossy();
        let root = root.trim_end_matches('/');
        self.probes
            .answered()
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

    /// Details-pane lines for a place a cloud source listed or a catalog names, when
    /// it has any.
    pub fn place_details(&self, path: &Path) -> Option<&[(String, String)]> {
        self.cloud
            .iter()
            .find_map(|s| s.place_details.get(path))
            .or_else(|| self.catalog_dataset(path).map(|(_, d)| &d.details))
            .or_else(|| self.bookmark(path).map(|(d, _)| &d.details))
            .map(Vec::as_slice)
    }

    /// The location as the title bar names it; cloud places read as a trail through the
    /// source's label.
    pub fn location_label(&self, path: &Path) -> String {
        let sep = crate::glyphs::get().trail;
        // Inside a catalog's remote dataset: the catalog, the dataset's name, and the path
        // below it.
        if let Some((catalog, dataset)) = self.remote_dataset_of(path) {
            let text = path.to_string_lossy();
            let rest = within_rest(&text, &dataset.location.to_string_lossy());
            let mut parts = vec![catalog.label.clone(), dataset.name.clone()];
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
            } else if let Some((account, container, key)) = crate::cloud::source::azure_parts(&text)
            {
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
                let (_, plain) = crate::cloud::source::split_source_id(&text);
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

    /// Put the search results into `sections`, or take them out, after every rebuild and
    /// batch. Only while there is a filter: unfiltered, everything matches.
    pub fn sync_search_section(&mut self) {
        // The other sections keep their hits: results land in batches while the user types.
        let found = (self.sections.iter()).position(|s| s.title == Self::SEARCH_SECTION);
        let mut kept = self.take_hits();
        self.sections.retain(|s| s.title != Self::SEARCH_SECTION);
        self.changed();
        if let (Some(hits), Some(at)) = (kept.as_mut(), found)
            && at < hits.sections.len()
        {
            hits.sections.remove(at);
        }
        *self.rows_cache.rescored.get_mut() = kept;

        if self.filter.is_empty() {
            return;
        }
        // Bucket names already listed; nothing is fetched (that would bill per keystroke).
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
                        entry.cost.source = Some(source.api.name().to_string());
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
                let subtitle =
                    crate::glyphs::dotted(&format!("cloud · {} names", cloud_rows.len()));
                self.sections.push(Section {
                    subtitle: Some(subtitle),
                    ..Section::titled(Self::SEARCH_SECTION, cloud_rows)
                });
            }
            return;
        }

        // The last scored matches; those for an older filter (a scoring out) are rescored
        // here once, so nothing stale shows.
        let matches = self.search.matches.as_ref();
        // A dataset already listed under its directory does not appear again under the
        // search. Only rows named as a match can be one: the rest of thousands are passed
        // over on a short name, not a path hashed component by component.
        let names: std::collections::HashSet<&std::ffi::OsStr> = (matches.iter())
            .flat_map(|m| m.top.iter())
            .map(|e| e.path.file_name().unwrap_or(e.path.as_os_str()))
            .collect();
        let listed: std::collections::HashSet<&PathBuf> = (self.sections.iter())
            .flat_map(|s| s.rows.iter().map(|r| &r.path))
            .filter(|path| path.file_name().is_none_or(|name| names.contains(name)))
            .collect();
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
        let mut rows: Vec<Entry> = kept.into_iter().map(|(e, _)| e.clone()).collect();
        rows.extend(cloud_rows);
        // The walk's rows are snapshots: what this session measured of them is folded in
        // here, as nothing measures them again.
        for row in &mut rows {
            if let Some(m) = self.enriched.get(&row.path) {
                fold_measured(row, m);
            }
        }

        // Say an empty result when the walk stopped short: "no match" may be wrong then.
        let partial = self.search.limited.is_some();
        let scored = self.search.scored_for(&self.filter);
        if rows.is_empty() && !self.search.running && !partial && scored {
            return;
        }

        let subtitle = self.found_subtitle(rows.is_empty());

        self.sections.push(Section {
            subtitle: Some(subtitle),
            ..Section::titled(Self::SEARCH_SECTION, rows)
        });
    }

    /// `Found`'s rule: where it looked, how many matched, how far the walk got.
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
        // How many matched when more matched than listed; last, since the rule cuts long
        // notes from the start.
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
        // A batch from a walk the user left is dropped: walks are abandoned, not cancelled.
        if self.search.root.as_deref() != Some(root) {
            return;
        }
        // Earlier measurements give found rows their shape and columns (the filter matches
        // columns), strictly fingerprinted by size and mtime.
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
        // Carry matches forward over the new files alone: rescoring everything per batch
        // delayed typed keys during large walks.
        let limit = self.search_limit;
        let changed = match self.search.matches.as_mut() {
            Some(m) if m.query == self.filter && m.upto == start => m.extend(&batch, start, limit),
            _ => {
                let before = self.search.matches.as_ref().map(|m| m.upto);
                self.score_search_inline();
                self.search.matches.as_ref().map(|m| m.upto) != before
            }
        };
        // Rebuild `Found` only when its contents changed; otherwise only its progress moves.
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
            crate::home::search::score(&self.search.results, &self.filter, base, self.search_limit);
        self.search.matches = Some(scored);
    }

    /// The scoring a worker should do next, if owed (the filter changed, or files
    /// arrived). One at a time; the next is asked when it answers.
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
    pub fn search_scored(&mut self, epoch: u64, scored: crate::home::search::Matches) {
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
        // If typing left the cursor on nothing, the first match is where it belongs.
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

    /// Lines on screen: a header per non-empty section, then its matching rows unless
    /// folded. Results stay grouped while filtering, to show where a dataset lives.
    pub fn visible(&self) -> Vec<Row<'_>> {
        let view = self.view();
        view.slots.iter().map(|slot| self.row(slot)).collect()
    }

    /// Where each row of [`HomeState::visible`] falls in the list, `spaced` with a
    /// blank line before every header but the first; from the headers alone, so a frame
    /// costs the rows it shows, not the rows listed.
    pub fn list_lines(&self, spaced: bool) -> ListLines {
        let view = self.view();
        ListLines {
            headers: view.headers.clone(),
            rows: view.slots.len(),
            spaced,
        }
    }

    /// How many rows the filter matches: the sum of the section headers' counts.
    pub fn matched(&self) -> usize {
        let view = self.view();
        (view.headers.iter())
            .map(|&i| match &view.slots[i] {
                Slot::Plain(Row::Header { matches, .. }) => *matches,
                _ => 0,
            })
            .sum()
    }

    /// The first row of [`HomeState::visible`] that `wanted` picks, without building
    /// the rest.
    pub fn position(&self, mut wanted: impl FnMut(&Row<'_>) -> bool) -> Option<usize> {
        let view = self.view();
        view.slots.iter().position(|slot| wanted(&self.row(slot)))
    }

    /// How many rows [`HomeState::visible`] lists.
    pub fn row_count(&self) -> usize {
        self.view().slots.len()
    }

    /// Row `index` of [`HomeState::visible`].
    pub fn row_at(&self, index: usize) -> Option<Row<'_>> {
        self.view().slots.get(index).map(|slot| self.row(slot))
    }

    /// How many times the rows were built, for the test that a frame builds them at most
    /// once.
    pub fn rows_built(&self) -> usize {
        self.rows_cache.builds.get()
    }

    /// The rows have changed under the cache: built again on the next read.
    fn changed(&mut self) {
        *self.rows_cache.built.get_mut() = None;
        *self.rows_cache.rescored.get_mut() = None;
    }

    /// The hits the cache holds for the rows as listed now, for the filter as it is;
    /// the rows are built again on the next read.
    fn take_hits(&mut self) -> Option<Hits> {
        let built = self.rows_cache.built.get_mut().take();
        match self.rows_cache.rescored.get_mut().take() {
            Some(hits) => Some(hits),
            None => built
                .filter(|view| view.key.matches(self))
                .map(|view| view.hits),
        }
        .filter(|hits| hits.filter == self.filter)
    }

    /// Only `touched` rows (section, index) changed, in what they hold rather than in
    /// name or number: the next build scores those again and keeps the last build's
    /// hits for the rest.
    fn rows_changed(&mut self, touched: &[(usize, usize)]) {
        let Some(mut hits) = self.take_hits() else {
            return;
        };
        let needle = Needle::new(&self.filter);
        for &(si, i) in touched {
            if let Some(Some(section)) = hits.sections.get_mut(si)
                && let Some(hit) = section.get_mut(i)
            {
                *hit = needle.hit(&self.sections[si].rows[i]);
            }
        }
        *self.rows_cache.rescored.get_mut() = Some(hits);
    }

    /// Show these catalogs.
    pub fn set_catalogs(&mut self, catalogs: Vec<ShownCatalog>) {
        self.catalog_places = CatalogPlaces::of(&catalogs);
        self.catalogs = catalogs;
        self.changed();
    }

    /// How often and how lately each recent was opened, which ranks matches.
    pub fn set_visits(&mut self, visits: std::collections::HashMap<PathBuf, crate::cache::Visits>) {
        self.visits = visits;
        self.changed();
    }

    /// Show the whole of the section the `more` row at `section` cuts.
    pub fn show_all(&mut self, section: usize) {
        match self.sections.get(section) {
            Some(s) if s.grouped_by_place => self.recent_expanded = true,
            Some(Section {
                root: Some(root), ..
            }) => {
                self.shown_whole.insert(root.clone());
            }
            _ => {}
        }
    }

    /// With the cursor on a row the cut would hide in a section shown whole, cut it back,
    /// the cursor on the row standing for the rest. Whether it did.
    pub fn cut_again(&mut self, section: usize) -> bool {
        let Some(key) = self.selected_key() else {
            return false;
        };
        let Some(s) = self.sections.get(section) else {
            return false;
        };
        let title = s.title.clone();
        let whole = if s.grouped_by_place {
            None
        } else {
            match s.root.clone() {
                Some(root) => Some(root),
                None => return false,
            }
        };
        let was_whole = match &whole {
            None => std::mem::replace(&mut self.recent_expanded, false),
            Some(root) => self.shown_whole.remove(root),
        };
        if !was_whole {
            return false;
        }
        if self.listed(&key).is_some() {
            match whole {
                None => self.recent_expanded = true,
                Some(root) => {
                    self.shown_whole.insert(root);
                }
            }
            return false;
        }
        self.reselect(Some(RowKey::More(title)));
        true
    }

    /// The sections, to change in place; rows are relisted on the next read.
    pub fn sections_mut(&mut self) -> &mut Vec<Section> {
        self.changed();
        &mut self.sections
    }

    fn view(&self) -> std::cell::Ref<'_, View> {
        let fresh = self
            .rows_cache
            .built
            .borrow()
            .as_ref()
            .is_some_and(|view| view.key.matches(self));
        if !fresh {
            let view = self.build_view();
            self.rows_cache.builds.set(self.rows_cache.builds.get() + 1);
            *self.rows_cache.built.borrow_mut() = Some(view);
        }
        std::cell::Ref::map(self.rows_cache.built.borrow(), |view| {
            view.as_ref().expect("built above")
        })
    }

    fn row<'a>(&'a self, slot: &Slot) -> Row<'a> {
        match slot {
            Slot::Plain(row) => row.clone(),
            Slot::Entry {
                section,
                index,
                nested,
                hit,
            } => Row::Entry {
                section: *section,
                entry: &self.sections[*section].rows[*index],
                nested: *nested,
                hit: *hit,
            },
            Slot::Door { section } => Row::Door {
                section: *section,
                entry: self.sections[*section]
                    .door
                    .as_ref()
                    .expect("the shape says it has a door"),
            },
        }
    }

    fn build_view(&self) -> View {
        // Taken, so a build for any other change scores every row.
        let kept = (self.rows_cache.rescored.take()).filter(|hits| {
            hits.filter == self.filter && hits.sections.len() <= self.sections.len()
        });
        let hits = self.score_rows(kept);
        let slots = self.slots(&hits);
        View {
            key: ViewKey::of(self),
            headers: (slots.iter().enumerate())
                .filter(|(_, slot)| matches!(slot, Slot::Plain(Row::Header { .. })))
                .map(|(i, _)| i)
                .collect(),
            slots,
            hits,
            has_dataset: self
                .sections
                .iter()
                .flat_map(|s| s.rows.iter())
                .any(|e| e.kind.is_dataset() || e.kind.is_lake_table()),
        }
    }

    /// Every row scored against the filter, but for the sections `kept` holds for the
    /// rows as listed.
    fn score_rows(&self, kept: Option<Hits>) -> Hits {
        let needle = Needle::new(&self.filter);
        let mut kept = kept.map(|hits| hits.sections).unwrap_or_default();
        let sections =
            (self.sections.iter().enumerate())
                .map(|(si, section)| {
                    let reused = (kept.get_mut(si).and_then(Option::take))
                        .filter(|hits| hits.len() == section.rows.len());
                    Some(reused.unwrap_or_else(|| {
                        (section.rows.iter()).map(|row| needle.hit(row)).collect()
                    }))
                })
                .collect();
        Hits {
            filter: self.filter.clone(),
            sections,
        }
    }

    fn slots(&self, hits: &Hits) -> Vec<Slot> {
        let mut out: Vec<Slot> = Vec::new();
        for (si, section) in self.sections.iter().enumerate() {
            let mut matched: Vec<(usize, Hit)> = (section.rows.iter())
                .zip(hits.sections[si].iter().flatten())
                .enumerate()
                .filter(|(_, (row, _))| !(self.hide_unreadable && row.hidden_by_default()))
                .filter_map(|(i, (_, hit))| hit.map(|hit| (i, hit)))
                .collect();

            // Drop a section with nothing to show, unless it stands for a named or current root,
            // its rows are on the way, or it has a door (extensionless part files list nothing,
            // and the door is the only way to read them).
            let keep_empty = section.unavailable || section.waiting || section.origin.is_some();
            let has_door = section.door.is_some() && self.filter.is_empty();
            // Only inside a directory, where an empty listing needs a reason; root sections
            // leave them out quietly.
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
            // `Found` shows empty only to say something: a walk stopped short with no match.
            let says_why = section.title == Self::SEARCH_SECTION;
            if matched.is_empty()
                && !has_door
                && hidden == 0
                && !says_why
                && !(keep_empty && self.filter.is_empty())
            {
                continue;
            }

            // Rank by match quality within a section (unfiltered, scores tie and the curated
            // order stays). Ties go to the shorter name, fzf's tiebreak. Frecency lifts an
            // often-opened row by up to a few characters' worth of match.
            let entry = |i: usize| &section.rows[i];
            if !self.filter.is_empty() {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or_default();
                let lifted = |i: usize, score: i32| {
                    let frecency = self
                        .visits
                        .get(&entry(i).path)
                        .map_or(0.0, |v| v.frecency(now));
                    score.saturating_add((frecency.min(10.0) * FRECENCY_LIFT) as i32)
                };
                matched.sort_by_cached_key(|(i, hit)| {
                    (
                        std::cmp::Reverse(lifted(*i, hit.score)),
                        entry(*i).name.len(),
                    )
                });
            }

            // An explicit sort overrides both; rows with nothing to sort by go last, not as
            // zero.
            match self.sort {
                SortMode::Natural => {}
                SortMode::Size => {
                    matched.sort_by_key(|(i, _)| std::cmp::Reverse(entry(*i).size.unwrap_or(0)));
                }
                SortMode::Rows => {
                    matched.sort_by_key(|(i, _)| std::cmp::Reverse(entry(*i).rows.unwrap_or(0)));
                }
                SortMode::Modified => {
                    matched.sort_by_key(|(i, _)| {
                        std::cmp::Reverse(
                            entry(*i)
                                .modified
                                .and_then(|m| m.duration_since(std::time::UNIX_EPOCH).ok())
                                .map(|d| d.as_secs())
                                .unwrap_or(0),
                        )
                    });
                }
            }

            let collapsed = self.section_folded(section);
            out.push(Slot::Plain(Row::Header {
                section: si,
                // The door is not counted: it opens the directory, so three files must not read four.
                matches: matched.len(),
                collapsed,
            }));
            if collapsed {
                continue;
            }
            // The way up first, as `..` in any listing.
            let root = section.root.as_deref();
            if self.filter.is_empty()
                && root
                    .is_some_and(|root| self.browsing.is_some() || self.parent_of(root).is_some())
            {
                out.push(Slot::Plain(Row::Up { section: si }));
            }
            // The door next, whatever the sort. Not while filtering: its `all files` name
            // fuzzy-matches most of the alphabet.
            if has_door {
                out.push(Slot::Door { section: si });
            }
            // At the root listing a huge directory shows its first rows (a share of the
            // height) and one row for the rest, so it does not bury the sections below. A filter
            // searches them all.
            let shown = match root {
                Some(_)
                    if self.browsing.is_none()
                        && self.filter.is_empty()
                        && self.view_height > 0
                        && !root.is_some_and(|root| self.shown_whole.contains(root)) =>
                {
                    (self.view_height * 2 / 5).max(8)
                }
                _ => usize::MAX,
            };
            let rest = if matched.len() > shown.saturating_add(1) {
                matched.split_off(shown)
            } else {
                Vec::new()
            };
            // Sorted by something measured, rows past the cut are measured too, or the biggest
            // shown would pass for the biggest of all.
            let measuring = rest.iter().any(|(i, _)| self.sort_wants(entry(*i)));
            if section.grouped_by_place {
                self.slots_by_place(si, section, &matched, &mut out);
            } else {
                // A bookmark sits under its dataset while rows keep the catalog's order.
                let in_order = self.sort == SortMode::Natural
                    && self.filter.is_empty()
                    && section.origin.is_some_and(is_catalog_origin)
                    && section.root.is_none();
                out.extend(matched.into_iter().map(|(index, hit)| Slot::Entry {
                    section: si,
                    index,
                    nested: in_order && self.bookmark(&entry(index).path).is_some(),
                    hit,
                }));
            }
            if !rest.is_empty() {
                out.push(Slot::Plain(Row::More {
                    section: si,
                    hidden: rest.len(),
                    places: 0,
                    measuring,
                }));
            }
            if hidden > 0 {
                out.push(Slot::Plain(Row::Hidden {
                    section: si,
                    count: hidden,
                }));
            }
        }
        out
    }

    /// A grouped section's rows under each one's place, and what the cap hides. Places
    /// in order of their newest row; within a place the rows keep `matched`'s order, so
    /// a sort orders each place. Whole places are shown newest first until a third of
    /// the height is used (at least one), then one `… N more in M places` row. A filter
    /// shows every match, without the cap.
    fn slots_by_place(
        &self,
        si: usize,
        section: &Section,
        matched: &[(usize, Hit)],
        out: &mut Vec<Slot>,
    ) {
        let places: Vec<PathBuf> = section.rows.iter().map(|row| place_of(&row.path)).collect();
        let mut order: Vec<&PathBuf> = Vec::new();
        for place in &places {
            if !order.contains(&place) {
                order.push(place);
            }
        }
        let groups: Vec<(&PathBuf, Vec<&(usize, Hit)>)> = order
            .into_iter()
            .filter_map(|place| {
                let rows: Vec<&(usize, Hit)> = matched
                    .iter()
                    .filter(|(i, _)| places[*i] == *place)
                    .collect();
                (!rows.is_empty()).then_some((place, rows))
            })
            .collect();

        // Before the first frame there is no height; a caller with no screen gets it whole.
        let capped = !self.recent_expanded && self.filter.is_empty() && self.view_height > 0;
        let budget = self.view_height / 3;
        let mut used = 0usize;
        let mut shown = 0usize;
        for (place, rows) in &groups {
            let cost = 1 + rows.len();
            if capped && shown > 0 && used + cost > budget {
                break;
            }
            out.push(Slot::Plain(Row::Place {
                section: si,
                path: (*place).clone(),
                label: section.place_labels.get(*place).cloned(),
                // A place's rows share its filesystem, so the first speaks for it (from `annotate`).
                source: section.rows[rows[0].0].cost.source.clone(),
                held: places.iter().filter(|p| p == place).count(),
            }));
            out.extend(rows.iter().map(|(index, hit)| Slot::Entry {
                section: si,
                index: *index,
                nested: true,
                hit: *hit,
            }));
            used += cost;
            shown += 1;
        }
        if shown < groups.len() {
            out.push(Slot::Plain(Row::More {
                section: si,
                hidden: groups[shown..].iter().map(|(_, rows)| rows.len()).sum(),
                places: groups.len() - shown,
                measuring: false,
            }));
        }
    }

    /// The names the `~` prompt offers: those in the typed directory matching its last
    /// segment, best first; hidden names only after a typed dot.
    pub fn path_candidates(&self) -> Vec<&PathName> {
        let Some(listing) = self
            .path_listing
            .as_ref()
            .filter(|l| l.dir == typed_dir(&self.path_input))
        else {
            return Vec::new();
        };
        let segment = &self.path_input[listing.dir.len()..];
        let mut memo = (listing.matched.0.lock()).unwrap_or_else(|e| e.into_inner());
        let at = match memo.as_ref() {
            Some((typed, at)) if typed == segment => at.clone(),
            _ => {
                let at = path_matches(&listing.names, segment);
                *memo = Some((segment.to_string(), at.clone()));
                at
            }
        };
        drop(memo);
        at.iter().map(|&i| &listing.names[i]).collect()
    }

    /// Put the `~` prompt's pick on the first match, or none, so the list always shows
    /// what Enter and Tab take. ↑ from the first takes the typed path as is.
    pub fn pick_first_path(&mut self) {
        self.path_pick = (!self.path_candidates().is_empty()).then_some(0);
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

    /// What Tab makes of the typed path: the sole candidate, or the candidates' longest
    /// common start. `None` when it adds nothing.
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

    /// Every URL the screen knows (catalogs, buckets, listings, the index): what `s3://`
    /// completes from.
    pub fn known_urls(&self) -> Vec<String> {
        let mut urls: Vec<String> = Vec::new();
        let mut add = |path: &Path| {
            let text = path.to_string_lossy();
            if text.contains("://") && !is_cloud_place(path) {
                urls.push(text.into_owned());
            }
        };
        for catalog in &self.catalogs {
            for dataset in &catalog.datasets {
                add(&dataset.location);
            }
        }
        for source in &self.cloud {
            for bucket in &source.buckets {
                add(bucket);
            }
        }
        for (root, rows) in self.probes.answered() {
            add(root);
            for row in rows.iter() {
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
        self.row_at(self.selected)
    }

    /// The highlighted row when it is a dataset, door included (it can be opened; it is
    /// [`Row::Door`] only to stay out of path-keyed maps).
    pub fn selected_entry(&self) -> Option<&Entry> {
        match self.selected_row()? {
            Row::Entry { entry, .. } | Row::Door { entry, .. } => Some(entry),
            _ => None,
        }
    }

    /// Whether the cursor is on the door rather than on something in the directory.
    pub fn selection_is_the_door(&self) -> bool {
        matches!(self.selected_row(), Some(Row::Door { .. }))
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
        self.selected_row().map(|r| r.section())
    }

    /// The catalog whose section heading is selected, if the selection is one.
    pub fn selected_catalog(&self) -> Option<&ShownCatalog> {
        if !self.selection_is_header() {
            return None;
        }
        let section = self.sections.get(self.selected_section()?)?;
        let origin = section.origin?;
        self.catalogs
            .iter()
            .find(|c| c.label == section.title && c.origin_note() == origin)
    }

    /// Whether the highlighted row is a section header.
    pub fn selection_is_header(&self) -> bool {
        matches!(self.selected_row(), Some(Row::Header { .. }))
    }

    /// Remote roots that have neither answered nor been written off, for the caller to
    /// probe off the UI thread.
    pub fn pending_probes(&self) -> Vec<PathBuf> {
        let check = self.network_check;
        let mut out = Vec::new();
        // From the section, not its subtitle (which names the filesystem, not "network").
        for root in self.sections.iter().filter_map(|s| s.remote_root.as_ref()) {
            if !self.probes.settled(root) && !out.contains(root) {
                out.push(root.clone());
            }
        }
        // A browsed remote directory: its rows can only come from a probe, and the root scan
        // above does not cover it.
        if let Some(dir) = &self.browsing
            && check(dir)
            && cloud_source_id(dir).is_none()
            && !self.probes.settled(dir)
            && !out.contains(dir)
        {
            out.push(dir.clone());
        }
        out
    }

    /// Whether the browsed directory is below the browse start, so Esc has a level to
    /// climb before the listing.
    pub fn below_browse_start(&self) -> bool {
        let (Some(dir), Some(start)) = (&self.browsing, &self.browse_start) else {
            return false;
        };
        if dir == start {
            return false;
        }
        // Up through parents, not a path prefix: `s3://bucket` sits below `cloud://<id>`.
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
        ((self.network_check)(dir) && !self.probes.settled(dir)).then_some(dir)
    }

    /// Record what a probe found. An empty listing is still an answer.
    pub fn probe_ready(&mut self, root: PathBuf, rows: Vec<Entry>, cut_short: bool) {
        self.probes.insert(
            root.clone(),
            Probe::Listed {
                rows: rows.into(),
                cut_short,
            },
        );
        self.apply_cloud_kinds(&root);
    }

    /// Label every listed row a peek answered, in place: the probes' rows, then the
    /// sections showing them (a remote place's built again from its probe, Recent's
    /// rows directly). No listing is read again.
    pub fn take_cloud_kinds(&mut self) {
        let roots: Vec<PathBuf> = (self.probes.answered())
            .map(|(root, _)| root.clone())
            .collect();
        for root in &roots {
            self.apply_cloud_kinds(root);
        }
        let mut labeled = false;
        for section in &mut self.sections {
            for row in &mut section.rows {
                if row.kind == EntryKind::Directory
                    && let Some((kind, holds)) = self.cloud_kinds.get(&row.path)
                {
                    row.kind = *kind;
                    if !holds.is_empty() {
                        row.holds = holds.clone();
                    }
                    labeled = true;
                }
            }
        }
        if labeled {
            self.changed();
        }
        // A browsed directory's door is judged from its rows' kinds.
        if let Some(dir) = self.browsing.clone()
            && roots.contains(&dir)
        {
            self.relist_remote(&dir);
        }
    }

    /// Label the rows of a cloud listing with what peeking inside them found.
    pub fn apply_cloud_kinds(&mut self, root: &Path) {
        let Some(rows) = self.probes.listed_mut(root) else {
            return;
        };
        // Copied only when a listing being built still holds these rows.
        for row in std::sync::Arc::make_mut(rows).iter_mut() {
            if row.kind == EntryKind::Directory
                && let Some((kind, holds)) = self.cloud_kinds.get(&row.path)
            {
                row.kind = *kind;
                // The label is what the peek counted (`12 parquet`), as on disk. Only when there is
                // something: a claim without a count must not erase one the row has.
                if !holds.is_empty() {
                    row.holds = holds.clone();
                }
            }
        }
    }

    /// Cloud directories on or near the screen not yet peeked into, at most `limit`, the
    /// highlighted first. The cloud twin of [`Self::unclassified_visible`]: a peek is a
    /// request, worth spending on rows someone is looking at.
    pub fn cloud_directories_to_peek(&self, limit: usize) -> Vec<PathBuf> {
        if limit == 0 {
            return Vec::new();
        }
        let view = self.view();
        let mut out: Vec<PathBuf> = Vec::new();
        for entry in self.entries_near_cursor(&view, limit) {
            // Object-store directories by URL (`read_dir` on `s3://` finds nothing), hence a pass
            // of their own.
            if !is_object_store_url(&entry.path) || is_cloud_place(&entry.path) {
                continue;
            }
            if !matches!(entry.kind, EntryKind::Directory | EntryKind::Unknown) {
                continue;
            }
            // Catalog datasets and their bookmarks are listed by name; their store is not asked
            // until opened or entered.
            if self.browsing.is_none()
                && (self.catalog_dataset(&entry.path).is_some()
                    || self.bookmark(&entry.path).is_some())
            {
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

    /// Record that a probe could not read the root, and why when the service said.
    pub fn probe_failed(&mut self, root: PathBuf, why: Option<String>) {
        self.probes.insert(root, Probe::Unreachable(why));
    }

    /// Measure a batch of rows on the calling thread, for tests and library callers with
    /// safe paths. The app never calls this: footer reads can block, so the UI thread
    /// only picks rows ([`HomeState::unmeasured_visible`]) and a worker reads.
    pub fn measure_now(&mut self, limit: usize) -> bool {
        let wanted = self.unmeasured_visible(limit);
        let more = self.unmeasured_visible(limit + 1).len() > wanted.len();
        for entry in wanted {
            let mut probe = entry.clone();
            stat_and_recall(&mut probe, &self.known);
            if probe.rows.is_none() && probe.columns.is_empty() {
                discover::enrich(&mut probe);
            }
            self.record_measurement(entry.path.clone(), measured_from(&probe, &entry));
        }
        self.apply_new_measurements();
        more
    }

    /// Rows not yet measured, up to `limit`: the screen and a little either side of it
    /// (see [`HomeState::entries_on_screen`]), the highlighted row first. Nothing off
    /// screen is read for a keystroke, so a filter typed over thousands of files costs
    /// what it shows; a column name matches the rows whose columns are known, read here
    /// or remembered from an earlier run. Sorted by rows, the order needs every count, so
    /// then every row listed and those a cut hides: the sort asked for them.
    pub fn unmeasured_visible(&self, limit: usize) -> Vec<Entry> {
        let view = self.view();
        let mut out: Vec<Entry> = Vec::new();
        for entry in self.entries_on_screen(&view) {
            if self.wants_measuring(entry) && !out.iter().any(|e| e.path == entry.path) {
                out.push(entry.clone());
                if out.len() >= limit {
                    return out;
                }
            }
        }
        if self.sort == SortMode::Rows {
            self.sorted_rest(&view, limit, &mut out);
        }
        out
    }

    /// Rows a sort by size or time lacks a stat for, up to `limit`: every row listed and
    /// those a cut hides, which need their stat and nothing read.
    pub fn unstated_for_sort(&self, limit: usize) -> Vec<Entry> {
        let mut out = Vec::new();
        if matches!(self.sort, SortMode::Size | SortMode::Modified) {
            self.sorted_rest(&self.view(), limit, &mut out);
        }
        out
    }

    /// The rows the sort wants and lacks, listed or behind a cut, into `out`.
    fn sorted_rest(&self, view: &View, limit: usize, out: &mut Vec<Entry>) {
        let mut take = |entry: &Entry| {
            if self.sort_wants(entry) && !out.iter().any(|e| e.path == entry.path) {
                out.push(entry.clone());
            }
            out.len() >= limit
        };
        for entry in view.slots.iter().filter_map(|slot| self.entry_of(slot)) {
            if take(entry) {
                return;
            }
        }
        for slot in &view.slots {
            let Slot::Plain(Row::More {
                section,
                measuring: true,
                ..
            }) = slot
            else {
                continue;
            };
            for entry in &self.sections[*section].rows {
                if take(entry) {
                    return;
                }
            }
        }
    }

    /// Whether the sort orders by something `entry` lacks until measured.
    fn sort_wants(&self, entry: &Entry) -> bool {
        match self.sort {
            SortMode::Natural => false,
            SortMode::Rows => self.wants_measuring(entry),
            SortMode::Size | SortMode::Modified => {
                self.wants_measuring(entry) && entry.modified.is_none()
            }
        }
    }

    /// Whether `entry` is a local row with no count yet that measuring would give, or
    /// with no size and mtime yet, which a listing leaves to the rows shown.
    fn wants_measuring(&self, entry: &Entry) -> bool {
        if entry.measured || self.enriched.contains_key(&entry.path) {
            return false;
        }
        // A directory not yet looked into is the classification pass's, which stats it.
        if entry.kind != EntryKind::Unknown
            && discover::unstated(entry)
            && !(self.network_check)(&entry.path)
        {
            return true;
        }
        if entry.rows.is_some() {
            return false;
        }
        // The kind settles it before the mount table: this runs per row per frame, and the
        // cheap question first keeps thousands of rows free.
        if matches!(
            entry.kind,
            EntryKind::Directory | EntryKind::Unknown | EntryKind::Other
        ) || entry.kind.is_lake_table()
        {
            return false;
        }
        // Remote rows are measured by their root's probe; a second thread on a share that
        // may never answer is never reclaimed.
        !(self.network_check)(&entry.path)
    }

    /// Look into a batch of rows on the calling thread; for tests and library callers,
    /// like [`HomeState::measure_now`].
    pub fn classify_now(&mut self, limit: usize) -> bool {
        let wanted = self.unclassified_visible(limit);
        let more = self.unclassified_visible(limit + 1).len() > wanted.len();
        for entry in wanted {
            let mut stated = entry.clone();
            stat_and_recall(&mut stated, &self.known);
            let probe = look_into_as(&stated, &Default::default());
            self.record_measurement(entry.path.clone(), measured_from(&probe, &entry));
        }
        self.apply_new_measurements();
        more
    }

    /// Rows on or near the screen not yet looked into, up to `limit`. Unlike
    /// [`HomeState::unmeasured_visible`], which walks from the top, this asks the
    /// viewport plus a screen either side (thousands of partitions would take seconds to
    /// reach the cursor): the highlighted row first (about to be acted on), then the
    /// screen, the screen below, the screen above.
    pub fn unclassified_visible(&self, limit: usize) -> Vec<Entry> {
        self.unclassified_visible_where(limit, |_| true)
    }

    /// [`HomeState::unclassified_visible`], of the rows `keep` takes.
    pub fn unclassified_visible_where(
        &self,
        limit: usize,
        keep: impl Fn(&Entry) -> bool,
    ) -> Vec<Entry> {
        if limit == 0 {
            return Vec::new();
        }
        let view = self.view();
        let mut out: Vec<Entry> = Vec::new();
        for entry in self.entries_near_cursor(&view, limit) {
            if entry.kind != EntryKind::Unknown
                || self.missing.contains(&entry.path)
                || !keep(entry)
            {
                continue;
            }
            // Already looked into, even if that settled nothing: re-asking would stat per frame.
            if self.enriched.contains_key(&entry.path) {
                continue;
            }
            // Object-store places are peeked by listing; see
            // [`HomeState::cloud_directories_to_peek`].
            if is_object_store_url(&entry.path) || is_cloud_place(&entry.path) {
                continue;
            }
            // The same dataset may be listed twice (directory and Recent); look once.
            if out.iter().any(|e| e.path == entry.path) {
                continue;
            }
            out.push(entry.clone());
            if out.len() >= limit {
                break;
            }
        }
        out
    }

    /// The entry rows on screen: the highlighted one, the screen, then half a screen
    /// below and above it, so a step or a short scroll finds its rows already read.
    fn entries_on_screen<'a>(&'a self, view: &'a View) -> impl Iterator<Item = &'a Entry> + 'a {
        // Before the first frame there is no screen: the list from the top, as far as the
        // caller takes it.
        let height = if self.view_height == 0 {
            usize::MAX / 4
        } else {
            self.view_height
        };
        let rows = view.slots.len();
        let top = self.scroll.min(rows);
        let reach = height / 2;
        let bottom = top.saturating_add(height + reach).min(rows);
        let above = top.saturating_sub(reach);
        std::iter::once(self.selected)
            .chain(top..bottom)
            .chain(above..top)
            .filter_map(|i| self.entry_of(view.slots.get(i)?))
    }

    /// The entry rows near the cursor: the highlighted one, the screen, the screen below,
    /// the screen above.
    fn entries_near_cursor<'a>(
        &'a self,
        view: &'a View,
        limit: usize,
    ) -> impl Iterator<Item = &'a Entry> + 'a {
        // Before the first frame there is no height: take the top of the list, a batch's
        // worth.
        let height = if self.view_height == 0 {
            limit
        } else {
            self.view_height
        };
        let rows = view.slots.len();
        let top = self.scroll.min(rows);
        let ahead = top.saturating_add(2 * height).min(rows);
        let behind = top.saturating_sub(height);
        std::iter::once(self.selected)
            .chain(top..ahead)
            .chain(behind..top)
            .filter_map(|i| self.entry_of(view.slots.get(i)?))
    }

    /// The entry a row shows, when it is an entry row.
    fn entry_of(&self, slot: &Slot) -> Option<&Entry> {
        match slot {
            Slot::Entry { section, index, .. } => Some(&self.sections[*section].rows[*index]),
            _ => None,
        }
    }

    /// Record what measuring `path` found, for [`HomeState::apply_new_measurements`] to
    /// fold into its rows.
    pub fn record_measurement(&mut self, path: PathBuf, measured: Measured) {
        self.unapplied.insert(path.clone());
        self.enriched.insert(path, measured);
    }

    /// Record a size alone: a measurement that landed meanwhile keeps the rest.
    pub fn record_size(&mut self, path: PathBuf, measured: Measured) {
        match self.enriched.get_mut(&path) {
            Some(known) => {
                known.size = measured.size;
                self.unapplied.insert(path);
            }
            None => self.record_measurement(path, measured),
        }
    }

    /// Fold every known measurement into the rows currently listed, as a new listing
    /// needs.
    pub fn apply_measurements(&mut self) {
        self.unapplied.clear();
        for section in &mut self.sections {
            // The door too: it reads its directory's slot on purpose, showing numbers already
            // measured upstairs; nothing writes the door's answer.
            for row in section.rows.iter_mut().chain(section.door.iter_mut()) {
                if let Some(m) = self.enriched.get(&row.path) {
                    fold_measured(row, m);
                }
            }
            // The door's name says what it opens, and a measurement can change that: the
            // footers turn a directory of files down as one table, or name its keys.
            if let Some(door) = section.door.as_mut() {
                door.name = door_name(door, &section.rows);
            }
        }
        self.changed();
        self.land_again();
    }

    /// Fold the measurements recorded since the last fold into their rows. Only those
    /// rows change and are scored again: measurements land a file at a time while the
    /// user types, over listings of thousands.
    pub fn apply_new_measurements(&mut self) {
        if self.unapplied.is_empty() {
            return;
        }
        let unapplied = std::mem::take(&mut self.unapplied);
        // A row's name first: hashing a short name is cheaper than hashing a whole path
        // component by component, and few of thousands of rows are among those measured.
        // Equal paths have equal names, so no measured row is passed over.
        let names: std::collections::HashSet<&std::ffi::OsStr> = (unapplied.iter())
            .map(|path| path.file_name().unwrap_or(path.as_os_str()))
            .collect();
        let new = |path: &Path| {
            path.file_name().is_none_or(|name| names.contains(name)) && unapplied.contains(path)
        };
        let mut touched: Vec<(usize, usize)> = Vec::new();
        let mut any = false;
        let mut moved = false;
        let sort = self.sort;
        for (si, section) in self.sections.iter_mut().enumerate() {
            let mut here = false;
            for (i, row) in section.rows.iter_mut().enumerate() {
                if new(&row.path)
                    && let Some(m) = self.enriched.get(&row.path)
                {
                    let before = Standing::of(row, sort);
                    fold_measured(row, m);
                    moved |= Standing::of(row, sort) != before;
                    touched.push((si, i));
                    here = true;
                }
            }
            if let Some(door) = section.door.as_mut()
                && new(&door.path)
                && let Some(m) = self.enriched.get(&door.path)
            {
                fold_measured(door, m);
                here = true;
            }
            if here && let Some(door) = section.door.as_mut() {
                door.name = door_name(door, &section.rows);
            }
            any |= here;
        }
        if !any {
            return;
        }
        // The rows are drawn from the sections as they are now; the list is built again
        // only when a row moves, comes or goes, which is rare once the first answers are in.
        if moved || !self.hits_hold(&touched) {
            self.rows_changed(&touched);
        }
        self.land_again();
    }

    /// Whether the built list still answers the filter the same way for `touched` rows
    /// (section, index), so it stands as built. With no list built for the rows as they
    /// are, the hits kept for the next build are rescored instead.
    fn hits_hold(&self, touched: &[(usize, usize)]) -> bool {
        let built = self.rows_cache.built.borrow();
        let Some(view) = built.as_ref().filter(|view| view.key.matches(self)) else {
            return false;
        };
        let needle = Needle::new(&self.filter);
        touched.iter().all(|&(si, i)| {
            let was = view.hits.sections.get(si).and_then(|s| s.as_ref()?.get(i));
            was == Some(&needle.hit(&self.sections[si].rows[i]))
        })
    }

    /// Landed on a door the footers have since turned down, and not moved: the cursor
    /// goes where it would have landed had they been read first.
    fn land_again(&mut self) {
        if self.landing
            && let Some(Row::Door { entry, .. }) = self.row_at(self.selected)
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
        // Recent is ranked by frecency, and the last file opened is still one Enter away.
        if self.filter.is_empty()
            && let Some(newest) = self.newest_recent.as_ref()
            && let Some(at) = self.position(|r| {
                matches!(r, Row::Entry { section, entry, .. }
                    if entry.path == *newest
                        && self.sections[*section].title == Self::RECENT_SECTION)
            })
        {
            return at;
        }
        let first = self.position(|r| matches!(r, Row::Entry { .. } | Row::Door { .. }));
        let first = match first.and_then(|i| self.row_at(i)) {
            Some(Row::Door { entry, .. }) if !door_lands(entry) => self
                .position(|r| matches!(r, Row::Entry { .. } | Row::Hidden { .. }))
                .or(first),
            _ => first,
        };
        // A directory of files datui cannot open: the row that says so.
        first
            .or_else(|| self.position(|r| matches!(r, Row::Hidden { .. })))
            .unwrap_or(0)
    }

    pub fn clamp_selection(&mut self) {
        let n = self.row_count();
        if n == 0 {
            self.selected = 0;
        } else if self.selected >= n {
            self.selected = n - 1;
        }
    }

    /// Put the selection on row `index` of what is listed, as a click does.
    pub fn select(&mut self, index: usize) {
        if index < self.row_count() {
            self.returning = None;
            self.landing = false;
            self.selected = index;
        }
    }

    pub fn move_selection(&mut self, delta: isize) {
        self.returning = None;
        self.landing = false;
        let n = self.row_count();
        if n == 0 {
            return;
        }
        let cur = self.selected as isize;
        let next = (cur + delta).rem_euclid(n as isize);
        self.selected = next as usize;
    }

    /// Move the selection `delta` rows, stopping at the ends rather than wrapping (a
    /// wrapped page jump lands somewhere unexpected; single steps wrap).
    pub fn page_selection(&mut self, delta: isize) {
        self.returning = None;
        self.landing = false;
        let n = self.row_count();
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
        measured: false,
        format_spec: None,
        table: None,
    }
}

/// The row for one bucket.
fn bucket_entry(url: &Path) -> Entry {
    let mut entry = Entry::directory(url);
    // The bucket name, without a source id, rather than the URL's last segment.
    let text = url.to_string_lossy();
    let (_, plain) = crate::cloud::source::split_source_id(&text);
    entry.name = plain
        .rsplit('/')
        .find(|part| !part.is_empty())
        .unwrap_or("")
        .to_string();
    entry
}

/// Whether a remote path's name says it is a file: a data extension or any dot in its
/// last segment. A trailing slash is always a prefix.
pub fn names_a_file(path: &Path) -> bool {
    let named = path.to_string_lossy();
    // `file_name`, not a split on `/`, which on Windows took the whole path as the last
    // segment.
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
    // Classifying and stat'ing touch the filesystem, so a remote entry is listed by name
    // until its probe lands.
    let kind = if remote {
        // The name alone: an extension (readable or not) makes it a file, so → never enters
        // `data.dat` as a prefix; anything else stays Unknown, not a plain directory, which
        // would contradict its root's probe later. A trailing slash is a prefix whatever
        // the name (`exports/`, `2024.01.15/`).
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
        measured: false,
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
        // The platform's separator, so Windows reads `~\data\a.csv`.
        return format!("~{}{}", std::path::MAIN_SEPARATOR, rest.display());
    }
    path.display().to_string()
}

/// Complete a partly typed path against its directory: the longest unambiguous
/// extension of `typed` and the candidate count. Reads a directory, so only on a
/// worker.
pub fn complete_path(typed: &str) -> (String, usize) {
    let expanded = expand_user_path(typed);
    // `\` is a separator on Windows too: `C:\data\` lists inside `data`.
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
            // Dotfiles only when a dot is typed.
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

    // The candidates' common prefix: further would be guessing.
    let shared = names
        .iter()
        .skip(1)
        .fold(names[0].clone(), |acc, name| common_prefix(&acc, name));

    let mut completed = typed.to_string();
    completed.truncate(typed.len() - prefix.len());
    completed.push_str(&shared);

    // A single directory gets the separator being typed, so the next Tab descends
    // (`C:\Users\` stays `\`).
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
    /// The names the last typed segment matched, best first.
    pub matched: SegmentMatches,
}

/// Which of a [`PathListing`]'s names a typed segment matches, kept for that segment:
/// each frame and key asks, and scoring thousands of names each time is what a
/// keystroke must not cost. A memo, so not part of the listing's value.
#[derive(Debug, Default)]
pub struct SegmentMatches(std::sync::Mutex<Option<(String, std::sync::Arc<[usize]>)>>);

impl Clone for SegmentMatches {
    fn clone(&self) -> Self {
        Self::default()
    }
}

impl PartialEq for SegmentMatches {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}

impl Eq for SegmentMatches {}

/// Which of `names` the typed `segment` matches, best first: prefix matches, as a shell
/// completes, then fuzzy ones; hidden names only after a typed dot.
fn path_matches(names: &[PathName], segment: &str) -> std::sync::Arc<[usize]> {
    let mut matched: Vec<(usize, i32)> = (names.iter().enumerate())
        .filter(|(_, n)| !n.name.starts_with('.') || segment.starts_with('.'))
        .filter_map(|(i, n)| {
            if segment.is_empty() {
                return Some((i, 0));
            }
            let prefix = n.name.starts_with(segment) as i32 * 1_000_000;
            fuzzy_score(segment, &n.name).map(|score| (i, prefix + score))
        })
        .collect();
    matched
        .sort_by(|(a, sa), (b, sb)| sb.cmp(sa).then_with(|| names[*a].name.cmp(&names[*b].name)));
    matched.into_iter().map(|(i, _)| i).collect()
}

/// The most names a typed directory lists: a prompt finds one name by typing.
const PATH_LISTING_MAX: usize = 5_000;

/// The directory part of a typed path, through its last separator; for a URL at
/// least its scheme (`s3://`), so buckets list under it.
pub fn typed_dir(typed: &str) -> &str {
    let is_separator = |c: char| c == '/' || (cfg!(windows) && c == '\\');
    let floor = typed.find("://").map_or(0, |at| at + 3);
    match typed[floor..].rfind(is_separator) {
        Some(at) => &typed[..floor + at + 1],
        None => &typed[..floor],
    }
}

/// The separator a directory completed under `dir` ends with: a URL's `/`, or the
/// one being typed.
fn separator_in(dir: &str) -> char {
    if typed_dir_is_url(dir) {
        return '/';
    }
    dir.chars()
        .rev()
        .find(|c| *c == '/' || (cfg!(windows) && *c == '\\'))
        .unwrap_or(std::path::MAIN_SEPARATOR)
}

/// Whether a typed directory is a URL, listed from what datui knows rather than read.
pub fn typed_dir_is_url(dir: &str) -> bool {
    dir.contains("://")
}

/// A local directory typed at `~`, for the prompt's list; reads it, so runs on a
/// worker. Nothing typed lists the working directory.
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
            matched: Default::default(),
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
        matched: Default::default(),
    }
}

/// The names one level below `dir` among `urls`: how `s3://`, `gs://` and `az://`
/// complete, from what was listed, opened or cataloged. Nothing is asked of the store.
pub fn names_under(dir: &str, urls: impl IntoIterator<Item = String>) -> PathListing {
    let mut names: Vec<PathName> = Vec::new();
    for url in urls {
        // An Azure URL is known in its full form; `az://container/` is how one is typed.
        let forms = match crate::cloud::source::azure_parts(&url) {
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
            // Something below it, a trailing slash, or no extension: a bucket or prefix, as for
            // a recent.
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
        matched: Default::default(),
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

    /// The list and the pane are told one thing: the curated word in both, and for a
    /// bucket directory, where looking into it is.
    #[test]
    fn a_rows_label_is_one_decision_for_the_list_and_the_pane() {
        let mut directory = Entry::for_test(Path::new("s3://bucket/warehouse"), "warehouse");
        directory.kind = EntryKind::Directory;
        let said = |look, place_kind| describe(&directory, place_kind, look, 0, None);
        let g = crate::glyphs::get();

        let waiting = said(Some(CloudLook::Waiting), None);
        assert_eq!(
            (waiting.short.as_str(), waiting.words.as_str()),
            (g.ellipsis, "")
        );
        assert_eq!(said(Some(CloudLook::Looking), None).words, "");
        assert!(
            said(Some(CloudLook::Failed), None)
                .words
                .contains("listing failed")
        );
        assert_eq!(said(None, None).words, "directory");
        let curated = said(None, Some("dataset"));
        assert_eq!(
            (curated.short.as_str(), curated.words.as_str()),
            ("dataset", "dataset")
        );
        assert!(curated.curated);

        directory.holds = crate::home::discover::Holds {
            formats: vec![("parquet".to_string(), 12)],
            ..Default::default()
        };
        let counted = describe(&directory, None, None, 0, None);
        assert_eq!(counted.short, "12 parquet");
        assert_eq!(
            counted.words, "directory",
            "the count is the pane's `contains` line"
        );
        let curated = describe(&directory, Some("dataset"), None, 0, None);
        assert_eq!(
            curated.short, "dataset",
            "the curated word wins over the count"
        );

        directory.opens_whole_directory = true;
        assert_eq!(
            describe(&directory, Some("dataset"), None, 0, None),
            RowLabel::default()
        );
    }

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

    fn counted(n: usize) -> crate::home::discover::Holds {
        crate::home::discover::Holds {
            formats: vec![("parquet".to_string(), n)],
            ..Default::default()
        }
    }

    /// The claim `peek_cloud_directories` stakes before its answers arrive, so a rebuild
    /// in the meantime does not ask the store again: a `Directory` that counted nothing.
    fn in_flight() -> (EntryKind, crate::home::discover::Holds) {
        (
            EntryKind::Directory,
            crate::home::discover::Holds::default(),
        )
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
        home.probe_ready(root.clone(), vec![row], false);
        home.cloud_kinds.insert(path, in_flight());
        home.apply_cloud_kinds(&root);

        assert_eq!(
            home.probes.listed(&root).unwrap()[0].holds.label(),
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
        home.probe_ready(root.clone(), vec![row.clone()], false);
        home.browsing = Some(root);
        home.rebuild(&[]);
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
        home.probe_ready(root.clone(), vec![row], false);
        home.cloud_kinds
            .insert(path, (EntryKind::MultiFile, counted(40)));
        home.apply_cloud_kinds(&root);

        assert_eq!(
            home.probes.listed(&root).unwrap()[0].holds.label(),
            "40 parquet"
        );
        assert_eq!(
            home.probes.listed(&root).unwrap()[0].kind,
            EntryKind::MultiFile
        );
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
        home.probe_ready(root.clone(), vec![row], false);
        home.cloud_kinds
            .insert(settled, (EntryKind::Directory, counted(1)));
        home.apply_cloud_kinds(&root);

        assert_eq!(home.probes.listed(&root).unwrap()[0].kind, EntryKind::Hive);
        assert_eq!(
            home.probes.listed(&root).unwrap()[0].holds.label(),
            "40 parquet"
        );
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
        home.probe_ready(root.clone(), rows, false);
        home.browsing = Some(root.clone());
        home.view_height = 10;
        home.rebuild(&[]);
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
        home.sections.push(Section::titled("Here", vec![row]));
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
        look_into_batch(
            rows,
            &cache,
            &Default::default(),
            Reads::Files,
            |path, m| {
                let name = path.file_name().unwrap().to_string_lossy().into_owned();
                sent.push((name, m.kind, m.rows));
            },
        );

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
    /// A row given a kind from the cache is never looked into again — `look_into_as` only
    /// classifies an `Unknown`, and `unclassified_visible` skips anything else. So a
    /// count left behind is left behind for the session: the row says `dir` about a
    /// directory of fifteen Parquet files, and `enrich` goes on to describe it by
    /// whatever is in its subdirectories.
    #[test]
    fn what_a_directory_holds_is_restored_beside_its_kind() {
        let holds = crate::home::discover::Holds {
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
                classified_by: crate::home::discover::CLASSIFIER_VERSION,
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
            classified_by: crate::home::discover::CLASSIFIER_VERSION,
            holds: crate::home::discover::Holds {
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
        apply_known_facts(
            &mut row,
            &index(crate::home::discover::CLASSIFIER_VERSION),
            true,
        );
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
        let urls: Vec<String> = catalogs(&crate::config::AppConfig::default())
            .into_iter()
            .filter(|c| c.origin == crate::home::catalog::Origin::Bundled)
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

    /// An empty `examples.toml` of the user's replaces the Example datasets with
    /// nothing, and an empty catalog has no section: the section is gone.
    #[test]
    fn an_empty_examples_toml_hides_the_section() {
        let mut config = crate::config::AppConfig::default();
        // The examples are all HTTP or S3: a build that reads neither has none.
        assert_eq!(
            catalogs(&config)
                .iter()
                .any(|c| c.origin == crate::home::catalog::Origin::Bundled),
            cfg!(any(feature = "http", feature = "cloud"))
        );
        config.read_catalogs = vec![
            crate::home::catalog::parse(
                "label = \"Mine\"\n",
                crate::home::catalog::EXAMPLES,
                crate::home::catalog::Origin::Folder,
                None,
            )
            .unwrap(),
        ];
        assert!(catalogs(&config).is_empty(), "{:?}", catalogs(&config));
    }

    /// A catalog of the user's stays whole whatever the build: the user named it, and
    /// opening a dataset it cannot read says why.
    #[test]
    fn a_users_catalog_is_shown_whole() {
        let mut config = crate::config::AppConfig::default();
        let mine = crate::home::catalog::parse(
            r#"
            [bucket]
            name = "Bucket"
            url = "s3://bucket/prefix/"
            [web]
            name = "Web"
            url = "https://example.com/data.csv"
            "#,
            crate::home::catalog::MINE,
            crate::home::catalog::Origin::Mine,
            None,
        )
        .unwrap();
        config.read_catalogs = vec![mine];
        let shown = catalogs(&config);
        let mine = shown.iter().find(|c| c.id == "mine").unwrap();
        assert_eq!(mine.datasets.len(), 2);
        assert_eq!(mine.label, crate::home::catalog::MINE_LABEL);
    }
}

#[cfg(test)]
mod place_tests {
    use super::same_place;
    use std::path::Path;

    #[test]
    fn local_paths_are_one_place_however_spelled() {
        assert!(same_place(
            Path::new("/data/./sales/"),
            Path::new("/data/sales")
        ));
        assert!(!same_place(
            Path::new("/data/sales"),
            Path::new("/data/sale")
        ));
        assert!(same_place(
            Path::new("s3://bucket/dir/"),
            Path::new("s3://bucket/dir")
        ));
        if cfg!(windows) {
            assert!(same_place(
                Path::new("c:/data/sales.csv"),
                Path::new(r"C:\data\sales.csv")
            ));
        }
    }
}

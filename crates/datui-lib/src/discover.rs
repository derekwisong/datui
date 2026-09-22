//! Dataset discovery for the home screen.
//!
//! This is deliberately *not* a catalogue. Nothing here is persisted: every listing
//! is computed from the filesystem when asked for, and forgotten when the session
//! ends. The only state datui keeps between runs is a list of recently opened paths.
//!
//! Discovery is also deliberately shallow. Interesting datasets tend to live on
//! mounts — network filesystems, spinning disks, hive trees with a hundred thousand
//! partition files — so a recursive walk would make the home screen slowest exactly
//! where the data is most interesting. Every function here scans one directory level
//! and stops.

use std::path::{Path, PathBuf};

/// File extensions datui can open, used to tell a dataset from an ordinary file.
const DATA_EXTENSIONS: &[&str] = &[
    "parquet", "csv", "tsv", "txt", "json", "ndjson", "jsonl", "ipc", "arrow", "feather", "avro",
    "orc", "xlsx", "xls", "xlsm",
];

/// Compression suffixes that may follow a data extension (`sales.csv.gz`).
const COMPRESSION_EXTENSIONS: &[&str] = &["gz", "bz2", "xz", "zst", "zstd"];

/// How many directory entries to inspect before deciding whether a directory is a
/// hive dataset. A hive root's children are all `key=value`, so the answer is
/// apparent immediately — and enumerating every partition of a large dataset is
/// exactly the stall this cap exists to prevent.
const HIVE_PROBE_LIMIT: usize = 8;

/// Upper bound on entries read from a single directory, so a pathological directory
/// cannot hang the UI.
pub const MAX_ENTRIES_PER_DIR: usize = 5_000;

/// What a home-screen row represents.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EntryKind {
    /// A single data file.
    File,
    /// A directory of `key=value` partitions — one dataset, not a tree to walk.
    Hive,
    /// A directory of similarly-shaped data files, openable as one table.
    MultiFile,
    /// A Delta Lake table: `_delta_log/` beside the data files.
    Delta,
    /// An Apache Iceberg table: `metadata/` holding the snapshots, `data/` the files.
    Iceberg,
    /// An Apache Hudi table: `.hoodie/` holding the timeline.
    Hudi,
    /// An ordinary directory, to descend into.
    Directory,
    /// Somewhere remote that has not been looked at yet. Classifying it would mean
    /// reading it, which is the call that blocks when the network is gone — so it is
    /// offered as openable and left unlabelled rather than guessed at.
    ///
    /// Also what a kind this build does not recognize reads back as. The dataset index
    /// is one JSON map, and a value an older datui cannot parse would otherwise fail the
    /// whole map and discard every dataset fact it had — see `CLASSIFIER_VERSION`, which
    /// is why a new kind can appear in a file an older build reads.
    #[serde(other)]
    Unknown,
}

/// Bumped whenever a build starts classifying something differently.
///
/// A cached kind is the only thing a remote row has to go on — it was never stat'ed, and
/// classifying it means reading it — so it is restored rather than re-derived. That makes
/// it a way for an answer this build would not give to come back: a Delta root measured
/// before lake tables were recognized was recorded as `multifile`, and restoring that
/// opens it as one table, which is the whole of #237 read back off disk.
///
/// So the kind is restored only when the build that wrote it classified the way this one
/// does. Everything else in the record — rows, columns, cost — is a measurement rather
/// than a judgement, and survives.
pub const CLASSIFIER_VERSION: u32 = 2;

impl EntryKind {
    /// Short label shown next to the entry name.
    pub fn label(self) -> &'static str {
        match self {
            EntryKind::File => "",
            EntryKind::Hive => "hive",
            EntryKind::MultiFile => "multi",
            EntryKind::Delta => "delta",
            EntryKind::Iceberg => "iceberg",
            EntryKind::Hudi => "hudi",
            EntryKind::Directory => "dir",
            EntryKind::Unknown => "",
        }
    }

    /// Whether selecting this entry opens a dataset rather than navigating.
    ///
    /// An unexamined remote path counts: datui can open an object-store prefix or a
    /// hive directory directly, and descending into one is not possible anyway
    /// without the listing this deliberately has not fetched.
    pub fn is_dataset(self) -> bool {
        !matches!(self, EntryKind::Directory) && !self.is_lake_table()
    }

    /// Whether this row is *known* to be a dataset.
    ///
    /// [`EntryKind::is_dataset`] answers "may this be opened", and a row nothing has
    /// looked into answers yes: it is offered, and looked into before it is acted on.
    /// This one answers "is this a dataset", which such a row cannot answer at all —
    /// and that is the question counting asks. A folder of two hundred subdirectories
    /// nobody has looked into is not two hundred datasets.
    pub fn is_known_dataset(self) -> bool {
        self != EntryKind::Unknown && self.is_dataset()
    }

    /// A Delta, Iceberg or Hudi table root: a log beside the data files that says which
    /// of them are live, which datui does not read yet.
    pub fn is_lake_table(self) -> bool {
        matches!(
            self,
            EntryKind::Delta | EntryKind::Iceberg | EntryKind::Hudi
        )
    }

    /// The format's name for prose. `label` is the row's chip, and is lowercase like
    /// `hive` and `multi` beside it.
    pub fn lake_name(self) -> Option<&'static str> {
        match self {
            EntryKind::Delta => Some("Delta"),
            EntryKind::Iceberg => Some("Iceberg"),
            EntryKind::Hudi => Some("Hudi"),
            _ => None,
        }
    }
}

/// One row on the home screen.
#[derive(Debug, Clone)]
pub struct Entry {
    pub path: PathBuf,
    pub kind: EntryKind,
    /// Display name — the file or directory name, not the full path.
    pub name: String,
    /// Size in bytes. For a multi-file or hive dataset this is the sum of the files
    /// actually inspected, so it is a floor rather than an exact total.
    pub size: Option<u64>,
    pub modified: Option<std::time::SystemTime>,
    /// Row count, when it can be had without reading data (Parquet footers only).
    pub rows: Option<usize>,
    /// Column count, same caveat.
    pub cols: Option<usize>,
    /// Whether `cols` came from a spread of the folder rather than all of it. A folder
    /// past the footer budget is read at its ends and its middle, so the count is a
    /// floor: shown as `6+` rather than `6`, the way the row count is already shown as
    /// `?` when it is out of reach.
    pub cols_sampled: bool,
    /// Column names, when they were free to obtain. A Parquet footer carries them
    /// alongside the row count, so knowing what is *in* a dataset costs nothing
    /// beyond knowing how big it is.
    pub columns: Vec<String>,
    /// What opening this will cost: where it lives, how it is stored, how it is laid
    /// out. All of it derived from bytes datui already reads.
    pub cost: Cost,
}

/// What pressing Enter on a dataset will actually cost.
///
/// `rows`, `cols` and `size` say what a dataset *is*. None of them say what reading
/// it will do, and the difference is large: 200 MB of zstd-compressed Parquet is two
/// gigabytes in memory, and two gigabytes on a hotel-wifi NFS mount is a different
/// afternoon than two gigabytes on tmpfs.
///
/// Every field here comes from something datui already reads — the mount table, and
/// the same Parquet footer that yields the row count. Nothing here costs an extra
/// byte of the dataset itself.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Cost {
    /// Filesystem or URL scheme: `nfs4`, `ext4`, `tmpfs`, `fuse.sshfs`, `s3`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    /// Bytes once decompressed — what this will occupy, as against what it occupies
    /// on disk.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uncompressed: Option<u64>,
    /// Compression codec, as the file itself names it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub codec: Option<String>,
    /// Row groups. One enormous row group cannot be read in parallel or skipped
    /// through; a thousand tiny ones cost more in overhead than they save.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row_groups: Option<usize>,
    /// Partition layout, for a hive dataset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partitions: Option<Partitions>,
}

/// How a hive dataset is laid out on disk.
///
/// The shape of a partitioned dataset is the first thing anyone asks about it, and
/// the answer is in the directory names — no file needs opening to know it.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Partitions {
    /// Partition keys, outermost first: `["year", "month"]`.
    pub keys: Vec<String>,
    /// Distinct values seen for the outermost key, in sorted order. Bounded, so this
    /// is what was seen rather than necessarily all there is.
    pub first_key_values: Vec<String>,
    /// Directories counted at the outermost level.
    pub count: usize,
    /// The count stopped at a limit; there are more.
    pub more: bool,
}

impl Entry {
    /// A plain directory row — somewhere to step into, with nothing read from it.
    pub fn directory(path: &Path) -> Self {
        Self::new(path.to_path_buf(), EntryKind::Directory)
    }

    /// A file entry with a chosen display name, for tests that need a search result
    /// without running a walk to produce one.
    pub fn for_test(path: &Path, name: &str) -> Self {
        let mut entry = Self::new(path.to_path_buf(), EntryKind::File);
        entry.name = name.to_string();
        entry
    }

    pub(crate) fn new(path: PathBuf, kind: EntryKind) -> Self {
        let name = path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| path.to_string_lossy().into_owned());
        Self {
            path,
            kind,
            name,
            size: None,
            modified: None,
            rows: None,
            cols: None,
            cols_sampled: false,
            columns: Vec::new(),
            cost: Cost::default(),
        }
    }

    /// Attach size and mtime from a directory entry's metadata.
    pub(crate) fn with_fs_metadata(mut self, meta: &std::fs::Metadata) -> Self {
        if meta.is_file() {
            self.size = Some(meta.len());
        }
        self.modified = meta.modified().ok();
        self
    }
}

/// Whether an object key or path is Parquet: named `.parquet`, or a part file with no
/// extension inside a folder named `.parquet`, as Spark and GBIF write them
/// (`occurrence.parquet/000001`). Hidden and job files (`_SUCCESS`, `.crc`) are not.
pub fn is_parquet_key(key: &str) -> bool {
    let key = key.trim_end_matches('/');
    let (folder, name) = match key.rsplit_once('/') {
        Some((folder, name)) => (folder, name),
        None => ("", key),
    };
    if name.starts_with(['_', '.']) {
        return false;
    }
    if name.to_ascii_lowercase().ends_with(".parquet") {
        return true;
    }
    let folder_name = folder.rsplit('/').next().unwrap_or(folder);
    !name.contains('.') && folder_name.to_ascii_lowercase().ends_with(".parquet")
}

#[cfg(test)]
mod parquet_key_tests {
    use super::is_parquet_key;

    #[test]
    fn parquet_without_an_extension_is_known_by_its_folder() {
        assert!(is_parquet_key(
            "occurrence/2026-09-01/occurrence.parquet/000001"
        ));
        assert!(is_parquet_key("data/part-0.parquet"));
        assert!(is_parquet_key("DATA/PART-0.PARQUET"));
        assert!(!is_parquet_key(
            "occurrence/2026-09-01/occurrence.parquet/_SUCCESS"
        ));
        assert!(!is_parquet_key("occurrence.parquet/.part-0.crc"));
        assert!(!is_parquet_key("occurrence/2026-09-01/citation.txt"));
        assert!(!is_parquet_key("notes/000001"));
    }
}

/// Whether a local file is Parquet by its contents: `PAR1` at both ends.
pub fn has_parquet_magic(path: &Path) -> bool {
    use std::io::{Read, Seek, SeekFrom};
    let Ok(mut file) = std::fs::File::open(path) else {
        return false;
    };
    let mut head = [0u8; 4];
    let mut tail = [0u8; 4];
    file.read_exact(&mut head).is_ok()
        && file.seek(SeekFrom::End(-4)).is_ok()
        && file.read_exact(&mut tail).is_ok()
        && &head == b"PAR1"
        && &tail == b"PAR1"
}

/// Whether a path looks like something datui can open.
pub fn is_data_file(path: &Path) -> bool {
    let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
        return false;
    };
    let lower = name.to_ascii_lowercase();
    let mut parts: Vec<&str> = lower.rsplit('.').collect();
    parts.reverse();
    if parts.len() < 2 {
        return false;
    }
    // Walk back past a compression suffix so `sales.csv.gz` still reads as CSV.
    let mut idx = parts.len() - 1;
    if COMPRESSION_EXTENSIONS.contains(&parts[idx]) && idx > 1 {
        idx -= 1;
    }
    DATA_EXTENSIONS.contains(&parts[idx])
}

/// Whether a directory name is a hive partition (`year=2024`).
fn is_partition_dir(path: &Path) -> bool {
    path.file_name()
        .and_then(|n| n.to_str())
        .map(|n| {
            // `key=value`, with a non-empty key. The value may be empty in practice.
            matches!(n.find('='), Some(i) if i > 0)
        })
        .unwrap_or(false)
}

/// Classify a directory without walking it.
///
/// Reads at most [`HIVE_PROBE_LIMIT`] entries: enough to see whether the children
/// are `key=value` partitions or data files, and never enough to stall on a large
/// dataset.
pub fn classify_directory(path: &Path) -> EntryKind {
    // Before anything is counted: a lake table's data files genuinely do agree on a
    // schema, so every rule below says "one table" and is right about the schema and
    // wrong about the rows.
    if let Some(lake) = lake_table(path) {
        return lake;
    }
    let Ok(iter) = std::fs::read_dir(path) else {
        return EntryKind::Directory;
    };

    let mut partitions = 0usize;
    let mut data_files = 0usize;
    let mut seen = 0usize;
    // A multi-file dataset is homogeneous by definition; a folder that merely
    // contains two different spreadsheets is not one.
    let mut extension: Option<String> = None;
    let mut mixed_extensions = false;

    for entry in iter.flatten() {
        let entry_path = entry.path();
        let name = entry.file_name();
        // Skip dotfiles and the marker files data tools leave lying around.
        if name.to_string_lossy().starts_with('.') || name == "_SUCCESS" {
            continue;
        }
        if entry_path.is_dir() {
            if is_partition_dir(&entry_path) {
                partitions += 1;
            }
        } else if is_data_file(&entry_path) {
            data_files += 1;
            let ext = entry_path
                .extension()
                .and_then(|e| e.to_str())
                .map(|e| e.to_ascii_lowercase());
            match (&extension, ext) {
                (None, Some(e)) => extension = Some(e),
                (Some(current), Some(e)) if *current != e => mixed_extensions = true,
                _ => {}
            }
        }
        seen += 1;
        if seen >= HIVE_PROBE_LIMIT {
            break;
        }
    }

    if partitions > 0 && partitions >= data_files {
        return EntryKind::Hive;
    }

    // Require homogeneity *and* that data is what this directory is mostly for.
    // Without the majority test, any folder with a couple of stray CSVs in it would
    // be offered as a dataset, which is worse than useless: it hides the folder.
    let homogeneous = data_files > 1 && !mixed_extensions;
    let mostly_data = data_files * 2 >= seen;
    if homogeneous && mostly_data {
        EntryKind::MultiFile
    } else {
        // Everything else — including a directory holding a single data file — is a
        // place to look inside, not a dataset in its own right.
        EntryKind::Directory
    }
}

/// Entries under `metadata/` to look at before giving up on Iceberg. A table with a
/// long history has thousands, and the newest are not first in any order a directory
/// read promises — but `vN.metadata.json` is written on the first commit and never
/// removed, so one is always there to find.
const ICEBERG_METADATA_PROBE: usize = 64;

/// Whether `path` is the root of a lake table, and which.
///
/// Marker directory names are convention knowledge, which the one-table rule
/// deliberately keeps out: inferring a dataset from filenames is a list that is never
/// finished. These three are a different thing — a declared format with a specified
/// layout, where the marker is part of the spec.
///
/// Named directly rather than found by walking the listing, because a table with sixty
/// data files would not show its log within the probe limit, and which entries a
/// directory read returns first is not something to depend on.
fn lake_table(path: &Path) -> Option<EntryKind> {
    if path.join("_delta_log").is_dir() {
        return Some(EntryKind::Delta);
    }
    if path.join(".hoodie").is_dir() {
        return Some(EntryKind::Hudi);
    }
    // Iceberg's marker is a plain name, so it takes the whole shape: metadata beside
    // data, and a metadata file actually in it. `metadata/` alone is a folder anybody
    // may have.
    let metadata = path.join("metadata");
    if path.join("data").is_dir()
        && metadata.is_dir()
        && std::fs::read_dir(&metadata).is_ok_and(|entries| {
            entries
                .flatten()
                .take(ICEBERG_METADATA_PROBE)
                .any(|e| e.file_name().to_string_lossy().ends_with(".metadata.json"))
        })
    {
        return Some(EntryKind::Iceberg);
    }
    None
}

/// List one directory level, classified. Never recurses.
///
/// Errors are swallowed deliberately: an unreadable or unmounted directory yields an
/// empty listing rather than failing the home screen, and the caller reports
/// availability separately.
pub fn scan_dir(dir: &Path) -> Vec<Entry> {
    scan_dir_bounded(dir).entries
}

/// What one directory listing produced, and whether it saw all of it.
#[derive(Debug, Clone, Default)]
pub struct Scan {
    pub entries: Vec<Entry>,
    /// The directory held more than `MAX_ENTRIES_PER_DIR`; `entries` is a prefix of
    /// it. Worth saying out loud: a listing that silently stops at five thousand
    /// looks identical to a directory that simply has five thousand things in it.
    pub truncated: bool,
}

/// List one directory, doing a bounded amount of work regardless of what is in it.
///
/// The cost is one `read_dir` and a `stat` per entry, bounded by
/// [`MAX_ENTRIES_PER_DIR`], and nothing per subdirectory at all.
///
/// **Nothing here is classified.** Telling a hive dataset from a plain folder means
/// reading the folder, which is a round trip apiece on a share — so no listing pays
/// for it, however small. Every subdirectory comes back [`EntryKind::Unknown`], which
/// claims nothing, and is looked into later from the viewport, a batch at a time, by
/// whoever is actually reading the rows.
///
/// That is what makes a row's label a fact about the row. Classifying the first
/// sixty-four subdirectories and calling every identical one after them a plain
/// directory made it a fact about position instead; classifying them only when a
/// listing is small enough moved the arbitrariness rather than removing it, since two
/// directories of the same folders would still disagree about what to call them.
pub fn scan_dir_bounded(dir: &Path) -> Scan {
    let Ok(iter) = std::fs::read_dir(dir) else {
        return Scan::default();
    };

    let mut entries = Vec::new();
    let mut seen = 0usize;
    let mut truncated = false;

    // One past the cap: enough to know more exists without paying to process it.
    for dir_entry in iter.flatten().take(MAX_ENTRIES_PER_DIR + 1) {
        seen += 1;
        if seen > MAX_ENTRIES_PER_DIR {
            truncated = true;
            break;
        }

        let path = dir_entry.path();
        let name = dir_entry.file_name();
        if name.to_string_lossy().starts_with('.') {
            continue;
        }

        let Ok(meta) = dir_entry.metadata() else {
            continue;
        };

        let kind = if meta.is_dir() {
            EntryKind::Unknown
        } else if meta.is_file() && is_data_file(&path) {
            EntryKind::File
        } else {
            // Not data, not a directory, or not a regular file. A FIFO named
            // `x.parquet` is a listing entry datui must never offer to open.
            continue;
        };

        entries.push(Entry::new(path, kind).with_fs_metadata(&meta));
    }

    sort_entries(&mut entries);
    Scan { entries, truncated }
}

/// Datasets first, then directories; each group alphabetical.
///
/// Recency is a better sort for recents, but a directory listing is a place you
/// scan by name, so name order wins here.
///
/// A row nothing has looked into yet sorts with the directories, although
/// [`EntryKind::is_dataset`] offers it as openable. In a fresh listing that is every
/// subdirectory, so what this amounts to there is files first and folders after —
/// and it is the one ordering a folder can be given before anything is known about
/// it, since it is where the row lands if the folder turns out to be a plain one.
///
/// Which is the point: a kind arriving later never moves the row, because a row that
/// moves out from under the cursor while you are scrolling is worse than a label that
/// is late.
fn sort_entries(entries: &mut [Entry]) {
    entries.sort_by(|a, b| {
        let group = |k: EntryKind| if k.is_known_dataset() { 0 } else { 1 };
        group(a.kind).cmp(&group(b.kind)).then_with(|| {
            a.name
                .to_ascii_lowercase()
                .cmp(&b.name.to_ascii_lowercase())
        })
    });
}

/// Upper bound on Parquet footers read to size a multi-file or hive dataset.
///
/// Two partitions is cheap; five thousand is not, and a home screen that stalls on
/// the biggest dataset is worse than one that admits it does not know. Past this
/// bound the count is left blank rather than reported as a partial total.
const MAX_FOOTERS_PER_DATASET: usize = 64;

/// Fill in row and column counts for a dataset, from Parquet footers only.
///
/// Handles a single file, and sums a bounded number of files for hive and multi-file
/// datasets. Anything not backed by Parquet keeps `None`, which the UI shows as an
/// honest blank.
pub fn enrich(entry: &mut Entry) {
    match entry.kind {
        EntryKind::File => enrich_parquet(entry),
        EntryKind::Hive | EntryKind::MultiFile => enrich_dataset(entry),
        // Nothing to read for a plain directory, and nothing that *may* be read for
        // one that has not been looked at. Nor for a lake table: summing the footers
        // under one counts tombstoned rows, every rewritten version and both sides of
        // a compaction, which is the whole reason it is not offered as a dataset.
        EntryKind::Directory | EntryKind::Unknown => {}
        EntryKind::Delta | EntryKind::Iceberg | EntryKind::Hudi => {}
    }
}

/// Sum footers across a bounded set of Parquet files under `entry`.
fn enrich_dataset(entry: &mut Entry) {
    // The partition layout comes from directory names, so it is knowable even for a
    // dataset far too large to count the rows of — which is exactly the dataset whose
    // shape you most want described before opening it.
    if entry.kind == EntryKind::Hive {
        entry.cost.partitions = partition_layout(&entry.path);
    }

    // The stat'ed size of a dataset directory is its own inode: a couple of hundred
    // bytes that have nothing to do with the terabyte inside it. Dropped up front and
    // restored only if the files are actually totalled, so no path out of here can
    // leave it behind to be read as an answer.
    entry.size = None;

    let mut files = Vec::new();
    collect_parquet_files(&entry.path, 0, &mut files);
    if files.is_empty() || files.len() > MAX_FOOTERS_PER_DATASET {
        // Whether these are one table is still worth asking, and it does not need
        // every footer: three files spread across the folder answer it. Without this a
        // folder large enough to be past the counting limit would skip the check
        // entirely, which is backwards — the more tables it holds, the more a union of
        // them costs.
        let sampled = sample_footers(&files);
        let names: Vec<Vec<String>> = sampled.iter().map(column_names).collect();
        if entry.kind == EntryKind::MultiFile && !agree_on_a_schema(&names) {
            entry.columns = union_of(&names);
            downgrade_to_directory(entry);
            return;
        }
        // Still worth knowing the shape, even when the row count is out of reach.
        if let Some(meta) = sampled.first() {
            // Three files rather than the first, because a folder written over time
            // keeps its newest columns in its last file — and the first is where a
            // dataset that grew is narrowest. Still a sample and not a total: the
            // count beside it is already `?`.
            entry.columns = union_of(&names);
            entry.cols =
                Some(union_of(&sampled.iter().map(top_level_names).collect::<Vec<_>>()).len());
            entry.cols_sampled = true;
            // From one file, so it describes how the dataset is written rather
            // than its total: codec and row-group sizing are a property of the
            // writer and are uniform in practice.
            physical_facts(meta, &mut entry.cost);
            entry.cost.uncompressed = None;
        }
        return;
    }

    let mut rows = 0usize;
    let mut bytes = 0u64;
    // Every column any file has, in the order they first appear — not the first
    // file's. A dataset whose columns grew over time reported the shape it was born
    // with: Bitcoin transactions, whose `inputs` gained `address` and then
    // `txinwitness`, answered no to "which of these has `txinwitness`?".
    let mut columns: Vec<String> = Vec::new();
    let mut seen_columns = std::collections::HashSet::new();
    // The columns a reader sees, unioned the same way. Kept beside the leaves rather
    // than derived from them, because a leaf path cannot say whether its dots are
    // nesting or part of a name. See [`top_level_names`].
    let mut top_level: Vec<String> = Vec::new();
    let mut seen_top_level = std::collections::HashSet::new();
    let mut per_file: Vec<Vec<String>> = Vec::with_capacity(files.len());
    let mut cost = Cost::default();
    let mut uncompressed = 0u64;
    let mut row_groups = 0usize;
    for file in &files {
        let Some(meta) = crate::widgets::info::read_parquet_metadata(file) else {
            return; // A file we cannot read makes the total a guess; report nothing.
        };
        rows += meta.num_rows;
        let names = column_names(&meta);
        for name in &names {
            if seen_columns.insert(name.clone()) {
                columns.push(name.clone());
            }
        }
        for name in top_level_names(&meta) {
            if seen_top_level.insert(name.clone()) {
                top_level.push(name);
            }
        }
        // The columns a reader sees, not the leaves the footer names: see
        // [`crate::schema_union::top_level_columns`].
        per_file.push(crate::schema_union::top_level_columns(&names));
        let mut per_file = Cost::default();
        physical_facts(&meta, &mut per_file);
        uncompressed += per_file.uncompressed.unwrap_or(0);
        row_groups += per_file.row_groups.unwrap_or(0);
        if cost.codec.is_none() {
            cost.codec = per_file.codec;
        }
        if let Ok(m) = std::fs::metadata(file) {
            bytes += m.len();
        }
    }
    // The footers are read by now, so whether these files are one table is known
    // rather than guessed. A folder of separate tables is a place to look inside: its
    // row count is the sum of unrelated things, its column count belongs to whichever
    // file happened to be read first, and opening it unions tables that share nothing.
    //
    // Only `multi` is reconsidered. A `key=value` layout says what the writer meant,
    // and a hive folder's files hold the same table by construction.
    if entry.kind == EntryKind::MultiFile && !crate::schema_union::is_one_table(&per_file) {
        entry.size = Some(bytes);
        // Nothing here is one table's shape, but the names are what the folder holds,
        // and searching the home screen by column should still find the folder that
        // has one.
        entry.columns = columns;
        downgrade_to_directory(entry);
        return;
    }

    entry.rows = Some(rows);
    entry.cols = Some(top_level.len());
    entry.size = Some(bytes);
    entry.columns = columns;
    cost.uncompressed = (uncompressed > 0).then_some(uncompressed);
    cost.row_groups = (row_groups > 0).then_some(row_groups);
    cost.partitions = entry.cost.partitions.take();
    entry.cost = cost;
}

/// The footers at the ends and the middle of a folder too large to read every one of.
///
/// The ends and the middle, because keys and filenames sort: a folder written table by
/// table can easily start with several files of the same table, so its head answers
/// nothing. The last file earns its place twice over — in a folder written over time it
/// is the newest, which is where a column added last year is.
fn sample_footers(files: &[PathBuf]) -> Vec<crate::widgets::info::ParquetMetadataCache> {
    if files.is_empty() {
        return Vec::new();
    }
    let mut picks = vec![0, files.len() / 2, files.len() - 1];
    picks.dedup();
    picks
        .iter()
        .filter_map(|i| files.get(*i))
        .filter_map(|file| crate::widgets::info::read_parquet_metadata(file))
        .collect()
}

/// Whether a spread of a folder's files agree on a schema.
///
/// Fewer than two readable footers decide nothing, and the folder keeps the kind its
/// names suggested.
fn agree_on_a_schema(sampled: &[Vec<String>]) -> bool {
    let per_file: Vec<Vec<String>> = sampled
        .iter()
        .map(|names| crate::schema_union::top_level_columns(names))
        .collect();
    per_file.len() < 2 || crate::schema_union::is_one_table(&per_file)
}

/// A folder whose files turned out to be separate tables is a place to look inside.
///
/// Its row count would be the sum of unrelated things and its column count would
/// belong to whichever file was read first, so neither is reported.
fn downgrade_to_directory(entry: &mut Entry) {
    entry.kind = EntryKind::Directory;
    entry.rows = None;
    entry.cols = None;
    entry.cols_sampled = false;
    entry.cost = Cost {
        partitions: entry.cost.partitions.take(),
        ..Cost::default()
    };
}

/// The columns a reader sees: the schema's own top-level fields.
///
/// Not the leaves a footer names, and not those leaves split on a dot either. Leaves
/// counted directly double for a folder whose writer changed — the same nested column
/// written by parquet-mr and by Arrow gives `inputs.list.element.address` in one file and
/// `inputs.bag.array_element.address` in the other, and a union of leaf paths holds both.
/// Splitting the dotted path fixes that and breaks something else: a column literally
/// named `user.id` is one column, and so is a struct `user` with a field `id`, and the
/// string cannot tell them apart.
///
/// The schema knows. `fields()` is the root's own children, which is what a struct counts
/// as here, what `schema_preview` lists in the details pane, and what the table shows.
fn top_level_names(meta: &crate::widgets::info::ParquetMetadataCache) -> Vec<String> {
    meta.schema_descr
        .fields()
        .iter()
        .map(|field| field.name().to_string())
        .collect()
}

/// Every column name any of the files has, in the order they first appear.
fn union_of(per_file: &[Vec<String>]) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    per_file
        .iter()
        .flatten()
        .filter(|name| seen.insert(name.as_str()))
        .cloned()
        .collect()
}

/// Names taken from one directory before reading the rest stops being worth the walk.
/// Past it the spread `sample_footers` takes is over the names this listing saw rather
/// than over the folder — the bug this bound is a compromise with — and the row count is
/// long out of reach either way. Twenty thousand is the size `schema_union`'s own
/// measurements take as the large case.
const MAX_NAMES_PER_DIR: usize = 20_000;

/// Collect Parquet files under `dir`, breadth-bounded and depth-bounded, stopping
/// once the cap is exceeded so a huge dataset costs the same as a small one.
fn collect_parquet_files(dir: &Path, depth: u8, out: &mut Vec<PathBuf>) {
    if depth > 4 || out.len() > MAX_FOOTERS_PER_DATASET {
        return;
    }
    let Ok(iter) = std::fs::read_dir(dir) else {
        return;
    };
    let mut subdirs = Vec::new();
    // Every candidate in this directory, sorted before any is kept — not the first
    // handful the directory read happened to return. A directory read gives its entries
    // in whatever order the filesystem holds them, and every caller of this list reads
    // order as meaning something: the ends and the middle are the spread
    // `sample_footers` takes, and the last file is the newest in a folder written over
    // time. Truncating first and sorting after would sort an arbitrary subset, which is
    // the same wrong answer with the appearance of an order.
    //
    // Names only here: the `is_regular_file` stat that used to run on every candidate
    // now runs only on the ones actually kept. Reading the whole directory to sort it is
    // not free either — one `getdents` walk and one sort, where the old shape stopped at
    // the sixty-fifth entry — and that is what the sample meaning what it says costs.
    let mut files = Vec::new();
    for entry in iter.flatten() {
        let path = entry.path();
        // A table format's own files are not the table's. Delta writes its checkpoints
        // as Parquet holding the table's own columns, so counted here the home screen
        // promises a row count the table does not have — which is what opening it then
        // disagrees with. The same test the open makes, so the two agree.
        if path
            .file_name()
            .map(|n| n.to_string_lossy())
            .as_deref()
            .is_some_and(crate::schema_union::is_bookkeeping)
        {
            continue;
        }
        // The type the directory read already returned, rather than a `stat` per entry:
        // a folder of two hundred thousand files is visited whole here, and `is_dir` on
        // every one of them is the cost of doing so. A symlink still gets the stat,
        // because whether to walk into one is a question `d_type` cannot answer.
        let is_dir = match entry.file_type() {
            Ok(kind) if kind.is_symlink() => path.is_dir(),
            Ok(kind) => kind.is_dir(),
            Err(_) => path.is_dir(),
        };
        if is_dir {
            subdirs.push(path);
        } else if path
            .extension()
            .and_then(|e| e.to_str())
            .map(|e| e.eq_ignore_ascii_case("parquet"))
            .unwrap_or(false)
        {
            files.push(path);
            if files.len() >= MAX_NAMES_PER_DIR {
                break;
            }
        }
    }
    files.sort();
    // One past the budget is deliberate: the callers read `len() > MAX` as "too many to
    // count", so the list has to be able to say so.
    let room = (MAX_FOOTERS_PER_DATASET + 1).saturating_sub(out.len());
    out.extend(files.into_iter().filter(|p| is_regular_file(p)).take(room));
    if out.len() > MAX_FOOTERS_PER_DATASET {
        return;
    }
    subdirs.sort();
    for sub in subdirs {
        collect_parquet_files(&sub, depth + 1, out);
        if out.len() > MAX_FOOTERS_PER_DATASET {
            return;
        }
    }
}

/// Fill in row and column counts for a Parquet file from its footer.
///
/// Free in the sense that matters: no column data is read. Non-Parquet formats have
/// no equivalent — a CSV's row count cannot be known without scanning it — so those
/// entries keep `None`, and the UI shows the absence honestly rather than guessing.
pub fn enrich_parquet(entry: &mut Entry) {
    if entry.kind != EntryKind::File {
        return;
    }
    let is_parquet = entry
        .path
        .extension()
        .and_then(|e| e.to_str())
        .map(|e| e.eq_ignore_ascii_case("parquet"))
        .unwrap_or(false);
    if !is_parquet {
        return;
    }
    if !is_regular_file(&entry.path) {
        return;
    }
    if let Some(meta) = crate::widgets::info::read_parquet_metadata(&entry.path) {
        entry.rows = Some(meta.num_rows);
        entry.columns = column_names(&meta);
        // The columns a reader sees, as a folder's row reports them: `schema_descr`
        // names the leaves, so a file with one struct of three fields counted four and
        // then listed two in the pane beside it. See [`top_level_names`].
        entry.cols = Some(top_level_names(&meta).len());
        physical_facts(&meta, &mut entry.cost);
    }
}

/// Pull layout and compression out of a footer that has already been read.
///
/// Every one of these was being parsed and thrown away. They are the difference
/// between knowing how big a file is and knowing what reading it will do.
pub fn physical_facts(meta: &crate::widgets::info::ParquetMetadataCache, cost: &mut Cost) {
    if meta.row_groups.is_empty() {
        return;
    }
    cost.row_groups = Some(meta.row_groups.len());

    let mut uncompressed: u64 = 0;
    let mut codecs: Vec<String> = Vec::new();
    for rg in &meta.row_groups {
        uncompressed = uncompressed.saturating_add(rg.total_byte_size() as u64);
        for cc in rg.parquet_columns() {
            let codec = format!("{:?}", cc.compression()).to_lowercase();
            if !codecs.contains(&codec) {
                codecs.push(codec);
            }
        }
    }
    if uncompressed > 0 {
        cost.uncompressed = Some(uncompressed);
    }
    // A file usually uses one codec throughout. When it does not, say so rather than
    // picking one and implying uniformity that is not there.
    cost.codec = match codecs.len() {
        0 => None,
        1 => Some(codecs.remove(0)),
        n => Some(format!("mixed ({n})")),
    };
}

/// Outermost directories to look at when describing a hive dataset's partitioning.
///
/// Enough to name the keys and show the shape of the first one; bounded because a
/// dataset partitioned by day over a decade has thousands, and counting all of them
/// to print "3,653" is not worth a second of anyone's time on a network share.
const MAX_PARTITION_DIRS: usize = 512;

/// Describe how a hive dataset is partitioned, from directory names alone.
pub fn partition_layout(dir: &Path) -> Option<Partitions> {
    let iter = std::fs::read_dir(dir).ok()?;
    let mut values: Vec<String> = Vec::new();
    let mut keys: Vec<String> = Vec::new();
    let mut count = 0usize;
    let mut more = false;

    for entry in iter.flatten() {
        if count >= MAX_PARTITION_DIRS {
            more = true;
            break;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        let Some((key, value)) = name.split_once('=') else {
            continue;
        };
        if !entry.path().is_dir() {
            continue;
        }
        if keys.is_empty() {
            keys.push(key.to_string());
            // Only the first partition directory is descended into, for the nested
            // key names. One is representative, and a hive dataset that disagrees
            // with itself about its own schema is not a dataset datui can help with.
            keys.extend(nested_keys(&entry.path()));
        }
        values.push(value.to_string());
        count += 1;
    }

    if keys.is_empty() {
        return None;
    }
    values.sort();
    values.dedup();
    Some(Partitions {
        keys,
        first_key_values: values,
        count,
        more,
    })
}

/// Partition keys below `dir`, following the first child at each level.
fn nested_keys(dir: &Path) -> Vec<String> {
    let mut keys = Vec::new();
    let mut current = dir.to_path_buf();
    // Bounded: a hive path deeper than this is pathological, and each level costs a
    // directory read.
    for _ in 0..6 {
        let Ok(iter) = std::fs::read_dir(&current) else {
            break;
        };
        let Some(child) = iter
            .flatten()
            .find(|e| e.file_name().to_string_lossy().contains('=') && e.path().is_dir())
        else {
            break;
        };
        let name = child.file_name().to_string_lossy().into_owned();
        let Some((key, _)) = name.split_once('=') else {
            break;
        };
        keys.push(key.to_string());
        current = child.path();
    }
    keys
}

/// Render a byte count compactly for a listing (`340 MB`).
pub fn format_size(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{} {}", bytes, UNITS[0])
    } else if value >= 100.0 {
        format!("{:.0} {}", value, UNITS[unit])
    } else {
        format!("{:.1} {}", value, UNITS[unit])
    }
}

/// Render a row count compactly (`2.4M`).
pub fn format_rows(rows: usize) -> String {
    let r = rows as f64;
    if rows >= 1_000_000_000 {
        format!("{:.1}B", r / 1e9)
    } else if rows >= 1_000_000 {
        format!("{:.1}M", r / 1e6)
    } else if rows >= 10_000 {
        format!("{:.0}k", r / 1e3)
    } else if rows >= 1_000 {
        // Below ten thousand the exact count fits and rounding actively misleads:
        // 3,653 daily observations is ten years of data, and "4k" is not.
        let mut out = String::new();
        let digits = rows.to_string();
        for (i, c) in digits.chars().enumerate() {
            if i > 0 && (digits.len() - i).is_multiple_of(3) {
                out.push(',');
            }
            out.push(c);
        }
        out
    } else {
        rows.to_string()
    }
}

/// Render "how long ago" compactly (`2d`, `3h`).
pub fn format_age(t: std::time::SystemTime) -> String {
    let Ok(elapsed) = t.elapsed() else {
        return String::new();
    };
    let secs = elapsed.as_secs();
    if secs < 60 {
        "now".to_string()
    } else if secs < 3600 {
        format!("{}m", secs / 60)
    } else if secs < 86_400 {
        format!("{}h", secs / 3600)
    } else if secs < 86_400 * 365 {
        format!("{}d", secs / 86_400)
    } else {
        format!("{}y", secs / (86_400 * 365))
    }
}

/// Column name and type, for the home screen's preview pane.
pub type SchemaPreview = Vec<(String, polars::prelude::DataType)>;

/// Find the first Parquet file at or under `dir`, without walking the whole tree.
///
/// Bounded on both breadth and depth so a hive dataset with thousands of partitions
/// costs the same as one with three.
fn first_parquet_under(dir: &Path, depth: u8) -> Option<PathBuf> {
    if depth > 4 {
        return None;
    }
    let mut subdirs = Vec::new();
    for entry in std::fs::read_dir(dir).ok()?.flatten().take(64) {
        let path = entry.path();
        if path.is_dir() {
            subdirs.push(path);
        } else if path
            .extension()
            .and_then(|e| e.to_str())
            .map(|e| e.eq_ignore_ascii_case("parquet"))
            .unwrap_or(false)
            && is_regular_file(&path)
        {
            return Some(path);
        }
    }
    subdirs.sort();
    subdirs
        .into_iter()
        .take(4)
        .find_map(|d| first_parquet_under(&d, depth + 1))
}

/// Column names from a Parquet footer.
pub fn column_names(meta: &crate::widgets::info::ParquetMetadataCache) -> Vec<String> {
    meta.schema_descr
        .columns()
        .iter()
        .map(|c| c.path_in_schema.join("."))
        .collect()
}

/// Whether `path` is a regular file that is safe to open.
///
/// Opening a FIFO blocks until a writer appears — indefinitely, for a named pipe
/// nobody is writing to — and opening a device or a socket does something stranger
/// still. A directory listing happily reports any of these with a `.parquet` name,
/// so every read here is gated on the kind first. `symlink_metadata` follows nothing
/// and `metadata` only stats, so neither can block the way an open can.
fn is_regular_file(path: &Path) -> bool {
    std::fs::metadata(path)
        .map(|m| m.file_type().is_file())
        .unwrap_or(false)
}

/// Read a dataset's column names and types without reading any data.
///
/// Parquet only — a CSV's schema cannot be known without scanning it, and doing that
/// for every row the cursor passes over would defeat the point of a preview. Returns
/// `None` for anything else, and the UI says so rather than guessing.
pub fn schema_preview(entry: &Entry) -> Option<SchemaPreview> {
    // `SerReader` is what brings `ParquetReader::new` into scope.
    use polars::prelude::{ParquetReader, Schema, SchemaExt, SerReader};

    let file_path = match entry.kind {
        EntryKind::File => {
            let is_parquet = entry
                .path
                .extension()
                .and_then(|e| e.to_str())
                .map(|e| e.eq_ignore_ascii_case("parquet"))
                .unwrap_or(false);
            if !is_parquet {
                return None;
            }
            entry.path.clone()
        }
        EntryKind::Hive | EntryKind::MultiFile => first_parquet_under(&entry.path, 0)?,
        EntryKind::Directory | EntryKind::Unknown => return None,
        // One data file's schema is not the table's: Iceberg field IDs and Delta
        // column mapping both mean a renamed column reads as two.
        EntryKind::Delta | EntryKind::Iceberg | EntryKind::Hudi => return None,
    };

    if !is_regular_file(&file_path) {
        return None;
    }
    let file = std::fs::File::open(&file_path).ok()?;
    let mut reader = ParquetReader::new(file);
    let arrow_schema = reader.schema().ok()?;
    let schema = Schema::from_arrow_schema(arrow_schema.as_ref());
    Some(
        schema
            .iter()
            .map(|(name, dtype)| (name.to_string(), dtype.clone()))
            .collect(),
    )
}

#[cfg(test)]
mod classification_tests {
    use super::*;
    use polars::prelude::*;

    /// Write `columns` as a one-row Parquet file named `name` under `dir`.
    fn write(dir: &Path, name: &str, columns: &[&str]) {
        let mut frame = DataFrame::new(
            1,
            columns
                .iter()
                .map(|c| Column::new((*c).into(), &[1i32]))
                .collect::<Vec<_>>(),
        )
        .unwrap();
        let file = std::fs::File::create(dir.join(name)).unwrap();
        ParquetWriter::new(file).finish(&mut frame).unwrap();
    }

    /// A one-row Parquet file with a struct column, so the leaves and the columns a
    /// reader sees are genuinely different things rather than dots in a name.
    fn write_nested(dir: &Path, name: &str, struct_name: &str, fields: &[&str]) {
        let inner = DataFrame::new(
            1,
            fields
                .iter()
                .map(|f| Column::new((*f).into(), &[1i32]))
                .collect::<Vec<_>>(),
        )
        .unwrap();
        let nested = inner
            .into_struct(struct_name.into())
            .into_series()
            .into_column();
        let mut frame = DataFrame::new(1, vec![Column::new("id".into(), &[1i32]), nested]).unwrap();
        let file = std::fs::File::create(dir.join(name)).unwrap();
        ParquetWriter::new(file).finish(&mut frame).unwrap();
    }

    fn measured(dir: &Path) -> Entry {
        let mut entry = Entry {
            path: dir.to_path_buf(),
            kind: classify_directory(dir),
            name: dir.file_name().unwrap().to_string_lossy().into_owned(),
            size: None,
            modified: None,
            rows: None,
            cols: None,
            cols_sampled: false,
            columns: Vec::new(),
            cost: Cost::default(),
        };
        enrich(&mut entry);
        entry
    }

    /// The shape that prompted this: one Parquet file per table, sharing an extension
    /// and nothing else. Named for what it is rather than what it is called, because
    /// the filenames are exactly what cannot decide it.
    #[test]
    fn a_folder_of_separate_tables_is_a_directory() {
        let dir = tempfile::tempdir().unwrap();
        write(
            dir.path(),
            "circuits.parquet",
            &["circuit_id", "lat", "lng"],
        );
        write(
            dir.path(),
            "drivers.parquet",
            &["driver_id", "code", "nationality"],
        );
        write(
            dir.path(),
            "laps.parquet",
            &["lap", "position", "time_millis"],
        );

        assert_eq!(
            classify_directory(dir.path()),
            EntryKind::MultiFile,
            "the filenames alone still say multi"
        );
        let entry = measured(dir.path());
        assert_eq!(
            entry.kind,
            EntryKind::Directory,
            "reading the footers says otherwise"
        );
        assert_eq!(
            entry.rows, None,
            "a sum across separate tables is not a row count"
        );
        assert_eq!(entry.cols, None);
    }

    /// The rows of one table split across files, which is what `multi` is for.
    #[test]
    fn a_folder_of_one_table_stays_a_dataset() {
        let dir = tempfile::tempdir().unwrap();
        for part in 0..3 {
            write(
                dir.path(),
                &format!("part-0000{part}.parquet"),
                &["id", "ts", "amount"],
            );
        }

        let entry = measured(dir.path());
        assert_eq!(entry.kind, EntryKind::MultiFile);
        assert_eq!(entry.rows, Some(3));
        assert_eq!(entry.cols, Some(3));
    }

    /// A dataset whose columns changed over time is still one dataset. This is the
    /// case a rule about shared columns gets wrong: the older files have a third of
    /// what the newest one does.
    #[test]
    fn a_dataset_that_gained_columns_stays_a_dataset() {
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), "2009.parquet", &["id", "ts"]);
        write(dir.path(), "2015.parquet", &["id", "ts", "fee"]);
        write(
            dir.path(),
            "2025.parquet",
            &["id", "ts", "fee", "witness", "address", "value"],
        );

        let entry = measured(dir.path());
        assert_eq!(entry.kind, EntryKind::MultiFile);
        assert_eq!(entry.rows, Some(3));
        assert_eq!(
            entry.columns,
            vec!["id", "ts", "fee", "witness", "address", "value"],
            "every column any file has, in the order they first appear — not the \
             2009 shape"
        );
        assert_eq!(entry.cols, Some(6), "and the count is of those");
    }

    /// The same, for a hive tree: the row is the dataset's columns, not one
    /// partition's.
    #[test]
    fn a_hive_dataset_that_gained_columns_reports_all_of_them() {
        let dir = tempfile::tempdir().unwrap();
        for (part, columns) in [
            ("year=2009", &["id", "ts"][..]),
            ("year=2025", &["id", "ts", "address"][..]),
        ] {
            let sub = dir.path().join(part);
            std::fs::create_dir_all(&sub).unwrap();
            write(&sub, "part-0.parquet", columns);
        }

        let entry = measured(dir.path());
        assert_eq!(entry.kind, EntryKind::Hive);
        assert_eq!(entry.columns, vec!["id", "ts", "address"]);
        assert_eq!(entry.cols, Some(3));
    }

    /// Past the counting limit the columns come from a spread of the folder rather
    /// than its head, because a folder written over time is narrowest at the start.
    #[test]
    fn a_folder_too_large_to_count_still_reports_the_columns_it_gained() {
        let dir = tempfile::tempdir().unwrap();
        for part in 0..MAX_FOOTERS_PER_DATASET + 1 {
            let mut columns = vec!["id".to_string(), "ts".to_string()];
            if part > MAX_FOOTERS_PER_DATASET / 2 {
                columns.push("address".to_string());
            }
            let refs: Vec<&str> = columns.iter().map(String::as_str).collect();
            write(dir.path(), &format!("part-{part:03}.parquet"), &refs);
        }

        let entry = measured(dir.path());
        assert_eq!(entry.kind, EntryKind::MultiFile, "still one table");
        assert_eq!(entry.rows, None, "too many files to count");
        assert!(
            entry.columns.contains(&"address".to_string()),
            "the column the dataset gained is in the row: {:?}",
            entry.columns
        );
    }

    /// A lake table's data files agree on a schema, so the one-table rule says `multi`
    /// and is right about the schema and wrong about the rows: the files a delete
    /// tombstoned are still on disk, every rewritten version is here together, and
    /// compaction leaves both sides in place.
    #[test]
    fn a_lake_table_is_not_a_folder_of_parquet_files() {
        for (marker, expected) in [
            ("_delta_log", EntryKind::Delta),
            (".hoodie", EntryKind::Hudi),
        ] {
            let dir = tempfile::tempdir().unwrap();
            write(dir.path(), "part-0.parquet", &["id", "amount"]);
            write(dir.path(), "part-1.parquet", &["id", "amount"]);
            write(dir.path(), "part-2.parquet", &["id", "amount"]);
            let log = dir.path().join(marker);
            std::fs::create_dir_all(&log).unwrap();
            std::fs::write(log.join("00000000000000000000.json"), b"{}").unwrap();

            assert_eq!(
                classify_directory(dir.path()),
                expected,
                "{marker} says what this folder is"
            );
            let entry = measured(dir.path());
            assert_eq!(entry.kind, expected);
            assert_eq!(
                entry.rows, None,
                "and no row count is claimed for it: summing the footers would count \
                 the rows the log says are gone"
            );
            assert!(!entry.kind.is_dataset(), "it does not open as one table");
        }
    }

    /// Iceberg's marker is a plain name, so it takes the whole shape rather than the
    /// name alone.
    #[test]
    fn an_iceberg_root_is_metadata_beside_data() {
        let iceberg = tempfile::tempdir().unwrap();
        let data = iceberg.path().join("data");
        let metadata = iceberg.path().join("metadata");
        std::fs::create_dir_all(&data).unwrap();
        std::fs::create_dir_all(&metadata).unwrap();
        write(&data, "00000-0-abc.parquet", &["id", "amount"]);
        write(&data, "00001-0-def.parquet", &["id", "amount"]);
        std::fs::write(metadata.join("v2.metadata.json"), b"{}").unwrap();
        std::fs::write(metadata.join("snap-1.avro"), b"x").unwrap();
        assert_eq!(classify_directory(iceberg.path()), EntryKind::Iceberg);

        // A folder that merely has those names is not a table.
        let plain = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(plain.path().join("data")).unwrap();
        std::fs::create_dir_all(plain.path().join("metadata")).unwrap();
        std::fs::write(plain.path().join("metadata/notes.txt"), b"x").unwrap();
        assert_eq!(
            classify_directory(plain.path()),
            EntryKind::Directory,
            "no *.metadata.json, so no Iceberg table"
        );

        let no_data = tempfile::tempdir().unwrap();
        let metadata = no_data.path().join("metadata");
        std::fs::create_dir_all(&metadata).unwrap();
        std::fs::write(metadata.join("v1.metadata.json"), b"{}").unwrap();
        write(no_data.path(), "part-0.parquet", &["id"]);
        write(no_data.path(), "part-1.parquet", &["id"]);
        assert_eq!(
            classify_directory(no_data.path()),
            EntryKind::MultiFile,
            "metadata with no data/ beside it is somebody's folder, not a table root"
        );
    }

    /// A single file counts its columns the same way a folder does, and both count what
    /// opening it shows.
    ///
    /// `enrich_parquet` read `schema_descr.columns()`, which is the leaf list — so a file
    /// with one struct of two fields said `columns 3` above a schema list of two, and a
    /// folder holding only that file said something different again.
    #[test]
    fn a_file_and_a_folder_of_it_count_the_same_columns() {
        let dir = tempfile::tempdir().unwrap();
        write_nested(dir.path(), "one.parquet", "inputs", &["address", "value"]);

        let mut file = Entry::new(dir.path().join("one.parquet"), EntryKind::File);
        enrich(&mut file);
        assert_eq!(
            file.cols,
            Some(2),
            "`id` and `inputs`, which is what opening it shows: {:?}",
            file.columns
        );
        assert!(
            file.columns.iter().any(|c| c == "inputs.address"),
            "the leaves are still searchable: {:?}",
            file.columns
        );

        write_nested(dir.path(), "two.parquet", "inputs", &["address", "value"]);
        let folder = measured(dir.path());
        assert_eq!(folder.kind, EntryKind::MultiFile);
        assert_eq!(
            folder.cols, file.cols,
            "and a folder of them says the same number"
        );
    }

    /// Dots in a column's own name are not nesting, and are not counted as if they were.
    ///
    /// The obvious fix for the leaf problem — split the dotted path and count the roots —
    /// gets this wrong: `user.id` and `user.name` written by a flattening export are two
    /// columns, not one. The schema says which is which; the string cannot.
    #[test]
    fn a_dotted_column_name_is_its_own_column() {
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), "flat.parquet", &["id", "user.id", "user.name"]);

        let mut file = Entry::new(dir.path().join("flat.parquet"), EntryKind::File);
        enrich(&mut file);
        assert_eq!(file.cols, Some(3), "three columns: {:?}", file.columns);
    }

    /// A folder whose files encode the same nested column differently counts it once.
    ///
    /// The union is over leaf paths, and the same nested column written by parquet-mr and
    /// by Arrow gives different leaves — so the row reported roughly twice the width of a
    /// folder `is_one_table` had just called one dataset. Counted from each file's own
    /// root fields, the two spellings are one `inputs` whatever the leaves under it are.
    #[test]
    fn a_writer_change_does_not_double_the_column_count() {
        let dir = tempfile::tempdir().unwrap();
        write_nested(dir.path(), "old.parquet", "inputs", &["address"]);
        // The same column, one field wider, as a later writer left it.
        write_nested(dir.path(), "new.parquet", "inputs", &["address", "value"]);

        let entry = measured(dir.path());
        assert_eq!(entry.kind, EntryKind::MultiFile, "still one table");
        assert_eq!(
            entry.cols,
            Some(2),
            "one `inputs`, not one per shape of it: {:?}",
            entry.columns
        );
        assert!(
            entry.columns.len() > 2,
            "while every leaf stays searchable: {:?}",
            entry.columns
        );
    }

    /// A count read from a spread of a folder rather than all of it says it is a floor.
    #[test]
    fn a_sampled_column_count_says_it_is_a_floor() {
        let dir = tempfile::tempdir().unwrap();
        for part in 0..MAX_FOOTERS_PER_DATASET * 2 {
            write(
                dir.path(),
                &format!("part-{part:04}.parquet"),
                &["id", "ts"],
            );
        }
        let entry = measured(dir.path());
        assert_eq!(entry.rows, None, "too many files to count");
        assert!(entry.cols.is_some(), "but the width is still worth having");
        assert!(
            entry.cols_sampled,
            "and it is marked as the floor it is, not presented as a total"
        );

        // A folder small enough to read every footer of claims no such thing.
        let small = tempfile::tempdir().unwrap();
        write(small.path(), "a.parquet", &["id", "ts"]);
        write(small.path(), "b.parquet", &["id", "ts"]);
        assert!(!measured(small.path()).cols_sampled);
    }

    /// The files a folder offers come back in order, whatever order the directory was
    /// written in.
    ///
    /// Every caller reads order as meaning something — `sample_footers` takes the ends
    /// and the middle, and the union of the columns is built in the order the files
    /// appear. Unsorted, "the last file" was whichever one the filesystem happened to
    /// return last, which on the filesystems that return creation order is the one
    /// written first as often as not.
    #[test]
    fn the_files_a_folder_offers_come_back_in_order() {
        let dir = tempfile::tempdir().unwrap();
        for name in ["c.parquet", "a.parquet", "d.parquet", "b.parquet"] {
            write(dir.path(), name, &["id"]);
        }
        let mut files = Vec::new();
        collect_parquet_files(dir.path(), 0, &mut files);
        let names: Vec<String> = files
            .iter()
            .map(|p| p.file_name().unwrap().to_string_lossy().into_owned())
            .collect();
        assert_eq!(
            names,
            vec!["a.parquet", "b.parquet", "c.parquet", "d.parquet"],
            "sorted, not in the order the directory was written"
        );
    }

    /// A folder past the budget still says so, and the files it keeps are the folder's
    /// first rather than the listing's.
    ///
    /// The ordering itself is `the_files_a_folder_offers_come_back_in_order`'s to prove:
    /// a directory read may return sorted entries of its own accord, so an assertion
    /// here about order could hold for the wrong reason. What this pins is *which* files
    /// survive the cap, and that the cap still says "too many to count".
    #[test]
    fn a_folder_past_the_budget_keeps_the_folders_first_files() {
        let dir = tempfile::tempdir().unwrap();
        for part in 0..MAX_FOOTERS_PER_DATASET * 3 {
            write(dir.path(), &format!("part-{part:04}.parquet"), &["id"]);
        }
        let mut files = Vec::new();
        collect_parquet_files(dir.path(), 0, &mut files);

        assert_eq!(
            files.len(),
            MAX_FOOTERS_PER_DATASET + 1,
            "one past the budget, which is what says there are too many to count"
        );
        let names: Vec<String> = files
            .iter()
            .map(|p| p.file_name().unwrap().to_string_lossy().into_owned())
            .collect();
        let expected: Vec<String> = (0..=MAX_FOOTERS_PER_DATASET)
            .map(|part| format!("part-{part:04}.parquet"))
            .collect();
        // Not "sorted", which a directory read may be of its own accord, but the
        // folder's own first sixty-five. Sorting after truncating gives sixty-five
        // sorted names from wherever the read began, which is a different set.
        assert_eq!(
            names, expected,
            "the folder's first files, not the listing's"
        );
    }

    /// The log is named rather than looked for, because a table with more data files
    /// than the probe reads would not show it.
    #[test]
    fn a_lake_table_is_recognized_past_the_probe_limit() {
        let dir = tempfile::tempdir().unwrap();
        for part in 0..HIVE_PROBE_LIMIT * 4 {
            write(dir.path(), &format!("part-{part:03}.parquet"), &["id"]);
        }
        std::fs::create_dir_all(dir.path().join("_delta_log")).unwrap();
        assert_eq!(classify_directory(dir.path()), EntryKind::Delta);
    }

    /// Past the counting limit the row count is out of reach, but whether the folder
    /// is one table is not — and a folder of a hundred tables is exactly where reading
    /// them as one costs most.
    #[test]
    fn a_folder_too_large_to_count_is_still_checked() {
        let dir = tempfile::tempdir().unwrap();
        for table in 0..MAX_FOOTERS_PER_DATASET + 1 {
            write(
                dir.path(),
                &format!("table_{table:03}.parquet"),
                &[&format!("{table}_id"), &format!("{table}_value")],
            );
        }

        let entry = measured(dir.path());
        assert_eq!(entry.kind, EntryKind::Directory);
        assert_eq!(entry.rows, None, "too many files to count either way");
    }

    /// The same folder size, but one table split across it.
    #[test]
    fn a_large_folder_of_one_table_stays_a_dataset() {
        let dir = tempfile::tempdir().unwrap();
        for part in 0..MAX_FOOTERS_PER_DATASET + 1 {
            write(
                dir.path(),
                &format!("part-{part:05}.parquet"),
                &["id", "ts"],
            );
        }

        let entry = measured(dir.path());
        assert_eq!(entry.kind, EntryKind::MultiFile);
    }

    /// Searching the home screen by column should still find a folder that holds one,
    /// even once the folder is no longer offered as a single table.
    #[test]
    fn a_downgraded_folder_keeps_every_column_its_files_have() {
        let dir = tempfile::tempdir().unwrap();
        write(
            dir.path(),
            "circuits.parquet",
            &["circuit_id", "lat", "lng"],
        );
        write(
            dir.path(),
            "drivers.parquet",
            &["driver_id", "code", "nationality"],
        );

        let entry = measured(dir.path());
        assert_eq!(entry.kind, EntryKind::Directory);
        for column in [
            "circuit_id",
            "lat",
            "lng",
            "driver_id",
            "code",
            "nationality",
        ] {
            assert!(
                entry.columns.iter().any(|c| c == column),
                "{column} in {:?}",
                entry.columns
            );
        }
    }
}

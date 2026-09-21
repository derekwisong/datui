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

/// Subdirectories looked inside during a single scan.
///
/// Classification is what separates a hive dataset from a plain folder, and it costs
/// a `read_dir` plus a handful of stats *per subdirectory*. A directory holding
/// thousands of them turns one listing into thousands of round trips — milliseconds
/// locally, minutes on a network share. Past this many, a subdirectory is listed as
/// a place to step into and classified when you actually step into it.
const MAX_CLASSIFY_PER_DIR: usize = 64;

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
    /// An ordinary directory, to descend into.
    Directory,
    /// Somewhere remote that has not been looked at yet. Classifying it would mean
    /// reading it, which is the call that blocks when the network is gone — so it is
    /// offered as openable and left unlabelled rather than guessed at.
    Unknown,
}

impl EntryKind {
    /// Short label shown next to the entry name.
    pub fn label(self) -> &'static str {
        match self {
            EntryKind::File => "",
            EntryKind::Hive => "hive",
            EntryKind::MultiFile => "multi",
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
        !matches!(self, EntryKind::Directory)
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
/// Three separate limits apply, because a directory can be pathological in three
/// different ways: too many entries (`MAX_ENTRIES_PER_DIR`), too many subdirectories
/// worth looking inside (`MAX_CLASSIFY_PER_DIR`), and too many files inside any one
/// of those (`HIVE_PROBE_LIMIT`). None of them opens a data file.
pub fn scan_dir_bounded(dir: &Path) -> Scan {
    let Ok(iter) = std::fs::read_dir(dir) else {
        return Scan::default();
    };

    let mut entries = Vec::new();
    let mut classified = 0usize;
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
            // Past the budget a subdirectory is still listed, just not looked into.
            // Degrading to "a place to step into" costs a label; classifying every
            // one of ten thousand costs the listing.
            if classified < MAX_CLASSIFY_PER_DIR {
                classified += 1;
                classify_directory(&path)
            } else {
                EntryKind::Directory
            }
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
fn sort_entries(entries: &mut [Entry]) {
    entries.sort_by(|a, b| {
        let group = |k: EntryKind| if k.is_dataset() { 0 } else { 1 };
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
        // one that has not been looked at.
        EntryKind::Directory | EntryKind::Unknown => {}
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
        if entry.kind == EntryKind::MultiFile && !agree_on_a_schema(&files) {
            downgrade_to_directory(entry);
            return;
        }
        // Still worth knowing the shape, even when the row count is out of reach.
        if let Some(first) = files.first()
            && let Some(meta) = crate::widgets::info::read_parquet_metadata(first)
        {
            entry.cols = Some(meta.schema_descr.columns().len());
            entry.columns = column_names(&meta);
            // From one file, so it describes how the dataset is written rather
            // than its total: codec and row-group sizing are a property of the
            // writer and are uniform in practice.
            physical_facts(&meta, &mut entry.cost);
            entry.cost.uncompressed = None;
        }
        return;
    }

    let mut rows = 0usize;
    let mut cols = None;
    let mut bytes = 0u64;
    let mut columns = Vec::new();
    let mut per_file: Vec<Vec<String>> = Vec::with_capacity(files.len());
    let mut cost = Cost::default();
    let mut uncompressed = 0u64;
    let mut row_groups = 0usize;
    for file in &files {
        let Some(meta) = crate::widgets::info::read_parquet_metadata(file) else {
            return; // A file we cannot read makes the total a guess; report nothing.
        };
        rows += meta.num_rows;
        cols.get_or_insert(meta.schema_descr.columns().len());
        let names = column_names(&meta);
        if columns.is_empty() {
            columns = names.clone();
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
        // Every column any file has, rather than the first file's. Nothing here is one
        // table's shape, but the names are what the folder holds, and searching the
        // home screen by column should still find the folder that has one.
        entry.columns = union_of(&per_file);
        downgrade_to_directory(entry);
        return;
    }

    entry.rows = Some(rows);
    entry.cols = cols;
    entry.size = Some(bytes);
    entry.columns = columns;
    cost.uncompressed = (uncompressed > 0).then_some(uncompressed);
    cost.row_groups = (row_groups > 0).then_some(row_groups);
    cost.partitions = entry.cost.partitions.take();
    entry.cost = cost;
}

/// Whether a spread of a folder's files agree on a schema, for a folder with too many
/// files to read every footer of.
///
/// The ends and the middle, because keys and filenames sort, so a folder written table
/// by table can easily start with several files of the same table. Fewer than two
/// readable footers decide nothing, and the folder keeps the kind its names suggested.
fn agree_on_a_schema(files: &[PathBuf]) -> bool {
    if files.len() < 2 {
        return true;
    }
    let picks = [0, files.len() / 2, files.len() - 1];
    let per_file: Vec<Vec<String>> = picks
        .iter()
        .filter_map(|i| files.get(*i))
        .filter_map(|file| crate::widgets::info::read_parquet_metadata(file))
        .map(|meta| crate::schema_union::top_level_columns(&column_names(&meta)))
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
    entry.cost = Cost {
        partitions: entry.cost.partitions.take(),
        ..Cost::default()
    };
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
        if path.is_dir() {
            subdirs.push(path);
        } else if path
            .extension()
            .and_then(|e| e.to_str())
            .map(|e| e.eq_ignore_ascii_case("parquet"))
            .unwrap_or(false)
            && is_regular_file(&path)
        {
            out.push(path);
            if out.len() > MAX_FOOTERS_PER_DATASET {
                return;
            }
        }
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
        entry.cols = Some(meta.schema_descr.columns().len());
        entry.columns = column_names(&meta);
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

    fn measured(dir: &Path) -> Entry {
        let mut entry = Entry {
            path: dir.to_path_buf(),
            kind: classify_directory(dir),
            name: dir.file_name().unwrap().to_string_lossy().into_owned(),
            size: None,
            modified: None,
            rows: None,
            cols: None,
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

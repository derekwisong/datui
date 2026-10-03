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

/// Compression suffixes that may follow a data extension (`sales.csv.gz`).
const COMPRESSION_EXTENSIONS: &[&str] = &["gz", "bz2", "xz", "zst", "zstd"];

/// Upper bound on entries read from a single directory, so a pathological directory
/// cannot hang the UI.
pub const MAX_ENTRIES_PER_DIR: usize = 5_000;

/// What a home-screen row represents.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EntryKind {
    /// A single data file.
    File,
    /// A file datui has no reader for. Hidden on the home screen until `Ctrl+A` shows
    /// it, dimmed, so a directory can be seen as it is.
    Other,
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
///
/// 7: a file with no extension is a SQLite database when its first bytes say so.
///
/// 6: on a local disk, a file with no extension is data when its first bytes carry a
/// Parquet, Arrow, Avro or ORC signature, so a directory of Spark part files a 5 called
/// `dir` is one dataset. Unidentified ones are `unnamed` rather than `not_read`.
///
/// 5: a directory of CSV or NDJSON is judged by the names at the front of its files, the
/// way a directory of Parquet is judged by its footers — so one a 4 called `multi` on its
/// filenames alone may be a place to look inside. A cached kind is restored without
/// looking again, so a record written by 4 would keep the answer this build exists to
/// correct (#275 follow-up).
///
/// 4: a directory's row carries what one listing of it found, beside its kind, and the
/// two are restored together — a record written by 3 carries the kind and not the count,
/// and a row given a kind from the cache is never looked into again (#275, phase 2).
///
/// 3: one listing instead of a probe of the first eight entries, formats instead of
/// extension strings, and one bookkeeping predicate. A directory of `.arrow` beside
/// `.ipc` was `dir` and is now one dataset; a directory whose ninth entry decided it was
/// answered by whatever the filesystem returned first (#275, phase 1).
pub const CLASSIFIER_VERSION: u32 = 7;

impl EntryKind {
    /// Short label shown next to the entry name.
    pub fn label(self) -> &'static str {
        match self {
            // A file with no reader says nothing: it is dimmed, and Enter shows its
            // bytes. `binary` would be wrong for the README or log it often is.
            EntryKind::File | EntryKind::Other => "",
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
        !matches!(self, EntryKind::Directory | EntryKind::Other) && !self.is_lake_table()
    }

    /// Whether this row is *known* to be a dataset.
    ///
    /// [`EntryKind::is_dataset`] answers "may this be opened", and a row nothing has
    /// looked into answers yes: it is offered, and looked into before it is acted on.
    /// This one answers "is this a dataset", which such a row cannot answer at all —
    /// and that is the question counting asks. A directory of two hundred subdirectories
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

/// What one listing of a directory found in it, counted rather than judged.
///
/// The label a directory's row carries comes from here, so it says what is inside rather
/// than what `Enter` will do with it. A count that is wrong then costs a reader nothing:
/// `12 parquet` is true of a directory whether or not its files are one table.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Holds {
    /// Data files by format, commonest first. The name is [`crate::FileFormat::name`],
    /// kept as a string so a record written by one build reads in the next.
    #[serde(default)]
    pub formats: Vec<(String, usize)>,
    /// Subdirectories, partitions among them. Development builds of 0.4.0 wrote it as
    /// `folders`; the alias keeps a cache from one of those readable.
    #[serde(default, alias = "folders")]
    pub directories: usize,
    /// `key=value` subdirectories, which are also counted in `directories`.
    #[serde(default)]
    pub partitions: usize,
    /// Files datui has no reader for: a README, a script, a notebook. Neither data nor a
    /// writer's own, and without a count of their own they were in nothing — a directory
    /// of twenty of them read `dir` with no line at all, the same as an empty one.
    #[serde(default)]
    pub not_read: usize,
    /// Files with no extension. No name says what they are, so they are neither data
    /// nor `not_read`: Spark and GBIF write their part files this way, and the open
    /// reads them by their bytes.
    #[serde(default)]
    pub unnamed: usize,
    /// Entries skipped as a writer's own, and the first few by name for the pane.
    #[serde(default)]
    pub skipped: usize,
    #[serde(default)]
    pub skipped_names: Vec<String>,
    /// Whether the listing stopped at [`MAX_ENTRIES_PER_DIR`], so every count is a
    /// floor. Shown as `5000+`.
    #[serde(default)]
    pub truncated: bool,
    /// A Hugging Face DatasetDict saved with `save_to_disk`: `dataset_dict.json`
    /// beside directories, its splits. Read as Arrow, one split at a time.
    #[serde(default)]
    pub dataset_dict: bool,
}

/// How many skipped names are kept for the pane. Enough to recognise the convention.
pub(crate) const SKIPPED_NAMES_SHOWN: usize = 4;

/// A name cut to `width`, keeping both ends and marking the middle.
pub(crate) fn shorten(name: &str, width: usize) -> String {
    let chars: Vec<char> = name.chars().collect();
    if chars.len() <= width {
        return name.to_string();
    }
    let ellipsis = crate::glyphs::get().ellipsis;
    let room = width.saturating_sub(ellipsis.chars().count());
    let head = room.div_ceil(2);
    let tail = room - head;
    format!(
        "{}{ellipsis}{}",
        chars[..head].iter().collect::<String>(),
        chars[chars.len() - tail..].iter().collect::<String>()
    )
}

impl Holds {
    /// Data files of every format.
    pub fn data_files(&self) -> usize {
        self.formats.iter().map(|(_, n)| n).sum()
    }

    /// The one format this directory holds, when it holds exactly one.
    pub fn one_format(&self) -> Option<&str> {
        match self.formats.as_slice() {
            [(name, _)] => Some(name),
            _ => None,
        }
    }

    /// The weights' format and file count, when this directory is a model: weights of
    /// one format with nothing beside them but JSON. See [`is_model_directory`].
    pub fn model_weights(&self) -> Option<(&str, usize)> {
        if !is_model_directory(counts_names(self)) {
            return None;
        }
        self.formats
            .iter()
            .find(|(name, _)| is_weights(name))
            .map(|(name, count)| (name.as_str(), *count))
    }

    /// The label a directory's row carries when its kind does not name itself: `12
    /// parquet`, `mixed`, or `dir` for a directory with no data directly inside.
    pub fn label(&self) -> String {
        let more = if self.truncated { "+" } else { "" };
        // A model's weights beside its config and tokenizer JSON: the directory is the
        // model, and its label says so rather than `mixed`.
        if let Some((name, count)) = self.model_weights() {
            return format!("{count}{more} {name}");
        }
        match self.formats.as_slice() {
            // `dir` says there is no data file inside. A listing cut short cannot say
            // that — it found none among the entries it read, and more files can
            // unmake it, which is what separates this from `mixed`.
            [] => format!("dir{more}"),
            // The `+` hedges the whole claim, not only the number: past the cap a
            // second format may be among the entries that were not read, so `5000+
            // parquet` and `mixed` are both answers this directory can give depending on
            // the order it came back in. What is certain is that five thousand Parquet
            // files are in there.
            [(name, count)] => format!("{count}{more} {name}"),
            // No `+`: `mixed` is not a count, and more files cannot unmake it. The
            // pane's line carries the qualifier on each number it does report.
            _ => "mixed".to_string(),
        }
    }

    /// Whether nothing has been counted here: a file, or a directory nothing has looked
    /// into. A directory that was looked into and found empty is not this — it has no
    /// formats either, and `dir` is the right word for both.
    pub fn is_empty(&self) -> bool {
        self.formats.is_empty()
            && self.directories == 0
            && self.skipped == 0
            && self.not_read == 0
            && self.unnamed == 0
            // Every field, including the two that are counted elsewhere as well: a
            // partition is a directory and a skipped name is one of `skipped`, so on both
            // routes today these are implied. This is a `skip_serializing_if` and the
            // guard that stops a placeholder erasing a row's count, and neither should
            // turn on an invariant two other functions have to keep.
            && self.partitions == 0
            && self.skipped_names.is_empty()
            && !self.dataset_dict
            // A listing cut short before it found anything still says something: that
            // what it found is not all there is. Without this the row falls back to its
            // kind and reads `dir`, where `label` would have said `dir+`.
            && !self.truncated
    }

    /// The `contains` line in the details pane: the data files by format, the
    /// directories, and the partitions — what there is to open, and nothing else.
    ///
    /// Files datui cannot read and a writer's markers are left out. Counted here they
    /// read as a warning ("10 not read") about a directory with nothing wrong in it;
    /// inside the directory, a row of its own says what is not shown.
    pub fn line(&self, with_partitions: bool) -> Option<String> {
        let more = if self.truncated { "+" } else { "" };
        let mut parts: Vec<String> = self
            .formats
            .iter()
            .map(|(name, count)| format!("{count}{more} {name}"))
            .collect();
        // Partitions are directories too, and counted in `directories`; naming both would
        // count them twice. What is left is the directories that are not partitions.
        let plain = self.directories.saturating_sub(self.partitions);
        if plain > 0 {
            let word = if plain == 1 {
                "directory"
            } else {
                "directories"
            };
            parts.push(format!("{plain}{more} {word}"));
        }
        if with_partitions && self.partitions > 0 {
            let word = if self.partitions == 1 {
                "partition"
            } else {
                "partitions"
            };
            parts.push(format!("{}{more} {word}", self.partitions));
        }
        (!parts.is_empty()).then(|| parts.join(" · "))
    }
}

impl Entry {
    /// Whether the home screen leaves this row out until Ctrl+A: a file datui cannot
    /// open, or a database's own table.
    pub fn hidden_by_default(&self) -> bool {
        self.kind == EntryKind::Other || self.table.as_ref().is_some_and(|t| t.internal)
    }

    /// The short label beside a row's name: what it holds, rather than what `Enter`
    /// will do with it.
    ///
    /// A directory that has been looked into is described by the count — `12 parquet`,
    /// `mixed`, `dir` — and a lake table or a hive root by the format's own name, which
    /// is the thing it is. A row nothing has looked into has only its kind to go on.
    pub fn label(&self) -> std::borrow::Cow<'static, str> {
        match self.kind {
            // See `opens_whole_directory`: the one row whose label would be about a
            // different set of files than the row itself.
            _ if self.opens_whole_directory => "".into(),
            EntryKind::Directory | EntryKind::MultiFile if !self.holds.is_empty() => {
                self.holds.label().into()
            }
            EntryKind::File if self.format_spec.is_some() => {
                self.format_spec.clone().unwrap_or_default().into()
            }
            EntryKind::File if self.cost.tables.is_some() => {
                let n = self.cost.tables.unwrap_or_default();
                format!("{n} {}", if n == 1 { "table" } else { "tables" }).into()
            }
            kind => kind.label().into(),
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
    /// Whether `cols` came from a spread of the directory rather than all of it. A
    /// directory past the footer budget is read at its ends and its middle, so the count
    /// is a floor: shown as `6+` rather than `6`, the way the row count is already shown
    /// as `?` when it is out of reach.
    pub cols_sampled: bool,
    /// Column names, when they were free to obtain. A Parquet footer carries them
    /// alongside the row count, so knowing what is *in* a dataset costs nothing
    /// beyond knowing how big it is.
    pub columns: Vec<String>,
    /// What opening this will cost: where it lives, how it is stored, how it is laid
    /// out. All of it derived from bytes datui already reads.
    pub cost: Cost,
    /// What one listing of it found, for a directory. Empty for a file, and for a
    /// directory nothing has looked into.
    pub holds: Holds,
    /// Whether this row is the door that opens the directory being browsed, rather than
    /// something in it.
    ///
    /// It carries no label. Every other label counts what is directly inside a directory,
    /// and this row is the one that reads the whole of it — so `dir` beside `(all
    /// files)` would say there is no data here while offering to open it, and `2
    /// parquet` beside it would name two of the twenty it is about to read. The name
    /// says what it does; the numbers beside it, once measured, say how much.
    pub opens_whole_directory: bool,
    /// The format spec whose glob names this file, which reads it.
    pub format_spec: Option<String>,
    /// A table inside a file of tables (a SQLite database, a NumPy archive), for the
    /// rows listed inside one: its path is the file's with the table's name after it,
    /// which nothing on disk has.
    pub table: Option<TableOf>,
}

/// What a row inside a file of tables says about its table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableOf {
    /// The format of the file it is in.
    pub format: crate::FileFormat,
    /// What the file calls it: SQLite's `table`, `view`, `virtual` or `shadow`, or a
    /// NumPy archive's `array`.
    pub kind: String,
    /// SQLite's own (its schema, its statistics, a virtual table's shadows): hidden
    /// like a file datui cannot open until Ctrl+A shows it, and opened like any other.
    pub internal: bool,
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
    /// Tables of its own, for a SQLite database: one opens, several are listed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tables: Option<usize>,
    /// An Arrow file that is an IPC stream, which is converted before it is scanned,
    /// rather than an IPC file, which is scanned where it is. From its first bytes.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub ipc_stream: bool,
}

/// How opening a file row will read it. See [`how_read`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HowRead {
    pub mode: crate::ReadMode,
    /// A remote file that is downloaded whole before it is read.
    pub download: bool,
}

/// How opening `entry` will read it, as [`crate::FileFormat::read_mode`] says for its
/// format and how it is stored, and whether a remote one is downloaded first. `None`
/// for anything but a file whose name says its format.
pub fn how_read(entry: &Entry) -> Option<HowRead> {
    use crate::Stored;
    if entry.kind != EntryKind::File {
        return None;
    }
    let stored = if crate::CompressionFormat::from_extension(&entry.path).is_some() {
        Stored::Compressed { in_memory: false }
    } else if entry.cost.ipc_stream {
        Stored::Stream
    } else {
        Stored::Plain
    };
    let choice = match &entry.format_spec {
        Some(name) => crate::cli::FormatChoice::Spec(name.clone()),
        // A table inside a file of tables, at its path inside it (`shop.db/orders`).
        None if entry.table.is_some() => crate::cli::FormatChoice::Builtin(
            entry
                .table
                .as_ref()
                .map_or(crate::FileFormat::Sqlite, |t| t.format),
        ),
        None => crate::cli::FormatChoice::Builtin(data_format(&entry.path)?),
    };
    let mode = choice.read_mode(stored)?;
    let download = match crate::source::input_source(&entry.path) {
        crate::source::InputSource::Local(_) => false,
        crate::source::InputSource::Http(_) => choice.http_file() == crate::RemoteRead::Downloaded,
        // A remote Arrow file is not peeked at here, so it is taken for an IPC file.
        _ => choice.bucket_object(stored) == crate::RemoteRead::Downloaded,
    };
    Some(HowRead { mode, download })
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
            holds: Default::default(),
            opens_whole_directory: false,
            format_spec: None,
            table: None,
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
/// extension inside a directory named `.parquet`, as Spark and GBIF write them
/// (`occurrence.parquet/000001`). Hidden and job files (`_SUCCESS`, `.crc`) are not.
pub fn is_parquet_key(key: &str) -> bool {
    let key = key.trim_end_matches('/');
    let (directory, name) = match key.rsplit_once('/') {
        Some((directory, name)) => (directory, name),
        None => ("", key),
    };
    if is_bookkeeping(name) {
        return false;
    }
    if name.to_ascii_lowercase().ends_with(".parquet") {
        return true;
    }
    let directory_name = directory.rsplit('/').next().unwrap_or(directory);
    !name.contains('.') && directory_name.to_ascii_lowercase().ends_with(".parquet")
}

#[cfg(test)]
mod parquet_key_tests {
    use super::is_parquet_key;

    #[test]
    fn parquet_without_an_extension_is_known_by_its_directory() {
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

/// What a file with no usable extension turns out to be, from the bytes at its start.
///
/// Every format datui reads as a directory puts a fixed signature at the front — Parquet
/// at both ends, and the other three at the front alone. A name is the cheap answer and
/// the one every listing uses; this is the expensive one, and it is asked only of a
/// directory somebody is opening, never of a directory somebody is looking at.
///
/// Spark and GBIF both write part files with no extension — `occurrence.parquet/000001`
/// is read by its directory's name, and the same files under a directory named anything
/// else were not data at all as far as datui was concerned.
///
/// CSV and JSON are deliberately absent: they have no signature, and guessing from the
/// first line is a parse rather than a look.
pub fn sniff_format(path: &Path) -> Option<crate::FileFormat> {
    let mut head = [0u8; 16];
    let head = read_head(path, &mut head)?;
    if let Some(signed) = signed_format_of(head) {
        return Some(signed);
    }
    if crate::sqlite::looks_like(head) {
        return Some(crate::FileFormat::Sqlite);
    }
    if head.starts_with(b"PAR1") {
        // Both ends, because `PAR1` at the front alone is a truncated write — the
        // footer is what a Parquet reader actually needs.
        return has_parquet_magic(path).then_some(crate::FileFormat::Parquet);
    }
    if head.starts_with(b"ARROW1") {
        return Some(crate::FileFormat::Arrow);
    }
    if head.starts_with(b"Obj\x01") {
        return Some(crate::FileFormat::Avro);
    }
    if head.starts_with(b"ORC") {
        return Some(crate::FileFormat::Orc);
    }
    if crate::audio::looks_like_audio(head) {
        return Some(crate::FileFormat::Audio);
    }
    // An Arrow IPC stream has no magic, only its schema message, read whole to be sure.
    if crate::ipc_stream::is_stream_file(path) {
        return Some(crate::FileFormat::Arrow);
    }
    None
}

/// The first bytes of `path`, as many as fit in `buf`. A short read is the whole file:
/// a signature that does not fit is not one.
fn read_head<'a>(path: &Path, buf: &'a mut [u8]) -> Option<&'a [u8]> {
    use std::io::Read;
    let mut file = std::fs::File::open(path).ok()?;
    let mut filled = 0;
    loop {
        match file.read(&mut buf[filled..]) {
            Ok(0) => break,
            Ok(n) => filled += n,
            Err(_) => return None,
        }
        if filled == buf.len() {
            break;
        }
    }
    Some(&buf[..filled])
}

/// A model, MIDI or NumPy file by its first bytes: GGUF's magic, a SafeTensors header's
/// length and the `{` after it, `MThd` and its length of 6, or `\x93NUMPY`.
fn signed_format_of(head: &[u8]) -> Option<crate::FileFormat> {
    if crate::model_files::looks_like_gguf(head) {
        Some(crate::FileFormat::Gguf)
    } else if crate::model_files::looks_like_safetensors(head) {
        Some(crate::FileFormat::Safetensors)
    } else if crate::midi::looks_like_midi(head) {
        Some(crate::FileFormat::Midi)
    } else if crate::numpy::looks_like(head) {
        Some(crate::FileFormat::Numpy)
    } else {
        None
    }
}

/// Whether a file whose name says nothing is a SafeTensors or GGUF model or a MIDI
/// file, by its first bytes. Checkpoints are often saved as `.bin` or with no extension
/// at all.
///
/// Only these: their signatures are specific enough to trust on a file of any name,
/// where `ORC` at the front of a text file is a word, not a format.
pub fn sniff_signed_format(path: &Path) -> Option<crate::FileFormat> {
    let mut head = [0u8; 16];
    signed_format_of(read_head(path, &mut head)?)
}

/// Whether a file whose name says nothing is WAV or AIFF audio, by its first bytes.
/// Their signatures (`RIFF....WAVE`, `FORM....AIFF`) are specific enough to trust on a
/// file of any name.
pub fn sniff_audio_format(path: &Path) -> Option<crate::FileFormat> {
    let mut head = [0u8; 12];
    crate::audio::looks_like_audio(read_head(path, &mut head)?).then_some(crate::FileFormat::Audio)
}

/// How many extension-less files one listing looks inside. A directory of Spark output
/// is a few hundred part files; past this the rest are listed by name alone.
const MAX_SNIFFS_PER_DIR: usize = 256;

/// Whether a file's name has no extension at all: `part-00000`, `LICENSE`.
pub fn has_no_extension(path: &Path) -> bool {
    path.extension().is_none()
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

/// Whether a path names a Parquet file: by its extension, or by sitting as a part file
/// with no extension inside a `.parquet` directory.
///
/// Not [`is_parquet_key`], which also answers "does this count toward what a directory
/// holds" and so says no to a writer's own name. `_manifest.parquet` is a file somebody
/// may open and the listing shows it; reading its footer is a different question from
/// whether it makes the directory around it a dataset.
pub fn is_parquet_path(path: &Path) -> bool {
    path.extension()
        .and_then(|e| e.to_str())
        .is_some_and(|e| e.eq_ignore_ascii_case("parquet"))
        || is_parquet_key(&directory_and_name(path))
}

/// Whether a path looks like something datui can open.
///
/// Its name, or its place: a part file with no extension inside a `.parquet` directory is
/// Parquet, as Spark and GBIF write them. Every route that asks what a name means asks
/// here — the listing, the search, `~` input, the counts and the schema pane — because
/// the one that did not was always the one that disagreed.
pub fn is_data_file(path: &Path) -> bool {
    data_extension(path).is_some() || is_parquet_key(&directory_and_name(path))
}

/// The extension that says what a file *is*, with any compression suffix walked past.
///
/// `sales.csv.gz` is a CSV: `Path::extension` answers `gz`, which is how it is stored
/// rather than what it holds. Anything deciding a *format* wants this one — two files
/// named `.csv.gz` and `.json.gz` agree on their extension and on nothing that
/// matters.
///
/// `None` when the name does not end in something datui reads.
pub fn data_extension(path: &Path) -> Option<String> {
    let name = path.file_name().and_then(|n| n.to_str())?;
    let lower = name.to_ascii_lowercase();
    let mut parts: Vec<&str> = lower.rsplit('.').collect();
    parts.reverse();
    if parts.len() < 2 {
        return None;
    }
    // Walk back past a compression suffix so `sales.csv.gz` still reads as CSV.
    let mut idx = parts.len() - 1;
    if COMPRESSION_EXTENSIONS.contains(&parts[idx]) && idx > 1 {
        idx -= 1;
    }
    crate::FileFormat::from_extension(parts[idx]).map(|_| parts[idx].to_string())
}

/// What the home screen says of a file [`unreadable_by_name`] turns away.
pub const NO_READER: &str = "datui has no reader for this file";

/// Whether a file's name already says datui will not open it: an extension no reader
/// takes, under any compression suffix. A bare `data.gz` is left to the open, which
/// looks inside, and so is a name with no extension.
pub fn unreadable_by_name(path: &Path) -> bool {
    let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
        return false;
    };
    let name = name.to_ascii_lowercase();
    let parts: Vec<&str> = name.rsplit('.').collect();
    let compressed = |last: &str| COMPRESSION_EXTENSIONS.contains(&last);
    let ext = match parts[..] {
        [last, inner, _, ..] if compressed(last) => inner,
        [last, _] if compressed(last) => return false,
        [last, _, ..] => last,
        _ => return false,
    };
    crate::FileFormat::from_extension(ext).is_none()
}

/// The format a file's name says it holds, compression suffix walked past.
///
/// The question every listing actually asks. Named extensions are not formats: `.ipc`,
/// `.arrow`, `.arrows` and `.feather` are one format under four names, and a directory holding two
/// of them is one kind of thing. Asking [`crate::FileFormat`] rather than a list of its
/// own is what keeps the home screen from offering a file the reader has no route for,
/// which is how `.txt` came to be listed and refused and `.psv` readable and invisible.
pub fn data_format(path: &Path) -> Option<crate::FileFormat> {
    // A sharded checkpoint's index is the model, not a JSON table.
    if crate::model_files::is_safetensors_index(path) {
        return Some(crate::FileFormat::Safetensors);
    }
    crate::FileFormat::from_extension(&data_extension(path)?)
}

/// The two path segments `is_parquet_key` needs, as it splits them.
///
/// A whole path would reach it with backslashes on Windows, which it does not split on,
/// so `occurrence.parquet\000001` would arrive as one name that contains a dot and be
/// read as an ordinary file. The same reason `DataTableState::directory_and_name` exists.
pub(crate) fn directory_and_name(path: &Path) -> String {
    let name = path.file_name().unwrap_or_default().to_string_lossy();
    match path.parent().and_then(|p| p.file_name()) {
        Some(directory) => format!("{}/{name}", directory.to_string_lossy()),
        None => name.into_owned(),
    }
}

/// Commonest first, Parquet ahead of anything it ties with, then by name, so the line
/// reads the same way twice running.
///
/// One order, by name of format, for everything that ranks a directory's formats: the
/// local label, the local read that picks a reader, and the cloud label. They agreed on
/// the common case and not on a tie — a directory of two CSV and two Parquet was
/// *labelled* `2 csv · 2 parquet` and *read* as Parquet, so the row said one thing and
/// `Enter` did another. Parquet wins the tie because it is the format a directory of data
/// files is most likely to be about and the one every other route reads in place.
///
/// Named rather than written inline because `read_dir` order is exactly what it exists
/// to remove, and a fixture on disk cannot pin an order that depends on it: the tie is
/// the whole point and only a caller choosing the input order can put one there.
pub(crate) fn rank_formats(a: (&str, usize), b: (&str, usize)) -> std::cmp::Ordering {
    b.1.cmp(&a.1)
        .then_with(|| (a.0 != "parquet").cmp(&(b.0 != "parquet")))
        .then_with(|| a.0.cmp(b.0))
}

/// Whether a file is one Hugging Face `datasets` writes beside a dataset's Arrow
/// shards to describe them: `save_to_disk` writes both, and its cache the first. They
/// are the dataset's metadata, not its data, where `.arrow` files sit beside them; two
/// JSON files would otherwise outnumber a dataset of one shard and be read instead of
/// it.
pub(crate) fn is_hugging_face_metadata(name: &str) -> bool {
    matches!(name, "dataset_info.json" | "state.json")
}

fn order_formats(counts: &mut [(crate::FileFormat, usize)]) {
    counts.sort_by(|a, b| rank_formats((a.0.name(), a.1), (b.0.name(), b.1)));
}

/// Whether a listing entry is bookkeeping rather than data.
///
/// The one convention datui knows, and the only one: a leading `_` or `.`, which every
/// engine in the table uses for the files it leaves beside its output — `_SUCCESS`,
/// `_committed_*`, `_started_*`, `_metadata.json`, `.crc` — and the `_$folder$` marker
/// some tools write to stand in for a folder in a flat store.
///
/// One predicate rather than the five that had drifted apart: a local listing skipped
/// dotfiles and the literal `_SUCCESS`, a local open skipped both prefixes, and the
/// cloud listing knew three more names. A directory whose ninth entry is `_metadata.json`
/// answered `multi` locally and `dir` in a bucket for no better reason than that.
pub fn is_bookkeeping(name: &str) -> bool {
    // The `_$folder$` marker is asked about first, because it is a suffix and the
    // folder it stands in for can itself be a partition: legacy s3n and EMR write
    // `year=2024_$folder$` beside `year=2024/`, and a partition test looking only for
    // an `=` calls that marker data.
    if name.ends_with("_$folder$") {
        return true;
    }
    // A `key=value` name is a partition wherever it appears, whatever it starts with.
    // Spark and Hive partition on internal columns — `_date=2024-01-01`, `_c0=…` — and
    // reading those as a writer's own files loses the whole dataset.
    if is_partition_name(name) {
        return false;
    }
    name.starts_with(['_', '.'])
}

/// Whether a format, by name, is model weights.
fn is_weights(name: &str) -> bool {
    name == crate::FileFormat::Safetensors.name() || name == crate::FileFormat::Gguf.name()
}

/// Whether formats found side by side in one directory are a model: weights of one
/// format, with nothing else beside them but JSON (a config, a tokenizer). Such a
/// directory is the model, however many JSON files outnumber the shards.
pub(crate) fn is_model_directory<'a>(names: impl IntoIterator<Item = &'a str>) -> bool {
    let mut weights = None;
    for name in names {
        if is_weights(name) {
            if weights.is_some_and(|w| w != name) {
                return false;
            }
            weights = Some(name);
        } else if name != crate::FileFormat::Json.name() {
            return false;
        }
    }
    weights.is_some()
}

/// The format names `holds` counted.
fn counts_names(holds: &Holds) -> impl Iterator<Item = &str> {
    holds.formats.iter().map(|(name, _)| name.as_str())
}

/// Whether a name is a hive partition (`year=2024`): `key=value`, with a non-empty key.
/// The value may be empty in practice.
pub fn is_partition_name(name: &str) -> bool {
    matches!(name.find('='), Some(i) if i > 0)
}

/// What the data files sitting directly in a directory say about how to read it.
///
/// A directory opened as one dataset has to be read by *something*, and the only honest
/// source for that is the files in it. Before this existed the answer was assumed:
/// any directory was scanned as Parquet, so a directory of `.json.gz` was opened by
/// seeking to the end of each file for a `PAR1` that was never going to be there.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DirectoryFormat {
    /// Every data file directly in the directory reads as this one format, and these are
    /// the files. Sorted, because a concatenation's row order is its file order.
    One(crate::FileFormat, Vec<PathBuf>),
    /// The files name more than one format. The commonest of them is the table — a
    /// directory of a thousand CSVs and one stray JSON is a directory of CSVs — and the
    /// rest are counted by format so the read can say what it passed over. Parquet wins a
    /// tie, because it is the format a directory of data files is most likely to be about
    /// and the one every other route here reads in place.
    Mixed {
        format: crate::FileFormat,
        files: Vec<PathBuf>,
        passed_over: Vec<(crate::FileFormat, usize)>,
    },
    /// The directory settles nothing by itself: it holds no readable data file directly,
    /// or it has subdirectories and so may hold its data below. A hive dataset looks like
    /// this — its files are a level down, under `key=value`.
    Deeper,
}

/// What [`DirectoryFormat`] the data files directly in `dir` amount to.
///
/// One level only, and no file is opened: this reads names, exactly as the rest of
/// this module does. A directory of two hundred thousand files costs one listing
/// and nothing per file beyond it — the entry's own type comes back with the name, so
/// there is no `stat` to bound.
///
/// Deliberately *not* bounded by [`MAX_ENTRIES_PER_DIR`], unlike every listing in this
/// module. The files this returns are not a menu to show, they are the table to read:
/// stopping at five thousand of a directory's six thousand CSVs would open it with a row
/// count, a schema union and every aggregate quietly computed over a subset, and the
/// `take` running before the sort would drop whichever file the directory read
/// happened to return last. The Parquet route this mirrors hands the directory to a scan
/// that enumerates it, with no cap either.
pub fn directory_format(dir: &Path) -> DirectoryFormat {
    let Ok(iter) = std::fs::read_dir(dir) else {
        return DirectoryFormat::Deeper;
    };

    // Every format the directory names, with the files of each. A directory of one format
    // takes the only entry; a directory of several takes the commonest and reports the
    // rest, which is what stops one stray file deciding a directory cannot be read.
    let mut by_format: Vec<(crate::FileFormat, Vec<PathBuf>)> = Vec::new();
    // Files whose names say nothing, kept in case their bytes do. See below.
    let mut nameless: Vec<PathBuf> = Vec::new();
    let mut partitioned = false;
    for entry in iter.flatten() {
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        // A table format's own files are not the table's, and a dotfile is nobody's.
        // The same test every other route makes, so they agree on what is data.
        if is_bookkeeping(name) {
            continue;
        }
        // The type the directory read already returned, rather than a `stat` per
        // entry: on a share that is a round trip per entry, and the question is only
        // whether this is a file. A symlink still gets the stat, because `d_type`
        // cannot say what is on the other end of one.
        let is_file = match entry.file_type() {
            Ok(kind) if kind.is_symlink() => is_regular_file(&path),
            Ok(kind) => kind.is_file(),
            Err(_) => is_regular_file(&path),
        };
        if !is_file {
            // Only a partition, not any subdirectory: a directory of CSVs with some
            // unrelated directory beside them is still a directory of CSVs, and handing
            // that to a scanner that walks trees is the very thing this function exists
            // to stop.
            partitioned |= is_partition_dir(&path);
            continue;
        }
        let Some(found) = data_format(&path) else {
            // A name that says nothing. Kept rather than dropped, because the bytes may
            // still say what it is — see below, where they are asked.
            if path.extension().is_none() {
                nameless.push(path);
            }
            continue;
        };
        match by_format.iter_mut().find(|(f, _)| *f == found) {
            Some((_, of_that_format)) => of_that_format.push(path),
            None => by_format.push((found, vec![path])),
        }
    }

    // Files with no extension, in a directory whose names settled nothing. Spark and GBIF
    // both write part files this way; `occurrence.parquet/000001` is read by its
    // directory's name, and the same files under a directory named anything else were not
    // data at all as far as datui was concerned — the directory was `dir` and its files
    // were not listed.
    //
    // Only when the names have nothing to say. A directory of Parquet with a `LICENSE` in
    // it is a directory of Parquet, and opening the `LICENSE` to find out is a read per
    // file for an answer already given.
    //
    // A spread rather than every one, for the reason `sample_footers` takes a spread:
    // the cost is one open per file, and a directory written by one job holds one kind of
    // thing. They have to agree — a directory where the ends disagree is not one table by
    // any reading — and then all of them are taken as that format, because a scan that
    // reads what it can and says what it could not is what happens to the odd one out.
    if by_format.is_empty() && !nameless.is_empty() {
        nameless.sort();
        let mut picks = vec![0, nameless.len() / 2, nameless.len() - 1];
        picks.dedup();
        let sniffed: Vec<crate::FileFormat> = picks
            .iter()
            .filter_map(|i| nameless.get(*i))
            .filter_map(|f| sniff_format(f))
            .collect();
        if sniffed.len() == picks.len()
            && let Some(found) = sniffed.first().copied()
            && sniffed.iter().all(|f| *f == found)
        {
            by_format.push((found, nameless));
        }
    }

    // A Hugging Face dataset's own JSON files, beside its shards.
    if by_format
        .iter()
        .any(|(f, _)| *f == crate::FileFormat::Arrow)
    {
        for (format, files) in &mut by_format {
            if *format == crate::FileFormat::Json {
                files.retain(|f| {
                    !f.file_name()
                        .and_then(|n| n.to_str())
                        .is_some_and(is_hugging_face_metadata)
                });
            }
        }
        by_format.retain(|(_, files)| !files.is_empty());
    }

    // Decided after the whole listing rather than at the first entry that could settle
    // it, so the answer does not depend on the order a directory read happens to
    // return. One `key=value` below and the directory stops being the whole story: a hive
    // dataset's data is down there, whatever strays are lying at the top.
    if partitioned {
        return DirectoryFormat::Deeper;
    }

    // The one order every route ranks a directory's formats by, so the reader this picks
    // is the format the label names.
    by_format.sort_by(|a, b| rank_formats((a.0.name(), a.1.len()), (b.0.name(), b.1.len())));
    // A directory holding model weights is the model. Its config and tokenizer JSON
    // sit beside the shards and often outnumber them, which does not make it a table
    // of JSON; the JSON is what the read passes over.
    if is_model_directory(by_format.iter().map(|(f, _)| f.name()))
        && let Some(at) = by_format.iter().position(|(f, _)| is_weights(f.name()))
    {
        let weights = by_format.remove(at);
        by_format.insert(0, weights);
    }
    let mut by_format = by_format.into_iter();
    let Some((format, mut files)) = by_format.next() else {
        return DirectoryFormat::Deeper;
    };
    files.sort();
    let passed_over: Vec<(crate::FileFormat, usize)> =
        by_format.map(|(f, of_that)| (f, of_that.len())).collect();
    if passed_over.is_empty() {
        DirectoryFormat::One(format, files)
    } else {
        DirectoryFormat::Mixed {
            format,
            files,
            passed_over,
        }
    }
}

/// How far down a hive root is followed looking for the files it partitions.
///
/// A dataset partitioned by year, month, day and hour is four; past this the directory
/// is something other than a hive dataset, and guessing further costs a directory
/// read per level on a share.
const MAX_HIVE_DEPTH: usize = 16;

/// What the files under a hive root's `key=value` partitions actually are.
///
/// A hive root holds no data itself, so [`directory_format`] can only say `Deeper` about
/// one. This follows a single spine down — the same one path through the tree a hive
/// scan reads its schema from — and reports what it finds at the bottom.
///
/// One spine, and the first partition at each level, so a dataset of ten thousand
/// partitions costs what one of two costs. That makes it a sample: a tree whose
/// partitions disagree is reported as whatever the first one holds. The alternative
/// is walking the dataset to answer a question asked before it is opened.
pub fn hive_leaf_format(dir: &Path) -> DirectoryFormat {
    let mut at = dir.to_path_buf();
    for _ in 0..MAX_HIVE_DEPTH {
        match directory_format(&at) {
            // Nothing here settles it. Follow the partitions down, if there are any.
            DirectoryFormat::Deeper => match first_partition(&at) {
                Some(next) => at = next,
                None => return DirectoryFormat::Deeper,
            },
            settled => return settled,
        }
    }
    DirectoryFormat::Deeper
}

/// The first `key=value` subdirectory of `dir`, by name.
///
/// By name rather than in directory order: two runs asking what a dataset holds must
/// not look at different partitions and give different answers.
fn first_partition(dir: &Path) -> Option<PathBuf> {
    let iter = std::fs::read_dir(dir).ok()?;
    iter.flatten()
        .take(MAX_ENTRIES_PER_DIR)
        .map(|entry| entry.path())
        .filter(|path| is_partition_dir(path) && path.is_dir())
        .min()
}

/// Whether a directory name is a hive partition (`year=2024`).
fn is_partition_dir(path: &Path) -> bool {
    path.file_name()
        .and_then(|n| n.to_str())
        .is_some_and(is_partition_name)
}

/// Classify a directory without walking it.
///
/// Reads one listing, bounded by [`MAX_ENTRIES_PER_DIR`] rather than by a probe of the
/// first few entries. A probe makes the answer depend on the order the filesystem hands
/// entries back: a directory of eight Parquet files followed by `_metadata.json` answered
/// `multi` locally, where a bucket listing the same directory sorts the JSON first and
/// answered `dir`.
///
/// It costs more than the probe did: a plain directory row is enriched with nothing, so
/// its listing is read for this alone, and a directory of five thousand entries is read
/// whole where eight used to settle it — six hundred times the entries, for the worst
/// row, and `look_into_batch` walks a batch of sixteen of them one at a time.
///
/// That includes rows on a network mount: `network_check` gates listing a directory you
/// have browsed into, not classifying the rows of one. It is one `getdents` walk, with
/// a `stat` only for a symlink, since `d_type` cannot say what is on the far end of one
/// — so a directory of symlinks is the expensive case. `home_open_selected` makes the
/// call on the thread reading keys; every other caller is on a worker.
///
/// Capping it lower again would put the order-dependence back exactly where the
/// directories are biggest.
pub fn classify_directory(path: &Path) -> EntryKind {
    look_at_directory(path).0
}

/// The kind *and* what the listing found, from one read of it.
///
/// Two answers to two questions. The kind decides what `Enter` does with the directory;
/// the count says what is in it, and the row's label is written from that — so a label
/// that is wrong about the first is still true about the second.
pub fn look_at_directory(path: &Path) -> (EntryKind, Holds) {
    let mut holds = Holds::default();
    // Before anything is counted: a lake table's data files genuinely do agree on a
    // schema, so every rule below says "one table" and is right about the schema and
    // wrong about the rows.
    //
    // And before the listing, which it does not need: three `join` tests answer it, and
    // counting would walk up to `MAX_ENTRIES_PER_DIR` entries of every table in a
    // warehouse, on every pass, for a `holds` line beside a table whose files `enrich`
    // then refuses to read. A prefix in a bucket does carry one, because the listing it
    // is counted from had already been paid for.
    if let Some(lake) = lake_table(path) {
        return (lake, holds);
    }
    let Ok(iter) = std::fs::read_dir(path) else {
        return (EntryKind::Directory, holds);
    };

    let mut partitions = 0usize;
    let mut data_files = 0usize;
    let mut seen = 0usize;
    // As `scan_dir_bounded` does, so the tally agrees with the rows inside.
    let mut sniffs_left = if crate::home::is_remote_path(path) {
        0
    } else {
        MAX_SNIFFS_PER_DIR
    };
    let mut counts: Vec<(crate::FileFormat, usize)> = Vec::new();
    // A multi-file dataset is homogeneous by definition; a directory that merely
    // contains two different spreadsheets is not one. Compared as formats rather than
    // as extensions, so `.ipc` beside `.arrow` is one kind of thing and not two.
    let mut format: Option<crate::FileFormat> = None;
    let mut mixed_formats = false;
    // Counted as data until the listing is done; see `is_hugging_face_metadata`.
    let mut hugging_face: Vec<String> = Vec::new();

    // Bounded where the entries come from rather than after they are counted: a
    // Hadoop-style output directory is a `.crc` per data file, and skipping those before
    // the count would let the walk run to twice the cap. The cap is a cost bound and not
    // a correctness one either way — a directory past it is decided by whichever entries
    // came back first, whether they were data or a writer's own.
    // One past the cap, so "there is more" is known without paying to process it —
    // the same shape `scan_dir_bounded` uses, and the reason a directory of exactly five
    // thousand entries is a total rather than a floor.
    for (entries, entry) in iter.flatten().take(MAX_ENTRIES_PER_DIR + 1).enumerate() {
        if entries >= MAX_ENTRIES_PER_DIR {
            holds.truncated = true;
            break;
        }
        let entry_path = entry.path();
        let name = entry.file_name();
        // The markers and job files tools leave beside their output, by the one test
        // every route makes.
        let name = name.to_string_lossy().into_owned();
        if is_bookkeeping(&name) {
            holds.skipped += 1;
            // The first few by name, not the first few the filesystem returned: a line
            // in the pane that reads differently on two runs of the same directory is the
            // order-dependence this module just spent a release removing. Past
            // `MAX_ENTRIES_PER_DIR` it is the first few by name *of what was read*, and
            // where the walk stopped is the filesystem's order again — which the `+` on
            // every count beside them says.
            if holds.skipped_names.last().is_none_or(|last| &name < last)
                || holds.skipped_names.len() < SKIPPED_NAMES_SHOWN
            {
                holds.skipped_names.push(name);
                holds.skipped_names.sort();
                holds.skipped_names.truncate(SKIPPED_NAMES_SHOWN);
            }
            continue;
        }
        // The type the directory read already returned, rather than a `stat` per entry:
        // this walks the whole listing now, and on a share every stat is a round trip.
        // A symlink still gets one, because `d_type` cannot say what is on the far end.
        //
        // A regular file rather than "not a directory", the test `directory_format`
        // makes: a FIFO named `a.csv` blocks whoever opens it until a writer appears, and
        // a broken symlink named `b.csv` opens as nothing. Counting either as data offers
        // a directory that cannot be read.
        let followed = |path: &Path| {
            // One `stat`, not two: what is on the far end is one question, and asking
            // it twice is a second round trip on a share.
            std::fs::metadata(path).map_or((false, false), |m| (m.is_dir(), m.is_file()))
        };
        let (is_dir, is_file) = match entry.file_type() {
            Ok(kind) if kind.is_symlink() => followed(&entry_path),
            Ok(kind) => (kind.is_dir(), kind.is_file()),
            Err(_) => followed(&entry_path),
        };
        if is_dir {
            holds.directories += 1;
            if is_partition_dir(&entry_path) {
                partitions += 1;
            }
        } else if let Some(found) = data_format(&entry_path)
            // A sharded checkpoint's index is counted as the JSON it is, so the label
            // counts the shards; the read still takes it, for the metadata it carries.
            .map(
                |found| match crate::model_files::is_safetensors_index(&entry_path) {
                    true => crate::FileFormat::Json,
                    false => found,
                },
            )
            // Data by where it sits rather than by its name: see [`is_data_file`].
            .or_else(|| {
                is_parquet_key(&directory_and_name(&entry_path))
                    .then_some(crate::FileFormat::Parquet)
            })
            .or_else(|| {
                (is_file && sniffs_left > 0 && has_no_extension(&entry_path)).then(|| {
                    sniffs_left -= 1;
                    sniff_format(&entry_path)
                })?
            })
            .filter(|_| is_file)
        {
            if found == crate::FileFormat::Json && is_hugging_face_metadata(&name) {
                hugging_face.push(name.clone());
            }
            data_files += 1;
            match counts.iter_mut().find(|(f, _)| *f == found) {
                Some((_, n)) => *n += 1,
                None => counts.push((found, 1)),
            }
            match format {
                None => format = Some(found),
                Some(first) if first != found => mixed_formats = true,
                Some(_) => {}
            }
        } else if is_file && has_no_extension(&entry_path) {
            holds.unnamed += 1;
        } else {
            // Everything else in the listing: a file with no reader, and a name with
            // nothing behind it — a FIFO, a socket, a broken symlink. Named like data
            // or not, none of them can be read.
            holds.not_read += 1;
        }
        seen += 1;
    }

    // A Hugging Face dataset's own JSON files are its writer's, like `_SUCCESS`.
    if !hugging_face.is_empty() && counts.iter().any(|(f, _)| *f == crate::FileFormat::Arrow) {
        let n = hugging_face.len();
        for (format, count) in &mut counts {
            if *format == crate::FileFormat::Json {
                *count -= n;
            }
        }
        counts.retain(|(_, count)| *count > 0);
        data_files -= n;
        seen -= n;
        holds.skipped += n;
        holds.skipped_names.extend(hugging_face);
        holds.skipped_names.sort();
        holds.skipped_names.truncate(SKIPPED_NAMES_SHOWN);
        mixed_formats = counts.len() > 1;
        format = counts.first().map(|(f, _)| *f);
    }

    holds.partitions = partitions;
    order_formats(&mut counts);
    holds.formats = counts
        .into_iter()
        .map(|(f, n)| (f.name().to_string(), n))
        .collect();

    // Unchanged from before the probe went, deliberately. Reading the whole listing
    // makes one `notes=old` among twenty ordinary subdirectories a hive root every time
    // rather than only when it came back first, and two attempts at a majority to rule
    // that out each refused a real hive root instead — against everything present, one
    // with a README beside it; against the other directories, one with a `scripts/` and a
    // `docs/`. Refusing a dataset is the worse direction, and a rule per case is what
    // #275 exists to stop. The label stops deciding what `Enter` does in phase 3, and
    // the question goes with it.
    // Deterministic now rather than occasional, which is the cost of the whole listing:
    // a source tree with a `cfg=debug/` in it reads `hive` on every pass, and `enrich`
    // then walks it to depth four looking for footers. Left alone all the same — see
    // above for the two majorities that refused real hive roots instead.
    if partitions > 0 && partitions >= data_files {
        return (EntryKind::Hive, holds);
    }

    // Require a format that can actually be read as many files. Without this the home
    // screen offers a directory of `.tsv` or `.xlsx` as one dataset and the open refuses
    // it — the same "offered but unreadable" the one vocabulary exists to stop, one
    // layer up.
    let readable_as_one = format.is_some_and(crate::FileFormat::reads_many_files);
    // Require homogeneity *and* that data is what this directory is mostly for.
    // Without the majority test, any directory with a couple of stray CSVs in it would
    // be offered as a dataset, which is worse than useless: it hides the directory.
    let homogeneous = data_files > 1 && !mixed_formats && readable_as_one;
    let mostly_data = data_files * 2 >= seen;
    // A model directory opens as the model: its shards as one table, the JSON beside
    // them left out. One file of weights is a model too.
    let model = partitions == 0 && is_model_directory(counts_names(&holds));
    let kind = if (homogeneous && mostly_data) || model {
        EntryKind::MultiFile
    } else {
        // Everything else — including a directory holding a single data file — is a
        // place to look inside, not a dataset in its own right.
        EntryKind::Directory
    };
    (kind, holds)
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
/// Named directly rather than found by walking the listing: three `join` tests answer it
/// whatever the directory holds, where a walk pays for every entry of a table with a
/// hundred thousand data files to find one name it already knows.
fn lake_table(path: &Path) -> Option<EntryKind> {
    if path.join("_delta_log").is_dir() {
        return Some(EntryKind::Delta);
    }
    if path.join(".hoodie").is_dir() {
        return Some(EntryKind::Hudi);
    }
    // Iceberg's marker is a plain name, so it takes the whole shape: metadata beside
    // data, and a metadata file actually in it. `metadata/` alone is a directory anybody
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
/// **Nothing here is classified.** Telling a hive dataset from a plain directory means
/// reading the directory, which is a round trip apiece on a share — so no listing pays
/// for it, however small. Every subdirectory comes back [`EntryKind::Unknown`], which
/// claims nothing, and is looked into later from the viewport, a batch at a time, by
/// whoever is actually reading the rows.
///
/// That is what makes a row's label a fact about the row. Classifying the first
/// sixty-four subdirectories and calling every identical one after them a plain directory
/// made it a fact about position instead; classifying them only when a listing is small
/// enough moved the arbitrariness rather than removing it, since two directories holding
/// the same subdirectories would still disagree about what to call them.
pub fn scan_dir_bounded(dir: &Path) -> Scan {
    scan_dir_progressive(dir, |_| {})
}

/// How often a listing still being read shows what it has so far.
const LISTING_PROGRESS_EVERY: std::time::Duration = std::time::Duration::from_millis(250);

/// [`scan_dir_bounded`], handing `progress` the rows read so far, sorted, every
/// [`LISTING_PROGRESS_EVERY`] while the read goes on. A directory a share takes seconds
/// to list shows its first rows as they arrive rather than a spinner until the last.
pub fn scan_dir_progressive(dir: &Path, mut progress: impl FnMut(&[Entry])) -> Scan {
    let Ok(iter) = std::fs::read_dir(dir) else {
        return Scan::default();
    };
    let mut shown = std::time::Instant::now();

    let mut entries = Vec::new();
    let mut seen = 0usize;
    let mut truncated = false;
    // Files with no extension are looked at, a few bytes each, so a Spark part file
    // is listed as the data it is while a LICENSE stays out of the way. Never on a
    // share, where each open is a round trip and one that may not come back.
    let mut sniffs_left = if crate::home::is_remote_path(dir) {
        0
    } else {
        MAX_SNIFFS_PER_DIR
    };

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
        } else if meta.is_file() && sniffs_left > 0 && has_no_extension(&path) {
            sniffs_left -= 1;
            if sniff_format(&path).is_some() {
                EntryKind::File
            } else {
                EntryKind::Other
            }
        } else if meta.is_file() {
            EntryKind::Other
        } else {
            // Not a directory or a regular file. A FIFO named `x.parquet` is a
            // listing entry datui must never offer to open.
            continue;
        };

        entries.push(Entry::new(path, kind).with_fs_metadata(&meta));
        if shown.elapsed() >= LISTING_PROGRESS_EVERY {
            let mut so_far = entries.clone();
            sort_entries(&mut so_far);
            progress(&so_far);
            shown = std::time::Instant::now();
        }
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
/// subdirectory, so what this amounts to there is files first and directories after —
/// and it is the one ordering a directory can be given before anything is known about
/// it, since it is where the row lands if the directory turns out to be a plain one.
///
/// Which is the point: a kind arriving later never moves the row, because a row that
/// moves out from under the cursor while you are scrolling is worse than a label that
/// is late.
pub(crate) fn sort_entries(entries: &mut [Entry]) {
    entries.sort_by(|a, b| {
        // Data, then directories, then what datui cannot read.
        let group = |k: EntryKind| match k {
            k if k.is_known_dataset() => 0,
            EntryKind::Other => 2,
            _ => 1,
        };
        group(a.kind).cmp(&group(b.kind)).then_with(|| {
            a.name
                .to_ascii_lowercase()
                .cmp(&b.name.to_ascii_lowercase())
        })
    });
}

/// How far below a directory the footer walk goes. A hive dataset partitioned by year,
/// month, day and hour is four; past this the files belong to something else.
const MAX_WALK_DEPTH: u8 = 4;

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
    enrich_as(entry, &crate::schema_union::ReadAs::default())
}

/// As [`enrich`], reading each file the way the open that follows will read it.
///
/// The rule that decides whether a directory's files are one table reads the names at the
/// front of them, and where those names are is a reader setting. A pass that used its own
/// answers would judge a directory by a reading nobody is going to make — which is how
/// `datui --no-header directory/` came to open the home screen for a directory the flag
/// reads perfectly as one table.
pub fn enrich_as(entry: &mut Entry, as_read: &crate::schema_union::ReadAs) {
    match entry.kind {
        EntryKind::File => {
            enrich_parquet(entry);
            enrich_tables(entry);
            enrich_arrow(entry);
        }
        EntryKind::Hive | EntryKind::MultiFile => enrich_dataset(entry, as_read),
        // Nothing to read for a plain directory, and nothing that *may* be read for
        // one that has not been looked at. Nor for a lake table: summing the footers
        // under one counts tombstoned rows, every rewritten version and both sides of
        // a compaction, which is the whole reason it is not offered as a dataset.
        EntryKind::Directory | EntryKind::Unknown | EntryKind::Other => {}
        EntryKind::Delta | EntryKind::Iceberg | EntryKind::Hudi => {}
    }
}

/// Sum footers across a bounded set of Parquet files under `entry`.
fn enrich_dataset(entry: &mut Entry, as_read: &crate::schema_union::ReadAs) {
    // A directory of JSON is not described by the Parquet under it. The walk below
    // recurses — it has to, because that is what opening the directory reads — so for a
    // directory whose own files are a format this cannot count, every number it produced
    // belonged to something the row does not name: `6 json` reported the sixty-one
    // columns of the Parquet in its subdirectories.
    //
    // A directory of Parquet with more Parquet beneath it is the opposite case and keeps
    // the walk. The counts are a promise about what `Enter` gives, and `Enter` reads
    // the subtree; measuring only the top would promise three files and open
    // twenty-three, and would ask `is_one_table` about three files while unioning all
    // twenty-three. The `holds` line names the directory that explains the difference.

    // The partition layout comes from directory names, so it is knowable even for a
    // dataset far too large to count the rows of — which is exactly the dataset whose
    // shape you most want described before opening it.
    if entry.kind == EntryKind::Hive {
        entry.cost.partitions = partition_layout(&entry.path);
    }

    // Whether the footers below are this directory's own shape, or something else's. A
    // directory's own format is counted exactly, so this is exact for one.
    //
    // Not asked of a hive root at all. Its own files are strays beside the partitions —
    // a `schema.json` or a `manifest.csv` left at the top — so its counted format is
    // not its data's, and one such file would blank the whole dataset. Its data is down
    // in the partitions, where the format can only be sampled, and one spine tells the
    // two cases apart in neither direction: a CSV tree with a stray `snapshot.parquet`
    // in the sampled partition and a Parquet tree with a stray `notes.csv` in it both
    // come back `NotOneTable`. A stray Parquet in a CSV tree is still counted as the
    // dataset's, which #275 phase 4 settles by making the tree readable in its own
    // format.
    let reads_as_parquet = entry.kind == EntryKind::Hive
        || match entry.holds.one_format() {
            // No single format to object with, so nothing to object. No row reaches
            // this today — a directory of more than one format is a `Directory` and
            // `enrich` leaves those alone — so it is a default, and the safe one:
            // leaving the counts off a directory is a mistake opening it undoes.
            None => true,
            // A name this build cannot read back is not Parquet as far as anything here
            // knows. Leaving the counts off a directory is the mistake that can be undone
            // by opening it; giving it another format's numbers is not.
            Some(name) => crate::FileFormat::from_name(name) == Some(crate::FileFormat::Parquet),
        };
    if !reads_as_parquet {
        entry.size = None;
        judge_by_names(entry, as_read);
        return;
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
        // every footer: three files spread across the directory answer it. Without this a
        // directory large enough to be past the counting limit would skip the check
        // entirely, which is backwards — the more tables it holds, the more a union of
        // them costs.
        let sampled = sample_footers(&files);
        let names: Vec<Vec<String>> = sampled.iter().map(column_names).collect();
        if entry.kind == EntryKind::MultiFile && !files_nest(&names) {
            // Whether the directory is one table is asked of everything under it, because
            // that is what opening it would union. What it *holds* is the files the
            // label counts — the ones directly inside — and a downgraded row is never
            // opened as one table, so a *count* spanning the subtree would be a width
            // nothing produces. Three more footers, on a directory being downgraded, to
            // say `2 parquet` and mean those two.
            let own_files = direct_children(&files, &entry.path);
            let own = sample_footers(&own_files);
            // The names, though, are every one sampled under it, the same as the arm
            // below: they are the home screen's search index, and a directory is found by
            // a column that looking inside it will reach. Narrowing these to the direct
            // children made a big directory unfindable by a column a small one is found
            // by.
            entry.columns = union_of(&names);
            // A floor only when a footer was left unread. The directory is past the
            // counting budget, but its *own* files may be three of the seventy — and
            // then `5+ cols` claims a sample that did not happen.
            entry.cols_sampled = own.len() < own_files.len();
            // The columns a reader sees, from the schema rather than by splitting leaf
            // paths on a dot: a column named `user.id` and a struct `user` with a field
            // `id` are not the same thing, and a string cannot tell them apart.
            let top = union_of(&own.iter().map(top_level_names).collect::<Vec<_>>());
            downgrade_to_directory(entry, (!top.is_empty()).then_some(top.len()));
            return;
        }
        // Still worth knowing the shape, even when the row count is out of reach.
        if let Some(meta) = sampled.first() {
            // Three files rather than the first, because a directory written over time
            // keeps its newest columns in its last file — and the first is where a
            // dataset that grew is narrowest. Still a sample and not a total: the
            // count beside it is already `?`.
            entry.columns = union_of(&names);
            let top = union_of(&sampled.iter().map(top_level_names).collect::<Vec<_>>());
            entry.cols = Some(top.len() + partition_columns_beyond(entry, &top));
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
    // The width and the size, restricted to the directory's own files. A directory the
    // footers downgrade is never opened as one table, so a *count* spanning the subtree
    // would be a width nothing produces — and the label beside it counts only what is
    // inside. The column names stay the subtree's: they are the search index, not the
    // label.
    let mut own_bytes = 0u64;
    let mut own_top_level: Vec<String> = Vec::new();
    let mut own_seen_top = std::collections::HashSet::new();
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
        // And the same again for this directory's own files, which is what a downgraded
        // row is labelled from: `2 parquet` must mean those two.
        // One stat, feeding both totals: on a share each is a round trip, and a directory
        // of sixty-four files directly inside would have paid twice for every one.
        let file_bytes = std::fs::metadata(file).map(|m| m.len()).unwrap_or(0);
        bytes += file_bytes;
        if file.parent() == Some(entry.path.as_path()) {
            own_bytes += file_bytes;
            for name in top_level_names(&meta) {
                if own_seen_top.insert(name.clone()) {
                    own_top_level.push(name);
                }
            }
        }
        let mut per_file = Cost::default();
        physical_facts(&meta, &mut per_file);
        uncompressed += per_file.uncompressed.unwrap_or(0);
        row_groups += per_file.row_groups.unwrap_or(0);
        if cost.codec.is_none() {
            cost.codec = per_file.codec;
        }
    }
    // The footers are read by now, so whether these files are one table is known
    // rather than guessed. A directory of separate tables is a place to look inside: its
    // row count is the sum of unrelated things, its column count belongs to whichever
    // file happened to be read first, and opening it unions tables that share nothing.
    //
    // Only `multi` is reconsidered. A `key=value` layout says what the writer meant,
    // and a hive directory's files hold the same table by construction.
    if entry.kind == EntryKind::MultiFile && !crate::schema_union::is_nested(&per_file) {
        // Its own files' bytes, not the subtree's. The label counts what is directly
        // inside and so do the columns beside it; a size summed over a different set of
        // files is a third number on one row measured against neither of the other two.
        entry.size = Some(own_bytes);
        // Nothing here is one table's shape, but the names are what the directory holds,
        // and searching the home screen by column should still find the directory that
        // has one. The count is the directory's own files, which is what the label names.
        // The column *names* are every one under it: they are the home screen's search
        // index, and "which of these has a `txinwitness`?" is answered by the directory
        // that has one anywhere, which is where looking inside will find it.
        entry.columns = columns;
        entry.cols_sampled = false;
        downgrade_to_directory(
            entry,
            (!own_top_level.is_empty()).then_some(own_top_level.len()),
        );
        return;
    }

    entry.rows = Some(rows);
    entry.cols = Some(top_level.len() + partition_columns_beyond(entry, &top_level));
    entry.size = Some(bytes);
    entry.columns = columns;
    cost.uncompressed = (uncompressed > 0).then_some(uncompressed);
    cost.row_groups = (row_groups > 0).then_some(row_groups);
    cost.partitions = entry.cost.partitions.take();
    entry.cost = cost;
}

/// Partition keys the files do not carry themselves. The open hoists them in as
/// columns, so a hive table's width counts them: `12 × 4`, not the `12 × 2` its footers
/// say.
fn partition_columns_beyond(entry: &Entry, top_level: &[String]) -> usize {
    entry.cost.partitions.as_ref().map_or(0, |layout| {
        layout
            .keys
            .iter()
            .filter(|key| !top_level.contains(key))
            .count()
    })
}

/// The files of `dir` itself, out of a walk that went below it.
fn direct_children(files: &[PathBuf], dir: &Path) -> Vec<PathBuf> {
    files
        .iter()
        .filter(|f| f.parent() == Some(dir))
        .cloned()
        .collect()
}

/// The footers at the ends and the middle of a directory too large to read every one of.
///
/// The ends and the middle, because keys and filenames sort: a directory written table by
/// table can easily start with several files of the same table, so its head answers
/// nothing. The last file earns its place twice over — in a directory written over time
/// it is the newest, which is where a column added last year is.
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

/// Whether a spread of a directory's files agree on a schema.
///
/// Fewer than two readable footers decide nothing, and the directory keeps the kind its
/// names suggested.
fn files_nest(sampled: &[Vec<String>]) -> bool {
    let per_file: Vec<Vec<String>> = sampled
        .iter()
        .map(|names| crate::schema_union::top_level_columns(names))
        .collect();
    per_file.len() < 2 || crate::schema_union::is_nested(&per_file)
}

/// Ask a directory with no footers whether its files are one table, by the names at the
/// front of them.
///
/// The same rule as [`files_nest`] on the same evidence — the column names — from the
/// only place a CSV or an NDJSON file keeps them. Without this a directory of forty
/// unrelated CSVs was labelled `40 csv`, `Enter` promised one table because nothing had
/// looked, and the read then refused it: the permissive rule with the strict reader,
/// which is the pairing #275 exists to stop. Parquet has had the test since phase 3;
/// this is the rest of the formats catching up.
///
/// Silence is optimism, as it is for an unreadable footer: too few files, a format whose
/// schema costs a whole read, or a file that would not parse all leave the directory as
/// its names suggested. That is only safe because the read behind it unions by name and
/// widens types rather than failing — see `DataTableState::union_of_files`.
fn judge_by_names(entry: &mut Entry, as_read: &crate::schema_union::ReadAs) {
    if entry.kind != EntryKind::MultiFile {
        return;
    }
    let Some(format) = entry
        .holds
        .one_format()
        .and_then(crate::FileFormat::from_name)
    else {
        return;
    };
    // The directory's own files, which is what the label counts and what the open reads.
    // A `MultiFile` directory is flat by construction — a `key=value` below it would have
    // made it `Hive` — so there is no subtree to walk for these.
    //
    // `One` and nothing else. `one_format` above already returned for a directory of more
    // than one format, and `look_at_directory` only calls a directory `MultiFile` when
    // its formats agree, so `Mixed` cannot arrive here — matching it as well read as
    // coverage this does not have. A directory of forty disjoint CSVs beside one stray
    // `.json` is a `Directory` before it reaches this, and goes inside for that reason
    // rather than for this one.
    let DirectoryFormat::One(_, files) = directory_format(&entry.path) else {
        return;
    };
    // Read the way the open that follows will read it: where the header is decides
    // what these names are, and a verdict reached by another reading is about a directory
    // nobody is going to open.
    let sampled = crate::schema_union::sample_files(&files, format, as_read);
    if sampled.nests == Some(false) {
        // The columns the sample found, so searching the home screen by column still
        // finds the directory that has one — the same thing the Parquet path keeps when
        // it downgrades. From the spread that was read rather than from every file: a
        // directory of forty thousand CSVs must cost what a directory of four costs, and
        // this runs on the thread that opens a path named on the command line.
        // `cols_sampled` is what says the count is a floor.
        let cols = (!sampled.columns.is_empty()).then_some(sampled.columns.len());
        // A floor only when there were files the sample did not open. A directory of two
        // or three had every one read, and `N+ cols` on that row claims a hedge the
        // count does not need — the Parquet path next door works this out the same way.
        entry.cols_sampled = sampled.read < files.len();
        entry.columns = sampled.columns;
        downgrade_to_directory(entry, cols);
    }
}

/// A directory whose files turned out to be separate tables is a place to look inside.
///
/// Its row count would be the sum of unrelated things, so it is not reported. The column
/// count is: the union of what the directory's files hold is a true answer to "what is in
/// here" even when "how many rows" has none, so a directory of fifteen tables reads
/// `15 parquet · 72 columns` and no row count. Passed in rather than derived from
/// `columns`, which names leaves: see [`top_level_names`] for why a leaf path cannot be
/// split back into the columns a reader sees.
fn downgrade_to_directory(entry: &mut Entry, cols: Option<usize>) {
    entry.kind = EntryKind::Directory;
    entry.rows = None;
    entry.cols = cols;
    entry.cost = Cost {
        partitions: entry.cost.partitions.take(),
        ..Cost::default()
    };
}

/// The columns a reader sees: the schema's own top-level fields.
///
/// Not the leaves a footer names, and not those leaves split on a dot either. Leaves
/// counted directly double for a directory whose writer changed — the same nested column
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
/// than over the directory — the bug this bound is a compromise with — and the row count
/// is long out of reach either way. Twenty thousand is the size `schema_union`'s own
/// measurements take as the large case.
const MAX_NAMES_PER_DIR: usize = 20_000;

/// Collect Parquet files under `dir`, breadth-bounded and depth-bounded, stopping
/// once the cap is exceeded so a huge dataset costs the same as a small one.
fn collect_parquet_files(dir: &Path, depth: u8, out: &mut Vec<PathBuf>) {
    if depth > MAX_WALK_DEPTH || out.len() > MAX_FOOTERS_PER_DATASET {
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
    // `sample_footers` takes, and the last file is the newest in a directory written over
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
            .is_some_and(crate::discover::is_bookkeeping)
        {
            continue;
        }
        // The type the directory read already returned, rather than a `stat` per entry: a
        // directory of two hundred thousand files is visited whole here, and `is_dir` on
        // every one of them is the cost of doing so. A symlink still gets the stat,
        // because whether to walk into one is a question `d_type` cannot answer.
        let is_dir = match entry.file_type() {
            Ok(kind) if kind.is_symlink() => path.is_dir(),
            Ok(kind) => kind.is_dir(),
            Err(_) => path.is_dir(),
        };
        if is_dir {
            subdirs.push(path);
        } else if is_parquet_key(&directory_and_name(&path)) {
            // The same test the classifier and the open path make, so a directory offered
            // as a dataset is one whose files this can find. Extensionless part files
            // inside a `.parquet` directory were classified `multi` and then measured at
            // nothing: `? rows` and an empty schema pane, for ever.
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
    if !is_parquet_path(&entry.path) {
        return;
    }
    if !is_regular_file(&entry.path) {
        return;
    }
    if let Some(meta) = crate::widgets::info::read_parquet_metadata(&entry.path) {
        entry.rows = Some(meta.num_rows);
        entry.columns = column_names(&meta);
        // The columns a reader sees, as a directory's row reports them: `schema_descr`
        // names the leaves, so a file with one struct of three fields counted four and
        // then listed two in the pane beside it. See [`top_level_names`].
        entry.cols = Some(top_level_names(&meta).len());
        physical_facts(&meta, &mut entry.cost);
    }
}

/// A file of tables' tables (a SQLite database's schema, a NumPy archive's directory):
/// how many of its own, and the columns of the one when there is one. A `.db` file that
/// is not a SQLite database is one datui cannot open.
pub fn enrich_tables(entry: &mut Entry) {
    if entry.kind != EntryKind::File || entry.table.is_some() {
        return;
    }
    let named = data_format(&entry.path);
    if !is_regular_file(&entry.path) {
        return;
    }
    let Some(format) = crate::members::holder(&entry.path) else {
        if named == Some(crate::FileFormat::Sqlite) {
            entry.kind = EntryKind::Other;
        }
        return;
    };
    let Ok(tables) = crate::members::tables(&entry.path, format) else {
        return;
    };
    let own: Vec<&crate::sqlite::Table> = tables.iter().filter(|t| !t.internal).collect();
    entry.cost.tables = Some(own.len());
    if let [one] = own.as_slice() {
        entry.columns = one.columns.iter().map(|(name, _)| name.clone()).collect();
        entry.cols = Some(entry.columns.len());
    }
}

/// The rows of a file of tables' listing on the home screen: a database's tables and
/// views, or an archive's arrays, by name, as a directory lists its files, SQLite's own
/// marked to be hidden, each at its path inside the file.
pub fn database_rows(file: &Path) -> Vec<Entry> {
    let Some(format) = crate::members::holder(file) else {
        return Vec::new();
    };
    let Ok(mut tables) = crate::members::tables(file, format) else {
        return Vec::new();
    };
    // A database's tables by name; an archive's arrays in the order they were saved.
    if format == crate::FileFormat::Sqlite {
        tables.sort_by_cached_key(|t| t.name.to_lowercase());
    }
    let modified = std::fs::metadata(file).and_then(|m| m.modified()).ok();
    tables
        .into_iter()
        .map(|table| table_entry(file, format, table, modified))
        .collect()
}

/// The row of a table inside a file of tables named by its path (`app.db/users`), as a
/// recent is listed: `None` when the path names no table of such a file.
pub fn table_row(path: &Path) -> Option<Entry> {
    let (file, name) = crate::members::split(path)?;
    let format = crate::members::holder(&file)?;
    let table = crate::members::tables(&file, format)
        .ok()?
        .into_iter()
        .find(|t| t.name == name)?;
    let modified = std::fs::metadata(&file).and_then(|m| m.modified()).ok();
    let mut entry = table_entry(&file, format, table, modified);
    entry.path = path.to_path_buf();
    Some(entry)
}

fn table_entry(
    file: &Path,
    format: crate::FileFormat,
    table: crate::sqlite::Table,
    modified: Option<std::time::SystemTime>,
) -> Entry {
    let mut entry = Entry::new(crate::members::place(file, &table.name), EntryKind::File);
    entry.name = table.name;
    entry.modified = modified;
    entry.columns = table.columns.into_iter().map(|(name, _)| name).collect();
    entry.cols = (!entry.columns.is_empty()).then_some(entry.columns.len());
    entry.table = Some(TableOf {
        format,
        kind: table.kind,
        internal: table.internal,
    });
    entry
}

/// Whether an Arrow file is an IPC stream: an IPC file starts `ARROW1`, a stream with
/// its schema message. Eight bytes, so a listing can say which will be converted.
fn enrich_arrow(entry: &mut Entry) {
    if entry.kind != EntryKind::File
        || data_format(&entry.path) != Some(crate::FileFormat::Arrow)
        || crate::CompressionFormat::from_extension(&entry.path).is_some()
        || !is_regular_file(&entry.path)
    {
        return;
    }
    let mut head = [0u8; 8];
    if let Some(head) = read_head(&entry.path, &mut head) {
        entry.cost.ipc_stream = !head.starts_with(b"ARROW1");
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

/// The preview of a table of a file of tables: a row inside a database or an archive, a
/// database or archive of one table, or a NumPy array file. `None` when the entry is
/// none of these, `Some(None)` when it is and has nothing to show.
fn table_preview(entry: &Entry) -> Option<Option<SchemaPreview>> {
    let (file, format, name) = match &entry.table {
        Some(table) => match crate::members::split(&entry.path) {
            Some((file, _)) => (file, table.format, Some(entry.name.as_str())),
            None => return Some(None),
        },
        None if is_regular_file(&entry.path) => {
            let format = crate::members::holder(&entry.path).or_else(|| {
                (data_format(&entry.path) == Some(crate::FileFormat::Numpy))
                    .then_some(crate::FileFormat::Numpy)
            })?;
            (entry.path.clone(), format, None)
        }
        None => return None,
    };
    if format == crate::FileFormat::Numpy {
        return Some(crate::numpy::schema_preview(&file, name));
    }
    let preview = crate::sqlite::tables(&file)
        .ok()
        .and_then(|tables| crate::sqlite::pick(tables, name, &file).ok())
        .and_then(|pick| match pick {
            crate::sqlite::Pick::One(table) => crate::sqlite::schema_preview(&file, &table),
            crate::sqlite::Pick::Several(_) => None,
        });
    Some(preview)
}

/// Find the first Parquet file at or under `dir`, without walking the whole tree.
///
/// Bounded on both breadth and depth so a hive dataset with thousands of partitions
/// costs the same as one with three.
fn first_parquet_under(dir: &Path, depth: u8) -> Option<PathBuf> {
    if depth > MAX_WALK_DEPTH {
        return None;
    }
    let mut subdirs = Vec::new();
    for entry in std::fs::read_dir(dir).ok()?.flatten().take(64) {
        let path = entry.path();
        if path.is_dir() {
            subdirs.push(path);
        } else if is_parquet_path(&path) && is_regular_file(&path) {
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
            if let Some(preview) = table_preview(entry) {
                return preview;
            }
            if !is_parquet_path(&entry.path) {
                return None;
            }
            entry.path.clone()
        }
        EntryKind::Hive | EntryKind::MultiFile => first_parquet_under(&entry.path, 0)?,
        EntryKind::Directory | EntryKind::Unknown | EntryKind::Other => return None,
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
    let mut preview: SchemaPreview = Vec::new();
    // A hive table opens with its partition keys hoisted to the front, so the pane lists
    // them there too, typed from the one path already in hand the way the scan infers
    // them.
    if entry.kind == EntryKind::Hive
        && let Ok(below) = file_path.strip_prefix(&entry.path)
    {
        for part in below.parent().into_iter().flat_map(Path::components) {
            let part = part.as_os_str().to_string_lossy();
            if let Some((key, value)) = part.split_once('=')
                && !key.is_empty()
                && schema.get(key).is_none()
            {
                // The scan's own inference, dates and booleans included.
                let dtype = if value.is_empty() || value == "__HIVE_DEFAULT_PARTITION__" {
                    polars::prelude::DataType::String
                } else {
                    polars::io::csv::read::schema_inference::infer_field_schema(value, true, false)
                };
                preview.push((key.to_string(), dtype));
            }
        }
    }
    preview.extend(
        schema
            .iter()
            .map(|(name, dtype)| (name.to_string(), dtype.clone())),
    );
    Some(preview)
}

#[cfg(test)]
mod classification_tests {
    use super::*;
    use polars::prelude::*;

    /// A file row's read follows its format and how it is stored, and a remote file
    /// other than a Parquet object or a model file is downloaded first. Directories say nothing.
    #[test]
    fn how_a_row_is_read() {
        use crate::ReadMode::*;
        let how = |path: &str| how_read(&Entry::for_test(Path::new(path), path));
        let at = |path: &str, mode, download| {
            assert_eq!(how(path), Some(HowRead { mode, download }), "{path}");
        };
        at("/d/a.parquet", Lazy, false);
        at("/d/a.csv", Lazy, false);
        at("/d/a.csv.gz", Converted, false);
        at("/d/a.json", InMemory, false);
        at("/d/a.gpx", Converted, false);
        at("/d/a.arrow", Lazy, false);
        at("s3://b/a.parquet", Lazy, false);
        at("s3://b/a.csv", Lazy, true);
        at("gs://b/a.json", InMemory, true);
        at("https://example.com/a.parquet", Lazy, true);
        at("s3://b/m.safetensors", InMemory, false);
        at("https://example.com/m.gguf", InMemory, false);
        assert_eq!(how("/d/a.parquet.gz"), None, "does not open");
        assert_eq!(how("/d/README"), None);

        let mut stream = Entry::for_test(Path::new("/d/x.arrow"), "x.arrow");
        stream.cost.ipc_stream = true;
        assert_eq!(how_read(&stream).map(|h| h.mode), Some(Converted));
        let mut spec = Entry::for_test(Path::new("/d/day.l2.zst"), "day.l2.zst");
        spec.format_spec = Some("acme.l2feed".into());
        assert_eq!(how_read(&spec).map(|h| h.mode), Some(Converted));
        at("/d/shop.db", Lazy, false);
        at("s3://b/shop.sqlite", Lazy, true);
        let mut table = Entry::for_test(Path::new("/d/shop.db/orders"), "orders");
        table.table = Some(TableOf {
            format: crate::FileFormat::Sqlite,
            kind: "table".into(),
            internal: false,
        });
        assert_eq!(how_read(&table).map(|h| h.mode), Some(Lazy));
        assert_eq!(how_read(&Entry::directory(Path::new("/d/x"))), None);
    }

    /// An Arrow file is told a stream by its first bytes when it is measured.
    #[test]
    fn measuring_an_arrow_file_tells_a_stream() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("file.arrow");
        std::fs::write(&file, b"ARROW1\0\0rest").unwrap();
        let stream = dir.path().join("stream.arrow");
        std::fs::write(&stream, b"\xff\xff\xff\xff\x10\x01\0\0").unwrap();
        for (path, is_stream) in [(file, false), (stream, true)] {
            let mut entry = Entry::for_test(&path, "x.arrow");
            enrich(&mut entry);
            assert_eq!(entry.cost.ipc_stream, is_stream, "{}", path.display());
        }
    }

    /// Every extension the home screen offers has a reader behind it, and every
    /// extension a reader knows is offered. The two lists had drifted: `.psv` and
    /// `.xlsb` opened but were invisible, and `.txt` was listed and then refused.
    #[test]
    fn what_is_offered_and_what_opens_are_one_list() {
        for ext in [
            "parquet", "csv", "tsv", "psv", "json", "jsonl", "ndjson", "arrow", "arrows", "ipc",
            "feather", "avro", "orc", "xls", "xlsx", "xlsm", "xlsb",
        ] {
            let named = PathBuf::from(format!("sales.{ext}"));
            assert!(
                data_format(&named).is_some(),
                ".{ext} opens, so the home screen must offer it"
            );
        }
        // Offered and unreadable was the other half of the same drift.
        assert!(
            data_format(Path::new("README.txt")).is_none(),
            "a README is not a dataset"
        );
        assert!(data_format(Path::new("notes")).is_none());
    }

    /// A format's name is not an extension, and the one place that stores a name has
    /// to read it back with the inverse of what wrote it. `excel` is a name no
    /// extension spells, so parsing it as one answers `None` — and `None` there means
    /// "not Parquet", which leaves a directory's counts off rather than filling them from
    /// whatever Parquet is under it.
    #[test]
    fn a_format_name_round_trips_only_through_from_name() {
        use crate::FileFormat;
        for format in FileFormat::ALL {
            assert_eq!(
                FileFormat::from_name(format.name()),
                Some(format),
                "{} is a name",
                format.name()
            );
        }
        assert_eq!(FileFormat::from_extension("excel"), None);
        assert_eq!(FileFormat::from_name("xlsx"), None);
    }

    /// A compression suffix is how a file is stored, not what it holds, on both routes.
    #[test]
    fn a_compressed_name_reads_as_the_format_under_it() {
        assert_eq!(
            data_format(Path::new("sales.csv.gz")),
            Some(crate::FileFormat::Csv)
        );
        assert_eq!(
            data_format(Path::new("events.json.zst")),
            Some(crate::FileFormat::Json)
        );
    }

    /// `.ipc`, `.arrow` and `.feather` are one format under three names, so a directory
    /// holding two of them is one kind of thing rather than a mixture.
    #[test]
    fn one_format_under_several_names_is_not_a_mixture() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("a.arrow"), b"x").unwrap();
        std::fs::write(dir.path().join("b.ipc"), b"x").unwrap();
        assert_eq!(classify_directory(dir.path()), EntryKind::MultiFile);
    }

    /// A README is neither a marker nor data. Locally it counts toward the majority
    /// and does not disqualify the directory; the cloud route counted it as data and
    /// answered `dir` where the local one said `multi`.
    #[test]
    fn a_file_datui_does_not_read_does_not_disqualify_a_directory() {
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), "a.parquet", &["id"]);
        write(dir.path(), "b.parquet", &["id"]);
        std::fs::write(dir.path().join("README.txt"), b"notes").unwrap();

        #[cfg(feature = "cloud")]
        let objects: Vec<(String, u64)> = [
            ("out/a.parquet", 100u64),
            ("out/b.parquet", 100),
            ("out/README.txt", 12),
        ]
        .iter()
        .map(|(k, s)| ((*k).to_string(), *s))
        .collect();

        #[cfg(feature = "cloud")]
        assert_eq!(
            classify_directory(dir.path()),
            crate::cloud_browse::look_at_listing("out/", &[], &objects).0,
            "the two routes answer the same directory alike"
        );
        assert_eq!(classify_directory(dir.path()), EntryKind::MultiFile);
    }

    /// A label says what is inside, so it is true whatever `Enter` then does. The same
    /// directory of three tables reads `3 parquet` and is one to look inside.
    #[test]
    fn a_label_counts_what_is_there_rather_than_naming_a_decision() {
        let dir = tempfile::tempdir().unwrap();
        for name in ["a.parquet", "b.parquet", "c.parquet"] {
            write(dir.path(), name, &["id"]);
        }
        std::fs::create_dir_all(dir.path().join("archive")).unwrap();
        std::fs::write(dir.path().join("notes.csv"), b"x").unwrap();
        std::fs::write(dir.path().join("_SUCCESS"), b"").unwrap();
        std::fs::write(dir.path().join(".part.crc"), b"").unwrap();

        let entry = measured(dir.path());
        assert_eq!(entry.label(), "mixed", "two formats is two formats");
        assert_eq!(
            entry.holds.line(true).as_deref(),
            Some("3 parquet · 1 csv · 1 directory"),
            "and the pane says what the label boiled down"
        );
        assert_eq!(entry.holds.data_files(), 4);
        assert_eq!(entry.holds.directories, 1);
    }

    /// One format, and the count is the files.
    #[test]
    fn a_directory_of_one_format_is_labelled_by_it() {
        let dir = tempfile::tempdir().unwrap();
        for i in 0..12 {
            write(dir.path(), &format!("part-{i:05}.parquet"), &["id", "ts"]);
        }
        let entry = measured(dir.path());
        assert_eq!(entry.label(), "12 parquet");
        assert_eq!(entry.holds.line(true).as_deref(), Some("12 parquet"));

        // A directory with nothing in it datui reads is a place to look inside. The
        // files are counted, but the pane's line is about what can be opened, and
        // "20 not read" read as a fault in a directory with nothing wrong in it.
        let plain = tempfile::tempdir().unwrap();
        for i in 0..20 {
            std::fs::write(plain.path().join(format!("note{i}.md")), b"x").unwrap();
        }
        let plain = measured(plain.path());
        assert_eq!(plain.label(), "dir");
        assert_eq!(plain.holds.not_read, 20);
        assert_eq!(plain.holds.line(true), None);
    }

    /// A row nothing has looked into has only its kind to go on, and a hive root or a
    /// lake table is named by the thing it is rather than counted.
    #[test]
    fn a_kind_that_names_itself_keeps_its_name() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("year=2024")).unwrap();
        std::fs::create_dir_all(dir.path().join("year=2025")).unwrap();
        assert_eq!(measured(dir.path()).label(), "hive");

        let lake = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(lake.path().join("_delta_log")).unwrap();
        assert_eq!(measured(lake.path()).label(), "delta");

        let unlooked = Entry::new(PathBuf::from("/nowhere"), EntryKind::Unknown);
        assert_eq!(unlooked.label(), "");
    }

    /// A directory of exactly the cap is a total, not a floor. `5000+` claims there is
    /// more; saying so about a directory that was read whole is a lie in the direction
    /// nobody can check.
    #[test]
    fn a_directory_read_whole_does_not_claim_there_is_more() {
        let dir = tempfile::tempdir().unwrap();
        for i in 0..MAX_ENTRIES_PER_DIR {
            std::fs::write(dir.path().join(format!("f{i:05}.csv")), b"x").unwrap();
        }
        let holds = look_at_directory(dir.path()).1;
        assert!(!holds.truncated, "every entry was read");
        assert_eq!(holds.label(), format!("{MAX_ENTRIES_PER_DIR} csv"));

        std::fs::write(dir.path().join("one-more.csv"), b"x").unwrap();
        let holds = look_at_directory(dir.path()).1;
        assert!(holds.truncated, "and now there is more than was read");
        assert!(holds.label().contains('+'));
    }

    /// A lake table is answered by three `join` tests and costs no listing. Counting
    /// one would walk every table in a warehouse on every pass, for a line beside a
    /// table whose files `enrich` then refuses to read anyway.
    #[test]
    fn a_lake_table_is_not_counted() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("_delta_log")).unwrap();
        write(dir.path(), "part-00000.parquet", &["id"]);
        write(dir.path(), "part-00001.parquet", &["id"]);

        let (kind, holds) = look_at_directory(dir.path());
        assert_eq!(kind, EntryKind::Delta);
        assert!(holds.is_empty(), "and its label is the format's own name");
        let entry = measured(dir.path());
        assert_eq!(entry.label(), "delta");
    }

    /// A listing cut short cannot say there is no data in a directory, only that it found
    /// none among the entries it read. `mixed` needs no such qualifier — more files
    /// cannot unmake it — and `dir` does, because they can.
    #[test]
    fn a_cut_short_listing_does_not_claim_a_directory_is_empty() {
        let seen = Holds {
            skipped: 5000,
            truncated: true,
            ..Default::default()
        };
        assert_eq!(seen.label(), "dir+");

        let whole = Holds {
            skipped: 3,
            ..Default::default()
        };
        assert_eq!(whole.label(), "dir");

        // And a listing cut short before it found anything at all still says so: it is
        // not an empty tally, or the row falls back to its kind and reads `dir`.
        let nothing_yet = Holds {
            truncated: true,
            ..Default::default()
        };
        assert!(!nothing_yet.is_empty());
        assert_eq!(nothing_yet.label(), "dir+");
        assert!(Holds::default().is_empty());

        let mixed = Holds {
            formats: vec![("parquet".to_string(), 3), ("csv".to_string(), 2)],
            truncated: true,
            ..Default::default()
        };
        assert_eq!(mixed.label(), "mixed", "more files cannot unmake it");
    }

    /// A name cut to fit keeps both ends. A Hadoop output directory's `.crc` files are
    /// named for the file they check, and the head and the tail are what say so.
    #[test]
    fn a_long_name_keeps_both_ends() {
        let name = ".part-00000-8f3a91c2-7b4d-4e19-a6f0-c1d2e3f4a5b6-c000.snappy.parquet.crc";
        let line = shorten(name, 24);
        assert!(line.starts_with(".part-00000"), "the head: {line}");
        // The tail, as much of it as the ellipsis leaves: it takes three characters of
        // the twenty-four in the ASCII glyph set and one in the Unicode one.
        assert!(line.ends_with(".crc"), "and the tail: {line}");
        assert!(!line.contains("8f3a91c2"), "the middle goes: {line}");
        assert!(line.chars().count() <= 24, "{line}");
    }

    /// The same directory read twice reads the same. Skipped names come back in whatever
    /// order the filesystem holds them, so the pane takes the first few *by name*.
    #[test]
    fn what_a_directory_holds_reads_the_same_twice() {
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), "a.parquet", &["id"]);
        for marker in [
            "_SUCCESS",
            "_committed_9",
            "_committed_1",
            ".crc",
            "_started_4",
        ] {
            std::fs::write(dir.path().join(marker), b"").unwrap();
        }
        let first = look_at_directory(dir.path()).1;
        for _ in 0..8 {
            assert_eq!(look_at_directory(dir.path()).1, first);
        }
        assert_eq!(
            first.skipped_names,
            vec![".crc", "_SUCCESS", "_committed_1", "_committed_9"],
            "the first four by name, of five"
        );
        assert_eq!(first.skipped, 5);
    }

    /// The label counts what is directly inside; the numbers beside it are a promise
    /// about what `Enter` gives, and `Enter` reads the subtree. Measuring only the top
    /// would promise three files and open twenty-three — and would ask `is_one_table`
    /// about three files while unioning all twenty-three, which is the union the
    /// downgrade exists to prevent. The `holds` line names the directory that explains
    /// it.
    #[test]
    fn a_directories_numbers_are_what_opening_it_gives() {
        let dir = tempfile::tempdir().unwrap();
        for name in ["a.parquet", "b.parquet", "c.parquet"] {
            write(dir.path(), name, &["id", "legacy"]);
        }
        let archive = dir.path().join("archive");
        std::fs::create_dir_all(&archive).unwrap();
        for i in 0..20 {
            write(&archive, &format!("old-{i}.parquet"), &["id", "legacy"]);
        }

        let entry = measured(dir.path());
        assert_eq!(
            entry.label(),
            "3 parquet",
            "three files are directly inside"
        );
        assert_eq!(
            entry.holds.line(true).as_deref(),
            Some("3 parquet · 1 directory")
        );
        assert_eq!(entry.rows, Some(23), "and opening it reads all of them");
    }

    /// A directory past the counting budget whose *own* files were all read says an exact
    /// width. The budget is about the subtree; three files at the top are three
    /// footers, and `5+ cols` claims a sample that did not happen.
    #[test]
    fn a_width_is_a_floor_only_when_a_footer_went_unread() {
        let dir = tempfile::tempdir().unwrap();
        for name in ["a.parquet", "b.parquet", "c.parquet"] {
            write(dir.path(), name, &["id", "ts"]);
        }
        let archive = dir.path().join("archive");
        std::fs::create_dir_all(&archive).unwrap();
        for i in 0..MAX_FOOTERS_PER_DATASET + 6 {
            write(
                &archive,
                &format!("old-{i:03}.parquet"),
                &["wholly", "different"],
            );
        }

        let entry = measured(dir.path());
        assert_eq!(entry.kind, EntryKind::Directory, "not one table");
        assert_eq!(entry.label(), "3 parquet");
        assert_eq!(entry.cols, Some(2), "id and ts");
        assert!(
            !entry.cols_sampled,
            "all three of its own footers were read"
        );
    }

    #[test]
    fn a_big_directory_is_still_found_by_a_column_one_level_down() {
        let dir = tempfile::tempdir().unwrap();
        for name in ["a.parquet", "b.parquet", "c.parquet"] {
            write(dir.path(), name, &["id", "ts"]);
        }
        let archive = dir.path().join("archive");
        std::fs::create_dir_all(&archive).unwrap();
        for i in 0..MAX_FOOTERS_PER_DATASET + 6 {
            write(
                &archive,
                &format!("old-{i:03}.parquet"),
                &["wholly", "different"],
            );
        }

        let entry = measured(dir.path());
        assert_eq!(entry.kind, EntryKind::Directory);
        // The label and the width are the three files directly inside.
        assert_eq!(entry.label(), "3 parquet");
        assert_eq!(entry.cols, Some(2), "id and ts");
        // The names are not: they are the home screen's search index, and `wholly` has
        // to reach the directory that holds one whether the directory was small enough to
        // read every footer or, as here, too big and sampled instead. Narrowing these
        // to the directory's own files made the answer depend on the directory's size.
        assert!(
            entry.columns.contains(&"wholly".to_string()),
            "{:?}",
            entry.columns
        );
        assert!(entry.columns.contains(&"id".to_string()));
    }

    #[test]
    fn a_width_over_a_directories_own_files_is_a_floor_when_there_are_too_many() {
        let dir = tempfile::tempdir().unwrap();
        // Past the footer budget with the directory's *own* files, and no two of them one
        // table, so the downgrade samples its own files as well and says so. Every file
        // gets its own column, because which three get sampled is `read_dir` order.
        for i in 0..MAX_FOOTERS_PER_DATASET + 6 {
            write(
                dir.path(),
                &format!("f-{i:03}.parquet"),
                &[&format!("c{i}")],
            );
        }

        let entry = measured(dir.path());
        assert_eq!(entry.kind, EntryKind::Directory, "not one table");
        assert_eq!(entry.label(), "70 parquet");
        assert!(
            entry.cols_sampled,
            "three of seventy footers were read, so the width is a floor"
        );
    }

    #[test]
    fn a_directory_read_as_one_table_is_sized_by_everything_under_it() {
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), "a.parquet", &["id", "ts"]);
        write(dir.path(), "b.parquet", &["id", "ts"]);
        let more = dir.path().join("more");
        std::fs::create_dir_all(&more).unwrap();
        write(&more, "c.parquet", &["id", "ts"]);

        let entry = measured(dir.path());
        assert_eq!(entry.kind, EntryKind::MultiFile, "one table");
        // `Enter` unions the subtree, so the size and the rows beside it are the
        // subtree's — the opposite of a downgraded directory, whose numbers are its own
        // files because it is never opened as one table.
        let all: u64 = [
            dir.path().join("a.parquet"),
            dir.path().join("b.parquet"),
            more.join("c.parquet"),
        ]
        .iter()
        .map(|p| std::fs::metadata(p).unwrap().len())
        .sum();
        assert_eq!(entry.size, Some(all));
        assert_eq!(entry.rows, Some(3));
    }

    #[test]
    fn nothing_counted_is_the_only_thing_holds_calls_empty() {
        // `is_empty` stops a peek's answer reaching a row and keeps a `Holds` out of
        // the cache, so anything it calls empty is thrown away. Asked of each field on
        // its own, because the contract is the function's and not its callers': both
        // routes happen to set `directories` beside `partitions` and `skipped` beside
        // `skipped_names` today, which is exactly the kind of agreement that stops
        // holding one refactor later.
        assert!(Holds::default().is_empty());
        let one = |f: fn(&mut Holds)| {
            let mut h = Holds::default();
            f(&mut h);
            h
        };
        for (what, holds) in [
            ("a data file", one(|h| h.formats.push(("csv".into(), 1)))),
            ("a directory", one(|h| h.directories = 1)),
            ("a partition", one(|h| h.partitions = 1)),
            ("a file it cannot read", one(|h| h.not_read = 1)),
            ("a writer's own file", one(|h| h.skipped = 1)),
            (
                "the name of one",
                one(|h| h.skipped_names.push("_SUCCESS".into())),
            ),
            ("a listing cut short", one(|h| h.truncated = true)),
        ] {
            assert!(!holds.is_empty(), "{what} is something to say");
        }
    }

    #[test]
    fn formats_that_tie_are_ordered_by_name_whatever_order_they_arrived_in() {
        use crate::FileFormat;
        // Given in the order that is wrong on both counts, so neither clause of the
        // comparison can be the one doing nothing. Without the tie-break a directory of
        // two CSV and two JSON reads `2 csv · 2 json` on one pass and `2 json · 2 csv`
        // on the next, which is the `read_dir` order this release exists to remove.
        let mut counts = vec![
            (FileFormat::Json, 2),
            (FileFormat::Csv, 2),
            (FileFormat::Parquet, 5),
        ];
        order_formats(&mut counts);
        assert_eq!(
            counts,
            vec![
                (FileFormat::Parquet, 5),
                (FileFormat::Csv, 2),
                (FileFormat::Json, 2)
            ]
        );
    }

    /// The label and the read name the same format, including on a tie.
    ///
    /// They agreed on the common case and not on a tie: the label sorted equal counts
    /// by name and the read put Parquet first, so a directory of two CSV and two Parquet
    /// was labelled `2 csv · 2 parquet` and opened as Parquet. One order now, and this
    /// is the case that tells the two orders apart.
    #[test]
    fn the_label_and_the_read_pick_the_same_format_on_a_tie() {
        use crate::FileFormat;
        let tmp = tempfile::TempDir::new().unwrap();
        for name in ["a.csv", "b.csv", "c.parquet", "d.parquet"] {
            std::fs::write(tmp.path().join(name), b"x").unwrap();
        }

        let (_, holds) = look_at_directory(tmp.path());
        assert_eq!(
            holds.formats.first().map(|(f, n)| (f.as_str(), *n)),
            Some(("parquet", 2)),
            "the label names Parquet first: {:?}",
            holds.formats
        );

        match directory_format(tmp.path()) {
            DirectoryFormat::Mixed { format, .. } => assert_eq!(
                format,
                FileFormat::Parquet,
                "and so does the reader the open picks"
            ),
            other => panic!("a directory of two formats is mixed, got {other:?}"),
        }
    }

    /// A Hugging Face dataset saved to disk is one shard and two JSON files that
    /// describe it: a dataset of Arrow, labelled and read as one, the JSON its writer's
    /// own. Beside no Arrow, the same names are data.
    #[test]
    fn a_hugging_face_dataset_is_its_shards() {
        use crate::FileFormat;
        let tmp = tempfile::TempDir::new().unwrap();
        for name in [
            "data-00000-of-00002.arrow",
            "data-00001-of-00002.arrow",
            "dataset_info.json",
            "state.json",
        ] {
            std::fs::write(tmp.path().join(name), b"x").unwrap();
        }
        let (kind, holds) = look_at_directory(tmp.path());
        assert_eq!(kind, EntryKind::MultiFile);
        assert_eq!(holds.formats, [("arrow".to_string(), 2)]);
        assert_eq!(holds.skipped, 2);
        assert_eq!(holds.skipped_names, ["dataset_info.json", "state.json"]);
        match directory_format(tmp.path()) {
            DirectoryFormat::One(FileFormat::Arrow, files) => assert_eq!(files.len(), 2),
            other => panic!("the shards are the dataset, got {other:?}"),
        }

        std::fs::remove_file(tmp.path().join("data-00001-of-00002.arrow")).unwrap();
        assert!(matches!(
            directory_format(tmp.path()),
            DirectoryFormat::One(FileFormat::Arrow, _)
        ));

        let json = tempfile::TempDir::new().unwrap();
        for name in ["state.json", "other.json"] {
            std::fs::write(json.path().join(name), b"{}").unwrap();
        }
        assert_eq!(
            look_at_directory(json.path()).1.formats,
            [("json".to_string(), 2)]
        );
    }

    #[test]
    fn partitions_carry_a_directory_only_while_they_are_the_most_of_it() {
        // The boundary the local rule turns on, and the twin of the cloud route's
        // `partitions_carry_a_prefix_only_while_they_are_the_most_of_it`. Every directory
        // on disk goes through this one.
        let laid_out = |strays: usize| {
            let dir = tempfile::tempdir().unwrap();
            for year in ["year=2024", "year=2025"] {
                let part = dir.path().join(year);
                std::fs::create_dir_all(&part).unwrap();
                write(&part, "data.parquet", &["id"]);
            }
            for i in 0..strays {
                write(dir.path(), &format!("stray-{i}.parquet"), &["id"]);
            }
            classify_directory(dir.path())
        };
        assert_eq!(
            laid_out(2),
            EntryKind::Hive,
            "two partitions against two files beside them"
        );
        assert_ne!(
            laid_out(3),
            EntryKind::Hive,
            "one more file than partitions is a directory that holds a key=value"
        );
    }

    #[test]
    fn a_folder_marker_is_bookkeeping_even_beside_a_partition() {
        // Legacy s3n and EMR write a zero-byte `<name>_$folder$` object beside every
        // prefix. Where the prefix is a partition the marker carries the `=` too, so a
        // partition test that only looks for one calls the marker data and the pane
        // reports one unreadable file per partition.
        assert!(is_bookkeeping("year=2024_$folder$"));
        assert!(is_bookkeeping("alpha_$folder$"));
        assert!(!is_bookkeeping("year=2024"), "the partition itself is data");
        assert!(
            !is_bookkeeping("_date=2024-01-01"),
            "Spark partitions on internal columns"
        );
    }

    /// And the files under it are what the one-table test is asked about, since they
    /// are what the union would hold.
    #[test]
    fn a_table_hidden_under_a_directory_still_downgrades_it() {
        let dir = tempfile::tempdir().unwrap();
        for name in ["a.parquet", "b.parquet"] {
            write(dir.path(), name, &["id", "ts"]);
        }
        let archive = dir.path().join("archive");
        std::fs::create_dir_all(&archive).unwrap();
        write(
            &archive,
            "other.parquet",
            &["wholly", "different", "columns"],
        );

        let entry = measured(dir.path());
        assert_eq!(
            entry.kind,
            EntryKind::Directory,
            "a union over these is not one table"
        );
        assert_eq!(entry.rows, None);
        // And the width beside `2 parquet` is those two files. The check is asked of
        // everything under the directory, because that is what opening it would union;
        // a downgraded row is never opened as one, so reporting the subtree's union
        // would be a set of columns nothing produces.
        assert_eq!(entry.label(), "2 parquet");
        assert_eq!(entry.cols, Some(2), "id and ts");
        // The names are every column under the directory, because they are what the home
        // screen searches: the directory does hold a `wholly`, one level down.
        assert!(entry.columns.contains(&"wholly".to_string()));
        assert!(entry.columns.contains(&"id".to_string()));

        // And the size is those two files, not the subtree's: three numbers on one row
        // measured over three different sets of files is no row at all.
        let own: u64 = ["a.parquet", "b.parquet"]
            .iter()
            .map(|n| std::fs::metadata(dir.path().join(n)).unwrap().len())
            .sum();
        assert_eq!(entry.size, Some(own));
    }

    /// A hive tree of CSV is still laid out, whatever its rows cannot say. The layout
    /// is directory names — no footers, no opens — and it is the thing you most want
    /// before opening a dataset too large to count.
    #[test]
    fn a_hive_tree_of_another_format_is_still_laid_out() {
        let dir = tempfile::tempdir().unwrap();
        for year in ["year=2024", "year=2025"] {
            let part = dir.path().join(year);
            std::fs::create_dir_all(&part).unwrap();
            std::fs::write(part.join("data.csv"), b"id\n1\n").unwrap();
        }
        // A stray data file at the root, which is a hive root's ordinary furniture.
        std::fs::write(dir.path().join("summary.csv"), b"id\n1\n").unwrap();

        let entry = measured(dir.path());
        assert_eq!(entry.kind, EntryKind::Hive);
        assert!(entry.cost.partitions.is_some(), "the layout is named");
        assert_eq!(entry.rows, None, "and nothing is invented about its rows");
    }

    /// A hive root's own files are strays beside the partitions — a `schema.json` or a
    /// `manifest.csv` left at the top — so its counted format is not its data's, and
    /// asking it would blank the whole dataset for one such file.
    #[test]
    fn a_hive_dataset_is_described_despite_a_stray_file_at_its_root() {
        let dir = tempfile::tempdir().unwrap();
        for year in ["year=2024", "year=2025"] {
            let part = dir.path().join(year);
            std::fs::create_dir_all(&part).unwrap();
            write(&part, "data.parquet", &["id"]);
        }
        std::fs::write(dir.path().join("schema.json"), b"{}").unwrap();

        let entry = measured(dir.path());
        assert_eq!(entry.kind, EntryKind::Hive);
        assert_eq!(entry.holds.one_format(), Some("json"), "its own only file");
        assert_eq!(entry.rows, Some(2), "and the dataset is still counted");
        assert_eq!(entry.cols, Some(2), "`id` and the partition column `year`");
    }

    /// A hive dataset is described whatever odd file is lying in a partition. One
    /// spine cannot tell a Parquet tree with a stray CSV in it from a CSV tree with a
    /// stray Parquet, and blanking a dataset that opens perfectly is the worse of the
    /// two mistakes.
    #[test]
    fn a_hive_dataset_is_described_despite_a_stray_file() {
        let dir = tempfile::tempdir().unwrap();
        for year in ["year=2024", "year=2025"] {
            let part = dir.path().join(year);
            std::fs::create_dir_all(&part).unwrap();
            write(&part, "data.parquet", &["id"]);
        }
        // Somebody's notes, dropped in beside the data.
        std::fs::write(dir.path().join("year=2024/notes.csv"), b"x").unwrap();

        let entry = measured(dir.path());
        assert_eq!(entry.kind, EntryKind::Hive);
        assert_eq!(entry.rows, Some(2), "the dataset is still counted");
        assert!(
            entry.cost.partitions.is_some(),
            "and its layout still named"
        );
    }

    /// A directory is described by its own files, not by what is under them. The footer
    /// walk recurses, which is right for a hive root and wrong for a directory of JSON
    /// that happens to have Parquet in a subdirectory.
    #[test]
    fn a_directory_is_not_described_by_files_it_does_not_name() {
        let dir = tempfile::tempdir().unwrap();
        for name in ["a.json", "b.json", "c.json"] {
            std::fs::write(dir.path().join(name), b"{}").unwrap();
        }
        let under = dir.path().join("derived");
        std::fs::create_dir_all(&under).unwrap();
        write(&under, "one.parquet", &["id", "ts", "amount"]);
        write(&under, "two.parquet", &["id", "ts", "amount"]);

        let entry = measured(dir.path());
        assert_eq!(entry.label(), "3 json");
        assert_eq!(
            entry.cols, None,
            "the Parquet below it is not this directory's shape"
        );
        assert_eq!(entry.rows, None);
        assert!(entry.columns.is_empty());
    }

    /// The gate's default, for a dataset row that counted nothing.
    ///
    /// **Nothing produces this row today.** `enrich` only reaches the gate for `Hive`
    /// and `MultiFile`; `look_at_directory` cannot answer `MultiFile` without counting
    /// a format, a hive root skips the gate outright, `CLASSIFIER_VERSION` 4 refuses a
    /// cached kind that arrives without a tally, and the cloud `(all files)` row is
    /// built from a listing and never measured. So this constructs the row by hand, and
    /// it pins a default rather than a path.
    ///
    /// It is worth pinning because the default is the arguable one. Turning it away
    /// would blank the size, the width and the row count of any such row the moment one
    /// appeared, and leaving the counts off a directory is a mistake opening it undoes —
    /// giving it another format's numbers is not.
    #[test]
    fn a_dataset_row_that_counted_nothing_is_still_described() {
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), "a.parquet", &["id", "ts"]);
        write(dir.path(), "b.parquet", &["id", "ts"]);

        let mut entry = Entry {
            kind: EntryKind::MultiFile,
            ..Entry::for_test(dir.path(), "data")
        };
        assert!(entry.holds.one_format().is_none(), "nothing counted");

        enrich(&mut entry);
        assert!(entry.size.is_some(), "the footers were read");
        assert_eq!(entry.rows, Some(2));
        assert_eq!(entry.cols, Some(2), "id and ts");
    }

    /// A Parquet file whose name begins with `_` is still a Parquet file. It does not
    /// count toward what the directory around it holds — that is what `is_bookkeeping` is
    /// for — but the listing shows it, `Enter` opens it, and the row beside it must say
    /// how many rows it has rather than nothing at all.
    #[test]
    fn a_parquet_file_named_like_a_writers_file_is_still_measured() {
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), "_2024_sales.parquet", &["id", "amount"]);

        let mut entry = Entry {
            path: dir.path().join("_2024_sales.parquet"),
            kind: EntryKind::File,
            name: "_2024_sales.parquet".into(),
            size: None,
            modified: None,
            rows: None,
            cols: None,
            cols_sampled: false,
            columns: Vec::new(),
            cost: Cost::default(),
            holds: Default::default(),
            opens_whole_directory: false,
            format_spec: None,
            table: None,
        };
        enrich(&mut entry);
        assert_eq!(entry.rows, Some(1), "its footer was read");
        assert_eq!(entry.cols, Some(2));
        assert!(schema_preview(&entry).is_some(), "and the pane shows it");

        // And it still does not make the directory around it a dataset.
        assert!(is_bookkeeping("_2024_sales.parquet"));
        assert_eq!(classify_directory(dir.path()), EntryKind::Directory);
    }

    /// A hive table's pane lists its partition keys typed the way the scan types them:
    /// a date is a date and `true` a boolean, not text.
    #[test]
    fn a_hive_preview_types_its_keys_as_the_scan_does() {
        use polars::prelude::DataType;
        let dir = tempfile::tempdir().unwrap();
        let leaf = dir.path().join("day=2024-01-02/flag=true/n=3/x=1.5");
        std::fs::create_dir_all(&leaf).unwrap();
        write(&leaf, "part.parquet", &["id"]);
        let mut entry = Entry::directory(dir.path());
        entry.kind = EntryKind::Hive;
        let preview = schema_preview(&entry).expect("a footer to read");
        let types: Vec<(&str, &DataType)> = preview.iter().map(|(n, t)| (n.as_str(), t)).collect();
        assert_eq!(
            types[..4],
            [
                ("day", &DataType::Date),
                ("flag", &DataType::Boolean),
                ("n", &DataType::Int64),
                ("x", &DataType::Float64),
            ]
        );
        assert_eq!(types[4].0, "id");
    }

    /// Part files with no extension inside a `.parquet` directory are data by where they
    /// sit. The cloud route has always counted them; the local one said `dir`.
    #[cfg(feature = "cloud")]
    #[test]
    fn extensionless_part_files_are_data_on_both_routes() {
        let dir = tempfile::tempdir().unwrap();
        let table = dir.path().join("occurrence.parquet");
        std::fs::create_dir_all(&table).unwrap();
        std::fs::write(table.join("000001"), b"PAR1").unwrap();
        std::fs::write(table.join("000002"), b"PAR1").unwrap();

        let objects: Vec<(String, u64)> = [
            "gbif/occurrence.parquet/000001",
            "gbif/occurrence.parquet/000002",
        ]
        .iter()
        .map(|k| ((*k).to_string(), 10u64))
        .collect();

        assert_eq!(
            classify_directory(&table),
            crate::cloud_browse::look_at_listing("gbif/occurrence.parquet/", &[], &objects).0,
            "the two routes answer the same directory alike"
        );
        assert_eq!(classify_directory(&table), EntryKind::MultiFile);
    }

    /// And a directory offered as a dataset is one whose files can be counted. The same
    /// name test decides both, or the row promises a dataset and shows `?` rows and an
    /// empty schema for the rest of the session.
    #[test]
    fn extensionless_part_files_are_measured_not_just_offered() {
        let dir = tempfile::tempdir().unwrap();
        let table = dir.path().join("occurrence.parquet");
        std::fs::create_dir_all(&table).unwrap();
        // Named as GBIF and Spark leave them: no extension, inside a `.parquet`
        // directory.
        write(&table, "000001", &["id", "species"]);
        write(&table, "000002", &["id", "species"]);

        let entry = measured(&table);
        assert_eq!(entry.kind, EntryKind::MultiFile);
        assert_eq!(entry.rows, Some(2), "both footers were read");
        assert_eq!(entry.cols, Some(2));
        assert!(
            schema_preview(&entry).is_some(),
            "and the schema pane shows what those footers said, rather than asking \
             for a full read of files already read"
        );

        // → goes inside a directory labelled `multi`, so the listing has to show the
        // files the label was counted from — and each is a Parquet file in its own right.
        let mut listed = scan_dir(&table);
        assert_eq!(
            listed.iter().map(|e| e.name.as_str()).collect::<Vec<_>>(),
            vec!["000001", "000002"],
            "the directory the label promises is not an empty listing"
        );
        let part = listed.first_mut().expect("a part file is listed");
        enrich(part);
        assert_eq!(part.rows, Some(1), "a part file counts its own rows");
        assert_eq!(part.cols, Some(2));
    }

    /// One `key=value` prefix among files datui does not read is a hive root on both
    /// routes. It is not much of one — but the local route has always said so, and the
    /// cloud route disagreeing was the divergence. Pinned rather than left to be
    /// rediscovered: #275 phase 3 takes the consequence off the label.
    #[cfg(feature = "cloud")]
    #[test]
    fn one_partition_beside_files_datui_cannot_read_answers_alike() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("notes=old")).unwrap();
        for note in ["README.md", "LICENSE", "logo.png"] {
            std::fs::write(dir.path().join(note), b"x").unwrap();
        }
        let objects: Vec<(String, u64)> = ["out/README.md", "out/LICENSE", "out/logo.png"]
            .iter()
            .map(|k| ((*k).to_string(), 12u64))
            .collect();

        assert_eq!(
            classify_directory(dir.path()),
            crate::cloud_browse::look_at_listing("out/", &["out/notes=old/".to_string()], &objects)
                .0,
            "the two routes answer the same directory alike"
        );
    }

    /// A partition is a partition whatever it starts with. Spark and Hive partition on
    /// internal columns — `_date=2024-01-01`, `_c0=…` — and reading those as a writer's
    /// own files loses the whole dataset.
    #[test]
    fn a_partition_named_like_a_writers_file_is_still_a_partition() {
        let dir = tempfile::tempdir().unwrap();
        let mut directories = Vec::new();
        for day in ["2024-01-01", "2024-01-02", "2024-01-03"] {
            std::fs::create_dir_all(dir.path().join(format!("_date={day}"))).unwrap();
            directories.push(format!("events/_date={day}/"));
        }

        #[cfg(feature = "cloud")]
        assert_eq!(
            classify_directory(dir.path()),
            crate::cloud_browse::look_at_listing("events/", &directories, &[]).0,
            "the two routes answer the same directory alike"
        );
        assert_eq!(classify_directory(dir.path()), EntryKind::Hive);
        assert!(!is_bookkeeping("_date=2024-01-01"));
        assert!(is_bookkeeping("_temporary"));
    }

    /// A prefix a writer made for itself is not a directory somebody put data in, on
    /// either route. `_temporary/` counted toward the majority in a bucket and not
    /// locally, so the same directory came back two different kinds.
    #[cfg(feature = "cloud")]
    #[test]
    fn a_writers_own_directory_is_skipped_on_both_routes() {
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), "part-00000.parquet", &["id"]);
        write(dir.path(), "part-00001.parquet", &["id"]);
        std::fs::create_dir_all(dir.path().join("_temporary")).unwrap();
        std::fs::create_dir_all(dir.path().join("notes")).unwrap();
        std::fs::create_dir_all(dir.path().join("archive")).unwrap();

        let local = classify_directory(dir.path());
        let directories: Vec<String> = ["out/_temporary/", "out/notes/", "out/archive/"]
            .iter()
            .map(|f| (*f).to_string())
            .collect();
        let objects: Vec<(String, u64)> = [
            ("out/part-00000.parquet", 100u64),
            ("out/part-00001.parquet", 100),
        ]
        .iter()
        .map(|(k, s)| ((*k).to_string(), *s))
        .collect();
        let cloud = crate::cloud_browse::look_at_listing("out/", &directories, &objects).0;

        assert_eq!(
            local, cloud,
            "the two routes answer the same directory alike"
        );
        assert_eq!(local, EntryKind::MultiFile);
    }

    /// Every entry is in exactly one count, including the ones with nothing behind
    /// them. A FIFO and a broken symlink named like data are not data and are not a
    /// writer's own; without a count they were in nothing, and the pane said `1 csv`
    /// about a directory of three entries.
    #[cfg(unix)]
    #[test]
    fn every_entry_is_in_one_count() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("real.csv"), b"id\n1\n").unwrap();
        std::os::unix::fs::symlink(dir.path().join("gone"), dir.path().join("broken.csv")).unwrap();
        std::fs::write(dir.path().join("notes.md"), b"x").unwrap();
        std::fs::write(dir.path().join("_SUCCESS"), b"").unwrap();

        let holds = look_at_directory(dir.path()).1;
        assert_eq!(holds.data_files(), 1);
        assert_eq!(holds.not_read, 2, "the note and the broken link");
        assert_eq!(holds.skipped, 1);
        assert_eq!(holds.line(true).as_deref(), Some("1 csv"));
    }

    /// Named like data and impossible to read: a FIFO blocks whoever opens it until a
    /// writer appears, and a broken symlink opens as nothing. `directory_format` has
    /// always skipped both; the listing now agrees.
    #[cfg(unix)]
    #[test]
    fn a_name_with_nothing_behind_it_is_not_a_data_file() {
        let dir = tempfile::tempdir().unwrap();
        std::os::unix::fs::symlink(dir.path().join("gone.csv"), dir.path().join("a.csv")).unwrap();
        std::os::unix::fs::symlink(dir.path().join("gone.csv"), dir.path().join("b.csv")).unwrap();
        assert_eq!(
            classify_directory(dir.path()),
            EntryKind::Directory,
            "two broken symlinks are not a dataset"
        );
    }

    /// A checkpoint directory is the model: its shards are the table, the config and
    /// tokenizer JSON beside them are passed over however many there are, and the label
    /// names the weights rather than calling the directory mixed.
    #[test]
    fn a_model_directory_is_its_weights() {
        let dir = tempfile::tempdir().unwrap();
        for name in [
            "model-00001-of-00002.safetensors",
            "model-00002-of-00002.safetensors",
            "config.json",
            "generation_config.json",
            "tokenizer.json",
            "tokenizer_config.json",
            "model.safetensors.index.json",
        ] {
            std::fs::write(dir.path().join(name), b"x").unwrap();
        }
        let DirectoryFormat::Mixed {
            format,
            files,
            passed_over,
        } = directory_format(dir.path())
        else {
            panic!("weights and JSON are two formats");
        };
        assert_eq!(format, crate::FileFormat::Safetensors);
        assert_eq!(files.len(), 3, "the shards, and the index for its metadata");
        assert_eq!(passed_over, [(crate::FileFormat::Json, 4)]);
        let (kind, holds) = look_at_directory(dir.path());
        assert_eq!(kind, EntryKind::MultiFile, "opened as one");
        assert_eq!(holds.label(), "2 safetensors", "the shards, not the index");

        // The index is the model too, named by what it is rather than its extension.
        assert_eq!(
            data_format(Path::new("model.safetensors.index.json")),
            Some(crate::FileFormat::Safetensors)
        );
        // Weights beside another table format are not a model directory.
        std::fs::write(dir.path().join("data.parquet"), b"x").unwrap();
        let (kind, holds) = look_at_directory(dir.path());
        assert_eq!(
            (kind, holds.label().as_str()),
            (EntryKind::Directory, "mixed")
        );
    }

    /// A model or MIDI file is known by its first bytes under any name.
    #[test]
    fn signed_files_are_sniffed_by_their_first_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let gguf = dir.path().join("weights");
        std::fs::write(&gguf, b"GGUF\x03\x00\x00\x00").unwrap();
        let st = dir.path().join("checkpoint.bin");
        let mut bytes = 2u64.to_le_bytes().to_vec();
        bytes.extend_from_slice(b"{}");
        std::fs::write(&st, &bytes).unwrap();
        let text = dir.path().join("notes");
        std::fs::write(&text, b"just some text").unwrap();
        assert_eq!(sniff_format(&gguf), Some(crate::FileFormat::Gguf));
        assert_eq!(
            sniff_signed_format(&st),
            Some(crate::FileFormat::Safetensors)
        );
        assert_eq!(sniff_signed_format(&text), None);
        let midi = dir.path().join("song.bin");
        std::fs::write(&midi, b"MThd\0\0\0\x06\0\0\0\x01\0\x60").unwrap();
        assert_eq!(sniff_signed_format(&midi), Some(crate::FileFormat::Midi));
    }

    /// A directory is offered as one dataset only when its format can be read as many
    /// files. `.tsv`, `.psv` and Excel have a single-file reader and nothing that takes
    /// a list, so offering them puts the refusal one keystroke later instead of not
    /// making the promise.
    #[test]
    fn a_format_that_cannot_be_read_as_many_is_not_offered_as_one() {
        for ext in ["tsv", "psv", "xlsx", "xlsb"] {
            let dir = tempfile::tempdir().unwrap();
            std::fs::write(dir.path().join(format!("a.{ext}")), b"x").unwrap();
            std::fs::write(dir.path().join(format!("b.{ext}")), b"x").unwrap();
            assert_eq!(
                classify_directory(dir.path()),
                EntryKind::Directory,
                "a directory of .{ext} has no reader that takes a list"
            );
        }
        // The ones that do are unaffected — every arm the multi-path open handles.
        for ext in [
            "parquet", "csv", "json", "jsonl", "ndjson", "arrow", "arrows", "ipc", "feather",
            "avro", "orc",
        ] {
            let dir = tempfile::tempdir().unwrap();
            std::fs::write(dir.path().join(format!("a.{ext}")), b"x").unwrap();
            std::fs::write(dir.path().join(format!("b.{ext}")), b"x").unwrap();
            assert_eq!(
                classify_directory(dir.path()),
                EntryKind::MultiFile,
                ".{ext} reads as many files"
            );
        }
    }

    /// The readdir-order bug: eight Parquet files and a ninth entry that is a writer's
    /// own file. A probe of the first eight entries never saw the JSON and said `multi`;
    /// a bucket listing sorts `_metadata.json` first and said `dir`. Same directory, two
    /// answers, decided by the order the filesystem happened to return.
    #[cfg(feature = "cloud")]
    #[test]
    fn a_writers_own_file_is_skipped_whatever_order_it_is_listed_in() {
        let dir = tempfile::tempdir().unwrap();
        for part in 0..8 {
            write(dir.path(), &format!("{part}.parquet"), &["season"]);
        }
        std::fs::write(dir.path().join("_metadata.json"), b"{}").unwrap();

        let local = classify_directory(dir.path());
        // The same directory as a bucket lists it: lexicographic, so the JSON comes
        // first.
        let mut keys: Vec<(String, u64)> = vec![("jolpica/2000/_metadata.json".into(), 2)];
        for part in 0..8 {
            keys.push((format!("jolpica/2000/{part}.parquet"), 100));
        }
        keys.sort();
        let cloud = crate::cloud_browse::look_at_listing("jolpica/2000/", &[], &keys).0;

        assert_eq!(
            local, cloud,
            "the two routes answer the same directory alike"
        );
        assert_eq!(local, EntryKind::MultiFile);
    }

    /// The files a job leaves beside its output are skipped on every route, not just
    /// the two names each route happened to know.
    #[test]
    fn job_files_are_skipped_on_every_route() {
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), "part-00000.parquet", &["id"]);
        write(dir.path(), "part-00001.parquet", &["id"]);
        for marker in [
            "_SUCCESS",
            "_committed_1727",
            "_committed_1728",
            "_started_1727",
            ".part.crc",
        ] {
            std::fs::write(dir.path().join(marker), b"").unwrap();
        }
        assert_eq!(
            classify_directory(dir.path()),
            EntryKind::MultiFile,
            "five markers beside two data files do not outvote them"
        );

        #[cfg(feature = "cloud")]
        let keys: Vec<(String, u64)> = [
            ("out/_SUCCESS", 0u64),
            ("out/_committed_1727", 12),
            ("out/_committed_1728", 12),
            ("out/_started_1727", 12),
            ("out/.part.crc", 8),
            ("out/part-00000.parquet", 100),
            ("out/part-00001.parquet", 100),
        ]
        .iter()
        .map(|(k, s)| ((*k).to_string(), *s))
        .collect();
        #[cfg(feature = "cloud")]
        assert_eq!(
            crate::cloud_browse::look_at_listing("out/", &[], &keys).0,
            EntryKind::MultiFile,
            "and the same in a bucket"
        );
    }

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
        let (kind, holds) = look_at_directory(dir);
        let mut entry = Entry {
            path: dir.to_path_buf(),
            kind,
            name: dir.file_name().unwrap().to_string_lossy().into_owned(),
            size: None,
            modified: None,
            rows: None,
            cols: None,
            cols_sampled: false,
            columns: Vec::new(),
            cost: Cost::default(),
            holds,
            opens_whole_directory: false,
            format_spec: None,
            table: None,
        };
        enrich(&mut entry);
        entry
    }

    /// The shape that prompted this: one Parquet file per table, sharing an extension
    /// and nothing else. Named for what it is rather than what it is called, because
    /// the filenames are exactly what cannot decide it.
    /// A record cached before the rename still says how many subdirectories it saw.
    #[test]
    fn holds_written_as_folders_still_reads() {
        let old: Holds = serde_json::from_str(r#"{"folders":3,"partitions":2}"#).unwrap();
        assert_eq!((old.directories, old.partitions), (3, 2));
        let new = serde_json::to_string(&old).unwrap();
        assert!(new.contains(r#""directories":3"#), "{new}");
    }

    #[test]
    fn a_directory_of_separate_tables_is_not_a_dataset() {
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
        assert_eq!(
            entry.cols,
            Some(9),
            "the union of what the directory holds is still a true answer to what is in it"
        );
        assert_eq!(entry.label(), "3 parquet", "and the label counts the files");
    }

    /// The rows of one table split across files, which is what `multi` is for.
    /// The directories the old threshold took as one table and nesting does not.
    ///
    /// Two files that each bring a column the other lacks — a renamed column is the
    /// everyday case — scored two thirds against a bar of a half, so they opened as one
    /// table and the union carried both spellings with nulls under each. Nothing datui
    /// can see tells that apart from two tables that share most of their columns, which
    /// is why the number moved rather than the question.
    ///
    /// The directory is not refused. It is a place to look inside, and the row inside it
    /// opens the union anyway.
    #[test]
    fn a_directory_whose_files_each_bring_a_column_is_a_place_to_look_inside() {
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), "old.parquet", &["id", "ts", "amount"]);
        write(dir.path(), "new.parquet", &["id", "ts", "amt"]);

        assert_eq!(
            classify_directory(dir.path()),
            EntryKind::MultiFile,
            "the names alone still say two Parquet files"
        );
        let entry = measured(dir.path());
        assert_eq!(
            entry.kind,
            EntryKind::Directory,
            "and the footers say neither file's columns are in the other's"
        );
        assert_eq!(entry.label(), "2 parquet", "which the label still reports");
        assert_eq!(entry.rows, None, "a sum over two tables is not a number");
    }

    #[test]
    fn a_directory_of_one_table_stays_a_dataset() {
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
        assert_eq!(entry.cols, Some(4), "and the partition column `year`");
    }

    /// Past the counting limit the columns come from a spread of the directory rather
    /// than its head, because a directory written over time is narrowest at the start.
    #[test]
    fn a_directory_too_large_to_count_still_reports_the_columns_it_gained() {
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
    fn a_lake_table_is_not_a_directory_of_parquet_files() {
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
                "{marker} says what this directory is"
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

        // A directory that merely has those names is not a table.
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
            "metadata with no data/ beside it is somebody's directory, not a table root"
        );
    }

    /// A single file counts its columns the same way a directory does, and both count
    /// what opening it shows.
    ///
    /// `enrich_parquet` read `schema_descr.columns()`, which is the leaf list — so a file
    /// with one struct of two fields said `columns 3` above a schema list of two, and a
    /// directory holding only that file said something different again.
    #[test]
    fn a_file_and_a_directory_of_it_count_the_same_columns() {
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
        let directory = measured(dir.path());
        assert_eq!(directory.kind, EntryKind::MultiFile);
        assert_eq!(
            directory.cols, file.cols,
            "and a directory of them says the same number"
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

    /// A directory whose files encode the same nested column differently counts it once.
    ///
    /// The union is over leaf paths, and the same nested column written by parquet-mr and
    /// by Arrow gives different leaves — so the row reported roughly twice the width of a
    /// directory `is_one_table` had just called one dataset. Counted from each file's own
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

    /// A count read from a spread of a directory rather than all of it says it is a
    /// floor.
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

        // A directory small enough to read every footer of claims no such thing.
        let small = tempfile::tempdir().unwrap();
        write(small.path(), "a.parquet", &["id", "ts"]);
        write(small.path(), "b.parquet", &["id", "ts"]);
        assert!(!measured(small.path()).cols_sampled);
    }

    /// The files a directory offers come back in order, whatever order it was
    /// written in.
    ///
    /// Every caller reads order as meaning something — `sample_footers` takes the ends
    /// and the middle, and the union of the columns is built in the order the files
    /// appear. Unsorted, "the last file" was whichever one the filesystem happened to
    /// return last, which on the filesystems that return creation order is the one
    /// written first as often as not.
    #[test]
    fn the_files_a_directory_offers_come_back_in_order() {
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

    /// A directory past the budget still says so, and the files it keeps are the
    /// directory's first rather than the listing's.
    ///
    /// The ordering itself is `the_files_a_directory_offers_come_back_in_order`'s to
    /// prove: a directory read may return sorted entries of its own accord, so an
    /// assertion here about order could hold for the wrong reason. What this pins is
    /// *which* files survive the cap, and that the cap still says "too many to count".
    #[test]
    fn a_directory_past_the_budget_keeps_the_directories_first_files() {
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
        // directory's own first sixty-five. Sorting after truncating gives sixty-five
        // sorted names from wherever the read began, which is a different set.
        assert_eq!(
            names, expected,
            "the directory's first files, not the listing's"
        );
    }

    /// The log is named rather than looked for, so a table's own data files cannot
    /// crowd it out of the listing however many of them there are.
    #[test]
    fn a_lake_table_is_recognized_among_its_data_files() {
        let dir = tempfile::tempdir().unwrap();
        for part in 0..32 {
            write(dir.path(), &format!("part-{part:03}.parquet"), &["id"]);
        }
        std::fs::create_dir_all(dir.path().join("_delta_log")).unwrap();
        assert_eq!(classify_directory(dir.path()), EntryKind::Delta);
    }

    /// Past the counting limit the row count is out of reach, but whether the directory
    /// is one table is not — and a directory of a hundred tables is exactly where reading
    /// them as one costs most.
    #[test]
    fn a_directory_too_large_to_count_is_still_checked() {
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

    /// The same directory size, but one table split across it.
    #[test]
    fn a_large_directory_of_one_table_stays_a_dataset() {
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

    /// Searching the home screen by column should still find a directory that holds one,
    /// even once the directory is no longer offered as a single table.
    #[test]
    fn a_downgraded_directory_keeps_every_column_its_files_have() {
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
        // And one a level down, so "every column its files have" is a claim about more
        // than the directory's own: the row's width is its own files, its names are
        // everything under it, and a fixture with no subdirectory cannot tell those
        // apart.
        let seasons = dir.path().join("seasons");
        std::fs::create_dir_all(&seasons).unwrap();
        write(&seasons, "2024.parquet", &["season_year", "round"]);

        let entry = measured(dir.path());
        assert_eq!(entry.kind, EntryKind::Directory);
        for column in [
            "circuit_id",
            "lat",
            "lng",
            "driver_id",
            "code",
            "nationality",
            "season_year",
            "round",
        ] {
            assert!(
                entry.columns.iter().any(|c| c == column),
                "{column} in {:?}",
                entry.columns
            );
        }
    }

    #[test]
    fn a_name_no_reader_takes_is_refused_before_opening() {
        let refused = |name: &str| unreadable_by_name(std::path::Path::new(name));
        assert!(refused("gs://b/ml/onnx/pipeline_rf.onnx"));
        assert!(refused("model.onnx.gz"));
        assert!(refused("README.md"));
        for readable in [
            "a.csv",
            "a.CSV",
            "a.csv.gz",
            "a.parquet",
            "a.xlsx",
            "data.gz",
            "part-0000",
        ] {
            assert!(!refused(readable), "{readable}");
        }
    }
}

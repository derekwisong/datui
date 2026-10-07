//! Dataset discovery for the home screen. Not a catalog: listings are computed from
//! the filesystem when asked and forgotten at session end; between runs datui keeps
//! only recent paths and measured shapes (`remembered`, valid while the files are
//! unchanged). Every function scans one directory level, since data lives on slow
//! mounts and huge partition trees.

use std::path::{Path, PathBuf};

/// Compression suffixes that may follow a data extension (`sales.csv.gz`).
const COMPRESSION_EXTENSIONS: &[&str] = &["gz", "bz2", "xz", "zst", "zstd"];

/// Upper bound on entries read from one directory, so a huge one cannot hang the UI.
pub const MAX_ENTRIES_PER_DIR: usize = 5_000;

/// What a home-screen row represents.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EntryKind {
    /// A single data file.
    File,
    /// A file datui has no reader for: hidden on home until `Ctrl+A` shows it dimmed.
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
    /// Somewhere remote not looked at yet: classifying would read it, which blocks when
    /// the network is gone, so it is offered as openable, unlabeled. Also what an
    /// unrecognized kind deserializes as, so an older build can still read a dataset
    /// index written by a newer one (see `CLASSIFIER_VERSION`).
    #[serde(other)]
    Unknown,
}

/// Bump whenever classification changes. A cached kind is restored without
/// re-deriving it (a remote row cannot be read cheaply), so it is restored only when
/// written by a build with the same version; measurements in the record (rows,
/// columns, cost) survive regardless.
pub const CLASSIFIER_VERSION: u32 = 7;

impl EntryKind {
    /// Short label shown next to the entry name.
    pub fn label(self) -> &'static str {
        match self {
            // A file with no reader says nothing: it is dimmed, and Enter shows its bytes.
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

    /// Whether selecting this entry opens a dataset rather than navigating. An unexamined
    /// remote path counts: datui can open a prefix or hive directory directly.
    pub fn is_dataset(self) -> bool {
        !matches!(self, EntryKind::Directory | EntryKind::Other) && !self.is_lake_table()
    }

    /// Whether this row is known to be a dataset, for counting: unlike
    /// [`EntryKind::is_dataset`] ("may this be opened"), a row nothing has looked into
    /// does not count.
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

    /// The format's name for prose; `label` is the row's lowercase chip.
    pub fn lake_name(self) -> Option<&'static str> {
        match self {
            EntryKind::Delta => Some("Delta"),
            EntryKind::Iceberg => Some("Iceberg"),
            EntryKind::Hudi => Some("Hudi"),
            _ => None,
        }
    }
}

/// What one listing of a directory found, counted rather than judged. The row's label
/// comes from here (`12 parquet`), true whether or not the files are one table.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Holds {
    /// Data files by format, commonest first, as [`crate::FileFormat::name`] strings so
    /// records survive across builds.
    #[serde(default)]
    pub formats: Vec<(String, usize)>,
    /// Subdirectories, partitions among them (`folders` in 0.4.0 development builds).
    #[serde(default, alias = "folders")]
    pub directories: usize,
    /// `key=value` subdirectories, which are also counted in `directories`.
    #[serde(default)]
    pub partitions: usize,
    /// Files datui has no reader for (a README, a script, a notebook): neither data nor a
    /// writer's own.
    #[serde(default)]
    pub not_read: usize,
    /// Files with no extension, neither data nor `not_read` by name: Spark and GBIF part
    /// files, which the open reads by their bytes.
    #[serde(default)]
    pub unnamed: usize,
    /// Entries skipped as a writer's own, and the first few by name for the pane.
    #[serde(default)]
    pub skipped: usize,
    #[serde(default)]
    pub skipped_names: Vec<String>,
    /// The listing stopped at [`MAX_ENTRIES_PER_DIR`], so every count is a floor (`5000+`).
    #[serde(default)]
    pub truncated: bool,
    /// A Hugging Face DatasetDict saved with `save_to_disk` (`dataset_dict.json` beside
    /// split directories), read as Arrow, one split at a time.
    #[serde(default)]
    pub dataset_dict: bool,
}

/// How many skipped names are kept for the pane. Enough to recognise the convention.
pub(crate) const SKIPPED_NAMES_SHOWN: usize = 4;

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

    /// The weights' format and file count when this directory is a model (one weight
    /// format plus only JSON). See `is_model_directory`.
    pub fn model_weights(&self) -> Option<(&str, usize)> {
        if !is_model_directory(counts_names(self)) {
            return None;
        }
        self.formats
            .iter()
            .find(|(name, _)| is_weights(name))
            .map(|(name, count)| (name.as_str(), *count))
    }

    /// The directory row's label when its kind does not name itself: `12 parquet`,
    /// `mixed`, or `dir` when no data is directly inside.
    pub fn label(&self) -> String {
        let more = if self.truncated { "+" } else { "" };
        // A model's weights beside config and tokenizer JSON: labeled as the model, not
        // `mixed`.
        if let Some((name, count)) = self.model_weights() {
            return format!("{count}{more} {name}");
        }
        match self.formats.as_slice() {
            // `dir` says there is no data file inside; a cut listing cannot claim that. A
            // directory of directories counts them.
            [] if self.directories == 1 => format!("1 dir{more}"),
            [] if self.directories > 1 => format!("{}{more} dirs", self.directories),
            [] => format!("dir{more}"),
            // The `+` hedges the whole claim: past the cap another format may lurk.
            [(name, count)] => format!("{count}{more} {name}"),
            // No `+`: `mixed` is not a count, and more files cannot unmake it.
            _ => "mixed".to_string(),
        }
    }

    /// Whether nothing has been counted: a file, or a directory not looked into. (An
    /// empty directory that was looked into reads `dir` too.)
    pub fn is_empty(&self) -> bool {
        self.formats.is_empty()
            && self.directories == 0
            && self.skipped == 0
            && self.not_read == 0
            && self.unnamed == 0
            // Every field, even those implied by others today: this guards a placeholder from
            // erasing a row's count and should not depend on that invariant.
            && self.partitions == 0
            && self.skipped_names.is_empty()
            && !self.dataset_dict
            // A cut listing that found nothing still says more exists (`dir+`).
            && !self.truncated
    }

    /// The details pane's `contains` line: data files by format, directories and
    /// partitions. Unreadable files and writer markers are left out (they would read as a
    /// warning); a row inside the directory says what is hidden.
    pub fn line(&self, with_partitions: bool) -> Option<String> {
        let more = if self.truncated { "+" } else { "" };
        let mut parts: Vec<String> = self
            .formats
            .iter()
            .map(|(name, count)| format!("{count}{more} {name}"))
            .collect();
        // Partitions are counted in `directories` too; name only the rest.
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
    /// Whether Enter lists the tables inside: a file of several that is not itself a
    /// table. A file opening one of its tables, or a spec's variants file, opens on
    /// Enter; → lists them.
    pub fn enter_lists_tables(&self) -> bool {
        self.kind == EntryKind::File
            && self.cost.tables.is_some_and(|n| n > 1)
            && !self.cost.opens_one
            && self.format_spec.is_none()
    }

    /// Whether home hides this row until Ctrl+A: an unreadable file or a database's
    /// internal table.
    pub fn hidden_by_default(&self) -> bool {
        self.kind == EntryKind::Other || self.table.as_ref().is_some_and(|t| t.internal)
    }

    /// The short label beside a row's name, saying what it holds: a looked-into
    /// directory's count (`12 parquet`, `mixed`, `dir`), a lake table's or hive root's
    /// format, else the kind.
    pub fn label(&self) -> std::borrow::Cow<'static, str> {
        match self.kind {
            // See `opens_whole_directory`.
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
            // A file named for its contents (as a collection names one): its format, which the
            // name no longer says.
            EntryKind::File
                if crate::FileFormat::from_path(Path::new(&self.name)).is_none()
                    && crate::FileFormat::from_path(&self.path).is_some() =>
            {
                crate::FileFormat::from_path(&self.path)
                    .map(crate::FileFormat::name)
                    .unwrap_or_default()
                    .into()
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
    /// Size in bytes; for multi-file datasets the sum of files inspected, a floor.
    pub size: Option<u64>,
    pub modified: Option<std::time::SystemTime>,
    /// Row count, when it can be had without reading data (Parquet footers only).
    pub rows: Option<usize>,
    /// Column count, same caveat.
    pub cols: Option<usize>,
    /// Whether `cols` came from a spread of the directory (ends and middle, past the
    /// footer budget), so it is a floor, shown as `6+`.
    pub cols_sampled: bool,
    /// Column names when free to get (a Parquet footer carries them with the row count).
    pub columns: Vec<String>,
    /// What opening this costs: where it lives, how it is stored and laid out, from bytes
    /// already read.
    pub cost: Cost,
    /// What one listing found, for a directory; empty for a file or an unexamined
    /// directory.
    pub holds: Holds,
    /// Whether this row is the door opening the browsed directory. It carries no label:
    /// labels count what is directly inside, and this row reads all of it.
    pub opens_whole_directory: bool,
    /// The format spec that reads this file, by glob or by magic.
    pub format_spec: Option<String>,
    /// A table inside a file of tables (SQLite, a NumPy archive): its path is the file's
    /// with the table's name appended.
    pub table: Option<TableOf>,
}

/// What a row inside a file of tables says about its table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableOf {
    /// The containing file's format; `None` for a spec's variant (see
    /// [`Entry::format_spec`]).
    pub format: Option<crate::FileFormat>,
    /// What the file calls it: SQLite's `table`, `view`, `virtual` or `shadow`, or a NumPy
    /// archive's `array`.
    pub kind: String,
    /// SQLite's own (schema, statistics, shadow tables): hidden until Ctrl+A, opened like
    /// any other.
    pub internal: bool,
}

/// What pressing Enter on a dataset will cost (200 MB of zstd Parquet is gigabytes in
/// memory, and NFS is not tmpfs), all from what datui already reads: the mount table
/// and the footer.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Cost {
    /// Filesystem or URL scheme: `nfs4`, `ext4`, `tmpfs`, `fuse.sshfs`, `s3`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    /// Bytes once decompressed, as against on disk.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uncompressed: Option<u64>,
    /// Compression codec, as the file itself names it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub codec: Option<String>,
    /// Row groups: one huge group cannot be read in parallel or skipped; thousands of
    /// tiny ones cost overhead.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row_groups: Option<usize>,
    /// Partition layout, for a hive dataset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partitions: Option<Partitions>,
    /// Tables of its own, for a file of tables: one opens, several are listed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tables: Option<usize>,
    /// Whether a file of several tables opens one (a workbook's first sheet): Enter opens
    /// it and → lists them.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub opens_one: bool,
    /// An Arrow IPC stream (converted before scanning) rather than an IPC file (scanned in
    /// place), from its first bytes.
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

/// How opening `entry` reads it ([`crate::FileFormat::read_mode`] for its format and
/// storage), and whether a remote one is downloaded first. `None` unless a file's name
/// says its format.
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
        None if entry.table.is_some() => {
            crate::cli::FormatChoice::Builtin(entry.table.as_ref().and_then(|t| t.format)?)
        }
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

/// How a hive dataset is laid out, from directory names alone.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Partitions {
    /// Partition keys, outermost first: `["year", "month"]`.
    pub keys: Vec<String>,
    /// Distinct values seen for the outermost key, sorted; bounded, so not necessarily
    /// all.
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

    /// A file entry with a chosen display name, for tests.
    pub fn for_test(path: &Path, name: &str) -> Self {
        Self::new(path.to_path_buf(), EntryKind::File).with_name(name)
    }

    /// The row under a name other than its path's last part.
    pub(crate) fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
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

/// Whether a key or path is Parquet: named `.parquet`, or an extensionless part file
/// in a `.parquet` directory (Spark, GBIF: `occurrence.parquet/000001`). Hidden and
/// job files (`_SUCCESS`, `.crc`) are not.
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

/// What a file with no usable extension is, from its first bytes (Parquet, Arrow,
/// Avro and ORC carry signatures; CSV and JSON have none and are not guessed). Asked
/// only of a directory being opened, never one being looked at; which signatures a
/// listing trusts is each format's call (`crate::readers::Trusted::listing`).
pub fn sniff_format(path: &Path) -> Option<crate::FileFormat> {
    crate::readers::sniff_file(path, crate::readers::Asked::Listing)
}

/// What a listing finds a file to be by its first bytes.
#[derive(Debug, Clone)]
pub enum Sniffed {
    /// A format datui reads.
    Format,
    /// A format spec's, which reads it.
    Spec(std::sync::Arc<crate::formats::Spec>),
}

/// [`sniff_format`], else the format spec an open would pick (by glob, else by magic
/// and `match.where`), from one read of the file's head.
pub fn sniff_listed(path: &Path, formats: &crate::formats::Registry) -> Option<Sniffed> {
    use crate::readers::{Asked, HEAD, head_of, sniff};
    let head = head_of(path)?;
    if sniff(&head, Some(path), Asked::Listing, |_| true).is_some() {
        return Some(Sniffed::Format);
    }
    formats
        .listed(path, &head, head.len() < HEAD)
        .map(Sniffed::Spec)
}

/// Name `entry` a file of `spec`; a spec reading several record variants also makes
/// it a place whose tables → lists.
pub fn name_spec_file(entry: &mut Entry, spec: &crate::formats::Spec) {
    entry.kind = EntryKind::File;
    entry.format_spec = Some(spec.name.clone());
    if spec.lists_variants() {
        entry.cost.tables = Some(spec.records.variants.len());
    }
}

/// Name an unlisted local file row (a recent) by the spec that reads it, as a listing
/// would: by glob, else by magic when its name says nothing.
pub fn name_unlisted_file(entry: &mut Entry, formats: &crate::formats::Registry) {
    if formats.is_empty()
        || entry.kind != EntryKind::File
        || entry.table.is_some()
        || is_data_file(&entry.path)
    {
        return;
    }
    let spec = match formats.by_glob(&entry.path, false).into_iter().next() {
        Some(spec) => Some(spec),
        None if worth_sniffing(&entry.path) && is_regular_file(&entry.path) => {
            match sniff_listed(&entry.path, formats) {
                Some(Sniffed::Spec(spec)) => Some(spec),
                _ => None,
            }
        }
        None => None,
    };
    if let Some(spec) = spec {
        name_spec_file(entry, &spec);
    }
}

/// How many extensionless files one listing sniffs; past this the rest are listed by
/// name.
pub(crate) const MAX_SNIFFS_PER_DIR: usize = 256;

/// Whether a file's name has no extension at all: `part-00000`, `LICENSE`.
pub fn has_no_extension(path: &Path) -> bool {
    path.extension().is_none()
}

/// Whether a listing sniffs a file: no extension, an uninformative one (`.bin`), or
/// only text (`.log`, which candump writes; `.txt`).
pub fn worth_sniffing(path: &Path) -> bool {
    path.extension().is_none_or(|e| {
        e.eq_ignore_ascii_case("bin") || data_format(path).is_some_and(crate::FileFormat::is_lines)
    })
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

/// Whether a path names a Parquet file, by extension or as an extensionless part file
/// in a `.parquet` directory. Unlike [`is_parquet_key`], a writer's own file
/// (`_manifest.parquet`) counts: it can be opened, even if it does not make its
/// directory a dataset.
pub fn is_parquet_path(path: &Path) -> bool {
    path.extension()
        .and_then(|e| e.to_str())
        .is_some_and(|e| e.eq_ignore_ascii_case("parquet"))
        || is_parquet_key(&directory_and_name(path))
}

/// Whether a path looks openable by name or place (a part file in a `.parquet`
/// directory). Every route asks here so they agree.
pub fn is_data_file(path: &Path) -> bool {
    data_extension(path).is_some() || is_parquet_key(&directory_and_name(path))
}

/// The extension saying what a file is, past any compression suffix (`sales.csv.gz`
/// is CSV); `None` when not something datui reads.
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

/// Whether a name already rules the file out: an extension no reader takes, past any
/// compression suffix. A bare `data.gz` or extensionless name is left to the open.
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

/// The format a file's name says it holds, past any compression suffix. Extensions
/// are not formats (`.ipc`, `.arrow`, `.arrows`, `.feather` are one); asking
/// [`crate::FileFormat`] keeps home from listing what the reader cannot open.
pub fn data_format(path: &Path) -> Option<crate::FileFormat> {
    // A sharded checkpoint's index is the model, not a JSON table.
    crate::FileFormat::from_name_ending(path)
        .or_else(|| crate::FileFormat::from_extension(&data_extension(path)?))
}

/// The `dir/name` string `is_parquet_key` splits on `/`, so Windows backslashes do
/// not make `occurrence.parquet\000001` one dotted name.
pub(crate) fn directory_and_name(path: &Path) -> String {
    let name = path.file_name().unwrap_or_default().to_string_lossy();
    match path.parent().and_then(|p| p.file_name()) {
        Some(directory) => format!("{}/{name}", directory.to_string_lossy()),
        None => name.into_owned(),
    }
}

/// Format rank: commonest first, Parquet winning ties, then by name, so the order
/// never depends on `read_dir`. One order for the local label, the local reader
/// choice and the cloud label, so a row's label and `Enter` agree.
pub(crate) fn rank_formats(a: (&str, usize), b: (&str, usize)) -> std::cmp::Ordering {
    // Text loses: a README among data files is not the table.
    let text = crate::FileFormat::Text.name();
    (a.0 == text)
        .cmp(&(b.0 == text))
        .then_with(|| b.1.cmp(&a.1))
        .then_with(|| (a.0 != "parquet").cmp(&(b.0 != "parquet")))
        .then_with(|| a.0.cmp(b.0))
}

/// Whether a file is Hugging Face `datasets` metadata beside Arrow shards, so its
/// JSON does not outnumber a one-shard dataset and get read instead.
pub(crate) fn is_hugging_face_metadata(name: &str) -> bool {
    matches!(name, "dataset_info.json" | "state.json")
}

fn order_formats(counts: &mut [(crate::FileFormat, usize)]) {
    counts.sort_by(|a, b| rank_formats((a.0.name(), a.1), (b.0.name(), b.1)));
}

/// Whether a listing entry is bookkeeping: a leading `_` or `.` (`_SUCCESS`,
/// `_committed_*`, `_metadata.json`, `.crc`) or the `_$folder$` marker. One predicate
/// for every listing and open, so they agree.
pub fn is_bookkeeping(name: &str) -> bool {
    // `_$folder$` first: the folder it stands for may be a partition (`year=2024_$folder$`
    // beside `year=2024/`), which the partition test would call data.
    if name.ends_with("_$folder$") {
        return true;
    }
    // A `key=value` name is a partition, even with a leading `_` (Spark and Hive
    // partition on internal columns like `_date=…`).
    if is_partition_name(name) {
        return false;
    }
    name.starts_with(['_', '.'])
}

/// Whether a format, by name, is model weights.
fn is_weights(name: &str) -> bool {
    name == crate::FileFormat::Safetensors.name() || name == crate::FileFormat::Gguf.name()
}

/// Whether formats side by side are a model: one weight format with only JSON beside
/// (config, tokenizer), however many JSON files.
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

    // Extensionless files (Spark and GBIF part files) in a directory whose names settled
    // nothing; skipped when names already answer (a `LICENSE` beside Parquet). A
    // [`spread`] is sniffed, not every file: the ends must agree, and then all are taken
    // as that format (the scan reports any odd one out).
    if by_format.is_empty() && !nameless.is_empty() {
        nameless.sort();
        let picks = spread(nameless.len());
        let sniffed: Vec<crate::FileFormat> = picks
            .iter()
            .filter_map(|i| sniff_format(&nameless[*i]))
            .collect();
        if sniffed.len() == picks.len()
            && let Some(found) = sniffed.first().copied()
            && sniffed.iter().all(|f| *f == found)
        {
            by_format.push((found, nameless));
        }
    }

    // Text is data only where nothing else is: a README beside Parquet is not a
    // candidate.
    if by_format.iter().any(|(f, _)| !f.is_lines()) {
        by_format.retain(|(f, _)| !f.is_lines());
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

    // Decided after the whole listing, not at the first deciding entry, so read order
    // does not matter. One `key=value` below and the data is down there.
    if partitioned {
        return DirectoryFormat::Deeper;
    }

    // The shared format rank, so the reader picked is the format the label names.
    by_format.sort_by(|a, b| rank_formats((a.0.name(), a.1.len()), (b.0.name(), b.1.len())));
    // A directory of model weights is the model, however much config and tokenizer
    // JSON sits beside it; the JSON is passed over.
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

/// How far down a hive root is followed for its files (year/month/day/hour is four);
/// deeper is not a hive dataset, and each level costs a read on a share.
const MAX_HIVE_DEPTH: usize = 16;

/// What the files under a hive root's partitions are. [`directory_format`] can only
/// say `Deeper` of a root, so this follows one spine (the first partition at each
/// level, as a hive scan reads its schema) to the bottom. A sample: disagreeing
/// partitions report as the first one.
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

/// The first `key=value` subdirectory of `dir` by name, so runs agree.
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

/// An empty extensionless object: a tool's folder marker (`yellow/year=2032`), whether
/// or not the folder still exists. Nothing datui opens is both empty and nameless.
pub fn is_empty_marker(name: &str, size: u64) -> bool {
    size == 0 && !name.contains('.')
}

/// Classify a directory without walking it: one listing bounded by
/// [`MAX_ENTRIES_PER_DIR`] (not a probe of the first few entries, whose answer would
/// depend on read order). Includes rows on network mounts: one `getdents` walk,
/// stat'ing only symlinks. `home_open_selected` calls it on the key thread; others
/// on workers.
pub fn classify_directory(path: &Path) -> EntryKind {
    look_at_directory(path).0
}

/// The kind (what `Enter` does) and what the listing found (what the label says),
/// from one read.
pub fn look_at_directory(path: &Path) -> (EntryKind, Holds) {
    // Before the listing: three `join` tests answer it, where counting would walk every
    // table of a warehouse each pass. A bucket prefix finds markers in its listing.
    if let Some(lake) = lake_table(path) {
        return (lake, Holds::default());
    }
    let Ok(iter) = std::fs::read_dir(path) else {
        return (EntryKind::Directory, Holds::default());
    };
    let directory = path.file_name().unwrap_or_default().to_string_lossy();
    let rules = Rules {
        directory: &directory,
        // As the listing of the rows inside does, so the tally agrees with them.
        sniff: (!crate::home::is_remote_path(path)).then_some(path),
        in_bucket: false,
    };
    // One past the cap, so "there is more" is known; bounded where entries come from,
    // since `.crc` files beside each data file would double the walk. Past the cap the
    // first entries decide.
    let mut truncated = false;
    let seen = iter
        .flatten()
        .take(MAX_ENTRIES_PER_DIR + 1)
        .enumerate()
        .map_while(|(at, entry)| {
            truncated = at == MAX_ENTRIES_PER_DIR;
            (!truncated).then(|| seen_on_disk(&entry))
        });
    let (kind, mut holds) = classify(seen, &rules);
    holds.truncated = truncated;
    (kind, holds)
}

/// A local entry as [`classify`] sees it, from the read's file type (no stat per
/// entry; symlinks still need one). Regular files only: a FIFO named `a.csv` blocks
/// its opener, a broken symlink opens as nothing.
fn seen_on_disk(entry: &std::fs::DirEntry) -> Seen {
    let (is_dir, is_file) = match entry.file_type() {
        Ok(kind) if !kind.is_symlink() => (kind.is_dir(), kind.is_file()),
        _ => std::fs::metadata(entry.path()).map_or((false, false), |m| (m.is_dir(), m.is_file())),
    };
    Seen {
        name: entry.file_name().to_string_lossy().into_owned(),
        is_dir,
        is_file,
        size: None,
    }
}

/// One entry of a listing, on disk or in a bucket, as [`classify`] asks about it.
#[derive(Debug, Clone)]
pub struct Seen {
    pub name: String,
    pub is_dir: bool,
    /// A regular file an open can read (not a FIFO, socket or broken symlink).
    pub is_file: bool,
    /// Bytes, where listed; an empty extensionless file is a folder marker, not data.
    pub size: Option<u64>,
}

/// Where the local and the bucket listings deliberately differ.
pub struct Rules<'a> {
    /// The listed directory's name: an extensionless part file in `occurrence.parquet/` is
    /// Parquet by where it sits.
    pub directory: &'a str,
    /// Where to look inside nameless files, a few per listing; `None` where each open is
    /// a round trip that may not return.
    pub sniff: Option<&'a Path>,
    /// A bucket prefix: lake tables are found by names in the listing (Iceberg by layout
    /// alone), a DatasetDict by `dataset_dict.json`, and only Parquet (read in place) is
    /// offered as many files that are one table.
    pub in_bucket: bool,
}

/// A directory's kind and holdings from one listing level: the one rule for
/// [`look_at_directory`] and bucket prefixes, so a directory and its mirror agree;
/// [`Rules`] holds their differences. The caller sets truncation.
pub fn classify(seen: impl Iterator<Item = Seen>, rules: &Rules) -> (EntryKind, Holds) {
    use crate::FileFormat;
    let mut holds = Holds::default();
    let mut counts: Vec<(FileFormat, usize)> = Vec::new();
    let mut skipped: Vec<String> = Vec::new();
    // Counted as data until the listing is done; see `is_hugging_face_metadata`.
    let mut hugging_face: Vec<String> = Vec::new();
    let mut dict_file = false;
    let mut lake: Vec<&'static str> = Vec::new();
    // `present` is everything but a writer's own: what the majority is measured against.
    let (mut present, mut data_files, mut parquet) = (0usize, 0usize, 0usize);
    let mut sniffs_left = rules.sniff.map_or(0, |_| MAX_SNIFFS_PER_DIR);
    for s in seen {
        // Lake markers are spec, not strays: looked for before bookkeeping skips
        // `_delta_log`.
        if s.is_dir
            && rules.in_bucket
            && let Some(marker) = ["_delta_log", ".hoodie", "metadata", "data"]
                .into_iter()
                .find(|m| *m == s.name)
        {
            lake.push(marker);
        }
        if is_bookkeeping(&s.name) {
            skipped.push(s.name);
            continue;
        }
        present += 1;
        if s.is_dir {
            holds.directories += 1;
            holds.partitions += usize::from(is_partition_name(&s.name));
            continue;
        }
        if s.size.is_some_and(|size| is_empty_marker(&s.name, size)) {
            holds.not_read += 1;
            continue;
        }
        let name = Path::new(&s.name);
        let key = format!("{}/{}", rules.directory, s.name);
        let named = data_format(name);
        let found = named
            // Text by its name, unless its bytes say more: below.
            .filter(|f| !f.is_lines())
            // A sharded checkpoint's index counts as JSON, so the label counts shards; the read
            // still uses it.
            .map(|f| match crate::model_files::is_safetensors_index(name) {
                true => FileFormat::Json,
                false => f,
            })
            // Data by where it sits rather than by its name: see [`is_data_file`].
            .or_else(|| is_parquet_key(&key).then_some(FileFormat::Parquet))
            .or_else(|| {
                let dir = rules.sniff?;
                spend_sniff(&mut sniffs_left, s.is_file, name)
                    .then(|| sniff_format(&dir.join(name)))?
            })
            .or(named)
            .filter(|_| s.is_file);
        let Some(found) = found else {
            // No reader and no name; extensionless files are counted apart (Spark part files).
            match s.is_file && has_no_extension(name) {
                true => holds.unnamed += 1,
                false => holds.not_read += 1,
            }
            continue;
        };
        if found == FileFormat::Json && is_hugging_face_metadata(&s.name) {
            hugging_face.push(s.name.clone());
        }
        dict_file |= found == FileFormat::Json && s.name == crate::hf_splits::DATASET_DICT;
        data_files += 1;
        parquet += usize::from(is_parquet_key(&key));
        match counts.iter_mut().find(|(f, _)| *f == found) {
            Some((_, n)) => *n += 1,
            None => counts.push((found, 1)),
        }
    }

    // Hugging Face's JSON is its writer's own, like `_SUCCESS`, as is a DatasetDict's
    // `dataset_dict.json`.
    if !counts.iter().any(|(f, _)| *f == FileFormat::Arrow) {
        hugging_face.clear();
    }
    holds.dataset_dict = rules.in_bucket && dict_file && holds.directories > 0;
    if holds.dataset_dict {
        hugging_face.push(crate::hf_splits::DATASET_DICT.to_string());
    }
    if let Some((_, n)) = counts.iter_mut().find(|(f, _)| *f == FileFormat::Json) {
        *n -= hugging_face.len();
        data_files -= hugging_face.len();
        present -= hugging_face.len();
        skipped.append(&mut hugging_face);
    }
    // Text is data only where nothing else is.
    if counts.iter().any(|(f, _)| !f.is_lines()) {
        for (_, n) in counts.iter_mut().filter(|(f, _)| f.is_lines()) {
            data_files -= *n;
            holds.not_read += std::mem::take(n);
        }
    }
    counts.retain(|(_, n)| *n > 0);
    order_formats(&mut counts);
    let one_readable = matches!(counts.as_slice(), [(f, _)] if f.reads_many_files());
    holds.formats = counts
        .into_iter()
        .map(|(f, n)| (f.name().to_string(), n))
        .collect();
    // The first few by name, so runs agree; an object and prefix of one name are one
    // thing.
    skipped.sort();
    skipped.dedup();
    holds.skipped = skipped.len();
    skipped.truncate(SKIPPED_NAMES_SHOWN);
    holds.skipped_names = skipped;

    // Lake tables' data files agree on a schema, so the rules below would wrongly say
    // "one table". Iceberg needs its whole shape (data under `data/`).
    let marked = |m| lake.contains(&m);
    if marked("_delta_log") {
        return (EntryKind::Delta, holds);
    }
    if marked(".hoodie") {
        return (EntryKind::Hudi, holds);
    }
    if marked("metadata") && marked("data") && parquet == 0 {
        return (EntryKind::Iceberg, holds);
    }
    // No majority rule: one stray `notes=old/` reads `hive`, but majorities refused real
    // hive roots with a README or `scripts/` beside them, the worse mistake.
    if holds.partitions > 0 && holds.partitions >= data_files {
        return (EntryKind::Hive, holds);
    }
    // One multi-file-readable format that dominates: a couple of stray CSVs make a
    // place, not a dataset. A model opens as the model despite its JSON.
    let one_table = if rules.in_bucket {
        parquet > 1 && parquet == data_files && parquet * 2 >= present
    } else {
        (data_files > 1 && one_readable && data_files * 2 >= present)
            || (holds.partitions == 0 && is_model_directory(counts_names(&holds)))
    };
    let kind = match one_table {
        true => EntryKind::MultiFile,
        false => EntryKind::Directory,
    };
    (kind, holds)
}

/// Spend one of a listing's sniffs on `name`: a regular file whose name says nothing
/// ([`worth_sniffing`]), budget permitting.
fn spend_sniff(left: &mut usize, is_file: bool, name: &Path) -> bool {
    let spend = is_file && *left > 0 && worth_sniffing(name);
    *left -= usize::from(spend);
    spend
}

/// Entries under `metadata/` checked for Iceberg: `v1.metadata.json` is written on
/// the first commit and never removed, but read order is arbitrary.
const ICEBERG_METADATA_PROBE: usize = 64;

/// Whether `path` is a lake table root, and which. Marker names are part of these
/// formats' specs (unlike filename conventions); three `join` tests answer it
/// without walking a table's files.
fn lake_table(path: &Path) -> Option<EntryKind> {
    if path.join("_delta_log").is_dir() {
        return Some(EntryKind::Delta);
    }
    if path.join(".hoodie").is_dir() {
        return Some(EntryKind::Hudi);
    }
    // Iceberg's marker is a plain name, so it needs the whole shape: `metadata/` with a
    // metadata file, beside `data/`.
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

/// What one directory listing produced, and whether it saw all of it.
#[derive(Debug, Clone, Default)]
pub struct Scan {
    pub entries: Vec<Entry>,
    /// The directory held more than `MAX_ENTRIES_PER_DIR`; `entries` is a prefix, which
    /// the UI must say.
    pub truncated: bool,
}

/// [`scan_dir_progressive`], also naming sniffed files by `formats`' magic.
pub fn scan_dir_specs(dir: &Path, formats: &crate::formats::Registry) -> Scan {
    scan_dir_with(dir, formats, |_| {})
}

/// How often a listing still being read shows what it has so far.
const LISTING_PROGRESS_EVERY: std::time::Duration = std::time::Duration::from_millis(250);

/// List one directory with bounded work: one `read_dir` and a stat per entry, up to
/// [`MAX_ENTRIES_PER_DIR`]; errors give an empty listing. Nothing is classified:
/// subdirectories return [`EntryKind::Unknown`] and are looked into later from the
/// viewport, so a label is a fact about the row, not its position. `progress` gets
/// the rows read since the last call, every `LISTING_PROGRESS_EVERY`, so a slow share
/// shows rows as they arrive.
pub fn scan_dir_progressive(dir: &Path, progress: impl FnMut(&[Entry])) -> Scan {
    scan_dir_with(dir, &crate::formats::Registry::default(), progress)
}

fn scan_dir_with(
    dir: &Path,
    formats: &crate::formats::Registry,
    mut progress: impl FnMut(&[Entry]),
) -> Scan {
    let Ok(iter) = std::fs::read_dir(dir) else {
        return Scan::default();
    };
    let mut shown = std::time::Instant::now();

    let mut entries = Vec::new();
    // How many of `entries` `progress` has been handed.
    let mut sent = 0usize;
    let mut seen = 0usize;
    let mut truncated = false;
    // Extensionless files are sniffed (a few bytes) so part files list as data; never on
    // a share, where each open is a round trip.
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

        let mut spec = None;
        let kind = if meta.is_dir() {
            EntryKind::Unknown
        } else if meta.is_file() && is_data_file(&path) {
            EntryKind::File
        } else if spend_sniff(&mut sniffs_left, meta.is_file(), &path) {
            match sniff_listed(&path, formats) {
                Some(Sniffed::Format) => EntryKind::File,
                Some(Sniffed::Spec(found)) => {
                    spec = Some(found);
                    EntryKind::File
                }
                None => EntryKind::Other,
            }
        } else if meta.is_file() {
            EntryKind::Other
        } else {
            // Not a directory or regular file: a FIFO named `x.parquet` must never be offered.
            continue;
        };

        let mut entry = Entry::new(path, kind).with_fs_metadata(&meta);
        if let Some(spec) = spec {
            name_spec_file(&mut entry, &spec);
        }
        entries.push(entry);
        if shown.elapsed() >= LISTING_PROGRESS_EVERY {
            progress(&entries[sent..]);
            sent = entries.len();
            shown = std::time::Instant::now();
        }
    }

    sort_entries(&mut entries);
    Scan { entries, truncated }
}

/// Datasets first, then directories, each alphabetical (a listing is scanned by
/// name). Unexamined rows sort with directories, where they land if plain, so a kind
/// arriving later never moves a row under the cursor.
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

/// How far below a directory the footer walk goes (year/month/day/hour is four).
const MAX_WALK_DEPTH: u8 = 4;

/// Upper bound on footers read to size a multi-file or hive dataset; past it the count
/// is left blank rather than partial.
const MAX_FOOTERS_PER_DATASET: usize = 64;

/// Fill in row and column counts from Parquet footers: one file, or a bounded sum for
/// hive and multi-file datasets. Non-Parquet keeps `None`, a blank in the UI.
pub fn enrich(entry: &mut Entry) {
    enrich_as(entry, &crate::schema_union::ReadAs::default())
}

/// As [`enrich`], reading files as the following open will: where the header is
/// decides what the names are, so judging with other settings would misjudge (e.g.
/// `--no-header`).
pub fn enrich_as(entry: &mut Entry, as_read: &crate::schema_union::ReadAs) {
    enrich_with(entry, as_read, None)
}

/// As [`enrich_as`], using the shape an open kept in `remembered` while the files are
/// unchanged, instead of sampling footers.
pub fn enrich_with(
    entry: &mut Entry,
    as_read: &crate::schema_union::ReadAs,
    remembered: Option<&crate::cache::CacheManager>,
) {
    match entry.kind {
        EntryKind::File => {
            enrich_parquet(entry);
            enrich_tables(entry);
            enrich_arrow(entry);
        }
        EntryKind::Hive | EntryKind::MultiFile => enrich_dataset(entry, as_read, remembered),
        // Nothing to read for plain or unexamined directories, nor lake tables (summing their
        // footers counts tombstoned and rewritten rows).
        EntryKind::Directory | EntryKind::Unknown | EntryKind::Other => {}
        EntryKind::Delta | EntryKind::Iceberg | EntryKind::Hudi => {}
    }
}

/// Sum footers across a bounded set of Parquet files under `entry`.
fn enrich_dataset(
    entry: &mut Entry,
    as_read: &crate::schema_union::ReadAs,
    remembered: Option<&crate::cache::CacheManager>,
) {
    // A directory whose own files are a format this cannot count is not described by
    // Parquet beneath it (`6 json` reporting the subdirectories' columns). Parquet with
    // Parquet beneath keeps the walk: `Enter` reads the subtree, so counts must too; the
    // `holds` line explains the difference.

    // The partition layout comes from directory names, so it is known even when the rows
    // are too many to count.
    if entry.kind == EntryKind::Hive {
        entry.cost.partitions = partition_layout(&entry.path);
    }

    // Whether the footers below describe this directory (exact for its own format). Not
    // asked of a hive root: its own files are strays (a `schema.json`), and its data's
    // format can only be sampled.
    let reads_as_parquet = entry.kind == EntryKind::Hive
        || match entry.holds.one_format() {
            // No single format: unreachable today (mixed formats are `Directory`), so default to
            // the safe side.
            None => true,
            // An unreadable name is not Parquet: missing counts are undone by opening; another
            // format's counts are not.
            Some(name) => crate::FileFormat::from_name(name) == Some(crate::FileFormat::Parquet),
        };
    if !reads_as_parquet {
        entry.size = None;
        judge_by_names(entry, as_read);
        return;
    }

    // A directory's stat size is its inode, not its contents: dropped here, restored only
    // if the files are totalled.
    entry.size = None;

    let files = parquet_files_under(&entry.path);
    // Past the budget, footers an open kept are used if the listing still matches.
    if files.len() > MAX_FOOTERS_PER_DATASET
        && let Some((listed, footers)) = remembered
            .and_then(|cache| crate::dataset_files::remembered_footers(&entry.path, cache))
    {
        measure_from_footers(entry, &listed, &footers);
        return;
    }
    if files.is_empty() || files.len() > MAX_FOOTERS_PER_DATASET {
        // Whether these are one table needs only three spread footers, and matters most for
        // directories past the counting limit.
        let sampled = sample_footers(&files);
        let names: Vec<Vec<String>> = sampled.iter().map(column_names).collect();
        let tops: Vec<Vec<String>> = names
            .iter()
            .map(|n| crate::schema_union::top_level_columns(n))
            .collect();
        if entry.kind == EntryKind::MultiFile && one_table_from(&tops) == Some(false) {
            // One-table is asked of the subtree (what opening unions), but holdings count only
            // the direct files (the label's), since a downgraded row never opens as one table.
            let own_files = direct_children(&files, &entry.path);
            let own = sample_footers(&own_files);
            // The names are every sampled one under it: home's search index should find a
            // directory by any column looking inside reaches.
            entry.columns = union_of(&names);
            // A floor only if one of its own footers went unread.
            entry.cols_sampled = own.len() < own_files.len();
            // The columns a reader sees, from the schema rather than splitting leaf paths on dots
            // (`user.id` vs struct `user` with `id`).
            let top = union_of(&own.iter().map(top_level_names).collect::<Vec<_>>());
            downgrade_to_directory(entry, (!top.is_empty()).then_some(top.len()));
            return;
        }
        // Still worth knowing the shape, even when the row count is out of reach.
        if let Some(meta) = sampled.first() {
            // Three files, not the first: a growing dataset's newest columns are in its last
            // file. Still a sample; the count is already `?`.
            entry.columns = union_of(&names);
            let top = union_of(&sampled.iter().map(top_level_names).collect::<Vec<_>>());
            entry.cols = Some(top.len() + partition_columns_beyond(entry, &top));
            entry.cols_sampled = true;
            // From one file: codec and row-group sizing are the writer's, uniform in practice.
            physical_facts(meta, &mut entry.cost);
            entry.cost.uncompressed = None;
        }
        return;
    }

    let mut rows = 0usize;
    let mut bytes = 0u64;
    // Every column any file has, in first-seen order, so columns added over time are
    // found.
    let mut columns: Vec<String> = Vec::new();
    let mut seen_columns = std::collections::HashSet::new();
    // Top-level columns unioned the same way, kept beside the leaves (see
    // [`top_level_names`]).
    let mut top_level: Vec<String> = Vec::new();
    let mut seen_top_level = std::collections::HashSet::new();
    let mut per_file: Vec<Vec<String>> = Vec::with_capacity(files.len());
    // Width and size from the directory's own files only (a downgraded row is never one
    // table); column names stay the subtree's, for search.
    let mut own_bytes = 0u64;
    let mut own_top_level: Vec<String> = Vec::new();
    let mut own_seen_top = std::collections::HashSet::new();
    let mut cost = Cost::default();
    let mut uncompressed = 0u64;
    let mut row_groups = 0usize;
    for file in &files {
        let Some(meta) = crate::parquet_footer::read_parquet_metadata(file) else {
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
        // Top-level columns, not footer leaves: see
        // [`crate::schema_union::top_level_columns`].
        per_file.push(crate::schema_union::top_level_columns(&names));
        // Again for its own files (`2 parquet` must mean those two), from one stat feeding
        // both totals.
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
    // With the footers read, one-table is known. Separate tables make a place to look
    // inside (summed rows mean nothing). Only `multi` is reconsidered: hive files hold
    // one table by construction.
    if entry.kind == EntryKind::MultiFile && !crate::schema_union::is_nested(&per_file) {
        // Its own files' bytes, matching what the label and columns count.
        entry.size = Some(own_bytes);
        // Not one table, but column search should still find it: the count is its own
        // files', the names every one under it.
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

/// Partition keys the files lack, hoisted in as columns by the open, so the width
/// counts them (`12 × 4`, not `12 × 2`).
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

/// Which of `files` to read for a sample: the ends and the middle. Sorted names
/// cluster one table's files at the start, and the last is the newest. Three reads.
pub(crate) fn spread(files: usize) -> Vec<usize> {
    let mut picks = match files {
        0 => Vec::new(),
        n => vec![0, n / 2, n - 1],
    };
    picks.dedup();
    picks
}

/// Whether a few files' top-level columns are one table. `None` from fewer than two:
/// the directory keeps its name-based kind.
pub(crate) fn one_table_from(footers: &[Vec<String>]) -> Option<bool> {
    (footers.len() >= 2).then(|| crate::schema_union::is_nested(footers))
}

/// The footers of the [`spread`] of a directory too large to read every one of.
fn sample_footers(files: &[PathBuf]) -> Vec<crate::parquet_footer::Footer> {
    spread(files.len())
        .into_iter()
        .filter_map(|i| crate::parquet_footer::read_parquet_metadata(&files[i]))
        .collect()
}

/// Ask a footerless directory whether its files are one table by their header names:
/// [`one_table_from`]'s rule for CSV and NDJSON, so `Enter` does not promise a table
/// the read then refuses. Silence (too few files, costly schema, a parse failure)
/// keeps the name-based kind, safe because the read unions by name and widens types
/// (`crate::readers::polars::union_of_files`).
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
    // The directory's own files, which the label counts and the open reads; `MultiFile`
    // is flat by construction and has one format, so only `One` arrives.
    let DirectoryFormat::One(_, files) = directory_format(&entry.path) else {
        return;
    };
    // Read as the following open will: header placement decides the names.
    let sampled = crate::schema_union::sample_files(&files, format, as_read);
    if sampled.nests == Some(false) {
        // The sample's columns, for column search, as the Parquet path keeps when
        // downgrading; from the spread, so cost does not grow with the directory.
        let cols = (!sampled.columns.is_empty()).then_some(sampled.columns.len());
        // A floor only when files went unopened.
        entry.cols_sampled = sampled.read < files.len();
        entry.columns = sampled.columns;
        downgrade_to_directory(entry, cols);
    }
}

/// A directory of separate tables becomes a place to look inside: no row count (a sum
/// of unrelated tables), but the union's column count (`15 parquet · 72 columns`).
/// `cols` is passed in because leaf paths cannot be split back into top-level
/// columns (see [`top_level_names`]).
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
fn top_level_names(meta: &crate::parquet_footer::Footer) -> Vec<String> {
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

/// The Parquet files under `dir`, sorted, as deep as a dataset goes, stopping one past
/// the budget (which says there are too many to count). The open's own walk.
fn parquet_files_under(dir: &Path) -> Vec<PathBuf> {
    let mut files = crate::dataset_files::LocalFiles::new(dir)
        .first_files(MAX_WALK_DEPTH as usize + 1, MAX_FOOTERS_PER_DATASET);
    // The walk takes a directory entry's own type; a file this reads must be one.
    files.retain(|p| is_regular_file(p));
    files
}

/// Measure a dataset from footers an open kept: rows, width, size, row groups, and
/// whether its files are one table; nothing is read.
fn measure_from_footers(
    entry: &mut Entry,
    files: &[crate::dataset_files::DatasetFile],
    footers: &[Option<crate::schema_union::FileFooter>],
) {
    let per_file: Vec<Vec<String>> = footers
        .iter()
        .flatten()
        .map(|f| f.schema.iter_names().map(|n| n.to_string()).collect())
        .collect();
    let columns = union_of(&per_file);
    if entry.kind == EntryKind::MultiFile && !crate::schema_union::is_nested(&per_file) {
        // As a full footer read judges it: label, width and size count the directory's own
        // files.
        let own: Vec<usize> = files
            .iter()
            .enumerate()
            .filter(|(_, f)| Path::new(&f.key).parent() == Some(entry.path.as_path()))
            .map(|(i, _)| i)
            .collect();
        entry.size = Some(own.iter().map(|&i| files[i].size).sum());
        let own_columns = union_of(
            &own.iter()
                .filter_map(|&i| footers[i].as_ref())
                .map(|f| f.schema.iter_names().map(|n| n.to_string()).collect())
                .collect::<Vec<_>>(),
        );
        entry.columns = columns;
        entry.cols_sampled = false;
        downgrade_to_directory(
            entry,
            (!own_columns.is_empty()).then_some(own_columns.len()),
        );
        return;
    }
    let footers: Vec<&crate::schema_union::FileFooter> = footers.iter().flatten().collect();
    let uncompressed: u64 = footers
        .iter()
        .flat_map(|f| &f.column_bytes)
        .map(|(_, bytes)| *bytes as u64)
        .sum();
    let row_groups: usize = footers.iter().map(|f| f.row_group_rows.len()).sum();
    entry.rows = Some(footers.iter().map(|f| f.rows()).sum());
    entry.cols = Some(columns.len() + partition_columns_beyond(entry, &columns));
    entry.size = Some(files.iter().map(|f| f.size).sum());
    entry.columns = columns;
    entry.cols_sampled = false;
    entry.cost = Cost {
        uncompressed: (uncompressed > 0).then_some(uncompressed),
        row_groups: (row_groups > 0).then_some(row_groups),
        partitions: entry.cost.partitions.take(),
        ..Cost::default()
    };
}

/// Fill in a Parquet file's counts from its footer, reading no column data. Other
/// formats keep `None` (a CSV's rows need a scan).
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
    if let Some(meta) = crate::parquet_footer::read_parquet_metadata(&entry.path) {
        entry.rows = Some(meta.num_rows);
        entry.columns = column_names(&meta);
        // Top-level columns, as a directory's row reports them: `schema_descr` names leaves
        // (one struct of three fields would count four). See [`top_level_names`].
        entry.cols = Some(top_level_names(&meta).len());
        physical_facts(&meta, &mut entry.cost);
    }
}

/// A file of tables' tables (SQLite schema, NumPy archive directory): how many, whether
/// Enter opens one, and that one's columns. A file whose bytes contradict its name (a
/// `.db` that is not SQLite) is unopenable.
pub fn enrich_tables(entry: &mut Entry) {
    if entry.kind != EntryKind::File || entry.table.is_some() {
        return;
    }
    let named = data_format(&entry.path);
    if !is_regular_file(&entry.path) {
        return;
    }
    let Some(format) = crate::members::holder(&entry.path) else {
        if named.is_some_and(|f| f.holds_tables() && crate::readers::of(f).bytes_decide) {
            entry.kind = EntryKind::Other;
        }
        return;
    };
    let Ok(tables) = crate::members::tables(&entry.path, format) else {
        return;
    };
    let own: Vec<&crate::sqlite::Table> = tables.iter().filter(|t| !t.internal).collect();
    entry.cost.tables = Some(own.len());
    entry.cost.opens_one = format.opens_one_table();
    if let [one] = own.as_slice()
        && !one.columns.is_empty()
    {
        entry.columns = one.columns.iter().map(|(name, _)| name.clone()).collect();
        entry.cols = Some(entry.columns.len());
    }
}

/// A file of tables listed as rows (a database's tables and views, an archive's arrays),
/// each at its path inside the file; SQLite's own marked hidden.
pub fn database_rows(file: &Path) -> Vec<Entry> {
    let Some(format) = crate::members::holder(file) else {
        return Vec::new();
    };
    let Ok(mut tables) = crate::members::tables(file, format) else {
        return Vec::new();
    };
    // A database's tables by name; an archive's arrays in the order they were saved.
    if format
        .descriptor()
        .tables
        .as_ref()
        .is_some_and(|t| t.by_name)
    {
        tables.sort_by_cached_key(|t| t.name.to_lowercase());
    }
    let modified = std::fs::metadata(file).and_then(|m| m.modified()).ok();
    tables
        .into_iter()
        .map(|table| table_entry(file, format, table, modified))
        .collect()
}

/// A Hugging Face cache directory's splits as rows at their paths inside it
/// (`cache/test`), opened with `--table`. Empty otherwise, or for one split (its door
/// opens it).
pub fn split_rows(dir: &Path) -> Vec<Entry> {
    let splits = crate::hf_splits::cache_splits(dir);
    if splits.len() < 2 {
        return Vec::new();
    }
    splits
        .into_iter()
        .map(|split| split_entry(dir, split))
        .collect()
}

/// The row of a split named by its path inside its cache directory (a recent); `None`
/// if none.
pub fn split_row(path: &Path) -> Option<Entry> {
    let (dir, split) = crate::hf_splits::split_place(path)?;
    Some(split_entry(&dir, split))
}

fn split_entry(dir: &Path, split: String) -> Entry {
    let mut entry = Entry::new(dir.join(&split), EntryKind::File).with_name(split);
    entry.table = Some(TableOf {
        format: Some(crate::FileFormat::Arrow),
        kind: "split".to_string(),
        internal: false,
    });
    entry
}

/// A spec-read file's variants as rows at their paths inside it (`day.itch/add`),
/// opened with `--table`. Empty for other files.
pub fn variant_rows(file: &Path, formats: &crate::formats::Registry) -> Vec<Entry> {
    let Some((spec, tables)) = crate::members::variants(file, formats) else {
        return Vec::new();
    };
    let modified = std::fs::metadata(file).and_then(|m| m.modified()).ok();
    tables
        .into_iter()
        .map(|table| variant_entry(file, &spec, table, modified))
        .collect()
}

/// The row of a variant named by its path inside its file (a recent); `None` if none.
pub fn variant_row(path: &Path, formats: &crate::formats::Registry) -> Option<Entry> {
    let (file, name) = crate::members::split_variant(path, formats)?;
    let (spec, tables) = crate::members::variants(&file, formats)?;
    let table = tables.into_iter().find(|t| t.name == name)?;
    let modified = std::fs::metadata(&file).and_then(|m| m.modified()).ok();
    let mut entry = variant_entry(&file, &spec, table, modified);
    entry.path = path.to_path_buf();
    Some(entry)
}

fn variant_entry(
    file: &Path,
    spec: &str,
    table: crate::sqlite::Table,
    modified: Option<std::time::SystemTime>,
) -> Entry {
    let mut entry =
        Entry::new(crate::members::place(file, &table.name), EntryKind::File).with_name(table.name);
    entry.modified = modified;
    entry.columns = table.columns.into_iter().map(|(name, _)| name).collect();
    entry.cols = (!entry.columns.is_empty()).then_some(entry.columns.len());
    entry.format_spec = Some(spec.to_string());
    entry.table = Some(TableOf {
        format: None,
        kind: table.kind,
        internal: false,
    });
    entry
}

/// The row of a table named by its path inside its file (`app.db/users`, a recent);
/// `None` if none.
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
    let mut entry =
        Entry::new(crate::members::place(file, &table.name), EntryKind::File).with_name(table.name);
    entry.modified = modified;
    entry.columns = table.columns.into_iter().map(|(name, _)| name).collect();
    entry.cols = (!entry.columns.is_empty()).then_some(entry.columns.len());
    entry.table = Some(TableOf {
        format: Some(format),
        kind: table.kind,
        internal: table.internal,
    });
    entry
}

/// The first bytes of `path`, as many as fit in `buf`. A short read is the whole file.
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

/// Whether an Arrow file is an IPC stream (an IPC file starts `ARROW1`): eight bytes,
/// so the listing can say which will be converted.
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

/// Pull layout and compression from an already-read footer: what reading the file
/// will do, beyond its size.
pub fn physical_facts(meta: &crate::parquet_footer::Footer, cost: &mut Cost) {
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
    // Usually one codec; when not, say so rather than imply uniformity.
    cost.codec = match codecs.len() {
        0 => None,
        1 => Some(codecs.remove(0)),
        n => Some(format!("mixed ({n})")),
    };
}

/// Outermost directories examined to describe a hive partitioning: enough to name the
/// keys and show the shape; a decade of daily partitions is not worth counting on a
/// share.
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
            // Only the first partition is descended for nested keys: one is representative.
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
    // Bounded: each level is a directory read.
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
        // Below ten thousand show the exact count: 3,653 days is ten years; "4k" is not.
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

/// The preview of a table inside a file of tables, or of such a file (or NumPy array
/// file) as it opens. `None` for other entries, `Some(None)` when there is nothing to
/// show.
fn table_preview(entry: &Entry) -> Option<Option<SchemaPreview>> {
    let (file, format, name) = match &entry.table {
        Some(table) => match (crate::members::split(&entry.path), table.format) {
            (Some((file, _)), Some(format)) => (file, format, Some(entry.name.as_str())),
            _ => return Some(None),
        },
        None if is_regular_file(&entry.path) => {
            let format = crate::members::holder(&entry.path).or_else(|| {
                data_format(&entry.path)
                    .filter(|f| f.holds_tables() && crate::readers::of(*f).table_schema.is_some())
            })?;
            (entry.path.clone(), format, None)
        }
        None => return None,
    };
    Some(
        crate::readers::of(format)
            .table_schema
            .and_then(|schema| schema(&file, name)),
    )
}

/// The first Parquet file at or under `dir`, bounded in breadth and depth so thousands
/// of partitions cost what three do.
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
pub fn column_names(meta: &crate::parquet_footer::Footer) -> Vec<String> {
    meta.schema_descr
        .columns()
        .iter()
        .map(|c| c.path_in_schema.join("."))
        .collect()
}

/// Whether `path` is a regular file safe to open: a FIFO, device or socket named
/// `.parquet` would block or worse, so reads are gated on kind (stat never blocks
/// like an open can).
fn is_regular_file(path: &Path) -> bool {
    std::fs::metadata(path)
        .map(|m| m.file_type().is_file())
        .unwrap_or(false)
}

/// A dataset's column names and types without reading data; Parquet only, `None`
/// otherwise (the UI says so).
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
        // One data file's schema is not a lake table's (Iceberg field IDs and Delta column
        // mapping rename columns).
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
    // A hive table opens with partition keys hoisted first, so the pane lists them there,
    // typed from the path as the scan infers them.
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
mod classification_tests;

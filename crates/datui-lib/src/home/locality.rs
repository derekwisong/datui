//! Where a dataset physically lives, and what that implies about opening it (2 GB on
//! tmpfs and on hotel-wifi NFS are the same row and a thousandfold apart). Derived from
//! `/proc/self/mountinfo`, a kernel-generated local read that cannot block on a share
//! that stopped answering.

use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Filesystems whose reads cross a network: grouped by behavior (a read can stall while
/// the far end is unreachable, uninterruptibly on `hard` NFS), not protocol.
pub const NETWORK_FILESYSTEMS: &[&str] = &[
    "nfs",
    "nfs4",
    "cifs",
    "smb3",
    "smbfs",
    "afs",
    "9p",
    "ceph",
    "glusterfs",
    "fuse.sshfs",
    "fuse.rclone",
    "fuse.s3fs",
    "fuse.davfs",
    "davfs",
    "ftpfs",
    // An automount point that has not been triggered yet blocks on first access,
    // which is exactly what the marker is warning about. Once it triggers, the real
    // filesystem shadows it in the mount table and is judged on its own merits.
    "autofs",
];

/// Filesystems backed by RAM. Reads from these are free, which is worth saying when
/// every other row on screen is not.
const MEMORY_FILESYSTEMS: &[&str] = &["tmpfs", "ramfs", "devtmpfs"];

/// How a dataset is reached, in the terms that predict what opening it costs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Locality {
    /// A disk on this machine.
    Local,
    /// RAM. Reading is as fast as it gets.
    Memory,
    /// Reads cross a network and can stall.
    Network,
    /// An object store, reached by URL rather than by path.
    Object,
    /// Nothing in the mount table covered it.
    Unknown,
}

impl Locality {
    /// The locality a [`Source::fstype`] implies. Rows cache the filesystem name, and
    /// reclassifying it is free, unlike rereading `/proc` on the draw thread. Object-store
    /// schemes come first: they never appear in the mount table.
    pub fn of_fstype(fstype: &str) -> Locality {
        match fstype {
            "s3" | "s3a" | "gs" | "gcs" | "az" | "cloud" | "http" | "https" => Locality::Object,
            "" | "unknown" => Locality::Unknown,
            other => classify(other),
        }
    }
}

/// Where something lives and what the kernel calls it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Source {
    /// The filesystem type, or the URL scheme for an object store: `nfs4`, `ext4`,
    /// `fuse.sshfs`, `tmpfs`, `s3`, `gs`.
    pub fstype: String,
    pub locality: Locality,
}

impl Source {
    /// A short label for the interface. The filesystem's own name is the most
    /// informative thing available and costs one word: `nfs4` and `fuse.sshfs` fail
    /// in different ways, and neither behaves like `tmpfs`.
    pub fn label(&self) -> &str {
        &self.fstype
    }

    pub fn network(&self) -> bool {
        self.locality == Locality::Network
    }

    /// Classify a filesystem name on its own, for a source recorded earlier rather
    /// than resolved from a path just now.
    pub fn from_fstype(fstype: &str) -> Self {
        if let Some(scheme) = ["s3", "gs", "http", "https", "az", "hdfs"]
            .into_iter()
            .find(|s| *s == fstype)
        {
            return Self {
                fstype: scheme.to_string(),
                locality: Locality::Object,
            };
        }
        Self {
            locality: classify(fstype),
            fstype: fstype.to_string(),
        }
    }
}

/// How long a mount-table read is reused by [`Mounts::cached`]: mounts change on a human
/// timescale and answers are hints, so a few seconds is unnoticeable, and keystrokes
/// that ask per row do not read `/proc` each.
const MOUNTS_TTL: Duration = Duration::from_secs(5);

/// The last read of the mount table, and when it was taken.
static CACHED_MOUNTS: Mutex<Option<(Instant, Arc<Mounts>)>> = Mutex::new(None);

/// The mount table, parsed once; resolving paths against it is string work, so a whole
/// listing needs one read.
#[derive(Debug, Clone, Default)]
pub struct Mounts {
    /// (mount point, filesystem type), in the order the kernel listed them.
    entries: Vec<(String, String)>,
}

impl Mounts {
    /// Read the current mount table. An unreadable one yields an empty table, which
    /// reports everything as unknown rather than as wrong.
    pub fn current() -> Self {
        std::fs::read_to_string("/proc/self/mountinfo")
            .map(|s| Self::parse(&s))
            .unwrap_or_default()
    }

    /// The mount table as of at most `MOUNTS_TTL` ago, shared: reading `/proc` is most of
    /// an answer's cost, and five thousand rows asked it per row per frame.
    pub fn cached() -> Arc<Self> {
        let now = Instant::now();
        // Read under the lock rather than around it: two threads arriving on a cold
        // cache would otherwise both read `/proc`, and the loser's read would replace
        // a table just as good as its own.
        let mut slot = CACHED_MOUNTS
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some((read_at, mounts)) = slot.as_ref()
            && now.duration_since(*read_at) < MOUNTS_TTL
        {
            return Arc::clone(mounts);
        }
        let mounts = Arc::new(Self::current());
        *slot = Some((now, Arc::clone(&mounts)));
        mounts
    }

    pub fn parse(mountinfo: &str) -> Self {
        let mut entries = Vec::new();
        for line in mountinfo.lines() {
            // Fields before the separator end with the mount point at index 4; the
            // filesystem type is the first field after it.
            let Some((before, after)) = line.split_once(" - ") else {
                continue;
            };
            let Some(point) = before.split_whitespace().nth(4) else {
                continue;
            };
            let Some(fstype) = after.split_whitespace().next() else {
                continue;
            };
            entries.push((point.to_string(), fstype.to_string()));
        }
        Self { entries }
    }

    /// The filesystem covering `path`: the deepest mount, and at one point the last listed
    /// (a later mount shadows an earlier one, as NFS automounted over its autofs entry).
    pub fn fstype_for(&self, path: &Path) -> Option<&str> {
        self.covering(path).map(|(_, fstype)| fstype)
    }

    /// Where the filesystem covering `path` is mounted; empty when nothing covers it.
    pub fn mount_point_for(&self, path: &Path) -> std::path::PathBuf {
        self.covering(path)
            .map(|(point, _)| std::path::PathBuf::from(point))
            .unwrap_or_default()
    }

    /// The mount covering `path`, as (mount point, filesystem type).
    fn covering(&self, path: &Path) -> Option<(&str, &str)> {
        // Mount points are absolute: join a relative path to the working directory (string
        // work, unlike canonicalizing, which would touch the filesystem).
        let joined;
        // `has_root`, not `is_absolute`: on Windows `/mnt/nas/data` is "relative", and joining
        // the cwd would break it.
        let path = if path.has_root() {
            path
        } else {
            match std::env::current_dir() {
                Ok(cwd) => {
                    joined = cwd.join(path);
                    &joined
                }
                Err(_) => path,
            }
        };

        let mut best: Option<(&str, &str)> = None;
        for (point, fstype) in &self.entries {
            if !path.starts_with(point) {
                continue;
            }
            if best.is_none_or(|(at, _)| point.len() >= at.len()) {
                best = Some((point, fstype));
            }
        }
        best
    }

    /// How `path` is reached.
    pub fn describe(&self, path: &Path) -> Source {
        if let Some(scheme) = object_scheme(path) {
            return Source {
                fstype: scheme,
                locality: Locality::Object,
            };
        }
        match self.fstype_for(path) {
            Some(fstype) => Source {
                locality: classify(fstype),
                fstype: fstype.to_string(),
            },
            None => Source {
                fstype: "unknown".to_string(),
                locality: Locality::Unknown,
            },
        }
    }

    pub fn is_network(&self, path: &Path) -> bool {
        self.describe(path).network()
    }

    /// Whether a call on `path` can hang: a network filesystem, or any FUSE one, whose
    /// answers come from a process that may be waiting on a network (gcsfuse, juicefs,
    /// a stalled sshfs) or on nothing at all.
    pub fn could_block(&self, path: &Path) -> bool {
        let source = self.describe(path);
        source.network() || source.fstype.starts_with("fuse.")
    }
}

fn classify(fstype: &str) -> Locality {
    if NETWORK_FILESYSTEMS.contains(&fstype) {
        Locality::Network
    } else if MEMORY_FILESYSTEMS.contains(&fstype) {
        Locality::Memory
    } else {
        Locality::Local
    }
}

/// The URL scheme of an object-store path, if any: never in the mount table, never
/// walked or measured, only listed from history.
pub fn object_scheme(path: &Path) -> Option<String> {
    match crate::cloud::source::input_source(path) {
        crate::cloud::source::InputSource::Local(_) => None,
        crate::cloud::source::InputSource::S3(_) => Some("s3".to_string()),
        crate::cloud::source::InputSource::Gcs(_) => Some("gs".to_string()),
        crate::cloud::source::InputSource::Azure(_) => Some("az".to_string()),
        crate::cloud::source::InputSource::Http(_) => Some("http".to_string()),
    }
}

#[cfg(test)]
mod locality_of_fstype_tests {
    use super::*;

    #[test]
    fn object_store_schemes_are_not_local_disks() {
        // The case the round trip exists for. These never appear in the mount table, so
        // a classifier that only knew filesystems would call every one of them a disk
        // on this machine -- and the row would then claim a cloud object is local.
        for scheme in ["s3", "s3a", "gs", "gcs", "http", "https"] {
            assert_eq!(
                Locality::of_fstype(scheme),
                Locality::Object,
                "{scheme} should be an object store"
            );
        }
    }

    #[test]
    fn network_filesystems_are_network() {
        for fstype in ["nfs", "nfs4", "cifs", "smb3"] {
            assert_eq!(
                Locality::of_fstype(fstype),
                Locality::Network,
                "{fstype} should be network"
            );
        }
    }

    #[test]
    fn memory_filesystems_are_memory() {
        assert_eq!(Locality::of_fstype("tmpfs"), Locality::Memory);
    }

    #[test]
    fn ordinary_filesystems_are_local() {
        for fstype in ["ext4", "btrfs", "xfs", "apfs", "ntfs"] {
            assert_eq!(
                Locality::of_fstype(fstype),
                Locality::Local,
                "{fstype} should be local"
            );
        }
    }

    #[test]
    fn nothing_known_is_not_guessed_at() {
        assert_eq!(Locality::of_fstype(""), Locality::Unknown);
        assert_eq!(Locality::of_fstype("unknown"), Locality::Unknown);
    }

    /// What `describe` reports and what `of_fstype` makes of it have to agree, or a row
    /// is classified one way for the detail pane and another for its marker.
    #[test]
    fn it_agrees_with_describe() {
        let mounts = Mounts::parse("");
        for path in ["s3://bucket/key.parquet", "gs://bucket/key.parquet"] {
            let source = mounts.describe(std::path::Path::new(path));
            assert_eq!(source.locality, Locality::of_fstype(&source.fstype));
        }
    }
}

//! Where a dataset physically lives, and what that implies about opening it.
//!
//! The home screen's job is to let someone decide what to open. Size and row count
//! answer "what is this"; they say nothing about "what will happen when I press
//! Enter". A 2 GB file on tmpfs and a 2 GB file on a hotel-wifi NFS mount are the
//! same row and a thousandfold different experience.
//!
//! Everything here is derived from `/proc/self/mountinfo`, which is a local read of a
//! kernel-generated file: it cannot block on the filesystem it describes, which is
//! the whole reason it is safe to consult about a share that has stopped answering.

use std::path::Path;

/// Filesystems whose reads cross a network.
///
/// The list is about *behaviour*, not about protocol families: what these have in
/// common is that a read can stall for as long as the far end is unreachable, and on
/// a `hard` NFS mount that stall is uninterruptible.
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
#[derive(Debug, Clone, PartialEq, Eq)]
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

    /// Whether this is worth flagging on a row. Local disk is the unremarkable case
    /// and saying so on every row would be noise; everything else changes what
    /// pressing Enter means.
    pub fn notable(&self) -> bool {
        !matches!(self.locality, Locality::Local)
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

/// The mount table, parsed once.
///
/// Resolving a path against this is string work, so a whole listing can be described
/// from a single read rather than re-reading `/proc/self/mountinfo` per row.
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

    /// The filesystem covering `path`.
    ///
    /// The deepest mount wins, and among mounts at the same point the *last* one
    /// wins: mountinfo lists them in mount order, so a later entry shadows an earlier
    /// one. An NFS share automounted at a path appears after the autofs entry
    /// covering the same path, and it is the NFS entry that describes what a read
    /// will actually do.
    pub fn fstype_for(&self, path: &Path) -> Option<&str> {
        // Mount points are absolute, so a relative path matches nothing and would
        // report "unknown" for a file sitting on the disk under the caller's feet.
        // Joining the working directory is pure string work -- unlike canonicalising,
        // which touches the filesystem and is exactly what must not happen here.
        let joined;
        let path = if path.is_absolute() {
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

        let mut best: Option<(usize, &str)> = None;
        for (point, fstype) in &self.entries {
            if !path.starts_with(point) {
                continue;
            }
            let len = point.len();
            if best.is_none_or(|(n, _)| len >= n) {
                best = Some((len, fstype));
            }
        }
        best.map(|(_, f)| f)
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

/// The URL scheme of an object-store path, if it is one.
///
/// These never appear in the mount table and are never walked or measured: they are
/// listed from history so that what you opened before is still findable.
pub fn object_scheme(path: &Path) -> Option<String> {
    match crate::source::input_source(path) {
        crate::source::InputSource::Local(_) => None,
        crate::source::InputSource::S3(_) => Some("s3".to_string()),
        crate::source::InputSource::Gcs(_) => Some("gs".to_string()),
        crate::source::InputSource::Http(_) => Some("http".to_string()),
    }
}

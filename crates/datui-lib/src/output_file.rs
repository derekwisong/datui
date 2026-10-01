//! A file written whole or not at all.
//!
//! [`OutputFile`] writes to a private temporary file beside the destination and
//! moves it into place in [`OutputFile::commit`]. Until then the destination is
//! untouched: a failure while serializing, finishing an encoder or flushing
//! leaves the old file's bytes and permissions as they were, and a new export
//! leaves no partial file. Dropping an uncommitted `OutputFile`, including in a
//! panic, removes the temporary file.
//!
//! The file is synced before the rename, since some write errors (a network
//! filesystem's quota, a disk's I/O error) are reported only then. The rename
//! itself is not, so a power cut just after it can still lose the new file.
//!
//! Every file datui writes for the user goes through here: data exports, the
//! Data Quality report and chart images.

use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};

/// What the commit may do to a file already at the destination.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Overwrite {
    /// Nothing was there when the export was asked for. A file that appears
    /// before the commit is left alone and the export fails.
    Forbid,
    /// The user agreed to replace the file there.
    Replace,
}

/// Why [`OutputFile`] would not write a destination. Carried inside the
/// `io::Error`, whose kind says the same to code that matches on kinds; its
/// text is for the user and leaves the path to the caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refused {
    /// Something is at the path and replacing it was not agreed to: the
    /// overwrite was not asked about, because nothing was there then.
    Appeared,
    ReadOnly,
    Directory,
    NotAFile,
}

impl Refused {
    /// The refusal inside `error`, if it is one of these rather than the OS's.
    pub fn of(error: &io::Error) -> Option<Self> {
        error.get_ref()?.downcast_ref::<Self>().copied()
    }

    fn kind(self) -> io::ErrorKind {
        match self {
            Self::Appeared => io::ErrorKind::AlreadyExists,
            Self::ReadOnly => io::ErrorKind::PermissionDenied,
            Self::Directory => io::ErrorKind::IsADirectory,
            Self::NotAFile => io::ErrorKind::InvalidInput,
        }
    }
}

impl std::fmt::Display for Refused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Appeared => "a file appeared there during the export and was left as it was",
            Self::ReadOnly => "the file is read-only",
            Self::Directory => "it is a directory",
            Self::NotAFile => "it is not a regular file",
        })
    }
}

impl std::error::Error for Refused {}

impl From<Refused> for io::Error {
    fn from(refused: Refused) -> Self {
        io::Error::new(refused.kind(), refused)
    }
}

/// A file being written to a temporary sibling of its destination.
#[derive(Debug)]
pub struct OutputFile {
    temp: tempfile::NamedTempFile,
    /// Where the file lands: the destination, or what a symlink there points to,
    /// so a link is written through as it was before rather than replaced.
    target: PathBuf,
    overwrite: Overwrite,
}

impl OutputFile {
    /// Start writing `path`. Fails early, before any work, when the destination
    /// cannot be written under `overwrite`: something is there and replacing was
    /// not agreed to, or it is read-only, a directory or not a regular file.
    pub fn create(path: &Path, overwrite: Overwrite) -> io::Result<Self> {
        let target = resolve_link(path)?;
        replaceable(&target, overwrite)?;
        let dir = match target.parent() {
            Some(dir) if !dir.as_os_str().is_empty() => dir,
            _ => Path::new("."),
        };
        // Hidden, named for datui and the destination, and ending in the
        // destination's extension, which a writer that picks its encoding from
        // the path (plotters' PNG) needs. Created 0600 on Unix.
        let temp = tempfile::Builder::new()
            .prefix(".datui-")
            .suffix(&temp_suffix(&target))
            .tempfile_in(dir)?;
        Ok(Self {
            temp,
            target,
            overwrite,
        })
    }

    /// The temporary file, for a writer that takes a `Write`.
    pub fn file(&mut self) -> &mut File {
        self.temp.as_file_mut()
    }

    /// The temporary file's path, for a writer that opens a path itself. It must
    /// write to this path in place (truncating is fine), not replace it.
    pub fn path(&self) -> &Path {
        self.temp.path()
    }

    /// Sync the written file and move it into place. The caller has finished
    /// every encoder and flushed every buffer over [`Self::file`]; an error
    /// from those must stop it before it gets here.
    ///
    /// A replaced file's permission bits carry over on Unix; a new file gets the
    /// mode a plain create would (0666 less the umask). Ownership, ACLs, extended
    /// attributes and hard links of a replaced file do not carry over: the
    /// destination is a new file. Under [`Overwrite::Forbid`] a file that
    /// appeared meanwhile fails the commit with `AlreadyExists`, atomically where
    /// the filesystem has a no-replace rename or hard links (see
    /// [`Self::persist_new`]).
    pub fn commit(self) -> io::Result<()> {
        let Self {
            temp,
            target,
            overwrite,
        } = self;
        temp.as_file().sync_all()?;
        match overwrite {
            Overwrite::Replace => {
                let existing = replaceable(&target, Overwrite::Replace)?;
                set_final_permissions(&temp, existing.as_ref())?;
                temp.persist(&target).map_err(|e| e.error)?;
                Ok(())
            }
            Overwrite::Forbid => {
                set_final_permissions(&temp, None)?;
                Self::persist_new(temp, &target)
            }
        }
    }

    /// Persist without replacing anything. Linux and macOS rename with
    /// `RENAME_NOREPLACE`, falling back to a hard link; Windows moves without
    /// `MOVEFILE_REPLACE_EXISTING`. All of these are atomic. A filesystem with
    /// neither (FAT, some network mounts) gets a check and a plain rename: a
    /// file created in the instant between the two is replaced.
    fn persist_new(temp: tempfile::NamedTempFile, target: &Path) -> io::Result<()> {
        match temp.persist_noclobber(target) {
            Ok(_) => Ok(()),
            Err(e) if e.error.kind() == io::ErrorKind::AlreadyExists => {
                Err(Refused::Appeared.into())
            }
            Err(e) => {
                if fs::symlink_metadata(target).is_ok() {
                    return Err(Refused::Appeared.into());
                }
                e.file.persist(target).map(drop).map_err(|e| e.error)
            }
        }
    }
}

/// `path`, or the file a symlink at `path` points to, followed to the end. A
/// dangling link resolves to the missing file it names.
fn resolve_link(path: &Path) -> io::Result<PathBuf> {
    let mut path = path.to_path_buf();
    // A bound rather than cycle detection: the OS refuses loops this long too.
    for _ in 0..40 {
        match fs::symlink_metadata(&path) {
            Ok(meta) if meta.file_type().is_symlink() => {
                let link = fs::read_link(&path)?;
                path = match path.parent() {
                    Some(dir) => dir.join(link),
                    None => link,
                };
            }
            _ => return Ok(path),
        }
    }
    Err(io::Error::other(format!(
        "{} is a loop of symbolic links",
        path.display()
    )))
}

/// The permissions of the regular, writable file at `target`, or None when
/// nothing is there. Errors where `overwrite` forbids replacing what is there,
/// or where it could not be written to in place before: a file datui may not
/// write is refused rather than replaced by rename.
fn replaceable(target: &Path, overwrite: Overwrite) -> io::Result<Option<fs::Permissions>> {
    let meta = match fs::metadata(target) {
        Ok(meta) => meta,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    if overwrite == Overwrite::Forbid {
        return Err(Refused::Appeared.into());
    }
    if meta.is_dir() {
        return Err(Refused::Directory.into());
    }
    if !meta.is_file() {
        return Err(Refused::NotAFile.into());
    }
    if meta.permissions().readonly() {
        return Err(Refused::ReadOnly.into());
    }
    // A write bit in the mode is not leave to write: another user's file in a
    // directory of ours has one, and a rename would replace it where opening
    // it fails. Opening for write, without truncating, asks the OS.
    fs::OpenOptions::new().write(true).open(target)?;
    Ok(Some(meta.permissions()))
}

/// `.datui-XXXXXX-<name>`, or `.datui-XXXXXX.<ext>` for a name too long to
/// carry whole within the 255 bytes most filesystems allow.
fn temp_suffix(target: &Path) -> String {
    let name = target
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    if name.len() <= 200 {
        return format!("-{name}");
    }
    match target.extension() {
        Some(ext) if ext.len() <= 32 => format!(".{}", ext.to_string_lossy()),
        _ => String::new(),
    }
}

/// Give the temporary file its final mode before it becomes visible under the
/// destination's name: the replaced file's, or a fresh file's.
#[cfg(unix)]
fn set_final_permissions(
    temp: &tempfile::NamedTempFile,
    existing: Option<&fs::Permissions>,
) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let permissions = match existing {
        Some(permissions) => permissions.clone(),
        None => fs::Permissions::from_mode(fresh_mode(temp.path())?),
    };
    temp.as_file().set_permissions(permissions)
}

/// Windows has one permission bit, read-only, which [`replaceable`] refuses;
/// the replacement keeps the default attributes and the directory's ACLs.
#[cfg(not(unix))]
fn set_final_permissions(
    _temp: &tempfile::NamedTempFile,
    _existing: Option<&fs::Permissions>,
) -> io::Result<()> {
    Ok(())
}

/// The mode a plain `File::create` would give a file beside `temp`: 0666 less
/// the umask. Read from an empty probe rather than by setting the umask, which
/// is process-wide and would race other threads creating files.
#[cfg(unix)]
fn fresh_mode(temp: &Path) -> io::Result<u32> {
    use std::os::unix::fs::PermissionsExt;
    let dir = match temp.parent() {
        Some(dir) if !dir.as_os_str().is_empty() => dir,
        _ => Path::new("."),
    };
    let probe = tempfile::Builder::new()
        .prefix(".datui-mode-")
        .permissions(fs::Permissions::from_mode(0o666))
        .tempfile_in(dir)?;
    Ok(probe.as_file().metadata()?.permissions().mode() & 0o777)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    /// Names in `dir` other than `keep`: what an export left behind.
    fn leftovers(dir: &Path, keep: &[&str]) -> Vec<String> {
        fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|name| !keep.contains(&name.as_str()))
            .collect()
    }

    #[test]
    fn a_commit_writes_a_new_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.csv");
        let mut out = OutputFile::create(&path, Overwrite::Forbid).unwrap();
        out.file().write_all(b"a\n1\n").unwrap();
        assert!(!path.exists(), "nothing lands before the commit");
        out.commit().unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"a\n1\n");
        assert!(leftovers(dir.path(), &["out.csv"]).is_empty());
    }

    #[test]
    fn an_uncommitted_new_file_leaves_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.csv");
        let mut out = OutputFile::create(&path, Overwrite::Forbid).unwrap();
        out.file().write_all(b"partial").unwrap();
        drop(out);
        assert!(leftovers(dir.path(), &[]).is_empty());
    }

    #[test]
    fn an_uncommitted_replacement_keeps_the_old_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.csv");
        fs::write(&path, b"old").unwrap();
        let mut out = OutputFile::create(&path, Overwrite::Replace).unwrap();
        out.file().write_all(b"new and partial").unwrap();
        drop(out);
        assert_eq!(fs::read(&path).unwrap(), b"old");
        assert!(leftovers(dir.path(), &["out.csv"]).is_empty());
    }

    /// A writer that panics part way, as a worker thread can, unwinds through
    /// the drop: the old file stays and the temporary file goes.
    #[test]
    fn a_panic_while_writing_keeps_the_old_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.csv");
        fs::write(&path, b"old").unwrap();
        let result = std::panic::catch_unwind(|| {
            let mut out = OutputFile::create(&path, Overwrite::Replace).unwrap();
            out.file().write_all(b"new and partial").unwrap();
            panic!("the writer failed");
        });
        assert!(result.is_err());
        assert_eq!(fs::read(&path).unwrap(), b"old");
        assert!(leftovers(dir.path(), &["out.csv"]).is_empty());
    }

    #[test]
    fn an_approved_replacement_lands() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.csv");
        fs::write(&path, b"old").unwrap();
        let mut out = OutputFile::create(&path, Overwrite::Replace).unwrap();
        out.file().write_all(b"new").unwrap();
        out.commit().unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"new");
        assert!(leftovers(dir.path(), &["out.csv"]).is_empty());
    }

    #[test]
    fn an_existing_file_is_not_replaced_without_approval() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.csv");
        fs::write(&path, b"old").unwrap();
        let err = OutputFile::create(&path, Overwrite::Forbid).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
        assert_eq!(fs::read(&path).unwrap(), b"old");
        assert!(leftovers(dir.path(), &["out.csv"]).is_empty());
    }

    /// The race the confirmation cannot see: nothing was there when asked, and
    /// something is by the time the file is done.
    #[test]
    fn a_file_that_appears_before_the_commit_is_left_alone() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.csv");
        let mut out = OutputFile::create(&path, Overwrite::Forbid).unwrap();
        out.file().write_all(b"ours").unwrap();
        fs::write(&path, b"theirs").unwrap();
        let err = out.commit().unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
        assert_eq!(fs::read(&path).unwrap(), b"theirs");
        assert!(leftovers(dir.path(), &["out.csv"]).is_empty());
    }

    /// Approval covers what is there at the commit, including a file that was
    /// deleted meanwhile: the export is created.
    #[test]
    fn an_approved_replacement_of_a_vanished_file_creates_it() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.csv");
        fs::write(&path, b"old").unwrap();
        let mut out = OutputFile::create(&path, Overwrite::Replace).unwrap();
        out.file().write_all(b"new").unwrap();
        fs::remove_file(&path).unwrap();
        out.commit().unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"new");
    }

    #[test]
    fn a_read_only_file_is_refused_and_kept() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.csv");
        fs::write(&path, b"old").unwrap();
        let writable = fs::metadata(&path).unwrap().permissions();
        let mut read_only = writable.clone();
        read_only.set_readonly(true);
        fs::set_permissions(&path, read_only).unwrap();
        let err = OutputFile::create(&path, Overwrite::Replace).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::PermissionDenied);
        assert_eq!(fs::read(&path).unwrap(), b"old");
        assert!(fs::metadata(&path).unwrap().permissions().readonly());
        assert!(leftovers(dir.path(), &["out.csv"]).is_empty());
        // Writable again, or the temp dir cannot be removed on Windows.
        fs::set_permissions(&path, writable).unwrap();
    }

    #[test]
    fn a_directory_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.csv");
        fs::create_dir(&path).unwrap();
        let err = OutputFile::create(&path, Overwrite::Replace).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::IsADirectory);
        assert!(path.is_dir());
    }

    #[test]
    fn a_missing_directory_fails_before_any_work() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("missing").join("out.csv");
        let err = OutputFile::create(&path, Overwrite::Forbid).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
    }

    /// A writer that opens the path itself, such as plotters', writes in place,
    /// and the extension it reads its encoding from is the destination's.
    #[test]
    fn the_temporary_path_keeps_the_extension() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("chart.png");
        let out = OutputFile::create(&path, Overwrite::Forbid).unwrap();
        assert_eq!(out.path().extension().unwrap(), "png");
        assert_eq!(out.path().parent(), Some(dir.path()));
        fs::write(out.path(), b"png bytes").unwrap();
        out.commit().unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"png bytes");
    }

    #[test]
    fn a_long_name_keeps_its_extension() {
        let name = format!("{}.parquet", "x".repeat(240));
        assert_eq!(temp_suffix(Path::new(&name)), ".parquet");
        assert_eq!(temp_suffix(Path::new("out.csv")), "-out.csv");
    }

    #[cfg(unix)]
    mod unix {
        use super::*;
        use std::os::unix::fs::PermissionsExt;

        fn mode(path: &Path) -> u32 {
            fs::metadata(path).unwrap().permissions().mode() & 0o7777
        }

        #[test]
        fn a_replacement_keeps_the_old_mode() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("out.csv");
            fs::write(&path, b"old").unwrap();
            fs::set_permissions(&path, fs::Permissions::from_mode(0o640)).unwrap();
            let mut out = OutputFile::create(&path, Overwrite::Replace).unwrap();
            out.file().write_all(b"new").unwrap();
            out.commit().unwrap();
            assert_eq!(mode(&path), 0o640);
        }

        #[test]
        fn a_failed_replacement_keeps_the_old_mode() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("out.csv");
            fs::write(&path, b"old").unwrap();
            fs::set_permissions(&path, fs::Permissions::from_mode(0o604)).unwrap();
            let out = OutputFile::create(&path, Overwrite::Replace).unwrap();
            drop(out);
            assert_eq!(mode(&path), 0o604);
            assert_eq!(fs::read(&path).unwrap(), b"old");
        }

        /// Writable by its group only: not by us, its owner, though the mode
        /// is not read-only. Writing in place was refused, so replacing by
        /// rename is too.
        #[test]
        fn a_file_we_may_not_write_is_refused() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("out.csv");
            fs::write(&path, b"old").unwrap();
            fs::set_permissions(&path, fs::Permissions::from_mode(0o060)).unwrap();
            if fs::OpenOptions::new().write(true).open(&path).is_ok() {
                return; // root writes anything
            }
            let err = OutputFile::create(&path, Overwrite::Replace).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::PermissionDenied);
            assert_eq!(mode(&path), 0o060);
            assert!(leftovers(dir.path(), &["out.csv"]).is_empty());
        }

        /// Private while written; a plain create's mode once it lands.
        #[test]
        fn a_new_file_is_private_until_it_lands() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("out.csv");
            let out = OutputFile::create(&path, Overwrite::Forbid).unwrap();
            assert_eq!(mode(out.path()), 0o600);
            let plain = dir.path().join("plain");
            File::create(&plain).unwrap();
            out.commit().unwrap();
            assert_eq!(mode(&path), mode(&plain));
        }

        /// A link is written through, as `File::create` did, not replaced. The
        /// temporary file sits beside the file, not the link, so the rename
        /// stays on the file's filesystem.
        #[test]
        fn a_symlink_is_written_through() {
            let dir = tempfile::tempdir().unwrap();
            let data = dir.path().join("data");
            let links = dir.path().join("links");
            fs::create_dir(&data).unwrap();
            fs::create_dir(&links).unwrap();
            let real = data.join("real.csv");
            let link = links.join("link.csv");
            fs::write(&real, b"old").unwrap();
            std::os::unix::fs::symlink("../data/real.csv", &link).unwrap();
            let mut out = OutputFile::create(&link, Overwrite::Replace).unwrap();
            assert_eq!(
                out.path().parent().unwrap().canonicalize().unwrap(),
                data.canonicalize().unwrap()
            );
            out.file().write_all(b"new").unwrap();
            out.commit().unwrap();
            assert!(leftovers(&data, &["real.csv"]).is_empty());
            assert!(leftovers(&links, &["link.csv"]).is_empty());
            assert!(
                fs::symlink_metadata(&link)
                    .unwrap()
                    .file_type()
                    .is_symlink()
            );
            assert_eq!(fs::read(&real).unwrap(), b"new");
        }
    }
}

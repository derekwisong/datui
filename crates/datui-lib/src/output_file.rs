//! A file written whole or not at all. [`OutputFile`] writes a private temp file beside
//! the destination and moves it into place in [`OutputFile::commit`]; until then a
//! failure leaves the old file (bytes and permissions) untouched and a new export
//! leaves nothing; dropping uncommitted (even in a panic) removes the temp file. The file
//! is synced before the rename (some errors surface only then); the rename is not, so a
//! power cut right after can lose it. Every user-facing file goes through here: exports,
//! the Data Quality report, chart images.

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
        // the path needs. Created 0600 on Unix.
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

    /// Sync the file and move it into place; the caller has finished every encoder and
    /// flush over [`Self::file`]. A replaced file's permission bits carry over on Unix (a new
    /// one gets 0666 less umask); ownership, ACLs, xattrs and hard links do not. Under
    /// [`Overwrite::Forbid`] a file appearing meanwhile fails with `AlreadyExists`,
    /// atomically where supported (see `Self::persist_new`).
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

    /// Persist without replacing: `RENAME_NOREPLACE` on Linux and macOS (falling back to a
    /// hard link), no `MOVEFILE_REPLACE_EXISTING` on Windows, all atomic. Filesystems with
    /// neither (FAT, some network mounts) check then rename, racing a file created between.
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
mod tests;

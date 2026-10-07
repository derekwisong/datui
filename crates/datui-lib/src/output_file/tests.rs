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

/// A writer that opens the path itself writes in place,
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

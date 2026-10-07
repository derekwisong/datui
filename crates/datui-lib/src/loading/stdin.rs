//! Data piped in: `cmd | datui` and `datui -`.
//!
//! Standard input is read once, to a temporary file, as a phase of the open
//! (`crate::loading`); the scan of that file then stays lazy, as for any file. The
//! format is read off the first bytes, since a pipe has no extension to go by, unless
//! `--format` or `--compression` says. Keys come from the terminal meanwhile: Crossterm
//! reads `/dev/tty` on Unix when standard input is not one, and `CONIN$` on Windows.

use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicU64;

use crate::cloud::download::{Opened, StreamError, TempDownload};
use crate::loading::unfinished::Writer;
use crate::{CompressionFormat, FileFormat, OpenOptions};

/// The path that names standard input on the command line.
pub const PATH: &str = "-";

/// What data read from standard input is called on screen and in messages.
pub const NAME: &str = "stdin";

/// Whether `path` names standard input.
pub fn is_stdin(path: &Path) -> bool {
    path.as_os_str() == PATH
}

/// The name `path` goes by: `stdin` for standard input, itself otherwise.
pub fn named(path: &Path) -> PathBuf {
    if is_stdin(path) {
        PathBuf::from(NAME)
    } else {
        path.to_path_buf()
    }
}

/// Whether standard input carries data: a pipe or a file. A terminal there is the
/// user, and a device such as `/dev/null` or Windows' `NUL`, where a launcher points
/// it, holds nothing.
pub fn piped() -> bool {
    use std::io::IsTerminal;
    let stdin = std::io::stdin();
    carries_data(stdin.is_terminal(), is_device(&stdin))
}

/// What [`piped`] decides from what standard input is: neither a terminal nor a
/// device.
pub fn carries_data(terminal: bool, device: bool) -> bool {
    !terminal && !device
}

/// Whether standard input is a character device. One that cannot be asked is taken
/// for one, so nothing is read from it.
#[cfg(unix)]
fn is_device(stdin: &std::io::Stdin) -> bool {
    use std::os::fd::AsFd;
    use std::os::unix::fs::FileTypeExt;
    stdin
        .as_fd()
        .try_clone_to_owned()
        .and_then(|fd| std::fs::File::from(fd).metadata())
        .ok()
        .is_none_or(|meta| meta.file_type().is_char_device())
}

/// Whether standard input is anything but a file or a pipe: `NUL`, a console, which
/// `is_terminal` has already answered for, or no handle at all, as a process started
/// without one has; none of these is read.
#[cfg(windows)]
fn is_device(stdin: &std::io::Stdin) -> bool {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::{FILE_TYPE_DISK, FILE_TYPE_PIPE, GetFileType};
    // SAFETY: the handle is standard input's, open for the life of the process; a null
    // or invalid one makes `GetFileType` answer `FILE_TYPE_UNKNOWN`.
    let kind = unsafe { GetFileType(stdin.as_raw_handle()) };
    !matches!(kind, FILE_TYPE_DISK | FILE_TYPE_PIPE)
}

#[cfg(not(any(unix, windows)))]
fn is_device(_stdin: &std::io::Stdin) -> bool {
    false
}

/// `path` as a host other than the command line means it: `-` is a file of that name,
/// since only the command line reads standard input.
pub fn as_file(path: PathBuf) -> PathBuf {
    if is_stdin(&path) {
        Path::new(".").join(path)
    } else {
        path
    }
}

/// The paths to open: those named, or standard input when none are and something is
/// piped in.
pub fn paths_or_stdin(paths: Vec<PathBuf>, piped: bool) -> Vec<PathBuf> {
    if paths.is_empty() && piped {
        vec![PathBuf::from(PATH)]
    } else {
        paths
    }
}

/// Why standard input cannot be read as asked, before anything is read: nothing is
/// piped in, or it is named with other paths.
pub fn refuse(paths: &[PathBuf], piped: bool) -> Option<&'static str> {
    if !paths.iter().any(|path| is_stdin(path)) {
        return None;
    }
    if paths.len() > 1 {
        return Some("Standard input (-) is read on its own. Name it without other paths.");
    }
    (!piped)
        .then_some("Nothing is piped to standard input. Pipe data in, as in: cat data.csv | datui")
}

/// The format and compression the first bytes of a file say it is: compression by its
/// magic numbers, then whatever a format's signature says (`crate::formats::readers::sniff`),
/// and text no format claims as JSON, CSV or TSV on evidence, lines otherwise
/// ([`crate::formats::lines::guess`]). `head` is all there is when it is shorter than `HEAD`.
pub fn sniff(head: &[u8]) -> (FileFormat, Option<CompressionFormat>) {
    let (format, compression, _) = sniffed(head);
    (format, compression)
}

/// [`sniff`] as `options` ask: a guess of lines is CSV with `--delimiter`. Also says
/// whether the format was guessed rather than said by a signature.
pub(crate) fn sniff_for(
    head: &[u8],
    options: &OpenOptions,
) -> (FileFormat, Option<CompressionFormat>, bool) {
    let (format, compression, guessed) = sniffed(head);
    match guessed {
        true => (
            crate::formats::lines::as_asked(format, options),
            compression,
            true,
        ),
        false => (format, compression, false),
    }
}

/// [`sniff`], and whether the format was guessed rather than said by a signature.
fn sniffed(head: &[u8]) -> (FileFormat, Option<CompressionFormat>, bool) {
    const COMPRESSED: [(&[u8], CompressionFormat); 4] = [
        (b"\x1f\x8b", CompressionFormat::Gzip),
        (b"\x28\xb5\x2f\xfd", CompressionFormat::Zstd),
        (b"BZh", CompressionFormat::Bzip2),
        (b"\xfd7zXZ\x00", CompressionFormat::Xz),
    ];
    if let Some((_, compression)) = COMPRESSED.iter().find(|(magic, _)| head.starts_with(magic)) {
        // What is inside is looked at once it is on disk ([`inside`]).
        return (FileFormat::TEXT, Some(*compression), false);
    }
    if let Some(format) =
        crate::formats::readers::sniff(head, None, crate::formats::readers::Asked::Pipe, |_| true)
    {
        return (format, None, false);
    }
    let format = crate::formats::lines::guess(head, head.len() < HEAD).unwrap_or(FileFormat::TEXT);
    (format, None, true)
}

/// What compressed data piped in holds, by its first bytes once decompressed: a
/// format read through its compression by its signature, else delimited text on
/// evidence, else lines.
fn inside(file: &Path, compression: CompressionFormat) -> (FileFormat, bool) {
    let Some(head) = crate::formats::head_of(file, Some(compression), HEAD as u64) else {
        return (FileFormat::TEXT, false);
    };
    if let Some(format) = crate::formats::readers::sniff(
        &head,
        None,
        crate::formats::readers::Asked::Pipe,
        FileFormat::reads_into,
    ) {
        return (format, false);
    }
    let format = crate::formats::lines::guess(&head, head.len() < HEAD)
        .filter(|f| f.decompressed_once())
        .unwrap_or(FileFormat::TEXT);
    (format, true)
}

/// Bytes [`sniff`] looks at.
const HEAD: usize = crate::formats::readers::HEAD;

/// Where what comes in on standard input is copied: `--temp-dir`, else `spool` in the
/// cache directory, on disk, rather than the system's temp directory, which is memory
/// on many Linux machines. The system's when the cache directory cannot be made.
pub(crate) fn spool_dir(options: &OpenOptions) -> Option<PathBuf> {
    if let Some(dir) = &options.temp_dir {
        return Some(dir.clone());
    }
    let dir = crate::cache::CacheManager::new(crate::APP_NAME)
        .ok()?
        .cache_dir()
        .join("spool");
    std::fs::create_dir_all(&dir).ok()?;
    forget_old_spools(&dir);
    Some(dir)
}

/// How old a spool left behind is before it goes: one a session that crashed did not
/// remove. Long enough that no session still reading its own is near it.
const OLD_SPOOL: std::time::Duration = std::time::Duration::from_secs(7 * 24 * 60 * 60);

/// Remove the spools in `dir` older than [`OLD_SPOOL`]. Best effort: the cache is
/// not the system temp directory, which a reboot empties.
fn forget_old_spools(dir: &Path) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let old = entry
            .metadata()
            .and_then(|m| m.modified())
            .ok()
            .and_then(|at| at.elapsed().ok())
            .is_some_and(|age| age > OLD_SPOOL);
        // Not one a live datui still holds, however long it has been reading.
        #[cfg(unix)]
        let free = !crate::cloud::download::held_elsewhere(&entry.path());
        // Elsewhere a file held open cannot be removed, and the removal fails.
        #[cfg(not(unix))]
        let free = true;
        if old && free && entry.path().extension().is_some_and(|e| e == "tmp") {
            let _ = std::fs::remove_file(entry.path());
        }
    }
}

/// Read what `open` answers with into a temporary file in the spool directory
/// ([`spool_dir`]), counting the bytes into `read`, and say what it holds: `options`
/// with the format and compression the first bytes say, where the user did not.
/// Stops, removing the file, once `writer`'s open is stopped.
pub(crate) fn spool<R: Read>(
    open: impl FnOnce() -> Opened<R> + Send + 'static,
    options: OpenOptions,
    writer: &Writer,
    read: &AtomicU64,
) -> Result<(TempDownload, OpenOptions), String> {
    let file =
        crate::cloud::download::spool_to_temp(spool_dir(&options).as_deref(), open, writer, read)
            .map_err(|error| match error {
            StreamError::Open(e) | StreamError::Read(e) => {
                format!("Could not read standard input: {e}")
            }
            StreamError::Write(report) => {
                crate::error_display::user_message_from_report(&report, None)
            }
            StreamError::Short { .. } | StreamError::Cut => {
                "Reading standard input was stopped.".to_string()
            }
        })?;
    let options = described(file.path(), options)?;
    Ok((file, options))
}

/// What all of standard input, copied to `file`, holds: `options` with the format and
/// compression its first bytes say, where the user did not.
pub(crate) fn described(file: &Path, options: OpenOptions) -> Result<OpenOptions, String> {
    let mut head = Vec::with_capacity(HEAD);
    std::fs::File::open(file)
        .and_then(|f| f.take(HEAD as u64).read_to_end(&mut head))
        .map_err(|e| format!("Could not read standard input back: {e}"))?;
    if head.is_empty() {
        return Err("Nothing came in on standard input.".to_string());
    }
    let (mut format, compression, mut guessed) = sniffed(&head);
    if options.format.is_none()
        && let Some(compression) = options.compression.or(compression)
    {
        (format, guessed) = inside(file, compression);
    }
    if guessed {
        format = crate::formats::lines::as_asked(format, &options);
    }
    Ok(match (options.format, options.compression) {
        // A delimited format named and compression not: the bytes say whether it is
        // compressed, as a file's extension would.
        (Some(named), None) if named.separator().is_some() => OpenOptions {
            compression,
            ..options
        },
        // Named by the user: theirs, compression and all.
        (Some(_), _) => options,
        // Compression named, and what it holds read through it.
        (None, Some(_)) => OpenOptions {
            format: Some(format),
            format_guessed: guessed,
            ..options
        },
        (None, None) => OpenOptions {
            format: Some(format),
            compression,
            format_guessed: guessed,
            ..options
        },
    })
}

/// Whether standard input opened with `options` may be shown as it arrives: nothing
/// named rules it out (a format read once it is finished, or compression).
pub(crate) fn may_read_as_it_arrives(options: &OpenOptions) -> bool {
    options.compression.is_none()
        && options
            .format
            .is_none_or(|format| format.follows() || format == FileFormat::Arrow)
}

#[cfg(test)]
mod tests;

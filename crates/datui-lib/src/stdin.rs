//! Data piped in: `cmd | datui` and `datui -`.
//!
//! Standard input is read once, to a temporary file, as a phase of the open
//! ([`crate::loading`]); the scan of that file then stays lazy, as for any file. The
//! format is read off the first bytes, since a pipe has no extension to go by, unless
//! `--format` or `--compression` says. Keys come from the terminal meanwhile: Crossterm
//! reads `/dev/tty` on Unix when standard input is not one, and `CONIN$` on Windows.

use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicU64;

use crate::download::{Opened, StreamError, TempDownload};
use crate::unfinished::Writer;
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

/// The format and compression the first bytes of a file say it is. Columnar formats
/// and compression have magic numbers. Text starting with `[` is a JSON array; with
/// `{`, one object per line, unless the first line leaves the object open, as a
/// pretty-printed object does, which is read as JSON. A first line with tabs and no
/// commas is TSV; anything else is CSV.
pub fn sniff(head: &[u8]) -> (FileFormat, Option<CompressionFormat>) {
    const COMPRESSED: [(&[u8], CompressionFormat); 4] = [
        (b"\x1f\x8b", CompressionFormat::Gzip),
        (b"\x28\xb5\x2f\xfd", CompressionFormat::Zstd),
        (b"BZh", CompressionFormat::Bzip2),
        (b"\xfd7zXZ\x00", CompressionFormat::Xz),
    ];
    if let Some((_, compression)) = COMPRESSED.iter().find(|(magic, _)| head.starts_with(magic)) {
        // What is inside is not looked at: CSV, unless `--format` names TSV or PSV.
        return (FileFormat::Csv, Some(*compression));
    }
    if head.starts_with(b"PAR1") {
        return (FileFormat::Parquet, None);
    }
    if head.starts_with(b"ARROW1") {
        return (FileFormat::Arrow, None);
    }
    if head.starts_with(b"Obj\x01") {
        return (FileFormat::Avro, None);
    }
    if crate::sqlite::looks_like(head) {
        return (FileFormat::Sqlite, None);
    }
    if crate::model_files::looks_like_gguf(head) {
        return (FileFormat::Gguf, None);
    }
    if crate::model_files::looks_like_safetensors(head) {
        return (FileFormat::Safetensors, None);
    }
    if crate::audio::looks_like_audio(head) {
        return (FileFormat::Audio, None);
    }
    if crate::midi::looks_like_midi(head) {
        return (FileFormat::Midi, None);
    }
    // An Arrow IPC stream, which is what pyarrow and Polars write to a pipe.
    if crate::ipc_stream::is_stream_head(head) {
        return (FileFormat::Arrow, None);
    }
    // Before the text guesses below: a GPS log is text that says what it is.
    if let Some(gps) = crate::gps::sniff(head) {
        return (gps, None);
    }
    let text = head.strip_prefix(b"\xef\xbb\xbf").unwrap_or(head);
    let text = &text[text
        .iter()
        .position(|b| !b.is_ascii_whitespace())
        .unwrap_or(text.len())..];
    // Up to the first newline; a line longer than the head is taken as it stands.
    let line = text.split(|&b| b == b'\n').next().unwrap_or_default();
    match text.first() {
        Some(b'[') => (FileFormat::Json, None),
        Some(b'{') if !line.trim_ascii_end().ends_with(b"}") && line.len() < text.len() => {
            (FileFormat::Json, None)
        }
        Some(b'{') => (FileFormat::Jsonl, None),
        _ if line.contains(&b'\t') && !line.contains(&b',') => (FileFormat::Tsv, None),
        _ => (FileFormat::Csv, None),
    }
}

/// Bytes [`sniff`] looks at.
const HEAD: usize = 4096;

/// Read what `open` answers with into a temporary file in `--temp-dir` (the system's
/// otherwise), counting the bytes into `read`, and say what it holds: `options` with
/// the format and compression the first bytes say, where the user did not. Stops,
/// removing the file, once `writer`'s open is stopped.
pub(crate) fn spool<R: Read>(
    open: impl FnOnce() -> Opened<R> + Send + 'static,
    options: OpenOptions,
    writer: &Writer,
    read: &AtomicU64,
) -> Result<(TempDownload, OpenOptions), String> {
    let file = crate::download::spool_to_temp(options.temp_dir.as_deref(), open, writer, read)
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
    let mut head = Vec::with_capacity(HEAD);
    std::fs::File::open(file.path())
        .and_then(|f| f.take(HEAD as u64).read_to_end(&mut head))
        .map_err(|e| format!("Could not read standard input back: {e}"))?;
    if head.is_empty() {
        return Err("Nothing came in on standard input.".to_string());
    }
    let (format, compression) = sniff(&head);
    let options = match (options.format, options.compression) {
        // A delimited format named and compression not: the bytes say whether it is
        // compressed, as a file's extension would.
        (Some(named), None) if named.separator().is_some() => OpenOptions {
            compression,
            ..options
        },
        // Named by the user: theirs, compression and all.
        (Some(_), _) | (None, Some(_)) => OpenOptions {
            format: options.format.or(Some(FileFormat::Csv)),
            ..options
        },
        (None, None) => OpenOptions {
            format: Some(format),
            compression,
            ..options
        },
    };
    Ok((file, options))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    fn files_in(dir: &Path) -> usize {
        std::fs::read_dir(dir).unwrap().count()
    }

    fn options_in(dir: &Path) -> OpenOptions {
        OpenOptions {
            temp_dir: Some(dir.to_path_buf()),
            ..Default::default()
        }
    }

    /// Each format by its first bytes; whitespace and a byte-order mark before JSON
    /// are not the data's first character.
    #[test]
    fn the_first_bytes_say_the_format() {
        let cases: [(&[u8], FileFormat, Option<CompressionFormat>); 27] = [
            (b"PAR1\x15\x04", FileFormat::Parquet, None),
            (b"SQLite format 3\0\x10\x00", FileFormat::Sqlite, None),
            (b"GGUF\x03\x00\x00\x00", FileFormat::Gguf, None),
            (b"MThd\0\0\0\x06\0\x01", FileFormat::Midi, None),
            (
                b"\x02\x00\x00\x00\x00\x00\x00\x00{}",
                FileFormat::Safetensors,
                None,
            ),
            (b"$GPGGA,123519,4807.038,N", FileFormat::Nmea, None),
            // A CSV header with the shape of a sentence.
            (b"$USD,$EUR\n1,2\n", FileFormat::Csv, None),
            (
                b"<?xml version=\"1.0\"?>\n<gpx version=\"1.1\">",
                FileFormat::Gpx,
                None,
            ),
            (b"RIFF\x24\x00\x00\x00WAVEfmt ", FileFormat::Audio, None),
            (b"FORM\x00\x00\x00\x2eAIFFCOMM", FileFormat::Audio, None),
            (b"ARROW1\x00\x00", FileFormat::Arrow, None),
            // An Arrow IPC stream, cut short of its schema message.
            (b"\xff\xff\xff\xff\x10\x01\x00\x00", FileFormat::Arrow, None),
            (b"Obj\x01\x04", FileFormat::Avro, None),
            (
                b"\x1f\x8b\x08\x00",
                FileFormat::Csv,
                Some(CompressionFormat::Gzip),
            ),
            (
                b"\x28\xb5\x2f\xfd\x04",
                FileFormat::Csv,
                Some(CompressionFormat::Zstd),
            ),
            (b"BZh91AY", FileFormat::Csv, Some(CompressionFormat::Bzip2)),
            (
                b"\xfd7zXZ\x00\x00",
                FileFormat::Csv,
                Some(CompressionFormat::Xz),
            ),
            (b"  \n[{\"a\": 1}]", FileFormat::Json, None),
            (b"\xef\xbb\xbf{\"a\": 1}\n", FileFormat::Jsonl, None),
            (b"a,b\n1,2\n", FileFormat::Csv, None),
            (b"1\n2\n3\n", FileFormat::Csv, None),
            // A pretty-printed object, as `curl` gets from an API, is not one per line.
            (b"{\n  \"a\": 1\n}\n", FileFormat::Json, None),
            (b"{\"a\": 1}  \r\n{\"a\": 2}", FileFormat::Jsonl, None),
            (b"{\"a\": 1}", FileFormat::Jsonl, None),
            (b"id\tname\n1\tx\n", FileFormat::Tsv, None),
            (b"id\tname,first\n", FileFormat::Csv, None),
            (b"id,name\n1,a\tb\n", FileFormat::Csv, None),
        ];
        for (head, format, compression) in cases {
            assert_eq!(sniff(head), (format, compression), "{head:?}");
        }
    }

    /// A terminal is the user and a device (`/dev/null`, `NUL`) holds nothing; only
    /// a pipe or a file is read (#567).
    #[test]
    fn only_a_pipe_or_a_file_carries_data() {
        assert!(carries_data(false, false), "a pipe or a file");
        assert!(!carries_data(true, false), "a terminal");
        assert!(!carries_data(false, true), "/dev/null or NUL");
        assert!(!carries_data(true, true), "a console");
    }

    /// `-` is standard input wherever it is named, alone; no paths and a pipe on
    /// standard input select it, and a terminal there does not.
    #[test]
    fn stdin_is_chosen_by_a_dash_or_a_pipe() {
        assert_eq!(paths_or_stdin(Vec::new(), true), vec![PathBuf::from("-")]);
        assert!(paths_or_stdin(Vec::new(), false).is_empty());
        let listed = vec![PathBuf::from("a.csv")];
        assert_eq!(paths_or_stdin(listed.clone(), true), listed);

        assert_eq!(refuse(&listed, false), None);
        assert_eq!(refuse(&[PathBuf::from("-")], true), None);
        assert!(refuse(&[PathBuf::from("-")], false).is_some());
        assert!(refuse(&[PathBuf::from("-"), PathBuf::from("a.csv")], true).is_some());
        assert_eq!(named(Path::new("-")), PathBuf::from("stdin"));
        assert_eq!(named(Path::new("./-")), PathBuf::from("./-"));
        assert!(!is_stdin(&as_file(PathBuf::from("-"))));
        assert_eq!(as_file(PathBuf::from("a.csv")), PathBuf::from("a.csv"));
    }

    /// Spooled whole, counted, and its format read off the file; what the user named
    /// wins over the bytes.
    #[test]
    fn a_reader_is_spooled_whole_and_its_format_read() {
        let dir = tempfile::tempdir().unwrap();
        let read = AtomicU64::new(0);
        let body = b"[{\"a\": 1}]".to_vec();
        let (file, options) = spool(
            move || Ok((std::io::Cursor::new(body), None)),
            options_in(dir.path()),
            &Writer::default(),
            &read,
        )
        .unwrap();
        assert_eq!(std::fs::read(file.path()).unwrap(), b"[{\"a\": 1}]");
        assert_eq!(read.load(Ordering::Relaxed), 10);
        assert_eq!(options.format, Some(FileFormat::Json));
        assert_eq!(options.compression, None);

        let named = OpenOptions {
            compression: Some(CompressionFormat::Zstd),
            ..options_in(dir.path())
        };
        let (_, options) = spool(
            || Ok((std::io::Cursor::new(b"\x1f\x8b".to_vec()), None)),
            named,
            &Writer::default(),
            &AtomicU64::new(0),
        )
        .unwrap();
        assert_eq!(options.format, Some(FileFormat::Csv));
        assert_eq!(options.compression, Some(CompressionFormat::Zstd));

        // A delimited format named alone: its compression still comes from the bytes.
        let named = OpenOptions {
            format: Some(FileFormat::Tsv),
            ..options_in(dir.path())
        };
        let (_, options) = spool(
            || Ok((std::io::Cursor::new(b"\x1f\x8b".to_vec()), None)),
            named,
            &Writer::default(),
            &AtomicU64::new(0),
        )
        .unwrap();
        assert_eq!(options.format, Some(FileFormat::Tsv));
        assert_eq!(options.compression, Some(CompressionFormat::Gzip));

        let empty = spool(
            || Ok((std::io::empty(), None)),
            options_in(dir.path()),
            &Writer::default(),
            &AtomicU64::new(0),
        );
        assert!(empty.is_err(), "nothing piped in is said");
    }

    /// A producer that goes quiet mid-stream: what came is counted, and stopping the
    /// open ends the read and removes the partial file, though the pipe stays open.
    #[test]
    fn a_stop_mid_spool_removes_the_partial_file() {
        let dir = tempfile::tempdir().unwrap();
        let (reader, mut pipe) = std::io::pipe().unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let writer = crate::unfinished::Unfinished::default().writer(stop.clone());
        let read = Arc::new(AtomicU64::new(0));
        let worker = {
            let (writer, read) = (writer.clone(), read.clone());
            let options = options_in(dir.path());
            std::thread::spawn(move || spool(move || Ok((reader, None)), options, &writer, &read))
        };
        pipe.write_all(b"a,b\n1,2\n").unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
        while read.load(Ordering::Relaxed) < 8 {
            assert!(
                std::time::Instant::now() < deadline,
                "the bytes never landed"
            );
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        assert_eq!(files_in(dir.path()), 1, "the partial file is there");
        stop.store(true, Ordering::Relaxed);
        let answer = worker.join().unwrap();
        assert!(answer.is_err(), "a stopped read is not a dataset");
        assert_eq!(files_in(dir.path()), 0, "and its file is gone");
        drop(pipe);
    }
}

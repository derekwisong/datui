//! `--tee FILE`: standard input recorded to a file the user keeps, while it is viewed.
//!
//! The copy itself is a [`crate::follow::Spool`] writing to FILE rather than to a
//! temporary file; this module creates FILE and, when the stream ends, fills in what
//! a producer writing to a pipe could not: a WAV file's sizes. `--tee -` passes the
//! stream on to standard output instead ([`pass_stdout_on`]).

use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

/// Create `path` for the copy, refusing one that is there unless `force`. Never
/// claimed for removal: the file is the user's, and stays whatever happens.
pub(crate) fn create(path: &Path, force: bool) -> Result<File, String> {
    let mut options = std::fs::OpenOptions::new();
    options.read(true).write(true);
    if force {
        options.create(true).truncate(true);
    } else {
        options.create_new(true);
    }
    options.open(path).map_err(|e| match e.kind() {
        std::io::ErrorKind::AlreadyExists => refusal(path),
        _ => format!("Could not create {}: {e}", path.display()),
    })
}

/// What is said of a FILE that is there already.
pub fn refusal(path: &Path) -> String {
    format!(
        "{} is there already; --force overwrites it.",
        path.display()
    )
}

/// Why `--tee -` cannot pass the stream on when standard output is the terminal.
const STDOUT_IS_THE_SCREEN: &str = "--tee - passes standard input on to standard output, \
     which is the screen here: send it on to a pipe or a file, as in: \
     some_logger | datui -f --tee - | gzip > run1.csv.gz";

/// `--tee -`: keep standard output for the stream, and point the process's standard
/// output at the terminal, where the screen is drawn. Everything that draws or writes
/// to standard output, Ratatui and Crossterm among them, then reaches the terminal
/// with no change of its own. Returns the stream's standard output.
#[cfg(unix)]
pub(crate) fn pass_stdout_on() -> Result<File, String> {
    use std::io::IsTerminal;
    use std::os::fd::{AsRawFd, FromRawFd};
    if std::io::stdout().is_terminal() {
        return Err(STDOUT_IS_THE_SCREEN.to_string());
    }
    let tty = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open("/dev/tty")
        .map_err(|e| format!("--tee - draws on the terminal, /dev/tty, which did not open: {e}"))?;
    // Close-on-exec, so a program datui starts does not hold the pipe open after the
    // stream ends.
    // SAFETY: plain fd calls; their results are checked.
    let kept = unsafe { libc::fcntl(libc::STDOUT_FILENO, libc::F_DUPFD_CLOEXEC, 3) };
    if kept < 0 {
        return Err(format!(
            "Could not keep standard output: {}",
            std::io::Error::last_os_error()
        ));
    }
    // SAFETY: `kept` was just opened and is owned by nothing else.
    let kept = unsafe { File::from_raw_fd(kept) };
    // SAFETY: both fds are open; dup2 replaces fd 1 atomically.
    if unsafe { libc::dup2(tty.as_raw_fd(), libc::STDOUT_FILENO) } < 0 {
        return Err(format!(
            "Could not draw on the terminal: {}",
            std::io::Error::last_os_error()
        ));
    }
    Ok(kept)
}

/// As on Unix, with the console's screen buffer, `CONOUT$`, as standard output. Rust's
/// standard output asks Windows for the handle on every write, and Crossterm sizes and
/// sets up the console through `CONOUT$` and `CONIN$` itself.
#[cfg(windows)]
pub(crate) fn pass_stdout_on() -> Result<File, String> {
    use std::io::IsTerminal;
    use std::os::windows::io::{FromRawHandle, IntoRawHandle};
    use windows_sys::Win32::System::Console::{GetStdHandle, STD_OUTPUT_HANDLE, SetStdHandle};
    if std::io::stdout().is_terminal() {
        return Err(STDOUT_IS_THE_SCREEN.to_string());
    }
    let console = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open("CONOUT$")
        .map_err(|e| format!("--tee - draws on the console, which did not open: {e}"))?;
    // SAFETY: GetStdHandle takes no pointers.
    let kept = unsafe { GetStdHandle(STD_OUTPUT_HANDLE) };
    if kept.is_null() || kept == windows_sys::Win32::Foundation::INVALID_HANDLE_VALUE {
        return Err(
            "--tee - passes the stream on to standard output, and there is none.".to_string(),
        );
    }
    // The console handle stays open for the life of the process, as standard output.
    let console = console.into_raw_handle();
    // SAFETY: a live handle, owned from here by the process's standard output.
    if unsafe { SetStdHandle(STD_OUTPUT_HANDLE, console) } == 0 {
        return Err(format!(
            "Could not draw on the console: {}",
            std::io::Error::last_os_error()
        ));
    }
    // SAFETY: the old standard output handle is no longer standard output; this is now
    // its one owner.
    Ok(unsafe { File::from_raw_handle(kept) })
}

#[cfg(not(any(unix, windows)))]
pub(crate) fn pass_stdout_on() -> Result<File, String> {
    Err("--tee - is not supported on this platform.".to_string())
}

/// What [`fix_wav_sizes`] did.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Fixed {
    /// Not a WAV file, or its sizes were right already.
    Nothing,
    /// The RIFF and `data` sizes now say how long the file is.
    Riff,
    /// Over 4 GB: the header is RF64's, its sizes in the `ds64` chunk that took the
    /// place of the `JUNK` the producer reserved.
    Rf64,
    /// Over 4 GB with no room reserved for RF64's sizes: they are left as written.
    TooLong,
}

/// Fill in the sizes of the WAV file `file` holds, which a producer writing to a pipe
/// could not seek back to: the RIFF size and the `data` chunk's, 0 or 0xFFFFFFFF while
/// it streamed. Over 4 GB the header becomes RF64's when the producer reserved room for
/// it with a `JUNK` chunk first, as Broadcast WAV writers do.
pub(crate) fn fix_wav_sizes(file: &mut File) -> std::io::Result<Fixed> {
    let len = file.metadata()?.len();
    let mut head = [0u8; 12];
    file.seek(SeekFrom::Start(0))?;
    if len < 12 || file.read_exact(&mut head).is_err() {
        return Ok(Fixed::Nothing);
    }
    let rf64 = &head[0..4] == b"RF64";
    if !(&head[0..4] == b"RIFF" || rf64) || &head[8..12] != b"WAVE" {
        return Ok(Fixed::Nothing);
    }
    // Walk the chunks to `data`, the last a streaming producer writes.
    let mut at = 12u64;
    let mut junk = None;
    let mut ds64 = None;
    let data = loop {
        if at + 8 > len {
            return Ok(Fixed::Nothing);
        }
        let mut chunk = [0u8; 8];
        file.seek(SeekFrom::Start(at))?;
        file.read_exact(&mut chunk)?;
        let size = u32::from_le_bytes([chunk[4], chunk[5], chunk[6], chunk[7]]) as u64;
        match &chunk[0..4] {
            b"data" => break at,
            b"JUNK" if at == 12 => junk = Some(size),
            b"ds64" => ds64 = Some(at),
            _ => {}
        }
        // Chunks are padded to an even length.
        at += 8 + size + (size & 1);
    };
    let data_size = len - (data + 8);
    let riff_size = len - 8;
    if let Some(ds64) = ds64.filter(|_| rf64) {
        // RF64 already: its sizes live in `ds64`.
        file.seek(SeekFrom::Start(ds64 + 8))?;
        file.write_all(&riff_size.to_le_bytes())?;
        file.write_all(&data_size.to_le_bytes())?;
        return Ok(Fixed::Rf64);
    }
    if riff_size <= u32::MAX as u64 {
        if read_u32(file, 4)? as u64 == riff_size && read_u32(file, data + 4)? as u64 == data_size {
            return Ok(Fixed::Nothing);
        }
        write_u32(file, 4, riff_size as u32)?;
        write_u32(file, data + 4, data_size as u32)?;
        return Ok(Fixed::Riff);
    }
    // A ds64 chunk is 28 bytes: the RIFF size, the data size, the sample count and an
    // empty table. It takes the reserved JUNK's place, size and all.
    match junk {
        Some(size) if size >= 28 => {
            file.seek(SeekFrom::Start(0))?;
            file.write_all(b"RF64")?;
            file.write_all(&u32::MAX.to_le_bytes())?;
            file.seek(SeekFrom::Start(12))?;
            file.write_all(b"ds64")?;
            file.write_all(&(size as u32).to_le_bytes())?;
            file.write_all(&riff_size.to_le_bytes())?;
            file.write_all(&data_size.to_le_bytes())?;
            // The sample count is the fact chunk's business, which PCM has none of.
            file.write_all(&0u64.to_le_bytes())?;
            file.write_all(&0u32.to_le_bytes())?;
            write_u32(file, data + 4, u32::MAX)?;
            Ok(Fixed::Rf64)
        }
        _ => Ok(Fixed::TooLong),
    }
}

fn read_u32(file: &mut File, at: u64) -> std::io::Result<u32> {
    let mut b = [0u8; 4];
    file.seek(SeekFrom::Start(at))?;
    file.read_exact(&mut b)?;
    Ok(u32::from_le_bytes(b))
}

fn write_u32(file: &mut File, at: u64, value: u32) -> std::io::Result<()> {
    file.seek(SeekFrom::Start(at))?;
    file.write_all(&value.to_le_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A 16-bit stereo PCM WAV header as a producer writing to a pipe writes it: the
    /// sizes 0xFFFFFFFF, and `junk` bytes reserved after `WAVE` when given.
    pub(crate) fn streamed_wav(frames: u32, junk: Option<u32>) -> Vec<u8> {
        let mut out = b"RIFF".to_vec();
        out.extend(u32::MAX.to_le_bytes());
        out.extend(b"WAVE");
        if let Some(size) = junk {
            out.extend(b"JUNK");
            out.extend(size.to_le_bytes());
            out.extend(vec![0u8; size as usize]);
        }
        out.extend(b"fmt ");
        out.extend(16u32.to_le_bytes());
        out.extend(1u16.to_le_bytes()); // PCM
        out.extend(2u16.to_le_bytes()); // channels
        out.extend(48_000u32.to_le_bytes());
        out.extend((48_000u32 * 4).to_le_bytes());
        out.extend(4u16.to_le_bytes());
        out.extend(16u16.to_le_bytes());
        out.extend(b"data");
        out.extend(u32::MAX.to_le_bytes());
        for i in 0..frames {
            out.extend((i as i16).to_le_bytes());
            out.extend((-(i as i16)).to_le_bytes());
        }
        out
    }

    fn file_of(bytes: &[u8]) -> (tempfile::TempDir, File) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("take.wav");
        std::fs::write(&path, bytes).unwrap();
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        (dir, file)
    }

    #[test]
    fn a_streamed_wav_gets_its_sizes() {
        let bytes = streamed_wav(100, None);
        let (_dir, mut file) = file_of(&bytes);
        assert_eq!(fix_wav_sizes(&mut file).unwrap(), Fixed::Riff);
        assert_eq!(read_u32(&mut file, 4).unwrap() as usize, bytes.len() - 8);
        assert_eq!(
            read_u32(&mut file, 40).unwrap(),
            400,
            "100 frames of 4 bytes"
        );
        let header =
            crate::audio::read_header(&std::fs::read(_dir.path().join("take.wav")).unwrap());
        assert!(header.is_ok(), "{header:?}");
        assert_eq!(fix_wav_sizes(&mut file).unwrap(), Fixed::Nothing, "once");
    }

    #[test]
    fn other_files_are_left_alone() {
        let (_dir, mut file) = file_of(b"a,b\n1,2\n");
        assert_eq!(fix_wav_sizes(&mut file).unwrap(), Fixed::Nothing);
        assert_eq!(
            std::fs::read(_dir.path().join("take.wav")).unwrap(),
            b"a,b\n1,2\n"
        );
    }

    /// Over 4 GB, the JUNK chunk a producer reserved becomes RF64's `ds64`. The file
    /// is sparse: only its header is ever read.
    #[test]
    fn over_four_gigabytes_the_header_becomes_rf64() {
        let bytes = streamed_wav(0, Some(28));
        let (dir, mut file) = file_of(&bytes);
        let len = 5u64 << 30;
        file.set_len(len).unwrap();
        assert_eq!(fix_wav_sizes(&mut file).unwrap(), Fixed::Rf64);
        let head = {
            let mut head = vec![0u8; 80];
            file.seek(SeekFrom::Start(0)).unwrap();
            file.read_exact(&mut head).unwrap();
            head
        };
        assert_eq!(&head[0..4], b"RF64");
        assert_eq!(&head[12..16], b"ds64");
        let u64_at = |at: usize| u64::from_le_bytes(head[at..at + 8].try_into().unwrap());
        assert_eq!(u64_at(20), len - 8, "the RIFF size");
        let data_at = 12 + 8 + 28 + 8 + 16;
        assert_eq!(&head[data_at..data_at + 4], b"data");
        assert_eq!(u64_at(28), len - (data_at as u64 + 8), "the data size");
        drop(dir);

        // No room reserved: the sizes stay as they were written.
        let (_dir, mut file) = file_of(&streamed_wav(0, None));
        file.set_len(len).unwrap();
        assert_eq!(fix_wav_sizes(&mut file).unwrap(), Fixed::TooLong);
    }

    /// FILE is never replaced without `--force`.
    #[test]
    fn an_existing_file_is_refused_without_force() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("run1.csv");
        std::fs::write(&path, "keep me").unwrap();
        let refused = create(&path, false).unwrap_err();
        assert!(refused.contains("--force"), "{refused}");
        assert_eq!(std::fs::read(&path).unwrap(), b"keep me");
        create(&path, true).unwrap();
        assert_eq!(
            std::fs::read(&path).unwrap(),
            b"",
            "overwritten with --force"
        );
    }
}

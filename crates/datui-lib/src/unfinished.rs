//! The temporary files an open's workers write, so that quitting removes them.
//!
//! A download or a decompression writes a temporary file that removes itself when its
//! last holder drops it. Quitting raises the open's stop flag, but the process can end
//! before a worker sees it, and then nothing removes the file it was writing. So each
//! writer [claims](Writer::claim) the file it creates, and the claim lives inside the
//! file's holder: it goes when the file does, after it. Quitting drops the app first,
//! which removes every file the event thread holds, and then [`ExitSweep`] waits a
//! short grace for the workers, which stop and remove their own; whatever is still
//! claimed after that is removed by the sweep. A file created after the sweep is
//! refused its claim and removed by its writer.
//!
//! A process killed outright (SIGKILL, or SIGTERM and SIGHUP, which datui does not
//! catch) runs none of this, and a partial file stays in the temp directory.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

/// How long the sweep at exit waits for writers to stop before removing their files.
const SWEEP_GRACE: Duration = Duration::from_secs(1);

/// The files claimed by the writers of one loader's opens.
#[derive(Clone, Debug, Default)]
pub(crate) struct Unfinished(Arc<(Mutex<Files>, Condvar)>);

#[derive(Debug, Default)]
struct Files {
    claimed: Vec<PathBuf>,
    swept: bool,
}

/// What one open hands its writers: its stop flag, and where to claim their files.
#[derive(Clone, Debug, Default)]
pub(crate) struct Writer {
    stop: Arc<AtomicBool>,
    files: Unfinished,
}

/// A hold on a file a writer created, kept by whatever holds the file and dropped
/// after it.
#[derive(Debug)]
#[must_use]
pub(crate) struct Claim {
    files: Unfinished,
    path: PathBuf,
}

impl Unfinished {
    /// The writer for an open whose stop flag is `stop`.
    pub(crate) fn writer(&self, stop: Arc<AtomicBool>) -> Writer {
        Writer {
            stop,
            files: self.clone(),
        }
    }

    /// Whether a file is still claimed.
    #[cfg(test)]
    pub(crate) fn writing(&self) -> bool {
        !self.lock().claimed.is_empty()
    }

    /// By `deadline`, remove every file still claimed. Their writers, stopped already,
    /// remove their own as they notice; one still busy at the deadline has its file
    /// removed under it, and fails to remove it again, harmlessly. A file claimed after
    /// this is refused.
    pub(crate) fn sweep(&self, deadline: Instant) {
        let (_, released) = &*self.0;
        let mut files = self.lock();
        while !files.claimed.is_empty() {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                break;
            }
            files = released
                .wait_timeout(files, left)
                .unwrap_or_else(|e| e.into_inner())
                .0;
        }
        files.swept = true;
        for path in files.claimed.drain(..) {
            let _ = std::fs::remove_file(path);
        }
    }

    fn lock(&self) -> MutexGuard<'_, Files> {
        self.0.0.lock().unwrap_or_else(|e| e.into_inner())
    }
}

impl Writer {
    /// Whether the open was stopped: a writer gives up at its next chunk.
    pub(crate) fn stopped(&self) -> bool {
        self.stop.load(Ordering::Relaxed)
    }

    /// Claim `path`, just created. `None` once the open is stopped or the files are
    /// swept: the caller removes the file and gives up.
    pub(crate) fn claim(&self, path: &Path) -> Option<Claim> {
        let mut files = self.files.lock();
        if files.swept || self.stopped() {
            return None;
        }
        files.claimed.push(path.to_path_buf());
        Some(Claim {
            files: self.files.clone(),
            path: path.to_path_buf(),
        })
    }
}

/// Dropped after the app, removes what the app's opens were still writing. See the
/// [module](self).
#[must_use]
pub struct ExitSweep(pub(crate) Unfinished);

impl Drop for ExitSweep {
    fn drop(&mut self) {
        self.0.sweep(Instant::now() + SWEEP_GRACE);
    }
}

impl Drop for Claim {
    fn drop(&mut self) {
        let mut files = self.files.lock();
        if let Some(at) = files.claimed.iter().position(|p| *p == self.path) {
            files.claimed.swap_remove(at);
        }
        drop(files);
        self.files.0.1.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file_in(dir: &Path) -> tempfile::NamedTempFile {
        tempfile::NamedTempFile::new_in(dir).unwrap()
    }

    /// A writer that stops in time cleans up after itself, and the sweep waits for it
    /// rather than for the whole grace.
    #[test]
    fn a_sweep_waits_for_a_writer_that_stops() {
        let dir = tempfile::tempdir().unwrap();
        let unfinished = Unfinished::default();
        let stop = Arc::new(AtomicBool::new(false));
        let writer = unfinished.writer(stop.clone());
        let (claimed, wait) = std::sync::mpsc::channel();
        let worker = {
            let dir = dir.path().to_path_buf();
            std::thread::spawn(move || {
                let file = file_in(&dir);
                let claim = writer.claim(file.path()).expect("not stopped yet");
                claimed.send(()).unwrap();
                while !writer.stopped() {
                    std::thread::sleep(Duration::from_millis(5));
                }
                // As a writer does: the file goes, then the claim.
                drop(file);
                drop(claim);
            })
        };
        wait.recv().unwrap();
        stop.store(true, Ordering::Relaxed);
        let began = Instant::now();
        unfinished.sweep(began + Duration::from_secs(30));
        assert!(
            began.elapsed() < Duration::from_secs(10),
            "not the whole grace"
        );
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
        assert!(!unfinished.writing());
        worker.join().unwrap();
    }

    /// A file still claimed at the deadline is removed by the sweep.
    #[test]
    fn a_sweep_removes_what_a_writer_still_holds() {
        let dir = tempfile::tempdir().unwrap();
        let unfinished = Unfinished::default();
        let writer = unfinished.writer(Arc::default());
        let (_, path) = file_in(dir.path()).keep().unwrap();
        let claim = writer.claim(&path).expect("not stopped yet");
        unfinished.sweep(Instant::now() + Duration::from_millis(20));
        assert!(!path.exists());
        assert!(!unfinished.writing());
        drop(claim);
    }

    /// A file created once the open is stopped, or after the sweep, is refused.
    #[test]
    fn a_stopped_or_swept_open_refuses_new_files() {
        let unfinished = Unfinished::default();
        let stop = Arc::new(AtomicBool::new(true));
        assert!(unfinished.writer(stop).claim(Path::new("late")).is_none());

        let writer = unfinished.writer(Arc::default());
        unfinished.sweep(Instant::now());
        assert!(writer.claim(Path::new("later")).is_none());
    }
}

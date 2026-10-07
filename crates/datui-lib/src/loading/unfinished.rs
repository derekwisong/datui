//! The temporary files an open's workers write, so quitting removes them. Such a file
//! removes itself when its last holder drops it, but the process can end before a
//! stopped worker gets there. So each writer [creates](Writer::create) its file through
//! the open, which claims it for the file's lifetime. On quit the app drops first, then
//! [`ExitSweep`] gives workers a short grace and removes whatever is still claimed
//! (waiting for files mid-creation; later ones are refused). On Windows a file mapped by
//! a frame cannot be removed, so its claim keeps the path for later claims and the exit
//! sweep to retry. SIGKILL skips all this.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

/// How long the sweep at exit waits for writers to stop before removing their files.
const SWEEP_GRACE: Duration = Duration::from_secs(1);

/// How often the sweep tries a file that would not go again, within its grace.
const RETRY_EVERY: Duration = Duration::from_millis(50);

/// The files claimed by the writers of one loader's opens.
#[derive(Clone, Debug, Default)]
pub(crate) struct Unfinished(Arc<(Mutex<Files>, Condvar)>);

#[derive(Debug, Default)]
struct Files {
    claimed: Vec<PathBuf>,
    /// Writers creating a file, not yet claimed: the sweep waits for them too, since
    /// the path is not known until the file exists.
    creating: usize,
    /// Files whose holder let go but could not remove them. Not waited for: nothing
    /// is writing them.
    left_over: Vec<PathBuf>,
    swept: bool,
    /// Set by a sweep as it starts waiting, so a test can tell it is blocked.
    #[cfg(test)]
    sweep_waiting: bool,
}

impl Files {
    fn busy(&self) -> bool {
        !self.claimed.is_empty() || self.creating > 0
    }
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

    /// Whether a file is still claimed or being created.
    #[cfg(test)]
    pub(crate) fn writing(&self) -> bool {
        self.lock().busy()
    }

    /// By `deadline`, remove every file still claimed; stopped writers remove their own,
    /// and one busy at the deadline loses its file (and harmlessly fails to remove it). Files
    /// mid-creation are waited for; later ones are refused.
    pub(crate) fn sweep(&self, deadline: Instant) {
        let (_, released) = &*self.0;
        let mut files = self.lock();
        while files.busy() {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                break;
            }
            // Set under the lock that `wait_timeout` releases, so whoever sees it next
            // sees a sweep already waiting.
            #[cfg(test)]
            {
                files.sweep_waiting = true;
                released.notify_all();
            }
            files = released
                .wait_timeout(files, left)
                .unwrap_or_else(|e| e.into_inner())
                .0;
        }
        files.swept = true;
        let mut left = std::mem::take(&mut files.claimed);
        left.append(&mut files.left_over);
        drop(files);
        loop {
            left.retain(|path| !removed(path));
            if left.is_empty() || Instant::now() + RETRY_EVERY > deadline {
                break;
            }
            std::thread::sleep(RETRY_EVERY);
        }
        for path in left {
            log::warn!("Could not remove the temporary file {}", path.display());
        }
    }

    /// Whether a sweep starts waiting on a busy writer within `limit`. Bounded, so a
    /// sweep that never waits fails the test instead of hanging it.
    #[cfg(test)]
    fn a_sweep_waits_within(&self, limit: Duration) -> bool {
        let (_, released) = &*self.0;
        released
            .wait_timeout_while(self.lock(), limit, |files| !files.sweep_waiting)
            .unwrap_or_else(|e| e.into_inner())
            .0
            .sweep_waiting
    }

    fn lock(&self) -> MutexGuard<'_, Files> {
        self.0.0.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn released(&self) {
        self.0.1.notify_all();
    }
}

impl Writer {
    /// Whether the open was stopped: a writer gives up at its next chunk.
    pub(crate) fn stopped(&self) -> bool {
        self.stop.load(Ordering::Relaxed)
    }

    /// Create a file with `make` and claim it, as one step to the sweep (a sweep during
    /// `make` waits, then removes it or finds it gone; claiming afterward would leave a
    /// gap). `None`, with nothing on disk, once the open is stopped or swept.
    pub(crate) fn create<F: AsRef<Path>, E>(
        &self,
        make: impl FnOnce() -> Result<F, E>,
    ) -> Result<Option<(F, Claim)>, E> {
        {
            let mut files = self.files.lock();
            if files.swept || self.stopped() {
                return Ok(None);
            }
            files.creating += 1;
        }
        let made = make();
        let mut files = self.files.lock();
        let claimed = match made {
            Ok(file) if !files.swept && !self.stopped() => {
                let path = file.as_ref().to_path_buf();
                files.claimed.push(path.clone());
                Ok(Some((
                    file,
                    Claim {
                        files: self.files.clone(),
                        path,
                    },
                )))
            }
            Ok(file) => {
                // Removed before the sweep stops waiting for it, and not under the lock.
                drop(files);
                drop(file);
                files = self.files.lock();
                Ok(None)
            }
            Err(e) => Err(e),
        };
        files.creating -= 1;
        drop(files);
        self.files.released();
        claimed
    }
}

/// Dropped after the app, removes what the app's opens were still writing. See the
/// module.
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
        // Ones left over earlier may have been let go since: a frame dropped.
        let earlier = std::mem::take(&mut files.left_over);
        drop(files);
        let mut left: Vec<PathBuf> = earlier.into_iter().filter(|p| !removed(p)).collect();
        // Its holder removed it before letting go, unless that failed.
        if std::fs::symlink_metadata(&self.path).is_ok() {
            left.push(self.path.clone());
        }
        self.files.lock().left_over.append(&mut left);
        self.files.released();
    }
}

/// Whether `path` is gone, removing it if it is there.
fn removed(path: &Path) -> bool {
    match std::fs::remove_file(path) {
        Ok(()) => true,
        Err(e) => e.kind() == std::io::ErrorKind::NotFound,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file_in(dir: &Path) -> std::io::Result<tempfile::NamedTempFile> {
        tempfile::NamedTempFile::new_in(dir)
    }

    fn files_in(dir: &Path) -> usize {
        std::fs::read_dir(dir).unwrap().count()
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
                let (file, claim) = writer
                    .create(|| file_in(&dir))
                    .unwrap()
                    .expect("not stopped yet");
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
        assert_eq!(files_in(dir.path()), 0);
        assert!(!unfinished.writing());
        worker.join().unwrap();
    }

    /// A file still claimed at the deadline is removed by the sweep, and its holder
    /// letting go later is harmless.
    #[test]
    fn a_sweep_removes_what_a_writer_still_holds() {
        let dir = tempfile::tempdir().unwrap();
        let unfinished = Unfinished::default();
        let writer = unfinished.writer(Arc::default());
        let (file, claim) = writer
            .create(|| file_in(dir.path()))
            .unwrap()
            .expect("not stopped yet");
        let path = file.path().to_path_buf();
        unfinished.sweep(Instant::now() + Duration::from_millis(20));
        assert!(!path.exists());
        assert!(!unfinished.writing());
        drop(file);
        drop(claim);
    }

    /// A file being created when the sweep starts is waited for: the sweep does not
    /// end, and the process with it, while a file it has not heard of is still to
    /// appear. Here the open is stopped, so the writer removes it itself.
    #[test]
    fn a_sweep_waits_for_a_file_being_created() {
        let dir = tempfile::tempdir().unwrap();
        let unfinished = Unfinished::default();
        let stop = Arc::new(AtomicBool::new(false));
        let writer = unfinished.writer(stop.clone());
        let (creating, wait) = std::sync::mpsc::channel();
        let (go, gate) = std::sync::mpsc::channel::<()>();
        let worker = {
            let dir = dir.path().to_path_buf();
            std::thread::spawn(move || {
                writer
                    .create(|| {
                        creating.send(()).unwrap();
                        gate.recv().unwrap();
                        file_in(&dir)
                    })
                    .unwrap()
                    .is_none()
            })
        };
        wait.recv().unwrap();
        stop.store(true, Ordering::Relaxed);
        let sweeper = {
            let unfinished = unfinished.clone();
            std::thread::spawn(move || unfinished.sweep(Instant::now() + Duration::from_secs(30)))
        };
        assert!(
            unfinished.a_sweep_waits_within(Duration::from_secs(30)),
            "the sweep waits for the file"
        );
        assert!(unfinished.writing());
        assert!(!sweeper.is_finished());
        go.send(()).unwrap();
        sweeper.join().unwrap();
        // Checked before joining the writer: its thread may still be exiting, but the
        // file it made must already be gone.
        assert_eq!(files_in(dir.path()), 0, "the sweep waited for the file");
        assert!(!unfinished.writing());
        assert!(worker.join().unwrap(), "a stopped open's file is refused");
    }

    /// A file its holder could not remove, as on Windows while a frame still maps it,
    /// is not forgotten: the next claim to let go tries it again, and so does the
    /// sweep, without waiting on it as on a writer.
    #[test]
    fn a_file_its_holder_could_not_remove_is_removed_later() {
        let dir = tempfile::tempdir().unwrap();
        let unfinished = Unfinished::default();
        let writer = unfinished.writer(Arc::default());
        // A holder whose removal failed: the claim goes, the file stays.
        let left_over = |name: &str| {
            let path = dir.path().join(name);
            let (path, claim) = writer
                .create(|| std::fs::write(&path, b"x").map(|()| path.clone()))
                .unwrap()
                .expect("not stopped yet");
            drop(claim);
            assert!(path.exists());
            path
        };
        let first = left_over("first");
        assert!(!unfinished.writing(), "nothing is writing it");
        let (file, claim) = writer
            .create(|| file_in(dir.path()))
            .unwrap()
            .expect("not stopped yet");
        drop(file);
        drop(claim);
        assert!(!first.exists(), "the next claim tried it again");

        let second = left_over("second");
        let began = Instant::now();
        unfinished.sweep(began + Duration::from_secs(30));
        assert!(began.elapsed() < Duration::from_secs(10), "not waited on");
        assert!(!second.exists(), "the sweep removed it");
        assert_eq!(files_in(dir.path()), 0);
    }

    /// A stopped or swept open creates nothing.
    #[test]
    fn a_stopped_or_swept_open_refuses_new_files() {
        let dir = tempfile::tempdir().unwrap();
        let unfinished = Unfinished::default();
        let made = |writer: &Writer| {
            writer
                .create(|| file_in(dir.path()))
                .unwrap()
                .map(|(file, _)| file)
        };
        let stopped = unfinished.writer(Arc::new(AtomicBool::new(true)));
        assert!(made(&stopped).is_none());

        let writer = unfinished.writer(Arc::default());
        unfinished.sweep(Instant::now());
        assert!(made(&writer).is_none());
        assert_eq!(files_in(dir.path()), 0);
    }
}

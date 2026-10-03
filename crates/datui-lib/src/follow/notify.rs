//! Waiting for a followed file to change, through inotify on Linux: the watcher sleeps
//! until the file or its directory changes, or it is rung, rather than checking the
//! file's size on a timer. Elsewhere, and on a file system whose changes another host
//! may make (NFS, SMB, FUSE), the watcher checks the size every interval instead.

use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::path::Path;

/// What woke a wait.
#[derive(Debug, PartialEq)]
pub(super) enum Woke {
    /// The file, or an entry of its directory, changed.
    Changed,
    /// The bell rang: a look was asked for, or the follow stops.
    Rung,
    /// The wait gave out (only a wait with a timeout).
    TimedOut,
}

/// Rings a waiting watcher awake from another thread.
pub(super) struct Bell {
    fd: Option<OwnedFd>,
}

impl Default for Bell {
    fn default() -> Bell {
        // SAFETY: eventfd takes no pointers; a negative return is an error.
        let fd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) };
        // SAFETY: a non-negative fd was just opened and is owned by nothing else.
        let fd = (fd >= 0).then(|| unsafe { OwnedFd::from_raw_fd(fd) });
        Bell { fd }
    }
}

impl Bell {
    pub(super) fn ring(&self) {
        if let Some(fd) = &self.fd {
            let one = 1u64.to_ne_bytes();
            // SAFETY: writes 8 bytes from a live buffer; a full counter only fails.
            unsafe { libc::write(fd.as_raw_fd(), one.as_ptr().cast(), one.len()) };
        }
    }

    /// Quiet it again after it rang.
    fn hush(&self) {
        if let Some(fd) = &self.fd {
            let mut count = [0u8; 8];
            // SAFETY: reads at most 8 bytes into a live buffer; nonblocking.
            unsafe { libc::read(fd.as_raw_fd(), count.as_mut_ptr().cast(), count.len()) };
        }
    }
}

/// File systems whose files another host can change without inotify hearing of it.
const REMOTE_FILE_SYSTEMS: [i64; 11] = [
    0x6969,                 // NFS
    0x517B,                 // SMB
    0xFF53_4D42_u32 as i64, // CIFS
    0xFE53_4D42_u32 as i64, // SMB2
    0x6573_5546,            // FUSE
    0x00C3_6400,            // Ceph
    0x5346_414F,            // AFS
    0x6B41_4653,            // kAFS
    0x0102_1997,            // 9P
    0x7375_7245,            // Coda
    0x0BD0_0BD0,            // Lustre
];

/// Whether `path` is on a file system another host may write.
fn remote(path: &Path) -> bool {
    let Ok(c_path) = std::ffi::CString::new(path.as_os_str().as_encoded_bytes()) else {
        return true;
    };
    // SAFETY: statfs is plain data, valid zeroed; the call fills it from a live
    // NUL-terminated path.
    let mut stat: libc::statfs = unsafe { std::mem::zeroed() };
    if unsafe { libc::statfs(c_path.as_ptr(), &mut stat) } != 0 {
        return true;
    }
    // The field's type differs between targets; compare as the kernel's 32 bits.
    let kind = stat.f_type as i64 & 0xFFFF_FFFF;
    REMOTE_FILE_SYSTEMS.contains(&kind)
}

/// An inotify instance watching a followed file and its directory.
pub(super) struct Notify {
    fd: OwnedFd,
}

/// Changes to the file's contents, or the file going away.
const FILE_EVENTS: u32 = libc::IN_MODIFY
    | libc::IN_ATTRIB
    | libc::IN_CLOSE_WRITE
    | libc::IN_MOVE_SELF
    | libc::IN_DELETE_SELF;

/// Changes to the directory's entries: a file put in place of the followed one, or the
/// followed one removed or renamed. Writes to a file in it, for a path whose file
/// changed under it.
const DIRECTORY_EVENTS: u32 =
    libc::IN_CREATE | libc::IN_MOVED_TO | libc::IN_MOVED_FROM | libc::IN_DELETE | libc::IN_MODIFY;

impl Notify {
    /// Watch `path` and its directory. `None` when changes to it cannot be heard of:
    /// inotify is unavailable or out of watches, or another host may write the file.
    pub(super) fn new(path: &Path) -> Option<Notify> {
        if remote(path) {
            return None;
        }
        // SAFETY: takes no pointers; a negative return is an error.
        let fd = unsafe { libc::inotify_init1(libc::IN_NONBLOCK | libc::IN_CLOEXEC) };
        if fd < 0 {
            return None;
        }
        // SAFETY: a non-negative fd was just opened and is owned by nothing else.
        let notify = Notify {
            fd: unsafe { OwnedFd::from_raw_fd(fd) },
        };
        let directory = match path.parent() {
            Some(parent) if !parent.as_os_str().is_empty() => parent,
            _ => Path::new("."),
        };
        (notify.watch(path, FILE_EVENTS) && notify.watch(directory, DIRECTORY_EVENTS))
            .then_some(notify)
    }

    /// Watch the file `path` names now, after it was replaced.
    pub(super) fn rewatch(&self, path: &Path) {
        self.watch(path, FILE_EVENTS);
    }

    fn watch(&self, path: &Path, mask: u32) -> bool {
        let Ok(c_path) = std::ffi::CString::new(path.as_os_str().as_encoded_bytes()) else {
            return false;
        };
        // SAFETY: a live inotify fd and a NUL-terminated path.
        unsafe { libc::inotify_add_watch(self.fd.as_raw_fd(), c_path.as_ptr(), mask) >= 0 }
    }

    /// Wait for a change or `bell`, for at most `timeout` when one is given.
    pub(super) fn wait(&self, bell: &Bell, timeout: Option<std::time::Duration>) -> Woke {
        let mut fds = [
            libc::pollfd {
                fd: self.fd.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: bell.fd.as_ref().map_or(-1, |fd| fd.as_raw_fd()),
                events: libc::POLLIN,
                revents: 0,
            },
        ];
        let timeout = timeout.map_or(-1, |t| i32::try_from(t.as_millis()).unwrap_or(i32::MAX));
        loop {
            // SAFETY: two live pollfds; a negative fd is ignored by poll.
            let n = unsafe { libc::poll(fds.as_mut_ptr(), fds.len() as libc::nfds_t, timeout) };
            if n < 0 && std::io::Error::last_os_error().kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            if n == 0 {
                return Woke::TimedOut;
            }
            if fds[1].revents != 0 {
                bell.hush();
                return Woke::Rung;
            }
            // A poll that failed is taken as a change: the size is checked either way.
            return Woke::Changed;
        }
    }

    /// Read the events that arrived, so the next wait waits for new ones.
    pub(super) fn drain(&self) {
        let mut buf = [0u8; 4096];
        // SAFETY: reads into a live buffer of its length; nonblocking, so it stops once
        // nothing is left.
        while unsafe { libc::read(self.fd.as_raw_fd(), buf.as_mut_ptr().cast(), buf.len()) } > 0 {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;
    use std::time::Duration;

    /// An append, a file put in its place and a ring each wake a wait; nothing happening
    /// does not.
    #[test]
    fn a_wait_wakes_for_changes_and_the_bell() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log.csv");
        std::fs::write(&path, "t\n1\n").unwrap();
        let notify = Notify::new(&path).expect("tmp takes an inotify watch");
        let bell = Bell::default();
        let short = Some(Duration::from_millis(20));
        assert_eq!(notify.wait(&bell, short), Woke::TimedOut);

        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        file.write_all(b"2\n").unwrap();
        let guard = Some(Duration::from_secs(30));
        assert_eq!(notify.wait(&bell, guard), Woke::Changed);
        notify.drain();
        assert_eq!(notify.wait(&bell, short), Woke::TimedOut, "drained");

        let next = dir.path().join("next.csv");
        std::fs::write(&next, "t\n9\n").unwrap();
        notify.drain();
        std::fs::rename(&next, &path).unwrap();
        assert_eq!(notify.wait(&bell, guard), Woke::Changed);
        notify.drain();

        bell.ring();
        assert_eq!(notify.wait(&bell, guard), Woke::Rung);
        assert_eq!(notify.wait(&bell, short), Woke::TimedOut, "hushed");
    }
}

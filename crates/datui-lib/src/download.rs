//! Remote files downloaded to local ones.
//!
//! A download is read on one side and written on the thread that asked, never on a
//! runtime worker, with at most [`QUEUED_CHUNKS`] waiting between the two: a full
//! queue stops the reading, so a slow disk holds the transfer back instead of the
//! file piling up in memory. A store's stream is polled on the app's runtime; a
//! blocking HTTP reader runs on a thread of its own.

use color_eyre::Result;
use color_eyre::eyre::eyre;
use std::path::Path;
use std::sync::Arc;

use crate::unfinished::{Claim, Writer};

/// A downloaded file, removed from disk when its last holder drops it: the app keeps
/// one to read the file again, the dataset scanning it keeps another, and an event
/// carrying one that is never handled removes it as it drops.
#[derive(Debug, Clone)]
pub struct TempDownload(Arc<Held>);

/// The file, then the open's claim on it: dropped in that order, so the claim is let
/// go only once the file is gone. See [`crate::unfinished`].
#[derive(Debug)]
struct Held {
    path: tempfile::TempPath,
    _claim: Option<Claim>,
}

impl TempDownload {
    /// An empty file in `dir` (the system temp directory when `None`) ending in
    /// `.extension`, or `.tmp` without one. Removed if dropped before [`Self::keep`].
    pub fn create(dir: Option<&Path>, extension: Option<&str>) -> Result<tempfile::NamedTempFile> {
        let dir = dir
            .map(Path::to_path_buf)
            .unwrap_or_else(std::env::temp_dir);
        let suffix = extension
            .map(|e| format!(".{e}"))
            .unwrap_or_else(|| ".tmp".to_string());
        tempfile::Builder::new()
            .suffix(&suffix)
            .tempfile_in(&dir)
            .map_err(|_| eyre!("Could not create a temporary file."))
    }

    /// A finished file from [`Self::create`], closed and held.
    pub fn keep(file: tempfile::NamedTempFile) -> TempDownload {
        Self::held(file, None)
    }

    fn held(file: tempfile::NamedTempFile, claim: Option<Claim>) -> TempDownload {
        TempDownload(Arc::new(Held {
            path: file.into_temp_path(),
            _claim: claim,
        }))
    }

    pub fn path(&self) -> &Path {
        &self.0.path
    }
}

/// Chunks queued between the side reading a download and the thread writing it.
pub const QUEUED_CHUNKS: usize = 4;

/// How often a download that has gone quiet checks whether it was stopped.
const STALL_CHECK: std::time::Duration = std::time::Duration::from_millis(100);

/// What opening a download answers: its stream or reader and its length, when the
/// source gave one, or why it could not be opened.
pub type Opened<S> = std::result::Result<(S, Option<u64>), String>;

/// Why a download did not arrive whole.
#[derive(Debug)]
pub enum StreamError {
    /// The request failed, or the opener turned its answer down.
    Open(String),
    /// A chunk failed partway.
    Read(String),
    /// The writer refused a chunk.
    Write(color_eyre::Report),
    /// The stream ended short of the length the store gave.
    Short { expected: u64, got: u64 },
    /// Stopped, or cut off by the runtime shutting down.
    Cut,
}

/// What the reading side hands the writer, in order.
enum Piece<B> {
    /// The request was answered; the length, when the source gave one.
    Opened(Option<u64>),
    Refused(String),
    Chunk(B),
    Failed(String),
    End,
}

/// Hand each chunk `next` gives to `write`, in order, until the end. `next` answers
/// `None` once the reading side is gone or `stop` says so. Returns the bytes written.
fn receive<B: AsRef<[u8]>>(
    stop: &impl Fn() -> bool,
    mut next: impl FnMut() -> Option<Piece<B>>,
    mut write: impl FnMut(&[u8]) -> Result<()>,
) -> std::result::Result<u64, StreamError> {
    let mut expected = None;
    let mut written = 0u64;
    loop {
        if stop() {
            return Err(StreamError::Cut);
        }
        match next() {
            Some(Piece::Opened(len)) => expected = len,
            Some(Piece::Refused(error)) => return Err(StreamError::Open(error)),
            Some(Piece::Chunk(chunk)) => {
                let chunk = chunk.as_ref();
                write(chunk).map_err(StreamError::Write)?;
                written += chunk.len() as u64;
            }
            Some(Piece::Failed(error)) => return Err(StreamError::Read(error)),
            Some(Piece::End) => break,
            None => return Err(StreamError::Cut),
        }
    }
    match expected {
        Some(expected) if expected != written => Err(StreamError::Short {
            expected,
            got: written,
        }),
        _ => Ok(written),
    }
}

/// A new file in `dir`, as [`TempDownload::create`] names it, filled by `fill`
/// through the writer it is handed. Any failure, and a stop, removes the partial
/// file before this returns.
///
/// The file is claimed through `writer` from the moment it exists until its last
/// holder drops it, so quitting removes it even while this thread is still writing
/// (see [`crate::unfinished`]). A stopped open's file is refused and removed.
fn fill_temp(
    dir: Option<&Path>,
    extension: Option<&str>,
    writer: &Writer,
    fill: impl FnOnce(&mut dyn FnMut(&[u8]) -> Result<()>) -> std::result::Result<u64, StreamError>,
) -> std::result::Result<TempDownload, StreamError> {
    use std::io::Write;

    let Some((mut file, claim)) = writer
        .create(|| TempDownload::create(dir, extension))
        .map_err(StreamError::Write)?
    else {
        return Err(StreamError::Cut);
    };
    let unwritable = |e: std::io::Error| eyre!("Could not write the downloaded file: {e}");
    let filled = fill(&mut |chunk| file.write_all(chunk).map_err(unwritable))
        .and_then(|_| file.flush().map_err(|e| StreamError::Write(unwritable(e))));
    match filled {
        Ok(_) => Ok(TempDownload::held(file, Some(claim))),
        Err(e) => {
            // The file before the claim, so a sweep never finds it let go but there.
            drop(file);
            drop(claim);
            Err(e)
        }
    }
}

/// Run `open` on `runtime` and hand each chunk of the stream it answers with to
/// `write` on this thread, in order. Returns the bytes written.
///
/// `open` gives the stream and its length, when known. `stop` is checked between
/// chunks, and every [`STALL_CHECK`] while the store is silent; so is whether this
/// side has stopped listening. Ends in an error, never a short success, when the
/// open or a chunk fails, `write` refuses one, `stop` says so, or the runtime shuts
/// down mid-transfer; the request is dropped with the stream then. Must not be
/// called on a runtime worker: it blocks.
#[cfg(feature = "cloud")]
pub fn stream_into<O, S, B, E>(
    runtime: &tokio::runtime::Handle,
    open: O,
    stop: impl Fn() -> bool + Clone + Send + Sync + 'static,
    write: impl FnMut(&[u8]) -> Result<()>,
) -> std::result::Result<u64, StreamError>
where
    O: std::future::Future<Output = Opened<S>> + Send + 'static,
    S: futures::Stream<Item = std::result::Result<B, E>> + Send + 'static,
    B: AsRef<[u8]> + Send + 'static,
    E: std::fmt::Display,
{
    let (tx, mut rx) = tokio::sync::mpsc::channel(QUEUED_CHUNKS);
    let stopped = stop.clone();
    // Spawned rather than polled from here: a shutdown drops the task, and the
    // closed channel ends the wait below, where a future polled on this thread would
    // touch a runtime that is gone.
    runtime.spawn(async move {
        // A writer that gave up has dropped the queue; a store gone quiet would
        // otherwise hold the request open until it answered.
        let gone = || stopped() || tx.is_closed();
        let (stream, len) = match until_stopped(open, &gone).await {
            Some(Ok(opened)) => opened,
            Some(Err(error)) => {
                let _ = tx.send(Piece::Refused(error)).await;
                return;
            }
            None => return,
        };
        if tx.send(Piece::Opened(len)).await.is_err() {
            return;
        }
        let mut stream = std::pin::pin!(stream);
        loop {
            let next = futures::StreamExt::next(&mut stream);
            let piece = match until_stopped(next, &gone).await {
                None => return,
                Some(Some(Ok(chunk))) => Piece::Chunk(chunk),
                Some(Some(Err(error))) => Piece::Failed(error.to_string()),
                Some(None) => Piece::End,
            };
            let last = !matches!(piece, Piece::Chunk(_));
            // Waits while the queue is full, which is the backpressure: the stream is
            // not polled again until the writer has taken a chunk.
            if tx.send(piece).await.is_err() || last {
                return;
            }
        }
    });
    receive(&stop, || rx.blocking_recv(), write)
}

/// `future`'s output, or `None` once `stop` says so while it is still pending.
#[cfg(feature = "cloud")]
async fn until_stopped<F: std::future::Future>(
    future: F,
    stop: &impl Fn() -> bool,
) -> Option<F::Output> {
    let mut future = std::pin::pin!(future);
    loop {
        if stop() {
            return None;
        }
        if let Ok(output) = tokio::time::timeout(STALL_CHECK, future.as_mut()).await {
            return Some(output);
        }
    }
}

/// Stream the object `open` answers with into a new file in `dir`, as
/// [`TempDownload::create`] names it, until `writer`'s open stops. Any failure, and a
/// stop, removes the partial file before this returns.
#[cfg(feature = "cloud")]
pub(crate) fn stream_to_temp<O, S, B, E>(
    runtime: &tokio::runtime::Handle,
    dir: Option<&Path>,
    extension: Option<&str>,
    open: O,
    writer: &Writer,
) -> std::result::Result<TempDownload, StreamError>
where
    O: std::future::Future<Output = Opened<S>> + Send + 'static,
    S: futures::Stream<Item = std::result::Result<B, E>> + Send + 'static,
    B: AsRef<[u8]> + Send + 'static,
    E: std::fmt::Display,
{
    let stop = {
        let writer = writer.clone();
        move || writer.stopped()
    };
    fill_temp(dir, extension, writer, |write| {
        stream_into(runtime, open, stop, write)
    })
}

/// Bytes asked of a blocking reader at a time.
#[cfg(feature = "http")]
const READ_CHUNK: usize = 64 * 1024;

/// Run `open` on a thread of its own and hand each chunk read from the reader it
/// answers with to `write` on this thread, in order. Returns the bytes written; a
/// length `open` gives must be what arrives.
///
/// The reads run at most [`QUEUED_CHUNKS`] ahead of the writes, and a server that
/// stops sending holds that thread and not this one: `stop` is checked between
/// chunks and every [`STALL_CHECK`] while nothing arrives. The reading thread ends at
/// its next chunk once this side has gone.
#[cfg(feature = "http")]
pub fn read_into<R: std::io::Read>(
    open: impl FnOnce() -> Opened<R> + Send + 'static,
    stop: impl Fn() -> bool,
    write: impl FnMut(&[u8]) -> Result<()>,
) -> std::result::Result<u64, StreamError> {
    use std::sync::mpsc::RecvTimeoutError;

    let (tx, rx) = std::sync::mpsc::sync_channel(QUEUED_CHUNKS);
    std::thread::Builder::new()
        .name("datui-download".to_string())
        .spawn(move || {
            let mut reader = match open() {
                Ok((reader, len)) => {
                    if tx.send(Piece::Opened(len)).is_err() {
                        return;
                    }
                    reader
                }
                Err(error) => {
                    let _ = tx.send(Piece::Refused(error));
                    return;
                }
            };
            loop {
                let mut chunk = vec![0; READ_CHUNK];
                let piece = match reader.read(&mut chunk) {
                    Ok(0) => Piece::End,
                    Ok(read) => {
                        chunk.truncate(read);
                        Piece::Chunk(chunk)
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(error) => Piece::Failed(error.to_string()),
                };
                let last = !matches!(piece, Piece::Chunk(_));
                if tx.send(piece).is_err() || last {
                    return;
                }
            }
        })
        .map_err(|e| StreamError::Open(e.to_string()))?;
    let next = || loop {
        match rx.recv_timeout(STALL_CHECK) {
            Ok(piece) => return Some(piece),
            Err(RecvTimeoutError::Timeout) if !stop() => {}
            Err(RecvTimeoutError::Timeout) => return None,
            // Only a reader that panicked leaves without saying how it ended.
            Err(RecvTimeoutError::Disconnected) => {
                return Some(Piece::Failed("the download stopped".to_string()));
            }
        }
    };
    receive(&stop, next, write)
}

/// Read what `open` answers with into a new file in `dir`, as
/// [`TempDownload::create`] names it, until `writer`'s open stops; see [`read_into`].
/// Any failure, and a stop, removes the partial file before this returns.
#[cfg(feature = "http")]
pub(crate) fn read_to_temp<R: std::io::Read>(
    dir: Option<&Path>,
    extension: Option<&str>,
    open: impl FnOnce() -> Opened<R> + Send + 'static,
    writer: &Writer,
) -> std::result::Result<TempDownload, StreamError> {
    fill_temp(dir, extension, writer, |write| {
        read_into(open, || writer.stopped(), write)
    })
}

#[cfg(all(test, feature = "cloud"))]
mod tests {
    use super::*;
    use futures::StreamExt;
    use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

    const CHUNK: usize = 64 * 1024;

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("runtime")
    }

    /// Chunk `i` of a test object: bytes that differ by position, so a reordered or
    /// dropped chunk shows.
    fn chunk(i: usize, len: usize) -> Vec<u8> {
        (0..len).map(|j| ((i * 31 + j * 7) % 251) as u8).collect()
    }

    /// Bytes alive in chunks the stream has made and nobody has dropped yet, and the
    /// most there ever were: what the transfer held in memory at its peak.
    #[derive(Default)]
    struct Live {
        now: AtomicU64,
        peak: AtomicU64,
    }

    struct Tracked(Vec<u8>, Arc<Live>);

    impl Tracked {
        fn new(bytes: Vec<u8>, live: &Arc<Live>) -> Tracked {
            let now = live.now.fetch_add(bytes.len() as u64, Ordering::SeqCst) + bytes.len() as u64;
            live.peak.fetch_max(now, Ordering::SeqCst);
            Tracked(bytes, live.clone())
        }
    }

    impl Drop for Tracked {
        fn drop(&mut self) {
            self.1.now.fetch_sub(self.0.len() as u64, Ordering::SeqCst);
        }
    }

    impl AsRef<[u8]> for Tracked {
        fn as_ref(&self) -> &[u8] {
            &self.0
        }
    }

    /// Sets its flag when dropped: when a stream holding it was let go.
    struct DropFlag(Arc<AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    async fn opened<S>(stream: S, len: Option<u64>) -> Opened<S> {
        Ok((stream, len))
    }

    fn never() -> impl Fn() -> bool + Clone + Send + Sync + 'static {
        || false
    }

    /// A writer whose open is never stopped.
    fn unstopped() -> Writer {
        Writer::default()
    }

    fn files_in(dir: &Path) -> usize {
        std::fs::read_dir(dir).unwrap().count()
    }

    /// Many chunks land in order, byte for byte, in a file named for the format.
    #[test]
    fn a_stream_lands_whole_in_order() {
        let rt = runtime();
        let dir = tempfile::tempdir().unwrap();
        let chunks = (0..40).map(|i| chunk(i, 1000 + i * 13)).collect::<Vec<_>>();
        let whole = chunks.concat();
        let stream = futures::stream::iter(chunks.into_iter().map(Ok::<_, String>));
        let file = stream_to_temp(
            rt.handle(),
            Some(dir.path()),
            Some("csv.gz"),
            opened(stream, Some(whole.len() as u64)),
            &unstopped(),
        )
        .unwrap();
        assert_eq!(std::fs::read(file.path()).unwrap(), whole);
        assert!(file.path().to_string_lossy().ends_with(".csv.gz"));
        assert_eq!(file.path().parent(), Some(dir.path()));
        let held = file.clone();
        drop(file);
        assert!(held.path().exists(), "a holder keeps it");
        let path = held.path().to_path_buf();
        drop(held);
        assert!(!path.exists(), "the last holder removes it");
    }

    /// A download is claimed from the moment its file exists until its last holder lets
    /// it go, so quitting finds it whoever holds it; an open already stopped gets no
    /// file at all.
    #[test]
    fn a_download_is_claimed_for_as_long_as_it_lives() {
        let rt = runtime();
        let dir = tempfile::tempdir().unwrap();
        let unfinished = crate::unfinished::Unfinished::default();
        let stop = Arc::new(AtomicBool::new(false));
        let writer = unfinished.writer(stop.clone());
        let stream = futures::stream::iter(vec![Ok::<_, String>(chunk(0, 10))]);
        let file = stream_to_temp(
            rt.handle(),
            Some(dir.path()),
            None,
            opened(stream, None),
            &writer,
        )
        .unwrap();
        let held = file.clone();
        drop(file);
        assert!(unfinished.writing(), "a holder keeps the claim");
        let path = held.path().to_path_buf();
        drop(held);
        assert!(!path.exists());
        assert!(!unfinished.writing(), "the claim goes with the file");

        stop.store(true, Ordering::SeqCst);
        let stream = futures::stream::iter(vec![Ok::<_, String>(chunk(0, 10))]);
        let error = stream_to_temp(
            rt.handle(),
            Some(dir.path()),
            None,
            opened(stream, None),
            &writer,
        )
        .unwrap_err();
        assert!(matches!(error, StreamError::Cut), "{error:?}");
        assert_eq!(files_in(dir.path()), 0);
    }

    /// A slow writer holds the stream back: the store is never more than the queue
    /// ahead of the disk, and no more than a few chunks are in memory at once however
    /// large the object.
    #[test]
    fn a_slow_writer_holds_the_stream_back() {
        let rt = runtime();
        let live = Arc::new(Live::default());
        let pulled = Arc::new(AtomicUsize::new(0));
        let chunks = 64;
        let stream = {
            let (live, pulled) = (live.clone(), pulled.clone());
            futures::stream::iter(0..chunks).map(move |i| {
                pulled.fetch_add(1, Ordering::SeqCst);
                Ok::<_, String>(Tracked::new(chunk(i, CHUNK), &live))
            })
        };
        let mut written = 0usize;
        let mut ahead = 0usize;
        let mut bytes = Vec::new();
        let total = stream_into(rt.handle(), opened(stream, None), never(), |piece| {
            std::thread::sleep(std::time::Duration::from_millis(2));
            written += 1;
            ahead = ahead.max(pulled.load(Ordering::SeqCst) - written);
            bytes.extend_from_slice(piece);
            Ok(())
        })
        .unwrap();
        assert_eq!(total, (chunks * CHUNK) as u64);
        assert_eq!(
            bytes,
            (0..chunks)
                .flat_map(|i| chunk(i, CHUNK))
                .collect::<Vec<_>>()
        );
        // The queue, one chunk waiting to go into it and one being written.
        let bound = QUEUED_CHUNKS + 2;
        assert!(ahead <= bound, "the store ran {ahead} chunks ahead");
        let peak = live.peak.load(Ordering::SeqCst);
        assert!(
            peak <= (bound * CHUNK) as u64,
            "{peak} bytes held at once, of {} streamed",
            chunks * CHUNK
        );
        assert_eq!(live.now.load(Ordering::SeqCst), 0, "every chunk was let go");
    }

    /// A chunk that fails partway fails the download and leaves no file.
    #[test]
    fn a_failure_mid_stream_leaves_no_file() {
        let rt = runtime();
        let dir = tempfile::tempdir().unwrap();
        let stream = futures::stream::iter(vec![
            Ok(chunk(0, CHUNK)),
            Ok(chunk(1, CHUNK)),
            Err("connection reset".to_string()),
            Ok(chunk(3, CHUNK)),
        ]);
        let error = stream_to_temp(
            rt.handle(),
            Some(dir.path()),
            None,
            opened(stream, None),
            &unstopped(),
        )
        .unwrap_err();
        assert!(
            matches!(&error, StreamError::Read(e) if e == "connection reset"),
            "{error:?}"
        );
        assert_eq!(files_in(dir.path()), 0);

        let refused = async {
            Err::<(futures::stream::Empty<Result<Vec<u8>, String>>, _), _>("403".to_string())
        };
        let error =
            stream_to_temp(rt.handle(), Some(dir.path()), None, refused, &unstopped()).unwrap_err();
        assert!(
            matches!(&error, StreamError::Open(e) if e == "403"),
            "{error:?}"
        );
        assert_eq!(files_in(dir.path()), 0);

        // Ended early without saying so: short of the length the store gave.
        let stream = futures::stream::iter(vec![Ok::<_, String>(chunk(0, 10))]);
        let error = stream_to_temp(
            rt.handle(),
            Some(dir.path()),
            None,
            opened(stream, Some(20)),
            &unstopped(),
        )
        .unwrap_err();
        assert!(
            matches!(
                error,
                StreamError::Short {
                    expected: 20,
                    got: 10
                }
            ),
            "{error:?}"
        );
        assert_eq!(files_in(dir.path()), 0);
    }

    /// A disk that refuses a write ends the transfer there: the store is not read to
    /// the end for nothing.
    #[test]
    fn a_refused_write_stops_the_stream() {
        let rt = runtime();
        let pulled = Arc::new(AtomicUsize::new(0));
        let stream = {
            let pulled = pulled.clone();
            futures::stream::iter(0..1000).map(move |i| {
                pulled.fetch_add(1, Ordering::SeqCst);
                Ok::<_, String>(chunk(i, 1024))
            })
        };
        let mut writes = 0;
        let error = stream_into(rt.handle(), opened(stream, None), never(), |_| {
            writes += 1;
            if writes == 3 {
                return Err(eyre!("No space left on device"));
            }
            Ok(())
        })
        .unwrap_err();
        assert!(matches!(&error, StreamError::Write(e) if e.to_string().contains("No space")));
        // The task sees the closed queue on its next send.
        std::thread::sleep(std::time::Duration::from_millis(200));
        let pulled = pulled.load(Ordering::SeqCst);
        assert!(pulled <= 3 + QUEUED_CHUNKS + 2, "{pulled} chunks read");

        // A store that goes quiet after the refused chunk is let go all the same,
        // rather than held open until it answers.
        let dropped = Arc::new(AtomicBool::new(false));
        let stream = {
            let guard = DropFlag(dropped.clone());
            futures::stream::iter(vec![Ok::<_, String>(chunk(0, 1024))])
                .chain(futures::stream::pending())
                .map(move |chunk| {
                    let _ = &guard;
                    chunk
                })
        };
        let error = stream_into(rt.handle(), opened(stream, None), never(), |_| {
            Err(eyre!("No space left on device"))
        })
        .unwrap_err();
        assert!(matches!(error, StreamError::Write(_)));
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !dropped.load(Ordering::SeqCst) {
            assert!(
                std::time::Instant::now() < deadline,
                "the request is still open"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        // And a file that cannot be created is the same refusal, before any request.
        let missing = tempfile::tempdir().unwrap().path().join("gone");
        let asked = Arc::new(AtomicBool::new(false));
        let open = {
            let asked = asked.clone();
            async move {
                asked.store(true, Ordering::SeqCst);
                Ok((futures::stream::empty::<Result<Vec<u8>, String>>(), None))
            }
        };
        let error =
            stream_to_temp(rt.handle(), Some(&missing), None, open, &unstopped()).unwrap_err();
        assert!(matches!(error, StreamError::Write(_)));
        assert!(!asked.load(Ordering::SeqCst));
    }

    /// A stop ends the download between chunks, and while the store is silent, and
    /// either way the partial file goes.
    #[test]
    fn a_stop_ends_the_download_and_removes_the_file() {
        let rt = runtime();
        let dir = tempfile::tempdir().unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let stopped = crate::unfinished::Unfinished::default().writer(stop.clone());

        let stream = {
            let stop = stop.clone();
            futures::stream::iter(0..100).map(move |i| {
                if i == 3 {
                    stop.store(true, Ordering::SeqCst);
                }
                Ok::<_, String>(chunk(i, CHUNK))
            })
        };
        let error = stream_to_temp(
            rt.handle(),
            Some(dir.path()),
            None,
            opened(stream, None),
            &stopped,
        )
        .unwrap_err();
        assert!(matches!(error, StreamError::Cut), "{error:?}");
        assert_eq!(files_in(dir.path()), 0);

        // One chunk, then nothing: a store that has stopped answering.
        stop.store(false, Ordering::SeqCst);
        let stream = futures::stream::iter(vec![Ok::<_, String>(chunk(0, CHUNK))])
            .chain(futures::stream::pending());
        let stopper = {
            let stop = stop.clone();
            std::thread::spawn(move || {
                std::thread::sleep(std::time::Duration::from_millis(100));
                stop.store(true, Ordering::SeqCst);
            })
        };
        let began = std::time::Instant::now();
        let error = stream_to_temp(
            rt.handle(),
            Some(dir.path()),
            None,
            opened(stream, None),
            &stopped,
        )
        .unwrap_err();
        stopper.join().unwrap();
        assert!(matches!(error, StreamError::Cut), "{error:?}");
        assert!(began.elapsed() < std::time::Duration::from_secs(5));
        assert_eq!(files_in(dir.path()), 0);
    }

    /// A runtime shut down mid-transfer, as quitting does, cuts the download off: the
    /// waiting thread gets an error rather than a panic or a short file, and the file
    /// goes.
    #[test]
    fn a_shutdown_mid_transfer_cuts_it_off() {
        let rt = runtime();
        let dir = tempfile::tempdir().unwrap();
        let stream = futures::stream::iter(vec![Ok::<_, String>(chunk(0, CHUNK))])
            .chain(futures::stream::pending());
        let handle = rt.handle().clone();
        let path = dir.path().to_path_buf();
        let waiter = std::thread::spawn(move || {
            stream_to_temp(
                &handle,
                Some(&path),
                None,
                opened(stream, None),
                &unstopped(),
            )
        });
        // Mid-transfer: the first chunk is on disk and the store has gone quiet.
        while !std::fs::read_dir(dir.path())
            .unwrap()
            .any(|f| f.unwrap().metadata().unwrap().len() == CHUNK as u64)
        {
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        rt.shutdown_background();
        let error = waiter.join().expect("no panic").unwrap_err();
        assert!(matches!(error, StreamError::Cut), "{error:?}");
        assert_eq!(files_in(dir.path()), 0);
    }
}

#[cfg(all(test, feature = "http"))]
mod read_tests {
    use super::*;
    use std::io::Read;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    /// Hands out `chunks` one per read, then blocks until `release` sends or is
    /// dropped, then ends. Counts its reads, and says when it is dropped.
    struct Source {
        chunks: std::vec::IntoIter<Vec<u8>>,
        release: Option<std::sync::mpsc::Receiver<()>>,
        reads: Arc<AtomicUsize>,
        dropped: Arc<AtomicBool>,
    }

    impl Read for Source {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            if let Some(chunk) = self.chunks.next() {
                buf[..chunk.len()].copy_from_slice(&chunk);
                return Ok(chunk.len());
            }
            if let Some(release) = self.release.take() {
                let _ = release.recv();
            }
            Ok(0)
        }
    }

    impl Drop for Source {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }

    fn source(chunks: Vec<Vec<u8>>, release: Option<std::sync::mpsc::Receiver<()>>) -> Source {
        Source {
            chunks: chunks.into_iter(),
            release,
            reads: Arc::default(),
            dropped: Arc::default(),
        }
    }

    fn files_in(dir: &Path) -> usize {
        std::fs::read_dir(dir).unwrap().count()
    }

    #[track_caller]
    fn wait_until(what: &str, done: impl Fn() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !done() {
            assert!(Instant::now() < deadline, "{what}");
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    /// Many reads land in order, byte for byte, and a length given is held to.
    #[test]
    fn a_reader_lands_whole_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let chunks = (0..40u8)
            .map(|i| vec![i; 1000 + usize::from(i) * 13])
            .collect::<Vec<_>>();
        let whole = chunks.concat();
        let len = whole.len() as u64;
        let reader = source(chunks.clone(), None);
        let file = read_to_temp(
            Some(dir.path()),
            Some("csv"),
            move || Ok((reader, Some(len))),
            &Writer::default(),
        )
        .unwrap();
        assert_eq!(std::fs::read(file.path()).unwrap(), whole);
        assert!(file.path().to_string_lossy().ends_with(".csv"));

        let reader = source(chunks, None);
        let error = read_to_temp(
            Some(dir.path()),
            None,
            move || Ok((reader, Some(len + 1))),
            &Writer::default(),
        )
        .unwrap_err();
        assert!(matches!(error, StreamError::Short { .. }), "{error:?}");
        drop(file);
        assert_eq!(files_in(dir.path()), 0);
    }

    /// A refused request and a read that fails each end in their error, with no file.
    #[test]
    fn a_refusal_or_failed_read_leaves_no_file() {
        struct Reset(bool);
        impl Read for Reset {
            fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
                if std::mem::replace(&mut self.0, true) {
                    return Err(std::io::ErrorKind::ConnectionReset.into());
                }
                buf[..3].copy_from_slice(b"a,b");
                Ok(3)
            }
        }

        let dir = tempfile::tempdir().unwrap();
        let error = read_to_temp(
            Some(dir.path()),
            None,
            || Err::<(Reset, _), _>("Server returned 404 Not Found.".to_string()),
            &Writer::default(),
        )
        .unwrap_err();
        assert!(
            matches!(&error, StreamError::Open(e) if e.contains("404")),
            "{error:?}"
        );
        let error = read_to_temp(
            Some(dir.path()),
            None,
            || Ok((Reset(false), None)),
            &Writer::default(),
        )
        .unwrap_err();
        assert!(matches!(error, StreamError::Read(_)), "{error:?}");
        assert_eq!(files_in(dir.path()), 0);
    }

    /// A slow disk holds the reads back: never more than the queue ahead.
    #[test]
    fn a_slow_writer_holds_the_reads_back() {
        let reader = source((0..64).map(|i| vec![i; 1024]).collect(), None);
        let reads = reader.reads.clone();
        let mut written = 0usize;
        let mut ahead = 0usize;
        let total = read_into(
            move || Ok((reader, None)),
            || false,
            |_| {
                std::thread::sleep(Duration::from_millis(2));
                written += 1;
                ahead = ahead.max(reads.load(Ordering::SeqCst) - written);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(total, 64 * 1024);
        // The queue, one read waiting to go into it and the one being written.
        assert!(ahead <= QUEUED_CHUNKS + 2, "read {ahead} chunks ahead");
    }

    /// A server gone quiet mid-transfer: a stop ends the download promptly and
    /// removes the file, and the reading thread lets go at its next chunk.
    #[test]
    fn a_stop_while_the_server_is_silent_ends_it() {
        let dir = tempfile::tempdir().unwrap();
        let (release, held) = std::sync::mpsc::channel();
        let reader = source(vec![vec![1; 1024]], Some(held));
        let dropped = reader.dropped.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let stopper = {
            let stop = stop.clone();
            let dir = dir.path().to_path_buf();
            std::thread::spawn(move || {
                // Once the first chunk is on disk, the server has gone quiet. The size
                // comes from the file, not its directory entry: Windows updates the
                // entry's only when the writer closes it.
                let deadline = Instant::now() + Duration::from_secs(5);
                let landed = loop {
                    let sizes = std::fs::read_dir(&dir)
                        .unwrap()
                        .filter_map(|f| std::fs::metadata(f.unwrap().path()).ok())
                        .map(|m| m.len());
                    if sizes.into_iter().any(|len| len == 1024) {
                        break true;
                    }
                    if Instant::now() > deadline {
                        break false;
                    }
                    std::thread::sleep(Duration::from_millis(5));
                };
                // Stopped either way, so a chunk that never lands fails the test
                // rather than leaving the read waiting on the server for good.
                stop.store(true, Ordering::SeqCst);
                (landed, Instant::now())
            })
        };
        let error = read_to_temp(
            Some(dir.path()),
            None,
            move || Ok((reader, None)),
            &crate::unfinished::Unfinished::default().writer(stop),
        )
        .unwrap_err();
        let (landed, stopped_at) = stopper.join().unwrap();
        assert!(landed, "the first chunk landed");
        assert!(matches!(error, StreamError::Cut), "{error:?}");
        assert!(stopped_at.elapsed() < Duration::from_secs(2));
        assert_eq!(files_in(dir.path()), 0);

        assert!(
            !dropped.load(Ordering::SeqCst),
            "still waiting on the server"
        );
        release.send(()).unwrap();
        wait_until("the reader was let go", || dropped.load(Ordering::SeqCst));
    }
}

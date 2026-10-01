//! Remote objects streamed to local files.
//!
//! A store answers a GET with a stream of chunks. The stream is polled on the app's
//! runtime and its chunks are written on the thread that asked, never on a runtime
//! worker, with at most [`QUEUED_CHUNKS`] waiting between the two: a full queue stops
//! the stream being polled, so a slow disk holds the transfer back instead of the
//! object piling up in memory.

use color_eyre::Result;
use color_eyre::eyre::eyre;
use std::path::Path;
use std::sync::Arc;

/// A downloaded file, removed from disk when its last holder drops it: the app keeps
/// one to read the file again, the dataset scanning it keeps another, and an event
/// carrying one that is never handled removes it as it drops.
#[derive(Debug, Clone)]
pub struct TempDownload(Arc<tempfile::TempPath>);

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
        TempDownload(Arc::new(file.into_temp_path()))
    }

    pub fn path(&self) -> &Path {
        &self.0
    }
}

/// Chunks queued between a store's stream and the thread writing them.
#[cfg(feature = "cloud")]
pub const QUEUED_CHUNKS: usize = 4;

/// How often a stream that has gone quiet checks whether it was stopped.
#[cfg(feature = "cloud")]
const STALL_CHECK: std::time::Duration = std::time::Duration::from_millis(100);

/// What opening a stream answers: the stream and its length, when the store gave
/// one, or why it could not be opened.
#[cfg(feature = "cloud")]
pub type Opened<S> = std::result::Result<(S, Option<u64>), String>;

/// Why a stream did not arrive whole.
#[cfg(feature = "cloud")]
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

/// What the runtime side hands the writer, in order.
#[cfg(feature = "cloud")]
enum Piece<B> {
    /// The request was answered; the length, when the store gave one.
    Opened(Option<u64>),
    Refused(String),
    Chunk(B),
    Failed(String),
    End,
}

/// Run `open` on `runtime` and hand each chunk of the stream it answers with to
/// `write` on this thread, in order. Returns the bytes written.
///
/// `open` gives the stream and its length, when known. `stop` is checked between
/// chunks, and every [`STALL_CHECK`] while the store is silent. Ends in an error,
/// never a short success, when the open or a chunk fails, `write` refuses one, `stop`
/// says so, or the runtime shuts down mid-transfer; the request is dropped with the
/// stream then. Must not be called on a runtime worker: it blocks.
#[cfg(feature = "cloud")]
pub fn stream_into<O, S, B, E>(
    runtime: &tokio::runtime::Handle,
    open: O,
    stop: impl Fn() -> bool + Clone + Send + Sync + 'static,
    mut write: impl FnMut(&[u8]) -> Result<()>,
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
        let (stream, len) = match until_stopped(open, &stopped).await {
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
            let piece = match until_stopped(next, &stopped).await {
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
    let mut expected = None;
    let mut written = 0u64;
    loop {
        if stop() {
            return Err(StreamError::Cut);
        }
        match rx.blocking_recv() {
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
/// [`TempDownload::create`] names it. Any failure, and a stop, removes the partial
/// file before this returns.
#[cfg(feature = "cloud")]
pub fn stream_to_temp<O, S, B, E>(
    runtime: &tokio::runtime::Handle,
    dir: Option<&Path>,
    extension: Option<&str>,
    open: O,
    stop: impl Fn() -> bool + Clone + Send + Sync + 'static,
) -> std::result::Result<TempDownload, StreamError>
where
    O: std::future::Future<Output = Opened<S>> + Send + 'static,
    S: futures::Stream<Item = std::result::Result<B, E>> + Send + 'static,
    B: AsRef<[u8]> + Send + 'static,
    E: std::fmt::Display,
{
    use std::io::Write;

    let mut file = TempDownload::create(dir, extension).map_err(StreamError::Write)?;
    let unwritable = |e: std::io::Error| eyre!("Could not write the downloaded file: {e}");
    stream_into(runtime, open, stop, |chunk| {
        file.write_all(chunk).map_err(unwritable)
    })?;
    file.flush()
        .map_err(|e| StreamError::Write(unwritable(e)))?;
    Ok(TempDownload::keep(file))
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

    async fn opened<S>(stream: S, len: Option<u64>) -> Opened<S> {
        Ok((stream, len))
    }

    fn never() -> impl Fn() -> bool + Clone + Send + Sync + 'static {
        || false
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
            never(),
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
            never(),
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
            stream_to_temp(rt.handle(), Some(dir.path()), None, refused, never()).unwrap_err();
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
            never(),
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
        let error = stream_to_temp(rt.handle(), Some(&missing), None, open, never()).unwrap_err();
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
        let stopped = {
            let stop = stop.clone();
            move || stop.load(Ordering::SeqCst)
        };

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
            stopped.clone(),
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
            stopped,
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
        let (started_tx, started_rx) = std::sync::mpsc::channel();
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
                move || {
                    let _ = started_tx.send(());
                    false
                },
            )
        });
        started_rx.recv().unwrap();
        rt.shutdown_background();
        let error = waiter.join().expect("no panic").unwrap_err();
        assert!(matches!(error, StreamError::Cut), "{error:?}");
        assert_eq!(files_in(dir.path()), 0);
    }
}

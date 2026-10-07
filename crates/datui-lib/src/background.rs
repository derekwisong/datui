//! What background work carries and owes: the buffer collect in flight, the row count,
//! and the answers a worker sends however it ends.

use std::path::PathBuf;
use std::sync::{Arc, mpsc::Sender};

use polars::datatypes::AnyValue;
use polars::prelude::LazyFrame;

use crate::table::DataTableState;
use crate::{AppEvent, logging};

/// The buffer collect in flight, the payload of [`crate::jobs::Job::Rows`]. A collect already
/// covering the view is left to land rather than restarted (the first frame's
/// recollect would redo a row-group download), provided its job is current and the
/// data unchanged (`len_generation` moves with every change to `lf`). Whether
/// anyone waits on it is the job's keys: a load-ahead starts unwaited, and a scroll
/// that finds its rows coming waits on it.
#[derive(Debug, Clone, Copy)]
pub(crate) struct InflightCollect {
    /// When the request went out and how many files it reads, for the Last page
    /// measurement; `files` is `None` when Polars scans the whole frame.
    pub(crate) began: std::time::Instant,
    pub(crate) files: Option<usize>,
    pub(crate) dataset: u64,
    /// The columns it reads ([`Self::columns_of`]); another projection needs other rows.
    pub(crate) columns: u64,
    pub(crate) start: usize,
    pub(crate) end: usize,
}

impl InflightCollect {
    /// The columns a read of `state` projects, as a hash: kept `Copy`.
    pub(crate) fn columns_of(state: &DataTableState) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        state.get_column_order().hash(&mut hasher);
        hasher.finish()
    }

    /// A read of rows `start..end` of no data in particular, for tests.
    #[cfg(test)]
    pub(crate) fn for_tests(start: usize, end: usize) -> Self {
        Self {
            began: std::time::Instant::now(),
            files: None,
            dataset: 0,
            columns: 0,
            start,
            end,
        }
    }

    pub(crate) fn covers(&self, state: &DataTableState) -> bool {
        // The view ends at the data when there is less than a screen of it.
        let bound = state.num_rows_if_valid().unwrap_or(usize::MAX);
        let view_end = (state.start_row() + state.visible_rows).min(bound);
        // A row group being stitched on to the buffer covers the view with it.
        let (mut start, mut end) = (self.start, self.end);
        let (held_start, held_end) = (state.buffered_start(), state.buffered_end());
        if state.stitches_buffer() && (start == held_end || end == held_start) {
            start = start.min(held_start);
            end = end.max(held_end);
        }
        self.dataset == state.len_generation()
            && self.columns == Self::columns_of(state)
            && start <= state.start_row()
            && view_end <= end
    }
}

/// A frame's exact row count, run off the UI thread: the footer sum for a pristine
/// local Parquet hive or a remote multi-file dataset, else `len()`. Results for a
/// changed `len_generation` are dropped.
pub(crate) struct LenCount {
    pub(crate) len_generation: u64,
    pub(crate) count_dir: Option<PathBuf>,
    pub(crate) files: Option<crate::table::FileCounter>,
    /// The view's own count, from a source that runs the view (a SQLite table).
    pub(crate) counter: Option<crate::formats::pushdown::Counter>,
    pub(crate) lf: LazyFrame,
    pub(crate) streaming: bool,
    /// The open's meter: a local directory's count re-reads every footer, tallied with
    /// the open's pass.
    pub(crate) meter: Arc<crate::measurements::Meter>,
    /// Footers read of how many, for the progress line; cancelling stops the count (Esc).
    pub(crate) progress: Arc<crate::formats::schema_union::FooterProgress>,
}

/// A count, and for a remote dataset of many files the row groups it was summed from.
pub(crate) struct Counted {
    pub(crate) rows: usize,
    file_row_groups: Option<Vec<Vec<usize>>>,
}

impl From<usize> for Counted {
    fn from(rows: usize) -> Self {
        Counted {
            rows,
            file_row_groups: None,
        }
    }
}

impl LenCount {
    pub(crate) fn for_state(state: &DataTableState) -> Self {
        Self {
            len_generation: state.len_generation(),
            count_dir: state.parquet_count_dir(),
            files: state.remote_files_counter(),
            counter: state.source_counter(),
            meter: state.measurements().clone(),
            lf: state.lf_clone(),
            streaming: state.polars_streaming_enabled(),
            // Store footers are round trips, many awaited at once; disk footers go a wave at a
            // time.
            progress: Arc::new(if state.is_remote_source() {
                crate::formats::schema_union::FooterProgress::counting()
            } else {
                crate::formats::schema_union::FooterProgress::default()
            }),
        }
    }

    /// Whether this count reads only footers, so it can run beside a buffer read.
    pub(crate) fn reads_footers(&self) -> bool {
        self.files.is_some() || self.count_dir.is_some()
    }

    /// Count the rows. Blocks; `Err` when the count could not be taken.
    pub(crate) fn run(&self) -> Result<Counted, ()> {
        let kind = if self.reads_footers() {
            "footers"
        } else {
            "scan"
        };
        let began = std::time::Instant::now();
        log::debug!(target: "datui", "row count {} ({kind}): started", self.len_generation);
        let counted = self.count();
        match &counted {
            Ok(counted) => log::debug!(
                target: "datui",
                "row count {} ({kind}): {} rows in {:.1?}",
                self.len_generation,
                counted.rows,
                began.elapsed()
            ),
            Err(()) => log::debug!(
                target: "datui",
                "row count {} ({kind}): failed after {:.1?}",
                self.len_generation,
                began.elapsed()
            ),
        }
        counted
    }

    fn count(&self) -> Result<Counted, ()> {
        if let Some(counter) = &self.counter {
            return counter()
                .map(Counted::from)
                .map_err(|e| log::warn!(target: "datui", "row count failed: {e}"));
        }
        // A dataset's footers, many at once. If one fails, the scan counts itself; a stopped
        // count does not.
        if let Some(count) = &self.files {
            match count(&self.progress) {
                Ok(groups) => {
                    return Ok(Counted {
                        rows: groups.iter().flatten().sum(),
                        file_row_groups: Some(groups),
                    });
                }
                Err(_) if self.progress.is_cancelled() => return Err(()),
                Err(_) => {}
            }
        }
        match &self.count_dir {
            Some(dir) => crate::formats::dataset_files::LocalFiles::new(dir)
                .count_rows(&self.meter, &self.progress)
                .map(Counted::from)
                .map_err(|e| log::warn!(target: "datui", "row count failed: {e:#}")),
            None => {
                match crate::statistics::collect_lazy(
                    crate::table::row_count_lf(&self.lf),
                    self.streaming,
                ) {
                    Ok(df) => Ok(match df.get(0) {
                        Some(col) => match col.first() {
                            Some(AnyValue::UInt64(n)) => *n as usize,
                            _ => 0,
                        },
                        None => 0,
                    }
                    .into()),
                    Err(e) => {
                        log::warn!(target: "datui", "row count failed: {e}");
                        Err(())
                    }
                }
            }
        }
    }

    /// The count once a collect of `requested` rows from `start` returned `returned`. A
    /// short read that began inside the data (at the top, or finding a row) ran off its
    /// end, giving the total. A full read, or a deep empty slice that may lie past the
    /// data, leaves the count to `run`.
    pub(crate) fn after_collect(
        &self,
        start: usize,
        returned: usize,
        requested: usize,
    ) -> Result<Counted, ()> {
        if returned < requested && (start == 0 || returned > 0) {
            Ok((start + returned).into())
        } else {
            self.run()
        }
    }

    /// Report the count. A failure leaves the total provisional and retried on a later
    /// interaction.
    fn send(&self, counted: Result<Counted, ()>, tx: &Sender<AppEvent>) {
        let _ = tx.send(match counted {
            Ok(counted) => AppEvent::BackgroundLenReady {
                len_generation: self.len_generation,
                num_rows: counted.rows,
                file_row_groups: counted.file_row_groups,
            },
            Err(()) => AppEvent::BackgroundLenFailed {
                len_generation: self.len_generation,
            },
        });
    }
}

/// A started count, owing `len_count_inflight` an answer however its worker ends: a
/// panic, or a failure of the collect it rides in, reports failed. Unanswered, the
/// marker would stand for the session, spinning, with `End` waiting on nothing.
pub(crate) struct OwedCount {
    job: Option<LenCount>,
    tx: Sender<AppEvent>,
}

impl OwedCount {
    pub(crate) fn new(job: LenCount, tx: Sender<AppEvent>) -> Self {
        Self { job: Some(job), tx }
    }

    /// Count with `count` and report it, a panic as a failure.
    pub(crate) fn answer(mut self, count: impl FnOnce(&LenCount) -> Result<Counted, ()>) {
        if let Some(job) = self.job.take() {
            let counted = logging::catch_panic(|| count(&job)).unwrap_or(Err(()));
            job.send(counted, &self.tx);
        }
    }
}

impl Drop for OwedCount {
    fn drop(&mut self) {
        if let Some(job) = self.job.take() {
            job.send(Err(()), &self.tx);
        }
    }
}

/// The answer a home worker owes its in-flight marker, sent instead if it panics
/// first; unanswered, the marker stands for the session. Not a [`crate::jobs::Jobs`] job:
/// keyed by place and `home_generation`, no lease, no keys. The panic hook logs and
/// flashes the panic itself.
pub(crate) struct OwedAnswer {
    pub(crate) tx: Sender<AppEvent>,
    pub(crate) instead: Option<AppEvent>,
    #[cfg(test)]
    pub(crate) dies: bool,
}

impl OwedAnswer {
    /// Run the worker, which sends its own answer.
    pub(crate) fn run(mut self, work: impl FnOnce()) {
        #[cfg(test)]
        if self.dies {
            panic!("worker died");
        }
        work();
        self.instead = None;
    }
}

impl Drop for OwedAnswer {
    fn drop(&mut self) {
        if let Some(instead) = self.instead.take() {
            let _ = self.tx.send(instead);
        }
    }
}

/// Cache writes an open makes for home (the recent, the shape): off the UI thread,
/// counted so the home listing waits for them, else a quick `q` could list the
/// cache before the recent is in.
#[derive(Clone, Default)]
pub(crate) struct CacheWrites(Arc<(std::sync::Mutex<usize>, std::sync::Condvar)>);

impl CacheWrites {
    /// The longest a listing waits: past the history lock's timeout, so a giving-up
    /// write has given up.
    const SETTLE: std::time::Duration = std::time::Duration::from_secs(5);

    /// Write on a thread of its own, counted until it ends, panic or not.
    pub(crate) fn spawn(&self, write: impl FnOnce() + Send + 'static) {
        *self.0.0.lock().unwrap_or_else(|e| e.into_inner()) += 1;
        let done = WriteDone(self.clone());
        std::thread::spawn(move || {
            let _done = done;
            write();
        });
    }

    /// Wait until no counted write is in flight, or [`Self::SETTLE`] passes. Called on a
    /// worker, never the UI thread.
    pub(crate) fn settle(&self) {
        let (count, ended) = &*self.0;
        let count = count.lock().unwrap_or_else(|e| e.into_inner());
        let _ = ended.wait_timeout_while(count, Self::SETTLE, |n| *n > 0);
    }
}

impl std::fmt::Debug for CacheWrites {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CacheWrites")
    }
}

struct WriteDone(CacheWrites);

impl Drop for WriteDone {
    fn drop(&mut self) {
        let (count, ended) = &*(self.0).0;
        *count.lock().unwrap_or_else(|e| e.into_inner()) -= 1;
        ended.notify_all();
    }
}

#[cfg(test)]
mod cache_writes_tests {
    use super::CacheWrites;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, mpsc};

    #[test]
    fn settle_waits_for_a_write_in_flight() {
        let writes = CacheWrites::default();
        let (go, wait) = mpsc::channel::<()>();
        let written = Arc::new(AtomicBool::new(false));
        let flag = written.clone();
        writes.spawn(move || {
            let _ = wait.recv();
            flag.store(true, Ordering::SeqCst);
        });
        let settled = {
            let writes = writes.clone();
            std::thread::spawn(move || writes.settle())
        };
        // Proving a wait does not end takes a while to give it the chance.
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert!(!settled.is_finished(), "settled while the write was held");
        go.send(()).unwrap();
        settled.join().unwrap();
        assert!(written.load(Ordering::SeqCst));
    }

    #[test]
    fn a_write_that_panics_still_ends() {
        let writes = CacheWrites::default();
        writes.spawn(|| panic!("write died"));
        let started = std::time::Instant::now();
        writes.settle();
        assert!(started.elapsed() < CacheWrites::SETTLE);
    }
}

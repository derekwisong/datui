//! What opening a dataset cost, measured rather than guessed.
//!
//! Every number here is one datui produced itself: it timed its own listing, counted
//! its own requests, and added up the bytes it received. Where Polars does the reading
//! datui cannot count the requests or the bytes, and this module records nothing rather
//! than record a figure it cannot stand behind. That is why so much of what it holds is
//! optional: a stretch reports a file count, a request count and a byte count only
//! where it has one, and `docs/user-guide/dataset-info.md` says which routes have
//! which.
//!
//! The tallies are written by the threads doing the reading, which is why they are
//! atomic, and read by the render, which is why nothing here ever blocks.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

/// One stretch of work, and what it cost.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cost {
    /// How long it took, from the first request to the last answer.
    pub took: Duration,
    /// The data files found, or the footers read — `None` where the stretch did the
    /// work without ever learning a count.
    ///
    /// Every route that reaches a user today knows its count. The one that does not is
    /// the walk to a prefix's two ends, which never lists what lies between them; that
    /// route cannot currently be reached (see `schema_from_one_cloud_hive`), and this
    /// is what keeps it from reporting the two ends as the size of the dataset if it
    /// ever is.
    pub files: Option<usize>,
    /// Requests datui made itself and can count, with the bytes they returned.
    ///
    /// `None` on every listing, and not only the local ones: a directory is read rather
    /// than requested, and a remote prefix is one call whose round trips happen inside
    /// the object store, which does not say how many there were. A figure of zero would
    /// read as "no data moved" rather than "not measured here".
    pub over_the_wire: Option<OverTheWire>,
}

/// Two stretches' wire figures as one.
///
/// The requests add. The bytes add only where both stretches weighed theirs: one that
/// did not leaves the pair unable to say what its requests brought back, and carrying
/// the other's figure forward would present it as the bytes behind all of them.
fn combine_wire(a: OverTheWire, b: OverTheWire) -> OverTheWire {
    OverTheWire {
        requests: a.requests + b.requests,
        bytes: a.bytes.zip(b.bytes).map(|(x, y)| x + y),
    }
}

/// Every stretch added up: how long the open has taken, and what it asked for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Total {
    /// The stretches' times added together.
    pub took: Duration,
    /// The requests and bytes of the stretches that counted any, or `None` if none did.
    pub over_the_wire: Option<OverTheWire>,
}

/// What datui asked for over a network, exactly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OverTheWire {
    /// Requests datui issued.
    pub requests: usize,
    /// Bytes those requests returned, where datui counted them.
    ///
    /// Nothing reports requests without weighing them today — only the footer reads
    /// report requests at all, and they weigh what comes back. It is optional so that a
    /// stretch which one day counts round trips it does not read cannot be made to
    /// claim a figure of zero, which would say they arrived empty.
    pub bytes: Option<u64>,
}

/// A running tally of one kind of work.
///
/// `ran` is not redundant with the other fields: a listing that returned nothing still
/// took time and is still a measurement, so zero cannot stand in for "this never
/// happened".
#[derive(Debug, Default)]
struct Tally {
    ran: AtomicBool,
    nanos: AtomicU64,
    files: AtomicUsize,
    /// Counted as the requests are made, and still climbing while a pass runs.
    live_requests: AtomicUsize,
    live_bytes: AtomicU64,
    /// Whether anything recorded here counted bytes at all.
    counted_bytes: AtomicBool,
    /// What those counters stood at when a pass last finished, which is what is shown.
    ///
    /// Separate from the live pair because the time and the file count only move when a
    /// pass ends: showing the live figures beside them would put a pass's requests next
    /// to the previous pass's time, and read as forty-one requests over two files.
    counted: AtomicBool,
    requests: AtomicUsize,
    bytes: AtomicU64,
    /// Whether any stretch recorded here knew a count at all.
    counted_files: AtomicBool,
}

impl Tally {
    /// Record a finished stretch of work, adding it to whatever this tally already
    /// holds.
    ///
    /// Adding rather than replacing because one kind of work can happen in more than
    /// one stretch. A cloud dataset past a wave of concurrent reads opens on two
    /// footers and then reads every footer behind the open; a dataset whose open could
    /// not settle its row count reads every footer again to take it. What the footers
    /// cost is all of those passes, re-reads included, which is why the count this
    /// keeps is footers read rather than files. A dataset being opened starts from a
    /// tally that holds nothing, so nothing is carried over from the last one.
    fn record(&self, took: Duration, files: Option<usize>, over_the_wire: bool) {
        self.nanos.fetch_add(
            took.as_nanos().min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
        if let Some(files) = files {
            self.files.fetch_add(files, Ordering::Relaxed);
            self.counted_files.store(true, Ordering::Relaxed);
        }
        if over_the_wire {
            // Read from the live counters here rather than taken from the caller: the
            // caller would have had to read them a moment earlier, and a request landing
            // in between would be written back out again.
            // `fetch_max`, not `store`: were two metered passes ever to finish at
            // once, whichever read the live counter first would write its lower figure
            // back over the higher one. Today they cannot — a count is refused while
            // the pass behind an open is still reading, and every other pair is
            // serialized — so this guards an interleaving the rest of the design
            // currently forbids. It costs nothing, and within a dataset these counters
            // only climb, so taking the larger is correct whether or not that holds.
            self.requests.fetch_max(
                self.live_requests.load(Ordering::Relaxed),
                Ordering::Relaxed,
            );
            self.bytes
                .fetch_max(self.live_bytes.load(Ordering::Relaxed), Ordering::Relaxed);
            self.counted.store(true, Ordering::Relaxed);
        }
        // Last, and released, so a render that sees `ran` sees every field behind it.
        self.ran.store(true, Ordering::Release);
    }

    /// Record a stretch in place of whatever this tally held, rather than adding to it.
    fn replace(&self, took: Duration, files: Option<usize>) {
        self.nanos.store(
            took.as_nanos().min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
        match files {
            Some(files) => {
                self.files.store(files, Ordering::Relaxed);
                self.counted_files.store(true, Ordering::Relaxed);
            }
            None => self.counted_files.store(false, Ordering::Relaxed),
        }
        self.ran.store(true, Ordering::Release);
    }

    /// One more request, and the bytes it returned. Called from the threads doing the
    /// reading, once per request, before the stretch is recorded.
    fn request(&self, bytes: u64) {
        self.live_requests.fetch_add(1, Ordering::Relaxed);
        self.live_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.counted_bytes.store(true, Ordering::Relaxed);
    }

    /// A whole pass's worth of requests at once, for work counted against a meter of
    /// its own and then folded in here.
    fn add_requests(&self, wire: OverTheWire) {
        self.live_requests
            .fetch_add(wire.requests, Ordering::Relaxed);
        if let Some(bytes) = wire.bytes {
            self.live_bytes.fetch_add(bytes, Ordering::Relaxed);
            self.counted_bytes.store(true, Ordering::Relaxed);
        }
    }

    fn cost(&self) -> Option<Cost> {
        if !self.ran.load(Ordering::Acquire) {
            return None;
        }
        Some(Cost {
            took: Duration::from_nanos(self.nanos.load(Ordering::Relaxed)),
            files: self
                .counted_files
                .load(Ordering::Relaxed)
                .then(|| self.files.load(Ordering::Relaxed)),
            over_the_wire: self.counted.load(Ordering::Relaxed).then(|| OverTheWire {
                requests: self.requests.load(Ordering::Relaxed),
                bytes: self
                    .counted_bytes
                    .load(Ordering::Relaxed)
                    .then(|| self.bytes.load(Ordering::Relaxed)),
            }),
        })
    }
}

/// What an open reports as it goes: how far its footer pass has got, and what the work
/// has cost so far.
///
/// The two travel together down every route an open can take, so they are handed down
/// together. Both are shared with the threads doing the reading, and both belong to the
/// open that created them — see [`Meter`] for why a new open builds new ones rather
/// than clearing these.
#[derive(Debug, Clone, Default)]
pub struct OpenReport {
    /// How far the footer pass has got, for the loading screen.
    pub progress: std::sync::Arc<crate::schema_union::FooterProgress>,
    /// What the work has cost, for the Info panel.
    pub meter: std::sync::Arc<Meter>,
    /// Where to look for what a previous open of this dataset learned, and where to
    /// leave what this one learns. `None` for a route with nowhere to keep it, and for
    /// the tests that do not care.
    ///
    /// It travels with the other two because it belongs to the same moment — an open
    /// reports what it is doing, records what it cost, and remembers what it found, and
    /// all three are handed down the same routes.
    pub remembered: Option<crate::cache::CacheManager>,
    /// Where what is remembered for the home screen is written, off the open's path:
    /// the app's, which the home listing settles before it reads.
    pub(crate) writes: crate::background::CacheWrites,
}

/// What the open on screen cost, as it is measured.
///
/// Shared with the threads that do the listing and the footer reads, which is why it is
/// held behind an `Arc` and written through `&self`.
///
/// There is no way to clear one. Opening a dataset builds a new meter instead, for the
/// reason the footer counter does: abandoning a load cancels nothing, so the reads of
/// the directory that was walked away from are still running, and a meter they still held
/// would go on adding their figures to the next dataset's.
#[derive(Debug, Default)]
pub struct Meter {
    listing: Tally,
    footers: Tally,
    /// The most recent page of rows, replacing rather than adding: this one says what
    /// the page on screen cost, not what every page since the open came to.
    last_page: Tally,
    /// Whether a pass to settle the row count has already been counted.
    counted_rows: AtomicBool,
}

impl Meter {
    /// Finding the dataset's files took `took` and returned `files` of them.
    ///
    /// `over_the_wire` is always `false` here today, and the parameter is kept so the
    /// two stretches record the same way. No listing route can count its requests: a
    /// local walk makes none, and every remote one — the flat `list` and the
    /// level-by-level walk a glob uses alike — hands the paging to the object store,
    /// which does not say how many round trips it took.
    pub fn listed(&self, took: Duration, files: Option<usize>, over_the_wire: bool) {
        self.listing.record(took, files, over_the_wire);
    }

    /// A pass over `files` footers took `took`.
    ///
    /// `over_the_wire` publishes the requests counted by [`Self::footer_request`] since
    /// the meter was made. A pass that read from a disk passes `false`: there were no
    /// requests, and a zero would read as none having been needed.
    pub fn read_footers(&self, took: Duration, files: Option<usize>, over_the_wire: bool) {
        self.footers.record(took, files, over_the_wire);
    }

    /// A pass over `files` footers, run to settle the dataset's row count, took `took`.
    ///
    /// Recorded once and then never again, and the return says which happened. Counting
    /// runs whenever the row count is invalidated — clearing a filter does it, so a few
    /// minutes of exploring runs it several times — and those later passes are re-work
    /// on a dataset that is already open. Adding them would make a section headed by
    /// what opening the dataset cost climb for as long as the session lasted.
    pub fn counted_rows(
        &self,
        took: Duration,
        files: Option<usize>,
        wire: Option<OverTheWire>,
    ) -> bool {
        // Only for an open this meter measured, and checked before the one shot rather
        // than after it. A dataset opened by a route that reports nothing — a directory
        // handed straight to Polars because `single_spine_schema` is off — still has
        // its rows counted afterwards, and that count writing here would raise a
        // section out of nothing whose every figure is work done after the dataset was
        // already on screen.
        if self.listing.cost().is_none() {
            return false;
        }
        if self
            .counted_rows
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return false;
        }
        // Folded in only once the one-shot has been won. A pass that is declined must
        // leave the counters alone, or its requests would be published by whichever
        // pass records next.
        if let Some(w) = wire {
            self.footers.add_requests(w);
        }
        self.footers.record(took, files, wire.is_some());
        true
    }

    /// Reading the page now on screen took `took` and read `files` of the dataset's
    /// files.
    ///
    /// Replaces rather than adds: this is the cost of the page a user is looking at,
    /// and adding every page they have scrolled through would answer a question nobody
    /// asked. `files` is `None` where Polars was handed the whole scan and decided for
    /// itself what to read.
    ///
    /// No byte figure. Polars does this read and does not report what it fetched, and
    /// the row-group sizes the footers hold would give the size of whole row groups for
    /// every column — not the columns on screen, and not what crossed the wire. A
    /// number that wrong is worse than none.
    pub fn read_page(&self, took: Duration, files: Option<usize>) {
        self.last_page.replace(took, files);
    }

    /// What the page now on screen cost, or `None` before one has been read.
    pub fn last_page(&self) -> Option<Cost> {
        self.last_page.cost()
    }

    /// One footer request, and the bytes it returned. Counted as the reads happen; what
    /// they come to is published when the pass ends.
    pub fn footer_request(&self, bytes: u64) {
        self.footers.request(bytes);
    }

    /// What finding the files cost, or `None` if no listing has been measured.
    pub fn listing(&self) -> Option<Cost> {
        self.listing.cost()
    }

    /// What reading the footers cost, or `None` if no footer pass has been measured.
    pub fn footers(&self) -> Option<Cost> {
        self.footers.cost()
    }

    /// Everything measured so far, added up. `None` until something has been measured.
    ///
    /// No file count. The stretches count different things — the listing counts the
    /// dataset's files, the footer pass counts footers read, which on a staged open is
    /// more than there are files — so adding them gives a number that is not the size
    /// of anything, sitting under the same word the listing row uses for the size of
    /// the dataset.
    ///
    /// The wire figures are the sum of the stretches that counted them, and are `None`
    /// when no stretch did: a local dataset's total is a time, not a time and a
    /// pretence of nothing having been transferred.
    pub fn total(&self) -> Option<Total> {
        // Listing and footers only. The page is not part of opening the dataset — it is
        // what looking at one costs, and it changes every time the view moves.
        let parts: Vec<Cost> = [self.listing(), self.footers()]
            .into_iter()
            .flatten()
            .collect();
        // Two stretches or none. One stretch's total is that stretch, printed twice
        // under two labels — which a single remote object would do, having a footer to
        // read and nothing to list.
        if parts.len() < 2 {
            return None;
        }
        Some(Total {
            took: parts.iter().map(|p| p.took).sum(),
            over_the_wire: parts
                .iter()
                .filter_map(|p| p.over_the_wire)
                .reduce(combine_wire),
        })
    }
}

/// How long the event loop's own work takes: each frame drawn and each event handled,
/// for the debug overlay and the log.
#[derive(Debug, Default)]
pub struct LoopTimes {
    pub frames: Durations,
    pub handlers: Durations,
}

impl LoopTimes {
    /// One frame drawn. Every [`Durations::WINDOW`] frames the log hears both
    /// summaries at debug level.
    pub fn frame(&mut self, took: Duration) {
        self.frames.record(took);
        if self.frames.count.is_multiple_of(Durations::WINDOW as u64) {
            log::debug!(
                target: "datui",
                "frames {}; handlers {}",
                self.frames.summary(),
                self.handlers.summary()
            );
        }
    }

    /// One event handled, its key included.
    pub fn handler(&mut self, took: Duration) {
        self.handlers.record(took);
    }
}

/// The most recent durations of one kind of work, for its median, 99th percentile and
/// worst.
#[derive(Debug, Default)]
pub struct Durations {
    recent: std::collections::VecDeque<Duration>,
    count: u64,
}

impl Durations {
    /// How many of the latest durations the figures are over.
    pub const WINDOW: usize = 240;

    pub fn record(&mut self, took: Duration) {
        if self.recent.len() == Self::WINDOW {
            self.recent.pop_front();
        }
        self.recent.push_back(took);
        self.count += 1;
    }

    /// How many were ever recorded.
    pub fn count(&self) -> u64 {
        self.count
    }

    /// The duration `q` of the way up the recent ones (0.5 the median, 1.0 the worst).
    pub fn quantile(&self, q: f64) -> Option<Duration> {
        let mut sorted: Vec<Duration> = self.recent.iter().copied().collect();
        sorted.sort_unstable();
        let last = sorted.len().checked_sub(1)?;
        sorted.get(((last as f64) * q).round() as usize).copied()
    }

    /// `p50 1.2ms p99 3.4ms max 5.0ms`, or `-` before the first.
    pub fn summary(&self) -> String {
        let ms = |d: Duration| format!("{:.1}ms", d.as_secs_f64() * 1000.0);
        match (self.quantile(0.5), self.quantile(0.99), self.quantile(1.0)) {
            (Some(p50), Some(p99), Some(max)) => {
                format!("p50 {} p99 {} max {}", ms(p50), ms(p99), ms(max))
            }
            _ => "-".to_string(),
        }
    }
}

#[cfg(test)]
mod tests;

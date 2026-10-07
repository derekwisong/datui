//! What opening a dataset cost, measured rather than guessed: every figure is one datui
//! produced itself (its own listing times, request counts, byte totals). Where Polars
//! reads, datui cannot count, so nothing is recorded; hence the optional fields (see
//! `docs/user-guide/dataset-info.md`). Written atomically by reading threads, read by
//! the render without blocking.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

/// One stretch of work, and what it cost.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cost {
    /// How long it took, from the first request to the last answer.
    pub took: Duration,
    /// Data files found or footers read; `None` where the stretch never learned a count
    /// (only the unreachable two-ends walk, see `schema_from_one_cloud_hive`, so it never
    /// reports two files as the dataset's size).
    pub files: Option<usize>,
    /// Requests datui made and counted, with bytes returned. `None` on every listing (a
    /// directory is read, not requested; a remote prefix pages inside the object store), so
    /// zero never reads as "no data moved".
    pub over_the_wire: Option<OverTheWire>,
}

/// Two stretches' wire figures as one: requests add; bytes add only when both weighed
/// theirs.
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
    /// Bytes those requests returned, where counted; optional so a stretch can never claim
    /// zero for unweighed requests.
    pub bytes: Option<u64>,
}

/// A running tally of one kind of work. `ran` matters: an empty listing still took time
/// and is a measurement.
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
    /// The counters at the last finished pass, which are shown: time and file count move
    /// only at a pass's end, so live counters would pair one pass's requests with the
    /// previous pass's time.
    counted: AtomicBool,
    requests: AtomicUsize,
    bytes: AtomicU64,
    /// Whether any stretch recorded here knew a count at all.
    counted_files: AtomicBool,
}

impl Tally {
    /// Record a finished stretch, added to the tally: one kind of work can span stretches
    /// (a staged open's footer pass, a recount), so the count is footers read, not files.
    /// Each dataset starts from an empty tally.
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
            // Read from the live counters here, so a request landing meanwhile is not lost.
            // `fetch_max`: concurrent passes cannot finish together today, but the counters only
            // climb, so the larger is right regardless.
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

/// What an open reports as it goes: footer progress and cost so far, handed down every
/// route together and owned by the open that made them (see [`Meter`]).
#[derive(Debug, Clone, Default)]
pub struct OpenReport {
    /// How far the footer pass has got, for the loading screen.
    pub progress: std::sync::Arc<crate::formats::schema_union::FooterProgress>,
    /// What the work has cost, for the Info panel.
    pub meter: std::sync::Arc<Meter>,
    /// Where to find what a previous open learned and leave what this one learns; `None`
    /// for routes with nowhere to keep it, and for tests.
    pub remembered: Option<crate::cache::CacheManager>,
    /// Where what is remembered for the home screen is written, off the open's path:
    /// the app's, which the home listing settles before it reads.
    pub(crate) writes: crate::background::CacheWrites,
}

/// What the open on screen cost, as measured; shared with reading threads (`Arc`,
/// written through `&self`). Never cleared: each open builds a new one, since an
/// abandoned load's reads still run and would add to the next dataset's figures.
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
    /// Finding the files took `took` and found `files`. `over_the_wire` is always false
    /// today (local walks make no requests; remote listings page inside the object store),
    /// kept so both stretches record alike.
    pub fn listed(&self, took: Duration, files: Option<usize>, over_the_wire: bool) {
        self.listing.record(took, files, over_the_wire);
    }

    /// A pass over `files` footers took `took`. `over_the_wire` publishes requests counted
    /// by [`Self::footer_request`]; a disk pass passes false (no requests, not zero).
    pub fn read_footers(&self, took: Duration, files: Option<usize>, over_the_wire: bool) {
        self.footers.record(took, files, over_the_wire);
    }

    /// A count pass over `files` footers took `took`: recorded once only (the return says
    /// whether), since counts rerun on every invalidation and would inflate the open's
    /// cost.
    pub fn counted_rows(
        &self,
        took: Duration,
        files: Option<usize>,
        wire: Option<OverTheWire>,
    ) -> bool {
        // Only for an open this meter measured (a route reporting nothing, like Polars-handed
        // directories, must not gain a section of after-the-fact work), checked before the
        // one-shot.
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
        // Folded in only after winning the one-shot, so a declined pass leaves counters alone.
        if let Some(w) = wire {
            self.footers.add_requests(w);
        }
        self.footers.record(took, files, wire.is_some());
        true
    }

    /// The page on screen took `took` and read `files` files (`None` when Polars chose).
    /// Replaces rather than adds: the page being viewed. No byte figure: Polars does not
    /// report what it fetched, and row-group sizes would overstate it.
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

    /// Everything measured, added; `None` until something is. No file count (listing counts
    /// files, footer passes count footers). Wire figures sum the stretches that counted
    /// them, `None` when none did.
    pub fn total(&self) -> Option<Total> {
        // Listing and footers only. The page is not part of opening the dataset — it is
        // what looking at one costs, and it changes every time the view moves.
        let parts: Vec<Cost> = [self.listing(), self.footers()]
            .into_iter()
            .flatten()
            .collect();
        // Two stretches or none: one stretch's total would repeat it under two labels.
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
    #[cfg(test)]
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

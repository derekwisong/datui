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
#[derive(Debug, Clone)]
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

#[cfg(test)]
mod tests {
    use super::*;

    /// A meter that has measured nothing says so, rather than saying nothing happened.
    ///
    /// The two are different claims and the difference is the whole point of the
    /// section: a dataset opened by a route that does not measure must show no figures
    /// at all, not a listing that took no time over no files.
    #[test]
    fn nothing_measured_is_not_the_same_as_nothing_having_happened() {
        let meter = Meter::default();
        assert_eq!(meter.listing(), None);
        assert_eq!(meter.footers(), None);
        assert_eq!(meter.total(), None);

        // A listing that genuinely returned nothing is still a measurement.
        meter.listed(Duration::ZERO, Some(0), false);
        assert_eq!(
            meter.listing(),
            Some(Cost {
                took: Duration::ZERO,
                files: Some(0),
                over_the_wire: None
            }),
            "an empty prefix was still listed, and the time it took is a real figure"
        );
    }

    /// A staged open reads two footers to open and the rest behind it, and what the
    /// footers cost is both passes.
    ///
    /// The second pass adding to the first is the behaviour under test: replacing would
    /// report the tail of the work as though it were all of it, which on a large prefix
    /// is the difference between "four seconds of footers" and "one".
    #[test]
    fn a_second_footer_pass_adds_to_the_first_rather_than_replacing_it() {
        let meter = Meter::default();
        meter.footer_request(1_000);
        meter.footer_request(1_000);
        meter.read_footers(Duration::from_millis(200), Some(2), true);

        // The pass behind the open, reading the rest against the same meter.
        for _ in 0..98 {
            meter.footer_request(1_000);
        }
        meter.read_footers(Duration::from_millis(3_000), Some(98), true);

        let footers = meter.footers().expect("both passes were measured");
        assert_eq!(footers.took, Duration::from_millis(3_200), "both times");
        assert_eq!(footers.files, Some(100), "both counts");
        assert_eq!(
            footers.over_the_wire,
            Some(OverTheWire {
                requests: 100,
                bytes: Some(100_000)
            }),
            "and every request either pass made"
        );
    }

    /// The total adds the stretches, and claims wire figures only from the stretches
    /// that had any.
    #[test]
    fn the_total_adds_what_there_is_and_claims_no_more() {
        let meter = Meter::default();
        meter.listed(Duration::from_millis(100), Some(3), false);
        meter.footer_request(4_096);
        meter.read_footers(Duration::from_millis(900), Some(3), true);

        assert_eq!(
            meter.total(),
            Some(Total {
                took: Duration::from_secs(1),
                over_the_wire: Some(OverTheWire {
                    requests: 1,
                    bytes: Some(4_096)
                }),
            }),
            "the times of both, and the requests of the one that made them — and no \
             file count, which would be two different denominators added together"
        );

        // A total over stretches that made no requests claims none, rather than zero.
        let local = Meter::default();
        local.listed(Duration::from_millis(1), Some(2), false);
        local.read_footers(Duration::from_millis(2), Some(2), false);
        assert_eq!(
            local.total().and_then(|t| t.over_the_wire),
            None,
            "nothing crossed a wire, so there is no figure to give"
        );
    }

    /// A total carries only the figures its stretches actually had.
    ///
    /// A listing never reports requests — no route can count a listing's round trips,
    /// since the object store does its own paging — so a total's requests and bytes are
    /// the footer reads', and a listing beside them adds only time.
    #[test]
    fn a_total_gives_no_figure_it_cannot_account_for() {
        let meter = Meter::default();
        meter.listed(Duration::from_millis(1), None, false);
        for _ in 0..4 {
            meter.footer_request(250);
        }
        meter.read_footers(Duration::from_millis(3), Some(2), true);

        let listing = meter.listing().expect("the walk was measured");
        assert_eq!(
            listing.over_the_wire, None,
            "a listing claims nothing over the wire, having no way to count it"
        );

        let total = meter.total().expect("and a total over the two stretches");
        assert_eq!(
            total.took,
            Duration::from_millis(4),
            "the two times added up"
        );
        assert_eq!(
            total.over_the_wire,
            Some(OverTheWire {
                requests: 4,
                bytes: Some(1_000)
            }),
            "and the footer reads' requests and bytes, which account for each other"
        );
    }

    /// A stretch that counted no bytes leaves the total unable to name any.
    ///
    /// Nothing produces this shape today — every stretch that reports requests also
    /// counts their bytes — so this holds the arithmetic rather than a route: were a
    /// stretch ever to report requests it could not weigh, carrying the other's bytes
    /// forward would present them as the bytes behind all of them.
    #[test]
    fn a_total_will_not_weigh_requests_nothing_weighed() {
        let a = Cost {
            took: Duration::from_millis(1),
            files: None,
            over_the_wire: Some(OverTheWire {
                requests: 4,
                bytes: None,
            }),
        };
        let b = Cost {
            took: Duration::from_millis(3),
            files: Some(2),
            over_the_wire: Some(OverTheWire {
                requests: 4,
                bytes: Some(1_000),
            }),
        };
        assert_eq!(
            Some(combine_wire(
                a.over_the_wire.unwrap(),
                b.over_the_wire.unwrap()
            )),
            Some(OverTheWire {
                requests: 8,
                bytes: None
            }),
            "eight requests, and no byte figure: a thousand bytes is what four of them \
             returned, not eight"
        );
    }

    /// The page replaces, and is no part of what opening the dataset cost.
    ///
    /// Every other stretch adds, because reading a dataset's footers twice really did
    /// cost twice. A page is different: there is one on screen, the figure describes
    /// that one, and scrolling through a hundred of them must not report the hundred
    /// added together as though the last one had taken a minute.
    #[test]
    fn the_page_on_screen_replaces_the_one_before_it_and_is_not_part_of_the_open() {
        let meter = Meter::default();
        meter.listed(Duration::from_millis(10), Some(3), false);
        meter.read_footers(Duration::from_millis(20), Some(3), false);

        meter.read_page(Duration::from_millis(500), Some(2));
        meter.read_page(Duration::from_millis(300), Some(1));
        assert_eq!(
            meter.last_page(),
            Some(Cost {
                took: Duration::from_millis(300),
                files: Some(1),
                over_the_wire: None
            }),
            "the page on screen is the one before last replaced, not added to it"
        );

        let total = meter.total().expect("the open was measured");
        assert_eq!(
            total.took,
            Duration::from_millis(30),
            "and the total is the listing and the footers — looking at a page is not \
             part of opening the dataset"
        );

        // A page Polars chose the files for reports a time and no count.
        meter.read_page(Duration::from_millis(40), None);
        assert_eq!(
            meter.last_page().and_then(|c| c.files),
            None,
            "a whole-scan read says how long it took and not how many files it touched"
        );
    }

    /// Publishing never moves a figure backwards.
    ///
    /// Two metered passes can finish at once — a dataset filtered while the pass behind
    /// its open is still reading — and the one that read the live counter first would,
    /// storing, write its lower figure over the other's higher one. The state below is
    /// exactly that interleaving caught mid-way: a figure already published, and a live
    /// counter that a slower pass read before it climbed.
    #[test]
    fn publishing_cannot_lower_a_figure_another_pass_has_already_published() {
        let tally = Tally::default();
        tally.requests.store(100, Ordering::Relaxed);
        tally.bytes.store(100_000, Ordering::Relaxed);
        tally.live_requests.store(50, Ordering::Relaxed);
        tally.live_bytes.store(50_000, Ordering::Relaxed);
        // As a pass that really read bytes leaves it.
        tally.counted_bytes.store(true, Ordering::Relaxed);

        tally.record(Duration::from_millis(1), Some(1), true);

        let cost = tally.cost().expect("the pass was recorded");
        assert_eq!(
            cost.over_the_wire,
            Some(OverTheWire {
                requests: 100,
                bytes: Some(100_000)
            }),
            "the higher figure stands; storing the live read would have halved both"
        );
    }

    /// A count pass the one shot declines leaves the figures completely alone.
    ///
    /// Not just the time and the count: its requests must not reach the live counters
    /// either, or the next pass to record would publish them as its own.
    #[test]
    fn a_declined_count_adds_nothing_at_all() {
        let meter = Meter::default();
        // As an open that measured itself leaves it: counting belongs to one of those.
        meter.listed(Duration::from_millis(1), Some(3), false);
        let wire = OverTheWire {
            requests: 2,
            bytes: Some(8_192),
        };
        assert!(
            meter.counted_rows(Duration::from_millis(100), Some(3), Some(wire)),
            "the first count is the one that is measured"
        );
        let after_first = meter.footers().expect("and it was measured");

        for _ in 0..3 {
            assert!(
                !meter.counted_rows(Duration::from_millis(100), Some(3), Some(wire)),
                "later counts are re-work and are declined"
            );
        }
        assert_eq!(
            meter.footers(),
            Some(after_first),
            "so the figures stand where the first count left them"
        );

        // And the declined passes left nothing behind for the next pass to publish.
        meter.read_footers(Duration::from_millis(50), Some(1), true);
        assert_eq!(
            meter.footers().and_then(|c| c.over_the_wire),
            Some(wire),
            "a pass recording afterwards publishes the two requests that were really \
             made, not the eight the declined passes would have added"
        );
    }
}

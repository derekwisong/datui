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

/// The figures are over the latest window only, and say nothing before the first.
#[test]
fn durations_are_summed_up_over_the_latest_window() {
    let mut d = Durations::default();
    assert_eq!(d.summary(), "-");
    for ms in 1..=10u64 {
        d.record(Duration::from_millis(ms));
    }
    assert_eq!(d.quantile(0.5), Some(Duration::from_millis(6)));
    assert_eq!(d.quantile(1.0), Some(Duration::from_millis(10)));
    assert_eq!(d.summary(), "p50 6.0ms p99 10.0ms max 10.0ms");
    for _ in 0..Durations::WINDOW {
        d.record(Duration::from_millis(1));
    }
    assert_eq!(
        d.quantile(1.0),
        Some(Duration::from_millis(1)),
        "the old ones aged out"
    );
    assert_eq!(d.count(), 10 + Durations::WINDOW as u64);
}

use super::*;
use crate::analysis::data_quality::fixtures::measure;
use crate::analysis::data_quality::{TemporalRoleAssignment, TimeInterpretation, TimeKind};
use polars::prelude::*;

const HOUR: i64 = 3_600_000_000;

/// Two days of sends and receipts, with a receipt missing, one early and one
/// over an hour; and validity periods, one open and one that ends first.
fn frame() -> LazyFrame {
    let day = 1_704_067_200_000_000i64; // 2024-01-01
    let sent = (0..8)
        .map(|row| Some(day + (row / 4) * 24 * HOUR + row * HOUR))
        .collect::<Vec<_>>();
    let seen = sent
        .iter()
        .enumerate()
        .map(|(row, at)| match row {
            1 => None,
            2 => at.map(|at| at - 60_000_000),
            5 => at.map(|at| at + 2 * HOUR),
            _ => at.map(|at| at + 600_000_000),
        })
        .collect::<Vec<_>>();
    let datetimes = |name: &str, values: Vec<Option<i64>>| -> Column {
        Series::new(name.into(), values)
            .cast(&DataType::Datetime(TimeUnit::Microseconds, None))
            .unwrap()
            .into()
    };
    let days = |name: &str, values: [Option<i32>; 8]| -> Column {
        Series::new(name.into(), values)
            .cast(&DataType::Date)
            .unwrap()
            .into()
    };
    DataFrame::new(
        8,
        vec![
            datetimes("sent", sent),
            datetimes("seen", seen),
            Column::new("stamp".into(), vec!["2024-01-01T00:10:00Z"; 8]),
            days("from", [Some(19_723); 8]),
            days(
                "to",
                [
                    Some(19_730),
                    None,
                    Some(19_720),
                    Some(19_730),
                    Some(19_730),
                    Some(19_730),
                    Some(19_730),
                    Some(19_723),
                ],
            ),
        ],
    )
    .unwrap()
    .lazy()
}

fn role(role: TemporalRole, column: &str) -> TemporalRoleAssignment {
    TemporalRoleAssignment {
        role,
        column: column.to_string(),
        timezone: None,
    }
}

struct Screen {
    state: DataTableState,
    plan: DataQualityPlan,
    /// What Setup offers roles: the frame's date and time columns, and text.
    candidates: Vec<String>,
    results: DataQualityResults,
    theme: Theme,
    ctx: RenderContext,
}

impl Screen {
    fn new(plan: DataQualityPlan) -> Self {
        let lf = frame();
        let schema = Arc::new((*lf.clone().collect_schema().unwrap()).clone());
        let state = DataTableState::from_schema_and_lazyframe(
            schema,
            lf.clone(),
            &crate::OpenOptions::default(),
            None,
        )
        .unwrap();
        let results = measure(&lf, Some(8), &plan);
        Self {
            state,
            plan,
            candidates: ["sent", "seen", "from", "to", "stamp"]
                .map(String::from)
                .to_vec(),
            results,
            theme: Theme::from_config(&crate::config::ThemeConfig::default()).unwrap(),
            ctx: RenderContext::for_test(),
        }
    }

    fn studied() -> Self {
        Self::new(DataQualityPlan {
            compute: QualityCompute::Full,
            temporal_roles: vec![
                role(TemporalRole::Event, "sent"),
                role(TemporalRole::Received, "seen"),
                role(TemporalRole::ValidFrom, "from"),
                role(TemporalRole::ValidTo, "to"),
            ],
            latency_threshold_seconds: Some(3_600),
            grain: QualityGrain::TimeWindows {
                column: "sent".to_string(),
                every: "1d".to_string(),
            },
            ..DataQualityPlan::default()
        })
    }

    fn draw(
        &self,
        page: QualityPage,
        interval: usize,
        selected: usize,
        size: (u16, u16),
    ) -> String {
        let config = DataQualityWidgetConfig {
            checks_expanded: false,
            state: &self.state,
            plan: &self.plan,
            measured: &self.plan,
            results: Some(&self.results),
            from_cache: false,
            metric: QualityMetric::NullRate,
            segment_index: 0,
            interval_index: interval,
            trend_line: 0,
            expected_form: None,
            segments_by_change: false,
            page,
            setup: SetupView {
                time_candidates: &self.candidates,
                ..SetupView::default()
            },
            plan_field: selected,
            show_access: false,
            observation_detail: false,
            findings: &FindingsView {
                column: None,
                check: None,
                order: FindingOrder::Ranked,
            },
            rows_kept: false,
            evidence_read: None,
            focus: AnalysisFocus::Main,
            theme: &self.theme,
            ctx: &self.ctx,
            intent_form: None,
            export_form: None,
        };
        let (width, height) = size;
        let area = Rect::new(0, 0, width, height);
        let mut buf = Buffer::empty(area);
        let mut table = TableState::default();
        table.select(Some(selected));
        let mut sidebar = TableState::default();
        render(
            config,
            &mut table,
            &mut sidebar,
            &mut DetailScroll::default(),
            area,
            &mut buf,
        );
        (0..height)
            .map(|y| {
                (0..width)
                    .map(|x| buf[(x, y)].symbol().to_string())
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join("\n")
    }
}

/// Every character outside ASCII is a glyph slot, which `LANG=C` swaps for its
/// ASCII twin.
fn assert_glyph_slots(text: &str) {
    let g = glyphs::get();
    let slots = [
        g.rail,
        g.rule_h,
        g.rule_h_focused,
        g.middot,
        g.ellipsis,
        g.selector,
        g.checkbox_on,
        g.checkbox_off,
    ]
    .concat();
    for c in text.chars().filter(|c| !c.is_ascii()) {
        assert!(
            slots.contains(c) || "╭╮╰╯│─".contains(c),
            "{c:?} is not a glyph slot:\n{text}"
        );
    }
}

/// The list says what its last column counts and out of what; each row names
/// its interval and segment where there is room, and the selected row has the
/// selector at every size.
#[test]
fn the_interval_list_fits_80x24_and_60x20() {
    let screen = Screen::studied();
    let intervals = &screen.results.temporal;
    assert_eq!(intervals.len(), 4, "two intervals over two days");
    for size in [(120, 32), (80, 24), (60, 20)] {
        for (selected, interval) in intervals.iter().enumerate() {
            let text = screen.draw(QualityPage::Intervals, 0, selected, size);
            assert!(text.contains("Time between dates"), "{text}");
            assert!(
                text.contains("Over: duration > 1 hour, of rows with both ends"),
                "{size:?}: {text}"
            );
            let row = text
                .lines()
                // Leading: the ASCII selector, `> `, is also in "duration > 1 hour".
                .find(|line| line.trim_start().starts_with(glyphs::get().selector))
                .unwrap_or_else(|| panic!("a selected row at {size:?}:\n{text}"));
            // In full, or cut with an ellipsis where the width runs out.
            let label = interval.label();
            assert!(
                row.contains(&label)
                    || row.contains(&label[..12]) && row.contains(glyphs::get().ellipsis),
                "{size:?}: {row}"
            );
            if size.0 >= 80 {
                assert!(row.contains(&interval.segment), "{row}");
            }
            assert_glyph_slots(&text);
        }
    }
    // Wide enough, the list adds the denominator and each end's missing count.
    let wide = screen.draw(QualityPage::Intervals, 0, 0, (160, 32));
    assert!(
        wide.contains("Both ends") && wide.contains("Missing s/e"),
        "{wide}"
    );
}

/// An interval's detail at 80x24 holds every count at once; at 60x20 it scrolls
/// to the count under the cursor. Missing ends, unread text and negative
/// durations are rows of their own, and each count says what it is out of.
#[test]
fn an_interval_detail_keeps_every_count_at_80x24_and_60x20() {
    let screen = Screen::studied();
    let first = &screen.results.temporal[0];
    assert_eq!(first.label(), "event to received");
    assert_eq!(
        (first.evaluated_rows, first.paired_rows, first.missing_end),
        (4, 3, 1)
    );
    let labels = [
        "Start",
        "End",
        "Segment",
        "Rows",
        "Both ends",
        "Missing start",
        "Missing end",
        "Negative",
        "Zero",
        "p50, p90",
        "p95, p99",
        "Maximum",
        "Threshold",
        "Over 1 hour",
    ];
    let text = screen.draw(QualityPage::IntervalDetail, 0, 0, (80, 24));
    for label in labels {
        assert!(text.contains(label), "{label}:\n{text}");
    }
    assert!(
        text.contains("3 of 4 (75.0%)"),
        "both ends out of rows:\n{text}"
    );
    assert!(
        text.contains("1 of 3 (33.3%) with both ends"),
        "negative out of both ends:\n{text}"
    );
    assert!(text.contains("duration > 1 hour, strictly"), "{text}");
    assert!(!text.contains("more"), "nothing cut at 80x24:\n{text}");
    let facts = IntervalFact::ALL
        .into_iter()
        .filter(|fact| first.count(*fact, &screen.plan).is_some())
        .collect::<Vec<_>>();
    // 60x17 is what a 60x20 terminal leaves the page under its bars.
    for size in [(80, 24), (60, 20), (60, 17)] {
        for (selected, fact) in facts.iter().enumerate() {
            let text = screen.draw(QualityPage::IntervalDetail, 0, selected, size);
            let rail = format!("{} {}", glyphs::get().rail, fact.label(first));
            assert!(
                text.contains(&rail),
                "{fact:?} under the cursor at {size:?}:\n{text}"
            );
            assert_glyph_slots(&text);
            // Scrolled, the page ends on its last line or the count of what
            // is below, never on blank lines.
            if !text.contains("Start") {
                let lines = text.lines().collect::<Vec<_>>();
                let last = lines.iter().rposition(|line| !line.trim().is_empty());
                assert_eq!(last, Some(lines.len() - 2), "{fact:?}:\n{text}");
            }
        }
    }
}

/// Where Enter cannot help, the page says why: a report of file metadata reads
/// no times, and a row chunk's rows are not a value a count can open.
#[test]
fn intervals_say_why_nothing_opens() {
    let roles = vec![
        role(TemporalRole::Event, "sent"),
        role(TemporalRole::Received, "seen"),
    ];
    let screen = Screen::new(DataQualityPlan {
        compute: QualityCompute::Metadata,
        temporal_roles: roles.clone(),
        ..DataQualityPlan::default()
    });
    assert!(screen.results.temporal.is_empty());
    let text = screen.draw(QualityPage::Intervals, 0, 0, (80, 24));
    assert!(
        text.contains("File metadata only · no values read"),
        "{text}"
    );

    let screen = Screen::new(DataQualityPlan {
        compute: QualityCompute::Full,
        temporal_roles: roles,
        grain: QualityGrain::RowChunks(4),
        ..DataQualityPlan::default()
    });
    for size in [(80, 24), (60, 20)] {
        let text = screen.draw(QualityPage::IntervalDetail, 0, 0, size);
        assert!(text.contains("Rows do not open: a row chunk"), "{text}");
    }
}

/// Windowing intervals by their ends groups once per end column, and on a full
/// scan Setup says how many of its passes that is before Run.
#[test]
fn setup_counts_the_passes_a_window_clock_takes() {
    let mut plan = DataQualityPlan {
        compute: QualityCompute::Full,
        temporal_roles: vec![
            role(TemporalRole::Event, "sent"),
            role(TemporalRole::Received, "seen"),
            role(TemporalRole::Processed, "to"),
        ],
        grain: QualityGrain::TimeWindows {
            column: "sent".to_string(),
            every: "1d".to_string(),
        },
        interval_clock: IntervalClock::End,
        ..DataQualityPlan::default()
    };
    let text = Screen::new(plan.clone()).draw(QualityPage::Setup, 0, 0, (120, 40));
    assert!(
        text.contains("Window by each interval's end: 2 of those passes, 1 per column"),
        "{text}"
    );
    plan.interval_clock = IntervalClock::Grain;
    let text = Screen::new(plan).draw(QualityPage::Setup, 0, 0, (120, 40));
    assert!(!text.contains("of those passes"), "{text}");
}

/// A full scan's Read says how it gets the rows of a remote source before Run:
/// one fetch into a copy, a copy fetched earlier, or the source in every pass
/// with the reason there is no copy.
#[test]
fn setup_says_how_a_full_scan_reads_a_remote_source() {
    const MIB: u64 = 1024 * 1024;
    let lines = |copy: CopyPlan, copy_released: bool| {
        let view = SetupView {
            copy,
            copy_released,
            ..SetupView::default()
        };
        copy_lines(&view, 7).join("\n")
    };
    let fetch = CopyPlan::Fetch {
        bytes: 17 * MIB,
        objects: 8,
    };
    let text = lines(fetch, false);
    assert!(
        text.starts_with(
            "1 fetch of 8 objects (17.0 MiB) to a local copy · up to 7 passes over it"
        ),
        "{text}"
    );
    assert!(
        text.contains("Local copy kept for later full scans · d releases"),
        "{text}"
    );
    assert!(!text.contains("Released since"), "{text}");
    assert!(lines(fetch, true).contains("Released since last copy · fetched again"));
    let one = CopyPlan::Fetch {
        bytes: MIB,
        objects: 1,
    };
    assert!(lines(one, false).starts_with("1 fetch of 1 object (1.0 MiB)"));
    assert_eq!(
        lines(
            CopyPlan::Kept {
                bytes: 17 * MIB,
                objects: 8
            },
            false
        ),
        "Every eligible row · up to 7 passes over the local copy (17.0 MiB) · no source read"
    );
    assert!(lines(CopyPlan::NotApplicable, false).contains("passes over the scope"));
    for (why, says) in [
        (NoCopy::Off, "No local copy: quality_local_copy = 0"),
        (NoCopy::SizeUnknown, "No local copy: object sizes unknown"),
        (
            NoCopy::Unusable,
            "No local copy: copy did not read as the source",
        ),
        (
            NoCopy::PartOfTheSource,
            "No local copy: scope reads part of the source",
        ),
        (
            NoCopy::TooLarge {
                bytes: 3 * 1024 * MIB,
                limit: 2 * 1024 * MIB,
            },
            "No local copy: 3.0 GiB over the 2.0 GiB limit",
        ),
        (
            NoCopy::NoRoom {
                bytes: 17 * MIB,
                free: Some(MIB),
            },
            "No local copy: 17.0 MiB needed, 1.0 MiB free on disk",
        ),
        (
            NoCopy::NoRoom {
                bytes: 17 * MIB,
                free: None,
            },
            "No local copy: free disk space unknown",
        ),
    ] {
        let text = lines(CopyPlan::Passes(why), false);
        assert!(
            text.starts_with("Every eligible row · up to 7 passes over the source"),
            "{text}"
        );
        assert!(text.ends_with(says), "{text}");
    }
}

/// The Read rule names a kept copy beside kept rows, or alone.
#[test]
fn the_read_rule_names_a_kept_copy() {
    let rows = KeptRows {
        samples: 1,
        rows: 100_000,
        bytes: 13 * 1024 * 1024,
        copy_bytes: 0,
    };
    let middot = glyphs::get().middot;
    assert_eq!(rows.label(), format!("100,000 rows kept {middot} 13.0 MiB"));
    let both = KeptRows {
        copy_bytes: 17 * 1024 * 1024,
        ..rows
    };
    assert_eq!(
        both.label(),
        format!("100,000 rows kept {middot} 13.0 MiB, local copy {middot} 17.0 MiB")
    );
    let copy = KeptRows {
        samples: 0,
        rows: 0,
        bytes: 0,
        ..both
    };
    assert_eq!(copy.label(), format!("local copy {middot} 17.0 MiB"));
}

/// Valid from to valid to is a validity period: no end is open, an end before
/// the start ends first.
#[test]
fn a_validity_period_reads_as_one() {
    let screen = Screen::studied();
    let index = screen
        .results
        .temporal
        .iter()
        .position(|interval| interval.is_validity())
        .unwrap();
    let text = screen.draw(QualityPage::IntervalDetail, index, 0, (80, 24));
    assert!(text.contains("Open, no end"), "{text}");
    assert!(text.contains("Ends first"), "{text}");
    assert!(!text.contains("Negative"), "{text}");
}

/// Before a run, Setup names roles no interval uses and says how a time with
/// no zone meets one with an offset; the pairs editor lists every start and end.
#[test]
fn setup_names_unpaired_roles_and_how_zones_compare() {
    let screen = Screen::new(DataQualityPlan {
        temporal_roles: vec![
            role(TemporalRole::Event, "sent"),
            role(TemporalRole::Received, "stamp"),
            role(TemporalRole::Created, "from"),
        ],
        time_formats: vec![TimeInterpretation {
            column: "stamp".to_string(),
            kind: TimeKind::Datetime,
            format: "%Y-%m-%dT%H:%M:%S%.f%#z".to_string(),
        }],
        ..DataQualityPlan::default()
    });
    let text = screen.draw(QualityPage::Setup, 0, 0, (120, 32));
    assert!(
        text.contains("In no interval: created · set Intervals"),
        "{text}"
    );
    assert!(text.contains("No time zone, read as UTC: sent"), "{text}");
    assert!(text.contains("event to received"), "{text}");

    let text = screen.draw(QualityPage::IntervalPairs, 0, 0, (80, 24));
    assert!(text.contains("Intervals  1 of 6"), "{text}");
    let g = glyphs::get();
    assert!(
        text.contains(&format!("{} event to received", g.checkbox_on)),
        "{text}"
    );
    assert!(
        text.contains(&format!("{} created to event", g.checkbox_off)),
        "{text}"
    );
    assert_glyph_slots(&text);
}

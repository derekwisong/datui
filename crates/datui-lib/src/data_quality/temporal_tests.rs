use super::*;
use crate::data_quality::fixtures::measure;

const HOUR: i64 = 3_600_000_000;

fn datetimes(name: &str, values: &[Option<i64>]) -> Column {
    Series::new(name.into(), values)
        .cast(&DataType::Datetime(TimeUnit::Microseconds, None))
        .unwrap()
        .into()
}

fn roles(pairs: &[(TemporalRole, &str)]) -> Vec<TemporalRoleAssignment> {
    pairs
        .iter()
        .map(|(role, column)| TemporalRoleAssignment {
            role: *role,
            column: column.to_string(),
            timezone: None,
        })
        .collect()
}

/// Both ways a run measures: the sample's rows in memory, and a full scan.
fn both_ways(frame: &LazyFrame, rows: usize, plan: &DataQualityPlan) -> [DataQualityResults; 2] {
    [QualityCompute::Sample, QualityCompute::Full].map(|compute| {
        let plan = DataQualityPlan {
            compute,
            ..plan.clone()
        };
        measure(frame, Some(rows), &plan)
    })
}

/// Eight rows: an hour exactly, an hour and a second, two hours, a missing
/// start, a missing end, both missing, half a second early, and no time at all.
fn delays() -> LazyFrame {
    let start = [
        Some(0),
        Some(0),
        Some(0),
        None,
        Some(0),
        None,
        Some(500_000),
        Some(0),
    ];
    let end = [
        Some(HOUR),
        Some(HOUR + 1_000_000),
        Some(2 * HOUR),
        Some(HOUR),
        None,
        None,
        Some(0),
        Some(0),
    ];
    DataFrame::new(8, vec![datetimes("sent", &start), datetimes("seen", &end)])
        .unwrap()
        .lazy()
}

/// A breach is `duration > threshold`, counted out of the rows with both ends:
/// an hour exactly is not over an hour, and the rows less each end's missing
/// count would be the wrong denominator, since one row misses both.
#[test]
fn breaches_are_out_of_rows_with_both_ends() {
    let plan = DataQualityPlan {
        temporal_roles: roles(&[
            (TemporalRole::Event, "sent"),
            (TemporalRole::Received, "seen"),
        ]),
        latency_threshold_seconds: Some(3_600),
        ..DataQualityPlan::default()
    };
    for results in both_ways(&delays(), 8, &plan) {
        let [latency] = results.temporal.as_slice() else {
            panic!("one interval: {:?}", results.temporal);
        };
        assert_eq!(latency.evaluated_rows, 8);
        assert_eq!((latency.missing_start, latency.missing_end), (2, 2));
        assert_eq!(latency.paired_rows, 5);
        assert_ne!(
            latency.evaluated_rows - latency.missing_start - latency.missing_end,
            latency.paired_rows
        );
        assert_eq!(latency.threshold_seconds, Some(3_600));
        assert_eq!(latency.above_threshold_count, Some(2));
        // Half a second early is early, though it is zero whole seconds.
        assert_eq!(latency.negative_count, 1);
        assert_eq!(latency.zero_count, 1);
        assert_eq!(latency.max_seconds, Some(7_200));
        assert_eq!(
            latency.count(IntervalFact::OverThreshold, &plan),
            Some((2, 5))
        );
        assert_eq!(latency.count(IntervalFact::MissingEnd, &plan), Some((2, 8)));
        assert_eq!(latency.count(IntervalFact::UnparsedStart, &plan), None);
    }
}

/// Any start and end can be chosen, not only the pairs the roles suggest; the
/// first choice makes the list explicit, and a role in no interval is named.
#[test]
fn a_chosen_pair_is_measured_and_an_unpaired_role_is_named() {
    let mut plan = DataQualityPlan {
        temporal_roles: roles(&[
            (TemporalRole::Created, "sent"),
            (TemporalRole::Processed, "seen"),
        ]),
        ..DataQualityPlan::default()
    };
    assert!(plan.interval_pairs().is_empty(), "no suggested pair");
    assert_eq!(
        plan.unpaired_roles(),
        vec![TemporalRole::Created, TemporalRole::Processed]
    );
    assert_eq!(
        plan.candidate_pairs(),
        vec![
            (TemporalRole::Created, TemporalRole::Processed),
            (TemporalRole::Processed, TemporalRole::Created),
        ]
    );
    plan.toggle_interval((TemporalRole::Created, TemporalRole::Processed));
    assert_eq!(
        plan.interval_pairs(),
        vec![(TemporalRole::Created, TemporalRole::Processed)]
    );
    assert!(plan.unpaired_roles().is_empty());
    for results in both_ways(&delays(), 8, &plan) {
        let [latency] = results.temporal.as_slice() else {
            panic!("one interval: {:?}", results.temporal);
        };
        assert_eq!(latency.label(), "created to processed");
        assert_eq!(latency.paired_rows, 5);
    }

    // A suggested pair taken away stays away.
    let mut plan = DataQualityPlan {
        temporal_roles: roles(&[
            (TemporalRole::Event, "sent"),
            (TemporalRole::Received, "seen"),
        ]),
        ..DataQualityPlan::default()
    };
    plan.toggle_interval((TemporalRole::Event, TemporalRole::Received));
    assert_eq!(plan.intervals, Some(Vec::new()));
    assert!(plan.interval_pairs().is_empty());
    let results = measure(&delays(), Some(8), &plan);
    assert!(results.temporal.is_empty());
}

/// Valid from and valid to make an interval without choosing it, and read as a
/// validity period: no end is open, an end first is not valid.
#[test]
fn a_validity_period_counts_open_and_backwards_periods() {
    let days = |name: &str, values: &[Option<i32>]| -> Column {
        Series::new(name.into(), values)
            .cast(&DataType::Date)
            .unwrap()
            .into()
    };
    let frame = DataFrame::new(
        4,
        vec![
            days(
                "from",
                &[Some(19_000), Some(19_000), Some(19_010), Some(19_020)],
            ),
            days("to", &[Some(19_005), None, Some(19_009), Some(19_020)]),
        ],
    )
    .unwrap()
    .lazy();
    let plan = DataQualityPlan {
        temporal_roles: roles(&[
            (TemporalRole::ValidFrom, "from"),
            (TemporalRole::ValidTo, "to"),
        ]),
        ..DataQualityPlan::default()
    };
    for results in both_ways(&frame, 4, &plan) {
        let [period] = results.temporal.as_slice() else {
            panic!("one interval: {:?}", results.temporal);
        };
        assert!(period.is_validity());
        assert_eq!(period.paired_rows, 3);
        assert_eq!(period.missing_end, 1);
        assert_eq!(period.negative_count, 1);
        assert_eq!(period.zero_count, 1);
        assert_eq!(IntervalFact::MissingEnd.label(period), "Open, no end");
        assert_eq!(IntervalFact::Negative.label(period), "Ends first");
    }
}

/// Text with an offset is an instant: `10:00+05:00` is 05:00 UTC, an hour
/// before a time with no zone that reads 06:00, which is taken as UTC.
#[test]
fn zoned_text_compares_with_naive_time_as_utc() {
    let frame = DataFrame::new(
        2,
        vec![
            Column::new(
                "stamped".into(),
                ["2024-01-01T10:00:00+05:00", "2024-01-01T06:00:00Z"],
            ),
            datetimes(
                "logged",
                &[
                    Some(1_704_088_800_000_000), // 2024-01-01 06:00:00
                    Some(1_704_088_800_000_000),
                ],
            ),
        ],
    )
    .unwrap()
    .lazy();
    let offset = TimeInterpretation {
        column: "stamped".to_string(),
        kind: TimeKind::Datetime,
        format: "%Y-%m-%dT%H:%M:%S%.f%#z".to_string(),
    };
    assert!(offset.zoned());
    assert!(offset.reads("2024-01-01T10:00:00+05:00"));
    assert!(offset.reads("2024-01-01T06:00:00Z"));
    assert!(
        !offset.reads("2024-01-01T06:00:00"),
        "no offset, no instant"
    );
    let plan = DataQualityPlan {
        temporal_roles: roles(&[
            (TemporalRole::Event, "stamped"),
            (TemporalRole::Received, "logged"),
        ]),
        time_formats: vec![offset],
        ..DataQualityPlan::default()
    };
    let schema = frame.clone().collect_schema().unwrap();
    assert_eq!(plan.zoned("stamped", &schema), Some(true));
    assert_eq!(plan.zoned("logged", &schema), Some(false));
    for results in both_ways(&frame, 2, &plan) {
        let [latency] = results.temporal.as_slice() else {
            panic!("one interval: {:?}", results.temporal);
        };
        assert_eq!(latency.paired_rows, 2);
        assert_eq!(latency.max_seconds, Some(3_600));
        assert_eq!((latency.zero_count, latency.negative_count), (1, 0));
    }
}

/// With time windows, an interval goes in the window of the grain's column, or
/// of its own start or end: a delay across midnight lands on the day it ended.
#[test]
fn the_window_clock_puts_an_interval_on_its_start_or_end() {
    let late = 1_704_150_000_000_000; // 2024-01-01 23:00:00
    let frame = DataFrame::new(
        1,
        vec![
            datetimes("sent", &[Some(late)]),
            datetimes("seen", &[Some(late + 2 * HOUR)]),
        ],
    )
    .unwrap()
    .lazy();
    let mut plan = DataQualityPlan {
        temporal_roles: roles(&[
            (TemporalRole::Event, "sent"),
            (TemporalRole::Received, "seen"),
        ]),
        grain: QualityGrain::TimeWindows {
            column: "sent".to_string(),
            every: "1d".to_string(),
        },
        ..DataQualityPlan::default()
    };
    assert!(plan.windows_intervals());
    let schema = frame.clone().collect_schema().unwrap();
    for (clock, day) in [
        (IntervalClock::Grain, "2024-01-01"),
        (IntervalClock::Start, "2024-01-01"),
        (IntervalClock::End, "2024-01-02"),
    ] {
        plan.interval_clock = clock;
        assert_eq!(interval_passes(&plan, &schema), 1);
        for results in both_ways(&frame, 1, &plan) {
            let segments = results
                .temporal
                .iter()
                .map(|latency| latency.segment.as_str())
                .collect::<Vec<_>>();
            assert_eq!(segments, vec![day], "{clock:?}");
        }
    }
    // By their ends, intervals ending in different columns are cut twice; by
    // the grain, once however many there are.
    plan.temporal_roles
        .extend(roles(&[(TemporalRole::Processed, "seen2")]));
    let frame = frame.with_column(col("seen").alias("seen2"));
    let schema = frame.clone().collect_schema().unwrap();
    assert_eq!(plan.interval_pairs().len(), 3);
    assert_eq!(interval_passes(&plan, &schema), 2);
    plan.interval_clock = IntervalClock::Grain;
    assert_eq!(interval_passes(&plan, &schema), 1);
}

/// The rows a detail opens for a fact are the rows it counted, in the segment
/// it counted them in.
#[test]
fn a_facts_rows_are_the_rows_it_counted() {
    let frame = delays().with_column(
        when(col("sent").is_null())
            .then(lit("2024-01-02"))
            .otherwise(lit("2024-01-01"))
            .str()
            .to_date(StrptimeOptions::default())
            .alias("day"),
    );
    for grain in [
        QualityGrain::Dataset,
        QualityGrain::Partition("day".to_string()),
        QualityGrain::TimeWindows {
            column: "day".to_string(),
            every: "1d".to_string(),
        },
    ] {
        let plan = DataQualityPlan {
            temporal_roles: roles(&[
                (TemporalRole::Event, "sent"),
                (TemporalRole::Received, "seen"),
            ]),
            latency_threshold_seconds: Some(3_600),
            grain: grain.clone(),
            ..DataQualityPlan::default()
        };
        for results in both_ways(&frame, 8, &plan) {
            assert!(!results.temporal.is_empty());
            for latency in &results.temporal {
                for fact in IntervalFact::ALL {
                    let Some((count, _)) = latency.count(fact, &plan) else {
                        assert!(latency.evidence_predicate(fact, &plan, None).is_none());
                        continue;
                    };
                    let predicate = latency
                        .evidence_predicate(fact, &plan, None)
                        .unwrap_or_else(|| panic!("{grain:?} {fact:?} opens nothing"));
                    let rows = frame.clone().filter(predicate).collect().unwrap().height();
                    assert_eq!(rows, count, "{grain:?} {} {fact:?}", latency.segment);
                }
            }
        }
    }
    // A row chunk is a stretch of rows, not a value to filter on.
    let plan = DataQualityPlan {
        temporal_roles: roles(&[
            (TemporalRole::Event, "sent"),
            (TemporalRole::Received, "seen"),
        ]),
        grain: QualityGrain::RowChunks(4),
        ..DataQualityPlan::default()
    };
    let results = measure(&frame, Some(8), &plan);
    assert!(results.temporal.iter().all(|latency| {
        latency
            .evidence_predicate(IntervalFact::Negative, &plan, None)
            .is_none()
    }));
}

/// A segment's label finds its rows whatever the partition holds: text with
/// `=` and spaces, integers, booleans, floats and datetimes (the last two write
/// differently cast to text), and a zoned column's windows at every width,
/// across New York's spring-forward day.
#[test]
fn a_facts_rows_are_found_by_any_segment_label() {
    let spring = 1_710_054_000_000_000i64; // 2024-03-10 07:00 UTC
    let zoned: Column = Series::new(
        "zoned".into(),
        [0, 3, 20, -1, -10, 40, 0, 960]
            .map(|hours| (hours >= 0 || hours == -10).then_some(spring + hours * HOUR)),
    )
    .cast(&DataType::Datetime(
        TimeUnit::Nanoseconds,
        TimeZone::opt_try_new(Some("America/New_York")).unwrap(),
    ))
    .unwrap()
    .into();
    let mut frame = delays().collect().unwrap();
    for column in [
        Column::new(
            "key=part".into(),
            [
                Some("a=b"),
                Some(" x "),
                Some(""),
                None,
                Some("é"),
                Some("a=b"),
                Some("1.0"),
                Some(" x "),
            ],
        ),
        Column::new("int".into(), [1i64, 2, 3, 1, 2, 3, 1, 2]),
        Column::new(
            "float".into(),
            [0.1f64, 1e20, 2.5, 0.1, 1e20, 2.5, 0.1, 3.0],
        ),
        Column::new(
            "bool".into(),
            [true, false, true, false, true, false, true, false],
        ),
        datetimes("stamp", &[0, HOUR, 0, HOUR, 0, HOUR, 1, 0].map(Some)),
        zoned,
    ] {
        frame.with_column(column).unwrap();
    }
    let frame = frame.lazy();
    let grains = ["key=part", "int", "float", "bool", "stamp"]
        .map(|column| QualityGrain::Partition(column.to_string()))
        .into_iter()
        .chain(
            QUALITY_WINDOW_WIDTHS.map(|every| QualityGrain::TimeWindows {
                column: "zoned".to_string(),
                every: every.to_string(),
            }),
        );
    for grain in grains {
        for clock in IntervalClock::ALL {
            let plan = DataQualityPlan {
                temporal_roles: roles(&[
                    (TemporalRole::Event, "sent"),
                    (TemporalRole::Received, "seen"),
                ]),
                latency_threshold_seconds: Some(3_600),
                grain: grain.clone(),
                interval_clock: clock,
                ..DataQualityPlan::default()
            };
            for results in both_ways(&frame, 8, &plan) {
                for latency in &results.temporal {
                    for fact in IntervalFact::ALL {
                        let Some((count, _)) = latency.count(fact, &plan) else {
                            continue;
                        };
                        let predicate = latency.evidence_predicate(fact, &plan, None).unwrap();
                        let rows = frame.clone().filter(predicate).collect().unwrap();
                        assert_eq!(
                            rows.height(),
                            count,
                            "{grain:?} {clock:?} {} {fact:?}",
                            latency.segment
                        );
                    }
                }
            }
        }
    }
}

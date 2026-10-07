use super::*;
use crate::analysis::data_quality::fixtures::measure;
use crate::analysis::data_quality::{ExpectedWindows, QualityCompute, UnsampledSegment};
use polars::prelude::{DataType, IntoLazy, LazyFrame, col, df};

fn at(text: &str) -> NaiveDateTime {
    NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M").unwrap()
}

/// A label a run gives a window reads back to the window's start, at every
/// width, and the start makes the same label again.
#[test]
fn window_labels_read_back_to_their_start() {
    for (every, start, label) in [
        ("1h", "2024-03-05 13:00", "2024-03-05 13:00"),
        ("1d", "2024-03-05 00:00", "2024-03-05"),
        ("1w", "2024-03-04 00:00", "week of 2024-03-04"),
        ("1mo", "2024-03-01 00:00", "2024-03"),
    ] {
        let start_text = at(start).format("%Y-%m-%d %H:%M:%S").to_string();
        assert_eq!(
            crate::analysis::data_quality::time_window_label("day", every, Some(&start_text)),
            label
        );
        assert_eq!(window_start(label, every), Some(at(start)), "{label}");
        assert_eq!(floor_window(at("2024-03-05 13:27"), every), at(start));
    }
    assert_eq!(window_start("day ∅", "1d"), None);
    assert_eq!(
        next_window(at("2024-01-01 00:00"), "1mo"),
        Some(at("2024-02-01 00:00"))
    );
    assert_eq!(
        window_count(at("2024-01-01 00:00"), at("2024-04-01 00:00"), "1mo"),
        3
    );
    assert_eq!(
        window_count(at("2024-01-01 00:00"), at("2024-04-02 00:00"), "1mo"),
        4
    );
    assert_eq!(
        window_count(at("2024-01-01 00:00"), at("2024-01-08 00:00"), "1d"),
        7
    );
    assert_eq!(
        calendar_span(at("2024-01-01 00:00"), at("2024-01-22 00:00"), "1w"),
        "2024-01-01 to 2024-01-28"
    );
    assert_eq!(
        calendar_span(at("2024-01-01 05:00"), at("2024-01-01 05:00"), "1h"),
        "2024-01-01 05:00 to 2024-01-01 05:59"
    );
}

/// The interval is where a rate likely sits: around it, inside 0 to 1, wide for a
/// thin sample, and not a point at a count of zero.
#[test]
fn a_wilson_interval_stays_honest_at_the_edges() {
    let (low, high) = wilson_interval(5.0, 100.0).unwrap();
    assert!(
        low < 0.05 && 0.05 < high && high - low < 0.12,
        "{low} {high}"
    );
    let (low, high) = wilson_interval(0.0, 10.0).unwrap();
    assert_eq!(low, 0.0);
    assert!(high > 0.25, "ten rows say little: {high}");
    // Every row counted: the mirror image, up against 1.
    let (low, high) = wilson_interval(10.0, 10.0).unwrap();
    assert!((1.0 - high).abs() < 1e-12 && low < 0.75, "{low} {high}");
    assert!(wilson_interval(0.0, 0.0).is_none());
}

/// Eight weeks of days, Monday to Friday only, with the second week missing and
/// a column whose nulls start in the sixth.
fn weekdays() -> LazyFrame {
    let monday = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
    let days = (0..56)
        .map(|day| monday + Duration::days(day))
        .filter(|day| day.weekday().num_days_from_monday() < 5)
        .filter(|day| !(7..14).contains(&(*day - monday).num_days()))
        .collect::<Vec<_>>();
    let rows = days
        .iter()
        .flat_map(|day| std::iter::repeat_n(*day, 40))
        .collect::<Vec<_>>();
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let day = rows
        .iter()
        .map(|day| (*day - epoch).num_days() as i32)
        .collect::<Vec<_>>();
    let value = rows
        .iter()
        .enumerate()
        .map(|(row, day)| ((*day - monday).num_days() < 35 || row % 2 == 0).then_some(row as i64))
        .collect::<Vec<_>>();
    df!("day" => day, "value" => value)
        .unwrap()
        .lazy()
        .with_column(col("day").cast(DataType::Date))
}

fn daily(compute: QualityCompute, rows: usize) -> DataQualityPlan {
    DataQualityPlan {
        compute,
        dataset_rows: rows,
        grain: QualityGrain::TimeWindows {
            column: "day".to_string(),
            every: "1d".to_string(),
        },
        ..DataQualityPlan::default()
    }
}

/// No stated windows, no gaps: a weekend with no rows is not a defect until
/// someone says rows are expected on it.
#[test]
fn gaps_appear_only_with_stated_windows() {
    let frame = weekdays();
    let plan = daily(QualityCompute::Full, 0);
    let results = measure(&frame, None, &plan);
    assert_eq!(expected_gaps(&plan, &results), None);
    // Stated on a grain that is not time windows, it waits for one.
    let whole = DataQualityPlan {
        grain: QualityGrain::Dataset,
        expected: Some(ExpectedWindows::default()),
        ..plan.clone()
    };
    assert_eq!(expected_gaps(&whole, &results), None);
}

/// Every day expected: the weekends and the missing week are empty, by exact
/// count. Weekdays only: just the missing week, in one run of five.
#[test]
fn an_exact_count_calls_a_window_empty() {
    let frame = weekdays();
    let mut plan = daily(QualityCompute::Full, 0);
    plan.expected = Some(ExpectedWindows::default());
    let results = measure(&frame, None, &plan);
    let Some(Gaps::Checked(every_day)) = expected_gaps(&plan, &results) else {
        panic!("checked");
    };
    assert_eq!(every_day.expected, 54, "first Monday to the last Friday");
    assert_eq!(every_day.with_rows, 35);
    assert_eq!(
        every_day.empty, 19,
        "seven days of week two and six weekends"
    );
    assert_eq!((every_day.unsampled, every_day.out_of_scope), (0, 0));
    assert!(every_day.counted);

    plan.expected = Some(ExpectedWindows {
        weekdays: true,
        ..ExpectedWindows::default()
    });
    let Some(Gaps::Checked(weekdays)) = expected_gaps(&plan, &results) else {
        panic!("checked");
    };
    assert_eq!(weekdays.weekend, 14);
    assert_eq!(weekdays.empty, 5);
    assert_eq!(
        weekdays.runs,
        [GapRun {
            kind: GapKind::Empty,
            first: at("2024-01-08 00:00"),
            last: at("2024-01-12 00:00"),
            windows: 5,
            rows: None,
        }]
    );
    // A stated range past the data: the weeks after it are empty too.
    plan.expected = Some(ExpectedWindows {
        weekdays: true,
        from: Some("2024-01-01".to_string()),
        before: Some("2024-03-04".to_string()),
    });
    let Some(Gaps::Checked(longer)) = expected_gaps(&plan, &results) else {
        panic!("checked");
    };
    assert_eq!(longer.empty, 5 + 5);
    assert_eq!(longer.runs.len(), 2);
}

/// A sample that missed a day says so, with the day's rows; a day with no rows
/// stays empty, since the run counted every day.
#[test]
fn a_window_the_sample_missed_is_not_empty() {
    let frame = weekdays();
    let mut plan = daily(QualityCompute::Sample, 30);
    plan.expected = Some(ExpectedWindows {
        weekdays: true,
        ..ExpectedWindows::default()
    });
    let results = measure(&frame, Some(35 * 40), &plan);
    assert_eq!(results.precision, QualityPrecision::Sampled);
    assert!(
        !results.unsampled_segments.is_empty(),
        "30 rows cannot reach 35 days"
    );
    assert!(
        results
            .unsampled_segments
            .iter()
            .all(|segment| segment.total_rows == 40)
    );
    let Some(Gaps::Checked(check)) = expected_gaps(&plan, &results) else {
        panic!("checked");
    };
    assert!(check.counted);
    assert_eq!(check.unsampled, results.unsampled_segments.len());
    assert_eq!(check.empty, 5, "the missing week, from the count");
    assert_eq!(check.with_rows + check.unsampled, 35);
    let missed = check
        .runs
        .iter()
        .filter(|run| run.kind == GapKind::Unsampled)
        .map(|run| run.rows)
        .collect::<Vec<_>>();
    assert!(
        missed
            .iter()
            .all(|rows| rows.is_some_and(|rows| rows % 40 == 0))
    );

    // The trend keeps the missed days in order, as bars with nothing drawn.
    let view = trend_view(&results, QualityMetric::NullRate, 35);
    assert_eq!(view.slots.len(), 35);
    assert_eq!(view.per_bar, 1);
    assert_eq!(view.coverage().0, results.unsampled_segments.len());
    let (unsampled, thin) = view.coverage();
    assert_eq!(segment_coverage(&results), (view.sampled, unsampled, thin));
    assert_eq!(view.lines[0].names, ["rows"]);
    assert_eq!(view.lines[1].names, ["sampled rows"]);
    let missed = view.bars.iter().position(|bar| bar.unsampled == 1).unwrap();
    assert_eq!(view.lines[0].bars[missed], Some(40.0), "counted");
    assert_eq!(view.lines[1].bars[missed], Some(0.0), "drawn none");
    if let Some(value) = view.lines.iter().find(|line| line.names == ["value"]) {
        assert_eq!(value.bars[missed], None);
    }
}

/// Windows outside the time range the scope reads are out of scope, never
/// empty: the run did not look there.
#[test]
fn windows_outside_the_scope_are_out_of_scope() {
    let frame = weekdays();
    let mut plan = daily(QualityCompute::Full, 0);
    plan.scope = QualityScope::SourceTimeRange {
        column: "day".to_string(),
        start: "2024-01-15".to_string(),
        end: "2024-02-01".to_string(),
    };
    plan.expected = Some(ExpectedWindows {
        weekdays: true,
        from: Some("2024-01-01".to_string()),
        before: Some("2024-02-05".to_string()),
    });
    let scoped =
        crate::analysis::data_quality::apply_quality_scope(frame, &plan.scope, None).unwrap();
    let results = measure(&scoped, None, &plan);
    let Some(Gaps::Checked(check)) = expected_gaps(&plan, &results) else {
        panic!("checked");
    };
    assert_eq!(
        check.out_of_scope,
        10 + 2,
        "two weeks before, two days after"
    );
    assert_eq!(check.empty, 0);
    assert_eq!(check.with_rows, 13);
    assert_eq!(check.runs[0].kind, GapKind::OutOfScope);

    // A week cut by the scope: the weeks it holds rows in have rows, whatever
    // part of them it left out; only a week with none is out of scope.
    plan.grain = QualityGrain::TimeWindows {
        column: "day".to_string(),
        every: "1w".to_string(),
    };
    plan.scope = QualityScope::SourceTimeRange {
        column: "day".to_string(),
        start: "2024-01-17".to_string(),
        end: "2024-02-07".to_string(),
    };
    let scoped =
        crate::analysis::data_quality::apply_quality_scope(weekdays(), &plan.scope, None).unwrap();
    let results = measure(&scoped, None, &plan);
    let Some(Gaps::Checked(check)) = expected_gaps(&plan, &results) else {
        panic!("checked");
    };
    assert_eq!(check.expected, 5, "January 1 to before February 5");
    assert_eq!(
        check.with_rows, 3,
        "the weeks of January 15, 22 and 29: {check:?}"
    );
    assert_eq!(check.out_of_scope, 2, "the weeks of January 1 and 8");
}

/// A range too long for its grain is refused whole, never checked in part.
#[test]
fn the_windows_checked_are_bounded() {
    let frame = weekdays();
    let mut plan = DataQualityPlan {
        compute: QualityCompute::Full,
        grain: QualityGrain::TimeWindows {
            column: "day".to_string(),
            every: "1h".to_string(),
        },
        ..DataQualityPlan::default()
    };
    plan.expected = Some(ExpectedWindows {
        from: Some("2020-01-01".to_string()),
        before: Some("2024-01-01".to_string()),
        ..ExpectedWindows::default()
    });
    let results = measure(&frame, None, &plan);
    assert_eq!(
        expected_gaps(&plan, &results),
        Some(Gaps::TooMany { windows: 35_064 })
    );
}

/// Lines keep their order at any width, so the line a bar detail opened is the
/// line it shows after a resize.
#[test]
fn trend_lines_keep_their_order_at_any_width() {
    let rows: i32 = 60 * 20;
    let frame = df!(
            "day" => (0..rows).map(|row| 19_723 + row / 20).collect::<Vec<_>>(),
            "steady" => (0..rows).map(|row| (row % 10 != 0).then_some(1i64)).collect::<Vec<_>>(),
            "late" => (0..rows).map(|row| (row < 900 || row % 2 == 0).then_some(1i64)).collect::<Vec<_>>(),
            "spike" => (0..rows).map(|row| !(400..440).contains(&row)).map(|kept| kept.then_some(1i64)).collect::<Vec<_>>(),
        )
        .unwrap()
        .lazy()
        .with_column(col("day").cast(DataType::Date));
    let results = measure(&frame, None, &daily(QualityCompute::Full, 0));
    let order = |bars: usize| {
        trend_view(&results, QualityMetric::NullRate, bars)
            .lines
            .into_iter()
            .map(|line| line.names)
            .collect::<Vec<_>>()
    };
    let first = order(8);
    assert_eq!(first.len(), 4, "rows and three columns: {first:?}");
    for bars in [1, 15, 23, 60, 200] {
        assert_eq!(order(bars), first, "{bars} bars");
    }
}

/// A segment the sample missed sorts among the ones it drew.
#[test]
fn missed_segments_keep_their_place() {
    let frame = weekdays();
    let plan = daily(QualityCompute::Full, 0);
    let mut results = measure(&frame, None, &plan);
    let moved = results.segments.remove(3);
    results.unsampled_segments.push(UnsampledSegment {
        label: moved.label.clone(),
        total_rows: 40,
    });
    let slots = trend_slots(&results);
    assert_eq!(slots[3].label, moved.label);
    assert!(slots[3].profile.is_none());
}

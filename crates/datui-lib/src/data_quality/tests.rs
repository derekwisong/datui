use super::*;
use crate::data_quality::fixtures::measure;

fn fixture() -> LazyFrame {
    df!(
        "id" => &[1i64, 2, 3, 4],
        "amount" => &[1.0f64, f64::NAN, f64::INFINITY, 4.0],
        "constant" => &["x", "x", "x", "x"],
        "text_number" => &[Some("1"), Some("2.5"), Some("bad"), None],
        "dirty" => &[Some(""), Some("   "), Some("ok"), None],
    )
    .unwrap()
    .lazy()
}

/// A date past the calendar's range splits like any other value: by
/// partition it is its own segment, named by its stored number, and in time
/// windows it falls in none, as a null does. Each segment's rows are the ones
/// it counted.
/// A grain names its column; one named for its unit says it is the column.
#[test]
fn a_grain_label_never_reads_day_of_day() {
    let grain = |column: &str| QualityGrain::TimeWindows {
        column: column.to_string(),
        every: "1d".to_string(),
    };
    assert_eq!(grain("date").label(), "by day of date");
    assert_eq!(grain("day").label(), "by day of the day column");
}

#[test]
fn dates_past_the_calendar_fall_in_segments_without_a_panic() {
    let edges = [i64::MIN + 1, 0, i64::MAX];
    let lf = DataFrame::new(
        3,
        vec![
            Column::new("id".into(), [1i64, 2, 3]),
            Series::new("t".into(), edges)
                .cast(&DataType::Datetime(TimeUnit::Milliseconds, None))
                .unwrap()
                .into_column(),
            Series::new("d".into(), [i32::MIN, 0, i32::MAX])
                .cast(&DataType::Date)
                .unwrap()
                .into_column(),
        ],
    )
    .unwrap()
    .lazy();
    for grain in [
        QualityGrain::Partition("t".into()),
        QualityGrain::Partition("d".into()),
        QualityGrain::TimeWindows {
            column: "t".into(),
            every: "1d".into(),
        },
        QualityGrain::TimeWindows {
            column: "d".into(),
            every: "1w".into(),
        },
    ] {
        let plan = DataQualityPlan {
            compute: QualityCompute::Full,
            grain: grain.clone(),
            ..DataQualityPlan::default()
        };
        let results = measure(&lf, Some(3), &plan);
        let labels: Vec<&str> = results.segments.iter().map(|s| s.label.as_str()).collect();
        if let QualityGrain::Partition(column) = &grain {
            assert!(
                labels
                    .iter()
                    .any(|l| l.starts_with(&format!("{column}=-"))
                        && l.contains(" since 1970-01-01")),
                "{grain:?}: {labels:?}"
            );
        } else {
            assert_eq!(labels.len(), 2, "{grain:?}: {labels:?}");
        }
        let schema = lf.clone().collect_schema().unwrap();
        for segment in &results.segments {
            // By the column's type, and by each row's label: the same rows.
            for schema in [Some(schema.as_ref()), None] {
                let predicate = segment_predicate(&plan, &grain, &segment.label, schema)
                    .unwrap()
                    .unwrap();
                let rows = lf.clone().filter(predicate).collect().unwrap().height();
                assert_eq!(
                    Some(rows),
                    segment.total_rows,
                    "{grain:?} {}",
                    segment.label
                );
            }
        }
    }
}

/// A partition segment's rows are the same found by the column's type as by each
/// row's label, for text, whole numbers, decimals and the floats a label rounds.
#[test]
fn partition_segments_find_the_same_rows_by_type_as_by_label() {
    let lf = df!(
        "region" => &[Some("west"), Some("east"), None, Some("west")],
        "year" => &[2020i16, 2021, 2020, 2020],
        "price" => &["1.50", "2.00", "1.50", "3.25"],
        "share" => &[1.0 / 3.0, 0.333_333_3, 0.5, 1.0 / 3.0],
    )
    .unwrap()
    .lazy()
    .with_column(col("price").cast(DataType::Decimal(10, 2)));
    let schema = lf.clone().collect_schema().unwrap();
    for column in ["region", "year", "price", "share"] {
        let grain = QualityGrain::Partition(column.into());
        let plan = DataQualityPlan {
            compute: QualityCompute::Full,
            grain: grain.clone(),
            ..DataQualityPlan::default()
        };
        let results = measure(&lf, Some(4), &plan);
        for segment in &results.segments {
            for schema in [Some(schema.as_ref()), None] {
                let predicate = segment_predicate(&plan, &grain, &segment.label, schema)
                    .unwrap()
                    .unwrap();
                let rows = lf.clone().filter(predicate).collect().unwrap().height();
                assert_eq!(Some(rows), segment.total_rows, "{}", segment.label);
            }
        }
    }
}

/// A full run over the whole scope takes its one segment from the column profile
/// it already measured: the numbers a second pass over the scope gave.
#[test]
fn a_full_whole_scope_segment_is_the_column_profile() {
    let plan = DataQualityPlan {
        compute: QualityCompute::Full,
        ..DataQualityPlan::default()
    };
    let results = measure(&fixture(), Some(4), &plan);
    let schema = fixture().collect_schema().unwrap();
    let read = profile_segments_lazy(&fixture(), 4, &plan, None, &schema, false).unwrap();
    assert_eq!(results.segments.len(), 1);
    let (reused, read) = (&results.segments[0], &read[0]);
    assert_eq!(reused.label, read.label);
    assert_eq!(reused.total_rows, read.total_rows);
    assert_eq!(reused.evaluated_rows, read.evaluated_rows);
    assert_eq!(reused.null_cells, read.null_cells);
    assert_eq!(reused.null_rate, read.null_rate);
    let counts = |segment: &SegmentQualityProfile| {
        segment
            .columns
            .iter()
            .map(|column| {
                (
                    column.name.clone(),
                    column.null_count,
                    column.distinct_count,
                )
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(counts(reused), counts(read));
}

#[test]
fn full_profile_reports_core_counts() {
    let plan = DataQualityPlan {
        compute: QualityCompute::Full,
        ..DataQualityPlan::default()
    };
    let results = measure(&fixture(), Some(4), &plan);

    assert_eq!(results.precision, QualityPrecision::Exact);
    assert_eq!(results.evaluated_rows, 4);
    let dirty = results
        .columns
        .iter()
        .find(|profile| profile.name == "dirty")
        .unwrap();
    assert_eq!(dirty.null_count, 1);
    assert_eq!(dirty.distinct_count, Some(3));
    assert_eq!(dirty.empty_count, Some(1));
    assert_eq!(dirty.whitespace_count, Some(1));

    let amount = results
        .columns
        .iter()
        .find(|profile| profile.name == "amount")
        .unwrap();
    assert_eq!(amount.nan_count, Some(1));
    assert_eq!(amount.positive_infinity_count, Some(1));

    let constant = results
        .columns
        .iter()
        .find(|profile| profile.name == "constant")
        .unwrap();
    assert_eq!(constant.distinct_count, Some(1));
    assert!(
        results
            .observations
            .iter()
            .any(|item| item.kind == ObservationKind::Constant)
    );
}

#[test]
fn exact_observation_predicates_select_matching_rows() {
    let examples = [
        (ObservationKind::Nulls, "dirty", 1),
        (ObservationKind::Empty, "dirty", 1),
        (ObservationKind::Whitespace, "dirty", 1),
        (ObservationKind::NonFinite, "amount", 2),
        (ObservationKind::Constant, "constant", 4),
    ];
    let none = crate::data_quality::fixtures::results_with(Vec::new(), Vec::new());
    for (kind, column, expected) in examples {
        let observation = QualityObservation {
            kind,
            column: column.to_string(),
            affected_rows: expected,
            evaluated_rows: 4,
            fact: String::new(),
            normalized_category: None,
            files: Vec::new(),
            time_format: None,
            full_scale: None,
        };
        let rows = fixture()
            .filter(observation.evidence_predicate(&none).unwrap())
            .collect()
            .unwrap();
        assert_eq!(rows.height(), expected, "{kind:?}");
    }
    let category = QualityObservation {
        kind: ObservationKind::CategoryVariants,
        column: "category".to_string(),
        affected_rows: 3,
        evaluated_rows: 3,
        fact: String::new(),
        normalized_category: Some("north".to_string()),
        files: Vec::new(),
        time_format: None,
        full_scale: None,
    };
    let rows = df!("category" => &["North", " north ", "NORTH"])
        .unwrap()
        .lazy()
        .filter(category.evidence_predicate(&none).unwrap())
        .collect()
        .unwrap();
    assert_eq!(rows.height(), 3);
}

/// Duplicate rows come back exactly as counted, copies together and the most
/// copied first; the text a reading does not parse is exactly the text the
/// profile left out of its count; and a run over kept rows keeps a few of each.
#[test]
fn duplicate_and_parse_failure_evidence_match_their_counts() {
    let lf = df!(
        "id" => &[1i64, 2, 1, 3, 2, 1, 4],
        "code" => &["10", "20", "10", "3x", "20", "10", "n/a"],
    )
    .unwrap()
    .lazy();
    let plan = DataQualityPlan {
        compute: QualityCompute::Sample,
        dataset_rows: 100,
        ..DataQualityPlan::default()
    };
    let (results, kept) =
        compute_data_quality_kept(&lf, Some(7), &plan, None, false, None).unwrap();
    assert!(kept.is_some(), "the rows the run read are kept");
    let identity = results.identity.as_ref().unwrap();
    assert_eq!((identity.duplicate_groups, identity.rows_involved), (2, 5));
    let rows = duplicate_rows(lf.clone(), &["id".into(), "code".into()], false).unwrap();
    assert_eq!(rows.height(), identity.rows_involved);
    let ids = rows
        .column("id")
        .unwrap()
        .i64()
        .unwrap()
        .into_no_null_iter()
        .collect::<Vec<_>>();
    assert_eq!(ids, [1, 1, 1, 2, 2], "copies together, most copied first");
    let streamed = duplicate_rows(lf.clone(), &["id".into(), "code".into()], true).unwrap();
    assert!(streamed.equals(&rows), "the streaming engine agrees");
    assert_eq!(
        identity.examples,
        [
            DuplicateExample {
                copies: 3,
                values: vec!["1".to_string(), "\"10\"".to_string()],
            },
            DuplicateExample {
                copies: 2,
                values: vec!["2".to_string(), "\"20\"".to_string()],
            },
        ]
    );

    // Five of seven parse: below the share a finding needs, so ask the reading
    // of a column that clears it.
    let codes = df!("code" => (0..40).map(|n| n.to_string()).chain(["n/a".to_string()]).collect::<Vec<_>>())
            .unwrap()
            .lazy();
    let (results, _) =
        compute_data_quality_kept(&codes, Some(41), &plan, None, false, None).unwrap();
    let profile = &results.columns[0];
    let (parsed, reading) = text_reading(profile).unwrap();
    assert_eq!((parsed, reading), (40, TextReading::WholeNumber));
    let failed = codes
        .filter(unparsed_text(profile).unwrap())
        .collect()
        .unwrap();
    assert_eq!(failed.height(), profile.non_null_rows() - parsed);
    assert_eq!(
        results.examples_of(ObservationKind::ParseableText, "code"),
        ["\"n/a\""]
    );
}

#[test]
fn sample_is_disclosed_and_bounded() {
    let plan = DataQualityPlan {
        compute: QualityCompute::Sample,
        dataset_rows: 2,
        sample_seed: 7,
        ..DataQualityPlan::default()
    };
    let results = measure(&fixture(), Some(4), &plan);
    assert_eq!(results.precision, QualityPrecision::Sampled);
    assert_eq!(results.total_rows, Some(4));
    assert_eq!(results.evaluated_rows, 2);
}

#[test]
/// The sampler counts the whole scope as it samples it, so a sampled run knows the
/// total it was drawn from even when no count was cached; metadata still does not.
fn a_sampled_run_knows_the_total_it_was_drawn_from() {
    let frame = DataFrame::new(
        100,
        vec![Column::new("id".into(), (0..100).collect::<Vec<_>>())],
    )
    .unwrap()
    .lazy();
    let plan = DataQualityPlan {
        dataset_rows: 10,
        ..DataQualityPlan::default()
    };
    let results = measure(&frame, None, &plan);
    assert_eq!(results.total_rows, Some(100));
    assert_eq!(results.evaluated_rows, 10);
    assert_eq!(results.precision, QualityPrecision::Sampled);
    assert_eq!(results.segments[0].total_rows, Some(100));

    let short = frame.clone().limit(8);
    let results = measure(&short, None, &plan);
    assert_eq!(results.total_rows, Some(8));
    assert_eq!(results.evaluated_rows, 8);
    assert_eq!(results.precision, QualityPrecision::Exact);

    let metadata = DataQualityPlan {
        compute: QualityCompute::Metadata,
        ..plan
    };
    let results = measure(&frame, None, &metadata);
    assert_eq!(results.total_rows, None);
    assert_eq!(results.evaluated_rows, 0);
}

/// A dataset-grain sample is spread across the whole scope. A table sorted by year
/// whose head is all one year must not come back with a single-value year, and the
/// run knows the whole table's size rather than its head's.
#[test]
fn a_dataset_sample_spreads_across_a_sorted_table() {
    let rows = 40_000;
    let frame = df!(
        "id" => (0..rows as i64).collect::<Vec<_>>(),
        "year" => (0..rows).map(|row| 2020 + (row * 4 / rows) as i32).collect::<Vec<_>>(),
    )
    .unwrap()
    .lazy();
    let plan = DataQualityPlan {
        dataset_rows: 1_000,
        ..DataQualityPlan::default()
    };
    let results = measure(&frame, None, &plan);
    assert_eq!(results.precision, QualityPrecision::Sampled);
    assert_eq!(results.evaluated_rows, 1_000);
    assert_eq!(results.total_rows, Some(rows));
    let year = results
        .columns
        .iter()
        .find(|profile| profile.name == "year")
        .unwrap();
    assert_eq!(year.distinct_count, Some(4), "every year is in the sample");
    assert!(
        !results
            .observations
            .iter()
            .any(|observation| observation.kind == ObservationKind::Constant)
    );

    // Seeded: the same seed draws the same rows, another seed others.
    let ids = |seed| {
        let plan = DataQualityPlan {
            sample_seed: seed,
            ..plan.clone()
        };
        let results = measure(&frame, None, &plan);
        let id = results
            .columns
            .iter()
            .find(|profile| profile.name == "id")
            .unwrap();
        (id.min.clone(), id.max.clone())
    };
    assert_eq!(ids(1), ids(1));
    assert_ne!(ids(1), ids(2));
}

/// Partition segments are named as the directory names them and read in the
/// order their values count, so "previous" is the partition before; a segment's
/// drill-in puts the measure that moved most first.
#[test]
fn partitions_compare_with_the_one_before_in_value_order() {
    let years = (0..300)
        .map(|row| [9i64, 10, 11][row / 100])
        .collect::<Vec<_>>();
    // Year 10 loses a tenth of its prices; 11 has them all again.
    let price = (0..300)
        .map(|row| (!(100..110).contains(&row)).then_some(row as f64))
        .collect::<Vec<_>>();
    let frame = df!("year" => years, "price" => price).unwrap().lazy();
    let plan = DataQualityPlan {
        compute: QualityCompute::Full,
        grain: QualityGrain::Partition("year".to_string()),
        comparison: QualityComparison::Previous,
        ..DataQualityPlan::default()
    };
    let results = measure(&frame, Some(300), &plan);
    let labels = results
        .segments
        .iter()
        .map(|segment| segment.label.as_str())
        .collect::<Vec<_>>();
    assert_eq!(labels, ["year=9", "year=10", "year=11"]);
    assert_eq!(results.segments[0].compared_with, None);
    assert_eq!(results.segments[1].compared_with.as_deref(), Some("year=9"));
    assert_eq!(
        results.segments[1].largest_change.as_deref(),
        Some("price nulls +10.0 pp")
    );
    let changes = segment_changes(&results, 1);
    assert_eq!(changes[0].column, "price");
    assert_eq!(changes[0].metric, QualityMetric::NullRate);
    assert_eq!(changes[0].before, Some(0.0));
    assert!((changes[0].change().unwrap() - 10.0).abs() < 1e-9);
    assert!(natural_cmp("part-2", "part-10").is_lt());
    assert!(natural_cmp("year=2024", "year=2025").is_lt());
}

/// A thin sample a day names a change only past sampling noise, knows each
/// day's exact rows, and says when a day's rows halve; Trends pools the days and
/// draws columns that go missing together once.
#[test]
fn a_daily_sample_names_real_changes_and_counts_every_day() {
    let mut day = Vec::new();
    let (mut switched, mut noisy, mut twin) = (Vec::new(), Vec::new(), Vec::new());
    for d in 0..200i32 {
        // Day 150 delivered 20 rows instead of 50.
        let rows = if d == 150 { 20 } else { 50 };
        for r in 0..rows {
            let key = d * 50 + r;
            day.push(d);
            // Filled until day 100, then never.
            switched.push((d < 100).then_some(1i64));
            // About 30% missing every day: steady, and noisy on a sample.
            let gap = (key * 7919) % 10 < 3;
            noisy.push((!gap).then_some(1i64));
            twin.push((!gap).then_some(2i64));
        }
    }
    let total = day.len();
    let frame = df!("day" => day, "switched" => switched, "noisy" => noisy, "twin" => twin)
        .unwrap()
        .lazy()
        .with_column(col("day").cast(DataType::Date));
    let plan = DataQualityPlan {
        dataset_rows: 5_000,
        grain: QualityGrain::TimeWindows {
            column: "day".to_string(),
            every: "1d".to_string(),
        },
        comparison: QualityComparison::Previous,
        ..DataQualityPlan::default()
    };
    let results = measure(&frame, Some(total), &plan);
    assert_eq!(results.precision, QualityPrecision::Sampled);
    assert_eq!(results.segments.len(), 200);
    assert_eq!(
        results.segments[0].total_rows,
        Some(50),
        "counted, not sampled"
    );
    assert_eq!(
        results.segments[100].largest_change.as_deref(),
        Some("switched nulls +100.0 pp")
    );
    assert_eq!(
        results.segments[150].largest_change.as_deref(),
        Some("rows 20 (-60%)")
    );
    let named = results
        .segments
        .iter()
        .filter_map(|segment| segment.largest_change.as_deref())
        .collect::<Vec<_>>();
    assert!(
        named.iter().all(|change| !change.starts_with("noisy")),
        "a steady rate is never named: {named:?}"
    );
    // The clearest changes first; the rest keep their order.
    let order = segment_order(&results, true);
    assert!(order[..3].contains(&100) && order[..3].contains(&150));

    let view = crate::quality_trends::trend_view(&results, QualityMetric::NullRate, 20);
    let (rows, per_bar) = (view.lines, view.per_bar);
    assert_eq!(per_bar, 10);
    assert_eq!(rows[0].names, ["rows"]);
    assert_eq!(rows[0].bars[0], Some(50.0));
    assert_eq!(
        rows[1].names,
        ["sampled rows"],
        "the sample's reach beside it"
    );
    assert_eq!(rows[2].names, ["switched"], "the column that moved leads");
    assert_eq!(rows[2].bars[0], Some(0.0));
    assert_eq!(rows[2].bars[19], Some(1.0));
    assert!(
        rows.iter()
            .any(|row| row.names == ["noisy".to_string(), "twin".to_string()]),
        "columns missing together are one line"
    );
}

/// Hundreds of segments are profiled in one grouped query, and each keeps its
/// own counts: every other day here has one missing price.
#[test]
fn every_segment_keeps_its_own_counts() {
    let days = 400i32;
    let day = (0..days * 3).map(|row| row / 3).collect::<Vec<_>>();
    let price = (0..days * 3)
        .map(|row| (!(row % 3 == 0 && (row / 3) % 2 == 0)).then_some(f64::from(row)))
        .collect::<Vec<_>>();
    let frame = df!("day" => day, "price" => price)
        .unwrap()
        .lazy()
        .with_column(col("day").cast(DataType::Date));
    let plan = DataQualityPlan {
        dataset_rows: 10_000,
        grain: QualityGrain::TimeWindows {
            column: "day".to_string(),
            every: "1d".to_string(),
        },
        ..DataQualityPlan::default()
    };
    let results = measure(&frame, Some(days as usize * 3), &plan);
    assert_eq!(results.segments.len(), days as usize);
    for (index, segment) in results.segments.iter().enumerate() {
        assert_eq!(segment.evaluated_rows, 3, "{}", segment.label);
        let price = segment.columns.iter().find(|c| c.name == "price").unwrap();
        assert_eq!(
            price.null_count,
            usize::from(index % 2 == 0),
            "{}",
            segment.label
        );
    }
}

/// Row chunks are cut from the shared sample by where each sampled row sat, and
/// a chunk's size is known without reading it.
#[test]
fn row_chunks_cut_the_shared_sample_where_its_rows_sat() {
    let frame = DataFrame::new(
        100,
        vec![Column::new("id".into(), (0..100i64).collect::<Vec<_>>())],
    )
    .unwrap()
    .lazy();
    let plan = DataQualityPlan {
        dataset_rows: 30,
        sample_seed: 1,
        grain: QualityGrain::RowChunks(10),
        ..DataQualityPlan::default()
    };
    let results = measure(&frame, Some(100), &plan);
    assert_eq!(results.total_rows, Some(100));
    assert_eq!(results.evaluated_rows, 30);
    assert_eq!(results.precision, QualityPrecision::Sampled);
    assert!(!plan.requires_confirmation(), "a sample never asks first");
    assert_eq!(
        results
            .segments
            .iter()
            .map(|segment| segment.evaluated_rows)
            .sum::<usize>(),
        30
    );
    for segment in &results.segments {
        assert_eq!(segment.total_rows, Some(10), "{}", segment.label);
        // Every sampled id sits inside the chunk its label names.
        let (start, end) = segment
            .label
            .trim_start_matches("rows ")
            .split_once('-')
            .map(|(a, b)| (a.parse::<i64>().unwrap(), b.parse::<i64>().unwrap()))
            .unwrap();
        let id = segment.columns.iter().find(|c| c.name == "id").unwrap();
        let min = id.min.as_deref().unwrap().parse::<i64>().unwrap() + 1;
        let max = id.max.as_deref().unwrap().parse::<i64>().unwrap() + 1;
        assert!(
            start <= min && max <= end,
            "{} holds {min}..{max}",
            segment.label
        );
    }
    let again = measure(&frame, Some(100), &plan);
    assert_eq!(
        results
            .segments
            .iter()
            .map(|s| s.label.clone())
            .collect::<Vec<_>>(),
        again
            .segments
            .iter()
            .map(|s| s.label.clone())
            .collect::<Vec<_>>(),
        "seeded"
    );
}

/// A run that changes only how the rows are cut reads nothing: it cuts the rows
/// the last run kept. An equal-per-value sample counted its values as it read, so
/// a grain by the same column is sized from that; another grain is counted once
/// and the count kept. The frame handed to the later runs fails on any read.
#[test]
fn a_grain_change_cuts_the_kept_sample() {
    let frame = df!(
        "id" => (0..120i64).collect::<Vec<_>>(),
        "region" => (0..120)
            .map(|row| if row < 100 { "big" } else { "small" })
            .collect::<Vec<_>>(),
        "kind" => (0..120)
            .map(|row| if row % 2 == 0 { "x" } else { "y" })
            .collect::<Vec<_>>(),
    )
    .unwrap()
    .lazy();
    // The same schema, and an error the moment a row is read.
    let poisoned = frame.clone().filter(
        (col("id") + lit(1_000i64))
            .strict_cast(DataType::UInt8)
            .is_not_null(),
    );
    let totals = |results: &DataQualityResults| {
        results
            .segments
            .iter()
            .map(|segment| (segment.label.clone(), segment.total_rows))
            .collect::<Vec<_>>()
    };
    let whole = DataQualityPlan {
        method: crate::sampling::SampleMethod::PerPartition {
            column: "region".into(),
        },
        dataset_rows: 5,
        ..DataQualityPlan::default()
    };
    let (_, kept) = compute_data_quality_kept(&frame, None, &whole, None, false, None).unwrap();
    let kept = kept.unwrap();

    let by_region = DataQualityPlan {
        grain: QualityGrain::Partition("region".into()),
        ..whole.clone()
    };
    let (results, again) =
        compute_data_quality_kept(&poisoned, None, &by_region, None, false, Some(&kept)).unwrap();
    assert_eq!(
        totals(&results),
        [
            ("region=big".to_string(), Some(100)),
            ("region=small".to_string(), Some(20))
        ]
    );
    assert!(again.unwrap().counted.is_empty(), "counted by the sampler");
    let fresh = measure(&frame, None, &by_region);
    assert_eq!(
        format!("{:?}", results.segments),
        format!("{:?}", fresh.segments),
        "the same as reading afresh"
    );

    let by_kind = DataQualityPlan {
        grain: QualityGrain::Partition("kind".into()),
        ..whole
    };
    let (counted, kept) =
        compute_data_quality_kept(&frame, None, &by_kind, None, false, Some(&kept)).unwrap();
    let (recut, _) =
        compute_data_quality_kept(&poisoned, None, &by_kind, None, false, kept.as_ref()).unwrap();
    assert_eq!(
        totals(&recut),
        [
            ("kind=x".to_string(), Some(60)),
            ("kind=y".to_string(), Some(60))
        ]
    );
    assert_eq!(totals(&recut), totals(&counted));
}

/// Segments are the shared sample's rows, split. A random sample gives each
/// segment its share; equal per value gives each the same number, which is how a
/// small partition is measured as well as a large one.
#[test]
fn segments_are_the_shared_sample_split() {
    let frame = df!(
        "id" => (0..120i32).collect::<Vec<_>>(),
        "region" => (0..120)
            .map(|row| if row < 100 { "big" } else { "small" })
            .collect::<Vec<_>>(),
    )
    .unwrap()
    .lazy();
    let grain = QualityGrain::Partition("region".into());
    let random = DataQualityPlan {
        dataset_rows: 24,
        grain: grain.clone(),
        ..DataQualityPlan::default()
    };
    let results = measure(&frame, None, &random);
    assert_eq!(results.total_rows, Some(120));
    assert_eq!(results.evaluated_rows, 24);
    assert_eq!(
        results
            .segments
            .iter()
            .map(|segment| segment.total_rows)
            .collect::<Vec<_>>(),
        [Some(100), Some(20)],
        "a partition's size is counted beside the sample, not guessed from it"
    );

    let equal = DataQualityPlan {
        method: crate::sampling::SampleMethod::PerPartition {
            column: "region".into(),
        },
        dataset_rows: 5,
        grain,
        ..DataQualityPlan::default()
    };
    let results = measure(&frame, None, &equal);
    assert_eq!(results.evaluated_rows, 10);
    assert_eq!(
        results
            .segments
            .iter()
            .map(|segment| (segment.label.as_str(), segment.evaluated_rows))
            .collect::<Vec<_>>(),
        vec![("region=big", 5), ("region=small", 5)]
    );
}

#[test]
fn metadata_mode_does_not_evaluate_values() {
    let plan = DataQualityPlan {
        compute: QualityCompute::Metadata,
        ..DataQualityPlan::default()
    };
    let results = measure(&fixture(), Some(4), &plan);
    assert_eq!(results.precision, QualityPrecision::Metadata);
    assert_eq!(results.evaluated_rows, 0);
    assert_eq!(results.columns.len(), 5);
}

#[test]
fn source_projection_preserves_rows_without_binary_payloads() {
    let source = QualitySourceContext {
        file_names: vec!["one.parquet".to_string()],
        file_starts: vec![0],
        row_index_column: "__datui_quality_row".to_string(),
        ..QualitySourceContext::default()
    };
    let frame = df!(
        "value" => &[1i64, 2, 3],
        "blob" => &[&b"one"[..], &b"two"[..], &b"three"[..]],
    )
    .unwrap()
    .lazy();
    let prepared = prepare_source_quality_scan(frame, Some(&source)).unwrap();
    let collected = prepared.collect().unwrap();
    assert_eq!(collected.height(), 3);
    assert_eq!(
        collected
            .column("__datui_quality_row")
            .unwrap()
            .u32()
            .unwrap()
            .get(2),
        Some(2)
    );
    assert_eq!(
        collected.column("blob").unwrap().str().unwrap().get(0),
        Some(crate::table::binary_stub())
    );
}

#[test]
fn scope_commands_round_trip_and_reject_invalid_ranges() {
    for command in [
        "view",
        "source",
        "rows 2..9",
        "files 1,3",
        "partition region=west",
        "time event=2024-01-01..2024-02-01",
    ] {
        let scope = QualityScope::parse_command(command).unwrap();
        assert_eq!(
            QualityScope::parse_command(&scope.command()).unwrap(),
            scope
        );
    }
    assert!(QualityScope::parse_command("rows 0..10").is_err());
    assert!(QualityScope::parse_command("rows 10..2").is_err());
    assert!(QualityScope::parse_command("files 0").is_err());
    assert!(QualityScope::parse_command("time event=2024-03-01..2024-01-01").is_err());
}

#[test]
fn scoped_frames_select_exact_view_source_file_partition_and_time_rows() {
    let frame = df!(
        "id" => &[1i32, 2, 3, 4, 5],
        "region" => &["west", "east", "west", "east", "west"],
        "day" => &[0i32, 1, 2, 3, 4],
    )
    .unwrap()
    .lazy()
    .with_columns([col("day").cast(DataType::Date)]);
    let ids = |scope: QualityScope, frame: LazyFrame, source: Option<&QualitySourceContext>| {
        let df = apply_quality_scope(frame, &scope, source)
            .unwrap()
            .collect()
            .unwrap();
        df.column("id")
            .unwrap()
            .i32()
            .unwrap()
            .into_no_null_iter()
            .collect::<Vec<_>>()
    };
    assert_eq!(
        ids(
            QualityScope::ViewRows { start: 2, end: 4 },
            frame.clone(),
            None
        ),
        vec![2, 3, 4]
    );
    let source = QualitySourceContext {
        file_names: vec!["one".into(), "two".into(), "three".into()],
        file_starts: vec![0, 2, 4],
        row_index_column: "__row".into(),
        ..QualitySourceContext::default()
    };
    assert_eq!(
        ids(
            QualityScope::SourceFiles(vec![1, 3]),
            frame.clone().with_row_index("__row", None),
            Some(&source)
        ),
        vec![1, 2, 5]
    );
    assert_eq!(
        ids(
            QualityScope::SourcePartition {
                column: "region".into(),
                value: "west".into()
            },
            frame.clone(),
            None
        ),
        vec![1, 3, 5]
    );
    assert_eq!(
        ids(
            QualityScope::SourceTimeRange {
                column: "day".into(),
                start: "1970-01-02".into(),
                end: "1970-01-04".into()
            },
            frame,
            None
        ),
        vec![2, 3]
    );
}

/// A partition value compares in the column's own type, whatever the type, one
/// value or a list of them; a value the type cannot read says so.
#[test]
fn partition_values_compare_in_the_columns_type() {
    let frame = df!(
        "id" => &[1i32, 2, 3, 4],
        "year" => &[2019i64, 2020, 2021, 2020],
        "share" => &[0.5f64, 0.25, 0.5, 1.0],
        "price" => &["1.50", "2.00", "1.50", "3.25"],
        "day" => &[19723i32, 19724, 19723, -800_000],
        "at" => &[0i64, 3_600_000_000, 0, 7_200_000_000],
    )
    .unwrap()
    .lazy()
    .with_columns([
        col("price").cast(DataType::Decimal(10, 2)),
        col("day").cast(DataType::Date),
        col("at").cast(DataType::Datetime(TimeUnit::Microseconds, None)),
    ]);
    let schema = frame.clone().collect_schema().unwrap();
    let ids = |column: &str, value: &str| -> Vec<i32> {
        let predicate = partition_predicate(column, value, &schema).unwrap();
        let df = frame.clone().filter(predicate).collect().unwrap();
        df.column("id")
            .unwrap()
            .i32()
            .unwrap()
            .into_no_null_iter()
            .collect()
    };
    assert_eq!(ids("year", "2020"), [2, 4]);
    assert_eq!(ids("year", "2019, 2021"), [1, 3]);
    assert_eq!(ids("year", "2020..2021"), [2, 3, 4]);
    assert_eq!(ids("share", "0.5"), [1, 3]);
    assert_eq!(ids("price", "1.5"), [1, 3], "1.5 is the column's 1.50");
    assert_eq!(ids("day", "2024-01-01"), [1, 3]);
    // A date past the calendar, as its label writes it.
    assert_eq!(ids("day", "-800000 days since 1970-01-01"), [4]);
    assert_eq!(ids("at", "1970-01-01 01:00"), [2]);
    assert_eq!(
        ids("at", "1970-01-01T00:00:00, 1970-01-01 02:00"),
        [1, 3, 4]
    );
    let error = partition_predicate("day", "2024-13-01", &schema).unwrap_err();
    assert_eq!(
        error.to_string(),
        "day: \"2024-13-01\" is not a date written YYYY-MM-DD"
    );
    assert!(partition_predicate("year", "2020..soon", &schema).is_err());
}

#[test]
fn time_scope_accepts_timezone_aware_datetime_bounds() {
    let frame = df!("id" => &[1i32, 2, 3], "ts" => &[0i64, 1_000_000, 2_000_000])
        .unwrap()
        .lazy()
        .with_columns([col("ts").cast(DataType::Datetime(
            TimeUnit::Microseconds,
            Some(TimeZone::UTC),
        ))]);
    let scope =
        QualityScope::parse_command("time ts=1970-01-01T01:00:01+01:00..1970-01-01T00:00:02Z")
            .unwrap();
    let result = apply_quality_scope(frame, &scope, None)
        .unwrap()
        .collect()
        .unwrap();
    assert_eq!(result.column("id").unwrap().i32().unwrap().get(0), Some(2));
    assert_eq!(result.height(), 1);
}

#[test]
fn full_profile_accepts_list_columns() {
    let lists = Column::new(
        "items".into(),
        &[
            Series::new("".into(), &[1i32, 2]),
            Series::new("".into(), &[3i32]),
        ],
    );
    let frame = DataFrame::new(2, vec![lists]).unwrap().lazy();
    let plan = DataQualityPlan {
        compute: QualityCompute::Full,
        ..DataQualityPlan::default()
    };
    let result = measure(&frame, Some(2), &plan);
    assert_eq!(result.columns[0].null_count, 0);
    assert_eq!(result.columns[0].min_length, Some(1));
    assert_eq!(result.columns[0].max_length, Some(2));
    let sampled = measure(&frame, Some(2), &DataQualityPlan::default());
    assert_eq!(sampled.columns[0].min_length, Some(1));
    assert_eq!(sampled.columns[0].max_length, Some(2));
}

#[test]
fn row_chunks_keep_denominators_and_compare_previous() {
    let plan = DataQualityPlan {
        compute: QualityCompute::Full,
        grain: QualityGrain::RowChunks(2),
        comparison: QualityComparison::Previous,
        ..DataQualityPlan::default()
    };
    let results = measure(&fixture(), Some(4), &plan);
    assert_eq!(results.segments.len(), 2);
    assert_eq!(results.segments[0].evaluated_rows, 2);
    let first_dirty = results.segments[0]
        .columns
        .iter()
        .find(|column| column.name == "dirty")
        .unwrap();
    let second_dirty = results.segments[1]
        .columns
        .iter()
        .find(|column| column.name == "dirty")
        .unwrap();
    assert_eq!(QualityMetric::EmptyRate.value(first_dirty), Some(0.5));
    assert_eq!(QualityMetric::EmptyRate.value(second_dirty), Some(0.0));
    assert_eq!(QualityMetric::NullRate.value(second_dirty), Some(0.5));
    assert_eq!(
        results.segments[1].compared_with.as_deref(),
        Some("rows 1-2")
    );
    assert!(results.segments[1].largest_change.is_some());

    let mut selected = results;
    let mut baseline_plan = plan.clone();
    baseline_plan.comparison = QualityComparison::Baseline;
    baseline_plan.baseline_segment = Some("rows 3-4".to_string());
    selected.compare_segments(&baseline_plan);
    assert_eq!(
        selected.segments[0].compared_with.as_deref(),
        Some("rows 3-4")
    );
    assert!(selected.segments[1].compared_with.is_none());
}

/// A comparison worked out from the segments a report holds, as a Compare edit
/// does with no read, is the comparison a fresh run with that Compare makes: on a
/// full scan and on a sample, against the previous segment, the first, a chosen
/// one and one that is not there.
#[test]
fn a_comparison_from_held_segments_matches_a_fresh_run() {
    let rows = 2_000usize;
    // Regions of different sizes and null rates, so both a row count and a rate
    // move between them.
    let region = |row: usize| match row % 10 {
        0..=4 => "a",
        5..=7 => "b",
        8 => "c",
        _ => "d",
    };
    let df = df!(
        "id" => (0..rows as i64).collect::<Vec<_>>(),
        "region" => (0..rows).map(region).collect::<Vec<_>>(),
        "amount" => (0..rows)
            .map(|row| (region(row) != "c" || row % 3 != 0).then_some(row as f64))
            .collect::<Vec<_>>(),
        "note" => (0..rows)
            .map(|row| if region(row) == "d" { "" } else { "ok" })
            .collect::<Vec<_>>(),
    )
    .unwrap()
    .lazy();
    let comparisons = [
        (QualityComparison::Previous, None),
        (QualityComparison::Baseline, None),
        (QualityComparison::Baseline, Some("region=c")),
        (QualityComparison::Baseline, Some("region=z")),
    ];
    for compute in [QualityCompute::Full, QualityCompute::Sample] {
        let base = DataQualityPlan {
            compute,
            dataset_rows: 1_000,
            sample_seed: 11,
            grain: QualityGrain::Partition("region".into()),
            ..DataQualityPlan::default()
        };
        let held = measure(&df, Some(rows), &base);
        assert_eq!(held.segments.len(), 4, "{compute:?}");
        for (comparison, baseline) in comparisons {
            let plan = DataQualityPlan {
                comparison,
                baseline_segment: baseline.map(str::to_string),
                ..base.clone()
            };
            let fresh = measure(&df, Some(rows), &plan);
            let mut derived = held.clone();
            derived.compare_segments(&plan);
            let compared = |results: &DataQualityResults| {
                results
                    .segments
                    .iter()
                    .map(|segment| {
                        (
                            segment.label.clone(),
                            segment.compared_with.clone(),
                            segment.largest_change.clone(),
                            segment.change_size.map(f64::to_bits),
                        )
                    })
                    .collect::<Vec<_>>()
            };
            assert_eq!(
                compared(&derived),
                compared(&fresh),
                "{compute:?} {comparison:?} {baseline:?}"
            );
            assert!(
                fresh
                    .segments
                    .iter()
                    .any(|segment| segment.largest_change.is_some()),
                "{compute:?} {comparison:?} {baseline:?}: something to compare"
            );
        }
    }
}

#[test]
fn temporal_roles_produce_latency_without_name_inference() {
    let event = Series::new(
        "happened_at".into(),
        [Some(0i64), Some(3_600_000_000), None, Some(10_800_000_000)],
    )
    .cast(&DataType::Datetime(TimeUnit::Microseconds, None))
    .unwrap();
    let received = Series::new(
        "landed_at".into(),
        [
            Some(3_600_000_000i64),
            Some(1_800_000_000),
            Some(7_200_000_000),
            None,
        ],
    )
    .cast(&DataType::Datetime(TimeUnit::Microseconds, None))
    .unwrap();
    let frame = DataFrame::new(4, vec![event.into(), received.into()])
        .unwrap()
        .lazy();
    let mut plan = DataQualityPlan {
        compute: QualityCompute::Full,
        ..DataQualityPlan::default()
    };

    let without_roles = measure(&frame, Some(4), &plan);
    assert!(without_roles.temporal.is_empty());

    plan.temporal_roles = vec![
        TemporalRoleAssignment {
            role: TemporalRole::Event,
            column: "happened_at".to_string(),
            timezone: None,
        },
        TemporalRoleAssignment {
            role: TemporalRole::Received,
            column: "landed_at".to_string(),
            timezone: None,
        },
    ];
    let results = measure(&frame, Some(4), &plan);
    assert_eq!(results.temporal.len(), 1);
    let latency = &results.temporal[0];
    assert_eq!(latency.missing_start, 1);
    assert_eq!(latency.missing_end, 1);
    assert_eq!(latency.negative_count, 1);
    assert_eq!(latency.p50_seconds, Some(3_600));
}

/// Every file counted, only the largest few named. A column missing from thousands
/// of files must not cost one struct per file to say so.
#[test]
fn a_column_missing_from_many_files_counts_them_all_and_names_the_largest() {
    use crate::schema_union::DriftGroup;

    const FILES: usize = 25;
    // File `i` holds `i + 1` rows, so the largest files are the last ones.
    let mut file_starts = Vec::with_capacity(FILES);
    let mut row = 0usize;
    for file in 0..FILES {
        file_starts.push(row);
        row += file + 1;
    }
    let source = QualitySourceContext {
        file_names: (0..FILES).map(|file| format!("{file}.parquet")).collect(),
        file_starts,
        dataset_rows: row,
        footers_read: FILES,
        // Group 1 is missing `fee`; every file is in it.
        file_group: vec![1; FILES],
        drift_groups: Arc::new(vec![
            DriftGroup::default(),
            DriftGroup {
                absent: vec!["fee".into()],
                unread: Vec::new(),
            },
        ]),
        ..QualitySourceContext::default()
    };

    let observations = drift_observations(&source, None, false, &QualityWatch::default());
    assert_eq!(observations.len(), 1);
    let absent = &observations[0];
    assert_eq!(absent.kind, ObservationKind::Absent);
    assert_eq!(
        (absent.affected_rows, absent.evaluated_rows),
        (row, row),
        "every file is counted, not only the named ones"
    );
    assert_eq!(absent.files.len(), MAX_EVIDENCE_FILES);
    assert_eq!(
        absent.files.first().map(|file| file.number),
        Some(FILES),
        "the largest file first"
    );
    assert!(
        absent
            .fact
            .starts_with("25 of 25 files have no such column, largest 20 named"),
        "{}",
        absent.fact
    );
    assert_eq!(
        absent.evidence_scope().map(|scope| match scope {
            QualityScope::SourceFiles(files) => files.len(),
            _ => 0,
        }),
        Some(MAX_EVIDENCE_FILES),
        "the drill-in opens the files it named"
    );
}

/// A column that is nearly a key and is not quite one: the repeats are the finding,
/// and a column with three values in a hundred rows is a category, not a near-miss.
#[test]
fn a_nearly_unique_column_that_repeats_is_reported_with_its_repeats() {
    let mut ids = (0..98i64).collect::<Vec<_>>();
    // Two values that appear twice: 98 distinct values over 100 non-null rows.
    ids.push(7);
    ids.push(11);
    let frame = df!(
        "id" => &ids,
        "region" => &(0..100).map(|row| ["north", "south"][row % 2]).collect::<Vec<_>>(),
    )
    .unwrap()
    .lazy();
    let plan = DataQualityPlan {
        compute: QualityCompute::Full,
        ..DataQualityPlan::default()
    };
    let results = measure(&frame, Some(100), &plan);
    let key_like = results
        .observations
        .iter()
        .filter(|observation| observation.kind == ObservationKind::KeyLike)
        .collect::<Vec<_>>();
    assert_eq!(
        key_like
            .iter()
            .map(|o| o.column.as_str())
            .collect::<Vec<_>>(),
        vec!["id"],
        "two values in a hundred rows is a category, not a key that slipped"
    );
    assert_eq!(
        (key_like[0].affected_rows, key_like[0].evaluated_rows),
        (2, 100),
        "rows beyond one per value: non-null rows minus distinct values"
    );
    // The drill-in is every row whose value is not the only one of its kind, which
    // is four rows for two values that each appear twice — more than the count
    // above it, which the detail pane says in so many words.
    let rows = frame
        .clone()
        .filter(key_like[0].evidence_predicate(&results).unwrap())
        .collect()
        .unwrap();
    assert_eq!(rows.height(), 4);

    // A distinct count does not extrapolate: in a sample of a large dataset every
    // repeated id looks unique, so the claim is not made at all.
    let sampled = measure(
        &frame,
        Some(1_000_000),
        &DataQualityPlan {
            compute: QualityCompute::Sample,
            dataset_rows: 10,
            ..DataQualityPlan::default()
        },
    );
    assert_eq!(sampled.precision, QualityPrecision::Sampled);
    assert!(
        !sampled
            .observations
            .iter()
            .any(|observation| observation.kind == ObservationKind::KeyLike),
        "a sampled distinct share cannot say a column is nearly a key"
    );
}

/// Data Quality reads the sample every tool reads, at its full size: past 50,000
/// rows too, where it used to stop, so it and Describe measure the same rows.
#[test]
fn a_sample_is_read_at_its_full_size() {
    let rows = 80_000;
    let frame = df!(
        "id" => (0..rows as i64).collect::<Vec<_>>(),
        "tag" => (0..rows).map(|row| ["a", "b", "c"][row % 3]).collect::<Vec<_>>(),
    )
    .unwrap()
    .lazy();
    let plan = DataQualityPlan {
        dataset_rows: 60_000,
        ..DataQualityPlan::default()
    };
    let results = measure(&frame, Some(rows), &plan);
    assert_eq!(results.evaluated_rows, 60_000);
    assert_eq!(results.precision, QualityPrecision::Sampled);
    let tag = results
        .columns
        .iter()
        .find(|profile| profile.name == "tag")
        .unwrap();
    assert!(
        tag.dominant_value.is_some(),
        "the most common value is measured"
    );
    assert_eq!(
        results
            .identity
            .as_ref()
            .map(|identity| identity.evaluated_rows),
        Some(60_000)
    );
}

/// An empty page names the one setting that fills it: a grain for Segments, time
/// roles for Trends when there are dates to assign, and a grain there otherwise.
#[test]
fn an_empty_page_names_the_setting_that_fills_it() {
    let mut plan = DataQualityPlan::default();
    let results = DataQualityResults::empty(Some(10), &Schema::default());
    let setup = |page, plan: &DataQualityPlan, dates| page_setup(page, plan, Some(&results), dates);
    assert_eq!(
        setup(QualityPage::Segments, &plan, false),
        Some(QualitySetup::Grain)
    );
    assert_eq!(
        setup(QualityPage::Intervals, &plan, true),
        Some(QualitySetup::TimeRoles)
    );
    assert_eq!(setup(QualityPage::Intervals, &plan, false), None);
    assert_eq!(
        setup(QualityPage::Trends, &plan, true),
        Some(QualitySetup::Grain)
    );
    assert_eq!(setup(QualityPage::Overview, &plan, true), None);
    // Two roles that make no interval want a pair chosen, not more roles.
    let mut paired = plan.clone();
    paired.temporal_roles = [TemporalRole::Created, TemporalRole::Processed]
        .map(|role| TemporalRoleAssignment {
            role,
            column: "at".to_string(),
            timezone: None,
        })
        .to_vec();
    assert_eq!(
        setup(QualityPage::Intervals, &paired, true),
        Some(QualitySetup::Intervals)
    );
    // A chosen pair that measured nothing, as on metadata only, is not fixed by
    // choosing pairs again: the page says what is, and Enter opens nothing.
    paired.toggle_interval((TemporalRole::Created, TemporalRole::Processed));
    assert_eq!(setup(QualityPage::Intervals, &paired, true), None);
    assert_eq!(
        page_setup(QualityPage::Segments, &plan, None, true),
        None,
        "nothing to set up before a run"
    );
    plan.grain = QualityGrain::RowChunks(5);
    assert_eq!(setup(QualityPage::Segments, &plan, false), None);
}

/// A partition scope takes one value, a list, or an inclusive range compared in the
/// column's own type: 9..10 includes 10, which as text would sort before 9.
#[test]
fn a_partition_scope_takes_a_value_a_list_or_a_range() {
    let frame = df!(
        "year" => [Some(8i64), Some(9), Some(10), Some(11), None],
        "id" => [1i64, 2, 3, 4, 5],
    )
    .unwrap()
    .lazy();
    let ids = |value: &str| {
        let scope = QualityScope::parse_command(&format!("partition year={value}")).unwrap();
        let rows = apply_quality_scope(frame.clone(), &scope, None)
            .unwrap()
            .collect()
            .unwrap();
        rows.column("id")
            .unwrap()
            .i64()
            .unwrap()
            .into_no_null_iter()
            .collect::<Vec<_>>()
    };
    assert_eq!(ids("9"), vec![2]);
    assert_eq!(ids("8,11"), vec![1, 4]);
    assert_eq!(ids("9..10"), vec![2, 3]);
    assert_eq!(ids("∅"), vec![5]);
}

/// A float measure is nearly unique by nature: prices and volumes repeat by
/// coincidence, and calling that a key that slipped is noise.
#[test]
fn a_nearly_unique_float_is_not_a_key() {
    let mut prices = (0..98).map(|row| row as f64 + 0.5).collect::<Vec<_>>();
    prices.push(7.5);
    prices.push(11.5);
    let frame = df!("price" => &prices).unwrap().lazy();
    let plan = DataQualityPlan {
        compute: QualityCompute::Full,
        ..DataQualityPlan::default()
    };
    let results = measure(&frame, Some(100), &plan);
    assert!(
        !results
            .observations
            .iter()
            .any(|observation| observation.kind == ObservationKind::KeyLike)
    );
}

/// One reading per text column, and only when nearly every value supports it: a
/// column of names with a few numeric ones is text, not numbers stored as text.
#[test]
fn text_is_read_as_numbers_only_when_nearly_all_of_it_parses() {
    let mut names = (0..97).map(|row| format!("name {row}")).collect::<Vec<_>>();
    names.extend(["1", "2", "3"].map(String::from));
    let codes = (0..100)
        .map(|row| format!("{:04}", row * 37))
        .collect::<Vec<_>>();
    let amounts = (0..100).map(|row| format!("{row}.25")).collect::<Vec<_>>();
    let frame = df!("name" => &names, "code" => &codes, "amount" => &amounts)
        .unwrap()
        .lazy();
    let plan = DataQualityPlan {
        compute: QualityCompute::Full,
        ..DataQualityPlan::default()
    };
    let results = measure(&frame, Some(100), &plan);
    let readings = results
        .observations
        .iter()
        .filter(|observation| observation.kind == ObservationKind::ParseableText)
        .map(|observation| {
            let profile = results
                .columns
                .iter()
                .find(|profile| profile.name == observation.column)
                .unwrap();
            let (parsed, reading) = text_reading(profile).unwrap();
            (observation.column.as_str(), parsed, reading.label())
        })
        .collect::<Vec<_>>();
    assert_eq!(
        readings,
        vec![
            ("code", 100, "whole numbers"),
            ("amount", 100, "decimal numbers"),
        ]
    );
    let code = results
        .columns
        .iter()
        .find(|profile| profile.name == "code")
        .unwrap();
    // 0000 and every value under 1000 keep a zero in front.
    assert_eq!(code.leading_zero_count, Some(28));
}

/// Equal null counts are a hint; the shared-null check says whether the columns
/// are missing on the same rows or merely as often.
#[test]
fn columns_missing_together_are_found_to_share_their_rows() {
    let missing = |rows: &[usize]| {
        (0..10)
            .map(|row| (!rows.contains(&row)).then_some(row as f64))
            .collect::<Vec<_>>()
    };
    let frame = df!(
        "open" => missing(&[2, 5]),
        "close" => missing(&[2, 5]),
        "volume" => missing(&[3, 8]),
        "note" => missing(&[3, 9]),
    )
    .unwrap()
    .lazy();
    for compute in [QualityCompute::Sample, QualityCompute::Full] {
        let plan = DataQualityPlan {
            compute,
            ..DataQualityPlan::default()
        };
        let results = measure(&frame, Some(10), &plan);
        assert_eq!(
            results.shared_nulls,
            vec![SharedNulls {
                columns: ["open", "close", "volume", "note"]
                    .map(String::from)
                    .to_vec(),
                null_rows: 2,
                rows_null_in_all: 0,
            }],
            "{compute:?}: four columns with two nulls each share none of them all"
        );
    }
    let frame = df!("open" => missing(&[2, 5]), "close" => missing(&[2, 5]))
        .unwrap()
        .lazy();
    let results = measure(&frame, Some(10), &DataQualityPlan::default());
    assert!(results.shared_nulls[0].same_rows());
}

/// The segment comparison names the sharpest single move, not the average of all
/// of them: a column that goes from never-null to always-null is the finding.
#[test]
fn the_largest_change_names_the_column_and_measurement_that_moved() {
    let frame = df!(
        "steady" => &[1i64, 2, 3, 4],
        "fee" => &[Some(1.5f64), Some(2.5), None, None],
    )
    .unwrap()
    .lazy();
    let plan = DataQualityPlan {
        compute: QualityCompute::Full,
        grain: QualityGrain::RowChunks(2),
        comparison: QualityComparison::Previous,
        ..DataQualityPlan::default()
    };
    let results = measure(&frame, Some(4), &plan);
    assert_eq!(results.segments.len(), 2);
    assert_eq!(
        results.segments[0].largest_change, None,
        "the first chunk has nothing to compare against"
    );
    let change = results.segments[1]
        .largest_change
        .as_deref()
        .expect("the second chunk compares with the first");
    assert!(
        change == "fee nulls +100.0 pp",
        "the column and the measurement that moved: {change}"
    );
}

/// With nothing over the material threshold, a range that moved is still a move.
#[test]
fn a_segment_whose_rates_hold_still_reports_the_range_that_moved() {
    let frame = df!("reading" => &[1i64, 2, 300, 400]).unwrap().lazy();
    let plan = DataQualityPlan {
        compute: QualityCompute::Full,
        grain: QualityGrain::RowChunks(2),
        comparison: QualityComparison::Previous,
        ..DataQualityPlan::default()
    };
    let results = measure(&frame, Some(4), &plan);
    let change = results.segments[1].largest_change.as_deref().unwrap();
    assert!(
        change.starts_with("reading range 1..2 -> 300..400"),
        "no rate moved, but the values did: {change}"
    );
}

/// Trends and Segments name the same chunk the same way, whichever compute budget
/// produced it. The padding a lexicographic sort needs is not a label.
#[test]
fn row_chunk_labels_agree_between_trends_and_segments_at_every_budget() {
    let frame = df!(
        "sent" => &[
            "2024-01-01T00:00:00", "2024-01-01T01:00:00",
            "2024-01-01T02:00:00", "2024-01-01T03:00:00",
        ],
        "landed" => &[
            "2024-01-01T01:00:00", "2024-01-01T03:00:00",
            "2024-01-01T04:00:00", "2024-01-01T06:00:00",
        ],
    )
    .unwrap()
    .lazy()
    .with_columns([
        col("sent")
            .str()
            .to_datetime(None, None, StrptimeOptions::default(), lit("raise")),
        col("landed")
            .str()
            .to_datetime(None, None, StrptimeOptions::default(), lit("raise")),
    ]);
    let roles = vec![
        TemporalRoleAssignment {
            role: TemporalRole::Published,
            column: "sent".to_string(),
            timezone: None,
        },
        TemporalRoleAssignment {
            role: TemporalRole::Received,
            column: "landed".to_string(),
            timezone: None,
        },
    ];
    for compute in [QualityCompute::Sample, QualityCompute::Full] {
        let plan = DataQualityPlan {
            compute,
            grain: QualityGrain::RowChunks(2),
            temporal_roles: roles.clone(),
            ..DataQualityPlan::default()
        };
        let results = measure(&frame, Some(4), &plan);
        let segments = results
            .segments
            .iter()
            .map(|segment| segment.label.clone())
            .collect::<Vec<_>>();
        assert_eq!(
            segments,
            vec!["rows 1-2", "rows 3-4"],
            "{compute:?} segments"
        );
        let mut trends = results
            .temporal
            .iter()
            .map(|profile| profile.segment.clone())
            .collect::<Vec<_>>();
        trends.dedup();
        assert_eq!(trends, segments, "{compute:?} trends");
    }
}

/// No role assigned means no latency to report, and nothing worth splitting the
/// rows up to discover.
#[test]
fn an_unassigned_plan_reports_no_latency() {
    let plan = DataQualityPlan {
        compute: QualityCompute::Sample,
        grain: QualityGrain::RowChunks(2),
        ..DataQualityPlan::default()
    };
    let results = measure(&fixture(), Some(4), &plan);
    assert!(results.temporal.is_empty());
}

#[test]
fn source_row_map_produces_file_segments_without_profiling_hidden_columns() {
    let frame = df!(
        "value" => &[1i64, 2, 3, 4],
        "__row" => &[0u32, 1, 2, 3],
    )
    .unwrap()
    .lazy();
    let source = QualitySourceContext {
        file_names: vec!["a.parquet".to_string(), "b.parquet".to_string()],
        file_starts: vec![0, 2],
        row_index_column: "__row".to_string(),
        ..QualitySourceContext::default()
    };
    let plan = DataQualityPlan {
        compute: QualityCompute::Full,
        grain: QualityGrain::File,
        ..DataQualityPlan::default()
    };
    let results = compute_data_quality(&frame, Some(4), &plan, Some(&source), false).unwrap();
    assert_eq!(results.columns.len(), 1);
    assert_eq!(results.segments.len(), 2);
    assert_eq!(results.segments[0].evaluated_rows, 2);
    assert!(results.segments[0].label.contains("a.parquet"));
}

#[test]
fn identity_and_category_groups_keep_distinct_duplicate_semantics() {
    let frame = df!(
        "id" => &[1i64, 1, 2, 3],
        "category" => &["North", "North", " north ", "NORTH"],
    )
    .unwrap()
    .lazy();
    let plan = DataQualityPlan {
        compute: QualityCompute::Full,
        ..DataQualityPlan::default()
    };
    let results = measure(&frame, Some(4), &plan);
    let identity = results.identity.unwrap();
    assert_eq!(identity.duplicate_groups, 1);
    assert_eq!(identity.extra_rows, 1);
    assert_eq!(identity.rows_involved, 2);
    assert_eq!(results.category_variants.len(), 1);
    assert_eq!(results.category_variants[0].rows_involved, 4);
    let category = results
        .columns
        .iter()
        .find(|profile| profile.name == "category")
        .unwrap();
    assert_eq!(category.dominant_value.as_deref(), Some("North"));
    assert_eq!(category.dominant_count, Some(2));
}

#[test]
fn sample_identity_does_not_equate_null_with_literal_text() {
    let frame = df!("value" => &[None, Some("<null>"), None])
        .unwrap()
        .lazy();
    let plan = DataQualityPlan {
        dataset_rows: 3,
        ..DataQualityPlan::default()
    };
    let results = measure(&frame, Some(3), &plan);
    let identity = results.identity.unwrap();
    assert_eq!(identity.duplicate_groups, 1);
    assert_eq!(identity.rows_involved, 2);
}

#[test]
fn partition_and_time_window_grains_create_ordered_profiles() {
    let timestamps = Series::new(
        "event_at".into(),
        [0i64, 86_400_000_000, 8 * 86_400_000_000],
    )
    .cast(&DataType::Datetime(TimeUnit::Microseconds, None))
    .unwrap();
    let frame = DataFrame::new(
        3,
        vec![
            Column::new("partition".into(), ["a", "a", "b"]),
            Column::new("value".into(), [Some(1i64), None, Some(3)]),
            timestamps.into(),
        ],
    )
    .unwrap()
    .lazy();

    let partition_plan = DataQualityPlan {
        compute: QualityCompute::Full,
        grain: QualityGrain::Partition("partition".to_string()),
        ..DataQualityPlan::default()
    };
    let partitioned = measure(&frame, Some(3), &partition_plan);
    assert_eq!(partitioned.segments.len(), 2);
    assert_eq!(partitioned.segments[0].evaluated_rows, 2);

    let window_plan = DataQualityPlan {
        compute: QualityCompute::Full,
        grain: QualityGrain::TimeWindows {
            column: "event_at".to_string(),
            every: "1w".to_string(),
        },
        ..DataQualityPlan::default()
    };
    let windowed = measure(&frame, Some(3), &window_plan);
    assert_eq!(windowed.segments.len(), 2);
    assert!(windowed.segments[0].label.starts_with("week of "));
}

#[test]
fn an_exact_run_measures_everything_a_sampled_run_does() {
    let frame = df!(
        "when" => &["2024-01-01", "2024-01-02", "not a date", "2024-03-09"],
        "amount" => &["1", "2.5", "3", "bad"],
    )
    .unwrap()
    .lazy();
    // Unambiguous winners, so the two paths cannot differ by tie-breaking.
    let dupes = df!(
        "label" => &[Some("x"), Some("x"), Some("x"), Some("y"), None],
        "n" => &[7i64, 7, 7, 7, 1],
    )
    .unwrap()
    .lazy();
    let measured = |compute| {
        let plan = DataQualityPlan {
            compute,
            ..DataQualityPlan::default()
        };
        let results = measure(&frame, Some(4), &plan);
        results
            .columns
            .iter()
            .map(|column| {
                (
                    column.name.clone(),
                    column.integer_parse_count,
                    column.decimal_parse_count,
                    column.date_parse_count,
                    column.datetime_parse_count,
                )
            })
            .collect::<Vec<_>>()
    };
    let sampled = measured(QualityCompute::Sample);
    assert_eq!(sampled, measured(QualityCompute::Full));

    let dominant = |compute| {
        let plan = DataQualityPlan {
            compute,
            ..DataQualityPlan::default()
        };
        measure(&dupes, Some(5), &plan)
            .columns
            .iter()
            .map(|column| (column.dominant_value.clone(), column.dominant_count))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        dominant(QualityCompute::Full),
        vec![
            (Some("x".to_string()), Some(3)),
            (Some("7".to_string()), Some(4))
        ]
    );
    assert_eq!(
        dominant(QualityCompute::Sample),
        dominant(QualityCompute::Full)
    );
    // The exact run must not be the quieter of the two.
    assert_eq!(sampled[0].3, Some(3), "three of four values are ISO dates");

    // RFC 3339 allows fractional seconds, and so does a bare space separator.
    let stamps = df!("t" => &[
        "2024-01-01T00:00:00Z",
        "2024-01-01T00:00:00.500Z",
        "2024-01-01T00:00:00+01:00",
        "2024-01-01 00:00:00",
        "2024-01-01T00:00:00",
        "garbage",
    ])
    .unwrap()
    .lazy();
    for compute in [QualityCompute::Sample, QualityCompute::Full] {
        let plan = DataQualityPlan {
            compute,
            ..DataQualityPlan::default()
        };
        let results = measure(&stamps, Some(6), &plan);
        assert_eq!(
            results.columns[0].datetime_parse_count,
            Some(5),
            "{compute:?} should accept every ISO timestamp but the garbage"
        );
    }
    assert_eq!(sampled[1].2, Some(3), "three of four parse as decimal");
    assert_eq!(sampled[1].1, Some(2), "two of four parse as integer");

    let observed = |compute| {
        let plan = DataQualityPlan {
            compute,
            ..DataQualityPlan::default()
        };
        let mut kinds = measure(&frame, Some(4), &plan)
            .observations
            .iter()
            .map(|item| (item.kind, item.column.clone()))
            .collect::<Vec<_>>();
        kinds.sort_by(|left, right| {
            left.1
                .cmp(&right.1)
                .then(format!("{:?}", left.0).cmp(&format!("{:?}", right.0)))
        });
        kinds
    };
    assert_eq!(
        observed(QualityCompute::Sample),
        observed(QualityCompute::Full)
    );
}

#[test]
fn a_plan_naming_a_column_the_scope_lost_does_not_kill_the_run() {
    let frame = df!("id" => &[1i64, 2, 3]).unwrap().lazy();
    // A role left over from a wider scope is simply unassigned here.
    let plan = DataQualityPlan {
        compute: QualityCompute::Full,
        temporal_roles: vec![
            TemporalRoleAssignment {
                role: TemporalRole::Event,
                column: "gone".to_string(),
                timezone: None,
            },
            TemporalRoleAssignment {
                role: TemporalRole::Received,
                column: "also_gone".to_string(),
                timezone: None,
            },
        ],
        ..DataQualityPlan::default()
    };
    for compute in [QualityCompute::Sample, QualityCompute::Full] {
        let results = compute_data_quality(
            &frame,
            Some(3),
            &DataQualityPlan {
                compute,
                ..plan.clone()
            },
            None,
            false,
        )
        .unwrap_or_else(|error| panic!("{compute:?} with a stale role: {error}"));
        assert!(results.temporal.is_empty());
        assert_eq!(results.columns.len(), 1);
    }

    // A grain the scope cannot satisfy is refused by name, not by a raw error.
    let error = compute_data_quality(
        &frame,
        Some(3),
        &DataQualityPlan {
            grain: QualityGrain::Partition("region".to_string()),
            ..plan
        },
        None,
        false,
    )
    .expect_err("a grain column that is not in scope must be refused");
    assert!(
        error.to_string().contains("region") && error.to_string().contains("not in scope"),
        "the refusal should name the column: {error}"
    );
}

#[test]
fn every_scope_survives_a_trip_through_the_editor() {
    for scope in [
        QualityScope::CurrentView,
        QualityScope::WholeSource,
        QualityScope::FirstRows(10_000),
        QualityScope::FirstRows(1_000_000),
        QualityScope::ViewRows {
            start: 100,
            end: 200,
        },
        QualityScope::SourceFiles(vec![1, 3]),
        QualityScope::SourcePartition {
            column: "region".to_string(),
            value: "west".to_string(),
        },
        QualityScope::SourceTimeRange {
            column: "event".to_string(),
            start: "2024-01-01".to_string(),
            end: "2024-02-01".to_string(),
        },
    ] {
        assert_eq!(
            QualityScope::parse_command(&scope.command()).unwrap(),
            scope,
            "{} should come back as itself",
            scope.command()
        );
    }
    // An empty or inverted range is still refused.
    assert!(QualityScope::parse_command("rows 1..0").is_err());
    assert!(QualityScope::parse_command("rows 0..5").is_err());
}

#[test]
fn metadata_mode_does_not_need_the_grain_column() {
    // It reads no values, so a grain left over from a wider scope is moot.
    let frame = df!("id" => &[1i64, 2]).unwrap().lazy();
    let plan = DataQualityPlan {
        compute: QualityCompute::Metadata,
        grain: QualityGrain::Partition("gone".to_string()),
        ..DataQualityPlan::default()
    };
    let results = measure(&frame, Some(2), &plan);
    assert_eq!(results.precision, QualityPrecision::Metadata);
}

#[test]
fn segments_come_back_in_one_order_however_much_was_read() {
    // ∅ sorts after ASCII but before U+6771, so ordering by the label alone
    // put the unplaceable rows in the middle of one path and last in the other.
    let frame = df!(
            "region" => &[Some("a"), Some("a"), Some("\u{6771}\u{4eac}"), Some("\u{6771}\u{4eac}"), None, None],
            "id" => &[1i64, 2, 3, 4, 5, 6],
        )
        .unwrap()
        .lazy();
    let plan = DataQualityPlan {
        grain: QualityGrain::Partition("region".to_string()),
        ..DataQualityPlan::default()
    };
    let labels = |compute| {
        measure(
            &frame,
            Some(6),
            &DataQualityPlan {
                compute,
                ..plan.clone()
            },
        )
        .segments
        .iter()
        .map(|segment| segment.label.clone())
        .collect::<Vec<_>>()
    };
    let full = labels(QualityCompute::Full);
    assert_eq!(labels(QualityCompute::Sample), full);
    assert_eq!(full.last().unwrap(), "region=\u{2205}");
}

#[test]
fn a_categorical_column_is_profiled_rather_than_failing_the_run() {
    let frame = df!(
        "label" => &["a", "b", "a", " c "],
        "n" => &[1i64, 2, 3, 4],
    )
    .unwrap()
    .lazy()
    .with_columns([col("label").cast(DataType::from_categories(Categories::global()))]);
    for compute in [QualityCompute::Sample, QualityCompute::Full] {
        let plan = DataQualityPlan {
            compute,
            ..DataQualityPlan::default()
        };
        let results = compute_data_quality(&frame, Some(4), &plan, None, false)
            .unwrap_or_else(|error| panic!("{compute:?} on a categorical column: {error}"));
        let label = results
            .columns
            .iter()
            .find(|column| column.name == "label")
            .expect("the categorical column is profiled");
        assert_eq!(label.null_count, 0);
        assert_eq!(label.distinct_count, Some(3));
    }
}

#[test]
fn every_offered_window_width_cuts_the_scope_it_names() {
    // One row per day from 1970-01-01, far enough to cross a month boundary.
    let day = 86_400_000_000i64;
    let days = 40i64;
    let stamps = Series::new(
        "event_at".into(),
        (0..days).map(|d| d * day).collect::<Vec<_>>(),
    )
    .cast(&DataType::Datetime(TimeUnit::Microseconds, None))
    .unwrap();
    let frame = DataFrame::new(
        days as usize,
        vec![
            Column::new("value".into(), (0..days).collect::<Vec<_>>()),
            stamps.into(),
        ],
    )
    .unwrap()
    .lazy();
    // 1970-01-01 was a Thursday, so 40 days touch seven Monday weeks and two months.
    let expected = [("1h", 40), ("1d", 40), ("1w", 7), ("1mo", 2)];
    for (every, segments) in expected {
        let plan = DataQualityPlan {
            compute: QualityCompute::Full,
            grain: QualityGrain::TimeWindows {
                column: "event_at".to_string(),
                every: every.to_string(),
            },
            ..DataQualityPlan::default()
        };
        let results = measure(&frame, Some(days as usize), &plan);
        assert_eq!(results.segments.len(), segments, "{every} windows");
        assert_eq!(
            results
                .segments
                .iter()
                .map(|segment| segment.evaluated_rows)
                .sum::<usize>(),
            days as usize,
            "{every} windows must account for every row"
        );
        // Named by where the window starts, to the precision its width needs.
        let label = &results.segments[0].label;
        match every {
            "1h" => assert_eq!(label.len(), "2024-01-01 00:00".len(), "{label}"),
            "1d" => assert_eq!(label.len(), "2024-01-01".len(), "{label}"),
            "1w" => assert!(label.starts_with("week of "), "{label}"),
            _ => assert_eq!(label.len(), "2024-01".len(), "{label}"),
        }
    }
}

#[test]
fn rows_without_a_window_clock_are_named_and_ordered_the_same_however_much_was_read() {
    let timestamps = Series::new(
        "event_at".into(),
        [Some(0i64), Some(8 * 86_400_000_000), None, None],
    )
    .cast(&DataType::Datetime(TimeUnit::Microseconds, None))
    .unwrap();
    let frame = DataFrame::new(
        4,
        vec![
            Column::new("value".into(), [1i64, 2, 3, 4]),
            timestamps.into(),
        ],
    )
    .unwrap()
    .lazy();
    let plan = DataQualityPlan {
        grain: QualityGrain::TimeWindows {
            column: "event_at".to_string(),
            every: "1w".to_string(),
        },
        ..DataQualityPlan::default()
    };

    let sampled = measure(&frame, Some(4), &plan);
    let full = measure(
        &frame,
        Some(4),
        &DataQualityPlan {
            compute: QualityCompute::Full,
            ..plan.clone()
        },
    );

    let labels = |results: &DataQualityResults| {
        results
            .segments
            .iter()
            .map(|segment| segment.label.clone())
            .collect::<Vec<_>>()
    };
    assert_eq!(labels(&sampled), labels(&full));
    // Two dated weeks, then the rows the clock could not place.
    assert_eq!(labels(&full).len(), 3);
    assert_eq!(labels(&full)[2], "event_at ∅");
    assert!(labels(&full)[0].starts_with("week of "));
}

fn text_times() -> LazyFrame {
    df!(
        "created" => [
            Some("2024-01-01 08:00:00"),
            Some("2024-01-01 09:30:00"),
            Some("2024-01-02 10:00:00"),
            Some("not a time"),
            None,
            Some("2024-01-03 12:00:00"),
        ],
        "sent" => [
            Some("2024-01-01 09:00:00"),
            Some("2024-01-01 09:00:00"),
            None,
            Some("2024-01-02 11:00:00"),
            Some("2024-01-02 11:00:00"),
            Some("2024-01-03 12:30:00"),
        ],
    )
    .unwrap()
    .lazy()
}

fn read_as_datetime(column: &str) -> TimeInterpretation {
    TimeInterpretation {
        column: column.to_string(),
        kind: TimeKind::Datetime,
        format: "%Y-%m-%d %H:%M:%S".to_string(),
    }
}

/// Text read through a format gives time windows and intervals, sampled or read
/// in full, alike. A value the format does not read is its own count, not a
/// missing value, and the column's own profile stays the text it is.
#[test]
fn text_read_as_time_windows_and_measures_intervals() {
    let plan = DataQualityPlan {
        grain: QualityGrain::TimeWindows {
            column: "created".to_string(),
            every: "1d".to_string(),
        },
        temporal_roles: vec![
            TemporalRoleAssignment {
                role: TemporalRole::Event,
                column: "created".to_string(),
                timezone: None,
            },
            TemporalRoleAssignment {
                role: TemporalRole::Received,
                column: "sent".to_string(),
                timezone: None,
            },
        ],
        time_formats: vec![read_as_datetime("created"), read_as_datetime("sent")],
        ..DataQualityPlan::default()
    };
    for compute in [QualityCompute::Sample, QualityCompute::Full] {
        let plan = DataQualityPlan {
            compute,
            ..plan.clone()
        };
        let results = compute_data_quality(&text_times(), Some(6), &plan, None, false)
            .unwrap_or_else(|error| panic!("{compute:?}: {error}"));
        let labels = results
            .segments
            .iter()
            .map(|segment| segment.label.clone())
            .collect::<Vec<_>>();
        assert_eq!(
            labels,
            ["2024-01-01", "2024-01-02", "2024-01-03", "created ∅"],
            "{compute:?}"
        );
        let (unparsed_start, missing_start) =
            results
                .temporal
                .iter()
                .fold((0, 0), |(unparsed, missing), latency| {
                    (
                        unparsed + latency.unparsed_start,
                        missing + latency.missing_start,
                    )
                });
        assert_eq!((unparsed_start, missing_start), (1, 1), "{compute:?}");
        let first_day = results
            .temporal
            .iter()
            .find(|latency| latency.segment == "2024-01-01")
            .unwrap();
        // 08:00 to 09:00, and 09:30 to 09:00.
        assert_eq!(first_day.negative_count, 1, "{compute:?}");
        assert_eq!(first_day.max_seconds, Some(3_600), "{compute:?}");

        let unparsed = results
            .observations
            .iter()
            .find(|observation| observation.kind == ObservationKind::UnparsedTime)
            .unwrap_or_else(|| panic!("{compute:?}: no unparsed finding"));
        assert_eq!(unparsed.column, "created");
        assert_eq!((unparsed.affected_rows, unparsed.evaluated_rows), (1, 5));
        let rows = text_times()
            .filter(unparsed.evidence_predicate(&results).unwrap())
            .collect()
            .unwrap();
        assert_eq!(rows.height(), 1, "the evidence is the unread value");
        let created = results
            .columns
            .iter()
            .find(|column| column.name == "created")
            .unwrap();
        assert_eq!(created.dtype, DataType::String, "still text to every check");
        assert_eq!(created.null_count, 1);
    }
}

/// A role on text with no format measures no interval, and a time-window grain on
/// it is refused with the remedy, rather than failing somewhere inside a read.
#[test]
fn text_without_a_format_is_not_read_as_time() {
    let plan = DataQualityPlan {
        temporal_roles: vec![
            TemporalRoleAssignment {
                role: TemporalRole::Event,
                column: "created".to_string(),
                timezone: None,
            },
            TemporalRoleAssignment {
                role: TemporalRole::Received,
                column: "sent".to_string(),
                timezone: None,
            },
        ],
        ..DataQualityPlan::default()
    };
    let results = measure(&text_times(), Some(6), &plan);
    assert!(results.temporal.is_empty());
    let windows = DataQualityPlan {
        grain: QualityGrain::TimeWindows {
            column: "created".to_string(),
            every: "1d".to_string(),
        },
        ..DataQualityPlan::default()
    };
    let error = compute_data_quality(&text_times(), Some(6), &windows, None, false)
        .unwrap_err()
        .to_string();
    assert!(error.contains("Text as time"), "{error}");
}

/// The formats Setup offers read on screen what a run reads: chrono for the
/// examples, Polars for the run, one answer.
#[test]
fn every_offered_format_reads_its_example_the_same_way_twice() {
    let samples = [
        "2024-01-31 08:15:00",
        "2024-01-31T08:15:00",
        "2024-01-31 08:15:00.250",
        "2024-01-31T08:15:00.5",
        "2024-01-31T08:15:00Z",
        "2024-01-31T08:15:00.250+05:00",
        "2024-01-31 08:15:00-0500",
        "2024-01-31 08:15",
        "2024-01-31",
        "20240131",
        "01/31/2024 08:15:00",
        "01/31/2024 08:15:00 AM",
        "31/01/2024 08:15:00",
        "31.01.2024 08:15:00",
        "01/31/2024",
        "31/01/2024",
        "31.01.2024",
        "not a time",
    ];
    let frame = df!("text" => samples).unwrap().lazy();
    for (kind, format) in TIME_FORMATS {
        let interpretation = TimeInterpretation {
            column: "text".to_string(),
            kind,
            format: format.to_string(),
        };
        let parsed = frame
            .clone()
            .select([interpretation.expr().is_not_null().alias("read")])
            .collect()
            .unwrap();
        let read = parsed.column("read").unwrap().bool().unwrap().clone();
        let mut any = false;
        for (index, sample) in samples.iter().enumerate() {
            let polars = read.get(index).unwrap_or(false);
            any |= polars;
            assert_eq!(
                interpretation.reads(sample),
                polars,
                "{format} on {sample:?}"
            );
        }
        assert!(any, "{format} reads none of the examples");
    }
}

/// A run names each stage once as it enters it, and says whether the stage reads
/// the source; a cancelled run stops at the next stage instead of finishing.
#[test]
fn a_run_reports_its_stages_and_stops_when_cancelled() {
    let stages = Arc::new(std::sync::Mutex::new(Vec::new()));
    let seen = Arc::clone(&stages);
    let watch = QualityWatch::new(move |phase| seen.lock().unwrap().push(phase));
    let plan = DataQualityPlan {
        grain: QualityGrain::Partition("constant".to_string()),
        ..DataQualityPlan::default()
    };
    let (results, kept) =
        compute_data_quality_watched(&fixture(), Some(4), &plan, None, false, None, &watch);
    results.unwrap();
    let stages = stages.lock().unwrap().clone();
    assert_eq!(stages.first().unwrap().stage, QualityStage::Preparing);
    assert_eq!(stages.last().unwrap().stage, QualityStage::Assembling);
    let read = stages
        .iter()
        .find(|phase| phase.stage == QualityStage::ReadingSample)
        .unwrap();
    assert!(read.reads_source);
    assert!(
        stages
            .iter()
            .filter(|phase| phase.stage == QualityStage::ProfilingColumns)
            .all(|phase| !phase.reads_source),
        "the sample is profiled in memory"
    );
    let distinct = stages
        .iter()
        .map(|phase| phase.stage.label())
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(
        stages.len(),
        distinct.len(),
        "each stage said once: {stages:?}"
    );

    // The same plan again reuses the rows it read, and says so.
    let again = Arc::new(std::sync::Mutex::new(Vec::new()));
    let seen = Arc::clone(&again);
    let watch = QualityWatch::new(move |phase| seen.lock().unwrap().push(phase));
    compute_data_quality_watched(
        &fixture(),
        Some(4),
        &plan,
        None,
        false,
        kept.as_ref(),
        &watch,
    )
    .0
    .unwrap();
    let again = again.lock().unwrap().clone();
    assert!(again.iter().all(|phase| !phase.reads_source), "{again:?}");
    assert!(
        again
            .iter()
            .any(|phase| phase.stage == QualityStage::ReusingSample)
    );

    let cancelled = QualityWatch::default();
    cancelled.cancel();
    let (results, kept) =
        compute_data_quality_watched(&fixture(), Some(4), &plan, None, false, None, &cancelled);
    assert_eq!(results.unwrap_err().to_string(), crate::sampling::CANCELLED);
    assert!(kept.is_none(), "stopped before its read, it read nothing");

    // Stopped after its read, the run still hands its rows back: they were paid for.
    let late = QualityWatch::default();
    let stopper = late.clone();
    let late = QualityWatch {
        report: Some(Arc::new(move |phase: QualityPhase| {
            if phase.stage == QualityStage::ProfilingColumns {
                stopper.cancel();
            }
        })),
        ..late
    };
    let (results, kept) =
        compute_data_quality_watched(&fixture(), Some(4), &plan, None, false, None, &late);
    assert!(results.is_err());
    assert!(kept.is_some(), "the sample it read comes back");
}

/// A table that counts the rows read from it. Every pass over it runs its rows
/// through the filter, whatever the pass selects, so a test counts reads rather
/// than inferring them from the stages a run names.
fn counting_table(rows: usize) -> (DataFrame, LazyFrame, Arc<std::sync::atomic::AtomicUsize>) {
    let start = chrono::NaiveDate::from_ymd_opt(2023, 12, 18)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap();
    // Every 97 minutes: a stride that lands in every hour of the day, across
    // week and month boundaries.
    let at: Vec<chrono::NaiveDateTime> = (0..rows)
        .map(|row| start + chrono::Duration::minutes(row as i64 * 97))
        .collect();
    let micros = |times: &[chrono::NaiveDateTime]| {
        times
            .iter()
            .map(|time| time.and_utc().timestamp_micros())
            .collect::<Vec<_>>()
    };
    let sent: Vec<chrono::NaiveDateTime> = at
        .iter()
        .enumerate()
        .map(|(row, time)| *time + chrono::Duration::seconds(30 + (row % 7) as i64))
        .collect();
    let datetime = DataType::Datetime(TimeUnit::Microseconds, None);
    let df = DataFrame::new(
        rows,
        vec![
            Column::new("id".into(), (0..rows as i64).collect::<Vec<_>>()),
            Column::new("at".into(), micros(&at))
                .cast(&datetime)
                .unwrap(),
            Column::new("sent".into(), micros(&sent))
                .cast(&datetime)
                .unwrap(),
            Column::new(
                "sent_text".into(),
                sent.iter()
                    .map(|time| time.format("%m/%d/%Y %H:%M:%S").to_string())
                    .collect::<Vec<_>>(),
            ),
            Column::new(
                "region".into(),
                (0..rows)
                    .map(|row| ["North", "South", "East"][row % 3])
                    .collect::<Vec<_>>(),
            ),
        ],
    )
    .unwrap();
    let read = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counter = Arc::clone(&read);
    let lf = df.clone().lazy().filter(col("id").map(
        move |column| {
            counter.fetch_add(column.len(), std::sync::atomic::Ordering::Relaxed);
            Ok(column.is_not_null().into_column())
        },
        |_, field| Ok(Field::new(field.name().clone(), DataType::Boolean)),
    ));
    (df, lf, read)
}

fn segment_totals(results: &DataQualityResults) -> BTreeMap<String, Option<usize>> {
    results
        .segments
        .iter()
        .map(|segment| (segment.label.clone(), segment.total_rows))
        .collect()
}

/// Every sampled segment's total is its rows in a full scan of the plain table.
fn assert_exact(results: &DataQualityResults, df: &DataFrame, plan: &DataQualityPlan) {
    let full = DataQualityPlan {
        compute: QualityCompute::Full,
        ..plan.clone()
    };
    let exact = segment_totals(&measure(&df.clone().lazy(), None, &full));
    assert!(!results.segments.is_empty());
    for (label, total) in segment_totals(results) {
        assert_eq!(total, exact[&label], "{label} of {:?}", plan.grain);
    }
    // Every segment with rows is one the sample drew or one it missed, never
    // both, and a missed one has its exact rows.
    for missed in &results.unsampled_segments {
        assert!(
            !results
                .segments
                .iter()
                .any(|segment| segment.label == missed.label),
            "{} of {:?}",
            missed.label,
            plan.grain
        );
        assert_eq!(Some(missed.total_rows), exact[&missed.label]);
    }
    if results
        .segments
        .iter()
        .all(|segment| segment.total_rows.is_some())
    {
        assert_eq!(
            results.segments.len() + results.unsampled_segments.len(),
            exact.len(),
            "{:?}",
            plan.grain
        );
    }
}

/// The rows each edit reads, counted at the table: the "What edits should cost"
/// table of #415 for a streamed sample. The first run counts its daily segments
/// in the pass that samples; roles, text read as time on a role, a coarser window
/// the daily counts nest in, and row chunks read nothing; a finer window, a
/// partition and a grain on newly interpreted text each read their key once; a
/// new seed is a new sample. Every total is the exact one a full scan finds.
#[test]
fn each_edit_reads_only_what_it_needs() {
    let rows = 3_000;
    let (df, lf, read) = counting_table(rows);
    let daily = DataQualityPlan {
        dataset_rows: 300,
        sample_seed: 5,
        grain: QualityGrain::TimeWindows {
            column: "at".into(),
            every: "1d".into(),
        },
        ..DataQualityPlan::default()
    };
    let roles = vec![
        TemporalRoleAssignment {
            role: TemporalRole::Event,
            column: "at".into(),
            timezone: None,
        },
        TemporalRoleAssignment {
            role: TemporalRole::Received,
            column: "sent_text".into(),
            timezone: None,
        },
    ];
    let sent_format = TimeInterpretation {
        column: "sent_text".into(),
        kind: TimeKind::Datetime,
        format: "%m/%d/%Y %H:%M:%S".into(),
    };
    let window = |column: &str, every: &str| QualityGrain::TimeWindows {
        column: column.into(),
        every: every.into(),
    };
    let with = |grain: QualityGrain| DataQualityPlan {
        grain,
        temporal_roles: roles.clone(),
        time_formats: vec![sent_format.clone()],
        ..daily.clone()
    };
    let mut kept: Option<QualitySample> = None;
    let mut run = |plan: &DataQualityPlan, reuse: bool| {
        read.store(0, std::sync::atomic::Ordering::Relaxed);
        let (results, acquired) = compute_data_quality_kept(
            &lf,
            None,
            plan,
            None,
            false,
            if reuse { kept.as_ref() } else { None },
        )
        .unwrap();
        kept = acquired;
        (results, read.load(std::sync::atomic::Ordering::Relaxed))
    };
    let passes = |read: usize| read as f64 / rows as f64;

    let (first, reads) = run(&daily, false);
    assert_eq!(passes(reads), 1.0, "sampled and counted in one pass");
    assert_eq!(first.precision, QualityPrecision::Sampled);
    assert_exact(&first, &df, &daily);

    // A role: the rows are all here. Text read as time for a role, the same.
    let roled = DataQualityPlan {
        temporal_roles: roles.clone(),
        ..daily.clone()
    };
    let (_, reads) = run(&roled, true);
    assert_eq!(reads, 0, "a role edit reads nothing");
    let interpreted = with(daily.grain.clone());
    let (results, reads) = run(&interpreted, true);
    assert_eq!(reads, 0, "an interpretation edit reads nothing");
    assert!(!results.temporal.is_empty(), "and measures the interval");

    // Days nest in weeks and months: summed, not read.
    for every in ["1w", "1mo"] {
        let plan = with(window("at", every));
        let (results, reads) = run(&plan, true);
        assert_eq!(reads, 0, "{every} from the daily counts");
        assert_exact(&results, &df, &plan);
    }
    // An hour does not come from a day, nor a region from time: one count each.
    for grain in [window("at", "1h"), QualityGrain::Partition("region".into())] {
        let plan = with(grain.clone());
        let (results, reads) = run(&plan, true);
        assert_eq!(passes(reads), 1.0, "{grain:?} is counted");
        assert_exact(&results, &df, &plan);
        let (_, reads) = run(&plan, true);
        assert_eq!(reads, 0, "{grain:?} is counted once");
    }
    // A grain on text read as time is a new key: counted once, through its format.
    let plan = with(window("sent_text", "1d"));
    let (results, reads) = run(&plan, true);
    assert_eq!(passes(reads), 1.0);
    assert_exact(&results, &df, &plan);

    // Row chunks: every row's position was kept by the first read.
    let chunks = with(QualityGrain::RowChunks(500));
    let (results, reads) = run(&chunks, true);
    assert_eq!(reads, 0, "row chunks after a first run read nothing");
    // Chunks the sample missed are named as the chunks it drew are.
    let fine = with(QualityGrain::RowChunks(10));
    let (missed, _) = run(&fine, true);
    assert!(!missed.unsampled_segments.is_empty());
    assert_exact(&missed, &df, &fine);
    let (fresh, _) = run(&chunks, false);
    assert_eq!(
        format!("{:?}", results.segments),
        format!("{:?}", fresh.segments),
        "the chunks a chunked read cuts"
    );

    // Another seed, size or scope is other rows: the caller keys the sample by
    // them and hands none over, and the run reads, counting in the same pass.
    for plan in [
        DataQualityPlan {
            sample_seed: 6,
            ..daily.clone()
        },
        DataQualityPlan {
            dataset_rows: 400,
            ..daily.clone()
        },
        DataQualityPlan {
            scope: QualityScope::FirstRows(2_000),
            ..daily.clone()
        },
    ] {
        let scoped = apply_quality_scope(lf.clone(), &plan.scope, None).unwrap();
        read.store(0, std::sync::atomic::Ordering::Relaxed);
        let (results, _) =
            compute_data_quality_kept(&scoped, None, &plan, None, false, None).unwrap();
        assert_eq!(passes(read.load(std::sync::atomic::Ordering::Relaxed)), 1.0);
        let scoped = apply_quality_scope(df.clone().lazy(), &plan.scope, None)
            .unwrap()
            .collect()
            .unwrap();
        assert_exact(&results, &scoped, &plan);
    }
}

/// A count taken in the sampling pass is the count a full scan finds, segment by
/// segment: nulls in the key are their own segment, a zoned column is cut in UTC
/// as a scan cuts it, and a date column is cut as the midnight it is. The rows it
/// counted then serve a coarser window with no read.
#[test]
fn counts_in_the_sampling_pass_match_a_full_scan() {
    let (df, _, _) = counting_table(3_000);
    let gaps = |name: &str| {
        when((col("id") % lit(11i64)).eq(lit(0i64)))
            .then(lit(NULL))
            .otherwise(col(name))
            .alias(name)
    };
    let df = df
        .lazy()
        .with_columns([gaps("at"), gaps("region")])
        .with_columns([
            col("at")
                .dt()
                .replace_time_zone(
                    TimeZone::opt_try_new(Some("America/New_York")).unwrap(),
                    lit("earliest"),
                    NonExistent::Null,
                )
                .alias("zoned"),
            col("at").cast(DataType::Date).alias("day"),
        ])
        .collect()
        .unwrap();
    let window = |column: &str, every: &str| QualityGrain::TimeWindows {
        column: column.into(),
        every: every.into(),
    };
    for grain in [
        window("at", "1h"),
        window("zoned", "1d"),
        window("day", "1d"),
        QualityGrain::Partition("region".into()),
    ] {
        let plan = DataQualityPlan {
            dataset_rows: 200,
            sample_seed: 3,
            grain: grain.clone(),
            ..DataQualityPlan::default()
        };
        let (results, kept) =
            compute_data_quality_kept(&df.clone().lazy(), None, &plan, None, false, None).unwrap();
        assert_eq!(results.precision, QualityPrecision::Sampled);
        assert!(
            results
                .segments
                .iter()
                .any(|segment| segment.label.contains('∅')),
            "{grain:?} has a null segment"
        );
        assert_exact(&results, &df, &plan);
        let kept = kept.unwrap();
        assert_eq!(
            kept.segment_count(&plan),
            SegmentCount::Retained,
            "{grain:?}"
        );
        if let QualityGrain::TimeWindows { column, .. } = &grain {
            let monthly = DataQualityPlan {
                grain: window(column, "1mo"),
                ..plan.clone()
            };
            let (results, _) = compute_data_quality_kept(
                &df.clone().lazy(),
                None,
                &monthly,
                None,
                false,
                Some(&kept),
            )
            .unwrap();
            assert_exact(&results, &df, &monthly);
        }
    }
}

/// Hours sum into days, weeks and months, and days into weeks and months, to the
/// counts a read of the coarser window gives: on a plain, a zoned and a date
/// column, across month ends, week starts and a daylight saving change. A week
/// is not summed into months.
#[test]
fn finer_windows_sum_to_coarser_ones_exactly() {
    let (df, _, _) = counting_table(4_000);
    let df = df
        .lazy()
        .with_columns([
            col("at")
                .dt()
                .replace_time_zone(
                    TimeZone::opt_try_new(Some("America/New_York")).unwrap(),
                    lit("earliest"),
                    NonExistent::Null,
                )
                .alias("zoned"),
            col("at").cast(DataType::Date).alias("day"),
        ])
        .collect()
        .unwrap();
    let count = |column: &str, every: &str| {
        counted_segment_totals(
            &df.clone().lazy(),
            &DataQualityPlan {
                grain: QualityGrain::TimeWindows {
                    column: column.into(),
                    every: every.into(),
                },
                ..DataQualityPlan::default()
            },
            false,
        )
        .unwrap()
    };
    let widths = ["1h", "1d", "1w", "1mo"];
    for column in ["at", "zoned", "day"] {
        for fine in widths {
            for coarse in widths
                .into_iter()
                .filter(|coarse| window_nests(fine, coarse))
            {
                assert_eq!(
                    roll_up_windows(&count(column, fine), coarse).unwrap(),
                    count(column, coarse),
                    "{column}: {fine} into {coarse}"
                );
            }
        }
    }
    assert!(!window_nests("1w", "1mo"));
    assert!(!window_nests("1d", "1h"));
    assert!(!window_nests("1d", "1d"));
}

/// Setup's account of where totals come from matches what the run does: a
/// retained count, a finer count summed, a count pass, or too many to count.
#[test]
fn a_sample_says_where_its_segment_totals_come_from() {
    let (_, lf, _) = counting_table(2_000);
    let daily = DataQualityPlan {
        dataset_rows: 100,
        grain: QualityGrain::TimeWindows {
            column: "at".into(),
            every: "1d".into(),
        },
        ..DataQualityPlan::default()
    };
    assert_eq!(
        fresh_segment_count(&daily, false),
        SegmentCount::InSamplePass
    );
    assert_eq!(fresh_segment_count(&daily, true), SegmentCount::CountPass);
    let head = DataQualityPlan {
        method: crate::sampling::SampleMethod::FirstRows,
        ..daily.clone()
    };
    assert_eq!(fresh_segment_count(&head, false), SegmentCount::CountPass);
    let (_, kept) = compute_data_quality_kept(&lf, None, &daily, None, false, None).unwrap();
    let mut kept = kept.unwrap();
    let grain = |every: &str| DataQualityPlan {
        grain: QualityGrain::TimeWindows {
            column: "at".into(),
            every: every.into(),
        },
        ..daily.clone()
    };
    assert_eq!(kept.segment_count(&daily), SegmentCount::Retained);
    assert_eq!(
        kept.segment_count(&grain("1w")),
        SegmentCount::RolledUp("1d".into())
    );
    assert_eq!(kept.segment_count(&grain("1h")), SegmentCount::CountPass);
    let chunks = DataQualityPlan {
        grain: QualityGrain::RowChunks(100),
        ..daily.clone()
    };
    assert_eq!(kept.segment_count(&chunks), SegmentCount::NotNeeded);

    // A grain whose count gave up names the remedy, and the rows stay.
    // Each such grain is remembered, not only the last.
    let by_region = DataQualityPlan {
        grain: QualityGrain::Partition("region".into()),
        ..daily.clone()
    };
    kept.too_many.push(segment_key(&grain("1h")));
    kept.too_many.push(segment_key(&by_region));
    assert_eq!(kept.segment_count(&grain("1h")), SegmentCount::TooMany);
    assert_eq!(kept.segment_count(&by_region), SegmentCount::TooMany);
    let error =
        compute_data_quality_kept(&lf, None, &grain("1h"), None, false, Some(&kept)).unwrap_err();
    assert!(
        error.to_string().contains("choose a coarser grain"),
        "{error}"
    );
}

/// A stop reaches into a streamed read: the sampler ends at its next batch and
/// the partial rows never become a sample.
#[test]
fn a_stopped_stream_is_not_a_sample() {
    let watch = crate::sampling::ReadWatch::default();
    watch.stop();
    let sample = crate::sampling::Sample {
        scope: QualityScope::CurrentView,
        method: crate::sampling::SampleMethod::PerPartition {
            column: "constant".to_string(),
        },
        rows: 1,
        seed: 7,
    };
    let read = crate::sampling::read_rows_watched(&fixture(), &sample, None, false, Some(&watch));
    let Err(error) = read else {
        panic!("a stopped read returned rows");
    };
    assert_eq!(error.to_string(), crate::sampling::CANCELLED);
}

/// A CSV of `rows` rows on disk: a source whose read takes many batches.
fn csv_source(rows: usize) -> (tempfile::TempDir, LazyFrame) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rows.csv");
    let ids = (0..rows as i64).collect::<Vec<_>>();
    let labels = (0..rows)
        .map(|row| if row % 7 == 0 { "b" } else { "a" })
        .collect::<Vec<_>>();
    let mut df = df!("id" => ids, "label" => labels).unwrap();
    CsvWriter::new(std::fs::File::create(&path).unwrap())
        .finish(&mut df)
        .unwrap();
    let lf = LazyCsvReader::new(PlRefPath::try_from_path(&path).unwrap())
        .finish()
        .unwrap();
    (dir, lf)
}

/// A full run's passes stop within a batch when cancelled mid-read, rather than
/// running their collect to its end, and say they can.
#[cfg(feature = "streaming")]
#[test]
fn a_full_run_stops_inside_its_read() {
    const ROWS: usize = 2_000_000;
    let (_dir, lf) = csv_source(ROWS);
    let plan = DataQualityPlan {
        compute: QualityCompute::Full,
        ..DataQualityPlan::default()
    };
    let stages = Arc::new(std::sync::Mutex::new(Vec::new()));
    let seen = Arc::clone(&stages);
    let watch = QualityWatch::new(move |phase| seen.lock().unwrap().push(phase));
    // Cancel from inside the read, as the source yields its second batch: that
    // batch has yet to reach the watch above it, so the read is still under way
    // whatever the machine's load. A thread that waited to cancel lost the race
    // to a read that had finished meanwhile.
    let stopper = watch.clone();
    let batches = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let lf = lf.map(
        move |df: DataFrame| {
            if batches.fetch_add(1, std::sync::atomic::Ordering::Relaxed) == 1 {
                stopper.cancel();
            }
            Ok(df)
        },
        OptFlags::PROJECTION_PUSHDOWN | OptFlags::PREDICATE_PUSHDOWN | OptFlags::STREAMING,
        None,
        Some("cancel inside the read"),
    );
    let (results, _) =
        compute_data_quality_watched(&lf, Some(ROWS), &plan, None, true, None, &watch);
    assert_eq!(results.unwrap_err().to_string(), crate::sampling::CANCELLED);
    let stages = stages.lock().unwrap().clone();
    let last = stages.last().unwrap();
    assert_eq!(last.stage, QualityStage::ProfilingColumns, "{stages:?}");
    assert!(last.reads_source && last.interruptible);
    let observed = watch.observed();
    assert!(
        observed.rows < ROWS,
        "stopped partway through the first pass: {observed:?}"
    );
}

/// Without the streaming engine every read is one collect a cancel cannot enter,
/// and no stage promises otherwise (#498).
#[cfg(not(feature = "streaming"))]
#[test]
fn without_streaming_no_read_says_it_stops_partway() {
    let (_dir, lf) = csv_source(1_000);
    for compute in [QualityCompute::Full, QualityCompute::Sample] {
        let plan = DataQualityPlan {
            compute,
            ..DataQualityPlan::default()
        };
        let stages = Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen = Arc::clone(&stages);
        let watch = QualityWatch::new(move |phase| seen.lock().unwrap().push(phase));
        let (results, _) =
            compute_data_quality_watched(&lf, Some(1_000), &plan, None, true, None, &watch);
        results.unwrap();
        let stages = stages.lock().unwrap().clone();
        assert!(stages.iter().any(|phase| phase.reads_source), "{stages:?}");
        assert!(
            stages.iter().all(|phase| !phase.interruptible),
            "{stages:?}"
        );
    }
}

/// A file that stores a conflicting column as dates gives a date past the
/// calendar as its stored number, as the table shows it, where Polars' cast to
/// text panicked and lost every file's examples (#506).
#[test]
fn conflict_examples_give_a_date_past_the_calendar_as_its_stored_number() {
    let paris = TimeZone::opt_try_new(Some("Europe/Paris")).unwrap();
    let values = move |name: &str| match name {
        "d" => Series::new("n".into(), [0, i32::MAX]).cast(&DataType::Date),
        "ms" => Series::new("n".into(), [0, i64::MIN + 1])
            .cast(&DataType::Datetime(TimeUnit::Milliseconds, None)),
        _ => Series::new("n".into(), [0, i64::MIN + 1])
            .cast(&DataType::Datetime(TimeUnit::Microseconds, paris.clone())),
    };
    let scan = QualityConflictScan(Arc::new(move |files, _| {
        let n = values(&files[0])?;
        Ok(DataFrame::new_infer_height(vec![n.into()])?.lazy())
    }));
    let mut files: Vec<QualityFileEvidence> = ["d", "ms", "us_tz"]
        .into_iter()
        .enumerate()
        .map(|(i, name)| QualityFileEvidence {
            number: i + 1,
            name: name.to_string(),
            rows: 2,
            stored_type: None,
            examples: Vec::new(),
        })
        .collect();
    let watch = QualityWatch::new(|_| {});
    for streaming in [false, true] {
        read_conflict_examples(&scan, "n", &mut files, streaming, &watch);
        let examples: Vec<&[String]> = files.iter().map(|f| f.examples.as_slice()).collect();
        assert_eq!(
            examples,
            [
                ["1970-01-01", "2147483647 days since 1970-01-01"],
                [
                    "1970-01-01 00:00:00.000",
                    "-9223372036854775807 ms since 1970-01-01 UTC"
                ],
                [
                    "1970-01-01 01:00:00.000000+01:00",
                    "-9223372036854775807 us since 1970-01-01 UTC"
                ],
            ]
        );
    }
}

/// The values a type conflict hides are read a file at a time, so a cancel stops
/// between files on either engine and in any build.
#[test]
fn the_conflict_read_stops_between_files_on_any_engine() {
    let lf = df!("id" => [1i64, 2, 3]).unwrap().lazy();
    let source = QualitySourceContext {
        conflict_scan: Some(QualityConflictScan(Arc::new(|_, _| {
            Ok(df!("id" => ["1"]).unwrap().lazy())
        }))),
        ..QualitySourceContext::default()
    };
    let plan = DataQualityPlan {
        compute: QualityCompute::Full,
        ..DataQualityPlan::default()
    };
    let stages = Arc::new(std::sync::Mutex::new(Vec::new()));
    let seen = Arc::clone(&stages);
    let watch = QualityWatch::new(move |phase| seen.lock().unwrap().push(phase));
    let (results, _) =
        compute_data_quality_watched(&lf, Some(3), &plan, Some(&source), false, None, &watch);
    results.unwrap();
    let stages = stages.lock().unwrap().clone();
    let conflicts = stages
        .iter()
        .find(|phase| phase.stage == QualityStage::ReadingConflicts)
        .unwrap_or_else(|| panic!("{stages:?}"));
    assert!(conflicts.interruptible, "{stages:?}");
}

/// A finished full run counts the rows every pass traversed; a sampled run the
/// rows its sampler streamed; a run that uses rows already read reads nothing.
#[test]
fn a_run_counts_the_rows_its_reads_traverse() {
    let (_dir, lf) = csv_source(10_000);
    let full = DataQualityPlan {
        compute: QualityCompute::Full,
        ..DataQualityPlan::default()
    };
    let watch = QualityWatch::default();
    let (results, _) =
        compute_data_quality_watched(&lf, Some(10_000), &full, None, true, None, &watch);
    let reads = results.unwrap().reads.unwrap();
    assert_eq!(reads.reads, reads.counted, "every pass counted: {reads:?}");
    assert!(reads.reads >= 2, "{reads:?}");
    assert_eq!(reads.rows % 10_000, 0, "whole passes: {reads:?}");
    assert!(reads.rows >= 2 * 10_000, "{reads:?}");

    let sampled = DataQualityPlan {
        dataset_rows: 100,
        ..DataQualityPlan::default()
    };
    let (results, kept) = compute_data_quality_watched(
        &lf,
        Some(10_000),
        &sampled,
        None,
        true,
        None,
        &QualityWatch::default(),
    );
    assert_eq!(
        results.unwrap().reads,
        Some(ObservedReads {
            reads: 1,
            counted: 1,
            rows: 10_000,
            copy: None,
        }),
        "the sampler streamed the scope once"
    );
    let (results, _) = compute_data_quality_watched(
        &lf,
        Some(10_000),
        &sampled,
        None,
        true,
        kept.as_ref(),
        &QualityWatch::default(),
    );
    assert_eq!(results.unwrap().reads, Some(ObservedReads::default()));
}

/// Wide, nearly unique text: a report keeps only bounded pieces of it (examples
/// cut short, at most 100 spelling groups), and the memory budget weighs every
/// piece it keeps, the spellings' full text included.
#[test]
fn a_report_on_wide_text_is_weighed_by_the_text_it_holds() {
    let wide = "x".repeat(2_000);
    let rows = 600;
    let names = (0..rows)
        .map(|row| {
            let name = format!("Vendor {:04} {wide}", row / 2);
            if row % 2 == 0 {
                name
            } else {
                name.to_uppercase()
            }
        })
        .collect::<Vec<_>>();
    let df = df!(
        "id" => (0..rows as i64).collect::<Vec<_>>(),
        "name" => names,
        "note" => (0..rows).map(|row| format!("{row} {wide}")).collect::<Vec<_>>(),
    )
    .unwrap();
    for compute in [QualityCompute::Sample, QualityCompute::Full] {
        let plan = DataQualityPlan {
            compute,
            dataset_rows: rows,
            ..DataQualityPlan::default()
        };
        let results = measure(&df.clone().lazy(), None, &plan);
        assert_eq!(results.category_variants.len(), 100, "{compute:?}");
        let spellings = results
            .category_variants
            .iter()
            .map(|group| {
                group.column.len()
                    + group.normalized.len()
                    + group
                        .variants
                        .iter()
                        .map(|(variant, _)| variant.len())
                        .sum::<usize>()
            })
            .sum::<usize>();
        assert!(spellings > 100 * 3 * 2_000, "{spellings}");
        // Each group's finding names its spelling again.
        let spellings = spellings
            + results
                .observations
                .iter()
                .filter_map(|observation| observation.normalized_category.as_ref())
                .map(String::len)
                .sum::<usize>();
        assert!(
            results.estimated_bytes() >= spellings,
            "{compute:?}: {} bytes budgeted for {spellings} of text",
            results.estimated_bytes()
        );
        for value in results.examples.iter().flat_map(|found| &found.values) {
            assert!(crate::glyphs::display_width(value) <= 26, "{value}");
        }
    }
}

/// A grain finer than a report can show: past 1,000,000 keys the count the
/// sampling pass takes is dropped rather than grown with the table, the run
/// names the remedy, and the rows the pass read are kept, so a coarser grain
/// reads nothing. A count pass of its own stops at the same limit.
#[test]
fn a_count_past_a_million_keys_gives_up_and_keeps_the_rows() {
    let rows = crate::sampling::MAX_COUNTED_KEYS + 1;
    let df = df!("id" => (0..rows as i64).collect::<Vec<_>>()).unwrap();
    let read = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counter = Arc::clone(&read);
    let lf = df.lazy().filter(col("id").map(
        move |column| {
            counter.fetch_add(column.len(), std::sync::atomic::Ordering::Relaxed);
            Ok(column.is_not_null().into_column())
        },
        |_, field| Ok(Field::new(field.name().clone(), DataType::Boolean)),
    ));
    let by_id = DataQualityPlan {
        dataset_rows: 1_000,
        grain: QualityGrain::Partition("id".into()),
        ..DataQualityPlan::default()
    };
    let watch = QualityWatch::default();
    let (results, kept) =
        compute_data_quality_watched(&lf, None, &by_id, None, false, None, &watch);
    let error = results.unwrap_err().to_string();
    assert!(
        error.contains("More than 1,000,000 segments") && error.contains("coarser grain"),
        "{error}"
    );
    let kept = kept.expect("the rows the pass read are kept");
    assert_eq!(kept.df.height(), 1_000);
    assert_eq!(kept.segment_count(&by_id), SegmentCount::TooMany);
    assert!(
        kept.estimated_bytes() < 1_000_000,
        "no map of a million keys kept"
    );

    read.store(0, std::sync::atomic::Ordering::Relaxed);
    let dataset = DataQualityPlan {
        grain: QualityGrain::Dataset,
        ..by_id.clone()
    };
    let (results, _) =
        compute_data_quality_kept(&lf, None, &dataset, None, false, Some(&kept)).unwrap();
    assert_eq!(results.evaluated_rows, 1_000);
    assert_eq!(read.load(std::sync::atomic::Ordering::Relaxed), 0);

    // Rows kept without a count, a first-rows sample, count the grain in a pass
    // of their own, which gives up at the same limit.
    let head = DataQualityPlan {
        method: crate::sampling::SampleMethod::FirstRows,
        ..by_id
    };
    let (results, kept) = compute_data_quality_watched(&lf, None, &head, None, false, None, &watch);
    let error = results.unwrap_err().to_string();
    assert!(error.contains("More than 1,000,000 segments"), "{error}");
    let kept = kept.expect("the head is kept");
    assert_eq!(kept.segment_count(&head), SegmentCount::TooMany);
}

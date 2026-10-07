use super::*;
use crate::analysis::data_quality::fixtures::measure;
use crate::analysis::data_quality::{DataQualityResults, QualityCompute, compute_data_quality};
use crate::analysis::quality_report::{Outcome, build_report, checks, coverage, describe};

fn fixture() -> DataFrame {
    df!(
            "id" => &[Some(1i64), Some(2), Some(2), Some(3), Some(4), Some(5), None],
            "status" => &[Some("open"), Some("closed"), Some("open"), Some("void"), Some("Open"), None, Some("closed")],
            "amount" => &[5.0f64, 50.0, -1.0, 20.0, 200.0, 10.0, 0.0],
            "code" => &["1", "2", "x", "4", "5", "6", "7"],
        )
        .unwrap()
}

fn declared() -> DeclaredIntent {
    DeclaredIntent {
        key: vec!["id".to_string()],
        columns: vec![
            ColumnIntent {
                required: true,
                allowed: vec!["open".to_string(), "closed".to_string()],
                ..ColumnIntent::new("status")
            },
            ColumnIntent {
                min: Some("0".to_string()),
                max: Some("100".to_string()),
                ..ColumnIntent::new("amount")
            },
            ColumnIntent {
                number: Some(NumberReading::Whole),
                min: Some("2".to_string()),
                ..ColumnIntent::new("code")
            },
        ],
    }
}

fn plan(compute: QualityCompute) -> DataQualityPlan {
    DataQualityPlan {
        compute,
        intent: declared(),
        ..DataQualityPlan::default()
    }
}

fn run(df: &DataFrame, plan: &DataQualityPlan) -> DataQualityResults {
    measure(&df.clone().lazy(), Some(df.height()), plan)
}

fn affected(results: &DataQualityResults, kind: ObservationKind, column: &str) -> usize {
    results
        .observations
        .iter()
        .find(|observation| observation.kind == kind && observation.column == column)
        .map_or(0, |observation| observation.affected_rows)
}

/// Rows the evidence predicate picks out of `df`: the rows the finding counted.
fn matching(
    df: &DataFrame,
    results: &DataQualityResults,
    kind: ObservationKind,
    column: &str,
) -> usize {
    let predicate = results
        .observations
        .iter()
        .find(|observation| observation.kind == kind && observation.column == column)
        .and_then(|observation| observation.evidence_predicate(results))
        .expect("a predicate");
    df.clone()
        .lazy()
        .filter(predicate)
        .collect()
        .unwrap()
        .height()
}

/// Every rule on a full read, counted exactly, its rows found by its predicate.
#[test]
fn a_full_read_counts_every_declared_rule() {
    let df = fixture();
    let results = run(&df, &plan(QualityCompute::Full));
    let intent = results.intent.as_ref().expect("intent measured");
    assert!(intent.measured);
    assert_eq!(intent.precision, QualityPrecision::Exact);
    let key = intent.key.as_ref().unwrap();
    assert_eq!(
        (key.missing, key.groups, key.extra_rows, key.rows_involved),
        (1, 1, 1, 2)
    );
    let status = intent.column("status").unwrap();
    assert_eq!(status.missing, Some(1));
    assert_eq!(status.values, 6);
    assert_eq!(status.outside, Some(2));
    let amount = intent.column("amount").unwrap();
    assert_eq!((amount.below, amount.above), (Some(1), Some(1)));
    assert_eq!(amount.lowest.as_deref(), Some("-1.0"));
    assert_eq!(amount.highest.as_deref(), Some("200.0"));
    let code = intent.column("code").unwrap();
    assert_eq!(code.unparsed, Some(1));
    // The range compares what reads as a number: six of the seven.
    assert_eq!((code.compared, code.below), (Some(6), Some(1)));
    // A full read keeps no rows, so it lists no examples.
    assert!(status.outside_examples.is_empty());

    for (kind, column, count) in [
        (ObservationKind::KeyRepeated, "id", 2),
        (ObservationKind::KeyMissing, "id", 1),
        (ObservationKind::RequiredMissing, "status", 1),
        (ObservationKind::NotAllowed, "status", 2),
        (ObservationKind::OutOfRange, "amount", 2),
        (ObservationKind::UnparsedNumber, "code", 1),
        (ObservationKind::OutOfRange, "code", 1),
    ] {
        assert_eq!(affected(&results, kind, column), count, "{kind:?} {column}");
        assert_eq!(
            matching(&df, &results, kind, column),
            count,
            "{kind:?} {column}"
        );
    }

    // Problems, stated as facts, and the check names its reach.
    let report = build_report(&results);
    for kind in [
        ObservationKind::KeyRepeated,
        ObservationKind::KeyMissing,
        ObservationKind::RequiredMissing,
        ObservationKind::NotAllowed,
        ObservationKind::OutOfRange,
        ObservationKind::UnparsedNumber,
    ] {
        let finding = report
            .findings
            .iter()
            .find(|finding| finding.kind == Some(kind))
            .unwrap_or_else(|| panic!("{kind:?}"));
        assert_eq!(
            finding.severity,
            crate::analysis::quality_report::Severity::Problem
        );
    }
    let all = checks(&results, &report);
    assert_eq!(all[0].name, crate::analysis::quality_report::INTENT_CHECK);
    assert_eq!(all[0].basis, QualityPrecision::Exact);
    assert!(matches!(all[0].outcome, Outcome::Found { .. }));
    let repeated = report
        .findings
        .iter()
        .find(|finding| finding.kind == Some(ObservationKind::KeyRepeated))
        .unwrap();
    let (headline, _) = describe(repeated, &results);
    assert_eq!(
        headline,
        "2 of 7 rows (28.6%) share their key with another row"
    );
    // A key with no repeat on a full read leaves no limit behind.
    assert!(
        !coverage(&results, &all, &plan(QualityCompute::Full))
            .limits()
            .iter()
            .any(|limit| limit.contains("key"))
    );
}

/// A sample counts what its rows show, says so, and keeps examples from them.
#[test]
fn a_sample_counts_its_rows_and_says_what_it_cannot() {
    let ids = (0..2_000i64).map(|row| row % 1_000).collect::<Vec<_>>();
    let status = (0..2_000)
        .map(|row| if row % 10 == 0 { "lost" } else { "open" })
        .collect::<Vec<_>>();
    let df = df!("id" => ids, "status" => status).unwrap();
    let plan = DataQualityPlan {
        dataset_rows: 400,
        intent: DeclaredIntent {
            key: vec!["id".to_string()],
            columns: vec![ColumnIntent {
                allowed: vec!["open".to_string()],
                ..ColumnIntent::new("status")
            }],
        },
        ..DataQualityPlan::default()
    };
    let results = run(&df, &plan);
    assert_eq!(results.precision, QualityPrecision::Sampled);
    let intent = results.intent.as_ref().unwrap();
    assert_eq!(intent.precision, QualityPrecision::Sampled);
    assert_eq!(intent.evaluated_rows, 400);
    let status = intent.column("status").unwrap();
    let outside = status.outside.unwrap();
    assert!(outside > 0 && outside < 400);
    // The examples come from the rows in memory, with their counts.
    assert_eq!(status.outside_examples, vec![("lost".to_string(), outside)]);
    // Every id repeats once in the data; the sample holds some of the pairs, and
    // a repeat among its distinct rows is one in the data.
    let key = intent.key.as_ref().unwrap();
    assert!(key.rows_involved <= 400);
    assert_eq!(key.rows_involved, key.groups * 2);

    let report = build_report(&results);
    let all = checks(&results, &report);
    assert_eq!(all[0].basis, QualityPrecision::Sampled);
    let limits = coverage(&results, &all, &plan).limits();
    assert!(
        limits.contains(&"key repeats among 400 sampled rows only".to_string()),
        "{limits:?}"
    );
    let not_allowed = report
        .findings
        .iter()
        .find(|finding| finding.kind == Some(ObservationKind::NotAllowed))
        .unwrap();
    let (_, evidence) = describe(not_allowed, &results);
    assert!(
        evidence
            .iter()
            .any(|line| line.starts_with("Found: \"lost\"")),
        "{evidence:?}"
    );
    if key.groups > 0 {
        let repeated = report
            .findings
            .iter()
            .find(|finding| finding.kind == Some(ObservationKind::KeyRepeated))
            .unwrap();
        let (headline, evidence) = describe(repeated, &results);
        assert!(headline.contains("of 400 sampled rows"), "{headline}");
        assert!(evidence.iter().any(|line| line.contains("not checked")));
    }
}

/// No repeat in a sample is not a unique key: the check passes on the sampled
/// rows and the coverage says how far that reaches.
#[test]
fn a_sample_without_repeats_claims_only_its_rows() {
    let df = df!("id" => (0..5_000i64).collect::<Vec<_>>()).unwrap();
    let plan = DataQualityPlan {
        dataset_rows: 500,
        intent: DeclaredIntent {
            key: vec!["id".to_string()],
            columns: Vec::new(),
        },
        ..DataQualityPlan::default()
    };
    let results = run(&df, &plan);
    let report = build_report(&results);
    assert!(
        !report
            .findings
            .iter()
            .any(|finding| finding.kind == Some(ObservationKind::KeyRepeated))
    );
    let all = checks(&results, &report);
    assert_eq!(all[0].outcome, Outcome::Passed);
    assert_eq!(all[0].basis, QualityPrecision::Sampled);
    assert!(
        coverage(&results, &all, &plan)
            .limits()
            .contains(&"key repeats among 500 sampled rows only".to_string())
    );
}

/// Declaring a column the key answers what "Nearly unique" could only suggest.
#[test]
fn a_declared_key_replaces_the_nearly_unique_note() {
    let ids = (0..100i64)
        .map(|row| if row == 99 { 0 } else { row })
        .collect::<Vec<_>>();
    let df = df!("id" => ids).unwrap();
    let undeclared = run(
        &df,
        &DataQualityPlan {
            compute: QualityCompute::Full,
            ..DataQualityPlan::default()
        },
    );
    assert!(
        undeclared
            .observations
            .iter()
            .any(|o| o.kind == ObservationKind::KeyLike)
    );
    let declared = run(
        &df,
        &DataQualityPlan {
            compute: QualityCompute::Full,
            intent: DeclaredIntent {
                key: vec!["id".to_string()],
                columns: Vec::new(),
            },
            ..DataQualityPlan::default()
        },
    );
    assert!(
        !declared
            .observations
            .iter()
            .any(|o| o.kind == ObservationKind::KeyLike)
    );
    assert_eq!(affected(&declared, ObservationKind::KeyRepeated, "id"), 2);
}

/// A composite key repeats only where every part does.
#[test]
fn a_composite_key_repeats_where_all_its_parts_do() {
    let df = df!(
            "region" => &[Some("east"), Some("east"), Some("west"), Some("west"), Some("east"), Some("east"), None],
            "id" => &[Some(1i64), Some(2), Some(1), Some(1), None, None, Some(1)],
        )
        .unwrap();
    let results = run(
        &df,
        &DataQualityPlan {
            compute: QualityCompute::Full,
            intent: DeclaredIntent {
                key: vec!["region".to_string(), "id".to_string()],
                columns: Vec::new(),
            },
            ..DataQualityPlan::default()
        },
    );
    let key = results.intent.as_ref().unwrap().key.clone().unwrap();
    // Rows missing a part are incomplete, never a repeat of each other.
    assert_eq!((key.groups, key.rows_involved, key.missing), (1, 2, 3));
    assert_eq!(
        matching(&df, &results, ObservationKind::KeyRepeated, "region"),
        2
    );
    assert_eq!(
        matching(&df, &results, ObservationKind::KeyMissing, "id"),
        3
    );
    // One finding names both columns.
    let report = build_report(&results);
    let repeated = report
        .findings
        .iter()
        .find(|f| f.kind == Some(ObservationKind::KeyRepeated))
        .unwrap();
    assert_eq!(repeated.columns, vec!["region", "id"]);
}

/// Values not read, nothing checked: the check says so rather than passing.
#[test]
fn metadata_only_leaves_the_intent_unavailable() {
    let results = run(&fixture(), &plan(QualityCompute::Metadata));
    let intent = results.intent.as_ref().unwrap();
    assert!(!intent.measured);
    let report = build_report(&results);
    let all = checks(&results, &report);
    assert_eq!(all[0].outcome, Outcome::Unavailable("values not read"));
    assert!(intent.observations().is_empty());
}

/// Dates and times compare as instants, text read as time through its format.
#[test]
fn ranges_compare_dates_and_text_read_as_time() {
    let df = df!(
        "day" => &["2024-01-01", "2024-02-15", "2023-12-31", "bad"],
    )
    .unwrap();
    let mut plan = DataQualityPlan {
        compute: QualityCompute::Full,
        intent: DeclaredIntent {
            key: Vec::new(),
            columns: vec![ColumnIntent {
                min: Some("2024-01-01".to_string()),
                max: Some("2024-01-31".to_string()),
                ..ColumnIntent::new("day")
            }],
        },
        ..DataQualityPlan::default()
    };
    crate::analysis::analysis_modal::set_time_format(
        &mut plan,
        "day",
        Some((TimeKind::Date, "%Y-%m-%d")),
    );
    let results = run(&df, &plan);
    let day = results
        .intent
        .as_ref()
        .unwrap()
        .column("day")
        .unwrap()
        .clone();
    assert_eq!(
        (day.compared, day.below, day.above),
        (Some(3), Some(1), Some(1))
    );
    assert_eq!(day.lowest.as_deref(), Some("2023-12-31"));
    assert_eq!(
        matching(&df, &results, ObservationKind::OutOfRange, "day"),
        2
    );
}

/// A date or datetime past the calendar is the furthest out of range a value can
/// be, and counts so, where its conversion to microseconds overflowed and the
/// rule never compared it (#518). Values in range count as they did.
#[test]
fn a_date_past_the_calendar_is_out_of_range() {
    const DAY_MS: i64 = 86_400_000;
    let paris = TimeZone::opt_try_new(Some("Europe/Paris")).unwrap();
    let df = df!(
        "d" => &[Some(19_737i32), Some(19_000), Some(i32::MAX), Some(i32::MIN), None],
        "ms" => &[
            Some(19_737 * DAY_MS),
            Some(19_000 * DAY_MS),
            Some(i64::MAX),
            Some(i64::MIN + 1),
            None,
        ],
        "us" => &[
            Some(19_737 * DAY_MS * 1000),
            Some(19_000 * DAY_MS * 1000),
            Some(i64::MAX),
            Some(i64::MIN + 1),
            None,
        ],
    )
    .unwrap()
    .lazy()
    .with_columns([
        col("d").cast(DataType::Date),
        col("ms").cast(DataType::Datetime(TimeUnit::Milliseconds, None)),
        col("us").cast(DataType::Datetime(TimeUnit::Microseconds, paris)),
    ])
    .collect()
    .unwrap();
    let plan = DataQualityPlan {
        compute: QualityCompute::Full,
        intent: DeclaredIntent {
            key: Vec::new(),
            columns: ["d", "ms", "us"]
                .into_iter()
                .map(|column| ColumnIntent {
                    min: Some("2024-01-01".to_string()),
                    max: Some("2024-12-31".to_string()),
                    ..ColumnIntent::new(column)
                })
                .collect(),
        },
        ..DataQualityPlan::default()
    };
    for streaming in [false, true] {
        let results = compute_data_quality(
            &df.clone().lazy(),
            Some(df.height()),
            &plan,
            None,
            streaming,
        )
        .unwrap();
        for (column, unit) in [("d", "days"), ("ms", "ms"), ("us", "us")] {
            let check = results.intent.as_ref().unwrap().column(column).unwrap();
            // 2024-01-15 in range; 2022-01-08 and the two past the calendar out.
            assert_eq!(
                (check.compared, check.below, check.above),
                (Some(4), Some(2), Some(1)),
                "{column}"
            );
            let (low, high) = match unit {
                "days" => (i64::from(i32::MIN), i64::from(i32::MAX)),
                _ => (i64::MIN + 1, i64::MAX),
            };
            let since = |v: i64| match unit {
                "days" => format!("{v} days since 1970-01-01"),
                unit => format!("{v} {unit} since 1970-01-01 UTC"),
            };
            assert_eq!(check.lowest, Some(since(low)), "{column}");
            assert_eq!(check.highest, Some(since(high)), "{column}");
            assert_eq!(
                matching(&df, &results, ObservationKind::OutOfRange, column),
                3,
                "{column}"
            );
        }
    }
}

#[test]
fn allowed_values_are_split_trimmed_and_checked_against_the_type() {
    assert_eq!(
        parse_allowed(&DataType::String, " open, closed ,,open ").unwrap(),
        vec!["open", "closed"]
    );
    assert!(parse_allowed(&DataType::Int64, "1, two").is_err());
    assert_eq!(
        parse_allowed(&DataType::Boolean, "true").unwrap(),
        vec!["true"]
    );
    let many = (0..=MAX_ALLOWED_VALUES)
        .map(|value| value.to_string())
        .collect::<Vec<_>>()
        .join(",");
    assert!(parse_allowed(&DataType::String, &many).is_err());
}

/// An allowed set compares at the column's own type: a `u64` past `i64::MAX` is
/// itself, and a category is compared with its name.
#[test]
fn an_allowed_set_compares_at_the_columns_type() {
    let mut df = df!(
        "code" => &[u64::MAX, 1, u64::MAX - 1, 2],
        "kind" => &["open", "closed", "open", "lost"],
    )
    .unwrap();
    df = df
        .lazy()
        .with_column(col("kind").cast(DataType::from_categories(Categories::global())))
        .collect()
        .unwrap();
    assert!(parse_allowed(&DataType::UInt64, &u64::MAX.to_string()).is_ok());
    assert!(parse_allowed(&DataType::UInt64, "-1").is_err());
    let plan = DataQualityPlan {
        compute: QualityCompute::Full,
        intent: DeclaredIntent {
            key: Vec::new(),
            columns: vec![
                ColumnIntent {
                    allowed: vec![u64::MAX.to_string(), "1".into()],
                    ..ColumnIntent::new("code")
                },
                ColumnIntent {
                    allowed: vec!["open".into(), "closed".into()],
                    ..ColumnIntent::new("kind")
                },
            ],
        },
        ..DataQualityPlan::default()
    };
    let results = run(&df, &plan);
    let intent = results.intent.as_ref().unwrap();
    assert_eq!(intent.column("code").unwrap().outside, Some(2));
    assert_eq!(intent.column("kind").unwrap().outside, Some(1));
    assert_eq!(
        matching(&df, &results, ObservationKind::NotAllowed, "code"),
        2
    );
    assert_eq!(
        matching(&df, &results, ObservationKind::NotAllowed, "kind"),
        1
    );
}

/// A quoted value keeps its commas, spaces and doubled quotes, and is written
/// back quoted, so reopening the form reads the same set.
#[test]
fn a_quoted_allowed_value_holds_commas_and_spaces() {
    let typed = r#""a, b", c, " open", "say ""hi""", 5" pipe"#;
    let values = parse_allowed(&DataType::String, typed).unwrap();
    assert_eq!(values, vec!["a, b", "c", " open", "say \"hi\"", "5\" pipe"]);
    assert_eq!(
        parse_allowed(&DataType::String, &format_allowed(&values)).unwrap(),
        values
    );
    assert_eq!(
        parse_allowed(&DataType::Int64, r#""1", 2"#).unwrap(),
        vec!["1", "2"]
    );
    assert!(parse_allowed(&DataType::String, r#""a, b"#).is_err());
    assert!(parse_allowed(&DataType::String, r#""a" b, c"#).is_err());

    // Compared as stored: case and spaces count.
    let df = df!("label" => &["a, b", "c", " open", "open", "A, B"]).unwrap();
    let plan = DataQualityPlan {
        compute: QualityCompute::Full,
        intent: DeclaredIntent {
            key: Vec::new(),
            columns: vec![ColumnIntent {
                allowed: values,
                ..ColumnIntent::new("label")
            }],
        },
        ..DataQualityPlan::default()
    };
    let results = run(&df, &plan);
    let label = results.intent.as_ref().unwrap().column("label").unwrap();
    assert_eq!(label.outside, Some(2));
    assert_eq!(
        matching(&df, &results, ObservationKind::NotAllowed, "label"),
        2
    );
}

/// A date alone as a time's maximum keeps that whole day; as its minimum, the day
/// starts at midnight. Both bounds are in range.
#[test]
fn a_date_bounds_a_datetime_column_by_whole_days() {
    let at = |text: &str| {
        chrono::NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S")
            .unwrap()
            .and_utc()
            .timestamp_micros()
    };
    let df = df!(
        "at" => &[
            at("2024-06-01 00:00:00"),
            at("2024-06-30 23:59:59"),
            at("2024-07-01 00:00:00"),
            at("2024-05-31 23:59:59"),
        ],
    )
    .unwrap()
    .lazy()
    .with_column(col("at").cast(DataType::Datetime(TimeUnit::Microseconds, None)))
    .collect()
    .unwrap();
    let plan = DataQualityPlan {
        compute: QualityCompute::Full,
        intent: DeclaredIntent {
            key: Vec::new(),
            columns: vec![ColumnIntent {
                min: Some("2024-06-01".to_string()),
                max: Some("2024-06-30".to_string()),
                ..ColumnIntent::new("at")
            }],
        },
        ..DataQualityPlan::default()
    };
    let results = run(&df, &plan);
    let check = results.intent.as_ref().unwrap().column("at").unwrap();
    assert_eq!((check.below, check.above), (Some(1), Some(1)));
    // The same day as both ends is a day, not an empty range.
    let one_day = ColumnIntent {
        min: Some("2024-06-30".to_string()),
        max: Some("2024-06-30".to_string()),
        ..ColumnIntent::new("at")
    };
    assert!(
        check_intent(
            &one_day,
            &DataType::Datetime(TimeUnit::Microseconds, None),
            None
        )
        .is_ok()
    );
}

#[test]
fn a_range_needs_bounds_of_the_columns_kind_in_order() {
    let number = ColumnIntent {
        min: Some("10".to_string()),
        max: Some("2".to_string()),
        ..ColumnIntent::new("amount")
    };
    assert_eq!(
        check_intent(&number, &DataType::Float64, None),
        Err("Minimum is above maximum".to_string())
    );
    let date = ColumnIntent {
        min: Some("yesterday".to_string()),
        ..ColumnIntent::new("day")
    };
    assert!(check_intent(&date, &DataType::Date, None).is_err());
    let text = ColumnIntent {
        min: Some("a".to_string()),
        ..ColumnIntent::new("name")
    };
    assert!(check_intent(&text, &DataType::String, None).is_err());
    // Text read as a number takes a numeric range.
    let parsed = ColumnIntent {
        number: Some(NumberReading::Decimal),
        min: Some("0".to_string()),
        ..ColumnIntent::new("price")
    };
    assert_eq!(check_intent(&parsed, &DataType::String, None), Ok(()));
}

#[test]
fn an_empty_intent_removes_the_column() {
    let mut declared = DeclaredIntent::default();
    declared.set(ColumnIntent {
        required: true,
        ..ColumnIntent::new("id")
    });
    assert_eq!(declared.columns.len(), 1);
    declared.set(ColumnIntent::new("id"));
    assert!(declared.is_empty());
    declared.set_key("id", true);
    declared.set_key("id", true);
    assert_eq!(declared.key, vec!["id"]);
    declared.set_key("id", false);
    assert!(declared.is_empty());
}

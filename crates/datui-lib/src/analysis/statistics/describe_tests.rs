use super::*;

/// A frame with one column of each temporal type, a zoned datetime too, five
/// values and a null each.
pub(crate) fn temporal_frame() -> DataFrame {
    let day = 20_089i32; // 2025-01-01
    let dates = Series::new(
        "day".into(),
        &[
            Some(day + 4),
            Some(day),
            None,
            Some(day + 2),
            Some(day + 1),
            Some(day + 3),
        ],
    )
    .cast(&DataType::Date)
    .unwrap();
    let hour = 3_600_000_000i64; // microseconds
    let start = 1_735_678_075_000_000i64; // 2024-12-31 20:47:55
    let pickups = Series::new(
        "pickup".into(),
        &[
            Some(start + 4 * hour),
            Some(start),
            None,
            Some(start + 2 * hour),
            Some(start + hour),
            Some(start + 3 * hour),
        ],
    )
    .cast(&DataType::Datetime(TimeUnit::Microseconds, None))
    .unwrap();
    let second = 1_000_000_000i64; // nanoseconds
    let times = Series::new(
        "at".into(),
        &[
            Some(9 * 3600 * second + 40 * second),
            Some(9 * 3600 * second),
            None,
            Some(9 * 3600 * second + 20 * second),
            Some(9 * 3600 * second + 10 * second),
            Some(9 * 3600 * second + 30 * second),
        ],
    )
    .cast(&DataType::Time)
    .unwrap();
    // The same instants in New York, in milliseconds: the zone survives the cast back.
    let local = Series::new(
        "local".into(),
        &[
            Some(start / 1000 + 4 * hour / 1000),
            Some(start / 1000),
            None,
            Some(start / 1000 + 2 * hour / 1000),
            Some(start / 1000 + hour / 1000),
            Some(start / 1000 + 3 * hour / 1000),
        ],
    )
    .cast(&DataType::Datetime(
        TimeUnit::Milliseconds,
        TimeZone::opt_try_new(Some("America/New_York")).unwrap(),
    ))
    .unwrap();
    let minute = 60_000i64; // milliseconds
    let waits = Series::new(
        "wait".into(),
        &[
            Some(5 * minute),
            Some(minute),
            None,
            Some(3 * minute),
            Some(2 * minute),
            Some(4 * minute),
        ],
    )
    .cast(&DataType::Duration(TimeUnit::Milliseconds))
    .unwrap();
    DataFrame::new_infer_height(vec![
        dates.into(),
        pickups.into(),
        times.into(),
        local.into(),
        waits.into(),
    ])
    .unwrap()
}

#[test]
fn describe_gives_dates_and_times_their_range_in_their_own_format() {
    let df = temporal_frame();
    let every_row = crate::analysis::sampling::Sample {
        method: crate::analysis::sampling::SampleMethod::EveryRow,
        ..crate::analysis::sampling::Sample::default()
    };
    let lazy = compute_describe_from_lazy(&df.clone().lazy(), Some(6), &every_row, false)
        .unwrap()
        .column_statistics;
    let schema = df.schema().clone();
    let sampled = compute_describe_single_aggregation(&df, &schema, 6, None, false)
        .unwrap()
        .column_statistics;
    let expected = [
        [
            "2025-01-03",
            "2025-01-01",
            "2025-01-02",
            "2025-01-03",
            "2025-01-04",
            "2025-01-05",
        ],
        [
            "2024-12-31 22:47:55",
            "2024-12-31 20:47:55",
            "2024-12-31 21:47:55",
            "2024-12-31 22:47:55",
            "2024-12-31 23:47:55",
            "2025-01-01 00:47:55",
        ],
        [
            "09:00:20", "09:00:00", "09:00:10", "09:00:20", "09:00:30", "09:00:40",
        ],
        [
            "2024-12-31 17:47:55 EST",
            "2024-12-31 15:47:55 EST",
            "2024-12-31 16:47:55 EST",
            "2024-12-31 17:47:55 EST",
            "2024-12-31 18:47:55 EST",
            "2024-12-31 19:47:55 EST",
        ],
        ["3m", "1m", "2m", "3m", "4m", "5m"],
    ];
    for stats in [&lazy, &sampled] {
        assert_eq!(stats.len(), expected.len());
        for (column, want) in stats.iter().zip(expected) {
            assert!(column.numeric_stats.is_none(), "{}", column.name);
            assert_eq!(column.null_count, 1);
            let t = column.temporal_stats.as_ref().expect("temporal stats");
            let got = [&t.mean, &t.min, &t.q25, &t.median, &t.q75, &t.max]
                .map(|v| v.clone().unwrap_or_default());
            assert_eq!(got, want.map(String::from), "{}", column.name);
        }
    }
}

#[test]
fn describe_of_an_all_null_datetime_is_empty() {
    let empty = Series::new("never".into(), &[None::<i64>, None])
        .cast(&DataType::Datetime(TimeUnit::Microseconds, None))
        .unwrap();
    let df = DataFrame::new_infer_height(vec![empty.into()]).unwrap();
    let schema = df.schema().clone();
    let stats = compute_describe_single_aggregation(&df, &schema, 2, None, false)
        .unwrap()
        .column_statistics;
    let t = stats[0].temporal_stats.as_ref().expect("temporal stats");
    assert!(
        [&t.mean, &t.min, &t.q25, &t.median, &t.q75, &t.max]
            .iter()
            .all(|v| v.is_none())
    );
}

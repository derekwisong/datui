use super::*;

fn tz(name: &str) -> Option<TimeZone> {
    TimeZone::opt_try_new(Some(name)).unwrap()
}

fn datetime(unit: TimeUnit, zone: Option<&str>) -> DataType {
    DataType::Datetime(unit, zone.and_then(tz))
}

fn stored(text: &str, dtype: &DataType) -> i64 {
    match parse(text, dtype).unwrap().into_value() {
        AnyValue::DatetimeOwned(v, ..) | AnyValue::Duration(v, _) | AnyValue::Time(v) => v,
        AnyValue::Date(days) => i64::from(days),
        other => panic!("{other:?}"),
    }
}

#[test]
fn dates_and_times_read_in_their_forms() {
    assert_eq!(stored("2024-01-01", &DataType::Date), 19723);
    assert_eq!(
        stored("-800000 days since 1970-01-01", &DataType::Date),
        -800000
    );
    let us = datetime(TimeUnit::Microseconds, None);
    let midnight = 1_704_067_200_000_000;
    assert_eq!(stored("2024-01-01", &us), midnight);
    assert_eq!(
        stored("2024-01-01 05:00", &us),
        midnight + 5 * 3_600_000_000
    );
    assert_eq!(
        stored("2024-01-01T05:00:00", &us),
        midnight + 5 * 3_600_000_000
    );
    assert_eq!(
        stored("2024-01-01 05:00:00.25", &us),
        midnight + 5 * 3_600_000_000 + 250_000
    );
    // A clock in the column's zone: 05:00 in Paris is 04:00 UTC.
    let paris = datetime(TimeUnit::Microseconds, Some("Europe/Paris"));
    assert_eq!(
        stored("2024-01-01 05:00", &paris),
        midnight + 4 * 3_600_000_000
    );
    assert_eq!(
        stored("2024-01-01 05:00+00:00", &paris),
        midnight + 5 * 3_600_000_000
    );
    assert_eq!(
        stored("2024-01-01T05:00:00Z", &paris),
        midnight + 5 * 3_600_000_000
    );
    assert_eq!(
        stored("05:06", &DataType::Time),
        (5 * 3600 + 6 * 60) * 1_000_000_000
    );
    assert_eq!(
        stored("05:06:07.5", &DataType::Time),
        (5 * 3600 + 6 * 60 + 7) * 1_000_000_000 + 500_000_000
    );
    let ms = DataType::Duration(TimeUnit::Milliseconds);
    assert_eq!(stored("1d 2h", &ms), 26 * 3_600_000);
    assert_eq!(stored("-1m -30s", &ms), -90_000);
    assert_eq!(
        stored("1500µs", &DataType::Duration(TimeUnit::Microseconds)),
        1500
    );
}

#[test]
fn what_does_not_read_says_why() {
    let us = datetime(TimeUnit::Microseconds, None);
    let ms = datetime(TimeUnit::Milliseconds, None);
    let paris = datetime(TimeUnit::Microseconds, Some("Europe/Paris"));
    let err = |text: &str, dtype: &DataType| parse(text, dtype).unwrap_err();
    assert_eq!(
        err("2024-13-01", &DataType::Date),
        "\"2024-13-01\" is not a date written YYYY-MM-DD"
    );
    assert!(err("noon", &us).contains("is not a date and time"));
    assert!(err("2024-01-01 05:00:00.0001", &ms).contains("finer than the column's milliseconds"));
    assert!(err("2024-01-01 05:00+01:00", &us).contains("no time zone"));
    // Spring forward skips 02:30 in Paris; fall back repeats it.
    assert!(err("2024-03-31 02:30", &paris).contains("does not happen in Europe/Paris"));
    assert!(err("2024-10-27 02:30", &paris).contains("happens twice"));
    assert!(err("25:00", &DataType::Time).contains("is not a time"));
    assert!(
        err("1 fortnight", &DataType::Duration(TimeUnit::Microseconds))
            .contains("is not a duration")
    );
    assert!(err("1ns", &DataType::Duration(TimeUnit::Microseconds)).contains("finer"));
    assert!(err("abc", &DataType::Decimal(10, 2)).contains("is not a number"));
    assert!(err("yes", &DataType::Boolean).contains("true or false"));
}

#[test]
fn decimals_read_at_the_column_scale() {
    let dtype = DataType::Decimal(10, 2);
    assert_eq!(
        parse("1.5", &dtype).unwrap(),
        parse("1.50", &dtype).unwrap()
    );
    assert_eq!(
        parse("1.5", &dtype).unwrap().into_value(),
        AnyValue::Decimal(150, 10, 2)
    );
}

/// The text `+` and `-` write reads back to the very value, every type.
#[test]
fn every_value_reads_back_from_its_text() {
    let paris = tz("Europe/Paris");
    let cases: Vec<(DataType, AnyValue<'static>)> = vec![
        (DataType::Float64, AnyValue::Float64(0.1 + 0.2)),
        (DataType::Float32, AnyValue::Float32(0.1)),
        (DataType::Int8, AnyValue::Int8(-5)),
        (DataType::UInt64, AnyValue::UInt64(u64::MAX)),
        (DataType::Boolean, AnyValue::Boolean(true)),
        (DataType::Date, AnyValue::Date(19723)),
        (DataType::Date, AnyValue::Date(-800_000_000)),
        (
            DataType::Datetime(TimeUnit::Nanoseconds, None),
            AnyValue::DatetimeOwned(1_704_085_200_123_456_789, TimeUnit::Nanoseconds, None),
        ),
        (
            DataType::Datetime(TimeUnit::Microseconds, paris.clone()),
            AnyValue::DatetimeOwned(
                1_704_081_600_000_001,
                TimeUnit::Microseconds,
                paris.clone().map(Arc::new),
            ),
        ),
        // 02:30 on the night Paris falls back, the second time: only its offset tells it.
        (
            DataType::Datetime(TimeUnit::Microseconds, paris.clone()),
            AnyValue::DatetimeOwned(
                1_729_992_600_000_000,
                TimeUnit::Microseconds,
                paris.clone().map(Arc::new),
            ),
        ),
        (DataType::Time, AnyValue::Time(18_367_123_456_789)),
        (
            DataType::Duration(TimeUnit::Microseconds),
            AnyValue::Duration(93_784_000_005, TimeUnit::Microseconds),
        ),
        (
            DataType::Duration(TimeUnit::Nanoseconds),
            AnyValue::Duration(-1_500, TimeUnit::Nanoseconds),
        ),
        (DataType::Decimal(10, 2), AnyValue::Decimal(150, 10, 2)),
    ];
    for (dtype, value) in cases {
        let text = text_of(&value, &dtype).unwrap_or_else(|| panic!("{dtype} {value:?}"));
        assert!(same(&text, &dtype, &value), "{dtype}: {text}");
    }
}

#[test]
fn literals_read_back_in_python() {
    let py = |text: &str, dtype: DataType| python(&parse(text, &dtype).unwrap());
    assert_eq!(py("2024-01-01", DataType::Date), "pl.date(2024, 1, 1)");
    assert_eq!(
        py(
            "2024-01-01 05:00:00.25",
            datetime(TimeUnit::Microseconds, None)
        ),
        "pl.datetime(2024, 1, 1, 5, 0, 0, 250000, time_unit=\"us\")"
    );
    assert_eq!(
        py(
            "2024-01-01 05:00",
            datetime(TimeUnit::Microseconds, Some("Europe/Paris"))
        ),
        "pl.datetime(2024, 1, 1, 4, 0, 0, 0, time_unit=\"us\", time_zone=\"UTC\")\
             .dt.convert_time_zone(\"Europe/Paris\")"
    );
    assert_eq!(
        py(
            "2024-01-01 05:00:00.000000001",
            datetime(TimeUnit::Nanoseconds, None)
        ),
        "pl.lit(\"2024-01-01 05:00:00.000000001\").str.to_datetime(\"%Y-%m-%d %H:%M:%S%.f\", time_unit=\"ns\")"
    );
    assert_eq!(py("05:06:07.5", DataType::Time), "pl.time(5, 6, 7, 500000)");
    assert_eq!(
        py("1d", DataType::Duration(TimeUnit::Milliseconds)),
        "pl.duration(milliseconds=86400000, time_unit=\"ms\")"
    );
    assert_eq!(
        py("1.5", DataType::Decimal(10, 2)),
        "pl.lit(\"1.50\").cast(pl.Decimal(10, 2))"
    );
    assert_eq!(py("0.1", DataType::Float32), "0.10000000149011612");
}

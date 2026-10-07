//! Typed text read as a column's own type, so a filter or Data Quality partition stays
//! a plain `col op lit` comparison (comparing as text fails for dates, or casts every
//! row, blinding Parquet statistics and SQLite to the predicate).

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, TimeZone as _};
use polars::prelude::*;

/// The forms a value of `dtype` is written in, for a message about one that is not.
pub fn written_as(dtype: &DataType) -> &'static str {
    match dtype {
        DataType::Date => "a date written YYYY-MM-DD",
        DataType::Datetime(..) => "a date and time written YYYY-MM-DD HH:MM[:SS[.f]]",
        DataType::Time => "a time written HH:MM[:SS[.f]]",
        DataType::Duration(_) => "a duration such as 1d 2h 30m or 1500ms",
        DataType::Boolean => "true or false",
        dtype if dtype.is_integer() => "a whole number",
        _ => "a number",
    }
}

/// `text` as a literal of `dtype` (number, flag, date, zoned datetime, time, duration,
/// decimal at the column's scale); text and categories stay text. The error says the
/// expected form. Whole numbers read as `i64`/`u64` (so `< 300` works on `i8`); floats
/// at the column's precision.
pub fn parse(text: &str, dtype: &DataType) -> Result<Scalar, String> {
    let bad = || format!("{text:?} is not {}", written_as(dtype));
    let scalar = |value: AnyValue<'static>| Scalar::new(dtype.clone(), value);
    match dtype {
        DataType::String | DataType::Categorical(..) | DataType::Enum(..) => Ok(Scalar::new(
            DataType::String,
            AnyValue::StringOwned(text.into()),
        )),
        DataType::Boolean => text
            .parse()
            .map(|b| scalar(AnyValue::Boolean(b)))
            .map_err(|_| bad()),
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => text
            .parse()
            .map(|i| Scalar::new(DataType::Int64, AnyValue::Int64(i)))
            .map_err(|_| bad()),
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => text
            .parse()
            .map(|u| Scalar::new(DataType::UInt64, AnyValue::UInt64(u)))
            .map_err(|_| bad()),
        DataType::Float64 => text
            .parse()
            .map(|f| scalar(AnyValue::Float64(f)))
            .map_err(|_| bad()),
        DataType::Float32 => text
            .parse()
            .map(|f| scalar(AnyValue::Float32(f)))
            .map_err(|_| bad()),
        DataType::Date => parse_date(text.trim())
            .map(|days| scalar(AnyValue::Date(days)))
            .ok_or_else(bad),
        DataType::Datetime(unit, zone) => {
            let v = parse_datetime(text.trim(), *unit, zone.as_ref()).map_err(|why| {
                why.unwrap_or_else(bad)
                    .replace("{text}", &format!("{text:?}"))
            })?;
            Ok(scalar(AnyValue::DatetimeOwned(
                v,
                *unit,
                zone.clone().map(Arc::new),
            )))
        }
        DataType::Time => parse_time(text.trim())
            .map(|ns| scalar(AnyValue::Time(ns)))
            .ok_or_else(bad),
        DataType::Duration(unit) => {
            let ns = parse_duration(text.trim()).ok_or_else(bad)?;
            let v = in_unit(ns, *unit).ok_or_else(|| {
                format!("{text:?} is finer than the column's {}", unit_name(*unit))
            })?;
            Ok(scalar(AnyValue::Duration(v, *unit)))
        }
        DataType::Decimal(..) => {
            // Polars reads the text at the column's scale, as a cast of the column would.
            let read = Series::new(PlSmallStr::EMPTY, [text.trim()])
                .strict_cast(dtype)
                .map_err(|_| bad())?;
            match read.get(0).map_err(|_| bad())? {
                AnyValue::Null => Err(bad()),
                value => Ok(scalar(value.into_static())),
            }
        }
        _ => Err(format!("a {dtype} column has no value to compare with")),
    }
}

/// The text [`parse`] reads back to exactly `value` (what `+`/`-` filters use): floats
/// as shortest exact decimals, datetimes as their zoned clock to the last digit (with
/// offset when ambiguous). `None` for null or unsupported types.
pub fn text_of(value: &AnyValue, dtype: &DataType) -> Option<String> {
    if value.is_null() {
        return None;
    }
    let text = match (dtype, value) {
        (DataType::String, _) => value.get_str()?.to_string(),
        (DataType::Float64, AnyValue::Float64(f)) => crate::exact::f64_text(*f),
        (DataType::Float32, AnyValue::Float32(f)) => crate::exact::f32_text(*f),
        (DataType::Date | DataType::Time, _) => match crate::exact::out_of_range(value) {
            Some(stored) => stored,
            None => match value {
                AnyValue::Date(days) => epoch_date()
                    .checked_add_signed(chrono::TimeDelta::days(i64::from(*days)))?
                    .format("%Y-%m-%d")
                    .to_string(),
                AnyValue::Time(ns) => time_of(*ns)?.format("%H:%M:%S%.f").to_string(),
                _ => return None,
            },
        },
        (
            DataType::Datetime(unit, zone),
            AnyValue::Datetime(v, ..) | AnyValue::DatetimeOwned(v, ..),
        ) => {
            if let Some(stored) = crate::exact::out_of_range(value) {
                stored
            } else {
                let utc = utc_of(*v, *unit)?;
                let clock = "%Y-%m-%d %H:%M:%S%.f";
                match zone {
                    None => utc.format(clock).to_string(),
                    Some(zone) => {
                        let local = zone.to_chrono().ok()?.from_utc_datetime(&utc);
                        let plain = local.format(clock).to_string();
                        if same(&plain, dtype, value) {
                            plain
                        } else {
                            local.format("%Y-%m-%d %H:%M:%S%.f%:z").to_string()
                        }
                    }
                }
            }
        }
        (DataType::Duration(unit), AnyValue::Duration(v, _)) => {
            let shown = crate::exact::str_value(value).into_owned();
            if same(&shown, dtype, value) {
                shown
            } else {
                format!("{v}{}", unit_suffix(*unit))
            }
        }
        (
            DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Decimal(..)
            | DataType::Categorical(..)
            | DataType::Enum(..),
            _,
        ) => crate::exact::str_value(value).into_owned(),
        _ => return None,
    };
    let reads_back = matches!(dtype, DataType::Categorical(..) | DataType::Enum(..))
        || same(&text, dtype, value);
    reads_back.then_some(text)
}

/// Whether `text` reads back to `value`.
fn same(text: &str, dtype: &DataType, value: &AnyValue) -> bool {
    parse(text, dtype).is_ok_and(|read| {
        let read = read.into_value();
        match (&read, value) {
            // Read wide, stored narrow.
            (AnyValue::Int64(a), _) => value.extract::<i64>() == Some(*a),
            (AnyValue::UInt64(a), _) => value.extract::<u64>() == Some(*a),
            _ => read == value.clone().into_static(),
        }
    })
}

/// A literal as Python Polars: `pl.date(2024, 1, 1)`, `pl.datetime(...)`,
/// `pl.duration(...)`, a number, a string.
pub fn python(scalar: &Scalar) -> String {
    use crate::python_script::{py_bool, py_float, py_str};
    let unit_arg = |unit: TimeUnit| format!("time_unit={}", py_str(unit_name_short(unit)));
    match (scalar.dtype(), scalar.value()) {
        (_, AnyValue::Int64(i)) => i.to_string(),
        (_, AnyValue::UInt64(u)) => u.to_string(),
        (_, AnyValue::Float64(f)) => py_float(*f),
        (_, AnyValue::Float32(f)) => py_float(f64::from(*f)),
        (_, AnyValue::Boolean(b)) => py_bool(*b).to_string(),
        (_, AnyValue::StringOwned(s)) => py_str(s),
        (_, AnyValue::String(s)) => py_str(s),
        (DataType::Date, AnyValue::Date(days)) => {
            match epoch_date().checked_add_signed(chrono::TimeDelta::days(i64::from(*days))) {
                Some(date) if crate::exact::out_of_range(scalar.value()).is_none() => {
                    use chrono::Datelike;
                    format!("pl.date({}, {}, {})", date.year(), date.month(), date.day())
                }
                _ => format!("pl.lit({days}).cast(pl.Date)"),
            }
        }
        (DataType::Datetime(unit, zone), AnyValue::DatetimeOwned(v, ..)) => {
            let zone_arg = zone
                .as_ref()
                .map_or(String::new(), |_| ", time_zone=\"UTC\"".to_string());
            let convert = zone.as_ref().map_or(String::new(), |zone| {
                format!(".dt.convert_time_zone({})", py_str(zone))
            });
            match utc_of(*v, *unit) {
                Some(utc) if crate::exact::out_of_range(scalar.value()).is_none() => {
                    use chrono::{Datelike, Timelike};
                    let nanos = utc.nanosecond();
                    if nanos % 1000 == 0 {
                        format!(
                            "pl.datetime({}, {}, {}, {}, {}, {}, {}, {}{zone_arg}){convert}",
                            utc.year(),
                            utc.month(),
                            utc.day(),
                            utc.hour(),
                            utc.minute(),
                            utc.second(),
                            nanos / 1000,
                            unit_arg(*unit)
                        )
                    } else {
                        format!(
                            "pl.lit({}).str.to_datetime(\"%Y-%m-%d %H:%M:%S%.f\", {}{zone_arg}){convert}",
                            py_str(&utc.format("%Y-%m-%d %H:%M:%S%.f").to_string()),
                            unit_arg(*unit)
                        )
                    }
                }
                _ => format!(
                    "pl.lit({v}).cast(pl.Datetime({}{}))",
                    py_str(unit_name_short(*unit)),
                    zone.as_ref()
                        .map_or(String::new(), |zone| format!(", {}", py_str(zone)))
                ),
            }
        }
        (DataType::Time, AnyValue::Time(ns)) => match time_of(*ns) {
            Some(time) if ns % 1000 == 0 => {
                use chrono::Timelike;
                format!(
                    "pl.time({}, {}, {}, {})",
                    time.hour(),
                    time.minute(),
                    time.second(),
                    time.nanosecond() / 1000
                )
            }
            Some(time) => format!(
                "pl.lit({}).str.to_time(\"%H:%M:%S%.f\")",
                py_str(&time.format("%H:%M:%S%.f").to_string())
            ),
            None => format!("pl.lit({ns}).cast(pl.Time)"),
        },
        (DataType::Duration(unit), AnyValue::Duration(v, _)) => {
            let name = match unit {
                TimeUnit::Milliseconds => "milliseconds",
                TimeUnit::Microseconds => "microseconds",
                TimeUnit::Nanoseconds => "nanoseconds",
            };
            format!("pl.duration({name}={v}, {})", unit_arg(*unit))
        }
        (DataType::Decimal(precision, scale), value) => format!(
            "pl.lit({}).cast(pl.Decimal({precision}, {scale}))",
            py_str(&crate::exact::str_value(value))
        ),
        (_, value) => py_str(&crate::exact::str_value(value)),
    }
}

fn epoch_date() -> NaiveDate {
    NaiveDate::from_ymd_opt(1970, 1, 1).expect("the epoch is a date")
}

/// `YYYY-MM-DD`, or a date past the calendar as [`crate::exact::out_of_range`] writes
/// it: days since the epoch.
fn parse_date(text: &str) -> Option<i32> {
    if let Some(days) = text.strip_suffix(" days since 1970-01-01") {
        return days.trim().parse().ok();
    }
    let date = NaiveDate::parse_from_str(text, "%Y-%m-%d").ok()?;
    i32::try_from((date - epoch_date()).num_days()).ok()
}

/// `HH:MM`, `HH:MM:SS` or `HH:MM:SS.f`, as nanoseconds since midnight.
fn parse_time(text: &str) -> Option<i64> {
    if let Some(ns) = text.strip_suffix(" ns since midnight") {
        return ns.trim().parse().ok();
    }
    let time = NaiveTime::parse_from_str(text, "%H:%M:%S%.f")
        .or_else(|_| NaiveTime::parse_from_str(text, "%H:%M"))
        .ok()?;
    use chrono::Timelike;
    Some(i64::from(time.num_seconds_from_midnight()) * 1_000_000_000 + i64::from(time.nanosecond()))
}

fn time_of(ns: i64) -> Option<NaiveTime> {
    let secs = u32::try_from(ns.div_euclid(1_000_000_000)).ok()?;
    let nanos = u32::try_from(ns.rem_euclid(1_000_000_000)).ok()?;
    NaiveTime::from_num_seconds_from_midnight_opt(secs, nanos)
}

/// A datetime as a stored number in `unit`: a date (midnight), or date `T`/space
/// `HH:MM[:SS[.f]]`, optionally with an offset (`+01:00`, `Z`); otherwise read in
/// `zone`. `Err(Some(why))` names a non-form problem, `{text}` standing for the text.
fn parse_datetime(
    text: &str,
    unit: TimeUnit,
    zone: Option<&TimeZone>,
) -> Result<i64, Option<String>> {
    if let Some(stored) = text.strip_suffix(" since 1970-01-01 UTC") {
        let (v, written) = stored.trim().split_once(' ').ok_or(None)?;
        let v: i64 = v.parse().map_err(|_| None)?;
        return (written == unit_name_short(unit)).then_some(v).ok_or(None);
    }
    // One separator, a space, whichever was typed; `Z` is the offset it stands for.
    let mut text = text.to_string();
    if text.len() > 10 && text.as_bytes()[10] == b'T' {
        text.replace_range(10..11, " ");
    }
    if let Some(stripped) = text.strip_suffix('Z') {
        text = format!("{stripped}+00:00");
    }
    let with_offset = ["%Y-%m-%d %H:%M:%S%.f%#z", "%Y-%m-%d %H:%M%#z"]
        .iter()
        .find_map(|format| DateTime::parse_from_str(&text, format).ok());
    let utc = if let Some(at) = with_offset {
        if zone.is_none() {
            return Err(Some(
                "{text} carries an offset, and the column has no time zone".to_string(),
            ));
        }
        at.naive_utc()
    } else {
        let clock = ["%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%d %H:%M"]
            .iter()
            .find_map(|format| NaiveDateTime::parse_from_str(&text, format).ok())
            .or_else(|| {
                NaiveDate::parse_from_str(&text, "%Y-%m-%d")
                    .ok()
                    .and_then(|date| date.and_hms_opt(0, 0, 0))
            })
            .ok_or(None)?;
        match zone {
            None => clock,
            Some(zone) => {
                let tz = zone.to_chrono().map_err(|e| Some(e.to_string()))?;
                match tz.from_local_datetime(&clock) {
                    chrono::LocalResult::Single(at) => at.naive_utc(),
                    _ => {
                        return Err(Some(format!(
                            "{{text}} does not happen in {zone}, or happens twice there; \
                             add its offset, such as +01:00"
                        )));
                    }
                }
            }
        }
    };
    let utc = utc.and_utc();
    let nanos = i64::from(utc.timestamp_subsec_nanos());
    let finer = || {
        Some(format!(
            "{{text}} is finer than the column's {}",
            unit_name(unit)
        ))
    };
    match unit {
        TimeUnit::Nanoseconds => utc.timestamp_nanos_opt().ok_or(None),
        TimeUnit::Microseconds if nanos % 1_000 == 0 => Ok(utc.timestamp_micros()),
        TimeUnit::Milliseconds if nanos % 1_000_000 == 0 => Ok(utc.timestamp_millis()),
        _ => Err(finer()),
    }
}

/// The UTC clock of a value stored as `v` in `unit`.
fn utc_of(v: i64, unit: TimeUnit) -> Option<NaiveDateTime> {
    let at = match unit {
        TimeUnit::Milliseconds => DateTime::from_timestamp_millis(v)?,
        TimeUnit::Microseconds => DateTime::from_timestamp_micros(v)?,
        TimeUnit::Nanoseconds => DateTime::from_timestamp_nanos(v),
    };
    Some(at.naive_utc())
}

/// A duration as Polars writes one (`1d 2h 3m 4s 5µs`, `-1m -30s`, `1500ms`): parts
/// of a whole number and a unit, `d h m s ms us µs ns`, each signed on its own, as
/// nanoseconds.
fn parse_duration(text: &str) -> Option<i128> {
    let mut total: i128 = 0;
    let mut parts = 0;
    for part in text.split_whitespace() {
        let digits_end = part
            .char_indices()
            .find(|(i, c)| !(c.is_ascii_digit() || (*i == 0 && *c == '-')))
            .map(|(i, _)| i)?;
        let (number, suffix) = part.split_at(digits_end);
        let number: i128 = number.parse().ok()?;
        let per: i128 = match suffix {
            "d" => 86_400_000_000_000,
            "h" => 3_600_000_000_000,
            "m" => 60_000_000_000,
            "s" => 1_000_000_000,
            "ms" => 1_000_000,
            "us" | "µs" => 1_000,
            "ns" => 1,
            _ => return None,
        };
        total = total.checked_add(number.checked_mul(per)?)?;
        parts += 1;
    }
    (parts > 0).then_some(total)
}

/// `ns` in `unit`, when it is a whole number of them.
fn in_unit(ns: i128, unit: TimeUnit) -> Option<i64> {
    let per: i128 = match unit {
        TimeUnit::Milliseconds => 1_000_000,
        TimeUnit::Microseconds => 1_000,
        TimeUnit::Nanoseconds => 1,
    };
    (ns % per == 0)
        .then(|| i64::try_from(ns / per).ok())
        .flatten()
}

fn unit_name(unit: TimeUnit) -> &'static str {
    match unit {
        TimeUnit::Milliseconds => "milliseconds",
        TimeUnit::Microseconds => "microseconds",
        TimeUnit::Nanoseconds => "nanoseconds",
    }
}

fn unit_name_short(unit: TimeUnit) -> &'static str {
    match unit {
        TimeUnit::Milliseconds => "ms",
        TimeUnit::Microseconds => "us",
        TimeUnit::Nanoseconds => "ns",
    }
}

fn unit_suffix(unit: TimeUnit) -> &'static str {
    match unit {
        TimeUnit::Milliseconds => "ms",
        TimeUnit::Microseconds => "us",
        TimeUnit::Nanoseconds => "ns",
    }
}

#[cfg(test)]
mod tests {
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
        assert!(
            err("2024-01-01 05:00:00.0001", &ms).contains("finer than the column's milliseconds")
        );
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
}

//! Where an axis's ticks fall: on nice numbers, 1, 2 or 5 times a power of ten, or
//! on calendar boundaries such as hours, days, months and years. Pure arithmetic;
//! [`crate::widgets::axes`] decides how many fit and draws them.

use chrono::{Datelike, NaiveDate, NaiveDateTime, NaiveTime, TimeDelta, Timelike};

use crate::chart_data::{XAxisTemporalKind, x_datetime, x_time};

/// More ticks than this in one set is a step too fine to consider.
const MOST_TICKS: f64 = 4096.0;

/// The 1-2-5 steps for an axis from `lo` to `hi`, finest first: from the finest no
/// finer than `finest` to the first that spans the whole axis. 25, 250 and so on
/// step between 20 and 50 where they need no more places than their neighbors. A
/// `whole` axis steps by whole numbers only.
pub fn nice_steps(lo: f64, hi: f64, finest: f64, whole: bool) -> Vec<f64> {
    let span = hi - lo;
    if !(span > 0.0 && span.is_finite()) {
        return Vec::new();
    }
    let finest = finest.max(span / MOST_TICKS);
    let mut exp = finest.log10().floor() as i32;
    let mut steps = Vec::new();
    loop {
        for m in [1.0, 2.0, 2.5, 5.0] {
            let step = m * 10f64.powi(exp);
            if step < finest * (1.0 - 1e-9) || (whole && step < 1.0) || (m == 2.5 && exp < 1) {
                continue;
            }
            steps.push(step);
            if step >= span {
                return steps;
            }
        }
        exp += 1;
    }
}

/// `v` rounded to the places `step` is written in, so 0.1 * 3 reads 0.3.
fn clean(v: f64, step: f64) -> f64 {
    let places = (-(step.log10() + 1e-9).floor()).max(0.0) as i32 + 1;
    let scale = 10f64.powi(places.min(15));
    let rounded = (v * scale).round() / scale;
    // No -0.
    if rounded == 0.0 { 0.0 } else { rounded }
}

/// The multiples of `step` from `lo` to `hi`.
pub fn multiples(lo: f64, hi: f64, step: f64) -> Vec<f64> {
    if step.partial_cmp(&0.0) != Some(std::cmp::Ordering::Greater) || (hi - lo) / step > MOST_TICKS
    {
        return Vec::new();
    }
    let first = (lo / step - 1e-9).ceil() as i64;
    let last = (hi / step + 1e-9).floor() as i64;
    (first..=last)
        .map(|k| clean(k as f64 * step, step))
        .collect()
}

/// `lo` to `hi` widened out to multiples of `step`, never to nothing.
pub fn widen(lo: f64, hi: f64, step: f64) -> [f64; 2] {
    let wlo = clean((lo / step + 1e-9).floor() * step, step);
    let mut whi = clean((hi / step - 1e-9).ceil() * step, step);
    if whi <= wlo {
        whi = clean(wlo + step, step);
    }
    [wlo, whi]
}

/// A finer step that ticks between those of `step`, 1-2-5 as well: tenths of a
/// 1-step, fifths, halves; halves or quarters of a 2-step; fifths of a 5-step.
/// Finest first.
pub fn minor_steps(step: f64, whole: bool) -> Vec<f64> {
    let mantissa = step / 10f64.powf((step.log10() + 1e-9).floor());
    let divisors: &[f64] = if (mantissa - 1.0).abs() < 1e-6 {
        &[5.0, 2.0]
    } else if (mantissa - 2.0).abs() < 1e-6 {
        &[4.0, 2.0]
    } else {
        // Fifths of a 5-step, and of a 25-step.
        &[5.0]
    };
    divisors
        .iter()
        .map(|d| step / d)
        .filter(|s| !whole || *s >= 1.0 - 1e-9)
        .collect()
}

/// A calendar unit a time axis ticks on.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Unit {
    Second,
    Minute,
    Hour,
    Day,
    Month,
    Year,
}

impl Unit {
    /// About how long one is, in seconds.
    fn seconds(self) -> f64 {
        match self {
            Unit::Second => 1.0,
            Unit::Minute => 60.0,
            Unit::Hour => 3600.0,
            Unit::Day => 86_400.0,
            Unit::Month => 2_629_746.0,
            Unit::Year => 31_556_952.0,
        }
    }
}

/// Every `n` of a unit. Each step divides the unit above it, so a new day, month or
/// year always lands on a tick.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CalendarStep {
    pub unit: Unit,
    pub n: u32,
}

const CALENDAR_STEPS: &[(Unit, &[u32])] = &[
    (Unit::Second, &[1, 2, 5, 10, 15, 30]),
    (Unit::Minute, &[1, 2, 5, 10, 15, 30]),
    (Unit::Hour, &[1, 2, 3, 6, 12]),
    // Counted from the 1st of each month: 1, 8, 15, 22.
    (Unit::Day, &[1, 2, 7, 14]),
    (Unit::Month, &[1, 2, 3, 6]),
    (Unit::Year, &[1, 2, 5, 10, 20, 50, 100, 200, 500, 1000]),
];

/// The calendar steps a `kind` axis can take, finest first: a date ticks on days at
/// the finest, a time of day on hours at the coarsest.
pub fn calendar_steps(kind: XAxisTemporalKind) -> impl Iterator<Item = CalendarStep> {
    CALENDAR_STEPS
        .iter()
        .flat_map(|(unit, ns)| ns.iter().map(move |&n| CalendarStep { unit: *unit, n }))
        .filter(move |s| match kind {
            XAxisTemporalKind::Date => s.unit >= Unit::Day,
            XAxisTemporalKind::Time => s.unit <= Unit::Hour,
            _ => true,
        })
}

/// An axis value as the date and time it stands for; a time of day falls on the
/// epoch's day.
pub fn to_datetime(v: f64, kind: XAxisTemporalKind) -> Option<NaiveDateTime> {
    match kind {
        XAxisTemporalKind::Numeric => None,
        XAxisTemporalKind::Time => Some(epoch_day().and_time(x_time(v)?)),
        _ => x_datetime(v, kind),
    }
}

fn epoch_day() -> NaiveDate {
    NaiveDate::from_ymd_opt(1970, 1, 1).unwrap_or_default()
}

/// The axis value a date and time stands at, the inverse of [`to_datetime`].
pub fn from_datetime(at: NaiveDateTime, kind: XAxisTemporalKind) -> Option<f64> {
    let utc = at.and_utc();
    Some(match kind {
        XAxisTemporalKind::Numeric => return None,
        XAxisTemporalKind::Date => (at.date() - epoch_day()).num_days() as f64,
        XAxisTemporalKind::DatetimeUs => utc.timestamp_micros() as f64,
        XAxisTemporalKind::DatetimeMs => utc.timestamp_millis() as f64,
        XAxisTemporalKind::DatetimeNs => utc.timestamp_nanos_opt()? as f64,
        XAxisTemporalKind::Time => {
            let t = at.time();
            f64::from(t.num_seconds_from_midnight()) * 1e9 + f64::from(t.nanosecond())
        }
    })
}

/// The ticks of `step` from `lo` to `hi`, on its boundaries; none when there would
/// be too many to consider.
pub fn calendar_ticks(
    lo: NaiveDateTime,
    hi: NaiveDateTime,
    step: CalendarStep,
) -> Vec<NaiveDateTime> {
    let span = (hi - lo).as_seconds_f64();
    let n = step.n.max(1);
    if span < 0.0 || span / (step.unit.seconds() * f64::from(n)) > MOST_TICKS {
        return Vec::new();
    }
    let midnight = |d: NaiveDate| d.and_time(NaiveTime::MIN);
    let mut ticks = Vec::new();
    let mut push = |at: NaiveDateTime| {
        if at >= lo && at <= hi {
            ticks.push(at);
        }
        at <= hi
    };
    match step.unit {
        Unit::Year => {
            let n = n as i32;
            let mut year = lo.year().div_euclid(n) * n;
            while let Some(d) = NaiveDate::from_ymd_opt(year, 1, 1) {
                if !push(midnight(d)) {
                    break;
                }
                year += n;
            }
        }
        Unit::Month => {
            let n = n as i32;
            let first = lo.year() * 12 + lo.month0() as i32;
            let mut month = first - first.rem_euclid(n);
            while let Some(d) =
                NaiveDate::from_ymd_opt(month.div_euclid(12), month.rem_euclid(12) as u32 + 1, 1)
            {
                if !push(midnight(d)) {
                    break;
                }
                month += n;
            }
        }
        Unit::Day if n == 1 => {
            let mut day = lo.date();
            while push(midnight(day)) {
                let Some(next) = day.succ_opt() else { break };
                day = next;
            }
        }
        Unit::Day => {
            // From the 1st of each month, so every month starts on a tick; the last
            // gap of a month runs a little long.
            let last = if n == 2 { 29 } else { 28 };
            let (mut year, mut month) = (lo.year(), lo.month());
            'months: while let Some(first) = NaiveDate::from_ymd_opt(year, month, 1) {
                for day in (1..=last).step_by(n as usize) {
                    if let Some(d) = first.with_day(day)
                        && !push(midnight(d))
                    {
                        break 'months;
                    }
                }
                (year, month) = if month == 12 {
                    (year + 1, 1)
                } else {
                    (year, month + 1)
                };
            }
        }
        Unit::Hour | Unit::Minute | Unit::Second => {
            // Each step divides a day, so counting from midnight keeps them on the
            // hour, the quarter hour, the minute.
            let every = step.unit.seconds() as i64 * i64::from(n);
            let day = midnight(lo.date());
            let into = (lo - day).num_seconds();
            let mut at = day + TimeDelta::seconds(into.div_euclid(every) * every);
            while push(at) {
                at += TimeDelta::seconds(every);
            }
        }
    }
    ticks
}

/// Labels for ticks on `unit` boundaries, each naming the coarsest unit that turns
/// there: `2026` at a new year, `Apr` at a new month, `Mar 5` at a new day among
/// hours, otherwise the tick in its own unit (`12`, `06:00`). With `context` the
/// first tick also names the year (and the day, for hours) the axis starts in. A
/// time of day reads as a clock.
pub fn calendar_labels(
    ticks: &[NaiveDateTime],
    unit: Unit,
    kind: XAxisTemporalKind,
    context: bool,
) -> Vec<String> {
    ticks
        .iter()
        .enumerate()
        .map(|(i, at)| {
            let pattern = if kind == XAxisTemporalKind::Time {
                if unit == Unit::Second {
                    "%H:%M:%S"
                } else {
                    "%H:%M"
                }
            } else {
                calendar_pattern(at, unit, context && i == 0)
            };
            at.format(pattern).to_string()
        })
        .collect()
}

fn calendar_pattern(at: &NaiveDateTime, unit: Unit, first: bool) -> &'static str {
    let day_start = at.time() == NaiveTime::MIN;
    let month_start = day_start && at.day() == 1;
    let year_start = month_start && at.month() == 1;
    if year_start || unit == Unit::Year {
        return "%Y";
    }
    match unit {
        Unit::Month if first => "%b %Y",
        Unit::Month => "%b",
        Unit::Day if first => "%b %-d %Y",
        _ if month_start && !first => "%b",
        Unit::Day => "%-d",
        _ if day_start && first => "%b %-d %Y",
        _ if day_start => "%b %-d",
        Unit::Second if first => "%b %-d %Y %H:%M:%S",
        Unit::Second => "%H:%M:%S",
        _ if first => "%b %-d %Y %H:%M",
        _ => "%H:%M",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(y: i32, m: u32, d: u32, h: u32, min: u32) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(y, m, d)
            .unwrap()
            .and_hms_opt(h, min, 0)
            .unwrap()
    }

    /// Steps run 1, 2, 5 times a power of ten, from the finest asked for to one that
    /// spans the axis; a whole-number axis starts at 1.
    #[test]
    fn nice_steps_run_one_two_five() {
        assert_eq!(
            nice_steps(0.0, 100.0, 4.0, false),
            [5.0, 10.0, 20.0, 25.0, 50.0, 100.0]
        );
        assert_eq!(nice_steps(0.0, 7.0, 1.2, false), [2.0, 5.0, 10.0]);
        assert_eq!(nice_steps(0.0, 1.0, 0.15, false), [0.2, 0.5, 1.0]);
        assert_eq!(nice_steps(0.0, 7.0, 0.01, true), [1.0, 2.0, 5.0, 10.0]);
        assert!(nice_steps(3.0, 3.0, 0.1, false).is_empty());
    }

    /// Multiples land on clean values, never 0.30000000000000004 or -0.
    #[test]
    fn multiples_are_clean() {
        assert_eq!(multiples(-0.05, 0.45, 0.1), [0.0, 0.1, 0.2, 0.3, 0.4]);
        assert_eq!(multiples(1.0, 9.0, 2.5), [2.5, 5.0, 7.5]);
        assert_eq!(widen(3.0, 97.0, 20.0), [0.0, 100.0]);
        assert_eq!(widen(0.0, 0.0126, 0.002), [0.0, 0.014]);
        assert_eq!(widen(5.0, 5.0, 1.0), [5.0, 6.0]);
        assert_eq!(minor_steps(10.0, false), [2.0, 5.0]);
        assert_eq!(minor_steps(0.2, false), [0.05, 0.1]);
        assert_eq!(minor_steps(5.0, true), [1.0]);
        assert!(minor_steps(1.0, true).is_empty());
    }

    /// Months start on the 1st, aligned so a quarter starts in January; days restart
    /// each month; hours count from midnight.
    #[test]
    fn calendar_ticks_fall_on_boundaries() {
        let quarters = calendar_ticks(
            at(2024, 2, 10, 0, 0),
            at(2024, 12, 31, 0, 0),
            CalendarStep {
                unit: Unit::Month,
                n: 3,
            },
        );
        assert_eq!(
            quarters,
            [
                at(2024, 4, 1, 0, 0),
                at(2024, 7, 1, 0, 0),
                at(2024, 10, 1, 0, 0)
            ]
        );
        let weeks = calendar_ticks(
            at(2024, 1, 20, 0, 0),
            at(2024, 2, 16, 0, 0),
            CalendarStep {
                unit: Unit::Day,
                n: 7,
            },
        );
        let days: Vec<u32> = weeks.iter().map(|t| t.day()).collect();
        assert_eq!(days, [22, 1, 8, 15]);
        let hours = calendar_ticks(
            at(2024, 3, 1, 22, 30),
            at(2024, 3, 2, 7, 0),
            CalendarStep {
                unit: Unit::Hour,
                n: 3,
            },
        );
        let hours: Vec<u32> = hours.iter().map(|t| t.hour()).collect();
        assert_eq!(hours, [0, 3, 6]);
    }

    /// A label names the coarsest unit that turns at its tick, the first one the year
    /// it starts in.
    #[test]
    fn calendar_labels_name_the_unit_that_turns() {
        let ticks = calendar_ticks(
            at(2025, 10, 1, 0, 0),
            at(2026, 3, 1, 0, 0),
            CalendarStep {
                unit: Unit::Month,
                n: 1,
            },
        );
        let kind = XAxisTemporalKind::Date;
        assert_eq!(
            calendar_labels(&ticks, Unit::Month, kind, true),
            ["Oct 2025", "Nov", "Dec", "2026", "Feb", "Mar"]
        );
        assert_eq!(
            calendar_labels(&ticks, Unit::Month, kind, false),
            ["Oct", "Nov", "Dec", "2026", "Feb", "Mar"]
        );
        let ticks = calendar_ticks(
            at(2024, 3, 1, 12, 0),
            at(2024, 3, 2, 12, 0),
            CalendarStep {
                unit: Unit::Hour,
                n: 6,
            },
        );
        assert_eq!(
            calendar_labels(&ticks, Unit::Hour, XAxisTemporalKind::DatetimeUs, true),
            ["Mar 1 2024 12:00", "18:00", "Mar 2", "06:00", "12:00"]
        );
    }

    /// Values convert to dates and back for every temporal kind.
    #[test]
    fn values_round_trip_through_dates() {
        for (kind, v) in [
            (XAxisTemporalKind::Date, 19_783.0),
            (XAxisTemporalKind::DatetimeMs, 1_709_294_400_000.0),
            (XAxisTemporalKind::DatetimeUs, 1_709_294_400_000_000.0),
            (XAxisTemporalKind::Time, 3_600e9),
        ] {
            let dt = to_datetime(v, kind).unwrap();
            assert_eq!(from_datetime(dt, kind), Some(v), "{kind:?}");
        }
    }
}

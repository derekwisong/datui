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

/// A y axis fits its data: the per-origin means of departure delay by hour run
/// from -0.39 to 31.09, and a whole step either side made that -10 to 40. Ends
/// close to a tick still end on it, and an axis from zero starts at zero.
#[test]
fn a_y_axis_stops_short_of_a_step_it_would_leave_empty() {
    assert_eq!(widen_snug(-0.39, 31.09, 10.0, false), [-5.0, 35.0]);
    assert_eq!(widen_snug(0.69, 24.78, 5.0, false), [0.0, 25.0]);
    assert_eq!(widen_snug(0.0, 31.09, 10.0, false), [0.0, 35.0]);
    assert_eq!(widen_snug(3.0, 97.0, 20.0, false), [0.0, 100.0]);
    assert_eq!(widen_snug(3.0, 10.0, 5.0, true), [0.0, 10.0]);
    assert_eq!(widen_snug(5.0, 5.0, 1.0, true), [5.0, 6.0]);
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

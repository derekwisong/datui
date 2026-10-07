use super::*;
use crate::chart::chart_data::{XAxisTemporalKind, x_axis_label_at};

/// Every label of an axis at one level: its ticks' labels in order.
fn tick_labels(ticks: &[f64], numbers: &AxisNumbers, level: usize) -> Vec<String> {
    let format = AxisFormat::new(ticks, numbers);
    ticks
        .iter()
        .map(|&v| format.label(v, level).unwrap())
        .collect()
}

fn preset(name: &str) -> AxisNumbers {
    AxisNumbers {
        format: crate::numfmt::NumberFormat::preset(name).unwrap(),
        whole: false,
    }
}

/// A log axis writes every tick exactly in one form, whatever the closest two
/// are; its short form names each tick's own k, M or G.
#[test]
fn log_axis_labels_write_each_tick_exactly() {
    let ticks = [0.0, 1.0, 10.0, 100.0, 1e3, 1e4, 2e5, 1e6, 5e9];
    let labels = |numbers: &AxisNumbers, level| {
        let format = AxisFormat::log(&ticks, numbers);
        ticks
            .iter()
            .map(|&v| format.label(v, level).unwrap())
            .collect::<Vec<_>>()
    };
    assert_eq!(
        labels(&preset("thousands"), 0),
        [
            "0",
            "1",
            "10",
            "100",
            "1,000",
            "10,000",
            "200,000",
            "1,000,000",
            "5,000,000,000"
        ]
    );
    assert_eq!(
        labels(&AxisNumbers::default(), 1),
        ["0", "1", "10", "100", "1k", "10k", "200k", "1M", "5G"]
    );
    let short = [0.0, 0.25, 0.5, 0.75, 1.0];
    let format = AxisFormat::log(&short, &AxisNumbers::default());
    let written: Vec<_> = short.iter().map(|&v| format.label(v, 0).unwrap()).collect();
    assert_eq!(written, ["0.00", "0.25", "0.50", "0.75", "1.00"]);
    assert_eq!(
        format.label(1.0, 1),
        None,
        "no shorter form under a thousand"
    );
    let huge = AxisFormat::log(&[1e15, 2e16, 1e18], &AxisNumbers::default());
    assert_eq!(huge.label(2e16, 0).as_deref(), Some("2e16"));
}

/// Each form a narrow axis steps down to, the same for every tick on it.
#[test]
fn axis_labels_step_down_to_shorter_forms() {
    let plain = AxisNumbers::default();
    let ticks = [0.0, 12_345.0, 24_690.0];
    assert_eq!(tick_labels(&ticks, &plain, 0), ["0", "12345", "24690"]);
    assert_eq!(tick_labels(&ticks, &plain, 1), ["0", "12k", "25k"]);
    let ticks = [-1500.0, 0.0, 1500.0];
    assert_eq!(tick_labels(&ticks, &plain, 1), ["-1.5k", "0", "1.5k"]);
    let ticks = [0.0, 1.5e9, 3e9];
    assert_eq!(tick_labels(&ticks, &plain, 1), ["0", "1.5G", "3.0G"]);
    // Below a thousand there is no shorter form.
    let format = AxisFormat::new(&[0.0, 5.0], &plain);
    assert_eq!(format.label(5.0, 1), None);
    assert_eq!(format.label(5.0, 2), None);

    // 2020-01-01 and 2024-12-31 in days; the same instants in microseconds.
    let (lo, hi) = (18262.0, 20088.0);
    let numbers = AxisFormat::new(&[], &plain);
    let date =
        |v, bounds, level| x_axis_label_at(v, XAxisTemporalKind::Date, bounds, level, &numbers);
    let forms: Vec<_> = (0..).map_while(|level| date(hi, (lo, hi), level)).collect();
    assert_eq!(forms, ["2024-12-31", "2024-12", "2024"]);
    // Inside one year, month and day tell the ticks apart.
    assert_eq!(date(lo, (lo, lo + 30.0), 1).as_deref(), Some("01-01"));

    let us = 86_400.0 * 1e6;
    let kind = XAxisTemporalKind::DatetimeUs;
    let at = |v, bounds, level| x_axis_label_at(v, kind, bounds, level, &numbers);
    let forms: Vec<_> = (0..)
        .map_while(|level| at(lo * us, (lo * us, hi * us), level))
        .collect();
    assert_eq!(forms, ["2020-01-01 00:00", "2020-01-01", "2020-01", "2020"]);
    // Inside one day, the time of day.
    let day = (lo * us, lo * us + 3600e6);
    assert_eq!(at(lo * us + 3600e6, day, 1).as_deref(), Some("01:00"));
}

/// One format for every label on an axis: densities around 0.01 keep one
/// precision, where per tick they switched to scientific notation partway up.
#[test]
fn an_axis_keeps_one_notation_and_precision() {
    let plain = AxisNumbers::default();
    let density = tick_labels(&[0.0, 0.00651, 0.01302], &plain, 0);
    assert_eq!(density, ["0.0000", "0.0065", "0.0130"]);
    let density = tick_labels(&[0.0, 0.00451, 0.00902], &plain, 0);
    assert_eq!(density, ["0.00000", "0.00451", "0.00902"]);
    // Round ticks take the fewest places that write them all: `0.006`, not
    // `0.0060`; `20`, not `20.0`.
    let round = tick_labels(&[0.0, 0.006, 0.012], &plain, 0);
    assert_eq!(round, ["0.000", "0.006", "0.012"]);
    let round = tick_labels(&[0.0, 0.1 * 3.0, 0.6], &plain, 0);
    assert_eq!(round, ["0.0", "0.3", "0.6"]);
    let round = tick_labels(&[0.0, 20.0, 40.0, 60.0, 80.0], &plain, 0);
    assert_eq!(round, ["0", "20", "40", "60", "80"]);
    let round = tick_labels(&[0.0, 2_000.0, 4_000.0], &plain, 1);
    assert_eq!(round, ["0", "2k", "4k"]);
    // Too small to write in places: scientific, all of them.
    let tiny = tick_labels(&[0.0, 2.5e-8, 5e-8], &plain, 0);
    assert_eq!(tiny, ["0.00e0", "2.50e-8", "5.00e-8"]);
    // Three figures of the largest, and places enough to tell ticks apart.
    assert_eq!(
        tick_labels(&[3.21, 50.17, 97.2], &plain, 0),
        ["3.2", "50.2", "97.2"]
    );
    let close = tick_labels(&[1000.1, 1000.2, 1000.3], &plain, 0);
    assert_eq!(close, ["1000.1", "1000.2", "1000.3"]);
    // Nothing reads as a negative zero, not even a tie formatting rounds to it.
    assert_eq!(tick_labels(&[-0.0001, 1.0], &plain, 0), ["0.00", "1.00"]);
    let padded = [-0.5, 249.75, 500.0];
    assert_eq!(tick_labels(&padded, &plain, 0), ["0", "250", "500"]);
    let padded = [-500.0, 24_750.0, 50_000.0];
    assert_eq!(tick_labels(&padded, &plain, 1), ["0", "25k", "50k"]);
    assert_eq!(tick_labels(&[-0.0, 5e-8], &plain, 0), ["0.00e0", "5.00e-8"]);
    // A tick stepped a hair off zero is zero.
    let stepped = [-2e-8, 1.3e-24, 2e-8];
    assert_eq!(
        tick_labels(&stepped, &plain, 0),
        ["-2.00e-8", "0.00e0", "2.00e-8"]
    );
    // Ticks too close for places, or for three figures of scientific notation,
    // take the figures that tell them apart.
    let close = tick_labels(&[1.0, 1.000_000_1], &plain, 0);
    assert_eq!(close, ["1.0000000e0", "1.0000001e0"]);
    let nanoseconds = [1.727e18, 1.727_05e18, 1.7271e18];
    let labels = tick_labels(&nanoseconds, &plain, 0);
    assert_eq!(labels, ["1.72700e18", "1.72705e18", "1.72710e18"]);
    assert_eq!(tick_labels(&nanoseconds, &plain, 1), labels);
    // Past what places can hold, scientific, and its short form too.
    let huge = [0.0, 5e15];
    assert_eq!(tick_labels(&huge, &plain, 0), ["0.00e0", "5.00e15"]);
    assert_eq!(tick_labels(&huge, &plain, 1), ["0e0", "5e15"]);
    let huge = [1e15, 1.5e15, 2e15];
    assert_eq!(
        tick_labels(&huge, &plain, 1),
        ["1.0e15", "1.5e15", "2.0e15"]
    );
    // A whole-number axis prints whole numbers.
    let whole = AxisNumbers {
        whole: true,
        ..preset("thousands")
    };
    assert_eq!(
        tick_labels(&[0.0, 2161.0, 4322.0], &whole, 0),
        ["0", "2,161", "4,322"]
    );
}

/// The table's grouping and decimal separator, in the full form and the short:
/// `12,3k` under the european format.
#[test]
fn axis_labels_take_the_table_number_style() {
    let european = preset("european");
    let ticks = [12_000.0, 12_300.0, 12_600.0];
    assert_eq!(
        tick_labels(&ticks, &european, 0),
        ["12.000", "12.300", "12.600"]
    );
    assert_eq!(
        tick_labels(&ticks, &european, 1),
        ["12,0k", "12,3k", "12,6k"]
    );
    let ticks = [0.0, 0.25, 0.5];
    assert_eq!(tick_labels(&ticks, &european, 0), ["0,00", "0,25", "0,50"]);
    assert_eq!(
        tick_labels(&[0.0, 5e-8], &european, 0),
        ["0,00e0", "5,00e-8"]
    );

    let thousands = preset("thousands");
    let ticks = [0.0, 6172.4, 12345.0];
    assert_eq!(tick_labels(&ticks, &thousands, 0), ["0", "6,172", "12,345"]);
    let ticks = [0.0, 12_300.0];
    assert_eq!(tick_labels(&ticks, &thousands, 1), ["0", "12k"]);
}

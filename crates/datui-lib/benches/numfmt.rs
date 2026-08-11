//! Benchmarks for display-time number formatting.
//!
//! Cell formatting runs once per *visible* cell per frame — roughly
//! `visible_rows * visible_cols`, on the order of 1k cells for a full-screen
//! terminal. These benches exist less to prove the feature is fast (at that
//! volume it cannot be slow) than to pin the guarantees that keep it fast:
//!
//! - grouped integer formatting stays comparable to `i64::to_string`
//! - the passthrough path costs the same as today's unformatted rendering
//! - `width_i64` never builds a string
//!
//! A regression here means someone put an allocation back in the row loop.
//!
//! Measured on a full-screen frame (1000 cells), for reference:
//!
//! | path                                   | time     |
//! |----------------------------------------|----------|
//! | `passthrough` (`AnyValue::str_value`)  | ~56 µs   |
//! | `formatted` (grouping applied)         | ~25 µs   |
//! | `width_pass` (arithmetic width)        | ~2.8 µs  |
//!
//! Formatting is *faster* than the unformatted path because it skips Polars'
//! `str_value()` formatting machinery for numerics. Note that in isolation
//! `write_i64` is ~2x slower than `i64::to_string` (std uses a two-digit lookup
//! table); that gap is deliberate — matching it would mean carrying a lookup
//! table for a path that costs 0.15% of a frame budget.

use std::hint::black_box;

use criterion::{criterion_group, criterion_main, Criterion};
use datui_lib::numfmt::{
    display_width, format_any_value, CellFormatter, NumberFormat, NumberFormatSettings,
};
use polars::prelude::{AnyValue, DataType};

/// Genomic-coordinate-shaped values, the case from issue #51.
fn coordinates() -> Vec<i64> {
    (0..1024)
        .map(|i| 10_000 + (i as i64 * 2_999_983) % 3_088_269_832)
        .collect()
}

fn bench_integer_formatting(c: &mut Criterion) {
    let values = coordinates();
    let grouped = NumberFormat::preset("thousands").unwrap();

    let mut group = c.benchmark_group("integer_formatting");

    // Baseline: what the code does today for an integer cell.
    group.bench_function("to_string_baseline", |b| {
        b.iter(|| {
            let mut sink = 0usize;
            for &v in &values {
                sink += black_box(v.to_string()).len();
            }
            sink
        })
    });

    group.bench_function("grouped", |b| {
        b.iter(|| {
            let mut sink = 0usize;
            for &v in &values {
                let mut out = String::new();
                sink += grouped.write_i64(black_box(v), &mut out);
            }
            sink
        })
    });

    // Width without building a string — used by the locked-column measure pass.
    group.bench_function("width_only", |b| {
        b.iter(|| {
            let mut sink = 0usize;
            for &v in &values {
                sink += grouped.width_i64(black_box(v));
            }
            sink
        })
    });

    group.finish();
}

/// A full frame's worth of cells through the real `AnyValue` entry point, with
/// and without formatting active. The gap between these two is what a user pays
/// for turning the feature on.
fn bench_frame_of_cells(c: &mut Criterion) {
    let values: Vec<AnyValue> = coordinates()
        .into_iter()
        .take(1000)
        .map(AnyValue::Int64)
        .collect();

    let passthrough = CellFormatter::Passthrough;
    let formatted = CellFormatter::Number(NumberFormat::preset("thousands").unwrap());

    let mut group = c.benchmark_group("frame_of_cells");

    for (name, fmt) in [("passthrough", &passthrough), ("formatted", &formatted)] {
        group.bench_function(name, |b| {
            let mut scratch = String::new();
            b.iter(|| {
                let mut sink = 0usize;
                for v in &values {
                    sink += format_any_value(black_box(fmt), v, &mut scratch).len();
                }
                sink
            })
        });
    }

    group.bench_function("width_pass", |b| {
        b.iter(|| {
            let mut scratch = String::new();
            let mut sink = 0usize;
            for v in &values {
                sink += display_width(black_box(&formatted), v, &mut scratch);
            }
            sink
        })
    });

    group.finish();
}

/// Per-column resolution (including glob matching) must stay off the per-cell
/// path; this measures what one frame's worth of column resolution costs.
fn bench_column_resolution(c: &mut Criterion) {
    let settings = NumberFormatSettings {
        format: NumberFormat::preset("thousands").unwrap(),
        enabled: true,
        exclude: vec![
            datui_lib::numfmt::Glob::new("*_id"),
            datui_lib::numfmt::Glob::new("year"),
        ],
        align_numeric_right: true,
    };
    let names: Vec<String> = (0..32).map(|i| format!("column_{i}")).collect();

    c.bench_function("column_resolution_32_cols", |b| {
        b.iter(|| {
            let mut sink = 0usize;
            for n in &names {
                if !settings
                    .formatter_for(black_box(n), &DataType::Int64)
                    .is_passthrough()
                {
                    sink += 1;
                }
            }
            sink
        })
    });
}

criterion_group!(
    benches,
    bench_integer_formatting,
    bench_frame_of_cells,
    bench_column_resolution
);
criterion_main!(benches);

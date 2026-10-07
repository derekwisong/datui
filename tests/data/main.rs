//! Data: statistics and distribution analysis, pivot and melt, Excel. One test
//! executable, since each links the whole app; filter by module, as in
//! `scripts/dev/test.sh integration data statistics::`.
//!
//! `statistics` installs a counting global allocator for the whole executable.

#[path = "../common/mod.rs"]
mod common;

mod distribution;
mod excel;
mod reshape;
mod statistics;

/// Statistics over every row, or over a spread sample of `rows` drawn with `seed`.
fn analyze(
    lf: &polars::prelude::LazyFrame,
    rows: Option<usize>,
    seed: u64,
    options: datui::statistics::ComputeOptions,
) -> color_eyre::Result<datui::statistics::AnalysisResults> {
    use datui::sampling::{Sample, SampleMethod};
    let sample = Sample {
        method: if rows.is_some() {
            SampleMethod::Spread
        } else {
            SampleMethod::EveryRow
        },
        rows: rows.unwrap_or(0),
        seed,
        ..Sample::default()
    };
    datui::statistics::compute_statistics_for_sample(lf, &sample, None, options)
}

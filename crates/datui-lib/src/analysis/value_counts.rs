//! Value counts: how many rows of the view hold each value of one column, and a
//! summary of the column, from one read of that column.
//!
//! The count is exact: one streamed pass over the column that keeps a count per
//! value and no rows (`crate::chart::chart_data::Tally`). A view too large to count at
//! once, where the sampler can read part of it (one Parquet or IPC file, read a few
//! row groups at a time), is sampled first instead and says so; counting every row
//! is then the user's call. Where the sampler would stream every row anyway, the
//! exact count is the same read and is what runs.

use crate::analysis::sampling::ReadWatch;
use crate::chart::chart_data::{COUNT_COLUMN, Counted, Tally, count_frame};
use color_eyre::Result;
use color_eyre::eyre::eyre;
use polars::prelude::*;

/// Values listed one per line; the rest are summed into one `other` line.
pub const TOP_N: usize = 1_000;

/// Distinct values a count keeps before it stops: past this the column is an
/// identifier, and the counts would grow with the table.
pub const MAX_DISTINCT: usize = 2_000_000;

/// A local view this many times the sample size is sampled first, where a sample
/// reads less of it: 10,000,000 rows at the default sample size.
pub const LARGE_SAMPLES: usize = 100;

/// Bins of the histogram view. An integer column spanning fewer values than this
/// takes a bin per value instead.
pub const HISTOGRAM_BINS: usize = 40;

/// Which way the values are listed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Order {
    /// Most rows first; equal counts in value order.
    #[default]
    Count,
    /// In the column's own order: text A to Z, numbers ascending.
    Value,
}

impl Order {
    pub fn toggled(self) -> Self {
        match self {
            Order::Count => Order::Value,
            Order::Value => Order::Count,
        }
    }
}

/// How a count reads the view.
#[derive(Debug, Clone, PartialEq)]
pub enum Read {
    /// Exact, unless the view is large or remote and a sample reads less of it:
    /// then `sample_rows` of it, picked with `seed`.
    Quick {
        sample_rows: usize,
        seed: u64,
        remote: bool,
    },
    /// Every row.
    Exact,
}

/// A count to run off the UI thread: the view, the column, and how to read it.
pub struct Plan {
    /// The view as filtered and queried. Its order does not change a count.
    pub lf: LazyFrame,
    pub column: String,
    pub read: Read,
    /// The view's row count, when the table knows it.
    pub known_total: Option<usize>,
    pub streaming: bool,
}

impl Plan {
    /// Count the column, stopping when `watch` says to and counting the rows read.
    pub fn run(&self, watch: &ReadWatch) -> Result<ValueCounts> {
        let lf = self.lf.clone().select([col(self.column.as_str())]);
        let dtype = lf
            .clone()
            .collect_schema()?
            .get(self.column.as_str())
            .cloned()
            .ok_or_else(|| eyre!("no column {}", self.column))?;
        if let Some((rows, seed)) = self.sample(&lf) {
            let read = crate::analysis::sampling::sample_rows_counting(
                &lf,
                Some(rows),
                self.known_total,
                seed,
                self.streaming,
                Some(watch),
                None,
            )?;
            watch.check()?;
            let counted = count_frame(&read.rows.df, &self.column, MAX_DISTINCT)?;
            // A view no larger than the sample was read whole: that is exact.
            let of = read.rows.sample_size.map(|_| read.rows.total_rows);
            return ValueCounts::new(&self.column, dtype, counted, of);
        }
        let counted = stream_counts(&lf, &self.column, watch)?;
        ValueCounts::new(&self.column, dtype, counted, None)
    }

    /// The sample to read first, if one is worth it: the view is large or remote,
    /// and a sample of it reads only part of it.
    fn sample(&self, lf: &LazyFrame) -> Option<(usize, u64)> {
        let Read::Quick {
            sample_rows,
            seed,
            remote,
        } = self.read
        else {
            return None;
        };
        if sample_rows == 0 {
            return None;
        }
        let large = remote
            || self
                .known_total
                .is_none_or(|rows| rows > sample_rows.saturating_mul(LARGE_SAMPLES));
        (large && crate::analysis::sampling::slices_reach_into_the_scan(lf))
            .then_some((sample_rows, seed))
    }
}

/// Count `column` of `lf` in one streamed pass, stopping between batches when
/// `watch` says to.
fn stream_counts(lf: &LazyFrame, column: &str, watch: &ReadWatch) -> Result<Counted> {
    let tally = crate::analysis::sampling::stream_fold(
        lf.clone(),
        Some(watch),
        false,
        Tally::new(column, MAX_DISTINCT),
        |tally, batch| tally.observe(&batch),
    )?;
    // Part of the view counted is not a count of it.
    watch.check()?;
    Ok(tally.finish()?)
}

/// A number the summary adds up: whole for integer columns, so a large sum keeps
/// every digit.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Number {
    Int(i128),
    Float(f64),
}

/// The header strip: what can be said of the column from its counts alone.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Summary {
    /// Rows counted, nulls included.
    pub rows: usize,
    /// Distinct values, null not among them.
    pub distinct: usize,
    pub nulls: usize,
    /// Numbers only.
    pub sum: Option<Number>,
    pub mean: Option<f64>,
    /// Numbers and dates, times and durations.
    pub min: Option<AnyValue<'static>>,
    pub max: Option<AnyValue<'static>>,
}

impl Summary {
    /// The summary of a column whose distinct values are `values` (null among
    /// them at most once), each standing for `counts` rows.
    pub fn of(values: &Series, counts: &[u64]) -> PolarsResult<Self> {
        let rows = counts.iter().sum::<u64>() as usize;
        let nulls: u64 = values
            .is_null()
            .iter()
            .zip(counts)
            .filter(|(null, _)| null.unwrap_or(false))
            .map(|(_, n)| n)
            .sum();
        let nulls = nulls as usize;
        let distinct = values.len() - values.null_count();
        let dtype = values.dtype();
        let numeric = dtype.is_primitive_numeric() || matches!(dtype, DataType::Decimal(..));
        let ordered = numeric || dtype.is_temporal();
        let mut summary = Summary {
            rows,
            distinct,
            nulls,
            ..Summary::default()
        };
        if ordered && distinct > 0 {
            summary.min = Some(values.min_reduce()?.value().clone().into_static());
            summary.max = Some(values.max_reduce()?.value().clone().into_static());
        }
        if numeric {
            let sum = weighted_sum(values, counts)?;
            let present = rows - nulls;
            summary.mean = (present > 0).then(|| {
                let total = match sum {
                    Number::Int(n) => n as f64,
                    Number::Float(f) => f,
                };
                total / present as f64
            });
            summary.sum = Some(sum);
        }
        Ok(summary)
    }
}

/// Each value times the rows holding it, added up: whole for integers.
fn weighted_sum(values: &Series, counts: &[u64]) -> PolarsResult<Number> {
    let dtype = values.dtype();
    if dtype.is_integer() && !matches!(dtype, DataType::Int128) {
        // Unsigned 64-bit values past i64 have their own path; every other integer
        // fits in i64.
        let total: i128 = if matches!(dtype, DataType::UInt64) {
            values
                .u64()?
                .iter()
                .zip(counts)
                .filter_map(|(v, n)| v.map(|v| v as i128 * *n as i128))
                .sum()
        } else {
            values
                .cast(&DataType::Int64)?
                .i64()?
                .iter()
                .zip(counts)
                .filter_map(|(v, n)| v.map(|v| v as i128 * *n as i128))
                .sum()
        };
        return Ok(Number::Int(total));
    }
    let total = values
        .cast(&DataType::Float64)?
        .f64()?
        .iter()
        .zip(counts)
        .filter_map(|(v, n)| v.map(|v| v * *n as f64))
        .sum();
    Ok(Number::Float(total))
}

/// A number column's counts in bins: every value with its rows. The bins span the
/// values, or the 1st to the 99th percentile when the tails reach ten times past
/// it, and the values outside are counted. An integer column of few values has a
/// bin per value.
fn histogram_of(
    column: &str,
    values: &Series,
    rows: &[u64],
) -> Option<crate::chart::chart_data::HistogramData> {
    use crate::chart::chart_data::{Clipped, HistogramBin, HistogramData, RowsRead, ValueRange};
    let dtype = values.dtype();
    if !dtype.is_primitive_numeric() {
        return None;
    }
    let as_f64 = values.cast(&DataType::Float64).ok()?;
    let mut pairs: Vec<(f64, u64)> = as_f64
        .f64()
        .ok()?
        .iter()
        .zip(rows)
        .filter_map(|(v, n)| Some((v.filter(|v| v.is_finite())?, *n)))
        .collect();
    pairs.sort_by(|a, b| a.0.total_cmp(&b.0));
    let (min, max) = (pairs.first()?.0, pairs.last()?.0);
    let total: u64 = pairs.iter().map(|p| p.1).sum();
    // The value at quantile `q`, weighted by rows.
    let at = |q: f64| {
        let wanted = ((q * total as f64).ceil() as u64).max(1);
        let mut seen = 0;
        for (v, n) in &pairs {
            seen += n;
            if seen >= wanted {
                return *v;
            }
        }
        max
    };
    let (p1, p99) = (at(0.01), at(0.99));
    let clip = p99 > p1 && (max - min) > 10.0 * (p99 - p1);
    let (lo, hi) = if clip { (p1, p99) } else { (min, max) };
    let (bins, width, x_min) = if dtype.is_integer() && hi - lo < HISTOGRAM_BINS as f64 {
        ((hi - lo) as usize + 1, 1.0, lo - 0.5)
    } else if hi > lo {
        (HISTOGRAM_BINS, (hi - lo) / HISTOGRAM_BINS as f64, lo)
    } else {
        (1, 1.0, lo - 0.5)
    };
    let mut counts = vec![0.0_f64; bins];
    let mut outside = 0;
    for (v, n) in pairs {
        if v < lo || v > hi {
            outside += n as usize;
            continue;
        }
        let bin = (((v - x_min) / width).floor().max(0.0) as usize).min(bins - 1);
        counts[bin] += n as f64;
    }
    let max_count = counts.iter().copied().fold(0.0, f64::max);
    Some(HistogramData {
        column: column.to_string(),
        bins: counts
            .into_iter()
            .enumerate()
            .map(|(i, count)| HistogramBin {
                center: x_min + (i as f64 + 0.5) * width,
                count,
            })
            .collect(),
        groups: Vec::new(),
        other: false,
        share: false,
        x_min,
        x_max: x_min + bins as f64 * width,
        max_count,
        rows: RowsRead {
            total_rows: total as usize,
            sample_size: None,
            envelope_steps: None,
            seed: None,
        },
        clipped: clip.then_some(Clipped {
            range: ValueRange::Percentile1To99,
            outside,
        }),
    })
}

/// What one line of the listing stands for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LineKind {
    /// The value at this row of the counts.
    Value(usize),
    /// The rows with no value.
    Null,
    /// The values past the top ones, this many of them.
    Other(usize),
}

/// One line of the listing: what it stands for, its rows, and the rows of it and
/// every line above it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Line {
    pub kind: LineKind,
    pub rows: u64,
    pub cumulative: u64,
}

/// A column's values counted: every distinct value with its rows, the summary,
/// and the listing in either order.
#[derive(Debug, Clone)]
pub struct ValueCounts {
    pub column: String,
    pub dtype: DataType,
    /// Every distinct value and its rows: `column`, then [`COUNT_COLUMN`].
    counts: DataFrame,
    /// The rows of `counts` holding a value, by count and by value.
    by_count: Vec<usize>,
    by_value: Vec<usize>,
    /// The row of `counts` holding null, if any row is null.
    null_at: Option<usize>,
    /// The rows of the view when the counts are of a sample of them.
    pub sampled_of: Option<usize>,
    pub summary: Summary,
    count_lines: Vec<Line>,
    value_lines: Vec<Line>,
    /// A number column's counts in bins, for the histogram view; made with the
    /// counts, off the UI thread.
    pub histogram: Option<crate::chart::chart_data::HistogramData>,
}

impl ValueCounts {
    pub(crate) fn new(
        column: &str,
        dtype: DataType,
        counted: Counted,
        sampled_of: Option<usize>,
    ) -> Result<Self> {
        let counts = match counted {
            Counted::All {
                counts: Some(counts),
                ..
            } => counts,
            Counted::All { counts: None, .. } => DataFrame::new_infer_height(vec![
                Column::new_empty(column.into(), &dtype),
                Column::new_empty(COUNT_COLUMN.into(), &DataType::UInt64),
            ])?,
            Counted::TooMany => {
                return Err(eyre!(
                    "more than {} distinct values: counting stopped",
                    crate::numfmt::group_chrome(MAX_DISTINCT)
                ));
            }
        };
        let values = counts.column(column)?.as_materialized_series().clone();
        let rows = row_counts(&counts)?;
        let summary = Summary::of(&values, &rows)?;
        // Nulls sort last, and are left out: they have a line of their own.
        let present = values.len() - values.null_count();
        let by_value: Vec<usize> = values
            .arg_sort(
                SortOptions::default()
                    .with_nulls_last(true)
                    .with_maintain_order(true),
            )
            .iter()
            .flatten()
            .map(|i| i as usize)
            .take(present)
            .collect();
        let null_at = values
            .is_null()
            .iter()
            .position(|null| null.unwrap_or(false));
        // Stable from value order, so values with equal counts list in value order.
        let mut by_count = by_value.clone();
        by_count.sort_by(|&a, &b| rows[b].cmp(&rows[a]));
        let null_rows = summary.nulls as u64;
        let histogram = histogram_of(column, &values, &rows);
        Ok(Self {
            histogram,
            column: column.to_string(),
            dtype,
            count_lines: listing(&by_count, &rows, null_rows, true),
            value_lines: listing(&by_value, &rows, null_rows, false),
            by_count,
            by_value,
            null_at,
            counts,
            sampled_of,
            summary,
        })
    }

    /// Whether the counts are of a sample of the view.
    pub fn is_sample(&self) -> bool {
        self.sampled_of.is_some()
    }

    /// The listing in `order`: the top values, then the nulls, then the rest.
    pub fn lines(&self, order: Order) -> &[Line] {
        match order {
            Order::Count => &self.count_lines,
            Order::Value => &self.value_lines,
        }
    }

    /// The value at row `at` of the counts.
    pub fn value(&self, at: usize) -> PolarsResult<AnyValue<'static>> {
        Ok(self.counts.column(&self.column)?.get(at)?.into_static())
    }

    /// Every value with its rows, in `order`, the nulls after them: what a copy or an
    /// export of the counts writes. Nothing is summed into an `other` line.
    pub fn table(&self, order: Order) -> PolarsResult<DataFrame> {
        let rows = row_counts(&self.counts)?;
        let picked: Vec<usize> = match order {
            Order::Count => &self.by_count,
            Order::Value => &self.by_value,
        }
        .iter()
        .copied()
        .chain(self.null_at)
        .collect();
        let values = self.counts.column(&self.column)?.take(&IdxCa::from_vec(
            "order".into(),
            picked.iter().map(|&i| i as IdxSize).collect(),
        ))?;
        let counts: Vec<u64> = picked.iter().map(|&i| rows[i]).collect();
        let total = self.summary.rows.max(1) as f64;
        let percent: Vec<f64> = counts.iter().map(|&n| n as f64 * 100.0 / total).collect();
        let mut running = 0u64;
        let cumulative: Vec<f64> = counts
            .iter()
            .map(|n| {
                running += n;
                running as f64 * 100.0 / total
            })
            .collect();
        // Named so none takes the counted column's own name.
        let mut names = vec![self.column.clone()];
        let mut name = |wanted: &str| {
            let mut name = wanted.to_string();
            while names.contains(&name) {
                name.push('_');
            }
            names.push(name.clone());
            PlSmallStr::from(name)
        };
        DataFrame::new_infer_height(vec![
            values,
            Column::new(name("count"), counts),
            Column::new(name("percent"), percent),
            Column::new(name("cumulative_percent"), cumulative),
        ])
    }
}

fn row_counts(counts: &DataFrame) -> PolarsResult<Vec<u64>> {
    Ok(counts
        .column(COUNT_COLUMN)?
        .u64()?
        .into_no_null_iter()
        .collect())
}

/// The listing of values in `order` (rows of the counts, nulls not among them):
/// the first [`TOP_N`] with the nulls among them, then the rest summed into one
/// line. By count, the nulls rank by their rows, after values with as many; by
/// value, they come last, as a sort puts them.
fn listing(order: &[usize], rows: &[u64], nulls: u64, by_count: bool) -> Vec<Line> {
    let mut lines = Vec::with_capacity(order.len().min(TOP_N) + 2);
    let mut cumulative = 0u64;
    let mut push = |kind, n: u64, lines: &mut Vec<Line>| {
        cumulative += n;
        lines.push(Line {
            kind,
            rows: n,
            cumulative,
        });
    };
    let mut null_owed = nulls > 0;
    for &at in order.iter().take(TOP_N) {
        if null_owed && by_count && rows[at] < nulls {
            push(LineKind::Null, nulls, &mut lines);
            null_owed = false;
        }
        push(LineKind::Value(at), rows[at], &mut lines);
    }
    if null_owed {
        push(LineKind::Null, nulls, &mut lines);
    }
    if order.len() > TOP_N {
        let rest: u64 = order[TOP_N..].iter().map(|&at| rows[at]).sum();
        push(LineKind::Other(order.len() - TOP_N), rest, &mut lines);
    }
    lines
}

#[cfg(test)]
mod tests;

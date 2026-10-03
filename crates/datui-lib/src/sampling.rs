//! The one sampler every analysis tool reads through: which rows (a scope), how they
//! are picked (a method), how many, and the seed. Describe, Distribution, Correlation
//! and Data Quality all take their rows from [`read`], so a sample means the same
//! thing whichever tool shows it.

use crate::data_quality::{
    QualityScope, QualitySourceContext, apply_quality_scope, prepare_source_quality_scan,
};
use crate::numfmt;
use crate::statistics::{AnalysisRows, collect_lazy, sample_rank};
use color_eyre::Result;
use color_eyre::eyre::Report;
use polars::prelude::*;
use std::collections::HashMap;

/// The default sample size, before `[analysis] sample_rows` says otherwise.
pub const DEFAULT_SAMPLE_ROWS: usize = 100_000;

/// The sizes the Sample form steps through with ←/→.
pub const SAMPLE_SIZES: [usize; 6] = [1_000, 10_000, 50_000, 100_000, 500_000, 1_000_000];

/// A per-partition sample keeps at most this many partitions and rows in memory. Past
/// the rows it keeps fewer of each value; past the partitions it is refused, which a
/// column with that many values meets within its first few batches.
const MAX_GROUPS: usize = 10_000;
const MAX_GROUP_ROWS: usize = 2_000_000;

/// The row index the per-partition sampler ranks rows by, dropped before anyone sees it.
const GROUP_POSITION: &str = "__datui_group_sample_position";

/// The key a streamed pass counts rows by, computed beside the rows and taken off
/// each batch before the sampler keeps any of it.
pub(crate) const COUNT_KEY: &str = "__datui_count_key";

/// Distinct keys a pass counts before it gives up counting. Past this the grain is
/// finer than a report can show, and the map would grow with the table.
pub const MAX_COUNTED_KEYS: usize = 1_000_000;

/// What a streamed pass counted beside its sample.
#[derive(Debug, Clone, PartialEq)]
pub enum Counted {
    /// Every row of the scope by its key, named as a segment names its value
    /// (`AnyValue::str_value`), `None` for null.
    Totals(std::collections::BTreeMap<Option<String>, usize>),
    /// More than [`MAX_COUNTED_KEYS`] keys: the count was dropped, the sample kept.
    TooMany,
}

/// Rows by [`COUNT_KEY`], a batch at a time, bounded by [`MAX_COUNTED_KEYS`].
#[derive(Debug)]
pub(crate) struct KeyCounter {
    totals: HashMap<Option<String>, usize>,
    limit: usize,
    too_many: bool,
}

impl Default for KeyCounter {
    fn default() -> Self {
        Self::with_limit(MAX_COUNTED_KEYS)
    }
}

impl KeyCounter {
    fn with_limit(limit: usize) -> Self {
        Self {
            totals: HashMap::new(),
            limit,
            too_many: false,
        }
    }

    /// Count `batch`'s keys and take the key off it, so the rows kept are the
    /// table's own. A batch without the key is left as it is.
    pub(crate) fn observe(&mut self, batch: &mut DataFrame) -> PolarsResult<()> {
        if batch.column(COUNT_KEY).is_err() {
            return Ok(());
        }
        let key = batch.drop_in_place(COUNT_KEY)?;
        if self.too_many {
            return Ok(());
        }
        // Grouped in Polars first, so a key is turned into text once per batch
        // rather than once per row.
        let counts = key.as_materialized_series().value_counts(
            false,
            false,
            "__datui_count_rows".into(),
            false,
        )?;
        let keys = counts.column(COUNT_KEY)?;
        let rows = counts.column("__datui_count_rows")?;
        for row in 0..counts.height() {
            let value = keys.get(row)?;
            let key = (!value.is_null()).then(|| crate::exact::str_value(&value).into_owned());
            let n = rows.get(row)?.extract::<usize>().unwrap_or(0);
            *self.totals.entry(key).or_default() += n;
        }
        if self.totals.len() > self.limit {
            self.too_many = true;
            self.totals = HashMap::new();
        }
        Ok(())
    }

    pub(crate) fn finish(self) -> Counted {
        if self.too_many {
            Counted::TooMany
        } else {
            Counted::Totals(self.totals.into_iter().collect())
        }
    }
}

/// `lf` with `count`'s key beside its rows, for a pass that counts as it samples.
pub(crate) fn with_count_key(lf: LazyFrame, count: Option<&Expr>) -> LazyFrame {
    match count {
        Some(key) => lf.with_column(key.clone().alias(COUNT_KEY)),
        None => lf,
    }
}

/// What a read that was stopped says. Its work is dropped, never shown as a result.
pub const CANCELLED: &str = "Cancelled";

/// A read's line to the screen: told to stop, and telling how many rows it has seen.
///
/// Shared with the UI thread, which sets `stop` on a cancel and reads the count as
/// it draws. A streamed read checks `stop` between batches and a seeded block read
/// between blocks; a single collect cannot be stopped partway and runs to its end.
#[derive(Debug, Clone, Default)]
pub struct ReadWatch {
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    rows: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    /// Whether anything has counted rows yet: a read that cannot observe its batches
    /// has no count to show, which is not a count of zero.
    counted: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl ReadWatch {
    pub fn stop(&self) {
        self.stop.store(true, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn stopped(&self) -> bool {
        self.stop.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Rows the read has seen so far, once it has counted any.
    pub fn rows_seen(&self) -> Option<usize> {
        self.counted
            .load(std::sync::atomic::Ordering::Relaxed)
            .then(|| self.rows.load(std::sync::atomic::Ordering::Relaxed))
    }

    pub(crate) fn saw(&self, rows: usize) {
        self.counted
            .store(true, std::sync::atomic::Ordering::Relaxed);
        self.rows
            .fetch_add(rows, std::sync::atomic::Ordering::Relaxed);
    }

    /// Start counting again for the next read, handing back what the last one
    /// counted, if it counted anything.
    pub(crate) fn restart(&self) -> Option<usize> {
        let counted = self
            .counted
            .swap(false, std::sync::atomic::Ordering::Relaxed);
        let rows = self.rows.swap(0, std::sync::atomic::Ordering::Relaxed);
        counted.then_some(rows)
    }

    /// Stopped: the read's partial rows are not a sample, so it fails instead.
    pub(crate) fn check(&self) -> Result<()> {
        if self.stopped() {
            Err(Report::msg(CANCELLED))
        } else {
            Ok(())
        }
    }
}

/// How the rows of a scope are picked.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum SampleMethod {
    /// A seeded random sample spread across the whole scope.
    #[default]
    Spread,
    /// Up to the sample size from each value of a column, so a small partition is
    /// represented beside a large one.
    PerPartition { column: String },
    /// The first rows of the scope, in order: the quickest read, and only the head.
    FirstRows,
    /// Every row: no sampling.
    EveryRow,
}

impl SampleMethod {
    pub fn label(&self) -> String {
        match self {
            Self::Spread => "Random".to_string(),
            Self::PerPartition { column } => format!("Equal per {column}"),
            Self::FirstRows => "First rows".to_string(),
            Self::EveryRow => "Every row".to_string(),
        }
    }
}

/// Which rows an analysis reads and how it picks them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sample {
    pub scope: QualityScope,
    pub method: SampleMethod,
    /// Rows to keep: in all, or per partition for [`SampleMethod::PerPartition`].
    /// Ignored by [`SampleMethod::EveryRow`].
    pub rows: usize,
    pub seed: u64,
}

impl Default for Sample {
    fn default() -> Self {
        Self {
            scope: QualityScope::CurrentView,
            method: SampleMethod::Spread,
            rows: DEFAULT_SAMPLE_ROWS,
            seed: 42_891,
        }
    }
}

impl Sample {
    /// The sample as one short line: `100,000 spread · current view · seed 42891`.
    pub fn summary(&self) -> String {
        let rows = numfmt::group_chrome(self.rows);
        let how = match &self.method {
            SampleMethod::Spread => format!("{rows} random rows"),
            SampleMethod::PerPartition { column } => format!("{rows} rows per {column}"),
            SampleMethod::FirstRows => format!("first {rows} rows"),
            SampleMethod::EveryRow => "every row".to_string(),
        };
        let seeded = matches!(
            self.method,
            SampleMethod::Spread | SampleMethod::PerPartition { .. }
        );
        let middot = crate::glyphs::get().middot;
        if seeded {
            format!(
                "{how} {middot} {} {middot} seed {}",
                self.scope.label(),
                self.seed
            )
        } else {
            format!("{how} {middot} {}", self.scope.label())
        }
    }

    /// What was read, once it was: `sample of 100,000 of 36,839,175 rows`, then the
    /// scope when it is not simply the table as shown. `per_value` is how many rows
    /// an equal-per-value sample kept of each, when that was fewer than asked.
    pub fn outcome(
        &self,
        total_rows: usize,
        sample_size: Option<usize>,
        per_value: Option<usize>,
    ) -> String {
        let count = numfmt::group_chrome;
        let read = match (&self.method, sample_size) {
            (SampleMethod::FirstRows, Some(n)) => format!("first {} rows", count(n)),
            (SampleMethod::PerPartition { column }, Some(n)) => {
                let each = per_value.unwrap_or(self.rows).min(self.rows);
                let lowered = if each < self.rows {
                    format!(" (lowered from {})", count(self.rows))
                } else {
                    String::new()
                };
                format!(
                    "{} rows, up to {} per {column}{lowered}, of {}",
                    count(n),
                    count(each),
                    count(total_rows)
                )
            }
            (_, Some(n)) => format!("sample of {} of {} rows", count(n), count(total_rows)),
            (_, None) => format!("all {} rows", count(total_rows)),
        };
        if self.scope == QualityScope::CurrentView {
            read
        } else {
            format!(
                "{read} {} {}",
                crate::glyphs::get().middot,
                self.scope.label()
            )
        }
    }
}

/// Where a tool's rows come from before its scope cuts them: the table as shown, or
/// the loaded source with what its footers said. Built on the UI thread; cut in the
/// worker, since preparing a source scan can read its schema.
pub struct SampleSource {
    lf: LazyFrame,
    source: Option<QualitySourceContext>,
    from_source: bool,
}

impl SampleSource {
    /// The table as it is shown (query and filters applied).
    pub fn view(lf: LazyFrame) -> Self {
        Self {
            lf,
            source: None,
            from_source: false,
        }
    }

    /// The loaded source, before any query or filter.
    pub fn loaded(lf: LazyFrame, source: Option<QualitySourceContext>) -> Self {
        Self {
            lf,
            source,
            from_source: true,
        }
    }

    /// The frame cut to `scope`, with only the table's own columns: the provenance
    /// index a source scope needs to find its files is dropped once it has.
    pub fn cut(self, scope: &QualityScope) -> Result<LazyFrame> {
        let lf = if self.from_source {
            prepare_source_quality_scan(self.lf, self.source.as_ref())?
        } else {
            self.lf
        };
        let lf = apply_quality_scope(lf, scope, self.source.as_ref())?;
        let schema = lf.clone().collect_schema()?;
        let helpers = [
            crate::schema_union::DRIFT_COLUMN,
            "__datui_quality_row",
            self.source
                .as_ref()
                .map(|source| source.row_index_column.as_str())
                .unwrap_or(""),
        ];
        let keep = schema
            .iter_names()
            .filter(|name| !helpers.contains(&name.as_str()))
            .map(|name| col(name.clone()))
            .collect::<Vec<_>>();
        Ok(if keep.len() == schema.len() {
            lf
        } else {
            lf.select(keep)
        })
    }
}

/// How many rows a view scope holds, from the view's row count; `None` for a source
/// scope, whose size only a read can tell.
pub fn view_scope_rows(view_rows: Option<usize>, scope: &QualityScope) -> Option<usize> {
    let rows = view_rows?;
    match scope {
        QualityScope::CurrentView => Some(rows),
        QualityScope::FirstRows(limit) => Some(rows.min(*limit)),
        QualityScope::ViewRows { start, end } => {
            Some(rows.min(*end).saturating_sub(start.saturating_sub(1)))
        }
        _ => None,
    }
}

/// Read the rows `sample` asks for from a frame already cut to its scope.
///
/// `known_total` saves a count. The first-rows method reports the rows it read as the
/// total when none is known, and says so through `sample_size: None`, rather than pay
/// for a count the method exists to avoid.
pub fn read(
    lf: &LazyFrame,
    sample: &Sample,
    known_total: Option<usize>,
    polars_streaming: bool,
) -> Result<AnalysisRows> {
    let rows = read_rows(lf, sample, known_total, polars_streaming)?;
    if rows.total_rows == 0 {
        return Err(no_rows_error(&sample.scope));
    }
    Ok(rows)
}

/// A chosen set of rows that matches nothing is a mistake to say, not an empty
/// sample to analyze: a typed value that is not in the data, a range past its end.
/// The table as shown may simply be empty, and says so itself.
pub fn no_rows_error(scope: &QualityScope) -> Report {
    if *scope == QualityScope::CurrentView {
        Report::msg("The table has no rows to sample")
    } else {
        Report::msg(format!(
            "No rows match {}; change Rows from in the Sample form (s)",
            scope.label()
        ))
    }
}

pub(crate) fn read_rows(
    lf: &LazyFrame,
    sample: &Sample,
    known_total: Option<usize>,
    polars_streaming: bool,
) -> Result<AnalysisRows> {
    read_rows_watched(lf, sample, known_total, polars_streaming, None)
}

/// [`read_rows`], stopping when `watch` says to and counting the rows it streams.
pub(crate) fn read_rows_watched(
    lf: &LazyFrame,
    sample: &Sample,
    known_total: Option<usize>,
    polars_streaming: bool,
    watch: Option<&ReadWatch>,
) -> Result<AnalysisRows> {
    acquire(lf, sample, known_total, polars_streaming, watch, None).map(|read| read.rows)
}

/// The rows a sample kept, where each sat in the frame, and what its pass counted.
pub(crate) struct SampledRows {
    pub rows: AnalysisRows,
    /// Each kept row's position in the frame read, in the order of `rows.df`: what
    /// cuts a sample into row chunks without reading it again.
    pub positions: Vec<IdxSize>,
    /// Rows by `count`'s key, when the pass that read the sample saw every row.
    /// `None` when it did not (seeded runs, the head) or nothing was asked.
    pub counted: Option<Counted>,
}

/// [`read_rows_watched`], keeping each row's position, and counting every row by
/// `count` when the read is one streamed pass over all of them: the pass is being
/// paid for anyway, and a second read of the key is what this saves.
pub(crate) fn acquire(
    lf: &LazyFrame,
    sample: &Sample,
    known_total: Option<usize>,
    polars_streaming: bool,
    watch: Option<&ReadWatch>,
    count: Option<&Expr>,
) -> Result<SampledRows> {
    let n = sample.rows.max(1);
    match &sample.method {
        SampleMethod::EveryRow => crate::statistics::sample_rows_counting(
            lf,
            None,
            known_total,
            sample.seed,
            polars_streaming,
            watch,
            count,
        ),
        SampleMethod::Spread => crate::statistics::sample_rows_counting(
            lf,
            Some(n),
            known_total,
            sample.seed,
            polars_streaming,
            watch,
            count,
        ),
        SampleMethod::FirstRows => {
            let df = collect_lazy(lf.clone().limit(n as IdxSize), polars_streaming)
                .map_err(Report::from)?;
            let height = df.height();
            // Without a count, a full head means there may be more: call it a sample.
            let sampled = match known_total {
                Some(total) => total > height,
                None => height == n,
            };
            Ok(SampledRows {
                positions: (0..height as IdxSize).collect(),
                rows: AnalysisRows {
                    df,
                    total_rows: known_total.unwrap_or(height),
                    sample_size: sampled.then_some(height),
                    per_value: None,
                },
                counted: None,
            })
        }
        SampleMethod::PerPartition { column } => {
            let read =
                per_group_sample_within(lf, column, n, sample.seed, MAX_GROUP_ROWS, watch, count)?;
            let sample_size = (read.seen > read.df.height()).then_some(read.df.height());
            Ok(SampledRows {
                rows: AnalysisRows {
                    df: read.df,
                    total_rows: read.seen,
                    sample_size,
                    per_value: Some(read.per_value),
                },
                positions: read.positions,
                counted: read.counted,
            })
        }
    }
}

/// What an equal-per-value sample learned beside its rows, from the same pass.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PerValue {
    /// Rows kept of each value: the size asked for, or fewer when that many of every
    /// value would pass [`MAX_GROUP_ROWS`].
    pub kept: usize,
    /// Every row of the scope, counted by value as it streamed past. Keyed as a
    /// segment names a value (`AnyValue::str_value`), `None` for null, so a segment
    /// by the same column finds its count here instead of in a second read.
    pub totals: std::collections::BTreeMap<Option<String>, usize>,
}

/// What [`per_group_sample_within`] read.
struct GroupRead {
    df: DataFrame,
    seen: usize,
    per_value: PerValue,
    positions: Vec<IdxSize>,
    counted: Option<Counted>,
}

/// Up to `n` seeded rows from each value of `column`, from one streamed pass, in table
/// order, and how many rows there were, holding at most `limit` rows.
///
/// Never refused for keeping too many rows. Whether `n` of every value fits is only
/// known once every value has been seen, which is the end of the read, and a read
/// that ends in a refusal has been paid for and thrown away. So the size per value
/// comes down as values arrive, to what [`MAX_GROUP_ROWS`] holds for all of them; each
/// value keeps its lowest-ranked rows, which is a seeded uniform sample of it at any
/// size, and [`PerValue::kept`] says what the size came down to.
fn per_group_sample_within(
    lf: &LazyFrame,
    column: &str,
    n: usize,
    seed: u64,
    limit: usize,
    watch: Option<&ReadWatch>,
    count: Option<&Expr>,
) -> Result<GroupRead> {
    let schema = lf.clone().collect_schema()?;
    if schema.get(column).is_none() {
        return Err(Report::msg(format!(
            "partition column {column:?} is not in the rows sampled; choose another"
        )));
    }
    let state = std::sync::Arc::new(std::sync::Mutex::new(GroupState {
        column: column.to_string(),
        cap: n,
        limit,
        seed,
        ..Default::default()
    }));
    let callback_state = std::sync::Arc::clone(&state);
    let callback_watch = watch.cloned();
    let sink = with_count_key(lf.clone(), count)
        .with_row_index(GROUP_POSITION, None)
        .sink_batches(
            PlanCallback::new(move |batch: DataFrame| {
                // True stops the sink: a cancel ends the read at the next batch.
                if let Some(watch) = &callback_watch {
                    if watch.stopped() {
                        return Ok(true);
                    }
                    watch.saw(batch.height());
                }
                callback_state
                    .lock()
                    .map_err(|_| PolarsError::ComputeError("sampler lock failed".into()))?
                    .observe(batch)?;
                Ok(false)
            }),
            true,
            None,
        )?;
    // Streaming whatever the setting: holding the table is what this is here to avoid.
    collect_lazy(sink, true).map_err(Report::from)?;
    if let Some(watch) = watch {
        watch.check()?;
    }
    let state = std::mem::take(
        &mut *state
            .lock()
            .map_err(|_| Report::msg("sampler lock failed"))?,
    );
    let seen = state.seen;
    // Every value the same size: the cap may have come down after a value was last
    // trimmed, and one that arrived late was only ever held to the cap of its time.
    let cap = state
        .cap
        .min((state.limit / state.groups.len().max(1)).max(1));
    let mut totals = std::collections::BTreeMap::new();
    let mut out: Option<DataFrame> = None;
    for (key, mut group) in state.groups {
        totals.insert(key, group.total);
        group.trim(cap)?;
        let Some(rows) = group.rows else {
            continue;
        };
        out = Some(match out {
            Some(frame) => frame.vstack(&rows)?,
            None => rows,
        });
    }
    let (df, positions) = match out {
        Some(df) => {
            let df = df.sort([GROUP_POSITION], SortMultipleOptions::default())?;
            let positions = df
                .column(GROUP_POSITION)?
                .idx()?
                .into_no_null_iter()
                .collect();
            (df.drop(GROUP_POSITION)?, positions)
        }
        None => (
            collect_lazy(lf.clone().limit(0), true).map_err(Report::from)?,
            Vec::new(),
        ),
    };
    Ok(GroupRead {
        df,
        seen,
        per_value: PerValue { kept: cap, totals },
        positions,
        counted: count.is_some().then(|| state.counter.finish()),
    })
}

#[derive(Default)]
struct GroupState {
    column: String,
    /// Rows each value may keep: the size asked for until the values seen so far
    /// would not all fit, then what does.
    cap: usize,
    /// Rows held across every value at most, once trimmed.
    limit: usize,
    seed: u64,
    seen: usize,
    /// Rows held across every value.
    held: usize,
    groups: HashMap<Option<String>, GroupSample>,
    counter: KeyCounter,
}

#[derive(Default)]
struct GroupSample {
    rows: Option<DataFrame>,
    ranks: Vec<u64>,
    /// Rows of this value seen, kept or not.
    total: usize,
}

impl GroupSample {
    /// Keep the `cap` lowest-ranked rows: a seeded uniform sample of the value,
    /// whatever order its rows arrived in. Returns how many went.
    fn trim(&mut self, cap: usize) -> PolarsResult<usize> {
        if self.ranks.len() <= cap {
            return Ok(0);
        }
        let mut order: Vec<usize> = (0..self.ranks.len()).collect();
        order.sort_unstable_by_key(|i| self.ranks[*i]);
        order.truncate(cap);
        let take: Vec<IdxSize> = order.iter().map(|i| *i as IdxSize).collect();
        if let Some(kept) = self.rows.take() {
            self.rows = Some(kept.take(&IdxCa::from_vec("kept".into(), take))?);
        }
        let removed = self.ranks.len() - cap;
        self.ranks = order.iter().map(|i| self.ranks[*i]).collect();
        Ok(removed)
    }
}

impl GroupState {
    fn observe(&mut self, mut batch: DataFrame) -> PolarsResult<()> {
        self.counter.observe(&mut batch)?;
        self.seen += batch.height();
        let positions = batch.column(GROUP_POSITION)?.idx()?.clone();
        let keys = batch.column(&self.column)?.as_materialized_series().clone();
        let mut by_key: HashMap<Option<String>, (Vec<IdxSize>, Vec<u64>)> = HashMap::new();
        for (index, (key, position)) in keys.iter().zip(positions.into_no_null_iter()).enumerate() {
            // Named as a segment names its value, so the counts line up with segments.
            let key = (!key.is_null()).then(|| crate::exact::str_value(&key).into_owned());
            let entry = by_key.entry(key).or_default();
            entry.0.push(index as IdxSize);
            entry.1.push(sample_rank(self.seed, position as u64));
        }
        for (key, (indices, ranks)) in by_key {
            if !self.groups.contains_key(&key) && self.groups.len() >= MAX_GROUPS {
                return Err(PolarsError::ComputeError(
                    format!(
                        "more than {MAX_GROUPS} values of {}; sample per a coarser column",
                        self.column
                    )
                    .into(),
                ));
            }
            let group = self.groups.entry(key).or_default();
            group.total += indices.len();
            self.held += indices.len();
            let rows = batch.take(&IdxCa::from_vec("picked".into(), indices))?;
            group.rows = Some(match group.rows.take() {
                Some(kept) => kept.vstack(&rows)?,
                None => rows,
            });
            group.ranks.extend(ranks);
            self.held -= group.trim(self.cap)?;
        }
        // Past the row limit, every value's share comes down to what fits. A quarter
        // over before trimming, so a run of new values costs a trim now and then
        // rather than one per value.
        if self.held > self.limit.saturating_add(self.limit / 4) {
            self.cap = self.cap.min((self.limit / self.groups.len()).max(1));
            for group in self.groups.values_mut() {
                self.held -= group.trim(self.cap)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table() -> LazyFrame {
        // Three partitions of very different sizes, in order.
        let sizes = [("a", 9_000usize), ("b", 900), ("c", 100)];
        let mut part = Vec::new();
        let mut value = Vec::new();
        for (name, size) in sizes {
            for row in 0..size {
                part.push(name);
                value.push(row as i64);
            }
        }
        df!("part" => part, "value" => value).unwrap().lazy()
    }

    fn sample(method: SampleMethod, rows: usize) -> Sample {
        Sample {
            method,
            rows,
            ..Sample::default()
        }
    }

    /// Per partition keeps up to n from each, so the small one is all there and the
    /// large one does not crowd it out.
    #[test]
    fn per_partition_keeps_up_to_n_from_each_value() {
        let method = SampleMethod::PerPartition {
            column: "part".to_string(),
        };
        let rows = read(&table(), &sample(method, 200), None, false).unwrap();
        assert_eq!(rows.total_rows, 10_000);
        let counts = rows
            .df
            .column("part")
            .unwrap()
            .as_materialized_series()
            .value_counts(true, true, "n".into(), false)
            .unwrap();
        let n = |part: &str| {
            (0..counts.height())
                .find(|row| {
                    counts
                        .column("part")
                        .unwrap()
                        .get(*row)
                        .unwrap()
                        .str_value()
                        == part
                })
                .map(|row| {
                    counts
                        .column("n")
                        .unwrap()
                        .get(row)
                        .unwrap()
                        .try_extract::<u32>()
                        .unwrap()
                })
                .unwrap()
        };
        assert_eq!((n("a"), n("b"), n("c")), (200, 200, 100));
        assert_eq!(rows.sample_size, Some(500));
        assert!(
            rows.df.column(GROUP_POSITION).is_err(),
            "no helper column leaks"
        );
    }

    /// Past the row limit, every value keeps fewer rows rather than the read being
    /// refused at its end: the same rows a sample asking for that many from the start
    /// keeps, and every value's rows counted on the way.
    #[test]
    fn per_partition_past_the_limit_keeps_fewer_of_each_value() {
        let GroupRead {
            df,
            seen,
            per_value,
            ..
        } = per_group_sample_within(&table(), "part", 500, 42_891, 999, None, None).unwrap();
        assert_eq!(seen, 10_000);
        assert_eq!(per_value.kept, 333);
        assert_eq!(df.height(), 333 + 333 + 100);
        assert_eq!(
            per_value.totals,
            [("a", 9_000), ("b", 900), ("c", 100)]
                .into_iter()
                .map(|(part, rows)| (Some(part.to_string()), rows))
                .collect()
        );
        let asked =
            per_group_sample_within(&table(), "part", 333, 42_891, usize::MAX, None, None).unwrap();
        assert!(df.equals(&asked.df), "the rows a sample of 333 each keeps");

        let lowered = Sample {
            method: SampleMethod::PerPartition {
                column: "part".into(),
            },
            rows: 500,
            ..Sample::default()
        };
        assert_eq!(
            lowered.outcome(10_000, Some(766), Some(333)),
            "766 rows, up to 333 per part (lowered from 500), of 10,000"
        );
    }

    /// Every sampler says where each kept row sat, which is what cuts a sample into
    /// row chunks later without a read. A streamed pass counts every row by the key
    /// it is given while it samples, the counts a read of the key would give; the head
    /// does not see every row, so it counts nothing.
    #[test]
    fn a_sample_says_where_its_rows_sat_and_a_stream_counts_on_the_way() {
        // Sampled per one column and counted by another.
        let lf = table()
            .with_column((col("value") % lit(4)).alias("quarter"))
            .with_row_index("row", None);
        let totals: std::collections::BTreeMap<_, _> = [("a", 9_000), ("b", 900), ("c", 100)]
            .into_iter()
            .map(|(part, rows)| (Some(part.to_string()), rows))
            .collect();
        for method in [
            SampleMethod::Spread,
            SampleMethod::PerPartition {
                column: "quarter".to_string(),
            },
            SampleMethod::FirstRows,
        ] {
            let read = acquire(
                &lf,
                &sample(method.clone(), 50),
                None,
                false,
                None,
                Some(&col("part")),
            )
            .unwrap();
            let rows: Vec<IdxSize> = read
                .rows
                .df
                .column("row")
                .unwrap()
                .idx()
                .unwrap()
                .into_no_null_iter()
                .collect();
            assert_eq!(rows, read.positions, "{method:?}");
            assert!(read.rows.df.column(COUNT_KEY).is_err(), "{method:?}");
            if method == SampleMethod::FirstRows {
                assert_eq!(read.counted, None);
            } else {
                assert_eq!(
                    read.counted,
                    Some(Counted::Totals(totals.clone())),
                    "{method:?}"
                );
            }
        }
    }

    /// Past its limit a count stops counting and says so; the batch it saw still
    /// loses its key, so the rows the sampler keeps are the table's.
    #[test]
    fn a_count_past_its_limit_gives_up_and_says_so() {
        let mut counter = KeyCounter::with_limit(2);
        let mut batch = df!(COUNT_KEY => ["a", "b", "c"], "value" => [1, 2, 3]).unwrap();
        counter.observe(&mut batch).unwrap();
        assert_eq!(batch.get_column_names(), ["value"]);
        assert_eq!(counter.finish(), Counted::TooMany);
    }

    #[test]
    fn first_rows_is_the_head_and_every_row_is_all_of_it() {
        let head = read(
            &table(),
            &sample(SampleMethod::FirstRows, 50),
            Some(10_000),
            false,
        )
        .unwrap();
        assert_eq!(head.df.height(), 50);
        assert_eq!(head.sample_size, Some(50));
        assert_eq!(
            head.df.column("value").unwrap().i64().unwrap().get(49),
            Some(49)
        );
        let all = read(&table(), &sample(SampleMethod::EveryRow, 50), None, false).unwrap();
        assert_eq!((all.df.height(), all.sample_size), (10_000, None));
    }

    #[test]
    fn a_seeded_per_partition_sample_repeats() {
        let method = SampleMethod::PerPartition {
            column: "part".to_string(),
        };
        let one = read(&table(), &sample(method.clone(), 50), None, false).unwrap();
        let two = read(&table(), &sample(method.clone(), 50), None, false).unwrap();
        assert!(one.df.equals(&two.df));
        let other = read(
            &table(),
            &Sample {
                seed: 7,
                ..sample(method, 50)
            },
            None,
            false,
        )
        .unwrap();
        assert!(!one.df.equals(&other.df));
    }

    /// Rows chosen that match nothing are an error that names them, not an empty
    /// sample every tool would analyze as if it were the data.
    #[test]
    fn a_scope_that_matches_nothing_is_an_error() {
        let scope = QualityScope::parse_command("partition part=zzz").unwrap();
        let lf = SampleSource::view(table()).cut(&scope).unwrap();
        let Err(error) = read(
            &lf,
            &Sample {
                scope,
                ..Sample::default()
            },
            None,
            false,
        ) else {
            panic!("a scope that matches nothing must not sample");
        };
        assert!(error.to_string().contains("No rows match"), "{error}");
    }

    #[test]
    fn the_summary_says_what_will_be_read() {
        let middot = crate::glyphs::get().middot;
        assert_eq!(
            Sample::default().summary(),
            format!("100,000 random rows {middot} current view {middot} seed 42891")
        );
        assert_eq!(
            sample(SampleMethod::FirstRows, 1_000).summary(),
            format!("first 1,000 rows {middot} current view")
        );
    }
}

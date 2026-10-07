//! The one sampler every analysis tool reads through: which rows (a scope), how they
//! are picked (a method), how many, and the seed. Describe, Distribution, Correlation
//! and Data Quality all take their rows from [`read`], so a sample means the same
//! thing whichever tool shows it.

use crate::data_quality::{
    QualityScope, QualitySourceContext, apply_quality_scope, prepare_source_quality_scan,
};
use crate::numfmt;
use crate::statistics::collect_lazy;
use color_eyre::Result;
use color_eyre::eyre::Report;
use polars::prelude::*;
use std::collections::HashMap;

/// The default sample size, before `[analysis] sample_rows` says otherwise.
pub const DEFAULT_SAMPLE_ROWS: usize = 100_000;

/// Why a typed sample size cannot be read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SizeError {
    NotASize,
    Zero,
}

impl SizeError {
    /// A few words, for a row with little room.
    pub fn short(self) -> &'static str {
        match self {
            Self::NotASize => "not a size (50k, 2m)",
            Self::Zero => "at least 1 row",
        }
    }
}

impl std::fmt::Display for SizeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::NotASize => "Sample size is a number of rows, like 50000, 50k or 2m",
            Self::Zero => "Sample size is at least 1 row",
        })
    }
}

/// A typed sample size: `50000`, `50,000`, `50_000`, `50k`, `2m`, `2.5M`. A size of
/// no rows is refused; past `usize` it saturates and the caller clamps.
pub fn parse_size(text: &str) -> Result<usize, SizeError> {
    let refuse = || SizeError::NotASize;
    let cleaned: String = text
        .trim()
        .chars()
        .filter(|c| !matches!(c, ',' | '_'))
        .collect::<String>()
        .to_ascii_lowercase();
    let (number, scale) = match cleaned.strip_suffix('k') {
        Some(n) => (n, 1e3),
        None => match cleaned.strip_suffix('m') {
            Some(n) => (n, 1e6),
            None => (cleaned.as_str(), 1.0),
        },
    };
    if number.is_empty() || !number.chars().all(|c| c.is_ascii_digit() || c == '.') {
        return Err(refuse());
    }
    let rows = if scale == 1.0 {
        // Whole digits: exact, however long.
        if number.contains('.') {
            return Err(refuse());
        }
        number.parse::<usize>().unwrap_or(usize::MAX)
    } else {
        let value: f64 = number.parse().map_err(|_| refuse())?;
        (value * scale).round() as usize
    };
    if rows == 0 {
        return Err(SizeError::Zero);
    }
    Ok(rows)
}

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
    /// Judges the rows a sampler holds, in bytes, as it reads: the reason to stop
    /// when they would not fit.
    held: Option<HeldCheck>,
    /// Why the held rows stopped the read, once they did.
    memory: std::sync::Arc<std::sync::Mutex<Option<String>>>,
}

/// What [`ReadWatch::hold`] asks of the bytes a sampler holds and the rows they are.
pub type HeldJudge = dyn Fn(u64, usize) -> Option<String> + Send + Sync;

#[derive(Clone)]
struct HeldCheck(std::sync::Arc<HeldJudge>);

impl std::fmt::Debug for HeldCheck {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("HeldCheck")
    }
}

impl ReadWatch {
    /// A watch whose sampler stops, keeping what it holds, once `judge` says the
    /// bytes it holds will not fit.
    pub(crate) fn judging_held(judge: std::sync::Arc<HeldJudge>) -> Self {
        Self {
            held: Some(HeldCheck(judge)),
            ..Self::default()
        }
    }

    /// A sampler holds `bytes` in `rows` rows now: past what fits, the read stops.
    pub(crate) fn hold(&self, bytes: u64, rows: usize) {
        let Some(HeldCheck(judge)) = &self.held else {
            return;
        };
        if let Some(reason) = judge(bytes, rows) {
            *self.memory.lock().unwrap_or_else(|e| e.into_inner()) = Some(reason);
            self.stop();
        }
    }

    /// Why memory stopped the read, if it did: its rows so far are kept.
    pub(crate) fn memory_stopped(&self) -> Option<String> {
        self.memory
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

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

    /// Stopped: the read's partial rows are not a sample, so it fails instead. Not
    /// when memory stopped it: what it holds is kept.
    pub(crate) fn check(&self) -> Result<()> {
        if self.stopped() && self.memory_stopped().is_none() {
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
    /// The method's name, as the sample form offers it.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Spread => "Random",
            Self::PerPartition { .. } => "Equal per value",
            Self::FirstRows => "First rows",
            Self::EveryRow => "Every row",
        }
    }

    /// The name with the partition column in it: `Equal per region`.
    pub fn label(&self) -> String {
        match self {
            Self::PerPartition { column } => format!("Equal per {column}"),
            method => method.name().to_string(),
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

    /// The summary, against the `rows` the scope is known to hold: a sample of at
    /// least that many reads every one of them, and says so, `all 1,000 rows`,
    /// rather than promising 100,000 from a table of 1,000.
    pub fn summary_within(&self, rows: Option<usize>) -> String {
        match (rows, &self.method) {
            (Some(n), SampleMethod::Spread | SampleMethod::FirstRows) if n <= self.rows => {
                let middot = crate::glyphs::get().middot;
                format!(
                    "all {} rows {middot} {}",
                    numfmt::group_chrome(n),
                    self.scope.label()
                )
            }
            _ => self.summary(),
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
        SampleMethod::EveryRow => sample_rows_counting(
            lf,
            None,
            known_total,
            sample.seed,
            polars_streaming,
            watch,
            count,
        ),
        SampleMethod::Spread => sample_rows_counting(
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
    let held = watch.cloned();
    let state = stream_fold(
        with_count_key(lf.clone(), count).with_row_index(GROUP_POSITION, None),
        watch,
        true,
        GroupState {
            column: column.to_string(),
            cap: n,
            limit,
            seed,
            ..Default::default()
        },
        move |state, batch| {
            state.observe(batch)?;
            if let Some(watch) = &held {
                watch.hold(state.bytes(), state.held);
            }
            Ok(false)
        },
    )?;
    if let Some(watch) = watch {
        watch.check()?;
    }
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
    /// Bytes the rows held take.
    fn bytes(&self) -> u64 {
        self.groups
            .values()
            .filter_map(|group| group.rows.as_ref())
            .map(|rows| rows.estimated_size() as u64)
            .sum()
    }

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

/// The rows an analysis reads, and how many the table has.
pub struct AnalysisRows {
    pub df: DataFrame,
    pub total_rows: usize,
    /// How many rows were sampled, when the table had more than the analysis reads.
    pub sample_size: Option<usize>,
    /// What an equal-per-value sample kept and counted.
    pub per_value: Option<PerValue>,
}

/// How many places across the table a block sample reads from. Enough that no one
/// stretch of it decides the answer, few enough that each is a row group or two.
const SAMPLE_BLOCKS: usize = 50;

/// How many runs of a block sample are read at once.
const SAMPLE_READERS: usize = 8;

/// The row index the streaming sampler ranks rows by, dropped before anyone sees it.
const SAMPLE_POSITION: &str = "__datui_sample_position";

/// Count a frame's rows.
pub fn count_rows(lf: &LazyFrame, polars_streaming: bool) -> Result<usize> {
    let count_df =
        collect_lazy(crate::table::row_count_lf(lf), polars_streaming).map_err(Report::from)?;
    Ok(match count_df.get(0).and_then(|row| row.first().cloned()) {
        Some(AnyValue::UInt64(n)) => n as usize,
        Some(AnyValue::UInt32(n)) => n as usize,
        _ => 0,
    })
}

/// Read the rows an analysis works on: all of them when the table has no more than
/// `sample_rows` (or `sample_rows` is `None`), and otherwise a seeded sample of that
/// many, spread across the whole table rather than taken from its head.
///
/// Two ways to spread it, chosen by what the plan can do cheaply:
///
/// - A plan whose slices reach into a single Parquet or IPC scan reads
///   [`SAMPLE_BLOCKS`] short runs at seeded places across the table. Each run is a
///   row group or two, so a sample of a 400-million-row hive table reads a few dozen
///   row groups, not the table. `known_total` saves the count; the footers give it
///   cheaply otherwise.
/// - Anything else — a filter, a query, a union of files, a CSV — is read once as a
///   stream, keeping the rows whose seeded rank is lowest. That is a uniform sample
///   in bounded memory, and the same pass counts the rows, so a filtered view is
///   read once rather than counted and then read.
pub fn analysis_rows(
    lf: &LazyFrame,
    sample_rows: Option<usize>,
    known_total: Option<usize>,
    seed: u64,
    polars_streaming: bool,
) -> Result<AnalysisRows> {
    analysis_rows_watched(lf, sample_rows, known_total, seed, polars_streaming, None)
}

/// [`analysis_rows`], stopping when `watch` says to: the streamed pass between
/// batches, the seeded runs between runs. A whole read is one collect, which runs to
/// its end.
pub(crate) fn analysis_rows_watched(
    lf: &LazyFrame,
    sample_rows: Option<usize>,
    known_total: Option<usize>,
    seed: u64,
    polars_streaming: bool,
    watch: Option<&ReadWatch>,
) -> Result<AnalysisRows> {
    sample_rows_counting(
        lf,
        sample_rows,
        known_total,
        seed,
        polars_streaming,
        watch,
        None,
    )
    .map(|read| read.rows)
}

/// [`analysis_rows_watched`], keeping where each row sat, and counting every row by
/// `count` when the read sees every row: a streamed pass, or a table read whole
/// because it is under twice the sample. Seeded runs see too few rows to count, and
/// a read of the whole scope is not a sample, so neither counts.
pub(crate) fn sample_rows_counting(
    lf: &LazyFrame,
    sample_rows: Option<usize>,
    known_total: Option<usize>,
    seed: u64,
    polars_streaming: bool,
    watch: Option<&ReadWatch>,
    count: Option<&Expr>,
) -> Result<SampledRows> {
    let whole = |df: DataFrame, total_rows: usize| SampledRows {
        positions: (0..df.height() as IdxSize).collect(),
        rows: AnalysisRows {
            df,
            total_rows,
            sample_size: None,
            per_value: None,
        },
        counted: None,
    };
    let Some(n) = sample_rows.filter(|n| *n > 0) else {
        let df = collect_lazy(lf.clone(), polars_streaming).map_err(Report::from)?;
        let total_rows = df.height();
        return Ok(whole(df, total_rows));
    };
    if !slices_reach_into_the_scan(lf) {
        let read = stream_sample(lf, n, seed, watch, count)?;
        let sample_size = (read.seen > n).then_some(read.df.height());
        return Ok(SampledRows {
            rows: AnalysisRows {
                df: read.df,
                total_rows: read.seen,
                sample_size,
                per_value: None,
            },
            positions: read.positions,
            counted: read.counted,
        });
    }
    let total_rows = match known_total {
        Some(total) => total,
        None => count_rows(lf, polars_streaming)?,
    };
    if total_rows <= n {
        let df = collect_lazy(lf.clone(), polars_streaming).map_err(Report::from)?;
        return Ok(whole(df, total_rows));
    }
    let along = Along {
        watch,
        count,
        on_run: None,
    };
    let read = block_sample(lf, total_rows, n, seed, polars_streaming, along)?;
    Ok(SampledRows {
        rows: AnalysisRows {
            sample_size: Some(read.df.height()),
            df: read.df,
            total_rows,
            per_value: None,
        },
        positions: read.positions,
        counted: read.counted,
    })
}

/// Whether a slice of this plan is read by the scan of one file, skipping what comes
/// before it: true of a single Parquet or IPC file, which seeks by row group, with or
/// without columns stubbed above it. Not of a filter or a CSV, whose slice reads
/// everything ahead of it, nor of a scan of many files, where each slice opens the
/// footer of every file before it — measured on 135 files in S3, fifty slices took
/// longer than streaming all 37 million rows once.
///
/// Asked of the optimized plan because that is where the answer is, for every route a
/// frame can have been built by: pushed into the scan, the slice is a property of the
/// `SCAN` (`SLICE: Positive`); left above it, a node of its own (`SLICE[`). Should a
/// Polars upgrade change how the plan is described, this says no and the streaming
/// sampler takes over: slower, never wrong.
pub fn slices_reach_into_the_scan(lf: &LazyFrame) -> bool {
    let Ok(plan) = lf.clone().slice(1, 1).describe_optimized_plan() else {
        return false;
    };
    let scans: Vec<&str> = plan
        .lines()
        .map(str::trim)
        .filter(|l| l.starts_with("Parquet SCAN") || l.starts_with("IPC SCAN"))
        .collect();
    let [scan] = scans.as_slice() else {
        return false;
    };
    let one_source = !scan.contains("other sources") && !scan.contains(", ");
    let total_scans = plan.matches(" SCAN").count();
    one_source && total_scans == 1 && plan.contains("SLICE: Positive") && !plan.contains("SLICE[")
}

/// [`block_sample`] for a sample shown as it is drawn: each run goes to `on_run`, with
/// where it starts, as it lands. A table under twice the sample is read whole and cut,
/// and comes back as one frame instead. A stop ends the read with the runs so far
/// delivered.
pub(crate) fn block_sample_live(
    lf: &LazyFrame,
    total_rows: usize,
    n: usize,
    seed: u64,
    polars_streaming: bool,
    watch: &ReadWatch,
    on_run: &OnRun<'_>,
) -> Result<Option<DataFrame>> {
    let along = Along {
        watch: Some(watch),
        count: None,
        on_run: Some(on_run),
    };
    let read = block_sample(lf, total_rows, n, seed, polars_streaming, along)?;
    Ok((total_rows < 2 * n).then_some(read.df))
}

/// What a block sample does beside reading its runs: stops when `watch` says to,
/// counts `count`'s key, and hands each run to `on_run` as it lands.
struct Along<'a> {
    watch: Option<&'a ReadWatch>,
    count: Option<&'a Expr>,
    on_run: Option<&'a OnRun<'a>>,
}

/// Told of each run of a block sample as it lands, with where it starts.
pub(crate) type OnRun<'a> = dyn Fn(usize, &DataFrame) + Sync + 'a;

/// `n` rows as [`SAMPLE_BLOCKS`] runs at seeded places across `total_rows`, in table
/// order. Each run is collected on its own: as one union the runs share a subplan, and
/// Polars caches a shared subplan whole. They are collected [`SAMPLE_READERS`] at a
/// time, because on an object store each is a round trip and fifty in a row is the
/// wait this exists to avoid.
fn block_sample(
    lf: &LazyFrame,
    total_rows: usize,
    n: usize,
    seed: u64,
    polars_streaming: bool,
    along: Along<'_>,
) -> Result<StreamRead> {
    let Along {
        watch,
        count,
        on_run,
    } = along;
    // Under twice the sample, reading the table is about as cheap as reading runs of
    // it, and runs that must fit side by side would crowd or overlap. Read it and keep
    // a seeded uniform `n` of it instead, counting `count`'s key from the rows read.
    if total_rows < 2 * n {
        let df = collect_lazy(lf.clone(), polars_streaming).map_err(Report::from)?;
        let counted = match count {
            Some(key) => {
                let mut keys = df
                    .clone()
                    .lazy()
                    .select([key.clone().alias(COUNT_KEY)])
                    .collect()?;
                let mut counter = KeyCounter::default();
                counter.observe(&mut keys)?;
                Some(counter.finish())
            }
            None => None,
        };
        let mut ranked: Vec<(u64, IdxSize)> = (0..df.height())
            .map(|i| (sample_rank(seed, i as u64), i as IdxSize))
            .collect();
        ranked.sort_unstable();
        let mut keep: Vec<IdxSize> = ranked.into_iter().take(n).map(|(_, i)| i).collect();
        keep.sort_unstable();
        let df = df.take(&IdxCa::from_vec("sample".into(), keep.clone()))?;
        return Ok(StreamRead {
            df,
            seen: total_rows,
            positions: keep,
            counted,
        });
    }
    let blocks = SAMPLE_BLOCKS.min(n).max(1);
    // Exactly `n` rows between the runs, so none is cut off the end, and each fits in
    // its own stretch of the table: a stretch is at least `2n / blocks` rows long.
    let stride = total_rows / blocks;
    let runs: Vec<(usize, usize)> = (0..blocks)
        .map(|block| {
            let run = (block + 1) * n / blocks - block * n / blocks;
            let room = stride.saturating_sub(run) as u64;
            let offset = block * stride + (sample_rank(seed, block as u64) % (room + 1)) as usize;
            (offset, run)
        })
        .collect();
    let next = std::sync::atomic::AtomicUsize::new(0);
    let read: Vec<Result<(usize, DataFrame)>> = std::thread::scope(|scope| {
        let workers: Vec<_> = (0..SAMPLE_READERS.min(blocks))
            .map(|_| {
                scope.spawn(|| {
                    let mut read = Vec::new();
                    loop {
                        if watch.is_some_and(|watch| watch.stopped()) {
                            break;
                        }
                        let block = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        let Some((offset, run)) = runs.get(block) else {
                            break;
                        };
                        let rows = collect_lazy(
                            lf.clone().slice(*offset as i64, *run as IdxSize),
                            polars_streaming,
                        )
                        .map(|df| {
                            if let Some(watch) = watch {
                                watch.saw(df.height());
                            }
                            if let Some(on_run) = on_run {
                                on_run(*offset, &df);
                            }
                            (block, df)
                        })
                        .map_err(Report::from);
                        read.push(rows);
                    }
                    read
                })
            })
            .collect();
        workers
            .into_iter()
            .flat_map(|worker| match worker.join() {
                Ok(read) => read,
                // A reader that died is an error, not a smaller sample.
                Err(_) => vec![Err(Report::msg("a sample reader failed"))],
            })
            .collect()
    });
    if let Some(watch) = watch {
        watch.check()?;
    }
    let mut read = read.into_iter().collect::<Result<Vec<_>>>()?;
    read.sort_by_key(|(block, _)| *block);
    let mut out: Option<DataFrame> = None;
    let mut positions = Vec::with_capacity(n);
    for (block, rows) in read {
        let offset = runs[block].0;
        positions.extend((0..rows.height()).map(|row| (offset + row) as IdxSize));
        out = Some(match out {
            Some(frame) => frame.vstack(&rows)?,
            None => rows,
        });
    }
    Ok(StreamRead {
        df: out.unwrap_or_default(),
        seen: total_rows,
        positions,
        counted: None,
    })
}

/// What [`stream_sample`] or [`block_sample`] read.
struct StreamRead {
    df: DataFrame,
    /// Rows in the scope.
    seen: usize,
    positions: Vec<IdxSize>,
    counted: Option<Counted>,
}

/// Stream `lf` through `on_batch` a batch at a time, until it ends, `on_batch` says
/// true, or `watch` stops it; `watch` sees each batch before `on_batch` has it.
/// Streaming whatever the setting: holding the table is what this is here to avoid.
pub(crate) fn stream_batches(
    lf: LazyFrame,
    watch: Option<&ReadWatch>,
    maintain_order: bool,
    on_batch: impl Fn(DataFrame) -> PolarsResult<bool> + Send + Sync + 'static,
) -> Result<()> {
    let watch = watch.cloned();
    let sink = lf.sink_batches(
        PlanCallback::new(move |batch: DataFrame| {
            if let Some(watch) = &watch {
                if watch.stopped() {
                    return Ok(true);
                }
                watch.saw(batch.height());
            }
            on_batch(batch)
        }),
        maintain_order,
        None,
    )?;
    collect_lazy(sink, true).map_err(Report::from)?;
    Ok(())
}

/// [`stream_batches`] folding each batch into `state`, which comes back when the read
/// ends. `observe` says true to stop it.
pub(crate) fn stream_fold<S: Send + 'static>(
    lf: LazyFrame,
    watch: Option<&ReadWatch>,
    maintain_order: bool,
    state: S,
    observe: impl Fn(&mut S, DataFrame) -> PolarsResult<bool> + Send + Sync + 'static,
) -> Result<S> {
    let shared = std::sync::Arc::new(std::sync::Mutex::new(Some(state)));
    let held = std::sync::Arc::clone(&shared);
    stream_batches(lf, watch, maintain_order, move |batch| {
        let mut state = held
            .lock()
            .map_err(|_| PolarsError::ComputeError("a streamed read failed".into()))?;
        match state.as_mut() {
            Some(state) => observe(state, batch),
            None => Ok(true),
        }
    })?;
    let state = shared
        .lock()
        .map_err(|_| Report::msg("a streamed read failed"))?
        .take();
    state.ok_or_else(|| Report::msg("a streamed read failed"))
}

/// A uniform sample of `n` rows from one streamed pass, and how many rows there were,
/// with every row counted by `count` on the way.
fn stream_sample(
    lf: &LazyFrame,
    n: usize,
    seed: u64,
    watch: Option<&ReadWatch>,
    count: Option<&Expr>,
) -> Result<StreamRead> {
    let held = watch.cloned();
    let mut reservoir = stream_fold(
        with_count_key(lf.clone(), count).with_row_index(SAMPLE_POSITION, None),
        watch,
        true,
        Reservoir::new(n, seed),
        move |reservoir, batch| {
            reservoir.observe(batch)?;
            if let Some(watch) = &held {
                let kept = reservoir.kept.as_ref();
                watch.hold(
                    kept.map_or(0, |kept| kept.estimated_size() as u64),
                    kept.map_or(0, DataFrame::height),
                );
            }
            Ok(false)
        },
    )?;
    if let Some(watch) = watch {
        watch.check()?;
    }
    let seen = reservoir.seen;
    let counted = count
        .is_some()
        .then(|| std::mem::take(&mut reservoir.counter).finish());
    let (df, positions) = match reservoir.finish()? {
        Some(kept) => kept,
        // Nothing came through: an empty frame of the right shape.
        None => (
            collect_lazy(lf.clone().limit(0), true).map_err(Report::from)?,
            Vec::new(),
        ),
    };
    Ok(StreamRead {
        df,
        seen,
        positions,
        counted,
    })
}

/// The `n` rows with the lowest seeded rank seen so far. Held to at most twice `n`
/// between prunes, so memory is bounded by the sample and not by the table.
#[derive(Default)]
struct Reservoir {
    n: usize,
    seed: u64,
    seen: usize,
    kept: Option<DataFrame>,
    ranks: Vec<u64>,
    /// Rows ranked at or above this cannot make the sample: `n` lower ones are held.
    bar: u64,
    counter: KeyCounter,
}

impl Reservoir {
    fn new(n: usize, seed: u64) -> Self {
        Self {
            n,
            seed,
            bar: u64::MAX,
            ..Default::default()
        }
    }

    fn observe(&mut self, mut batch: DataFrame) -> PolarsResult<()> {
        self.counter.observe(&mut batch)?;
        self.seen += batch.height();
        let positions = batch.column(SAMPLE_POSITION)?.idx()?;
        let mut picked = Vec::new();
        let mut ranks = Vec::new();
        for (index, position) in positions.into_no_null_iter().enumerate() {
            let rank = sample_rank(self.seed, position as u64);
            if rank < self.bar {
                picked.push(index as IdxSize);
                ranks.push(rank);
            }
        }
        if picked.is_empty() {
            return Ok(());
        }
        let rows = batch.take(&IdxCa::from_vec("picked".into(), picked))?;
        self.kept = Some(match self.kept.take() {
            Some(kept) => kept.vstack(&rows)?,
            None => rows,
        });
        self.ranks.extend(ranks);
        if self.ranks.len() > 2 * self.n {
            self.prune()?;
        }
        Ok(())
    }

    /// Keep the `n` lowest-ranked rows, and raise the bar to the highest of them.
    fn prune(&mut self) -> PolarsResult<()> {
        let Some(kept) = self.kept.take() else {
            return Ok(());
        };
        let mut order: Vec<usize> = (0..self.ranks.len()).collect();
        order.sort_unstable_by_key(|i| self.ranks[*i]);
        order.truncate(self.n);
        let take: Vec<IdxSize> = order.iter().map(|i| *i as IdxSize).collect();
        self.kept = Some(kept.take(&IdxCa::from_vec("kept".into(), take))?);
        self.ranks = order.iter().map(|i| self.ranks[*i]).collect();
        if self.ranks.len() == self.n {
            self.bar = self.ranks.iter().copied().max().unwrap_or(u64::MAX);
        }
        Ok(())
    }

    /// The sample, back in table order without the position column, and where each
    /// of its rows sat.
    fn finish(mut self) -> PolarsResult<Option<(DataFrame, Vec<IdxSize>)>> {
        self.prune()?;
        let Some(kept) = self.kept else {
            return Ok(None);
        };
        let sorted = kept.sort([SAMPLE_POSITION], SortMultipleOptions::default())?;
        let positions = sorted
            .column(SAMPLE_POSITION)?
            .idx()?
            .into_no_null_iter()
            .collect();
        Ok(Some((sorted.drop(SAMPLE_POSITION)?, positions)))
    }
}

/// A seeded, well-mixed rank for a row position (SplitMix64's finalizer). The same seed
/// and table give the same sample; another seed gives another.
pub(crate) fn sample_rank(seed: u64, position: u64) -> u64 {
    let mut value = seed ^ position.wrapping_mul(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[cfg(test)]
mod sampler_tests;

#[cfg(test)]
mod tests {
    #[test]
    fn a_size_takes_shorthand() {
        use super::parse_size;
        for (text, rows) in [
            ("50000", 50_000),
            ("50,000", 50_000),
            ("1_000", 1_000),
            ("50k", 50_000),
            ("250K", 250_000),
            ("2m", 2_000_000),
            ("2.5M", 2_500_000),
            (" 7 ", 7),
            ("99999999999999999999999", usize::MAX),
        ] {
            assert_eq!(parse_size(text), Ok(rows), "{text}");
        }
        for bad in ["", "k", "12x", "1.5", "-3", "1e6", "2mm"] {
            assert_eq!(parse_size(bad), Err(super::SizeError::NotASize), "{bad}");
        }
        for zero in ["0", "0k", "0.0001k"] {
            assert_eq!(parse_size(zero), Err(super::SizeError::Zero), "{zero}");
        }
    }

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
        // A table smaller than the sample is read whole, and the line says so.
        assert_eq!(
            Sample::default().summary_within(Some(1_000)),
            format!("all 1,000 rows {middot} current view")
        );
        assert_eq!(
            Sample::default().summary_within(Some(1_000_000)),
            Sample::default().summary()
        );
        assert_eq!(
            Sample::default().summary_within(None),
            Sample::default().summary()
        );
    }
}

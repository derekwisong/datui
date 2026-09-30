//! The one sampler every analysis tool reads through: which rows (a scope), how they
//! are picked (a method), how many, and the seed. Describe, Distribution, Correlation
//! and Data Quality all take their rows from [`read`], so a sample means the same
//! thing whichever tool shows it.

use crate::data_quality::{
    QualityScope, QualitySourceContext, apply_quality_scope, prepare_source_quality_scan,
};
use crate::numfmt;
use crate::statistics::{AnalysisRows, analysis_rows, collect_lazy, sample_rank};
use color_eyre::Result;
use color_eyre::eyre::Report;
use polars::prelude::*;
use std::collections::HashMap;

/// The default sample size, before `[performance] analysis_sample_rows` says otherwise.
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
    let n = sample.rows.max(1);
    match &sample.method {
        SampleMethod::EveryRow => {
            analysis_rows(lf, None, known_total, sample.seed, polars_streaming)
        }
        SampleMethod::Spread => {
            analysis_rows(lf, Some(n), known_total, sample.seed, polars_streaming)
        }
        SampleMethod::FirstRows => {
            let df = collect_lazy(lf.clone().limit(n as IdxSize), polars_streaming)
                .map_err(Report::from)?;
            let height = df.height();
            // Without a count, a full head means there may be more: call it a sample.
            let sampled = match known_total {
                Some(total) => total > height,
                None => height == n,
            };
            Ok(AnalysisRows {
                df,
                total_rows: known_total.unwrap_or(height),
                sample_size: sampled.then_some(height),
                per_value: None,
            })
        }
        SampleMethod::PerPartition { column } => {
            let (df, total_rows, per_value) = per_group_sample(lf, column, n, sample.seed)?;
            let sample_size = (total_rows > df.height()).then_some(df.height());
            Ok(AnalysisRows {
                df,
                total_rows,
                sample_size,
                per_value: Some(per_value),
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

/// Up to `n` seeded rows from each value of `column`, from one streamed pass, in table
/// order, and how many rows there were.
///
/// Never refused for keeping too many rows. Whether `n` of every value fits is only
/// known once every value has been seen, which is the end of the read, and a read
/// that ends in a refusal has been paid for and thrown away. So the size per value
/// comes down as values arrive, to what [`MAX_GROUP_ROWS`] holds for all of them; each
/// value keeps its lowest-ranked rows, which is a seeded uniform sample of it at any
/// size, and [`PerValue::kept`] says what the size came down to.
fn per_group_sample(
    lf: &LazyFrame,
    column: &str,
    n: usize,
    seed: u64,
) -> Result<(DataFrame, usize, PerValue)> {
    per_group_sample_within(lf, column, n, seed, MAX_GROUP_ROWS)
}

/// [`per_group_sample`], holding at most `limit` rows.
fn per_group_sample_within(
    lf: &LazyFrame,
    column: &str,
    n: usize,
    seed: u64,
    limit: usize,
) -> Result<(DataFrame, usize, PerValue)> {
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
    let sink = lf
        .clone()
        .with_row_index(GROUP_POSITION, None)
        .sink_batches(
            PlanCallback::new(move |batch| {
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
    let df = match out {
        Some(df) => df
            .sort([GROUP_POSITION], SortMultipleOptions::default())?
            .drop(GROUP_POSITION)?,
        None => collect_lazy(lf.clone().limit(0), true).map_err(Report::from)?,
    };
    Ok((df, seen, PerValue { kept: cap, totals }))
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
    fn observe(&mut self, batch: DataFrame) -> PolarsResult<()> {
        self.seen += batch.height();
        let positions = batch.column(GROUP_POSITION)?.idx()?.clone();
        let keys = batch.column(&self.column)?.as_materialized_series().clone();
        let mut by_key: HashMap<Option<String>, (Vec<IdxSize>, Vec<u64>)> = HashMap::new();
        for (index, (key, position)) in keys.iter().zip(positions.into_no_null_iter()).enumerate() {
            // Named as a segment names its value, so the counts line up with segments.
            let key = (!key.is_null()).then(|| key.str_value().into_owned());
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
        let (df, seen, per_value) =
            per_group_sample_within(&table(), "part", 500, 42_891, 999).unwrap();
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
        let (asked, _, _) =
            per_group_sample_within(&table(), "part", 333, 42_891, usize::MAX).unwrap();
        assert!(df.equals(&asked), "the rows a sample of 333 each keeps");

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

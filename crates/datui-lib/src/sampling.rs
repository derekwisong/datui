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

/// A per-partition sample keeps at most this many partitions and rows in memory; past
/// either it is refused rather than trimmed, like Data Quality's per-segment budget.
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
            SampleMethod::Spread => format!("{rows} rows, spread"),
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
    /// scope when it is not simply the table as shown.
    pub fn outcome(&self, total_rows: usize, sample_size: Option<usize>) -> String {
        let count = numfmt::group_chrome;
        let read = match (&self.method, sample_size) {
            (SampleMethod::FirstRows, Some(n)) => format!("first {} rows", count(n)),
            (SampleMethod::PerPartition { column }, Some(n)) => format!(
                "{} rows, up to {} per {column}, of {}",
                count(n),
                count(self.rows),
                count(total_rows)
            ),
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
            })
        }
        SampleMethod::PerPartition { column } => {
            let (df, total_rows) = per_group_sample(lf, column, n, sample.seed)?;
            let sample_size = (total_rows > df.height()).then_some(df.height());
            Ok(AnalysisRows {
                df,
                total_rows,
                sample_size,
            })
        }
    }
}

/// Up to `n` seeded rows from each value of `column`, from one streamed pass, in table
/// order, and how many rows there were.
fn per_group_sample(
    lf: &LazyFrame,
    column: &str,
    n: usize,
    seed: u64,
) -> Result<(DataFrame, usize)> {
    let schema = lf.clone().collect_schema()?;
    if schema.get(column).is_none() {
        return Err(Report::msg(format!(
            "partition column {column:?} is not in the rows sampled; choose another"
        )));
    }
    let state = std::sync::Arc::new(std::sync::Mutex::new(GroupState {
        column: column.to_string(),
        n,
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
    let mut out: Option<DataFrame> = None;
    for group in state.groups.into_values() {
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
    Ok((df, seen))
}

#[derive(Default)]
struct GroupState {
    column: String,
    n: usize,
    seed: u64,
    seen: usize,
    groups: HashMap<String, GroupSample>,
}

#[derive(Default)]
struct GroupSample {
    rows: Option<DataFrame>,
    ranks: Vec<u64>,
}

impl GroupState {
    fn observe(&mut self, batch: DataFrame) -> PolarsResult<()> {
        self.seen += batch.height();
        let positions = batch.column(GROUP_POSITION)?.idx()?.clone();
        let keys = batch.column(&self.column)?.cast(&DataType::String)?;
        let keys = keys.str()?;
        let mut by_key: HashMap<String, (Vec<IdxSize>, Vec<u64>)> = HashMap::new();
        for (index, (key, position)) in keys.iter().zip(positions.into_no_null_iter()).enumerate() {
            let key = key.unwrap_or(crate::glyphs::get().null).to_string();
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
            let rows = batch.take(&IdxCa::from_vec("picked".into(), indices))?;
            group.rows = Some(match group.rows.take() {
                Some(kept) => kept.vstack(&rows)?,
                None => rows,
            });
            group.ranks.extend(ranks);
            // Keep the `n` lowest-ranked rows of the group: a seeded uniform sample of
            // it, whatever order its rows arrive in.
            if group.ranks.len() > self.n {
                let mut order: Vec<usize> = (0..group.ranks.len()).collect();
                order.sort_unstable_by_key(|i| group.ranks[*i]);
                order.truncate(self.n);
                let take: Vec<IdxSize> = order.iter().map(|i| *i as IdxSize).collect();
                if let Some(kept) = group.rows.take() {
                    group.rows = Some(kept.take(&IdxCa::from_vec("kept".into(), take))?);
                }
                group.ranks = order.iter().map(|i| group.ranks[*i]).collect();
            }
        }
        let kept: usize = self.groups.values().map(|group| group.ranks.len()).sum();
        if kept > MAX_GROUP_ROWS {
            return Err(PolarsError::ComputeError(
                format!(
                    "{} rows per {} across {} values would keep over {}; lower the rows",
                    numfmt::group_chrome(self.n),
                    self.column,
                    numfmt::group_chrome(self.groups.len()),
                    numfmt::group_chrome(MAX_GROUP_ROWS)
                )
                .into(),
            ));
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
            format!("100,000 rows, spread {middot} current view {middot} seed 42891")
        );
        assert_eq!(
            sample(SampleMethod::FirstRows, 1_000).summary(),
            format!("first 1,000 rows {middot} current view")
        );
    }
}

use crate::statistics::collect_lazy;
use color_eyre::Result;
use color_eyre::eyre::Report;
use polars::prelude::*;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

// Sampling is without replacement from a bounded prefix, never a full-source random scan.
const DEFAULT_SAMPLE_ROWS: usize = 10_000;
const DEFAULT_CHUNK_ROWS: usize = 1_000_000;
const QUALITY_SAMPLE_POSITION: &str = "__datui_quality_sample_position";
const QUALITY_WINDOW_START: &str = "__datui_quality_window_start";
const MAX_SAMPLE_SEGMENTS: usize = 10_000;
const MAX_RETAINED_SAMPLE_BYTES: usize = 512 * 1024 * 1024;
// A per-segment budget multiplies by the segment count, so a 10,000-row budget
// over 100 partitions keeps a million rows. Profiling those is eager and costs
// roughly ten seconds a million rows, so bound the total as well as the bytes.
pub const MAX_RETAINED_SAMPLE_ROWS: usize = 500_000;
pub const QUALITY_SOURCE_FILE_COLUMN: &str = "__datui_quality_source_file";
/// How nearly unique a column's values must be before its repeats are worth naming.
///
/// A key that is not quite one is the interesting case: an id that repeats twice in a
/// million rows is a fact about the data, while a category that repeats constantly is
/// just a category. The line has to fall somewhere, and 95% puts it where a column is
/// clearly meant to identify a row rather than to group them.
pub const KEY_LIKE_UNIQUENESS: f64 = 0.95;
/// Files named per drift observation, and values read from each of them. Both are the
/// evidence, not the measurement: the counts above them cover every file.
const MAX_EVIDENCE_FILES: usize = 20;
const MAX_CONFLICT_EXAMPLES: usize = 5;
/// Window widths offered for time-window grain, in the order the plan cycles them.
pub const QUALITY_WINDOW_WIDTHS: [&str; 4] = ["1h", "1d", "1w", "1mo"];

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum QualityScope {
    #[default]
    CurrentView,
    WholeSource,
    FirstRows(usize),
    ViewRows {
        start: usize,
        end: usize,
    },
    SourceFiles(Vec<usize>),
    SourcePartition {
        column: String,
        value: String,
    },
    SourceTimeRange {
        column: String,
        start: String,
        end: String,
    },
}

impl QualityScope {
    pub fn label(&self) -> String {
        match self {
            Self::CurrentView => "current view".to_string(),
            Self::WholeSource => "whole source".to_string(),
            Self::FirstRows(rows) => format!("first {rows} view rows"),
            Self::ViewRows { start, end } => format!("view rows {start}..{end}"),
            Self::SourceFiles(indices) => format!(
                "source files {}",
                indices
                    .iter()
                    .map(usize::to_string)
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            Self::SourcePartition { column, value } => format!("source {column}={value}"),
            Self::SourceTimeRange { column, start, end } => {
                format!("source {column} {start}..{end}")
            }
        }
    }

    pub fn uses_source(&self) -> bool {
        matches!(
            self,
            Self::WholeSource
                | Self::SourceFiles(_)
                | Self::SourcePartition { .. }
                | Self::SourceTimeRange { .. }
        )
    }

    pub fn command(&self) -> String {
        match self {
            Self::CurrentView => "view".to_string(),
            Self::WholeSource => "source".to_string(),
            Self::FirstRows(rows) => format!("rows 1..{rows}"),
            Self::ViewRows { start, end } => format!("rows {start}..{end}"),
            Self::SourceFiles(indices) => format!(
                "files {}",
                indices
                    .iter()
                    .map(usize::to_string)
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            Self::SourcePartition { column, value } => format!("partition {column}={value}"),
            Self::SourceTimeRange { column, start, end } => format!("time {column}={start}..{end}"),
        }
    }

    pub fn parse_command(text: &str) -> Result<Self> {
        let value = text.trim();
        if value == "view" {
            return Ok(Self::CurrentView);
        }
        if value == "source" {
            return Ok(Self::WholeSource);
        }
        if let Some(range) = value.strip_prefix("rows ") {
            let (start, end) = range
                .split_once("..")
                .ok_or_else(|| color_eyre::eyre::eyre!("use rows START..END"))?;
            let start = start.parse::<usize>()?;
            let end = end.parse::<usize>()?;
            if start == 0 || end < start {
                return Err(color_eyre::eyre::eyre!(
                    "row range must be 1-based with END >= START"
                ));
            }
            if start == 1 {
                // The same rows as FirstRows(end); use the one spelling so the
                // scope round-trips through the editor and keeps its cache entry.
                return Ok(Self::FirstRows(end));
            }
            return Ok(Self::ViewRows { start, end });
        }
        if let Some(files) = value.strip_prefix("files ") {
            let indices = files
                .split(',')
                .map(|part| part.trim().parse::<usize>())
                .collect::<std::result::Result<Vec<_>, _>>()?;
            if indices.is_empty() || indices.contains(&0) {
                return Err(color_eyre::eyre::eyre!(
                    "use 1-based file numbers, for example files 1,3"
                ));
            }
            let mut indices = indices;
            indices.sort_unstable();
            indices.dedup();
            return Ok(Self::SourceFiles(indices));
        }
        if let Some(partition) = value.strip_prefix("partition ") {
            let (column, value) = partition
                .split_once('=')
                .ok_or_else(|| color_eyre::eyre::eyre!("use partition COLUMN=VALUE"))?;
            if column.trim().is_empty() || value.trim().is_empty() {
                return Err(color_eyre::eyre::eyre!(
                    "partition column and value are required"
                ));
            }
            return Ok(Self::SourcePartition {
                column: column.trim().to_string(),
                value: value.trim().to_string(),
            });
        }
        if let Some(time) = value.strip_prefix("time ") {
            let (column, range) = time
                .split_once('=')
                .ok_or_else(|| color_eyre::eyre::eyre!("use time COLUMN=START..END"))?;
            let (start, end) = range
                .split_once("..")
                .ok_or_else(|| color_eyre::eyre::eyre!("use time COLUMN=START..END"))?;
            let (start, end) = (start.trim(), end.trim());
            if column.trim().is_empty()
                || parse_scope_time(start).is_none()
                || parse_scope_time(end).is_none()
                || parse_scope_time(end) <= parse_scope_time(start)
            {
                return Err(color_eyre::eyre::eyre!(
                    "time range needs a column and increasing ISO dates or UTC timestamps"
                ));
            }
            return Ok(Self::SourceTimeRange {
                column: column.trim().to_string(),
                start: start.to_string(),
                end: end.to_string(),
            });
        }
        Err(color_eyre::eyre::eyre!(
            "use view, source, rows, files, partition, or time"
        ))
    }
}

fn parse_scope_time(text: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(text)
        .ok()
        .map(|value| value.timestamp_micros())
        .or_else(|| {
            chrono::NaiveDate::parse_from_str(text, "%Y-%m-%d")
                .ok()
                .and_then(|value| value.and_hms_opt(0, 0, 0))
                .map(|value| value.and_utc().timestamp_micros())
        })
}

#[derive(Debug, Clone, Default)]
pub struct QualitySourceContext {
    pub file_names: Vec<String>,
    pub file_starts: Vec<usize>,
    pub row_index_column: String,
    /// Per file, in the order of `file_names`, its group in `drift_groups`. Empty when
    /// the dataset's files all agree with its schema, which is nearly all of them.
    pub file_group: Vec<u32>,
    /// The distinct ways this dataset's files differ from its schema, as the footers
    /// found them. Group 0 is always "nothing missing".
    pub drift_groups: Arc<Vec<crate::schema_union::DriftGroup>>,
    /// Per file, the type it holds each of its unreadable columns in. Empty for a file
    /// whose types all fit, which is why it is kept beside the groups rather than in
    /// them: the type is the only way back to the values a conflict hides.
    pub file_omitted: Vec<Vec<(PlSmallStr, DataType)>>,
    /// Rows in the whole loaded source, which is what closes the last file's range.
    pub dataset_rows: usize,
    /// How many of the source's footers were read. Below the file count on a dataset
    /// too large to read every footer, where a file nobody looked at is indistinguishable
    /// from one missing nothing — so a count over the files is a floor, not a total.
    pub footers_read: usize,
    /// How to read a column at the type a file wrote it in, for the values a type
    /// conflict hides. `None` for a dataset whose files agree, and for a run whose
    /// budget did not promise the extra reads.
    pub conflict_scan: Option<QualityConflictScan>,
}

impl QualitySourceContext {
    /// What the file at `file` is missing. Group 0 for a file that agrees with the
    /// dataset's schema, and for a dataset whose files were never grouped.
    fn group_of_file(&self, file: usize) -> Option<&crate::schema_union::DriftGroup> {
        let group = *self.file_group.get(file)? as usize;
        self.drift_groups.get(group)
    }

    /// The files this dataset is missing something from, by 1-based inventory number,
    /// paired with what each is missing. Only files that differ have an entry.
    fn drifting_files(&self) -> impl Iterator<Item = (usize, &crate::schema_union::DriftGroup)> {
        (0..self.file_names.len()).filter_map(move |file| {
            let group = self.group_of_file(file)?;
            (!group.is_empty()).then_some((file, group))
        })
    }

    /// Rows the file at `file` holds, from its footer.
    fn file_rows(&self, file: usize) -> usize {
        let Some(start) = self.file_starts.get(file) else {
            return 0;
        };
        self.file_starts
            .get(file + 1)
            .copied()
            .unwrap_or(self.dataset_rows)
            .saturating_sub(*start)
    }

    /// The type the file at `file` holds `column` in, when that is not the type the
    /// scan reads it as.
    fn stored_type(&self, file: usize, column: &str) -> Option<&DataType> {
        self.file_omitted
            .get(file)?
            .iter()
            .find(|(name, _)| name.as_str() == column)
            .map(|(_, dtype)| dtype)
    }
}

/// Prepare the loaded source in the worker, keeping only a provenance index
/// and replacing binary payloads before any value collection.
pub fn prepare_source_quality_scan(
    lf: LazyFrame,
    source: Option<&QualitySourceContext>,
) -> Result<LazyFrame> {
    let schema = lf.clone().collect_schema()?;
    let expressions = schema
        .iter()
        .filter_map(|(name, dtype)| {
            let column = name.as_str();
            if column == crate::schema_union::DRIFT_COLUMN
                && !source.is_some_and(|context| context.row_index_column == column)
            {
                return None;
            }
            Some(if matches!(dtype, DataType::Binary) {
                lit(crate::widgets::datatable::BINARY_STUB).alias(column)
            } else {
                col(column)
            })
        })
        .collect::<Vec<_>>();
    let lf = lf.select(expressions);
    Ok(
        if source.is_some_and(|context| context.row_index_column == "__datui_quality_row") {
            lf.with_row_index("__datui_quality_row", None)
        } else {
            lf
        },
    )
}

pub fn apply_quality_scope(
    lf: LazyFrame,
    scope: &QualityScope,
    source: Option<&QualitySourceContext>,
) -> Result<LazyFrame> {
    match scope {
        QualityScope::CurrentView | QualityScope::WholeSource => Ok(lf),
        QualityScope::FirstRows(rows) => Ok(lf.slice(0, (*rows).min(u32::MAX as usize) as u32)),
        QualityScope::ViewRows { start, end } => {
            if *start == 0 || end < start {
                return Err(color_eyre::eyre::eyre!("invalid 1-based view row range"));
            }
            let offset = i64::try_from(start - 1)?;
            let length = end
                .saturating_sub(*start)
                .saturating_add(1)
                .min(u32::MAX as usize) as u32;
            Ok(lf.slice(offset, length))
        }
        QualityScope::SourceFiles(indices) => {
            let source = source
                .ok_or_else(|| color_eyre::eyre::eyre!("source-file positions are unavailable"))?;
            let mut predicate: Option<Expr> = None;
            for index in indices {
                let file = index
                    .checked_sub(1)
                    .ok_or_else(|| color_eyre::eyre::eyre!("source file numbers start at 1"))?;
                let start = *source.file_starts.get(file).ok_or_else(|| {
                    color_eyre::eyre::eyre!("source file #{index} is unavailable")
                })?;
                let start = u32::try_from(start)?;
                let mut range = col(&source.row_index_column).gt_eq(lit(start));
                if let Some(end) = source.file_starts.get(*index) {
                    range = range.and(col(&source.row_index_column).lt(lit(u32::try_from(*end)?)));
                }
                predicate = Some(match predicate {
                    Some(previous) => previous.or(range),
                    None => range,
                });
            }
            Ok(lf.filter(
                predicate.ok_or_else(|| color_eyre::eyre::eyre!("select at least one file"))?,
            ))
        }
        QualityScope::SourcePartition { column, value } => {
            let schema = lf.clone().collect_schema()?;
            if !schema.contains(column.as_str()) {
                return Err(color_eyre::eyre::eyre!(
                    "partition column {column:?} is unavailable"
                ));
            }
            let predicate = if value == "∅" {
                col(column).is_null()
            } else {
                col(column).cast(DataType::String).eq(lit(value.clone()))
            };
            Ok(lf.filter(predicate))
        }
        QualityScope::SourceTimeRange { column, start, end } => {
            let schema = lf.clone().collect_schema()?;
            let dtype = schema
                .get(column.as_str())
                .ok_or_else(|| color_eyre::eyre::eyre!("time column {column:?} is unavailable"))?;
            if !matches!(dtype, DataType::Date | DataType::Datetime(..)) {
                return Err(color_eyre::eyre::eyre!(
                    "{column:?} is not a date or datetime column"
                ));
            }
            let start = parse_scope_time(start)
                .ok_or_else(|| color_eyre::eyre::eyre!("invalid start time"))?;
            let end =
                parse_scope_time(end).ok_or_else(|| color_eyre::eyre::eyre!("invalid end time"))?;
            if end <= start {
                return Err(color_eyre::eyre::eyre!("time end must be after start"));
            }
            let value = col(column).cast(DataType::Datetime(TimeUnit::Microseconds, None));
            Ok(lf.filter(
                value
                    .clone()
                    .gt_eq(lit(start).cast(DataType::Datetime(TimeUnit::Microseconds, None)))
                    .and(value.lt(lit(end).cast(DataType::Datetime(TimeUnit::Microseconds, None)))),
            ))
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum QualityPage {
    #[default]
    Plan,
    Scope,
    Overview,
    Columns,
    Segments,
    Trends,
    Detail,
    TimeRoles,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum QualityCompute {
    Metadata,
    #[default]
    Sample,
    Full,
}

impl QualityCompute {
    pub fn label(self) -> &'static str {
        match self {
            Self::Metadata => "metadata",
            Self::Sample => "sample",
            Self::Full => "full",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum QualityGrain {
    #[default]
    Dataset,
    File,
    Partition(String),
    RowChunks(usize),
    TimeWindows {
        column: String,
        every: String,
    },
}

impl QualityGrain {
    pub fn label(&self) -> String {
        match self {
            Self::Dataset => "dataset".to_string(),
            Self::File => "file".to_string(),
            Self::Partition(column) => format!("partition:{column}"),
            Self::RowChunks(rows) => format!("{rows} rows (physical order)"),
            Self::TimeWindows { column, every } => format!("{every} on {column}"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum QualityComparison {
    #[default]
    None,
    Previous,
    Baseline,
}

impl QualityComparison {
    pub fn label(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Previous => "previous",
            Self::Baseline => "baseline (first)",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TemporalRole {
    Event,
    Effective,
    PeriodEnd,
    Created,
    Published,
    Received,
    Processed,
    ValidFrom,
    ValidTo,
}

impl TemporalRole {
    pub const ALL: [Self; 9] = [
        Self::Event,
        Self::Effective,
        Self::PeriodEnd,
        Self::Created,
        Self::Published,
        Self::Received,
        Self::Processed,
        Self::ValidFrom,
        Self::ValidTo,
    ];

    pub fn label(self) -> &'static str {
        match self {
            Self::Event => "event",
            Self::Effective => "effective/as-of",
            Self::PeriodEnd => "period end",
            Self::Created => "created",
            Self::Published => "published",
            Self::Received => "received",
            Self::Processed => "processed",
            Self::ValidFrom => "valid from",
            Self::ValidTo => "valid to",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TemporalRoleAssignment {
    pub role: TemporalRole,
    pub column: String,
    pub timezone: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataQualityPlan {
    pub scope: QualityScope,
    pub compute: QualityCompute,
    pub sample_rows: usize,
    pub sample_seed: u64,
    pub grain: QualityGrain,
    pub comparison: QualityComparison,
    pub baseline_segment: Option<String>,
    pub temporal_roles: Vec<TemporalRoleAssignment>,
    pub latency_threshold_seconds: Option<i64>,
}

impl Default for DataQualityPlan {
    fn default() -> Self {
        Self {
            scope: QualityScope::CurrentView,
            compute: QualityCompute::Sample,
            sample_rows: DEFAULT_SAMPLE_ROWS,
            sample_seed: 42_891,
            grain: QualityGrain::Dataset,
            comparison: QualityComparison::None,
            baseline_segment: None,
            temporal_roles: Vec::new(),
            latency_threshold_seconds: None,
        }
    }
}

impl DataQualityPlan {
    pub fn samples_each_segment(&self) -> bool {
        self.compute == QualityCompute::Sample && !matches!(self.grain, QualityGrain::Dataset)
    }

    pub fn requires_confirmation(&self) -> bool {
        self.compute == QualityCompute::Full || self.samples_each_segment()
    }

    pub fn comparison_label(&self) -> String {
        if self.comparison == QualityComparison::Baseline {
            self.baseline_segment
                .as_ref()
                .map(|label| format!("baseline: {label}"))
                .unwrap_or_else(|| self.comparison.label().to_string())
        } else {
            self.comparison.label().to_string()
        }
    }

    pub fn set_row_chunks(&mut self) {
        self.grain = QualityGrain::RowChunks(DEFAULT_CHUNK_ROWS);
    }

    pub fn compact_summary(&self) -> String {
        format!(
            "scope {} -> grain {} -> compute {} -> compare {}",
            self.scope.label(),
            self.grain.label(),
            match self.compute {
                QualityCompute::Sample if self.samples_each_segment() => {
                    format!("{} rows/segment", self.sample_rows.min(50_000))
                }
                QualityCompute::Sample => format!("{} rows", self.sample_rows.min(50_000)),
                other => other.label().to_string(),
            },
            self.comparison_label()
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QualityPrecision {
    Metadata,
    Sampled,
    Exact,
    Estimated,
}

impl QualityPrecision {
    pub fn label(self) -> &'static str {
        match self {
            Self::Metadata => "metadata",
            Self::Sampled => "sampled",
            Self::Exact => "exact",
            Self::Estimated => "estimated",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum QualityMetric {
    #[default]
    NullRate,
    EmptyRate,
    WhitespaceRate,
    NonFiniteRate,
    DistinctShare,
    IntegerParseShare,
    DecimalParseShare,
}

impl QualityMetric {
    pub const ALL: [Self; 7] = [
        Self::NullRate,
        Self::EmptyRate,
        Self::WhitespaceRate,
        Self::NonFiniteRate,
        Self::DistinctShare,
        Self::IntegerParseShare,
        Self::DecimalParseShare,
    ];

    pub fn label(self) -> &'static str {
        match self {
            Self::NullRate => "Null rate",
            Self::EmptyRate => "Empty rate",
            Self::WhitespaceRate => "Whitespace rate",
            Self::NonFiniteRate => "Non-finite rate",
            Self::DistinctShare => "Distinct share",
            Self::IntegerParseShare => "Integer parse share",
            Self::DecimalParseShare => "Decimal parse share",
        }
    }

    pub fn value(self, column: &ColumnQualityProfile) -> Option<f64> {
        let ratio = |numerator: usize, denominator: usize| {
            (denominator > 0).then(|| numerator as f64 / denominator as f64)
        };
        match self {
            Self::NullRate => ratio(column.null_count, column.evaluated_rows),
            Self::EmptyRate => ratio(column.empty_count?, column.evaluated_rows),
            Self::WhitespaceRate => ratio(column.whitespace_count?, column.evaluated_rows),
            Self::NonFiniteRate => ratio(
                column.nan_count?
                    + column.positive_infinity_count?
                    + column.negative_infinity_count?,
                column.evaluated_rows,
            ),
            Self::DistinctShare => ratio(column.distinct_count?, column.non_null_rows()),
            Self::IntegerParseShare => ratio(column.integer_parse_count?, column.non_null_rows()),
            Self::DecimalParseShare => ratio(column.decimal_parse_count?, column.non_null_rows()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ColumnQualityProfile {
    pub name: String,
    pub dtype: DataType,
    pub evaluated_rows: usize,
    pub null_count: usize,
    pub empty_count: Option<usize>,
    pub whitespace_count: Option<usize>,
    pub nan_count: Option<usize>,
    pub positive_infinity_count: Option<usize>,
    pub negative_infinity_count: Option<usize>,
    pub distinct_count: Option<usize>,
    pub min: Option<String>,
    pub max: Option<String>,
    pub integer_parse_count: Option<usize>,
    pub decimal_parse_count: Option<usize>,
    pub date_parse_count: Option<usize>,
    pub datetime_parse_count: Option<usize>,
    pub dominant_value: Option<String>,
    pub dominant_count: Option<usize>,
    pub min_length: Option<usize>,
    pub max_length: Option<usize>,
}

impl ColumnQualityProfile {
    pub fn null_rate(&self) -> f64 {
        rate(self.null_count, self.evaluated_rows)
    }

    pub fn non_null_rows(&self) -> usize {
        self.evaluated_rows.saturating_sub(self.null_count)
    }

    pub fn uniqueness_rate(&self) -> Option<f64> {
        self.distinct_count
            .map(|count| rate(count, self.non_null_rows()))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObservationKind {
    Nulls,
    Empty,
    Whitespace,
    NonFinite,
    Constant,
    ParseableText,
    DuplicateRows,
    CategoryVariants,
    /// Rows whose own file has no such column. Their cells are absent, not null, and
    /// no measurement over values can tell the two apart.
    Absent,
    /// Rows whose file holds the column in a type the dataset's schema cannot read, so
    /// the column is not read from that file at all.
    TypeConflict,
    /// A column whose values are nearly unique and still repeat: the shape of a key
    /// that is not quite one.
    KeyLike,
}

impl ObservationKind {
    pub fn label(self) -> &'static str {
        match self {
            Self::Nulls => "Null",
            Self::Empty => "Empty",
            Self::Whitespace => "Whitespace",
            Self::NonFinite => "Non-finite",
            Self::Constant => "Constant",
            Self::ParseableText => "Stored as text",
            Self::DuplicateRows => "Duplicate rows",
            Self::CategoryVariants => "Category variants",
            Self::Absent => "Absent",
            Self::TypeConflict => "Type conflict",
            Self::KeyLike => "Key-like",
        }
    }

    /// What the check divides, as the detail pane and the user guide state it.
    pub fn definition(self) -> &'static str {
        match self {
            Self::Nulls => "Null values / evaluated rows",
            Self::Empty => "Exact empty strings / evaluated rows",
            Self::Whitespace => "Nonempty strings that trim to empty / evaluated rows",
            Self::NonFinite => "NaN or positive/negative infinity / evaluated rows",
            Self::Constant => "One distinct non-null value in evaluated rows",
            Self::ParseableText => "Values parseable as a typed value, stored as text",
            Self::DuplicateRows => "Equal complete rows; extras = sum(group size - 1)",
            Self::CategoryVariants => "Distinct originals equal after trim and lowercase",
            Self::Absent => "Rows in files whose footer has no such column / source rows",
            Self::TypeConflict => {
                "Rows in files holding the column in an unreadable type / source rows"
            }
            Self::KeyLike => {
                "Non-null rows - distinct values, where distinct >= 95% of non-null rows"
            }
        }
    }
}

/// One file behind a drift observation: what it holds, and what that costs the column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QualityFileEvidence {
    /// Position in the Scope page's file inventory, which numbers files from 1.
    pub number: usize,
    pub name: String,
    /// Rows this file holds, from its footer.
    pub rows: usize,
    /// The type this file holds the column in, when the scan cannot read it as the
    /// dataset's. `None` for a file that simply has no such column.
    pub stored_type: Option<String>,
    /// The first values this file holds, read at its own type and rendered as text.
    /// Empty until a full run reads them.
    pub examples: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct QualityObservation {
    pub kind: ObservationKind,
    pub column: String,
    pub affected_rows: usize,
    pub evaluated_rows: usize,
    pub fact: String,
    pub normalized_category: Option<String>,
    /// The files behind an [`ObservationKind::Absent`] or
    /// [`ObservationKind::TypeConflict`] measurement, commonest first. Empty for every
    /// check measured over values rather than over footers.
    pub files: Vec<QualityFileEvidence>,
}

impl QualityObservation {
    /// The scope that holds the rows behind this observation, when they are a set of
    /// files rather than a predicate over values. An absent or conflicting cell has no
    /// value to filter on — the rows are simply the ones the files contributed.
    pub fn evidence_scope(&self) -> Option<QualityScope> {
        if !matches!(
            self.kind,
            ObservationKind::Absent | ObservationKind::TypeConflict
        ) || self.files.is_empty()
        {
            return None;
        }
        Some(QualityScope::SourceFiles(
            self.files.iter().map(|file| file.number).collect(),
        ))
    }

    pub fn evidence_predicate(&self) -> Option<Expr> {
        let value = col(&self.column);
        match self.kind {
            ObservationKind::Nulls => Some(value.is_null()),
            ObservationKind::Empty => Some(value.eq(lit(""))),
            ObservationKind::Whitespace => Some(
                value
                    .clone()
                    .cast(DataType::String)
                    .str()
                    .strip_chars(lit(LiteralValue::untyped_null()))
                    .eq(lit(""))
                    .and(value.neq(lit(""))),
            ),
            ObservationKind::NonFinite => Some(
                value
                    .clone()
                    .is_nan()
                    .or(value.clone().eq(lit(f64::INFINITY)))
                    .or(value.eq(lit(f64::NEG_INFINITY))),
            ),
            ObservationKind::Constant => Some(value.is_not_null()),
            ObservationKind::CategoryVariants => Some(
                value
                    .cast(DataType::String)
                    .str()
                    .strip_chars(lit(LiteralValue::untyped_null()))
                    .str()
                    .to_lowercase()
                    .eq(lit(self.normalized_category.clone()?)),
            ),
            // Nearly unique and still repeating: the repeats are exactly the rows
            // whose value is not the only one of its kind. Nulls are outside the
            // measurement, so they are outside its rows too.
            ObservationKind::KeyLike => {
                Some(value.clone().is_duplicated().and(value.is_not_null()))
            }
            // Absent and conflicting rows are named by their files, not by a predicate
            // over values: the column is not in those rows to be tested.
            ObservationKind::ParseableText
            | ObservationKind::DuplicateRows
            | ObservationKind::Absent
            | ObservationKind::TypeConflict => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct IdentityProfile {
    pub duplicate_groups: usize,
    pub extra_rows: usize,
    pub rows_involved: usize,
    pub evaluated_rows: usize,
    pub precision: QualityPrecision,
}

#[derive(Debug, Clone)]
pub struct CategoryVariantGroup {
    pub column: String,
    pub normalized: String,
    pub variants: Vec<(String, usize)>,
    pub rows_involved: usize,
    pub complete: bool,
}

#[derive(Debug, Clone)]
pub struct SegmentQualityProfile {
    pub label: String,
    pub total_rows: Option<usize>,
    pub evaluated_rows: usize,
    pub columns: Vec<ColumnQualityProfile>,
    pub null_cells: usize,
    pub null_rate: f64,
    pub compared_with: Option<String>,
    pub largest_change: Option<String>,
}

#[derive(Debug, Clone)]
pub struct TemporalLatencyProfile {
    pub segment: String,
    pub start_role: TemporalRole,
    pub end_role: TemporalRole,
    pub start_column: String,
    pub end_column: String,
    pub evaluated_rows: usize,
    pub missing_start: usize,
    pub missing_end: usize,
    pub negative_count: usize,
    pub p50_seconds: Option<i64>,
    pub p90_seconds: Option<i64>,
    pub p95_seconds: Option<i64>,
    pub p99_seconds: Option<i64>,
    pub max_seconds: Option<i64>,
    pub above_threshold_count: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct DataQualityResults {
    pub total_rows: Option<usize>,
    pub evaluated_rows: usize,
    pub precision: QualityPrecision,
    pub sample_seed: u64,
    pub columns: Vec<ColumnQualityProfile>,
    pub observations: Vec<QualityObservation>,
    pub segments: Vec<SegmentQualityProfile>,
    pub temporal: Vec<TemporalLatencyProfile>,
    pub identity: Option<IdentityProfile>,
    pub category_variants: Vec<CategoryVariantGroup>,
}

impl DataQualityResults {
    pub fn compare_segments(&mut self, plan: &DataQualityPlan) {
        apply_comparisons(
            &mut self.segments,
            &plan.grain,
            plan.comparison,
            plan.baseline_segment.as_deref(),
            self.precision,
        );
    }

    pub fn empty(total_rows: Option<usize>, plan: &DataQualityPlan, schema: &Schema) -> Self {
        Self {
            total_rows,
            evaluated_rows: 0,
            precision: QualityPrecision::Metadata,
            sample_seed: plan.sample_seed,
            columns: schema
                .iter()
                .map(|(name, dtype)| ColumnQualityProfile {
                    name: name.to_string(),
                    dtype: dtype.clone(),
                    evaluated_rows: 0,
                    null_count: 0,
                    empty_count: None,
                    whitespace_count: None,
                    nan_count: None,
                    positive_infinity_count: None,
                    negative_infinity_count: None,
                    distinct_count: None,
                    min: None,
                    max: None,
                    integer_parse_count: None,
                    decimal_parse_count: None,
                    date_parse_count: None,
                    datetime_parse_count: None,
                    dominant_value: None,
                    dominant_count: None,
                    min_length: None,
                    max_length: None,
                })
                .collect(),
            observations: Vec::new(),
            segments: Vec::new(),
            temporal: Vec::new(),
            identity: None,
            category_variants: Vec::new(),
        }
    }
}

pub fn compute_data_quality(
    lf: &LazyFrame,
    total_rows: Option<usize>,
    plan: &DataQualityPlan,
    source: Option<&QualitySourceContext>,
    polars_streaming: bool,
) -> Result<DataQualityResults> {
    let collected_schema = lf.clone().collect_schema()?;
    let schema = visible_schema(&collected_schema, source);
    // What the footers already said: which files have which columns. Free at every
    // compute budget, including the one that reads no values at all.
    if plan.compute == QualityCompute::Metadata {
        let mut results = DataQualityResults::empty(total_rows, plan, &schema);
        if let Some(source) = source {
            results.observations = drift_observations(source, None, polars_streaming);
        }
        return Ok(results);
    }
    let grain_column = match &plan.grain {
        QualityGrain::Partition(column) | QualityGrain::TimeWindows { column, .. } => Some(column),
        _ => None,
    };
    if let Some(column) = grain_column
        && collected_schema.get(column).is_none()
    {
        return Err(Report::msg(format!(
            "Grain column {column} is not in scope {}; choose another grain or scope",
            plan.scope.label()
        )));
    }
    if plan.compute == QualityCompute::Full {
        let total_rows = match total_rows {
            Some(rows) => rows,
            None => {
                let count = collect_lazy(
                    crate::widgets::datatable::row_count_lf(lf),
                    polars_streaming,
                )
                .map_err(Report::from)?;
                let count_values = count
                    .get(0)
                    .ok_or_else(|| Report::msg("Data quality row count was not returned"))?;
                let Some(AnyValue::UInt64(rows)) = count_values.first() else {
                    return Err(Report::msg("Data quality row count was not UInt64"));
                };
                *rows as usize
            }
        };
        return compute_full_quality(lf, total_rows, plan, source, &schema, polars_streaming);
    }

    let (profile_df, sample_positions, evaluated_rows, precision, total_rows, segment_totals) =
        match plan.compute {
            QualityCompute::Sample if plan.samples_each_segment() => {
                let sampled = sample_quality_segments(lf, plan, source, polars_streaming)?;
                let height = sampled.rows.height();
                let precision = if height == sampled.total_rows {
                    QualityPrecision::Exact
                } else {
                    QualityPrecision::Sampled
                };
                (
                    sampled.rows,
                    Some(sampled.positions),
                    height,
                    precision,
                    Some(sampled.total_rows),
                    Some(sampled.segment_totals),
                )
            }
            QualityCompute::Sample
                if total_rows.is_none_or(|rows| rows > plan.sample_rows.min(50_000)) =>
            {
                let (df, positions, observed_total) =
                    sample_quality_rows(lf, plan.sample_rows, plan.sample_seed, polars_streaming)?;
                let height = df.height();
                let total_rows = total_rows.or(observed_total);
                let precision = if total_rows == Some(height) {
                    QualityPrecision::Exact
                } else {
                    QualityPrecision::Sampled
                };
                (df, Some(positions), height, precision, total_rows, None)
            }
            QualityCompute::Sample => {
                let df = collect_lazy(lf.clone(), polars_streaming).map_err(Report::from)?;
                let height = df.height();
                (
                    df,
                    None,
                    height,
                    QualityPrecision::Exact,
                    Some(height),
                    None,
                )
            }
            QualityCompute::Metadata | QualityCompute::Full => unreachable!(),
        };

    let profile_df = attach_source_file(profile_df, source)?;
    let mut columns = profile_columns(&profile_df, &schema, polars_streaming)?;
    add_value_details(&profile_df, &mut columns)?;
    let identity = profile_identity(&profile_df, &schema, precision)?;
    let category_variants = profile_category_variants(&profile_df, &schema)?;
    let mut observations = observations_from_profiles(&columns);
    observations.extend(identity_observations(&identity, &category_variants));
    // A sampled run does not promise the extra reads, so the counts come without the
    // values behind them.
    if let Some(source) = source {
        observations.extend(drift_observations(source, None, polars_streaming));
    }
    let segments = profile_segments(
        &profile_df,
        total_rows,
        plan,
        precision,
        &schema,
        SegmentSampleProvenance {
            positions: sample_positions.as_deref(),
            totals: segment_totals.as_ref(),
        },
        polars_streaming,
    )?;
    let temporal = profile_temporal(&profile_df, plan, sample_positions.as_deref())?;

    Ok(DataQualityResults {
        total_rows,
        evaluated_rows,
        precision,
        sample_seed: plan.sample_seed,
        columns,
        observations,
        segments,
        temporal,
        identity: Some(identity),
        category_variants,
    })
}

fn sample_quality_rows(
    lf: &LazyFrame,
    sample_rows: usize,
    seed: u64,
    polars_streaming: bool,
) -> Result<(DataFrame, Vec<u32>, Option<usize>)> {
    let requested = sample_rows.min(50_000);
    let candidate_rows = requested.saturating_mul(2).min(50_000);
    let probe_rows = candidate_rows.saturating_add(1).min(50_000);
    let mut candidates = collect_lazy(lf.clone().limit(probe_rows as u32), polars_streaming)
        .map_err(Report::from)?;
    let observed_total = (candidates.height() < probe_rows).then_some(candidates.height());
    if candidates.height() > candidate_rows {
        candidates = candidates.slice(0, candidate_rows);
    }
    if candidates.height() <= requested {
        let positions = (0..candidates.height() as u32).collect();
        return Ok((candidates, positions, observed_total));
    }
    let mut indices = (0..candidates.height() as u32).collect::<Vec<_>>();
    let mut random = seed;
    for index in 0..requested {
        // SplitMix64 produces a stable selection without adding a runtime dependency.
        random = random.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut value = random;
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        value ^= value >> 31;
        let selected = index + value as usize % (indices.len() - index);
        indices.swap(index, selected);
    }
    indices.truncate(requested);
    indices.sort_unstable();
    let sampled = candidates.take(&UInt32Chunked::new(
        "quality_sample".into(),
        indices.clone(),
    ))?;
    Ok((sampled, indices, observed_total))
}

#[derive(Default)]
struct SegmentSampleState {
    total_rows: usize,
    retained_rows: usize,
    retained_bytes: usize,
    segments: BTreeMap<String, SegmentSample>,
}

struct SegmentSample {
    total_rows: usize,
    rows: DataFrame,
    ranks: Vec<(u64, u32)>,
}

struct SegmentSampleOutput {
    rows: DataFrame,
    positions: Vec<u32>,
    segment_totals: BTreeMap<String, usize>,
    total_rows: usize,
}

impl SegmentSampleState {
    fn observe(
        &mut self,
        batch: DataFrame,
        plan: &DataQualityPlan,
        source: Option<&QualitySourceContext>,
    ) -> PolarsResult<()> {
        let positions = batch
            .column(QUALITY_SAMPLE_POSITION)?
            .u32()?
            .into_no_null_iter()
            .collect::<Vec<_>>();
        let labeled = if matches!(plan.grain, QualityGrain::File) {
            attach_source_file(batch.clone(), source)
                .map_err(|error| PolarsError::ComputeError(error.to_string().into()))?
        } else {
            batch.clone()
        };
        let groups = segment_rows(&labeled, plan, Some(&positions))
            .map_err(|error| PolarsError::ComputeError(error.to_string().into()))?;
        let requested = plan.sample_rows.clamp(1, 50_000);
        self.total_rows += batch.height();
        for group in groups {
            let mut candidates = group
                .indices
                .iter()
                .map(|index| {
                    let position = positions[*index as usize];
                    (sample_rank(plan.sample_seed, position), position, *index)
                })
                .collect::<Vec<_>>();
            candidates.sort_unstable();
            candidates.truncate(requested);
            let candidate_indices = candidates.iter().map(|item| item.2).collect::<Vec<_>>();
            let candidate_rows = take_rows(&batch, &candidate_indices)?;
            let candidate_ranks = candidates
                .iter()
                .map(|item| (item.0, item.1))
                .collect::<Vec<_>>();
            if let Some(segment) = self.segments.get_mut(&group.label) {
                self.retained_bytes -= segment.rows.estimated_size();
                self.retained_rows -= segment.rows.height();
                segment.total_rows += group.indices.len();
                let combined = segment.rows.vstack(&candidate_rows)?;
                let mut ranks = std::mem::take(&mut segment.ranks);
                ranks.extend(candidate_ranks);
                let mut order = (0..ranks.len()).collect::<Vec<_>>();
                order.sort_unstable_by_key(|index| ranks[*index]);
                order.truncate(requested);
                let indices = order.iter().map(|index| *index as u32).collect::<Vec<_>>();
                segment.rows = take_rows(&combined, &indices)?;
                segment.ranks = order.iter().map(|index| ranks[*index]).collect();
                self.retained_bytes += segment.rows.estimated_size();
                self.retained_rows += segment.rows.height();
            } else {
                if self.segments.len() >= MAX_SAMPLE_SEGMENTS {
                    return Err(PolarsError::ComputeError(
                        "Data quality sample exceeds 10,000 segments; narrow the scope".into(),
                    ));
                }
                self.retained_bytes += candidate_rows.estimated_size();
                self.retained_rows += candidate_rows.height();
                self.segments.insert(
                    group.label,
                    SegmentSample {
                        total_rows: group.indices.len(),
                        rows: candidate_rows,
                        ranks: candidate_ranks,
                    },
                );
            }
            if self.retained_rows > MAX_RETAINED_SAMPLE_ROWS {
                return Err(PolarsError::ComputeError(
                    format!(
                        "Data quality sample would keep more than {MAX_RETAINED_SAMPLE_ROWS} rows ({} rows/segment across {} segments so far); narrow the scope or reduce sample rows",
                        requested,
                        self.segments.len()
                    )
                    .into(),
                ));
            }
            if self.retained_bytes > MAX_RETAINED_SAMPLE_BYTES {
                return Err(PolarsError::ComputeError(
                    "Data quality sample exceeds 512 MiB retained; narrow the scope or reduce sample rows"
                        .into(),
                ));
            }
        }
        Ok(())
    }
}

fn sample_rank(seed: u64, position: u32) -> u64 {
    let mut value = seed ^ u64::from(position).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn sample_quality_segments(
    lf: &LazyFrame,
    plan: &DataQualityPlan,
    source: Option<&QualitySourceContext>,
    polars_streaming: bool,
) -> Result<SegmentSampleOutput> {
    let state = Arc::new(Mutex::new(SegmentSampleState::default()));
    let callback_state = Arc::clone(&state);
    let callback_plan = plan.clone();
    let callback_source = source.cloned();
    let sink = lf
        .clone()
        .with_row_index(QUALITY_SAMPLE_POSITION, None)
        .sink_batches(
            PlanCallback::new(move |batch| {
                callback_state
                    .lock()
                    .map_err(|_| {
                        PolarsError::ComputeError("Data quality sampler lock failed".into())
                    })?
                    .observe(batch, &callback_plan, callback_source.as_ref())?;
                Ok(false)
            }),
            true,
            None,
        )?;
    collect_lazy(sink, polars_streaming).map_err(Report::from)?;
    let mut state = state
        .lock()
        .map_err(|_| Report::msg("Data quality sampler lock failed"))?;
    let state = std::mem::take(&mut *state);
    if state.segments.is_empty() {
        return Ok(SegmentSampleOutput {
            rows: collect_lazy(lf.clone().limit(0), polars_streaming).map_err(Report::from)?,
            positions: Vec::new(),
            segment_totals: BTreeMap::new(),
            total_rows: 0,
        });
    }
    let mut rows: Option<DataFrame> = None;
    let mut positions = Vec::new();
    let mut totals = BTreeMap::new();
    for (label, segment) in state.segments {
        totals.insert(label, segment.total_rows);
        positions.extend(segment.ranks.iter().map(|rank| rank.1));
        rows = Some(match rows {
            Some(frame) => frame.vstack(&segment.rows)?,
            None => segment.rows,
        });
    }
    let mut order = (0..positions.len()).collect::<Vec<_>>();
    order.sort_unstable_by_key(|index| positions[*index]);
    let indices = order.iter().map(|index| *index as u32).collect::<Vec<_>>();
    let rows = take_rows(&rows.expect("sample has segments"), &indices)?;
    let positions = order.iter().map(|index| positions[*index]).collect();
    Ok(SegmentSampleOutput {
        rows,
        positions,
        segment_totals: totals,
        total_rows: state.total_rows,
    })
}

fn compute_full_quality(
    lf: &LazyFrame,
    total_rows: usize,
    plan: &DataQualityPlan,
    source: Option<&QualitySourceContext>,
    schema: &Schema,
    polars_streaming: bool,
) -> Result<DataQualityResults> {
    let aggregate = collect_lazy(
        lf.clone().select(build_profile_exprs(schema)),
        polars_streaming,
    )
    .map_err(Report::from)?;
    let mut columns = parse_profiles(&aggregate, schema, total_rows);
    add_dominance_lazy(lf, &mut columns, polars_streaming)?;
    let identity = profile_identity_lazy(lf, schema, total_rows, polars_streaming)?;
    let category_variants = profile_category_variants_lazy(lf, schema, polars_streaming)?;
    let mut observations = observations_from_profiles(&columns);
    observations.extend(identity_observations(&identity, &category_variants));
    // Only a run that already reads every value pays for the conflicting values, and
    // only that run's access plan promised the read.
    if let Some(source) = source {
        observations.extend(drift_observations(
            source,
            source.conflict_scan.as_ref(),
            polars_streaming,
        ));
    }
    let segments = profile_segments_lazy(lf, total_rows, plan, source, schema, polars_streaming)?;
    let temporal = profile_temporal_lazy(lf, plan, source, polars_streaming)?;
    Ok(DataQualityResults {
        total_rows: Some(total_rows),
        evaluated_rows: total_rows,
        precision: QualityPrecision::Exact,
        sample_seed: plan.sample_seed,
        columns,
        observations,
        segments,
        temporal,
        identity: Some(identity),
        category_variants,
    })
}

/// The most common value of every column, in one pass. A scan per column would
/// re-read the whole source once per column, which on a remote dataset is the
/// difference between one read and sixty — and the access plan promises one.
fn add_dominance_lazy(
    lf: &LazyFrame,
    profiles: &mut [ColumnQualityProfile],
    polars_streaming: bool,
) -> Result<()> {
    if profiles.is_empty() {
        return Ok(());
    }
    const COUNT: &str = "__quality_value_count";
    let exprs = profiles
        .iter()
        .enumerate()
        .map(|(index, profile)| {
            col(&profile.name)
                .drop_nulls()
                .value_counts(true, true, COUNT, false)
                .first()
                .alias(format!("__quality_dominant_{index}"))
        })
        .collect::<Vec<_>>();
    let top = collect_lazy(lf.clone().select(exprs), polars_streaming).map_err(Report::from)?;
    for (index, profile) in profiles.iter_mut().enumerate() {
        let Ok(column) = top.column(&format!("__quality_dominant_{index}")) else {
            continue;
        };
        let Ok(fields) = column.struct_() else {
            continue;
        };
        let Ok(value) = fields.field_by_name(&profile.name) else {
            continue;
        };
        let Ok(counts) = fields.field_by_name(COUNT) else {
            continue;
        };
        profile.dominant_value = value
            .get(0)
            .ok()
            .filter(|value| !value.is_null())
            .map(|value| value.str_value().to_string());
        profile.dominant_count = counts
            .get(0)
            .ok()
            .and_then(|value| value.try_extract::<u64>().ok())
            .map(|count| count as usize);
    }
    Ok(())
}

fn profile_category_variants_lazy(
    lf: &LazyFrame,
    schema: &Schema,
    polars_streaming: bool,
) -> Result<Vec<CategoryVariantGroup>> {
    let normalized_name = "__quality_normalized";
    let original_name = "__quality_original";
    let count_name = "__quality_variant_rows";
    let variant_count_name = "__quality_variant_count";
    let mut result = Vec::new();
    for (name, dtype) in schema.iter() {
        if !matches!(dtype, DataType::String | DataType::Categorical(..)) {
            continue;
        }
        let original = text_expr(col(name.as_str()), dtype);
        let normalized = original
            .clone()
            .str()
            .strip_chars(lit(LiteralValue::untyped_null()))
            .str()
            .to_lowercase();
        let variant_count = col(original_name)
            .n_unique()
            .over([col(normalized_name)])?
            .alias(variant_count_name);
        let query = lf
            .clone()
            .filter(original.clone().is_not_null())
            .select([
                normalized.alias(normalized_name),
                original.alias(original_name),
            ])
            .group_by([col(normalized_name), col(original_name)])
            .agg([len().alias(count_name)])
            .with_columns([variant_count])
            .filter(col(variant_count_name).gt(lit(1u32)))
            .limit(1_001);
        let groups = collect_lazy(query, polars_streaming).map_err(Report::from)?;
        let complete = groups.height() <= 1_000;
        let mut by_normalized = BTreeMap::<String, Vec<(String, usize)>>::new();
        for row in 0..groups.height().min(1_000) {
            let Some(normalized) = string_value_at(&groups, normalized_name, row) else {
                continue;
            };
            let Some(original) = string_value_at(&groups, original_name, row) else {
                continue;
            };
            let count = usize_value_at(&groups, count_name, row);
            by_normalized
                .entry(normalized)
                .or_default()
                .push((original, count));
        }
        for (normalized, variants) in by_normalized {
            if variants.len() < 2 {
                continue;
            }
            let rows_involved = variants.iter().map(|(_, count)| count).sum();
            result.push(CategoryVariantGroup {
                column: name.to_string(),
                normalized,
                variants,
                rows_involved,
                complete,
            });
            if result.len() >= 100 {
                return Ok(result);
            }
        }
    }
    Ok(result)
}

fn profile_identity_lazy(
    lf: &LazyFrame,
    schema: &Schema,
    total_rows: usize,
    polars_streaming: bool,
) -> Result<IdentityProfile> {
    let keys = schema
        .iter_names()
        .map(|name| col(name.as_str()))
        .collect::<Vec<_>>();
    let duplicate_count = "__quality_duplicate_count";
    let grouped = lf
        .clone()
        .group_by(keys)
        .agg([len().alias(duplicate_count)])
        .filter(col(duplicate_count).gt(lit(1u32)))
        .select([
            len().alias("duplicate_groups"),
            (col(duplicate_count) - lit(1u32)).sum().alias("extra_rows"),
            col(duplicate_count).sum().alias("rows_involved"),
        ]);
    let summary = collect_lazy(grouped, polars_streaming).map_err(Report::from)?;
    Ok(IdentityProfile {
        duplicate_groups: usize_value(&summary, "duplicate_groups"),
        extra_rows: usize_value(&summary, "extra_rows"),
        rows_involved: usize_value(&summary, "rows_involved"),
        evaluated_rows: total_rows,
        precision: QualityPrecision::Exact,
    })
}

fn visible_schema(schema: &Schema, source: Option<&QualitySourceContext>) -> Schema {
    let mut visible = Schema::with_capacity(schema.len());
    for (name, dtype) in schema.iter() {
        if source.is_some_and(|context| name.as_str() == context.row_index_column) {
            continue;
        }
        visible.insert(name.clone(), dtype.clone());
    }
    visible
}

fn attach_source_file(
    mut df: DataFrame,
    source: Option<&QualitySourceContext>,
) -> Result<DataFrame> {
    let Some(source) = source else {
        return Ok(df);
    };
    let rows = df.drop_in_place(&source.row_index_column)?;
    let rows = rows.u32()?;
    let names: Vec<Option<&str>> = rows
        .iter()
        .map(|row| {
            let row = row? as usize;
            let file = source
                .file_starts
                .partition_point(|start| *start <= row)
                .saturating_sub(1);
            source.file_names.get(file).map(String::as_str)
        })
        .collect();
    df.with_column(Column::new(QUALITY_SOURCE_FILE_COLUMN.into(), names))?;
    Ok(df)
}

fn profile_columns(
    df: &DataFrame,
    schema: &Schema,
    polars_streaming: bool,
) -> Result<Vec<ColumnQualityProfile>> {
    let aggregate = collect_lazy(
        df.clone().lazy().select(build_profile_exprs(schema)),
        polars_streaming,
    )
    .map_err(Report::from)?;
    Ok(parse_profiles(&aggregate, schema, df.height()))
}

/// Fills in the one measurement the shared expression set does not produce: the
/// most common value and its count.
fn add_value_details(df: &DataFrame, profiles: &mut [ColumnQualityProfile]) -> Result<()> {
    for profile in profiles {
        let values = df.column(&profile.name)?;
        let mut counts = BTreeMap::<String, usize>::new();
        for row in 0..df.height() {
            let value = values.get(row)?;
            if !value.is_null() {
                *counts.entry(value.str_value().to_string()).or_default() += 1;
            }
        }
        if let Some((value, count)) = counts
            .into_iter()
            .max_by(|left, right| left.1.cmp(&right.1).then_with(|| right.0.cmp(&left.0)))
        {
            profile.dominant_value = Some(value);
            profile.dominant_count = Some(count);
        }
    }
    Ok(())
}

fn profile_identity(
    df: &DataFrame,
    schema: &Schema,
    precision: QualityPrecision,
) -> Result<IdentityProfile> {
    let columns = schema
        .iter_names()
        .map(|name| df.column(name))
        .collect::<PolarsResult<Vec<_>>>()?;
    let mut groups = BTreeMap::<Vec<Option<String>>, usize>::new();
    for row in 0..df.height() {
        let mut key = Vec::with_capacity(columns.len());
        for column in &columns {
            let value = column.get(row)?;
            key.push((!value.is_null()).then(|| value.str_value().to_string()));
        }
        *groups.entry(key).or_default() += 1;
    }
    let duplicates = groups.into_values().filter(|count| *count > 1);
    let mut duplicate_groups = 0;
    let mut extra_rows = 0;
    let mut rows_involved = 0;
    for count in duplicates {
        duplicate_groups += 1;
        extra_rows += count - 1;
        rows_involved += count;
    }
    Ok(IdentityProfile {
        duplicate_groups,
        extra_rows,
        rows_involved,
        evaluated_rows: df.height(),
        precision,
    })
}

fn profile_category_variants(df: &DataFrame, schema: &Schema) -> Result<Vec<CategoryVariantGroup>> {
    let mut result = Vec::new();
    for (name, dtype) in schema.iter() {
        if !matches!(dtype, DataType::String | DataType::Categorical(..)) {
            continue;
        }
        let column = df.column(name)?;
        let mut normalized = BTreeMap::<String, BTreeMap<String, usize>>::new();
        for row in 0..df.height() {
            let value = column.get(row)?;
            if value.is_null() {
                continue;
            }
            let original = value.str_value().to_string();
            *normalized
                .entry(original.trim().to_lowercase())
                .or_default()
                .entry(original)
                .or_default() += 1;
        }
        for (normalized, variants) in normalized {
            if variants.len() <= 1 {
                continue;
            }
            let variants = variants.into_iter().collect::<Vec<_>>();
            let rows_involved = variants.iter().map(|(_, count)| count).sum();
            result.push(CategoryVariantGroup {
                column: name.to_string(),
                normalized,
                variants,
                rows_involved,
                complete: true,
            });
            if result.len() >= 100 {
                return Ok(result);
            }
        }
    }
    Ok(result)
}

fn identity_observations(
    identity: &IdentityProfile,
    variants: &[CategoryVariantGroup],
) -> Vec<QualityObservation> {
    let mut observations = Vec::new();
    if identity.duplicate_groups > 0 {
        observations.push(QualityObservation {
            kind: ObservationKind::DuplicateRows,
            column: "all columns".to_string(),
            affected_rows: identity.rows_involved,
            evaluated_rows: identity.evaluated_rows,
            fact: format!(
                "{} groups; {} extra rows ({})",
                identity.duplicate_groups,
                identity.extra_rows,
                identity.precision.label()
            ),
            normalized_category: None,
            files: Vec::new(),
        });
    }
    observations.extend(variants.iter().map(|group| QualityObservation {
        kind: ObservationKind::CategoryVariants,
        column: group.column.clone(),
        affected_rows: group.rows_involved,
        evaluated_rows: identity.evaluated_rows,
        fact: format!(
            "{}{} variants normalize to {:?}",
            if group.complete { "" } else { "at least " },
            group.variants.len(),
            group.normalized
        ),
        normalized_category: Some(group.normalized.clone()),
        files: Vec::new(),
    }));
    observations
}

#[derive(Debug)]
struct SegmentRows {
    label: String,
    indices: Vec<u32>,
}

fn segment_rows(
    df: &DataFrame,
    plan: &DataQualityPlan,
    sample_positions: Option<&[u32]>,
) -> Result<Vec<SegmentRows>> {
    let all_rows = || SegmentRows {
        label: "current view".to_string(),
        indices: (0..df.height() as u32).collect(),
    };
    let groups = match &plan.grain {
        QualityGrain::Dataset => vec![all_rows()],
        QualityGrain::RowChunks(size) => {
            let size = (*size).max(1);
            let mut chunks = BTreeMap::<usize, Vec<u32>>::new();
            for row in 0..df.height() {
                let position = sample_positions
                    .and_then(|positions| positions.get(row))
                    .copied()
                    .unwrap_or(row as u32) as usize;
                chunks.entry(position / size).or_default().push(row as u32);
            }
            chunks
                .into_iter()
                .map(|(chunk, indices)| SegmentRows {
                    label: format!(
                        "rows {}-{}",
                        chunk.saturating_mul(size) + 1,
                        (chunk + 1).saturating_mul(size)
                    ),
                    indices,
                })
                .collect()
        }
        QualityGrain::Partition(column) => group_by_value(df, column, "partition")?,
        QualityGrain::TimeWindows { column, every } => group_by_time_window(df, column, every)?,
        QualityGrain::File => {
            if df.column(QUALITY_SOURCE_FILE_COLUMN).is_ok() {
                group_by_value(df, QUALITY_SOURCE_FILE_COLUMN, "file")?
            } else {
                vec![SegmentRows {
                    label: "file mapping unavailable for this view".to_string(),
                    indices: (0..df.height() as u32).collect(),
                }]
            }
        }
    };
    Ok(groups)
}

fn group_by_value(df: &DataFrame, column: &str, kind: &str) -> Result<Vec<SegmentRows>> {
    let values = df.column(column)?;
    let mut groups: BTreeMap<String, Vec<u32>> = BTreeMap::new();
    let mut missing = Vec::new();
    for row in 0..df.height() {
        let value = values.get(row)?;
        if value.is_null() {
            missing.push(row as u32);
        } else {
            groups
                .entry(format!("{kind} {}", value.str_value()))
                .or_default()
                .push(row as u32);
        }
    }
    let mut result: Vec<SegmentRows> = groups
        .into_iter()
        .map(|(label, indices)| SegmentRows { label, indices })
        .collect();
    // Rows the grain could not place carry no order, so they follow the ones it
    // could — the same rule the scanned path applies. Sorting "∅" by codepoint
    // would put it before any value that outranks U+2205.
    if !missing.is_empty() {
        result.push(SegmentRows {
            label: format!("{kind} ∅"),
            indices: missing,
        });
    }
    Ok(result)
}

/// Where a row's window starts. Both the sampled and the full-scan path bucket
/// through this one expression, so a week never starts on a different day
/// depending on how much of it was read.
fn time_window_start(column: &str, every: &str) -> Expr {
    col(column)
        .cast(DataType::Datetime(TimeUnit::Microseconds, None))
        .dt()
        .truncate(lit(every.to_string()))
}

fn group_by_time_window(df: &DataFrame, column: &str, every: &str) -> Result<Vec<SegmentRows>> {
    let starts = df
        .clone()
        .lazy()
        .select([time_window_start(column, every).alias(QUALITY_WINDOW_START)])
        .collect()?;
    let starts = starts.column(QUALITY_WINDOW_START)?;
    let mut groups: BTreeMap<String, Vec<u32>> = BTreeMap::new();
    let mut missing = Vec::new();
    for row in 0..df.height() {
        let value = starts.get(row)?;
        if value.is_null() {
            missing.push(row as u32);
        } else {
            groups
                .entry(value.str_value().into_owned())
                .or_default()
                .push(row as u32);
        }
    }
    let mut result: Vec<SegmentRows> = groups
        .into_iter()
        .map(|(start, indices)| SegmentRows {
            label: time_window_label(column, every, Some(&start)),
            indices,
        })
        .collect();
    if !missing.is_empty() {
        result.push(SegmentRows {
            label: time_window_label(column, every, None),
            indices: missing,
        });
    }
    Ok(result)
}

fn time_window_label(column: &str, every: &str, start: Option<&str>) -> String {
    match start {
        Some(start) => format!("{start} / {every}"),
        None => format!("{column} ∅"),
    }
}

fn value_epoch_micros(value: AnyValue<'_>) -> Option<i64> {
    match value {
        AnyValue::Date(days) => Some(i64::from(days) * 86_400_000_000),
        AnyValue::Datetime(value, TimeUnit::Nanoseconds, _) => Some(value / 1_000),
        AnyValue::Datetime(value, TimeUnit::Microseconds, _) => Some(value),
        AnyValue::Datetime(value, TimeUnit::Milliseconds, _) => Some(value * 1_000),
        _ => None,
    }
}

fn take_rows(df: &DataFrame, indices: &[u32]) -> PolarsResult<DataFrame> {
    df.take(&UInt32Chunked::new("quality_rows".into(), indices.to_vec()))
}

struct SegmentSampleProvenance<'a> {
    positions: Option<&'a [u32]>,
    totals: Option<&'a BTreeMap<String, usize>>,
}

fn profile_segments(
    df: &DataFrame,
    total_rows: Option<usize>,
    plan: &DataQualityPlan,
    precision: QualityPrecision,
    schema: &Schema,
    sample: SegmentSampleProvenance<'_>,
    polars_streaming: bool,
) -> Result<Vec<SegmentQualityProfile>> {
    let groups = segment_rows(df, plan, sample.positions)?;
    let mut profiles = Vec::with_capacity(groups.len());
    for group in groups {
        let segment = take_rows(df, &group.indices)?;
        let columns = profile_columns(&segment, schema, polars_streaming)?;
        let null_cells = columns
            .iter()
            .map(|column| column.null_count)
            .sum::<usize>();
        let denominator = segment.height().saturating_mul(columns.len());
        let known_segment_rows = sample.totals.and_then(|totals| totals.get(&group.label));
        profiles.push(SegmentQualityProfile {
            label: group.label,
            total_rows: if let Some(total) = known_segment_rows {
                Some(*total)
            } else if matches!(plan.grain, QualityGrain::Dataset) {
                total_rows
            } else if precision == QualityPrecision::Exact {
                Some(segment.height())
            } else {
                None
            },
            evaluated_rows: segment.height(),
            columns,
            null_cells,
            null_rate: rate(null_cells, denominator),
            compared_with: None,
            largest_change: None,
        });
    }
    apply_comparisons(
        &mut profiles,
        &plan.grain,
        plan.comparison,
        plan.baseline_segment.as_deref(),
        precision,
    );
    Ok(profiles)
}

fn profile_segments_lazy(
    lf: &LazyFrame,
    total_rows: usize,
    plan: &DataQualityPlan,
    source: Option<&QualitySourceContext>,
    schema: &Schema,
    polars_streaming: bool,
) -> Result<Vec<SegmentQualityProfile>> {
    if matches!(plan.grain, QualityGrain::Dataset)
        || matches!(plan.grain, QualityGrain::File) && source.is_none()
    {
        let aggregate = collect_lazy(
            lf.clone().select(build_profile_exprs(schema)),
            polars_streaming,
        )
        .map_err(Report::from)?;
        let columns = parse_profiles(&aggregate, schema, total_rows);
        let null_cells = columns
            .iter()
            .map(|column| column.null_count)
            .sum::<usize>();
        let denominator = total_rows.saturating_mul(schema.len());
        return Ok(vec![SegmentQualityProfile {
            label: if matches!(plan.grain, QualityGrain::File) {
                "file mapping unavailable for this view".to_string()
            } else {
                "current view".to_string()
            },
            total_rows: Some(total_rows),
            evaluated_rows: total_rows,
            columns,
            null_cells,
            null_rate: rate(null_cells, denominator),
            compared_with: None,
            largest_change: None,
        }]);
    }

    let (grouped_lf, group) = grouped_frame(lf, &plan.grain, source)?;
    let mut aggregates = vec![len().alias("__quality_segment_rows")];
    aggregates.extend(build_profile_exprs(schema));
    let grouped = collect_lazy(
        grouped_lf
            .group_by([group.alias("__quality_segment")])
            .agg(aggregates),
        polars_streaming,
    )
    .map_err(Report::from)?;
    let mut segments = Vec::with_capacity(grouped.height());
    let mut unassigned = Vec::with_capacity(grouped.height());
    for row in 0..grouped.height() {
        let evaluated_rows = usize_value_at(&grouped, "__quality_segment_rows", row);
        let columns = parse_profiles_at(&grouped, schema, evaluated_rows, row);
        let null_cells = columns
            .iter()
            .map(|column| column.null_count)
            .sum::<usize>();
        let denominator = evaluated_rows.saturating_mul(schema.len());
        let raw_label = string_value_at(&grouped, "__quality_segment", row);
        unassigned.push(raw_label.is_none());
        segments.push(SegmentQualityProfile {
            label: segment_label(&plan.grain, raw_label.as_deref()),
            total_rows: Some(evaluated_rows),
            evaluated_rows,
            columns,
            null_cells,
            null_rate: rate(null_cells, denominator),
            compared_with: None,
            largest_change: None,
        });
    }
    // Rows the grain could not place carry no order, so they follow the ones it could.
    let mut ordered = unassigned.into_iter().zip(segments).collect::<Vec<_>>();
    ordered.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| left.1.label.cmp(&right.1.label))
    });
    let mut segments = ordered
        .into_iter()
        .map(|(_, segment)| segment)
        .collect::<Vec<_>>();
    if matches!(plan.grain, QualityGrain::RowChunks(_)) {
        for segment in &mut segments {
            segment.label = pretty_chunk_label(&segment.label);
        }
    }
    apply_comparisons(
        &mut segments,
        &plan.grain,
        plan.comparison,
        plan.baseline_segment.as_deref(),
        QualityPrecision::Exact,
    );
    Ok(segments)
}

fn grouped_frame(
    lf: &LazyFrame,
    grain: &QualityGrain,
    source: Option<&QualitySourceContext>,
) -> Result<(LazyFrame, Expr)> {
    match grain {
        QualityGrain::Dataset => Err(color_eyre::eyre::eyre!(
            "dataset grain does not need grouping"
        )),
        QualityGrain::Partition(column) => Ok((lf.clone(), col(column))),
        QualityGrain::RowChunks(size) => {
            let row = "__datui_quality_row";
            Ok((
                lf.clone().with_row_index(row, None),
                col(row).cast(DataType::UInt64) / lit((*size).max(1) as u64),
            ))
        }
        QualityGrain::TimeWindows { column, every } => {
            Ok((lf.clone(), time_window_start(column, every)))
        }
        QualityGrain::File => {
            let source = source
                .ok_or_else(|| color_eyre::eyre::eyre!("source-file mapping is unavailable"))?;
            let mut file = lit("unknown");
            for (start, name) in source.file_starts.iter().zip(source.file_names.iter()) {
                file = when(col(&source.row_index_column).gt_eq(lit(*start as u32)))
                    .then(lit(name.clone()))
                    .otherwise(file);
            }
            Ok((lf.clone(), file))
        }
    }
}

fn pretty_chunk_label(label: &str) -> String {
    let Some(range) = label.strip_prefix("rows ") else {
        return label.to_string();
    };
    let Some((start, end)) = range.split_once('-') else {
        return label.to_string();
    };
    let start = start.trim_start_matches('0');
    let end = end.trim_start_matches('0');
    format!(
        "rows {}-{}",
        if start.is_empty() { "0" } else { start },
        if end.is_empty() { "0" } else { end }
    )
}

fn segment_label(grain: &QualityGrain, raw: Option<&str>) -> String {
    match grain {
        QualityGrain::RowChunks(size) => {
            let raw = raw.unwrap_or("∅");
            raw.parse::<usize>()
                .map(|chunk| {
                    let start = chunk.saturating_mul(*size) + 1;
                    let end = start.saturating_add(*size).saturating_sub(1);
                    format!("rows {start:012}-{end:012}")
                })
                .unwrap_or_else(|_| format!("rows {raw}"))
        }
        QualityGrain::Partition(_) => format!("partition {}", raw.unwrap_or("∅")),
        QualityGrain::TimeWindows { column, every } => time_window_label(column, every, raw),
        QualityGrain::File => format!("file {}", raw.unwrap_or("∅")),
        QualityGrain::Dataset => "current view".to_string(),
    }
}

fn apply_comparisons(
    segments: &mut [SegmentQualityProfile],
    grain: &QualityGrain,
    comparison: QualityComparison,
    baseline_segment: Option<&str>,
    precision: QualityPrecision,
) {
    for segment in segments.iter_mut() {
        segment.compared_with = None;
        segment.largest_change = None;
    }
    if comparison == QualityComparison::Previous
        && matches!(grain, QualityGrain::File | QualityGrain::Partition(_))
    {
        for segment in segments {
            segment.largest_change = Some("previous unavailable: choose an order".to_string());
        }
        return;
    }
    let baseline_index = baseline_segment
        .and_then(|label| segments.iter().position(|segment| segment.label == label))
        .or_else(|| baseline_segment.is_none().then_some(0));
    if comparison == QualityComparison::Baseline && baseline_index.is_none() {
        for segment in segments {
            segment.largest_change = Some("selected baseline unavailable".to_string());
        }
        return;
    }
    for index in 0..segments.len() {
        let compared = match comparison {
            QualityComparison::None => None,
            QualityComparison::Previous if index > 0 => Some(index - 1),
            QualityComparison::Baseline if Some(index) != baseline_index => baseline_index,
            QualityComparison::Previous | QualityComparison::Baseline => None,
        };
        if let Some(other) = compared {
            let change = largest_material_change(&segments[index], &segments[other], precision);
            segments[index].compared_with = Some(segments[other].label.clone());
            segments[index].largest_change = Some(change);
        }
    }
}

/// How far a measurement has to move between segments before it is worth naming, in
/// percentage points.
const MATERIAL_CHANGE_PP: f64 = 1.0;

/// The largest measured move between two segments, over every column.
///
/// #196 asks where a column's null rate, distinct count or range shifts sharply, which
/// is a question about the sharpest single move rather than about the average of all
/// of them: one column going from never-null to always-null is the finding, and a mean
/// over sixty columns buries it. A range that moved is reported when no rate did,
/// because a column whose values slid into a new interval shifted without any rate
/// noticing.
fn largest_material_change(
    segment: &SegmentQualityProfile,
    baseline: &SegmentQualityProfile,
    precision: QualityPrecision,
) -> String {
    let mut largest: Option<(f64, String)> = None;
    let mut range: Option<String> = None;
    for column in &segment.columns {
        let Some(prior) = baseline
            .columns
            .iter()
            .find(|other| other.name == column.name)
        else {
            continue;
        };
        for metric in [QualityMetric::NullRate, QualityMetric::DistinctShare] {
            let (Some(now), Some(before)) = (metric.value(column), metric.value(prior)) else {
                continue;
            };
            let change = (now - before) * 100.0;
            if largest
                .as_ref()
                .is_none_or(|(most, _)| change.abs() > most.abs())
            {
                largest = Some((
                    change,
                    format!("{} {}", column.name, metric.label().to_lowercase()),
                ));
            }
        }
        if range.is_none() && (column.min != prior.min || column.max != prior.max) {
            range = Some(format!(
                "{} range {} -> {}",
                column.name,
                range_label(prior),
                range_label(column)
            ));
        }
    }
    let precision = precision.label();
    match largest {
        Some((change, what)) if change.abs() >= MATERIAL_CHANGE_PP => {
            format!("{what} {change:+.2} pp ({precision})")
        }
        _ => match range {
            Some(moved) => format!("{moved} ({precision})"),
            None => format!("nothing moved {MATERIAL_CHANGE_PP:.0} pp ({precision})"),
        },
    }
}

fn range_label(column: &ColumnQualityProfile) -> String {
    match (&column.min, &column.max) {
        (Some(min), Some(max)) => format!("{min}..{max}"),
        (Some(min), None) => format!("{min}.."),
        (None, Some(max)) => format!("..{max}"),
        (None, None) => "none".to_string(),
    }
}

fn profile_temporal(
    df: &DataFrame,
    plan: &DataQualityPlan,
    sample_positions: Option<&[u32]>,
) -> Result<Vec<TemporalLatencyProfile>> {
    let role_column = |role| {
        plan.temporal_roles
            .iter()
            .find(|assignment| assignment.role == role)
            .map(|assignment| assignment.column.as_str())
            .filter(|column| df.column(column).is_ok())
    };
    let pairs = [
        (TemporalRole::Event, TemporalRole::Published),
        (TemporalRole::Event, TemporalRole::Received),
        (TemporalRole::PeriodEnd, TemporalRole::Published),
        (TemporalRole::Published, TemporalRole::Received),
        (TemporalRole::Received, TemporalRole::Processed),
        (TemporalRole::Event, TemporalRole::Processed),
    ];
    // Resolved before the rows are grouped, as the lazy path does: the default plan
    // assigns no roles at all, and splitting the sample into ten thousand segments to
    // discover that costs a DataFrame copy per segment and answers nothing.
    let resolved = pairs
        .into_iter()
        .filter_map(|(start_role, end_role)| {
            Some((
                start_role,
                end_role,
                role_column(start_role)?,
                role_column(end_role)?,
            ))
        })
        .collect::<Vec<_>>();
    if resolved.is_empty() {
        return Ok(Vec::new());
    }
    let groups = segment_rows(df, plan, sample_positions)?;
    let mut profiles = Vec::new();
    for group in groups {
        let segment = take_rows(df, &group.indices)?;
        for (start_role, end_role, start_column, end_column) in &resolved {
            profiles.push(latency_profile(
                &segment,
                &group.label,
                *start_role,
                *end_role,
                start_column,
                end_column,
                plan.latency_threshold_seconds,
            )?);
        }
    }
    Ok(profiles)
}

fn profile_temporal_lazy(
    lf: &LazyFrame,
    plan: &DataQualityPlan,
    source: Option<&QualitySourceContext>,
    polars_streaming: bool,
) -> Result<Vec<TemporalLatencyProfile>> {
    let schema = lf.clone().collect_schema()?;
    let role_column = |role| {
        plan.temporal_roles
            .iter()
            .find(|assignment| assignment.role == role)
            .map(|assignment| assignment.column.as_str())
            .filter(|column| schema.get(column).is_some())
    };
    let supported = [
        (TemporalRole::Event, TemporalRole::Published),
        (TemporalRole::Event, TemporalRole::Received),
        (TemporalRole::PeriodEnd, TemporalRole::Published),
        (TemporalRole::Published, TemporalRole::Received),
        (TemporalRole::Received, TemporalRole::Processed),
        (TemporalRole::Event, TemporalRole::Processed),
    ];
    let pairs = supported
        .into_iter()
        .filter_map(|(start_role, end_role)| {
            Some((
                start_role,
                end_role,
                role_column(start_role)?.to_string(),
                role_column(end_role)?.to_string(),
            ))
        })
        .collect::<Vec<_>>();
    if pairs.is_empty() {
        return Ok(Vec::new());
    }

    let mut expressions = vec![len().alias("__quality_temporal_rows")];
    for (index, (_, _, start_column, end_column)) in pairs.iter().enumerate() {
        let prefix = format!("latency::{index}::");
        let start = col(start_column);
        let end = col(end_column);
        let duration = (end
            .clone()
            .cast(DataType::Datetime(TimeUnit::Microseconds, None))
            - start
                .clone()
                .cast(DataType::Datetime(TimeUnit::Microseconds, None)))
        .dt()
        .total_seconds(false);
        expressions.extend([
            start
                .is_null()
                .sum()
                .alias(format!("{prefix}missing_start")),
            end.is_null().sum().alias(format!("{prefix}missing_end")),
            duration
                .clone()
                .lt(lit(0i64))
                .sum()
                .alias(format!("{prefix}negative")),
            duration
                .clone()
                .quantile(lit(0.50), QuantileMethod::Nearest)
                .alias(format!("{prefix}p50")),
            duration
                .clone()
                .quantile(lit(0.90), QuantileMethod::Nearest)
                .alias(format!("{prefix}p90")),
            duration
                .clone()
                .quantile(lit(0.95), QuantileMethod::Nearest)
                .alias(format!("{prefix}p95")),
            duration
                .clone()
                .quantile(lit(0.99), QuantileMethod::Nearest)
                .alias(format!("{prefix}p99")),
            duration.clone().max().alias(format!("{prefix}max")),
        ]);
        if let Some(threshold) = plan.latency_threshold_seconds {
            expressions.push(
                duration
                    .gt(lit(threshold))
                    .sum()
                    .alias(format!("{prefix}above")),
            );
        }
    }

    let ungrouped = matches!(plan.grain, QualityGrain::Dataset)
        || matches!(plan.grain, QualityGrain::File) && source.is_none();
    let aggregate = if ungrouped {
        collect_lazy(lf.clone().select(expressions), polars_streaming).map_err(Report::from)?
    } else {
        let (grouped_lf, group) = grouped_frame(lf, &plan.grain, source)?;
        collect_lazy(
            grouped_lf
                .group_by([group.alias("__quality_segment")])
                .agg(expressions),
            polars_streaming,
        )
        .map_err(Report::from)?
    };

    let mut profiles = Vec::new();
    for row in 0..aggregate.height() {
        let segment = if ungrouped {
            if matches!(plan.grain, QualityGrain::File) {
                "file mapping unavailable for this view".to_string()
            } else {
                "current view".to_string()
            }
        } else {
            let raw = string_value_at(&aggregate, "__quality_segment", row);
            segment_label(&plan.grain, raw.as_deref())
        };
        let evaluated_rows = usize_value_at(&aggregate, "__quality_temporal_rows", row);
        for (index, (start_role, end_role, start_column, end_column)) in pairs.iter().enumerate() {
            let prefix = format!("latency::{index}::");
            profiles.push(TemporalLatencyProfile {
                segment: segment.clone(),
                start_role: *start_role,
                end_role: *end_role,
                start_column: start_column.clone(),
                end_column: end_column.clone(),
                evaluated_rows,
                missing_start: usize_value_at(&aggregate, &format!("{prefix}missing_start"), row),
                missing_end: usize_value_at(&aggregate, &format!("{prefix}missing_end"), row),
                negative_count: usize_value_at(&aggregate, &format!("{prefix}negative"), row),
                p50_seconds: optional_i64_at(&aggregate, &format!("{prefix}p50"), row),
                p90_seconds: optional_i64_at(&aggregate, &format!("{prefix}p90"), row),
                p95_seconds: optional_i64_at(&aggregate, &format!("{prefix}p95"), row),
                p99_seconds: optional_i64_at(&aggregate, &format!("{prefix}p99"), row),
                max_seconds: optional_i64_at(&aggregate, &format!("{prefix}max"), row),
                above_threshold_count: plan
                    .latency_threshold_seconds
                    .map(|_| usize_value_at(&aggregate, &format!("{prefix}above"), row)),
            });
        }
    }
    profiles.sort_by(|left, right| left.segment.cmp(&right.segment));
    // The zero padding exists so a lexicographic sort orders chunks numerically, and
    // comes off once it has. Segments does the same thing in the same place; leaving
    // it on here had Trends and Segments name one chunk two different ways.
    if matches!(plan.grain, QualityGrain::RowChunks(_)) {
        for profile in &mut profiles {
            profile.segment = pretty_chunk_label(&profile.segment);
        }
    }
    Ok(profiles)
}

fn latency_profile(
    df: &DataFrame,
    segment: &str,
    start_role: TemporalRole,
    end_role: TemporalRole,
    start_column: &str,
    end_column: &str,
    threshold_seconds: Option<i64>,
) -> Result<TemporalLatencyProfile> {
    let starts = df.column(start_column)?;
    let ends = df.column(end_column)?;
    let mut missing_start = 0;
    let mut missing_end = 0;
    let mut seconds = Vec::new();
    for row in 0..df.height() {
        let start = value_epoch_micros(starts.get(row)?);
        let end = value_epoch_micros(ends.get(row)?);
        if start.is_none() {
            missing_start += 1;
        }
        if end.is_none() {
            missing_end += 1;
        }
        if let (Some(start), Some(end)) = (start, end) {
            seconds.push((end - start) / 1_000_000);
        }
    }
    seconds.sort_unstable();
    let percentile = |percent: usize| {
        if seconds.is_empty() {
            None
        } else {
            let index = ((seconds.len() - 1) * percent + 50) / 100;
            seconds.get(index).copied()
        }
    };
    Ok(TemporalLatencyProfile {
        segment: segment.to_string(),
        start_role,
        end_role,
        start_column: start_column.to_string(),
        end_column: end_column.to_string(),
        evaluated_rows: df.height(),
        missing_start,
        missing_end,
        negative_count: seconds.iter().filter(|value| **value < 0).count(),
        p50_seconds: percentile(50),
        p90_seconds: percentile(90),
        p95_seconds: percentile(95),
        p99_seconds: percentile(99),
        max_seconds: seconds.last().copied(),
        above_threshold_count: threshold_seconds
            .map(|threshold| seconds.iter().filter(|value| **value > threshold).count()),
    })
}

/// A categorical column stores integer codes, not text: `.str()` rejects it and
/// a numeric cast would measure the codes. Read its values as strings instead.
fn text_expr(column: Expr, dtype: &DataType) -> Expr {
    if matches!(dtype, DataType::Categorical(..)) {
        column.cast(DataType::String)
    } else {
        column
    }
}

fn build_profile_exprs(schema: &Schema) -> Vec<Expr> {
    let mut exprs = Vec::new();
    for (name, dtype) in schema.iter() {
        let column = col(name.as_str());
        let prefix = format!("{}::", name);
        exprs.push(column.clone().null_count().alias(format!("{prefix}null")));
        exprs.push(
            column
                .clone()
                .filter(column.clone().is_not_null())
                .n_unique()
                .alias(format!("{prefix}distinct")),
        );

        if supports_range(dtype) {
            exprs.push(column.clone().min().alias(format!("{prefix}min")));
            exprs.push(column.clone().max().alias(format!("{prefix}max")));
        }

        if matches!(dtype, DataType::String | DataType::Categorical(..)) {
            let text = text_expr(column.clone(), dtype);
            let trimmed = text
                .clone()
                .str()
                .strip_chars(lit(LiteralValue::untyped_null()));
            exprs.push(
                text.clone()
                    .eq(lit(""))
                    .sum()
                    .alias(format!("{prefix}empty")),
            );
            exprs.push(
                trimmed
                    .eq(lit(""))
                    .and(text.clone().neq(lit("")))
                    .sum()
                    .alias(format!("{prefix}whitespace")),
            );
            exprs.push(
                text.clone()
                    .cast(DataType::Int64)
                    .is_not_null()
                    .and(text.clone().is_not_null())
                    .sum()
                    .alias(format!("{prefix}parse_int")),
            );
            exprs.push(
                text.clone()
                    .cast(DataType::Float64)
                    .is_not_null()
                    .and(text.clone().is_not_null())
                    .sum()
                    .alias(format!("{prefix}parse_decimal")),
            );
            // Named formats, not inference: "parses as an ISO date" has to mean
            // the same thing on every column, including one where nothing does.
            let strptime = |format: &str| StrptimeOptions {
                format: Some(PlSmallStr::from(format)),
                strict: false,
                exact: true,
                cache: true,
            };
            let as_datetime = |format: &str| {
                text.clone().str().to_datetime(
                    Some(TimeUnit::Microseconds),
                    None,
                    strptime(format),
                    lit(PlSmallStr::from_static("raise")),
                )
            };
            exprs.push(
                text.clone()
                    .str()
                    .to_date(strptime("%Y-%m-%d"))
                    .is_not_null()
                    .and(text.clone().is_not_null())
                    .sum()
                    .alias(format!("{prefix}parse_date")),
            );
            let datetime_formats = [
                "%Y-%m-%d %H:%M:%S%.f",
                "%Y-%m-%dT%H:%M:%S%.f%#z",
                "%Y-%m-%dT%H:%M:%S%.f",
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%dT%H:%M:%S%#z",
                "%Y-%m-%dT%H:%M:%S",
            ];
            let parses_as_datetime = datetime_formats
                .into_iter()
                .map(|format| as_datetime(format).is_not_null())
                .reduce(Expr::or)
                .expect("at least one datetime format");
            exprs.push(
                parses_as_datetime
                    .and(text.clone().is_not_null())
                    .sum()
                    .alias(format!("{prefix}parse_datetime")),
            );
            exprs.push(
                text.clone()
                    .str()
                    .len_chars()
                    .min()
                    .alias(format!("{prefix}min_length")),
            );
            exprs.push(
                text.str()
                    .len_chars()
                    .max()
                    .alias(format!("{prefix}max_length")),
            );
        }

        if matches!(dtype, DataType::List(_)) {
            exprs.push(
                column
                    .clone()
                    .list()
                    .len()
                    .min()
                    .alias(format!("{prefix}min_length")),
            );
            exprs.push(
                column
                    .clone()
                    .list()
                    .len()
                    .max()
                    .alias(format!("{prefix}max_length")),
            );
        }

        if dtype.is_float() {
            let float = column.cast(DataType::Float64);
            exprs.push(float.clone().is_nan().sum().alias(format!("{prefix}nan")));
            exprs.push(
                float
                    .clone()
                    .eq(lit(f64::INFINITY))
                    .sum()
                    .alias(format!("{prefix}pos_inf")),
            );
            exprs.push(
                float
                    .eq(lit(f64::NEG_INFINITY))
                    .sum()
                    .alias(format!("{prefix}neg_inf")),
            );
        }
    }
    exprs
}

fn supports_range(dtype: &DataType) -> bool {
    dtype.is_numeric()
        || dtype.is_temporal()
        || matches!(
            dtype,
            DataType::String | DataType::Categorical(..) | DataType::Boolean
        )
}

fn parse_profiles(
    aggregate: &DataFrame,
    schema: &Schema,
    evaluated_rows: usize,
) -> Vec<ColumnQualityProfile> {
    parse_profiles_at(aggregate, schema, evaluated_rows, 0)
}

fn parse_profiles_at(
    aggregate: &DataFrame,
    schema: &Schema,
    evaluated_rows: usize,
    row: usize,
) -> Vec<ColumnQualityProfile> {
    schema
        .iter()
        .map(|(name, dtype)| {
            let prefix = format!("{}::", name);
            ColumnQualityProfile {
                name: name.to_string(),
                dtype: dtype.clone(),
                evaluated_rows,
                null_count: usize_value_at(aggregate, &format!("{prefix}null"), row),
                empty_count: optional_usize_at(aggregate, &format!("{prefix}empty"), row),
                whitespace_count: optional_usize_at(aggregate, &format!("{prefix}whitespace"), row),
                nan_count: optional_usize_at(aggregate, &format!("{prefix}nan"), row),
                positive_infinity_count: optional_usize_at(
                    aggregate,
                    &format!("{prefix}pos_inf"),
                    row,
                ),
                negative_infinity_count: optional_usize_at(
                    aggregate,
                    &format!("{prefix}neg_inf"),
                    row,
                ),
                distinct_count: optional_usize_at(aggregate, &format!("{prefix}distinct"), row),
                min: string_value_at(aggregate, &format!("{prefix}min"), row),
                max: string_value_at(aggregate, &format!("{prefix}max"), row),
                integer_parse_count: optional_usize_at(
                    aggregate,
                    &format!("{prefix}parse_int"),
                    row,
                ),
                decimal_parse_count: optional_usize_at(
                    aggregate,
                    &format!("{prefix}parse_decimal"),
                    row,
                ),
                date_parse_count: optional_usize_at(aggregate, &format!("{prefix}parse_date"), row),
                datetime_parse_count: optional_usize_at(
                    aggregate,
                    &format!("{prefix}parse_datetime"),
                    row,
                ),
                dominant_value: None,
                dominant_count: None,
                min_length: optional_usize_at(aggregate, &format!("{prefix}min_length"), row),
                max_length: optional_usize_at(aggregate, &format!("{prefix}max_length"), row),
            }
        })
        .collect()
}

fn observations_from_profiles(columns: &[ColumnQualityProfile]) -> Vec<QualityObservation> {
    let mut observations = Vec::new();
    for profile in columns {
        if profile.null_count > 0 {
            observations.push(observation(
                ObservationKind::Nulls,
                profile,
                profile.null_count,
                format!("{:.2}% null", profile.null_rate() * 100.0),
            ));
        }
        if let Some(count) = profile.empty_count.filter(|count| *count > 0) {
            observations.push(observation(
                ObservationKind::Empty,
                profile,
                count,
                format!("{:.2}% empty", rate(count, profile.evaluated_rows) * 100.0),
            ));
        }
        if let Some(count) = profile.whitespace_count.filter(|count| *count > 0) {
            observations.push(observation(
                ObservationKind::Whitespace,
                profile,
                count,
                format!(
                    "{:.2}% whitespace only",
                    rate(count, profile.evaluated_rows) * 100.0
                ),
            ));
        }
        let non_finite = profile.nan_count.unwrap_or(0)
            + profile.positive_infinity_count.unwrap_or(0)
            + profile.negative_infinity_count.unwrap_or(0);
        if non_finite > 0 {
            observations.push(observation(
                ObservationKind::NonFinite,
                profile,
                non_finite,
                format!("{non_finite} NaN or infinite"),
            ));
        }
        if profile.distinct_count == Some(1) && profile.non_null_rows() > 0 {
            observations.push(observation(
                ObservationKind::Constant,
                profile,
                profile.non_null_rows(),
                "one non-null value".to_string(),
            ));
        }
        if let Some(parsed) = profile.decimal_parse_count
            && profile.non_null_rows() > 0
            && parsed > 0
        {
            observations.push(observation(
                ObservationKind::ParseableText,
                profile,
                parsed,
                format!(
                    "{:.2}% parse as decimal",
                    rate(parsed, profile.non_null_rows()) * 100.0
                ),
            ));
        }
        if let Some(parsed) = profile.date_parse_count.filter(|count| *count > 0) {
            observations.push(observation(
                ObservationKind::ParseableText,
                profile,
                parsed,
                format!(
                    "{:.2}% parse as ISO date",
                    rate(parsed, profile.non_null_rows()) * 100.0
                ),
            ));
        }
        if let Some(parsed) = profile.datetime_parse_count.filter(|count| *count > 0) {
            observations.push(observation(
                ObservationKind::ParseableText,
                profile,
                parsed,
                format!(
                    "{:.2}% parse as ISO datetime",
                    rate(parsed, profile.non_null_rows()) * 100.0
                ),
            ));
        }
        // Near-unique and still repeating. Both numbers are already measured, so this
        // check costs the comparison and nothing else.
        if let (Some(distinct), Some(uniqueness)) =
            (profile.distinct_count, profile.uniqueness_rate())
            && (KEY_LIKE_UNIQUENESS..1.0).contains(&uniqueness)
        {
            let repeats = profile.non_null_rows().saturating_sub(distinct);
            if repeats > 0 {
                let example = match (&profile.dominant_value, profile.dominant_count) {
                    (Some(value), Some(count)) if count > 1 => {
                        format!("; {value:?} appears {count} times")
                    }
                    _ => String::new(),
                };
                observations.push(observation(
                    ObservationKind::KeyLike,
                    profile,
                    repeats,
                    format!(
                        "{:.4}% distinct; {} of {} non-null rows repeat a value{example}",
                        uniqueness * 100.0,
                        repeats,
                        profile.non_null_rows()
                    ),
                ));
            }
        }
    }
    observations
}

/// How many one-column file reads a full run makes for the values type conflicts hide,
/// so the access plan can promise them before anything is read.
///
/// Over the footers rather than over a [`QualitySourceContext`]: the access plan asks
/// this on every frame it is open, and building a context to answer would clone a file
/// list per frame.
pub(crate) fn conflict_reads(
    file_group: &[u32],
    groups: &[crate::schema_union::DriftGroup],
) -> usize {
    let mut per_column = BTreeMap::<&str, usize>::new();
    for group in file_group {
        let Some(group) = groups.get(*group as usize) else {
            continue;
        };
        for column in &group.unread {
            *per_column.entry(column.as_str()).or_default() += 1;
        }
    }
    per_column
        .values()
        .map(|files| (*files).min(MAX_EVIDENCE_FILES))
        .sum()
}

/// Reads named columns of named files at the type each file wrote, which is the only
/// way back to the values a type conflict hides. Given to a run that already reads
/// every value, so the extra read is one column of the few files that disagree.
#[derive(Clone)]
pub struct QualityConflictScan(pub crate::widgets::datatable::FileScan);

impl std::fmt::Debug for QualityConflictScan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("QualityConflictScan")
    }
}

/// Absent columns and type conflicts, from the footers datui already read.
///
/// Both are facts about which files hold which columns, so they are measured over the
/// whole loaded source however the run was scoped: no value in a scope can say
/// anything about a column its file never had, and a conflicting column is not read
/// into the scope at all. The detail pane says so rather than leaving the reader to
/// notice that these two denominators are not the others.
/// What one column loses to the files that disagree: every one of them counted, and
/// the largest few kept by name.
///
/// A dataset of 6,541 files can have a column missing from nearly all of them, so the
/// names are pruned as they arrive. Holding one entry per file per column is how a
/// measurement that costs nothing to compute ends up costing hundreds of megabytes.
#[derive(Default)]
struct DriftTally {
    files: usize,
    rows: usize,
    named: Vec<QualityFileEvidence>,
}

impl DriftTally {
    fn add(&mut self, evidence: QualityFileEvidence) {
        self.files += 1;
        self.rows += evidence.rows;
        self.named.push(evidence);
        if self.named.len() > MAX_EVIDENCE_FILES * 2 {
            self.prune();
        }
    }

    /// Largest first: the files that cost the column the most rows are the ones worth
    /// naming and worth reading values from.
    fn prune(&mut self) {
        self.named.sort_by(|left, right| {
            right
                .rows
                .cmp(&left.rows)
                .then_with(|| left.number.cmp(&right.number))
        });
        self.named.truncate(MAX_EVIDENCE_FILES);
    }
}

fn drift_observations(
    source: &QualitySourceContext,
    conflicts: Option<&QualityConflictScan>,
    polars_streaming: bool,
) -> Vec<QualityObservation> {
    let mut absent = BTreeMap::<String, DriftTally>::new();
    let mut unread = BTreeMap::<String, DriftTally>::new();
    for (file, group) in source.drifting_files() {
        let evidence = |stored_type: Option<String>| QualityFileEvidence {
            number: file + 1,
            name: source
                .file_names
                .get(file)
                .cloned()
                .unwrap_or_else(|| format!("file {}", file + 1)),
            rows: source.file_rows(file),
            stored_type,
            examples: Vec::new(),
        };
        for column in &group.absent {
            absent
                .entry(column.to_string())
                .or_default()
                .add(evidence(None));
        }
        for column in &group.unread {
            let stored = source
                .stored_type(file, column)
                .map(|dtype| dtype.to_string());
            unread
                .entry(column.to_string())
                .or_default()
                .add(evidence(stored));
        }
    }

    let mut observations = Vec::new();
    for (kind, columns) in [
        (ObservationKind::Absent, absent),
        (ObservationKind::TypeConflict, unread),
    ] {
        for (column, mut tally) in columns {
            tally.prune();
            let mut files = tally.named;
            if kind == ObservationKind::TypeConflict
                && let Some(scan) = conflicts
            {
                read_conflict_examples(scan, &column, &mut files, polars_streaming);
            }
            let named = if tally.files > files.len() {
                format!(", largest {} named", files.len())
            } else {
                String::new()
            };
            let verb = if kind == ObservationKind::Absent {
                "has no such column"
            } else {
                "holds a type the scan cannot read"
            };
            // A file whose footer was not read looks exactly like one missing nothing,
            // so on a sampled dataset the count is a floor and has to say so.
            let sampled = if source.footers_read < source.file_names.len() {
                format!(", from {} footers read", source.footers_read)
            } else {
                String::new()
            };
            observations.push(QualityObservation {
                kind,
                column,
                affected_rows: tally.rows,
                evaluated_rows: source.dataset_rows,
                fact: format!(
                    "{} of {} files {verb}{sampled}{named}",
                    tally.files,
                    source.file_names.len()
                ),
                normalized_category: None,
                files,
            });
        }
    }
    observations
}

/// The first values each conflicting file holds, read at that file's own type.
///
/// One scan per file, of one column, limited to the first few rows: a conflict is a
/// property of the file rather than of any row, so the first values it holds are as
/// good evidence as any and stop the read at once. A file that cannot be read this way
/// keeps its count and loses only its examples.
fn read_conflict_examples(
    scan: &QualityConflictScan,
    column: &str,
    files: &mut [QualityFileEvidence],
    polars_streaming: bool,
) {
    let name = PlSmallStr::from(column);
    for file in files.iter_mut() {
        let Ok(lf) = (scan.0)(
            std::slice::from_ref(&file.name),
            std::slice::from_ref(&name),
        ) else {
            continue;
        };
        let query = lf
            .select([col(column).cast(DataType::String)])
            .drop_nulls(None)
            .limit(MAX_CONFLICT_EXAMPLES as u32);
        let Ok(values) = collect_lazy(query, polars_streaming) else {
            continue;
        };
        file.examples = (0..values.height())
            .filter_map(|row| string_value_at(&values, column, row))
            .collect();
    }
}

fn observation(
    kind: ObservationKind,
    profile: &ColumnQualityProfile,
    affected_rows: usize,
    fact: String,
) -> QualityObservation {
    QualityObservation {
        kind,
        column: profile.name.clone(),
        affected_rows,
        evaluated_rows: profile.evaluated_rows,
        fact,
        normalized_category: None,
        files: Vec::new(),
    }
}

fn rate(numerator: usize, denominator: usize) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}

fn optional_usize(df: &DataFrame, name: &str) -> Option<usize> {
    optional_usize_at(df, name, 0)
}

fn optional_usize_at(df: &DataFrame, name: &str, row: usize) -> Option<usize> {
    let value = df.column(name).ok()?.get(row).ok()?;
    match value {
        AnyValue::UInt32(value) => Some(value as usize),
        AnyValue::UInt64(value) => Some(value as usize),
        AnyValue::Int32(value) => usize::try_from(value).ok(),
        AnyValue::Int64(value) => usize::try_from(value).ok(),
        _ => None,
    }
}

fn usize_value(df: &DataFrame, name: &str) -> usize {
    optional_usize(df, name).unwrap_or(0)
}

fn usize_value_at(df: &DataFrame, name: &str, row: usize) -> usize {
    optional_usize_at(df, name, row).unwrap_or(0)
}

fn optional_i64_at(df: &DataFrame, name: &str, row: usize) -> Option<i64> {
    let value = df.column(name).ok()?.get(row).ok()?;
    match value {
        AnyValue::Int64(value) => Some(value),
        AnyValue::Int32(value) => Some(i64::from(value)),
        AnyValue::UInt64(value) => i64::try_from(value).ok(),
        AnyValue::UInt32(value) => Some(i64::from(value)),
        AnyValue::Float64(value) if value.is_finite() => Some(value.round() as i64),
        AnyValue::Float32(value) if value.is_finite() => Some(value.round() as i64),
        _ => None,
    }
}

fn string_value_at(df: &DataFrame, name: &str, row: usize) -> Option<String> {
    let value = df.column(name).ok()?.get(row).ok()?;
    if value.is_null() {
        None
    } else {
        Some(value.str_value().to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> LazyFrame {
        df!(
            "id" => &[1i64, 2, 3, 4],
            "amount" => &[1.0f64, f64::NAN, f64::INFINITY, 4.0],
            "constant" => &["x", "x", "x", "x"],
            "text_number" => &[Some("1"), Some("2.5"), Some("bad"), None],
            "dirty" => &[Some(""), Some("   "), Some("ok"), None],
        )
        .unwrap()
        .lazy()
    }

    #[test]
    fn full_profile_reports_core_counts() {
        let plan = DataQualityPlan {
            compute: QualityCompute::Full,
            ..DataQualityPlan::default()
        };
        let results = compute_data_quality(&fixture(), Some(4), &plan, None, false).unwrap();

        assert_eq!(results.precision, QualityPrecision::Exact);
        assert_eq!(results.evaluated_rows, 4);
        let dirty = results
            .columns
            .iter()
            .find(|profile| profile.name == "dirty")
            .unwrap();
        assert_eq!(dirty.null_count, 1);
        assert_eq!(dirty.distinct_count, Some(3));
        assert_eq!(dirty.empty_count, Some(1));
        assert_eq!(dirty.whitespace_count, Some(1));

        let amount = results
            .columns
            .iter()
            .find(|profile| profile.name == "amount")
            .unwrap();
        assert_eq!(amount.nan_count, Some(1));
        assert_eq!(amount.positive_infinity_count, Some(1));

        let constant = results
            .columns
            .iter()
            .find(|profile| profile.name == "constant")
            .unwrap();
        assert_eq!(constant.distinct_count, Some(1));
        assert!(
            results
                .observations
                .iter()
                .any(|item| item.kind == ObservationKind::Constant)
        );
    }

    #[test]
    fn exact_observation_predicates_select_matching_rows() {
        let examples = [
            (ObservationKind::Nulls, "dirty", 1),
            (ObservationKind::Empty, "dirty", 1),
            (ObservationKind::Whitespace, "dirty", 1),
            (ObservationKind::NonFinite, "amount", 2),
            (ObservationKind::Constant, "constant", 4),
        ];
        for (kind, column, expected) in examples {
            let observation = QualityObservation {
                kind,
                column: column.to_string(),
                affected_rows: expected,
                evaluated_rows: 4,
                fact: String::new(),
                normalized_category: None,
                files: Vec::new(),
            };
            let rows = fixture()
                .filter(observation.evidence_predicate().unwrap())
                .collect()
                .unwrap();
            assert_eq!(rows.height(), expected, "{}", kind.label());
        }
        let category = QualityObservation {
            kind: ObservationKind::CategoryVariants,
            column: "category".to_string(),
            affected_rows: 3,
            evaluated_rows: 3,
            fact: String::new(),
            normalized_category: Some("north".to_string()),
            files: Vec::new(),
        };
        let rows = df!("category" => &["North", " north ", "NORTH"])
            .unwrap()
            .lazy()
            .filter(category.evidence_predicate().unwrap())
            .collect()
            .unwrap();
        assert_eq!(rows.height(), 3);
    }

    #[test]
    fn sample_is_disclosed_and_bounded() {
        let plan = DataQualityPlan {
            compute: QualityCompute::Sample,
            sample_rows: 2,
            sample_seed: 7,
            ..DataQualityPlan::default()
        };
        let results = compute_data_quality(&fixture(), Some(4), &plan, None, false).unwrap();
        assert_eq!(results.precision, QualityPrecision::Sampled);
        assert_eq!(results.total_rows, Some(4));
        assert_eq!(results.evaluated_rows, 2);
    }

    #[test]
    fn unknown_sample_total_stays_unknown_until_bounded_probe_reaches_end() {
        let frame = DataFrame::new(
            100,
            vec![Column::new("id".into(), (0..100).collect::<Vec<_>>())],
        )
        .unwrap()
        .lazy();
        let plan = DataQualityPlan {
            sample_rows: 10,
            ..DataQualityPlan::default()
        };
        let results = compute_data_quality(&frame, None, &plan, None, false).unwrap();
        assert_eq!(results.total_rows, None);
        assert_eq!(results.evaluated_rows, 10);
        assert_eq!(results.precision, QualityPrecision::Sampled);
        assert_eq!(results.segments[0].total_rows, None);

        let short = frame.clone().limit(8);
        let results = compute_data_quality(&short, None, &plan, None, false).unwrap();
        assert_eq!(results.total_rows, Some(8));
        assert_eq!(results.evaluated_rows, 8);
        assert_eq!(results.precision, QualityPrecision::Exact);

        let metadata = DataQualityPlan {
            compute: QualityCompute::Metadata,
            ..plan
        };
        let results = compute_data_quality(&frame, None, &metadata, None, false).unwrap();
        assert_eq!(results.total_rows, None);
        assert_eq!(results.evaluated_rows, 0);
    }

    #[test]
    fn sample_is_seeded_without_replacement_per_row_chunk() {
        let frame = DataFrame::new(
            100,
            vec![Column::new("id".into(), (0..100).collect::<Vec<_>>())],
        )
        .unwrap()
        .lazy();
        let mut plan = DataQualityPlan {
            sample_rows: 3,
            sample_seed: 1,
            grain: QualityGrain::RowChunks(10),
            ..DataQualityPlan::default()
        };
        let first = sample_quality_rows(&frame, 20, 1, false).unwrap();
        let again = sample_quality_rows(&frame, 20, 1, false).unwrap();
        let other = sample_quality_rows(&frame, 20, 2, false).unwrap();
        assert_eq!(first, again);
        assert_ne!(first, other);
        assert_eq!(first.0.column("id").unwrap().n_unique().unwrap(), 20);

        let sampled = sample_quality_segments(&frame, &plan, None, false).unwrap();
        let same = sample_quality_segments(&frame, &plan, None, false).unwrap();
        assert_eq!(sampled.rows, same.rows);
        assert_eq!(sampled.positions, same.positions);
        assert_eq!(sampled.positions.len(), 30);
        assert_eq!(sampled.total_rows, 100);
        assert_eq!(sampled.segment_totals.len(), 10);
        plan.sample_seed += 1;
        let other = sample_quality_segments(&frame, &plan, None, false).unwrap();
        assert_ne!(sampled.positions, other.positions);

        let results = compute_data_quality(&frame, Some(100), &plan, None, false).unwrap();
        assert_eq!(results.total_rows, Some(100));
        assert_eq!(results.evaluated_rows, 30);
        assert_eq!(results.precision, QualityPrecision::Sampled);
        assert_eq!(results.segments.len(), 10);
        assert!(
            results
                .segments
                .iter()
                .all(|segment| segment.total_rows == Some(10) && segment.evaluated_rows == 3)
        );
        plan.compute = QualityCompute::Full;
        let full = compute_data_quality(&frame, Some(100), &plan, None, false).unwrap();
        assert!(
            full.segments
                .iter()
                .all(|segment| segment.total_rows.is_some())
        );
    }

    #[test]
    fn partition_and_time_window_samples_reach_later_segments() {
        let frame = df!(
            "id" => &[0i32, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11],
            "region" => &["a", "a", "a", "a", "b", "b", "b", "b", "c", "c", "c", "c"],
            "week" => &[0i64, 0, 0, 0, 604_800_000_000, 604_800_000_000,
                604_800_000_000, 604_800_000_000, 1_209_600_000_000,
                1_209_600_000_000, 1_209_600_000_000, 1_209_600_000_000]
        )
        .unwrap()
        .lazy()
        .with_columns([col("week").cast(DataType::Datetime(TimeUnit::Microseconds, None))]);
        for grain in [
            QualityGrain::Partition("region".into()),
            QualityGrain::TimeWindows {
                column: "week".into(),
                every: "1w".into(),
            },
        ] {
            let plan = DataQualityPlan {
                sample_rows: 2,
                grain,
                ..DataQualityPlan::default()
            };
            let results = compute_data_quality(&frame, None, &plan, None, false).unwrap();
            assert_eq!(results.total_rows, Some(12));
            assert_eq!(results.evaluated_rows, 6);
            assert_eq!(results.segments.len(), 3);
            assert!(
                results
                    .segments
                    .iter()
                    .all(|segment| segment.total_rows == Some(4) && segment.evaluated_rows == 2)
            );
            let full = compute_data_quality(
                &frame,
                Some(12),
                &DataQualityPlan {
                    compute: QualityCompute::Full,
                    ..plan
                },
                None,
                false,
            )
            .unwrap();
            assert_eq!(
                results
                    .segments
                    .iter()
                    .map(|segment| segment.label.as_str())
                    .collect::<Vec<_>>(),
                full.segments
                    .iter()
                    .map(|segment| segment.label.as_str())
                    .collect::<Vec<_>>()
            );
        }
    }

    #[test]
    fn metadata_mode_does_not_evaluate_values() {
        let plan = DataQualityPlan {
            compute: QualityCompute::Metadata,
            ..DataQualityPlan::default()
        };
        let results = compute_data_quality(&fixture(), Some(4), &plan, None, false).unwrap();
        assert_eq!(results.precision, QualityPrecision::Metadata);
        assert_eq!(results.evaluated_rows, 0);
        assert_eq!(results.columns.len(), 5);
    }

    #[test]
    fn compact_summary_keeps_plan_dimensions_visible() {
        let mut plan = DataQualityPlan::default();
        plan.set_row_chunks();
        plan.comparison = QualityComparison::Previous;
        assert_eq!(
            plan.compact_summary(),
            "scope current view -> grain 1000000 rows (physical order) -> compute 10000 rows/segment -> compare previous"
        );
    }

    #[test]
    fn source_projection_preserves_rows_without_binary_payloads() {
        let source = QualitySourceContext {
            file_names: vec!["one.parquet".to_string()],
            file_starts: vec![0],
            row_index_column: "__datui_quality_row".to_string(),
            ..QualitySourceContext::default()
        };
        let frame = df!(
            "value" => &[1i64, 2, 3],
            "blob" => &[&b"one"[..], &b"two"[..], &b"three"[..]],
        )
        .unwrap()
        .lazy();
        let prepared = prepare_source_quality_scan(frame, Some(&source)).unwrap();
        let collected = prepared.collect().unwrap();
        assert_eq!(collected.height(), 3);
        assert_eq!(
            collected
                .column("__datui_quality_row")
                .unwrap()
                .u32()
                .unwrap()
                .get(2),
            Some(2)
        );
        assert_eq!(
            collected.column("blob").unwrap().str().unwrap().get(0),
            Some(crate::widgets::datatable::BINARY_STUB)
        );
    }

    #[test]
    fn scope_commands_round_trip_and_reject_invalid_ranges() {
        for command in [
            "view",
            "source",
            "rows 2..9",
            "files 1,3",
            "partition region=west",
            "time event=2024-01-01..2024-02-01",
        ] {
            let scope = QualityScope::parse_command(command).unwrap();
            assert_eq!(
                QualityScope::parse_command(&scope.command()).unwrap(),
                scope
            );
        }
        assert!(QualityScope::parse_command("rows 0..10").is_err());
        assert!(QualityScope::parse_command("rows 10..2").is_err());
        assert!(QualityScope::parse_command("files 0").is_err());
        assert!(QualityScope::parse_command("time event=2024-03-01..2024-01-01").is_err());
    }

    #[test]
    fn scoped_frames_select_exact_view_source_file_partition_and_time_rows() {
        let frame = df!(
            "id" => &[1i32, 2, 3, 4, 5],
            "region" => &["west", "east", "west", "east", "west"],
            "day" => &[0i32, 1, 2, 3, 4],
        )
        .unwrap()
        .lazy()
        .with_columns([col("day").cast(DataType::Date)]);
        let ids = |scope: QualityScope, frame: LazyFrame, source: Option<&QualitySourceContext>| {
            let df = apply_quality_scope(frame, &scope, source)
                .unwrap()
                .collect()
                .unwrap();
            df.column("id")
                .unwrap()
                .i32()
                .unwrap()
                .into_no_null_iter()
                .collect::<Vec<_>>()
        };
        assert_eq!(
            ids(
                QualityScope::ViewRows { start: 2, end: 4 },
                frame.clone(),
                None
            ),
            vec![2, 3, 4]
        );
        let source = QualitySourceContext {
            file_names: vec!["one".into(), "two".into(), "three".into()],
            file_starts: vec![0, 2, 4],
            row_index_column: "__row".into(),
            ..QualitySourceContext::default()
        };
        assert_eq!(
            ids(
                QualityScope::SourceFiles(vec![1, 3]),
                frame.clone().with_row_index("__row", None),
                Some(&source)
            ),
            vec![1, 2, 5]
        );
        assert_eq!(
            ids(
                QualityScope::SourcePartition {
                    column: "region".into(),
                    value: "west".into()
                },
                frame.clone(),
                None
            ),
            vec![1, 3, 5]
        );
        assert_eq!(
            ids(
                QualityScope::SourceTimeRange {
                    column: "day".into(),
                    start: "1970-01-02".into(),
                    end: "1970-01-04".into()
                },
                frame,
                None
            ),
            vec![2, 3]
        );
    }

    #[test]
    fn time_scope_accepts_timezone_aware_datetime_bounds() {
        let frame = df!("id" => &[1i32, 2, 3], "ts" => &[0i64, 1_000_000, 2_000_000])
            .unwrap()
            .lazy()
            .with_columns([col("ts").cast(DataType::Datetime(
                TimeUnit::Microseconds,
                Some(TimeZone::UTC),
            ))]);
        let scope =
            QualityScope::parse_command("time ts=1970-01-01T01:00:01+01:00..1970-01-01T00:00:02Z")
                .unwrap();
        let result = apply_quality_scope(frame, &scope, None)
            .unwrap()
            .collect()
            .unwrap();
        assert_eq!(result.column("id").unwrap().i32().unwrap().get(0), Some(2));
        assert_eq!(result.height(), 1);
    }

    #[test]
    fn full_profile_accepts_list_columns() {
        let lists = Column::new(
            "items".into(),
            &[
                Series::new("".into(), &[1i32, 2]),
                Series::new("".into(), &[3i32]),
            ],
        );
        let frame = DataFrame::new(2, vec![lists]).unwrap().lazy();
        let plan = DataQualityPlan {
            compute: QualityCompute::Full,
            ..DataQualityPlan::default()
        };
        let result = compute_data_quality(&frame, Some(2), &plan, None, false).unwrap();
        assert_eq!(result.columns[0].null_count, 0);
        assert_eq!(result.columns[0].min_length, Some(1));
        assert_eq!(result.columns[0].max_length, Some(2));
        let sampled =
            compute_data_quality(&frame, Some(2), &DataQualityPlan::default(), None, false)
                .unwrap();
        assert_eq!(sampled.columns[0].min_length, Some(1));
        assert_eq!(sampled.columns[0].max_length, Some(2));
    }

    #[test]
    fn row_chunks_keep_denominators_and_compare_previous() {
        let plan = DataQualityPlan {
            compute: QualityCompute::Full,
            grain: QualityGrain::RowChunks(2),
            comparison: QualityComparison::Previous,
            ..DataQualityPlan::default()
        };
        let results = compute_data_quality(&fixture(), Some(4), &plan, None, false).unwrap();
        assert_eq!(results.segments.len(), 2);
        assert_eq!(results.segments[0].evaluated_rows, 2);
        let first_dirty = results.segments[0]
            .columns
            .iter()
            .find(|column| column.name == "dirty")
            .unwrap();
        let second_dirty = results.segments[1]
            .columns
            .iter()
            .find(|column| column.name == "dirty")
            .unwrap();
        assert_eq!(QualityMetric::EmptyRate.value(first_dirty), Some(0.5));
        assert_eq!(QualityMetric::EmptyRate.value(second_dirty), Some(0.0));
        assert_eq!(QualityMetric::NullRate.value(second_dirty), Some(0.5));
        assert_eq!(
            results.segments[1].compared_with.as_deref(),
            Some("rows 1-2")
        );
        assert!(results.segments[1].largest_change.is_some());

        let mut selected = results;
        let mut baseline_plan = plan.clone();
        baseline_plan.comparison = QualityComparison::Baseline;
        baseline_plan.baseline_segment = Some("rows 3-4".to_string());
        selected.compare_segments(&baseline_plan);
        assert_eq!(
            selected.segments[0].compared_with.as_deref(),
            Some("rows 3-4")
        );
        assert!(selected.segments[1].compared_with.is_none());
    }

    #[test]
    fn temporal_roles_produce_latency_without_name_inference() {
        let event = Series::new(
            "happened_at".into(),
            [Some(0i64), Some(3_600_000_000), None, Some(10_800_000_000)],
        )
        .cast(&DataType::Datetime(TimeUnit::Microseconds, None))
        .unwrap();
        let received = Series::new(
            "landed_at".into(),
            [
                Some(3_600_000_000i64),
                Some(1_800_000_000),
                Some(7_200_000_000),
                None,
            ],
        )
        .cast(&DataType::Datetime(TimeUnit::Microseconds, None))
        .unwrap();
        let frame = DataFrame::new(4, vec![event.into(), received.into()])
            .unwrap()
            .lazy();
        let mut plan = DataQualityPlan {
            compute: QualityCompute::Full,
            ..DataQualityPlan::default()
        };

        let without_roles = compute_data_quality(&frame, Some(4), &plan, None, false).unwrap();
        assert!(without_roles.temporal.is_empty());

        plan.temporal_roles = vec![
            TemporalRoleAssignment {
                role: TemporalRole::Event,
                column: "happened_at".to_string(),
                timezone: None,
            },
            TemporalRoleAssignment {
                role: TemporalRole::Received,
                column: "landed_at".to_string(),
                timezone: None,
            },
        ];
        let results = compute_data_quality(&frame, Some(4), &plan, None, false).unwrap();
        assert_eq!(results.temporal.len(), 1);
        let latency = &results.temporal[0];
        assert_eq!(latency.missing_start, 1);
        assert_eq!(latency.missing_end, 1);
        assert_eq!(latency.negative_count, 1);
        assert_eq!(latency.p50_seconds, Some(3_600));
    }

    /// Every file counted, only the largest few named. A column missing from thousands
    /// of files must not cost one struct per file to say so.
    #[test]
    fn a_column_missing_from_many_files_counts_them_all_and_names_the_largest() {
        use crate::schema_union::DriftGroup;

        const FILES: usize = 25;
        // File `i` holds `i + 1` rows, so the largest files are the last ones.
        let mut file_starts = Vec::with_capacity(FILES);
        let mut row = 0usize;
        for file in 0..FILES {
            file_starts.push(row);
            row += file + 1;
        }
        let source = QualitySourceContext {
            file_names: (0..FILES).map(|file| format!("{file}.parquet")).collect(),
            file_starts,
            dataset_rows: row,
            footers_read: FILES,
            // Group 1 is missing `fee`; every file is in it.
            file_group: vec![1; FILES],
            drift_groups: Arc::new(vec![
                DriftGroup::default(),
                DriftGroup {
                    absent: vec!["fee".into()],
                    unread: Vec::new(),
                },
            ]),
            ..QualitySourceContext::default()
        };

        let observations = drift_observations(&source, None, false);
        assert_eq!(observations.len(), 1);
        let absent = &observations[0];
        assert_eq!(absent.kind, ObservationKind::Absent);
        assert_eq!(
            (absent.affected_rows, absent.evaluated_rows),
            (row, row),
            "every file is counted, not only the named ones"
        );
        assert_eq!(absent.files.len(), MAX_EVIDENCE_FILES);
        assert_eq!(
            absent.files.first().map(|file| file.number),
            Some(FILES),
            "the largest file first"
        );
        assert!(
            absent
                .fact
                .starts_with("25 of 25 files has no such column, largest 20 named"),
            "{}",
            absent.fact
        );
        assert_eq!(
            absent.evidence_scope().map(|scope| match scope {
                QualityScope::SourceFiles(files) => files.len(),
                _ => 0,
            }),
            Some(MAX_EVIDENCE_FILES),
            "the drill-in opens the files it named"
        );
    }

    /// A column that is nearly a key and is not quite one: the repeats are the finding,
    /// and a column with three values in a hundred rows is a category, not a near-miss.
    #[test]
    fn a_nearly_unique_column_that_repeats_is_reported_with_its_repeats() {
        let mut ids = (0..98i64).collect::<Vec<_>>();
        // Two values that appear twice: 98 distinct values over 100 non-null rows.
        ids.push(7);
        ids.push(11);
        let frame = df!(
            "id" => &ids,
            "region" => &(0..100).map(|row| ["north", "south"][row % 2]).collect::<Vec<_>>(),
        )
        .unwrap()
        .lazy();
        let plan = DataQualityPlan {
            compute: QualityCompute::Full,
            ..DataQualityPlan::default()
        };
        let results = compute_data_quality(&frame, Some(100), &plan, None, false).unwrap();
        let key_like = results
            .observations
            .iter()
            .filter(|observation| observation.kind == ObservationKind::KeyLike)
            .collect::<Vec<_>>();
        assert_eq!(
            key_like
                .iter()
                .map(|o| o.column.as_str())
                .collect::<Vec<_>>(),
            vec!["id"],
            "two values in a hundred rows is a category, not a key that slipped"
        );
        assert_eq!(
            (key_like[0].affected_rows, key_like[0].evaluated_rows),
            (2, 100),
            "non-null rows minus distinct values"
        );
        // The drill-in is every row whose value is not the only one of its kind, which
        // is four rows for two values that each appear twice.
        let rows = frame
            .filter(key_like[0].evidence_predicate().unwrap())
            .collect()
            .unwrap();
        assert_eq!(rows.height(), 4);
    }

    /// The segment comparison names the sharpest single move, not the average of all
    /// of them: a column that goes from never-null to always-null is the finding.
    #[test]
    fn the_largest_change_names_the_column_and_measurement_that_moved() {
        let frame = df!(
            "steady" => &[1i64, 2, 3, 4],
            "fee" => &[Some(1.5f64), Some(2.5), None, None],
        )
        .unwrap()
        .lazy();
        let plan = DataQualityPlan {
            compute: QualityCompute::Full,
            grain: QualityGrain::RowChunks(2),
            comparison: QualityComparison::Previous,
            ..DataQualityPlan::default()
        };
        let results = compute_data_quality(&frame, Some(4), &plan, None, false).unwrap();
        assert_eq!(results.segments.len(), 2);
        assert_eq!(
            results.segments[0].largest_change, None,
            "the first chunk has nothing to compare against"
        );
        let change = results.segments[1]
            .largest_change
            .as_deref()
            .expect("the second chunk compares with the first");
        assert!(
            change.starts_with("fee null rate +100.00 pp"),
            "the column and the measurement that moved: {change}"
        );
    }

    /// With nothing over the material threshold, a range that moved is still a move.
    #[test]
    fn a_segment_whose_rates_hold_still_reports_the_range_that_moved() {
        let frame = df!("reading" => &[1i64, 2, 300, 400]).unwrap().lazy();
        let plan = DataQualityPlan {
            compute: QualityCompute::Full,
            grain: QualityGrain::RowChunks(2),
            comparison: QualityComparison::Previous,
            ..DataQualityPlan::default()
        };
        let results = compute_data_quality(&frame, Some(4), &plan, None, false).unwrap();
        let change = results.segments[1].largest_change.as_deref().unwrap();
        assert!(
            change.starts_with("reading range 1..2 -> 300..400"),
            "no rate moved, but the values did: {change}"
        );
    }

    /// Trends and Segments name the same chunk the same way, whichever compute budget
    /// produced it. The padding a lexicographic sort needs is not a label.
    #[test]
    fn row_chunk_labels_agree_between_trends_and_segments_at_every_budget() {
        let frame = df!(
            "sent" => &[
                "2024-01-01T00:00:00", "2024-01-01T01:00:00",
                "2024-01-01T02:00:00", "2024-01-01T03:00:00",
            ],
            "landed" => &[
                "2024-01-01T01:00:00", "2024-01-01T03:00:00",
                "2024-01-01T04:00:00", "2024-01-01T06:00:00",
            ],
        )
        .unwrap()
        .lazy()
        .with_columns([
            col("sent")
                .str()
                .to_datetime(None, None, StrptimeOptions::default(), lit("raise")),
            col("landed")
                .str()
                .to_datetime(None, None, StrptimeOptions::default(), lit("raise")),
        ]);
        let roles = vec![
            TemporalRoleAssignment {
                role: TemporalRole::Published,
                column: "sent".to_string(),
                timezone: None,
            },
            TemporalRoleAssignment {
                role: TemporalRole::Received,
                column: "landed".to_string(),
                timezone: None,
            },
        ];
        for compute in [QualityCompute::Sample, QualityCompute::Full] {
            let plan = DataQualityPlan {
                compute,
                grain: QualityGrain::RowChunks(2),
                temporal_roles: roles.clone(),
                ..DataQualityPlan::default()
            };
            let results = compute_data_quality(&frame, Some(4), &plan, None, false).unwrap();
            let segments = results
                .segments
                .iter()
                .map(|segment| segment.label.clone())
                .collect::<Vec<_>>();
            assert_eq!(
                segments,
                vec!["rows 1-2", "rows 3-4"],
                "{compute:?} segments"
            );
            let mut trends = results
                .temporal
                .iter()
                .map(|profile| profile.segment.clone())
                .collect::<Vec<_>>();
            trends.dedup();
            assert_eq!(trends, segments, "{compute:?} trends");
        }
    }

    /// No role assigned means no latency to report, and nothing worth splitting the
    /// rows up to discover.
    #[test]
    fn an_unassigned_plan_reports_no_latency() {
        let plan = DataQualityPlan {
            compute: QualityCompute::Sample,
            grain: QualityGrain::RowChunks(2),
            ..DataQualityPlan::default()
        };
        let results = compute_data_quality(&fixture(), Some(4), &plan, None, false).unwrap();
        assert!(results.temporal.is_empty());
    }

    #[test]
    fn source_row_map_produces_file_segments_without_profiling_hidden_columns() {
        let frame = df!(
            "value" => &[1i64, 2, 3, 4],
            "__row" => &[0u32, 1, 2, 3],
        )
        .unwrap()
        .lazy();
        let source = QualitySourceContext {
            file_names: vec!["a.parquet".to_string(), "b.parquet".to_string()],
            file_starts: vec![0, 2],
            row_index_column: "__row".to_string(),
            ..QualitySourceContext::default()
        };
        let plan = DataQualityPlan {
            compute: QualityCompute::Full,
            grain: QualityGrain::File,
            ..DataQualityPlan::default()
        };
        let results = compute_data_quality(&frame, Some(4), &plan, Some(&source), false).unwrap();
        assert_eq!(results.columns.len(), 1);
        assert_eq!(results.segments.len(), 2);
        assert_eq!(results.segments[0].evaluated_rows, 2);
        assert!(results.segments[0].label.contains("a.parquet"));
    }

    #[test]
    fn identity_and_category_groups_keep_distinct_duplicate_semantics() {
        let frame = df!(
            "id" => &[1i64, 1, 2, 3],
            "category" => &["North", "North", " north ", "NORTH"],
        )
        .unwrap()
        .lazy();
        let plan = DataQualityPlan {
            compute: QualityCompute::Full,
            ..DataQualityPlan::default()
        };
        let results = compute_data_quality(&frame, Some(4), &plan, None, false).unwrap();
        let identity = results.identity.unwrap();
        assert_eq!(identity.duplicate_groups, 1);
        assert_eq!(identity.extra_rows, 1);
        assert_eq!(identity.rows_involved, 2);
        assert_eq!(results.category_variants.len(), 1);
        assert_eq!(results.category_variants[0].rows_involved, 4);
        let category = results
            .columns
            .iter()
            .find(|profile| profile.name == "category")
            .unwrap();
        assert_eq!(category.dominant_value.as_deref(), Some("North"));
        assert_eq!(category.dominant_count, Some(2));
    }

    #[test]
    fn sample_identity_does_not_equate_null_with_literal_text() {
        let frame = df!("value" => &[None, Some("<null>"), None])
            .unwrap()
            .lazy();
        let plan = DataQualityPlan {
            sample_rows: 3,
            ..DataQualityPlan::default()
        };
        let results = compute_data_quality(&frame, Some(3), &plan, None, false).unwrap();
        let identity = results.identity.unwrap();
        assert_eq!(identity.duplicate_groups, 1);
        assert_eq!(identity.rows_involved, 2);
    }

    #[test]
    fn partition_and_time_window_grains_create_ordered_profiles() {
        let timestamps = Series::new(
            "event_at".into(),
            [0i64, 86_400_000_000, 8 * 86_400_000_000],
        )
        .cast(&DataType::Datetime(TimeUnit::Microseconds, None))
        .unwrap();
        let frame = DataFrame::new(
            3,
            vec![
                Column::new("partition".into(), ["a", "a", "b"]),
                Column::new("value".into(), [Some(1i64), None, Some(3)]),
                timestamps.into(),
            ],
        )
        .unwrap()
        .lazy();

        let partition_plan = DataQualityPlan {
            compute: QualityCompute::Full,
            grain: QualityGrain::Partition("partition".to_string()),
            ..DataQualityPlan::default()
        };
        let partitioned =
            compute_data_quality(&frame, Some(3), &partition_plan, None, false).unwrap();
        assert_eq!(partitioned.segments.len(), 2);
        assert_eq!(partitioned.segments[0].evaluated_rows, 2);

        let window_plan = DataQualityPlan {
            compute: QualityCompute::Full,
            grain: QualityGrain::TimeWindows {
                column: "event_at".to_string(),
                every: "1w".to_string(),
            },
            ..DataQualityPlan::default()
        };
        let windowed = compute_data_quality(&frame, Some(3), &window_plan, None, false).unwrap();
        assert_eq!(windowed.segments.len(), 2);
        assert!(windowed.segments[0].label.contains("1w"));
    }

    #[test]
    fn a_per_segment_budget_stops_before_it_multiplies_into_millions_of_rows() {
        // 60,000 rows over 30 partitions at 10,000 rows/segment wants 60,000 back
        // — under the cap, so it runs. Multiply the same budget by enough
        // segments and it must be refused rather than silently kept.
        let rows = 60_000usize;
        let frame = DataFrame::new(
            rows,
            vec![
                Column::new(
                    "part".into(),
                    (0..rows)
                        .map(|r| format!("p{:03}", r % 30))
                        .collect::<Vec<_>>(),
                ),
                Column::new("id".into(), (0..rows as i64).collect::<Vec<_>>()),
            ],
        )
        .unwrap()
        .lazy();
        let plan = DataQualityPlan {
            sample_rows: 10_000,
            grain: QualityGrain::Partition("part".to_string()),
            ..DataQualityPlan::default()
        };
        let results = compute_data_quality(&frame, Some(rows), &plan, None, false).unwrap();
        assert_eq!(results.evaluated_rows, rows);
        assert!(results.evaluated_rows <= MAX_RETAINED_SAMPLE_ROWS);

        // The same budget over 60 partitions of 10,000 rows wants 600,000 back.
        let big = 600_000usize;
        let wide = DataFrame::new(
            big,
            vec![
                Column::new(
                    "part".into(),
                    (0..big)
                        .map(|r| format!("p{:03}", r % 60))
                        .collect::<Vec<_>>(),
                ),
                Column::new("id".into(), (0..big as i64).collect::<Vec<_>>()),
            ],
        )
        .unwrap()
        .lazy();
        let error = compute_data_quality(&wide, Some(big), &plan, None, false)
            .expect_err("a budget that multiplies past the cap must be refused");
        let message = error.to_string();
        assert!(
            message.contains("narrow the scope") && message.contains("rows/segment"),
            "the refusal should say what to change: {message}"
        );
    }

    #[test]
    fn an_exact_run_measures_everything_a_sampled_run_does() {
        let frame = df!(
            "when" => &["2024-01-01", "2024-01-02", "not a date", "2024-03-09"],
            "amount" => &["1", "2.5", "3", "bad"],
        )
        .unwrap()
        .lazy();
        // Unambiguous winners, so the two paths cannot differ by tie-breaking.
        let dupes = df!(
            "label" => &[Some("x"), Some("x"), Some("x"), Some("y"), None],
            "n" => &[7i64, 7, 7, 7, 1],
        )
        .unwrap()
        .lazy();
        let measured = |compute| {
            let plan = DataQualityPlan {
                compute,
                ..DataQualityPlan::default()
            };
            let results = compute_data_quality(&frame, Some(4), &plan, None, false).unwrap();
            results
                .columns
                .iter()
                .map(|column| {
                    (
                        column.name.clone(),
                        column.integer_parse_count,
                        column.decimal_parse_count,
                        column.date_parse_count,
                        column.datetime_parse_count,
                    )
                })
                .collect::<Vec<_>>()
        };
        let sampled = measured(QualityCompute::Sample);
        assert_eq!(sampled, measured(QualityCompute::Full));

        let dominant = |compute| {
            let plan = DataQualityPlan {
                compute,
                ..DataQualityPlan::default()
            };
            compute_data_quality(&dupes, Some(5), &plan, None, false)
                .unwrap()
                .columns
                .iter()
                .map(|column| (column.dominant_value.clone(), column.dominant_count))
                .collect::<Vec<_>>()
        };
        assert_eq!(
            dominant(QualityCompute::Full),
            vec![
                (Some("x".to_string()), Some(3)),
                (Some("7".to_string()), Some(4))
            ]
        );
        assert_eq!(
            dominant(QualityCompute::Sample),
            dominant(QualityCompute::Full)
        );
        // The exact run must not be the quieter of the two.
        assert_eq!(sampled[0].3, Some(3), "three of four values are ISO dates");

        // RFC 3339 allows fractional seconds, and so does a bare space separator.
        let stamps = df!("t" => &[
            "2024-01-01T00:00:00Z",
            "2024-01-01T00:00:00.500Z",
            "2024-01-01T00:00:00+01:00",
            "2024-01-01 00:00:00",
            "2024-01-01T00:00:00",
            "garbage",
        ])
        .unwrap()
        .lazy();
        for compute in [QualityCompute::Sample, QualityCompute::Full] {
            let plan = DataQualityPlan {
                compute,
                ..DataQualityPlan::default()
            };
            let results = compute_data_quality(&stamps, Some(6), &plan, None, false).unwrap();
            assert_eq!(
                results.columns[0].datetime_parse_count,
                Some(5),
                "{compute:?} should accept every ISO timestamp but the garbage"
            );
        }
        assert_eq!(sampled[1].2, Some(3), "three of four parse as decimal");
        assert_eq!(sampled[1].1, Some(2), "two of four parse as integer");

        let observed = |compute| {
            let plan = DataQualityPlan {
                compute,
                ..DataQualityPlan::default()
            };
            let mut kinds = compute_data_quality(&frame, Some(4), &plan, None, false)
                .unwrap()
                .observations
                .iter()
                .map(|item| (item.kind, item.column.clone()))
                .collect::<Vec<_>>();
            kinds.sort_by(|left, right| {
                left.1
                    .cmp(&right.1)
                    .then(format!("{:?}", left.0).cmp(&format!("{:?}", right.0)))
            });
            kinds
        };
        assert_eq!(
            observed(QualityCompute::Sample),
            observed(QualityCompute::Full)
        );
    }

    #[test]
    fn a_plan_naming_a_column_the_scope_lost_does_not_kill_the_run() {
        let frame = df!("id" => &[1i64, 2, 3]).unwrap().lazy();
        // A role left over from a wider scope is simply unassigned here.
        let plan = DataQualityPlan {
            compute: QualityCompute::Full,
            temporal_roles: vec![
                TemporalRoleAssignment {
                    role: TemporalRole::Event,
                    column: "gone".to_string(),
                    timezone: None,
                },
                TemporalRoleAssignment {
                    role: TemporalRole::Received,
                    column: "also_gone".to_string(),
                    timezone: None,
                },
            ],
            ..DataQualityPlan::default()
        };
        for compute in [QualityCompute::Sample, QualityCompute::Full] {
            let results = compute_data_quality(
                &frame,
                Some(3),
                &DataQualityPlan {
                    compute,
                    ..plan.clone()
                },
                None,
                false,
            )
            .unwrap_or_else(|error| panic!("{compute:?} with a stale role: {error}"));
            assert!(results.temporal.is_empty());
            assert_eq!(results.columns.len(), 1);
        }

        // A grain the scope cannot satisfy is refused by name, not by a raw error.
        let error = compute_data_quality(
            &frame,
            Some(3),
            &DataQualityPlan {
                grain: QualityGrain::Partition("region".to_string()),
                ..plan
            },
            None,
            false,
        )
        .expect_err("a grain column that is not in scope must be refused");
        assert!(
            error.to_string().contains("region") && error.to_string().contains("not in scope"),
            "the refusal should name the column: {error}"
        );
    }

    #[test]
    fn every_scope_survives_a_trip_through_the_editor() {
        for scope in [
            QualityScope::CurrentView,
            QualityScope::WholeSource,
            QualityScope::FirstRows(10_000),
            QualityScope::FirstRows(1_000_000),
            QualityScope::ViewRows {
                start: 100,
                end: 200,
            },
            QualityScope::SourceFiles(vec![1, 3]),
            QualityScope::SourcePartition {
                column: "region".to_string(),
                value: "west".to_string(),
            },
            QualityScope::SourceTimeRange {
                column: "event".to_string(),
                start: "2024-01-01".to_string(),
                end: "2024-02-01".to_string(),
            },
        ] {
            assert_eq!(
                QualityScope::parse_command(&scope.command()).unwrap(),
                scope,
                "{} should come back as itself",
                scope.command()
            );
        }
        // An empty or inverted range is still refused.
        assert!(QualityScope::parse_command("rows 1..0").is_err());
        assert!(QualityScope::parse_command("rows 0..5").is_err());
    }

    #[test]
    fn metadata_mode_does_not_need_the_grain_column() {
        // It reads no values, so a grain left over from a wider scope is moot.
        let frame = df!("id" => &[1i64, 2]).unwrap().lazy();
        let plan = DataQualityPlan {
            compute: QualityCompute::Metadata,
            grain: QualityGrain::Partition("gone".to_string()),
            ..DataQualityPlan::default()
        };
        let results = compute_data_quality(&frame, Some(2), &plan, None, false).unwrap();
        assert_eq!(results.precision, QualityPrecision::Metadata);
    }

    #[test]
    fn segments_come_back_in_one_order_however_much_was_read() {
        // ∅ sorts after ASCII but before U+6771, so ordering by the label alone
        // put the unplaceable rows in the middle of one path and last in the other.
        let frame = df!(
            "region" => &[Some("a"), Some("a"), Some("\u{6771}\u{4eac}"), Some("\u{6771}\u{4eac}"), None, None],
            "id" => &[1i64, 2, 3, 4, 5, 6],
        )
        .unwrap()
        .lazy();
        let plan = DataQualityPlan {
            grain: QualityGrain::Partition("region".to_string()),
            ..DataQualityPlan::default()
        };
        let labels = |compute| {
            compute_data_quality(
                &frame,
                Some(6),
                &DataQualityPlan {
                    compute,
                    ..plan.clone()
                },
                None,
                false,
            )
            .unwrap()
            .segments
            .iter()
            .map(|segment| segment.label.clone())
            .collect::<Vec<_>>()
        };
        let full = labels(QualityCompute::Full);
        assert_eq!(labels(QualityCompute::Sample), full);
        assert_eq!(full.last().unwrap(), "partition \u{2205}");
    }

    #[test]
    fn a_categorical_column_is_profiled_rather_than_failing_the_run() {
        let frame = df!(
            "label" => &["a", "b", "a", " c "],
            "n" => &[1i64, 2, 3, 4],
        )
        .unwrap()
        .lazy()
        .with_columns([col("label").cast(DataType::from_categories(Categories::global()))]);
        for compute in [QualityCompute::Sample, QualityCompute::Full] {
            let plan = DataQualityPlan {
                compute,
                ..DataQualityPlan::default()
            };
            let results = compute_data_quality(&frame, Some(4), &plan, None, false)
                .unwrap_or_else(|error| panic!("{compute:?} on a categorical column: {error}"));
            let label = results
                .columns
                .iter()
                .find(|column| column.name == "label")
                .expect("the categorical column is profiled");
            assert_eq!(label.null_count, 0);
            assert_eq!(label.distinct_count, Some(3));
        }
    }

    #[test]
    fn every_offered_window_width_cuts_the_scope_it_names() {
        // One row per day from 1970-01-01, far enough to cross a month boundary.
        let day = 86_400_000_000i64;
        let days = 40i64;
        let stamps = Series::new(
            "event_at".into(),
            (0..days).map(|d| d * day).collect::<Vec<_>>(),
        )
        .cast(&DataType::Datetime(TimeUnit::Microseconds, None))
        .unwrap();
        let frame = DataFrame::new(
            days as usize,
            vec![
                Column::new("value".into(), (0..days).collect::<Vec<_>>()),
                stamps.into(),
            ],
        )
        .unwrap()
        .lazy();
        // 1970-01-01 was a Thursday, so 40 days touch seven Monday weeks and two months.
        let expected = [("1h", 40), ("1d", 40), ("1w", 7), ("1mo", 2)];
        for (every, segments) in expected {
            let plan = DataQualityPlan {
                compute: QualityCompute::Full,
                grain: QualityGrain::TimeWindows {
                    column: "event_at".to_string(),
                    every: every.to_string(),
                },
                ..DataQualityPlan::default()
            };
            let results =
                compute_data_quality(&frame, Some(days as usize), &plan, None, false).unwrap();
            assert_eq!(results.segments.len(), segments, "{every} windows");
            assert_eq!(
                results
                    .segments
                    .iter()
                    .map(|segment| segment.evaluated_rows)
                    .sum::<usize>(),
                days as usize,
                "{every} windows must account for every row"
            );
            assert!(results.segments[0].label.ends_with(&format!(" / {every}")));
        }
    }

    #[test]
    fn rows_without_a_window_clock_are_named_and_ordered_the_same_however_much_was_read() {
        let timestamps = Series::new(
            "event_at".into(),
            [Some(0i64), Some(8 * 86_400_000_000), None, None],
        )
        .cast(&DataType::Datetime(TimeUnit::Microseconds, None))
        .unwrap();
        let frame = DataFrame::new(
            4,
            vec![
                Column::new("value".into(), [1i64, 2, 3, 4]),
                timestamps.into(),
            ],
        )
        .unwrap()
        .lazy();
        let plan = DataQualityPlan {
            sample_rows: 1,
            grain: QualityGrain::TimeWindows {
                column: "event_at".to_string(),
                every: "1w".to_string(),
            },
            ..DataQualityPlan::default()
        };

        let sampled = compute_data_quality(&frame, Some(4), &plan, None, false).unwrap();
        let full = compute_data_quality(
            &frame,
            Some(4),
            &DataQualityPlan {
                compute: QualityCompute::Full,
                ..plan.clone()
            },
            None,
            false,
        )
        .unwrap();

        let labels = |results: &DataQualityResults| {
            results
                .segments
                .iter()
                .map(|segment| segment.label.clone())
                .collect::<Vec<_>>()
        };
        assert_eq!(labels(&sampled), labels(&full));
        // Two dated weeks, then the rows the clock could not place.
        assert_eq!(labels(&full).len(), 3);
        assert_eq!(labels(&full)[2], "event_at ∅");
        assert!(labels(&full)[0].ends_with(" / 1w"));
    }
}

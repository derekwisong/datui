use crate::statistics::collect_lazy;
use color_eyre::Result;
use color_eyre::eyre::Report;
use polars::chunked_array::cast::CastOptions;
use polars::prelude::*;
use std::collections::BTreeMap;
use std::sync::Arc;

// A dataset-grain sample is spread across the whole scope (see `sampling::analysis_rows`).
const DEFAULT_SAMPLE_ROWS: usize = 10_000;
const DEFAULT_CHUNK_ROWS: usize = 1_000_000;
const QUALITY_WINDOW_START: &str = "__datui_quality_window_start";
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
/// Values kept per finding from the rows a run read, and groups of duplicate rows:
/// enough to recognize the problem in the detail, which opens the rest.
pub const MAX_FINDING_EXAMPLES: usize = 3;
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
            Self::FirstRows(rows) => format!(
                "first {} rows of the view",
                crate::numfmt::group_chrome(*rows)
            ),
            Self::ViewRows { start, end } => format!(
                "view rows {}-{}",
                crate::numfmt::group_chrome(*start),
                crate::numfmt::group_chrome(*end)
            ),
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

pub(crate) fn parse_scope_time(text: &str) -> Option<i64> {
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
                lit(crate::table::binary_stub()).alias(column)
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

/// The rows of one partition value, a list of them (`2019,2021`), or an inclusive
/// range (`2020..2022`). Each value is read as the column's own type, so years and
/// dates compare as numbers and dates, `1.5` is a `Decimal(10, 2)` column's `1.50`,
/// and the predicate stays one a file's statistics can answer; `∅` is the null
/// partition. A value that does not read as the type says so.
fn partition_predicate(column: &str, value: &str, schema: &Schema) -> Result<Expr> {
    let dtype = schema
        .get(column)
        .ok_or_else(|| color_eyre::eyre::eyre!("partition column {column:?} is unavailable"))?;
    let read = |text: &str| {
        crate::typed_value::parse(text, dtype)
            .map(lit)
            .map_err(|why| color_eyre::eyre::eyre!("{column}: {why}"))
    };
    if let Some((start, end)) = value.split_once("..") {
        let (start, end) = (start.trim(), end.trim());
        if start.is_empty() || end.is_empty() {
            return Err(color_eyre::eyre::eyre!(
                "a partition range needs both ends, for example year=2020..2022"
            ));
        }
        return Ok(col(column)
            .gt_eq(read(start)?)
            .and(col(column).lt_eq(read(end)?)));
    }
    value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| {
            Ok(if value == "∅" {
                col(column).is_null()
            } else {
                col(column).eq(read(value)?)
            })
        })
        .reduce(|all, one| Ok(all?.or(one?)))
        .ok_or_else(|| color_eyre::eyre::eyre!("name at least one partition value"))?
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
            Ok(lf.filter(partition_predicate(column, value, &schema)?))
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
    /// Everything a run needs, staged until Enter runs it. Not a tab: a report's
    /// pages are tabs, and Setup is where a report comes from.
    #[default]
    Setup,
    Overview,
    Columns,
    Segments,
    Trends,
    Detail,
    /// One segment's columns beside the segment it is compared with.
    SegmentDetail,
    /// Each interval in each segment: the time between two roles.
    Intervals,
    /// One interval in one segment, every count it took and out of what.
    IntervalDetail,
    TimeRoles,
    /// Which starts and ends the intervals are, chosen from the assigned roles.
    IntervalPairs,
    /// One bar of one Trends line: the segments it pools, what they hold and how
    /// much of them was read.
    TrendDetail,
    /// The expected windows with no rows to show, by why.
    Gaps,
    /// Which time windows rows are expected in, edited from Setup.
    ExpectedWindows,
    /// What each column must hold, declared: the key and each column's rules.
    Intent,
}

impl QualityPage {
    /// The report's tabs, in the order ←→ walk them. A column's detail sits under
    /// Columns, a segment's under Segments and an interval's under Intervals.
    pub const TABS: [Self; 5] = [
        Self::Overview,
        Self::Columns,
        Self::Segments,
        Self::Trends,
        Self::Intervals,
    ];

    pub fn tab(self) -> Self {
        match self {
            Self::Detail => Self::Columns,
            Self::SegmentDetail => Self::Segments,
            Self::IntervalDetail => Self::Intervals,
            Self::TrendDetail | Self::Gaps => Self::Trends,
            Self::TimeRoles | Self::IntervalPairs | Self::ExpectedWindows | Self::Intent => {
                Self::Setup
            }
            page => page,
        }
    }

    /// Setup and its editors, which stage a run rather than show one.
    pub fn is_setup(self) -> bool {
        self.tab() == Self::Setup
    }

    pub fn title(self) -> &'static str {
        match self.tab() {
            Self::Overview => "Overview",
            Self::Columns => "Columns",
            Self::Segments => "Segments",
            Self::Trends => "Trends",
            Self::Intervals => "Intervals",
            _ => "Setup",
        }
    }
}

/// What an empty page is missing, which Enter opens in Setup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QualitySetup {
    Grain,
    TimeRoles,
    Intervals,
}

impl QualitySetup {
    pub fn label(self) -> &'static str {
        match self {
            Self::Grain => "Set grain",
            Self::TimeRoles => "Time roles",
            Self::Intervals => "Intervals",
        }
    }
}

/// Whether the Trends page can draw a column's measure across segments: that
/// needs segments in an order, and more than one of them.
pub fn shows_trend(plan: &DataQualityPlan, results: &DataQualityResults) -> bool {
    matches!(
        plan.grain,
        QualityGrain::RowChunks(_) | QualityGrain::TimeWindows { .. } | QualityGrain::Partition(_)
    ) && results.segments.len() + results.unsampled_segments.len() > 1
}

/// The plan setting a result page needs before it has anything to show, if any.
/// Intervals need time roles, and ask only when there are dates to assign.
pub fn page_setup(
    page: QualityPage,
    plan: &DataQualityPlan,
    results: Option<&DataQualityResults>,
    has_time_columns: bool,
) -> Option<QualitySetup> {
    let results = results?;
    match page {
        QualityPage::Segments if plan.grain == QualityGrain::Dataset => Some(QualitySetup::Grain),
        QualityPage::Trends if !shows_trend(plan, results) => Some(QualitySetup::Grain),
        // Roles that make no interval want a pair chosen; otherwise, roles. Pairs
        // that measured nothing (metadata only, text with no format) are not
        // fixed by either, and the page says what is.
        QualityPage::Intervals if results.temporal.is_empty() && has_time_columns => {
            if plan.candidate_pairs().is_empty() {
                Some(QualitySetup::TimeRoles)
            } else if plan.interval_pairs().is_empty() {
                Some(QualitySetup::Intervals)
            } else {
                None
            }
        }
        _ => None,
    }
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
    /// How the rows are split, in words: "by day of date", "by year".
    pub fn label(&self) -> String {
        match self {
            Self::Dataset => "whole dataset".to_string(),
            Self::File => "by file".to_string(),
            Self::Partition(column) => format!("by {column}"),
            Self::RowChunks(rows) => {
                format!("in chunks of {} rows", crate::numfmt::group_chrome(*rows))
            }
            Self::TimeWindows { column, every } => {
                let unit = match every.as_str() {
                    "1h" => "hour",
                    "1d" => "day",
                    "1w" => "week",
                    "1mo" => "month",
                    other => other,
                };
                // A column named for its unit would read "by day of day".
                if column.eq_ignore_ascii_case(unit) {
                    format!("by {unit} of the {column} column")
                } else {
                    format!("by {unit} of {column}")
                }
            }
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
    /// The comparison as a plan choice says it.
    pub fn choice_label(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Previous => "the segment before",
            Self::Baseline => "a baseline segment (the first, or b on Segments)",
        }
    }

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

/// The intervals a run measures when none are chosen, start role to end role: the
/// pairs whose order the roles themselves state. Any other start and end is a
/// choice under Intervals in Setup; a role in no interval measures nothing, and
/// Setup says so before a run.
pub const INTERVAL_PAIRS: [(TemporalRole, TemporalRole); 7] = [
    (TemporalRole::Event, TemporalRole::Published),
    (TemporalRole::Event, TemporalRole::Received),
    (TemporalRole::PeriodEnd, TemporalRole::Published),
    (TemporalRole::Published, TemporalRole::Received),
    (TemporalRole::Received, TemporalRole::Processed),
    (TemporalRole::Event, TemporalRole::Processed),
    (TemporalRole::ValidFrom, TemporalRole::ValidTo),
];

/// `event to received`.
pub fn interval_label((start, end): (TemporalRole, TemporalRole)) -> String {
    format!("{} to {}", start.label(), end.label())
}

/// Which time puts an interval in a window, when the grain is time windows: the
/// grain's own column, or the interval's start or end. By the end, an interval is
/// counted on the day it finished rather than the day it began.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum IntervalClock {
    #[default]
    Grain,
    Start,
    End,
}

impl IntervalClock {
    pub const ALL: [Self; 3] = [Self::Grain, Self::Start, Self::End];

    pub fn label(self) -> &'static str {
        match self {
            Self::Grain => "the grain's column",
            Self::Start => "each interval's start",
            Self::End => "each interval's end",
        }
    }
}

/// Whether text read as time is a date or a date with a time of day.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TimeKind {
    Date,
    Datetime,
}

impl TimeKind {
    pub fn label(self) -> &'static str {
        match self {
            Self::Date => "date",
            Self::Datetime => "datetime",
        }
    }
}

/// The formats Setup offers for reading text as time, the unambiguous ones first.
/// Named formats rather than inference: a run reads every row the same way, and a
/// value the format does not read is counted, not guessed at.
pub const TIME_FORMATS: [(TimeKind, &str); 16] = [
    (TimeKind::Datetime, "%Y-%m-%d %H:%M:%S"),
    (TimeKind::Datetime, "%Y-%m-%dT%H:%M:%S"),
    (TimeKind::Datetime, "%Y-%m-%d %H:%M:%S%.f"),
    (TimeKind::Datetime, "%Y-%m-%dT%H:%M:%S%.f"),
    // ISO 8601 with an offset: `%#z` takes `Z`, `+05:00`, `-0500` and `+05`, and
    // the values are read as instants in UTC.
    (TimeKind::Datetime, "%Y-%m-%dT%H:%M:%S%.f%#z"),
    (TimeKind::Datetime, "%Y-%m-%d %H:%M:%S%.f%#z"),
    (TimeKind::Datetime, "%Y-%m-%d %H:%M"),
    (TimeKind::Date, "%Y-%m-%d"),
    (TimeKind::Date, "%Y%m%d"),
    (TimeKind::Datetime, "%m/%d/%Y %H:%M:%S"),
    (TimeKind::Datetime, "%m/%d/%Y %I:%M:%S %p"),
    (TimeKind::Datetime, "%d/%m/%Y %H:%M:%S"),
    (TimeKind::Datetime, "%d.%m.%Y %H:%M:%S"),
    (TimeKind::Date, "%m/%d/%Y"),
    (TimeKind::Date, "%d/%m/%Y"),
    (TimeKind::Date, "%d.%m.%Y"),
];

/// A text column read as a date or datetime for one study. Grain and time roles see
/// the parsed value; every other check sees the text as stored, so a column's own
/// findings keep their physical meaning. A value the format does not read is counted
/// as unparsed, never folded into the column's missing values.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TimeInterpretation {
    pub column: String,
    pub kind: TimeKind,
    /// A strftime format, as Polars' `str.to_datetime` takes it. With an offset
    /// (`%z`), the values are instants in UTC; without one, times with no zone.
    pub format: String,
}

impl TimeInterpretation {
    /// Whether the format reads an offset, so its values are instants in UTC
    /// rather than times with no zone.
    pub fn zoned(&self) -> bool {
        self.format.contains('z')
    }

    /// `datetime %Y-%m-%d %H:%M:%S`.
    pub fn label(&self) -> String {
        format!("{} {}", self.kind.label(), self.format)
    }

    /// The column's values as time: null where the format does not read the text.
    pub fn expr(&self) -> Expr {
        let options = StrptimeOptions {
            format: Some(PlSmallStr::from(self.format.as_str())),
            strict: false,
            exact: true,
            cache: true,
        };
        // A categorical column holds codes; its values are read as the text they name.
        let text = col(self.column.as_str()).cast(DataType::String).str();
        match self.kind {
            TimeKind::Date => text.to_date(options),
            TimeKind::Datetime => text.to_datetime(
                Some(TimeUnit::Microseconds),
                None,
                options,
                lit(PlSmallStr::from_static("raise")),
            ),
        }
    }

    /// Rows holding text the format does not read.
    pub fn unparsed(&self) -> Expr {
        col(self.column.as_str())
            .is_not_null()
            .and(self.expr().is_null())
    }

    /// Whether the format reads `value`, the way a run will: for the examples Setup
    /// shows beside each format, from rows already on screen.
    pub fn reads(&self, value: &str) -> bool {
        match self.kind {
            TimeKind::Date => chrono::NaiveDate::parse_from_str(value, &self.format).is_ok(),
            // An offset format needs the offset: without one there is no instant.
            TimeKind::Datetime if self.zoned() => {
                chrono::DateTime::parse_from_str(value, &self.format).is_ok()
            }
            TimeKind::Datetime => {
                chrono::NaiveDateTime::parse_from_str(value, &self.format).is_ok()
            }
        }
    }
}

/// What a Data Quality run is doing now. The worker names each stage as it enters
/// it, and the progress view shows the latest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QualityStage {
    Preparing,
    CopyingSource,
    ReusingSample,
    ReadingSample,
    CountingRows,
    CountingSegments,
    ProfilingColumns,
    CheckingDuplicates,
    CheckingKey,
    CheckingSpellings,
    ReadingConflicts,
    ProfilingSegments,
    ComputingIntervals,
    CheckingSharedNulls,
    /// An audio file's samples, read whole for clipping, runs of zeros and DC offset.
    CheckingSignal,
    Assembling,
}

impl QualityStage {
    pub fn label(self) -> &'static str {
        match self {
            Self::Preparing => "Preparing the plan",
            Self::CopyingSource => "Copying the source locally",
            Self::ReusingSample => "Reusing the retained sample",
            Self::ReadingSample => "Reading the sample",
            Self::CountingRows => "Counting rows",
            Self::CountingSegments => "Counting segment rows",
            Self::ProfilingColumns => "Profiling columns",
            Self::CheckingDuplicates => "Checking duplicate rows",
            Self::CheckingKey => "Checking the declared key",
            Self::CheckingSpellings => "Checking category spellings",
            Self::ReadingConflicts => "Reading conflicting values",
            Self::ProfilingSegments => "Profiling segments",
            Self::ComputingIntervals => "Computing intervals",
            Self::CheckingSharedNulls => "Checking columns missing together",
            Self::CheckingSignal => "Checking the signal",
            Self::Assembling => "Assembling the report",
        }
    }
}

/// A stage, whether it reads the source or works on rows already read, and whether
/// a cancel stops it partway.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QualityPhase {
    pub stage: QualityStage,
    pub reads_source: bool,
    /// A cancel ends this stage within a batch. A stage that is one collect Polars
    /// cannot watch runs to its end, and the screen says so while it does.
    pub interruptible: bool,
}

/// What a run's reads of the source were seen to do, counted as the rows went by.
/// Bytes and requests are not counted: a Polars scan does not report them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ObservedReads {
    /// Stages that read the source.
    pub reads: usize,
    /// Of those, the ones whose rows were counted as they came.
    pub counted: usize,
    /// Rows the counted reads passed through from the scope, over every pass.
    pub rows: usize,
    /// The local copy a full scan's passes read instead of the source, when they did.
    pub copy: Option<CopyRead>,
}

/// A local copy of a remote source that a full scan's passes read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CopyRead {
    pub bytes: u64,
    pub objects: usize,
    /// This run fetched it; otherwise an earlier run did and this one reused it.
    pub fetched: bool,
}

/// A run's line to the screen: its stages as it enters them, the rows its reads
/// have seen, and a stop the run checks between stages and its reads check between
/// batches.
#[derive(Clone, Default)]
pub struct QualityWatch {
    read: crate::sampling::ReadWatch,
    report: Option<Arc<dyn Fn(QualityPhase) + Send + Sync>>,
    /// The stage under way, and what the stages before it read.
    last: Arc<std::sync::Mutex<(Option<QualityPhase>, ObservedReads)>>,
    /// Set once the scope reads a local copy: its passes then read no source.
    copy: Arc<std::sync::OnceLock<CopyRead>>,
}

impl std::fmt::Debug for QualityWatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QualityWatch")
            .field("read", &self.read)
            .finish_non_exhaustive()
    }
}

impl QualityWatch {
    /// A watch that hands each new stage to `report`.
    pub fn new(report: impl Fn(QualityPhase) + Send + Sync + 'static) -> Self {
        Self {
            report: Some(Arc::new(report)),
            ..Self::default()
        }
    }

    pub fn cancel(&self) {
        self.read.stop();
    }

    pub fn cancelled(&self) -> bool {
        self.read.stopped()
    }

    /// The reads' side: the stop, and the rows the stage under way has seen.
    pub fn read(&self) -> &crate::sampling::ReadWatch {
        &self.read
    }

    /// What the run's reads were seen to do, the one under way included.
    pub fn observed(&self) -> ObservedReads {
        let Ok(last) = self.last.lock() else {
            return ObservedReads::default();
        };
        let (phase, mut observed) = *last;
        if phase.is_some_and(|phase| phase.reads_source) {
            observed.reads += 1;
            if let Some(rows) = self.read.rows_seen() {
                observed.counted += 1;
                observed.rows += rows;
            }
        }
        observed.copy = self.copy.get().copied();
        observed
    }

    /// The scope is read from a local copy from here on.
    pub(crate) fn use_copy(&self, copy: CopyRead) {
        let _ = self.copy.set(copy);
    }

    /// Whether a pass over the scope reads the source: not once it reads a copy.
    fn scope_reads(&self, reads: bool) -> bool {
        reads && self.copy.get().is_none()
    }

    /// `lf`, watched: every batch that reaches its top is counted, and once the run
    /// is cancelled the next one fails the query. On the streaming engine a collect
    /// of it stops within a batch rather than at its end; in memory the scope
    /// arrives as one batch, after the read. Projections and filters pass through
    /// to the scan as they would without it.
    fn watched(&self, lf: &LazyFrame) -> LazyFrame {
        let read = self.read.clone();
        lf.clone().map(
            move |df: DataFrame| {
                if read.stopped() {
                    return Err(PolarsError::ComputeError(crate::sampling::CANCELLED.into()));
                }
                read.saw(df.height());
                Ok(df)
            },
            OptFlags::PROJECTION_PUSHDOWN | OptFlags::PREDICATE_PUSHDOWN | OptFlags::STREAMING,
            None,
            Some("quality watch"),
        )
    }

    /// Enter `stage`. Said once however often it is entered, and refused once the run
    /// is cancelled: between stages is where a run stops. Leaving a stage that read
    /// the source adds the rows it counted to what was observed.
    pub(crate) fn stage(
        &self,
        stage: QualityStage,
        reads_source: bool,
        interruptible: bool,
    ) -> Result<()> {
        self.read.check()?;
        let phase = QualityPhase {
            stage,
            reads_source,
            interruptible,
        };
        let mut last = self
            .last
            .lock()
            .map_err(|_| Report::msg("quality progress lock failed"))?;
        let (previous, observed) = &mut *last;
        if *previous != Some(phase) {
            // Rows on screen are the stage's own, so each read counts from zero.
            let seen = self.read.restart();
            if previous.is_some_and(|phase| phase.reads_source) {
                observed.reads += 1;
                if let Some(rows) = seen {
                    observed.counted += 1;
                    observed.rows += rows;
                }
            }
            *previous = Some(phase);
            if let Some(report) = &self.report {
                report(phase);
            }
        }
        Ok(())
    }

    /// A watched collect's error, or the cancel that caused it: stopped partway, it
    /// fails as a stopped sampler does.
    fn failed(&self, error: impl Into<Report>) -> Report {
        if self.cancelled() {
            Report::msg(crate::sampling::CANCELLED)
        } else {
            error.into()
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataQualityPlan {
    pub scope: QualityScope,
    pub compute: QualityCompute,
    /// How a dataset-grain sample picks its rows, from the shared analysis sample.
    pub method: crate::sampling::SampleMethod,
    /// Rows a dataset-grain sample keeps: the shared analysis sample's size.
    pub dataset_rows: usize,
    pub sample_seed: u64,
    pub grain: QualityGrain,
    pub comparison: QualityComparison,
    pub baseline_segment: Option<String>,
    pub temporal_roles: Vec<TemporalRoleAssignment>,
    /// The intervals chosen under Intervals, start role to end role. `None` until
    /// one is chosen: the suggested pairs the assigned roles make.
    pub intervals: Option<Vec<(TemporalRole, TemporalRole)>>,
    /// Which time puts an interval in a time window.
    pub interval_clock: IntervalClock,
    pub latency_threshold_seconds: Option<i64>,
    /// Text columns read as time for this study, by grain and roles only.
    pub time_formats: Vec<TimeInterpretation>,
    /// The time windows rows are expected in, when stated: what makes a window
    /// with no rows a gap. Read from the segments a run counted, so it changes what
    /// the report says, never what a run reads.
    pub expected: Option<ExpectedWindows>,
    /// What the columns must hold, declared: the key and each column's rules. Read
    /// from the rows the run reads; it decides no rows, so it is the report's.
    pub intent: crate::quality_intent::DeclaredIntent,
}

/// Which time windows a study expects rows in, as stated in Setup: every window of
/// the grain, or Monday to Friday's only, from one time and before another. Unset,
/// no window is called a gap: a quiet weekend is not a defect unless someone says so.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ExpectedWindows {
    /// Only Monday to Friday's hours or days are expected.
    pub weekdays: bool,
    /// The first expected time, as typed: a date or a UTC timestamp. `None` starts
    /// at the first window the run found.
    pub from: Option<String>,
    /// The time expected windows end before. `None` ends after the last window the
    /// run found.
    pub before: Option<String>,
}

impl ExpectedWindows {
    /// Whether `every` is a width whose windows can fall on a weekend: an hour or a
    /// day. A week or a month always holds weekdays.
    pub fn weekdays_apply(every: &str) -> bool {
        matches!(every, "1h" | "1d")
    }

    /// The cadence, in Setup's words: "every day", "weekdays".
    pub fn cadence_label(&self, every: &str) -> String {
        if self.weekdays && Self::weekdays_apply(every) {
            "weekdays".to_string()
        } else {
            let unit = match every {
                "1h" => "hour",
                "1d" => "day",
                "1w" => "week",
                "1mo" => "month",
                other => other,
            };
            format!("every {unit}")
        }
    }

    /// The range, in Setup's words: "2024-01-01 to before 2024-04-01", or the windows
    /// found where a side is not stated.
    pub fn range_label(&self) -> String {
        match (self.from.as_deref(), self.before.as_deref()) {
            (None, None) => "first to last window found".to_string(),
            (Some(from), None) => format!("{from} to the last window found"),
            (None, Some(before)) => format!("first window found to before {before}"),
            (Some(from), Some(before)) => format!("{from} to before {before}"),
        }
    }

    /// Why the typed range cannot be read, if it cannot.
    pub fn problem(&self) -> Option<String> {
        let read = |text: &Option<String>| match text.as_deref() {
            None => Ok(None),
            Some(text) => parse_scope_time(text)
                .map(Some)
                .ok_or_else(|| format!("{text} is not a date or UTC timestamp")),
        };
        match (read(&self.from), read(&self.before)) {
            (Err(problem), _) | (_, Err(problem)) => Some(problem),
            (Ok(Some(from)), Ok(Some(before))) if before <= from => {
                Some("Before must be after From".to_string())
            }
            _ => None,
        }
    }

    /// The typed range in microseconds since the epoch, each side when stated and
    /// readable.
    pub fn bounds(&self) -> (Option<i64>, Option<i64>) {
        (
            self.from.as_deref().and_then(parse_scope_time),
            self.before.as_deref().and_then(parse_scope_time),
        )
    }
}

impl Default for DataQualityPlan {
    fn default() -> Self {
        Self {
            scope: QualityScope::CurrentView,
            compute: QualityCompute::Sample,
            method: crate::sampling::SampleMethod::Spread,
            dataset_rows: DEFAULT_SAMPLE_ROWS,
            sample_seed: 42_891,
            grain: QualityGrain::Dataset,
            comparison: QualityComparison::None,
            baseline_segment: None,
            temporal_roles: Vec::new(),
            intervals: None,
            interval_clock: IntervalClock::Grain,
            latency_threshold_seconds: None,
            time_formats: Vec::new(),
            expected: None,
            intent: crate::quality_intent::DeclaredIntent::default(),
        }
    }
}

impl DataQualityPlan {
    /// Only a full scan asks first. Every grain reads the shared sample and cuts it
    /// into segments, so no grain reads more than the sample says.
    pub fn requires_confirmation(&self) -> bool {
        self.compute == QualityCompute::Full
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

    /// The shared analysis sample this plan carries, as the Sample form shows it.
    pub fn sample(&self) -> crate::sampling::Sample {
        crate::sampling::Sample {
            scope: self.scope.clone(),
            method: self.method.clone(),
            rows: self.dataset_rows,
            seed: self.sample_seed,
        }
    }

    /// Take `sample` as the rows this plan reads. Metadata-only stays metadata-only;
    /// otherwise every row is a full read and anything less a sampled one.
    ///
    /// Choosing equal rows per value of a column is choosing to look at that column's
    /// values side by side, and the grain is what does that. Taken only when the
    /// choice is new and the grain has not been set, so a grain chosen afterwards
    /// stays chosen.
    pub fn adopt_sample(&mut self, sample: &crate::sampling::Sample) {
        if self.scope != sample.scope {
            self.baseline_segment = None;
        }
        self.scope = sample.scope.clone();
        self.sample_seed = sample.seed;
        self.dataset_rows = sample.rows;
        if self.compute != QualityCompute::Metadata {
            self.compute = if sample.method == crate::sampling::SampleMethod::EveryRow {
                QualityCompute::Full
            } else {
                QualityCompute::Sample
            };
        }
        if let crate::sampling::SampleMethod::PerPartition { column } = &sample.method
            && self.method != sample.method
            && self.grain == QualityGrain::Dataset
        {
            self.grain = QualityGrain::Partition(column.clone());
            self.baseline_segment = None;
        }
        self.method = sample.method.clone();
    }

    /// How `column` is read as time, when it is text read through a format.
    pub fn time_format(&self, column: &str) -> Option<&TimeInterpretation> {
        self.time_formats
            .iter()
            .find(|interpretation| interpretation.column == column)
    }

    /// A column's values as time: parsed through its format when it has one, and as
    /// stored otherwise.
    pub fn time_value(&self, column: &str) -> Expr {
        self.time_format(column)
            .map(TimeInterpretation::expr)
            .unwrap_or_else(|| col(column))
    }

    /// Whether `column` of `schema` can be read as time: a date or time type, or text
    /// with a format.
    pub fn reads_as_time(&self, column: &str, schema: &Schema) -> bool {
        self.time_format(column).is_some() || schema.get(column).is_some_and(DataType::is_temporal)
    }

    /// The column a role is assigned to.
    pub fn role_column(&self, role: TemporalRole) -> Option<&str> {
        self.temporal_roles
            .iter()
            .find(|assignment| assignment.role == role)
            .map(|assignment| assignment.column.as_str())
    }

    /// The intervals this plan measures: the chosen ones, or until one is chosen the
    /// suggested pairs; either way only those whose two roles are assigned.
    pub fn interval_pairs(&self) -> Vec<(TemporalRole, TemporalRole)> {
        let assigned = |(start, end): &(TemporalRole, TemporalRole)| {
            self.role_column(*start).is_some() && self.role_column(*end).is_some()
        };
        match &self.intervals {
            None => INTERVAL_PAIRS.into_iter().filter(assigned).collect(),
            Some(chosen) => chosen.iter().copied().filter(assigned).collect(),
        }
    }

    /// Every start and end the assigned roles can make, the suggested pairs first,
    /// then the rest in role order: what Intervals in Setup lists.
    pub fn candidate_pairs(&self) -> Vec<(TemporalRole, TemporalRole)> {
        let roles = TemporalRole::ALL
            .into_iter()
            .filter(|role| self.role_column(*role).is_some())
            .collect::<Vec<_>>();
        let mut pairs = INTERVAL_PAIRS
            .into_iter()
            .filter(|(start, end)| roles.contains(start) && roles.contains(end))
            .collect::<Vec<_>>();
        for start in &roles {
            for end in &roles {
                if start != end && !pairs.contains(&(*start, *end)) {
                    pairs.push((*start, *end));
                }
            }
        }
        pairs
    }

    /// Measure `pair`, or stop measuring it. The first choice makes the list
    /// explicit, starting from what was measured.
    pub fn toggle_interval(&mut self, pair: (TemporalRole, TemporalRole)) {
        let mut chosen = self.interval_pairs();
        match chosen.iter().position(|chosen| *chosen == pair) {
            Some(index) => {
                chosen.remove(index);
            }
            None => chosen.push(pair),
        }
        self.intervals = Some(chosen);
    }

    /// Assigned roles that no measured interval uses: they measure nothing.
    pub fn unpaired_roles(&self) -> Vec<TemporalRole> {
        let pairs = self.interval_pairs();
        TemporalRole::ALL
            .into_iter()
            .filter(|role| {
                self.role_column(*role).is_some()
                    && !pairs
                        .iter()
                        .any(|(start, end)| start == role || end == role)
            })
            .collect()
    }

    /// Whether the clock choice means anything: intervals cut into time windows.
    pub fn windows_intervals(&self) -> bool {
        matches!(self.grain, QualityGrain::TimeWindows { .. }) && !self.interval_pairs().is_empty()
    }

    /// The grain an interval from `start` to `end` is cut by: the plan's, except
    /// that time windows go by the interval's own start or end when the clock says.
    pub fn interval_grain(&self, start: &str, end: &str) -> QualityGrain {
        match (&self.grain, self.interval_clock) {
            (QualityGrain::TimeWindows { every, .. }, IntervalClock::Start) => {
                QualityGrain::TimeWindows {
                    column: start.to_string(),
                    every: every.clone(),
                }
            }
            (QualityGrain::TimeWindows { every, .. }, IntervalClock::End) => {
                QualityGrain::TimeWindows {
                    column: end.to_string(),
                    every: every.clone(),
                }
            }
            (grain, _) => grain.clone(),
        }
    }

    /// Whether `column` holds instants (a zoned type, or text read with an
    /// offset) rather than times with no zone; `None` when it is not read as time.
    pub fn zoned(&self, column: &str, schema: &Schema) -> Option<bool> {
        if let Some(format) = self.time_format(column) {
            return Some(format.zoned());
        }
        match schema.get(column)? {
            DataType::Datetime(_, zone) => Some(zone.is_some()),
            DataType::Date => Some(false),
            _ => None,
        }
    }

    /// Whether `other` measures what this plan measures: they differ, if at all, in
    /// the windows they expect, which a report checks against the counts it already
    /// holds, or in what its segments are compared with, which is worked out from the
    /// segments it holds ([`DataQualityResults::compare_segments`]).
    pub fn same_measurement(&self, other: &Self) -> bool {
        let measured = |plan: &Self| Self {
            expected: None,
            comparison: QualityComparison::None,
            baseline_segment: None,
            ..plan.clone()
        };
        measured(self) == measured(other)
    }

    /// Whether `other` compares segments differently from this plan.
    pub fn compares_differently(&self, other: &Self) -> bool {
        self.comparison != other.comparison || self.baseline_segment != other.baseline_segment
    }

    /// The windows this plan expects rows in: only on a time-window grain.
    pub fn expected_windows(&self) -> Option<&ExpectedWindows> {
        matches!(self.grain, QualityGrain::TimeWindows { .. })
            .then_some(self.expected.as_ref())
            .flatten()
    }

    /// The next coarser grain to offer when segments are thin: a day for an hour, a
    /// week for a day, a month for a week, and a larger row chunk. Partitions and files
    /// have none.
    pub fn coarser_grain(&self) -> Option<QualityGrain> {
        match &self.grain {
            QualityGrain::TimeWindows { column, every } => {
                let coarser = match every.as_str() {
                    "1h" => "1d",
                    "1d" => "1w",
                    "1w" => "1mo",
                    _ => return None,
                };
                Some(QualityGrain::TimeWindows {
                    column: column.clone(),
                    every: coarser.to_string(),
                })
            }
            QualityGrain::RowChunks(rows) if *rows < DEFAULT_CHUNK_ROWS => {
                Some(QualityGrain::RowChunks(DEFAULT_CHUNK_ROWS))
            }
            _ => None,
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
                QualityCompute::Sample => format!(
                    "{} rows {}",
                    self.dataset_rows,
                    self.method.label().to_lowercase()
                ),
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

    /// The rows a rate is taken over: every row, or the rows with a value.
    pub fn denominator(self, column: &ColumnQualityProfile) -> usize {
        match self {
            Self::NullRate | Self::EmptyRate | Self::WhitespaceRate | Self::NonFiniteRate => {
                column.evaluated_rows
            }
            Self::DistinctShare | Self::IntegerParseShare | Self::DecimalParseShare => {
                column.non_null_rows()
            }
        }
    }

    /// The measure's name in a table cell or a change: "nulls", "distinct".
    pub fn short_label(self) -> &'static str {
        match self {
            Self::NullRate => "nulls",
            Self::EmptyRate => "empty",
            Self::WhitespaceRate => "blank",
            Self::NonFiniteRate => "NaN/inf",
            Self::DistinctShare => "distinct",
            Self::IntegerParseShare => "integer parse",
            Self::DecimalParseShare => "decimal parse",
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
    /// Text values that parse as whole numbers and are written with a leading zero:
    /// the mark of a code (a ZIP, an account, an industry code) rather than a number.
    pub leading_zero_count: Option<usize>,
    pub dominant_value: Option<String>,
    pub dominant_count: Option<usize>,
    pub min_length: Option<usize>,
    pub max_length: Option<usize>,
}

impl ColumnQualityProfile {
    /// A column with its null count at zero and nothing else measured.
    pub fn unmeasured(name: &str, dtype: DataType, evaluated_rows: usize) -> Self {
        Self {
            name: name.to_string(),
            dtype,
            evaluated_rows,
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
            leading_zero_count: None,
            dominant_value: None,
            dominant_count: None,
            min_length: None,
            max_length: None,
        }
    }

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
    /// Text read as time that the chosen format does not read.
    UnparsedTime,
    /// Rows sharing a value of the declared key.
    KeyRepeated,
    /// Rows with no value in some part of the declared key.
    KeyMissing,
    /// A column declared required, with no value.
    RequiredMissing,
    /// Values outside a column's declared allowed set.
    NotAllowed,
    /// Values outside a column's declared range.
    OutOfRange,
    /// Text declared to read as a number that does not.
    UnparsedNumber,
    /// Audio samples in runs at full scale: the waveform cut flat at the limit.
    Clipping,
    /// Audio samples in long runs of exact zeros: dropouts, or digital silence.
    ZeroRuns,
    /// An audio channel whose mean sits away from zero.
    DcOffset,
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
    /// What only the engine can say of a drift or audio measurement: the files, the
    /// runs. Empty for every kind the report phrases from the numbers itself.
    pub fact: String,
    pub normalized_category: Option<String>,
    /// The files behind an [`ObservationKind::Absent`] or
    /// [`ObservationKind::TypeConflict`] measurement, commonest first. Empty for every
    /// check measured over values rather than over footers.
    pub files: Vec<QualityFileEvidence>,
    /// The format an [`ObservationKind::UnparsedTime`] measurement read the text with.
    pub time_format: Option<TimeInterpretation>,
    /// The values at or past which an audio sample is at full scale, for an
    /// [`ObservationKind::Clipping`] measurement's rows.
    pub full_scale: Option<(f64, f64)>,
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

    /// The rows behind this observation, as a predicate over the rows the run read.
    /// The engine counts with the same expression where it can, so the count and the
    /// rows Enter opens agree.
    pub fn evidence_predicate(&self, results: &DataQualityResults) -> Option<Expr> {
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
            ObservationKind::UnparsedTime => {
                self.time_format.as_ref().map(TimeInterpretation::unparsed)
            }
            // Every sample at full scale, in a run or not: the runs are what is
            // counted, and the samples around them are what a look wants.
            ObservationKind::Clipping => self
                .full_scale
                .map(|(low, high)| value.clone().lt_eq(lit(low)).or(value.gt_eq(lit(high)))),
            ObservationKind::ZeroRuns => Some(value.eq(lit(0))),
            // The values that stop a cast: text the reading does not parse.
            ObservationKind::ParseableText => unparsed_text(
                results
                    .columns
                    .iter()
                    .find(|profile| profile.name == self.column)?,
            ),
            // Declared rules find their rows through what the run measured them with.
            ObservationKind::KeyRepeated => results.intent.as_ref()?.repeated_key(),
            ObservationKind::KeyMissing => results.intent.as_ref()?.missing_key(),
            ObservationKind::RequiredMissing => {
                results.intent.as_ref()?.required_missing(&self.column)
            }
            ObservationKind::NotAllowed => results.intent.as_ref()?.not_allowed(&self.column),
            ObservationKind::OutOfRange => results.intent.as_ref()?.out_of_range(&self.column),
            ObservationKind::UnparsedNumber => {
                results.intent.as_ref()?.unparsed_number(&self.column)
            }
            // Duplicates are rows equal to another, not rows a value picks out;
            // absent and conflicting rows are named by their files, the column not
            // being in those rows to be tested; an offset is in every sample.
            ObservationKind::DuplicateRows
            | ObservationKind::Absent
            | ObservationKind::TypeConflict
            | ObservationKind::DcOffset => None,
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
    /// The most copied groups, from the rows the run kept. Empty after a full scan,
    /// which keeps no rows.
    pub examples: Vec<DuplicateExample>,
}

/// One group of identical rows: how many there are, and the row, a value a column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DuplicateExample {
    pub copies: usize,
    /// Rendered for reading: text quoted, a null as `null`.
    pub values: Vec<String>,
}

/// A few of the values behind one column's finding, from the rows the run kept.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FindingExamples {
    pub kind: ObservationKind,
    pub column: String,
    /// Distinct values, first seen first, quoted.
    pub values: Vec<String>,
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
    /// How big `largest_change` is (points, or percent for a row count), to rank
    /// segments by; `None` when nothing clear moved.
    pub change_size: Option<f64>,
}

/// One column's measure in a segment, and in the segment it is compared with.
#[derive(Debug, Clone, PartialEq)]
pub struct SegmentChange {
    pub column: String,
    pub metric: QualityMetric,
    pub before: Option<f64>,
    pub now: f64,
    /// The move is past sampling noise (always, on an exact profile) and a point
    /// or more.
    pub clear: bool,
}

impl SegmentChange {
    /// Percentage points moved, when there is something to have moved from.
    pub fn change(&self) -> Option<f64> {
        self.before.map(|before| (self.now - before) * 100.0)
    }
}

/// The order Segments lists its rows in: as they fall, or the clearest change
/// first (ties, and segments with no clear change, keep their order).
pub fn segment_order(results: &DataQualityResults, by_change: bool) -> Vec<usize> {
    let mut order = (0..results.segments.len()).collect::<Vec<_>>();
    if by_change {
        order.sort_by(|&left, &right| {
            let size = |index: usize| results.segments[index].change_size.unwrap_or(-1.0);
            size(right).total_cmp(&size(left))
        });
    }
    order
}

/// Every column's measures in segment `index`: beside the segment it is compared
/// with and largest move first, or on its own worst first. A measure that is zero
/// on both sides says nothing and is left out.
pub fn segment_changes(results: &DataQualityResults, index: usize) -> Vec<SegmentChange> {
    let Some(segment) = results.segments.get(index) else {
        return Vec::new();
    };
    let compared = segment
        .compared_with
        .as_ref()
        .and_then(|label| results.segments.iter().find(|other| &other.label == label));
    let mut changes = Vec::new();
    for column in &segment.columns {
        let prior = compared.and_then(|other| other.columns.iter().find(|c| c.name == column.name));
        for metric in CHANGE_MEASURES {
            let Some(now) = metric.value(column) else {
                continue;
            };
            let before = prior.and_then(|prior| metric.value(prior));
            if now == 0.0 && before.unwrap_or(0.0) == 0.0 {
                continue;
            }
            let clear = match (prior, before) {
                (Some(prior), Some(before)) => {
                    (now - before).abs() * 100.0 >= MATERIAL_CHANGE_PP
                        && (results.precision == QualityPrecision::Exact
                            || beyond_noise(
                                now,
                                metric.denominator(column),
                                before,
                                metric.denominator(prior),
                            ))
                }
                _ => false,
            };
            changes.push(SegmentChange {
                column: column.name.clone(),
                metric,
                before,
                now,
                clear,
            });
        }
    }
    if compared.is_some() {
        // What cleared the noise first, then the rest, each largest first.
        changes.sort_by(|left, right| {
            let size = |change: &SegmentChange| change.change().unwrap_or(0.0).abs();
            right
                .clear
                .cmp(&left.clear)
                .then_with(|| size(right).total_cmp(&size(left)))
        });
    } else {
        changes.sort_by(|left, right| right.now.total_cmp(&left.now));
    }
    changes
}

#[derive(Debug, Clone)]
pub struct TemporalLatencyProfile {
    pub segment: String,
    pub start_role: TemporalRole,
    pub end_role: TemporalRole,
    pub start_column: String,
    pub end_column: String,
    /// Rows in the segment.
    pub evaluated_rows: usize,
    /// Rows with both endpoints present and read: the rows a duration is taken on,
    /// and what negative, zero and threshold counts are out of. Not the rows less
    /// the missing ones, since a row can miss both.
    pub paired_rows: usize,
    pub missing_start: usize,
    pub missing_end: usize,
    /// Text the start column's format did not read; not counted as missing.
    pub unparsed_start: usize,
    pub unparsed_end: usize,
    /// Durations below zero: the end before the start.
    pub negative_count: usize,
    /// Durations of exactly zero: the end at the start.
    pub zero_count: usize,
    pub p50_seconds: Option<i64>,
    pub p90_seconds: Option<i64>,
    pub p95_seconds: Option<i64>,
    pub p99_seconds: Option<i64>,
    pub max_seconds: Option<i64>,
    /// The threshold the breaches were counted against: `duration > threshold`,
    /// strictly, so a duration of exactly the threshold is not a breach.
    pub threshold_seconds: Option<i64>,
    pub above_threshold_count: Option<usize>,
}

impl TemporalLatencyProfile {
    pub fn pair(&self) -> (TemporalRole, TemporalRole) {
        (self.start_role, self.end_role)
    }

    /// `event to received`.
    pub fn label(&self) -> String {
        interval_label(self.pair())
    }

    /// A validity period, valid from to valid to: an end before the start is a
    /// period that is not valid, and no end is a period still open.
    pub fn is_validity(&self) -> bool {
        self.pair() == (TemporalRole::ValidFrom, TemporalRole::ValidTo)
    }

    /// How many rows `fact` counts, and out of how many. `None` for a fact this
    /// interval does not measure: unparsed text with no format, a threshold not set.
    pub fn count(&self, fact: IntervalFact, plan: &DataQualityPlan) -> Option<(usize, usize)> {
        let rows = self.evaluated_rows;
        let paired = self.paired_rows;
        match fact {
            IntervalFact::MissingStart => Some((self.missing_start, rows)),
            IntervalFact::MissingEnd => Some((self.missing_end, rows)),
            IntervalFact::UnparsedStart => plan
                .time_format(&self.start_column)
                .map(|_| (self.unparsed_start, rows)),
            IntervalFact::UnparsedEnd => plan
                .time_format(&self.end_column)
                .map(|_| (self.unparsed_end, rows)),
            IntervalFact::Negative => Some((self.negative_count, paired)),
            IntervalFact::Zero => Some((self.zero_count, paired)),
            IntervalFact::OverThreshold => self.above_threshold_count.map(|count| (count, paired)),
        }
    }

    /// Whether this interval's segment is a value its rows can be found by, rather
    /// than a stretch of rows or a file.
    pub fn segment_opens(&self, plan: &DataQualityPlan) -> bool {
        let grain = plan.interval_grain(&self.start_column, &self.end_column);
        segment_predicate(plan, &grain, &self.segment, None).is_some()
    }

    /// The rows behind `fact` in this interval's segment, as a predicate over the
    /// scope `plan` measured: `None` when a segment cannot be told by its values
    /// (row chunks, files) or the fact is not measured.
    /// `schema` is the data's, where known: with it a partition segment compares in
    /// its column's type.
    pub fn evidence_predicate(
        &self,
        fact: IntervalFact,
        plan: &DataQualityPlan,
        schema: Option<&Schema>,
    ) -> Option<Expr> {
        self.count(fact, plan)?;
        let micros = || interval_micros(plan, &self.start_column, &self.end_column);
        let rows = match fact {
            IntervalFact::MissingStart => col(self.start_column.as_str()).is_null(),
            IntervalFact::MissingEnd => col(self.end_column.as_str()).is_null(),
            IntervalFact::UnparsedStart => plan.time_format(&self.start_column)?.unparsed(),
            IntervalFact::UnparsedEnd => plan.time_format(&self.end_column)?.unparsed(),
            IntervalFact::Negative => micros().lt(lit(0i64)),
            IntervalFact::Zero => micros().eq(lit(0i64)),
            IntervalFact::OverThreshold => {
                micros().gt(lit(self.threshold_seconds?.saturating_mul(1_000_000)))
            }
        };
        let grain = plan.interval_grain(&self.start_column, &self.end_column);
        Some(
            match segment_predicate(plan, &grain, &self.segment, schema)? {
                Some(segment) => segment.and(rows),
                None => rows,
            },
        )
    }
}

/// What an interval's detail counts, each with the rows behind it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntervalFact {
    MissingStart,
    MissingEnd,
    UnparsedStart,
    UnparsedEnd,
    Negative,
    Zero,
    OverThreshold,
}

impl IntervalFact {
    pub const ALL: [Self; 7] = [
        Self::MissingStart,
        Self::MissingEnd,
        Self::UnparsedStart,
        Self::UnparsedEnd,
        Self::Negative,
        Self::Zero,
        Self::OverThreshold,
    ];

    /// The fact as its row in the detail names it. A validity period's missing end
    /// is an open period, and its negative duration one that ends before it starts.
    pub fn label(self, profile: &TemporalLatencyProfile) -> String {
        let validity = profile.is_validity();
        match self {
            Self::MissingStart => "Missing start".to_string(),
            Self::MissingEnd if validity => "Open, no end".to_string(),
            Self::MissingEnd => "Missing end".to_string(),
            Self::UnparsedStart => "Unparsed start".to_string(),
            Self::UnparsedEnd => "Unparsed end".to_string(),
            Self::Negative if validity => "Ends first".to_string(),
            Self::Negative => "Negative".to_string(),
            Self::Zero => "Zero".to_string(),
            Self::OverThreshold => format!(
                "Over {}",
                crate::analysis_modal::threshold_label(profile.threshold_seconds)
            ),
        }
    }

    /// Short words for a list of rows: a view's label.
    pub fn short(self) -> &'static str {
        match self {
            Self::MissingStart => "missing start",
            Self::MissingEnd => "missing end",
            Self::UnparsedStart => "unparsed start",
            Self::UnparsedEnd => "unparsed end",
            Self::Negative => "negative",
            Self::Zero => "zero",
            Self::OverThreshold => "over threshold",
        }
    }
}

/// The rows of the segment labeled `label` under `grain`, as a predicate: `Some(None)`
/// for the whole scope, `None` where a segment is a stretch of rows or a file and not
/// a value to filter on. Read back from the label, which names a partition's value
/// as its segment was keyed and a window's start exactly.
/// Each value of `column` as a segment label writes it, null where it is null. A
/// cast to text writes a float or a datetime differently than the label does, and
/// then the rows a label names would not be found.
fn label_text(column: &str) -> Expr {
    col(column).map(
        |values| {
            let text = (0..values.len())
                .map(|row| {
                    let value = values.get(row)?;
                    Ok((!value.is_null()).then(|| crate::exact::str_value(&value).into_owned()))
                })
                .collect::<PolarsResult<StringChunked>>()?;
            Ok(text.with_name(values.name().clone()).into_column())
        },
        |_, field| Ok(Field::new(field.name().clone(), DataType::String)),
    )
}

/// The rows of a partition segment whose label writes `value`. Where the column's
/// type writes each value one way (text, flags, whole numbers, dates, decimals) and
/// `value` reads back as that very label, a plain comparison, which file statistics
/// can answer; otherwise, such as a float the label rounds, the label of each row.
fn partition_label_predicate(column: &str, value: &str, schema: Option<&Schema>) -> Expr {
    let writes_each_once = |dtype: &&DataType| {
        dtype.is_integer()
            || matches!(
                dtype,
                DataType::String
                    | DataType::Boolean
                    | DataType::Date
                    | DataType::Decimal(..)
                    | DataType::Categorical(..)
                    | DataType::Enum(..)
            )
    };
    let native = schema
        .and_then(|schema| schema.get(column))
        .filter(writes_each_once)
        .and_then(|dtype| crate::typed_value::parse(value, dtype).ok())
        .filter(|scalar| crate::exact::str_value(scalar.value()) == value);
    match native {
        Some(scalar) => col(column).eq(lit(scalar)),
        None => label_text(column).eq(lit(value.to_string())),
    }
}

fn segment_predicate(
    plan: &DataQualityPlan,
    grain: &QualityGrain,
    label: &str,
    schema: Option<&Schema>,
) -> Option<Option<Expr>> {
    match grain {
        QualityGrain::Dataset => Some(None),
        QualityGrain::Partition(column) => {
            let value = label.strip_prefix(&format!("{column}="))?;
            Some(Some(if value == "∅" {
                col(column.as_str()).is_null()
            } else {
                partition_label_predicate(column, value, schema)
            }))
        }
        QualityGrain::TimeWindows { column, every } => {
            let value = plan.time_value(column);
            // The rows in no window: nulls, and dates past the calendar.
            if label == time_window_label(column, every, None) {
                return Some(Some(time_window_start(value, every).is_null()));
            }
            let date = |text: &str| chrono::NaiveDate::parse_from_str(text, "%Y-%m-%d").ok();
            let start = match every.as_str() {
                "1h" => chrono::NaiveDateTime::parse_from_str(label, "%Y-%m-%d %H:%M").ok()?,
                "1d" => date(label)?.and_hms_opt(0, 0, 0)?,
                "1w" => date(label.strip_prefix("week of ")?)?.and_hms_opt(0, 0, 0)?,
                "1mo" => date(&format!("{label}-01"))?.and_hms_opt(0, 0, 0)?,
                _ => return None,
            };
            Some(Some(
                time_window_start(value, every).eq(lit(start.and_utc().timestamp_micros())
                    .cast(DataType::Datetime(TimeUnit::Microseconds, None))),
            ))
        }
        QualityGrain::RowChunks(_) | QualityGrain::File => None,
    }
}
/// Columns that are null the same number of times, and how many rows are null in all
/// of them at once. When the two counts agree, the columns go missing together: one
/// fact about some rows, not one per column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SharedNulls {
    pub columns: Vec<String>,
    pub null_rows: usize,
    pub rows_null_in_all: usize,
}

impl SharedNulls {
    pub fn same_rows(&self) -> bool {
        self.rows_null_in_all == self.null_rows
    }
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
    pub shared_nulls: Vec<SharedNulls>,
    /// How many source files' footers were compared, when the scope has files to
    /// compare. `None` means the checks that compare files could not run.
    pub source_files: Option<usize>,
    /// Rows an equal-per-value sample kept of each value. See [`crate::sampling::PerValue`].
    pub per_value: Option<usize>,
    /// How many of `source_files` had their footers read: fewer on a dataset too
    /// large to read every footer, where the file checks cover only those.
    pub footers_read: Option<usize>,
    /// What the run's reads of the source were seen to do. `None` for results no
    /// watched run produced.
    pub reads: Option<ObservedReads>,
    /// Values behind text findings, from the rows the run kept; empty after a full
    /// scan.
    pub examples: Vec<FindingExamples>,
    /// Segments a sampled run counted rows in but drew none of, in segment order:
    /// the rows are there, the sample did not reach them. Not in `segments`, which
    /// profile only what was read.
    pub unsampled_segments: Vec<UnsampledSegment>,
    /// What the declared column intent found; `None` when nothing was declared.
    pub intent: Option<Box<crate::quality_intent::IntentResults>>,
    /// What the rows were read from, as the run that measured them labeled it.
    pub source: Option<Box<crate::quality_export::SourceIdentity>>,
    /// The report and checks read from the rest, once built.
    pub derived: crate::quality_report::ReportCache,
}

/// A segment the scope has rows in and a sample drew none of.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnsampledSegment {
    pub label: String,
    /// The rows the scope holds in it, by exact count.
    pub total_rows: usize,
}

impl DataQualityResults {
    /// Memory the report holds, near enough to budget by: a profile per column, and
    /// another per column of every segment, which is what grows, and the text it
    /// keeps, whole in spellings and cut short in examples.
    pub fn estimated_bytes(&self) -> usize {
        let profile = |column: &ColumnQualityProfile| {
            std::mem::size_of::<ColumnQualityProfile>()
                + column.name.len()
                + column.min.as_ref().map_or(0, String::len)
                + column.max.as_ref().map_or(0, String::len)
                + column.dominant_value.as_ref().map_or(0, String::len)
        };
        let segments = self
            .segments
            .iter()
            .map(|segment| {
                std::mem::size_of::<SegmentQualityProfile>()
                    + segment.label.len()
                    + segment.columns.iter().map(profile).sum::<usize>()
            })
            .sum::<usize>();
        let observations = self
            .observations
            .iter()
            .map(|observation| {
                std::mem::size_of::<QualityObservation>()
                    + observation.fact.len()
                    + observation.column.len()
                    + observation.normalized_category.as_ref().map_or(0, String::len)
                    // A footer finding names every file it applies to.
                    + observation
                        .files
                        .iter()
                        .map(|file| {
                            std::mem::size_of::<QualityFileEvidence>()
                                + file.name.len()
                                + file.stored_type.as_ref().map_or(0, String::len)
                                + file.examples.iter().map(String::len).sum::<usize>()
                        })
                        .sum::<usize>()
            })
            .sum::<usize>();
        let unsampled = self
            .unsampled_segments
            .iter()
            .map(|segment| std::mem::size_of::<UnsampledSegment>() + segment.label.len())
            .sum::<usize>();
        let texts = |values: &[String]| {
            values
                .iter()
                .map(|value| std::mem::size_of::<String>() + value.len())
                .sum::<usize>()
        };
        // Spellings are whole values, as wide as the column's text is.
        let spellings = self
            .category_variants
            .iter()
            .map(|group| {
                std::mem::size_of::<CategoryVariantGroup>()
                    + group.column.len()
                    + group.normalized.len()
                    + group
                        .variants
                        .iter()
                        .map(|(variant, _)| std::mem::size_of::<(String, usize)>() + variant.len())
                        .sum::<usize>()
            })
            .sum::<usize>();
        let examples = self
            .examples
            .iter()
            .map(|found| std::mem::size_of::<FindingExamples>() + texts(&found.values))
            .sum::<usize>()
            + self.identity.as_ref().map_or(0, |identity| {
                identity
                    .examples
                    .iter()
                    .map(|example| std::mem::size_of::<DuplicateExample>() + texts(&example.values))
                    .sum()
            });
        let temporal = self
            .temporal
            .iter()
            .map(|latency| {
                std::mem::size_of::<TemporalLatencyProfile>()
                    + latency.segment.len()
                    + latency.start_column.len()
                    + latency.end_column.len()
            })
            .sum::<usize>();
        let shared = self
            .shared_nulls
            .iter()
            .map(|shared| std::mem::size_of::<SharedNulls>() + texts(&shared.columns))
            .sum::<usize>();
        // Declared intent keeps whole values: the extremes and the commonest misfits.
        let intent = self.intent.as_ref().map_or(0, |intent| {
            let counted = |values: &[(String, usize)]| {
                values
                    .iter()
                    .map(|(value, _)| std::mem::size_of::<(String, usize)>() + value.len())
                    .sum::<usize>()
            };
            std::mem::size_of::<crate::quality_intent::IntentResults>()
                + intent
                    .columns
                    .iter()
                    .map(|check| {
                        std::mem::size_of::<crate::quality_intent::ColumnCheck>()
                            + check.lowest.as_ref().map_or(0, String::len)
                            + check.highest.as_ref().map_or(0, String::len)
                            + counted(&check.outside_examples)
                            + counted(&check.unparsed_examples)
                    })
                    .sum::<usize>()
        });
        std::mem::size_of::<Self>()
            + self.columns.iter().map(profile).sum::<usize>()
            + segments
            + unsampled
            + observations
            + temporal
            + spellings
            + examples
            + shared
            + intent
    }

    pub fn compare_segments(&mut self, plan: &DataQualityPlan) {
        apply_comparisons(
            &mut self.segments,
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
                .map(|(name, dtype)| ColumnQualityProfile::unmeasured(name, dtype.clone(), 0))
                .collect(),
            observations: Vec::new(),
            segments: Vec::new(),
            temporal: Vec::new(),
            identity: None,
            category_variants: Vec::new(),
            shared_nulls: Vec::new(),
            source_files: None,
            per_value: None,
            footers_read: None,
            reads: None,
            examples: Vec::new(),
            unsampled_segments: Vec::new(),
            intent: None,
            source: None,
            derived: Default::default(),
        }
    }

    /// The kept examples of `kind` in `column`.
    pub fn examples_of(&self, kind: ObservationKind, column: &str) -> &[String] {
        self.examples
            .iter()
            .find(|examples| examples.kind == kind && examples.column == column)
            .map(|examples| examples.values.as_slice())
            .unwrap_or_default()
    }
}

/// The rows a sampled run read, kept beside its results: an acquisition.
///
/// What decides these rows is the acquisition's identity — the dataset, the view, the
/// scope, the method, the size and the seed — which the caller keys it by. Everything
/// else a plan says (grain, comparison, time roles, text read as time, the latency
/// threshold) is the report's, and a run that changes only those cuts these rows
/// again rather than reading the source. Every column of the scope is kept, and where
/// each row sat, so any role, format or row-chunk grain finds what it needs here.
#[derive(Debug, Clone)]
pub struct QualitySample {
    df: DataFrame,
    /// Where each row sat in the scope, in the order of `df`.
    positions: Vec<IdxSize>,
    precision: QualityPrecision,
    total_rows: Option<usize>,
    per_value: Option<crate::sampling::PerValue>,
    /// Rows of the whole scope by segment key, by the grain they were counted for and
    /// the format its column was read through, when it is text read as time. Keyed as
    /// the key reads (`AnyValue::str_value`), `None` for null.
    counted: Vec<(SegmentKey, SegmentCounts)>,
    /// Grains whose count stopped at [`crate::sampling::MAX_COUNTED_KEYS`], so a run
    /// of one again says so rather than reading to find out.
    too_many: Vec<SegmentKey>,
}

/// Rows by segment key, as a count read them.
type SegmentCounts = BTreeMap<Option<String>, usize>;

/// What decides a segment count: the grain, and how its column was read as time.
type SegmentKey = (QualityGrain, Option<TimeInterpretation>);

fn segment_key(plan: &DataQualityPlan) -> SegmentKey {
    let format = match &plan.grain {
        QualityGrain::TimeWindows { column, .. } => plan.time_format(column).cloned(),
        _ => None,
    };
    (plan.grain.clone(), format)
}

/// How a full scan of a remote source gets its rows, as Setup says before Run: one
/// fetch into a local copy that every pass reads, a copy fetched earlier, or a pass
/// over the source for each check.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CopyPlan {
    /// Not a full scan of a remote source read in place.
    #[default]
    NotApplicable,
    /// Every pass reads the source, for the reason given.
    Passes(NoCopy),
    /// The objects are fetched once into the cache directory first.
    Fetch { bytes: u64, objects: usize },
    /// A copy fetched earlier this session serves every pass.
    Kept { bytes: u64, objects: usize },
}

/// Why a remote full scan reads the source in each pass instead of a local copy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NoCopy {
    /// `analysis.quality_local_copy` is 0.
    Off,
    /// The open did not learn every object's size.
    SizeUnknown,
    /// A copy fetched this session did not read as the source.
    Unusable,
    /// The scope reads only some of the rows or columns: its passes may read less
    /// than the whole objects a copy would fetch.
    PartOfTheSource,
    /// Larger than `analysis.quality_local_copy`.
    TooLarge { bytes: u64, limit: u64 },
    /// More than the cache directory has free, or its free space is unknown.
    NoRoom { bytes: u64, free: Option<u64> },
}

/// Where a run's exact segment totals come from, as Setup says before Run.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum SegmentCount {
    /// Nothing to count: the grain's sizes are known (files, row chunks, the whole
    /// scope), the run reads every row, or it reads no values.
    #[default]
    NotNeeded,
    /// An equal-per-value sample by the grain's column counts every value as it reads.
    PerValue,
    /// The pass that reads the sample counts the grain's key as it streams.
    InSamplePass,
    /// Counted by an earlier run of the rows being reused.
    Retained,
    /// Summed from a finer window's count of the same column, which it nests in
    /// exactly. Holds the finer width.
    RolledUp(String),
    /// A read of the grain's column of its own, after the sample.
    CountPass,
    /// The grain had more keys than a count holds: a coarser grain is needed.
    TooMany,
}

impl SegmentCount {
    /// Whether the count reads the source in a pass of its own.
    pub fn reads(&self) -> bool {
        *self == Self::CountPass
    }
}

/// A window width as a cadence: `1d` is daily.
pub fn window_cadence(every: &str) -> &str {
    match every {
        "1h" => "hourly",
        "1d" => "daily",
        "1w" => "weekly",
        "1mo" => "monthly",
        other => other,
    }
}

/// Whether windows of width `fine` nest exactly in windows of `coarse`: every hour in
/// one day, every day in one week (weeks start on Monday) and one month. Windows are
/// cut on the stored clock with no time zone (UTC for a zoned column; see
/// [`time_window_start`]), where no day has 23 or 25 hours, so a sum of the finer
/// counts is the coarser count. A week does not nest in a month.
pub fn window_nests(fine: &str, coarse: &str) -> bool {
    matches!(
        (fine, coarse),
        ("1h", "1d" | "1w" | "1mo") | ("1d", "1w" | "1mo")
    )
}

impl QualitySample {
    /// Whether `plan`'s segments need a count this sample does not hold: a partition
    /// or time-window grain on a sample, counted neither while sampling nor by an
    /// earlier run, nor summed from a finer count.
    pub fn needs_segment_count(&self, plan: &DataQualityPlan) -> bool {
        self.segment_count(plan).reads()
    }

    /// Where a run of `plan` over these rows gets its segment totals.
    pub fn segment_count(&self, plan: &DataQualityPlan) -> SegmentCount {
        if self.precision != QualityPrecision::Sampled || !segments_need_count(plan) {
            return SegmentCount::NotNeeded;
        }
        if per_value_counts(plan, self.per_value.as_ref()) {
            return SegmentCount::PerValue;
        }
        let key = segment_key(plan);
        if self.too_many.contains(&key) {
            return SegmentCount::TooMany;
        }
        if self.counted.iter().any(|(counted, _)| *counted == key) {
            return SegmentCount::Retained;
        }
        match self.finer_count(&key) {
            Some(((QualityGrain::TimeWindows { every, .. }, _), _)) => {
                SegmentCount::RolledUp(every.clone())
            }
            _ => SegmentCount::CountPass,
        }
    }

    /// A count of a finer window of the same column, read the same way, that `key`'s
    /// windows nest in exactly.
    fn finer_count(&self, key: &SegmentKey) -> Option<&(SegmentKey, SegmentCounts)> {
        let (QualityGrain::TimeWindows { column, every }, format) = key else {
            return None;
        };
        self.counted.iter().find(|((grain, counted_format), _)| {
            matches!(
                grain,
                QualityGrain::TimeWindows { column: counted, every: fine }
                    if counted == column && window_nests(fine, every)
            ) && counted_format == format
        })
    }

    /// The rows themselves, as the sample every tool reads.
    pub fn df(&self) -> &DataFrame {
        &self.df
    }

    /// Memory the rows, their positions and their counts hold, near enough to budget
    /// by.
    pub fn estimated_bytes(&self) -> usize {
        let counts = self
            .counted
            .iter()
            .flat_map(|(_, counts)| counts.keys())
            .map(|key| key.as_ref().map_or(0, String::len) + 64)
            .sum::<usize>();
        let per_value = self.per_value.as_ref().map_or(0, |per_value| {
            per_value
                .totals
                .keys()
                .map(|key| key.as_ref().map_or(0, String::len) + 64)
                .sum()
        });
        self.df.estimated_size()
            + self.positions.len() * std::mem::size_of::<IdxSize>()
            + counts
            + per_value
    }

    /// `df`, cut from these rows, described as the sampler described them.
    pub fn analysis_rows(&self, df: DataFrame) -> crate::sampling::AnalysisRows {
        crate::sampling::AnalysisRows {
            sample_size: (self.precision == QualityPrecision::Sampled).then_some(df.height()),
            total_rows: self.total_rows.unwrap_or(df.height()),
            per_value: self.per_value.clone(),
            df,
        }
    }
}

/// Whether a sampled run of `plan` counts its segments' rows: partitions and time
/// windows are counted for exact totals; files and row chunks are known without it.
pub fn segments_need_count(plan: &DataQualityPlan) -> bool {
    matches!(
        plan.grain,
        QualityGrain::Partition(_) | QualityGrain::TimeWindows { .. }
    )
}

/// Whether the pass that samples `plan`'s rows also counts its segments: an
/// equal-per-value sample by the column the grain splits by counts every value as it
/// streams.
pub fn sampler_counts_segments(plan: &DataQualityPlan) -> bool {
    matches!(
        (&plan.grain, &plan.method),
        (
            QualityGrain::Partition(column),
            crate::sampling::SampleMethod::PerPartition { column: sampled },
        ) if column == sampled
    )
}

/// Where a run of `plan` that reads a new sample gets its segment totals.
/// `may_read_blocks` is whether the sample may be seeded runs of one file, which see
/// too few rows to count; the head sees too few as well. Every other sample is one
/// streamed pass over the scope, which counts the grain's key as it goes.
pub fn fresh_segment_count(plan: &DataQualityPlan, may_read_blocks: bool) -> SegmentCount {
    if plan.compute != QualityCompute::Sample || !segments_need_count(plan) {
        return SegmentCount::NotNeeded;
    }
    if sampler_counts_segments(plan) {
        return SegmentCount::PerValue;
    }
    match plan.method {
        crate::sampling::SampleMethod::FirstRows => SegmentCount::CountPass,
        crate::sampling::SampleMethod::Spread if may_read_blocks => SegmentCount::CountPass,
        _ => SegmentCount::InSamplePass,
    }
}

/// The key a partition or time-window grain splits rows by, as both the count and
/// the segments read it.
fn segment_count_key(plan: &DataQualityPlan) -> Option<Expr> {
    match &plan.grain {
        QualityGrain::Partition(column) => Some(col(column.as_str())),
        QualityGrain::TimeWindows { column, every } => {
            Some(time_window_start(plan.time_value(column), every))
        }
        _ => None,
    }
}

/// Whether a sample's own counts are `plan`'s segment totals: the sampler counted
/// them, and the sample kept what it counted.
fn per_value_counts(plan: &DataQualityPlan, per_value: Option<&crate::sampling::PerValue>) -> bool {
    sampler_counts_segments(plan) && per_value.is_some()
}

pub fn compute_data_quality(
    lf: &LazyFrame,
    total_rows: Option<usize>,
    plan: &DataQualityPlan,
    source: Option<&QualitySourceContext>,
    polars_streaming: bool,
) -> Result<DataQualityResults> {
    compute_data_quality_kept(lf, total_rows, plan, source, polars_streaming, None)
        .map(|(results, _)| results)
}

/// [`compute_data_quality`], cutting `kept` instead of reading when it serves the
/// plan, and returning the sample a sampled run read so the next run can do the same.
pub fn compute_data_quality_kept(
    lf: &LazyFrame,
    total_rows: Option<usize>,
    plan: &DataQualityPlan,
    source: Option<&QualitySourceContext>,
    polars_streaming: bool,
    kept: Option<&QualitySample>,
) -> Result<(DataQualityResults, Option<QualitySample>)> {
    let (results, kept) = compute_data_quality_watched(
        lf,
        total_rows,
        plan,
        source,
        polars_streaming,
        kept,
        &QualityWatch::default(),
    );
    results.map(|results| (results, kept))
}

/// [`compute_data_quality_kept`], naming each stage to `watch` as it enters it and
/// stopping between stages, or inside a streamed read, once `watch` is cancelled.
///
/// The sample a sampled run read comes back whether or not the run finished: a run
/// stopped after its read has still paid for it, and the next run can cut it.
pub fn compute_data_quality_watched(
    lf: &LazyFrame,
    total_rows: Option<usize>,
    plan: &DataQualityPlan,
    source: Option<&QualitySourceContext>,
    polars_streaming: bool,
    kept: Option<&QualitySample>,
    watch: &QualityWatch,
) -> (Result<DataQualityResults>, Option<QualitySample>) {
    let mut acquired = None;
    let inputs = QualityInputs {
        lf,
        total_rows,
        plan,
        source,
        // Without the feature `collect_lazy` is one in-memory collect whatever the
        // setting, with no batch boundary for a cancel to stop at (#498).
        polars_streaming: polars_streaming && cfg!(feature = "streaming"),
        watch,
    };
    let results = profile_quality(inputs, kept, &mut acquired);
    (results, acquired)
}

/// What a run is asked to measure, and how it reports.
#[derive(Clone, Copy)]
struct QualityInputs<'a> {
    lf: &'a LazyFrame,
    total_rows: Option<usize>,
    plan: &'a DataQualityPlan,
    source: Option<&'a QualitySourceContext>,
    polars_streaming: bool,
    watch: &'a QualityWatch,
}

/// The run itself. A sampled run's rows go into `acquired` the moment they are
/// read, so they outlive a run that stops after.
fn profile_quality(
    inputs: QualityInputs<'_>,
    kept: Option<&QualitySample>,
    acquired: &mut Option<QualitySample>,
) -> Result<DataQualityResults> {
    let QualityInputs {
        lf,
        total_rows,
        plan,
        source,
        polars_streaming,
        watch,
    } = inputs;
    watch.stage(QualityStage::Preparing, false, false)?;
    let collected_schema = lf.clone().collect_schema()?;
    let schema = visible_schema(&collected_schema, source);
    // What the footers already said: which files have which columns. Free at every
    // compute budget, including the one that reads no values at all.
    if plan.compute == QualityCompute::Metadata {
        watch.stage(QualityStage::Assembling, false, false)?;
        let mut results = DataQualityResults::empty(total_rows, plan, &schema);
        if let Some(source) = source {
            results.observations = drift_observations(source, None, polars_streaming, watch);
        }
        results.source_files = source.map(|source| source.file_names.len());
        results.footers_read = source.map(|source| source.footers_read);
        results.reads = Some(watch.observed());
        results.intent =
            crate::quality_intent::IntentResults::unmeasured(plan, &schema).map(Box::new);
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
    if let QualityGrain::TimeWindows { column, .. } = &plan.grain
        && !plan.reads_as_time(column, &collected_schema)
    {
        return Err(Report::msg(format!(
            "Grain column {column} is text; choose a format for it under Text as time"
        )));
    }
    if plan.compute == QualityCompute::Full {
        let total_rows = match total_rows {
            Some(rows) => rows,
            None => {
                // Unwatched: Parquet and IPC answer a count from their metadata, which
                // a watch between the count and the scan would turn into a read.
                watch.stage(QualityStage::CountingRows, watch.scope_reads(true), false)?;
                let count = collect_lazy(crate::table::row_count_lf(lf), polars_streaming)
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
        if total_rows == 0 && plan.scope != QualityScope::CurrentView {
            return Err(crate::sampling::no_rows_error(&plan.scope));
        }
        return compute_full_quality(
            lf,
            total_rows,
            plan,
            source,
            &schema,
            polars_streaming,
            watch,
        );
    }

    // The shared analysis sampler, as every other tool reads: by default spread
    // across the whole scope, so a file sorted by date is not judged by its first
    // stretch. Every grain cuts its segments from this one sample, so a segmented
    // run reads no more than the sample says and measures the rows every tool reads.
    let kept = acquired.insert(match kept {
        Some(kept) => {
            watch.stage(QualityStage::ReusingSample, false, false)?;
            kept.clone()
        }
        None => {
            // The first rows are one collect; every other method streams in batches
            // or reads seeded runs, and stops between them. Without the streaming
            // engine a streamed sample's batches come after its whole read; seeded
            // runs still stop between runs, sooner than this promises.
            let interruptible = plan.method != crate::sampling::SampleMethod::FirstRows
                && cfg!(feature = "streaming");
            watch.stage(QualityStage::ReadingSample, true, interruptible)?;
            read_quality_sample(lf, total_rows, plan, polars_streaming, watch)?
        }
    });
    let profile_df = kept.df.clone();
    let sample_positions = kept.positions.clone();
    let evaluated_rows = profile_df.height();
    let precision = kept.precision;
    let total_rows = kept.total_rows;

    // Rows chosen by the sample that match nothing are a mistake to name, not an
    // empty report that reads as clean.
    if total_rows == Some(0) && plan.scope != QualityScope::CurrentView {
        return Err(crate::sampling::no_rows_error(&plan.scope));
    }
    let profile_df = attach_source_file(profile_df, source)?;
    watch.stage(QualityStage::ProfilingColumns, false, false)?;
    let mut columns = profile_columns(&profile_df, &schema, polars_streaming)?;
    // The same Polars aggregations a full scan uses, over the rows the sample kept:
    // they scale to any sample the shared form asks for, where a walk over rows did
    // not, and a sample and a scan are measured the same way.
    let profile_lf = profile_df.clone().lazy();
    add_dominance_lazy(&profile_lf, &mut columns, polars_streaming)?;
    // Text read as time and the declared intent are counted over the rows in memory,
    // as the columns were.
    let mut formats = interpretation_exprs(plan, &collected_schema);
    formats.extend(crate::quality_intent::intent_exprs(plan, &schema));
    let unparsed = if formats.is_empty() {
        DataFrame::default()
    } else {
        collect_lazy(profile_lf.clone().select(formats), polars_streaming).map_err(Report::from)?
    };
    watch.stage(QualityStage::CheckingDuplicates, false, false)?;
    let identity = profile_identity_lazy(
        &profile_lf,
        &schema,
        evaluated_rows,
        precision,
        polars_streaming,
    )?;
    // The declared key's repeats among the rows in memory: a repeat among distinct
    // sampled rows is a repeat in the data, and no repeat says nothing past them.
    let repeats = crate::quality_intent::key_repeats(&profile_lf, plan, &schema, polars_streaming)?;
    let intent = crate::quality_intent::IntentResults::from_counts(
        plan,
        &schema,
        &unparsed,
        repeats,
        evaluated_rows,
        precision,
        Some(&profile_lf),
    )?
    .map(Box::new);
    watch.stage(QualityStage::CheckingSpellings, false, false)?;
    let category_variants = profile_category_variants_lazy(&profile_lf, &schema, polars_streaming)?;
    let mut observations = observations_from_profiles(&columns, precision);
    observations.extend(interpretation_observations(
        &unparsed,
        plan,
        &collected_schema,
    ));
    observations.extend(identity_observations(&identity, &category_variants));
    if let Some(intent) = &intent {
        observations.extend(intent.observations());
    }
    crate::quality_intent::supersede(&mut observations, plan);
    // The rows are in memory, so the detail can show a few of the values behind a
    // finding without reading anything again.
    let mut identity = identity;
    if identity.duplicate_groups > 0 {
        identity.examples = duplicate_examples(&profile_lf, &schema, polars_streaming)?;
    }
    let examples = finding_examples(&profile_lf, &columns, &observations, polars_streaming)?;
    // A sampled run does not promise the extra reads, so the counts come without the
    // values behind them.
    if let Some(source) = source {
        observations.extend(drift_observations(source, None, polars_streaming, watch));
    }
    let totals = {
        let mut totals = known_segment_totals(plan, total_rows, source);
        if precision == QualityPrecision::Sampled {
            totals.extend(sampled_segment_totals(
                lf,
                plan,
                kept,
                polars_streaming,
                watch,
            )?);
        }
        totals
    };
    watch.stage(QualityStage::ProfilingSegments, false, false)?;
    let (segments, unsampled_segments) = profile_segments(
        &profile_df,
        total_rows,
        plan,
        precision,
        &schema,
        SegmentSampleProvenance {
            positions: Some(sample_positions.as_slice()),
            totals: &totals,
        },
        polars_streaming,
    )?;
    watch.stage(QualityStage::ComputingIntervals, false, false)?;
    let temporal = profile_temporal(&profile_df, plan, Some(sample_positions.as_slice()))?;
    watch.stage(QualityStage::CheckingSharedNulls, false, false)?;
    let shared_nulls = profile_shared_nulls(&profile_df.lazy(), &columns, polars_streaming)?;
    let per_value = kept.per_value.as_ref().map(|per_value| per_value.kept);
    watch.stage(QualityStage::Assembling, false, false)?;

    let results = DataQualityResults {
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
        shared_nulls,
        source_files: source.map(|source| source.file_names.len()),
        per_value,
        footers_read: source.map(|source| source.footers_read),
        reads: Some(watch.observed()),
        examples,
        unsampled_segments,
        intent,
        source: None,
        derived: Default::default(),
    };
    Ok(results)
}

/// Read the rows a sampled run measures, counting the grain's segments in the same
/// pass when the sampler streams every row.
fn read_quality_sample(
    lf: &LazyFrame,
    total_rows: Option<usize>,
    plan: &DataQualityPlan,
    polars_streaming: bool,
    watch: &QualityWatch,
) -> Result<QualitySample> {
    let sample = crate::sampling::Sample {
        scope: QualityScope::CurrentView,
        method: plan.method.clone(),
        rows: plan.dataset_rows,
        seed: plan.sample_seed,
    };
    let count = if sampler_counts_segments(plan) {
        None
    } else {
        segment_count_key(plan)
    };
    let sampled = crate::sampling::acquire(
        lf,
        &sample,
        total_rows,
        polars_streaming,
        Some(watch.read()),
        count.as_ref(),
    )?;
    let precision = if sampled.rows.sample_size.is_some() {
        QualityPrecision::Sampled
    } else {
        QualityPrecision::Exact
    };
    let mut kept = QualitySample {
        df: sampled.rows.df,
        positions: sampled.positions,
        precision,
        total_rows: Some(sampled.rows.total_rows),
        per_value: sampled.rows.per_value,
        counted: Vec::new(),
        too_many: Vec::new(),
    };
    match sampled.counted {
        Some(crate::sampling::Counted::Totals(totals)) => {
            kept.counted.push((segment_key(plan), totals));
        }
        Some(crate::sampling::Counted::TooMany) => kept.too_many.push(segment_key(plan)),
        None => {}
    }
    Ok(kept)
}

/// How many rows each segment of a sampled run holds, reading only what nothing has
/// counted yet.
///
/// An equal-per-value sample counted every value as it streamed, so a grain by the
/// same column is already counted, and a streamed sample counted the grain it was read
/// for. A coarser window is summed from a finer window's count when it nests in it
/// exactly. Anything else is counted by a read of its key, once: the count is kept with
/// the sample for the next run.
fn sampled_segment_totals(
    lf: &LazyFrame,
    plan: &DataQualityPlan,
    kept: &mut QualitySample,
    polars_streaming: bool,
    watch: &QualityWatch,
) -> Result<BTreeMap<String, usize>> {
    if !segments_need_count(plan) {
        return Ok(BTreeMap::new());
    }
    let labeled = |counts: &SegmentCounts| {
        counts
            .iter()
            .map(|(raw, rows)| (segment_label(&plan.grain, raw.as_deref()), *rows))
            .collect::<BTreeMap<_, _>>()
    };
    if per_value_counts(plan, kept.per_value.as_ref())
        && let Some(per_value) = &kept.per_value
    {
        return Ok(labeled(&per_value.totals));
    }
    let key = segment_key(plan);
    if kept.too_many.contains(&key) {
        return Err(too_many_segments(plan));
    }
    if let Some((_, counts)) = kept.counted.iter().find(|(counted, _)| *counted == key) {
        return Ok(labeled(counts));
    }
    if let Some((_, finer)) = kept.finer_count(&key)
        && let QualityGrain::TimeWindows { every, .. } = &plan.grain
    {
        let counts = roll_up_windows(finer, every)?;
        let totals = labeled(&counts);
        kept.counted.push((key, counts));
        return Ok(totals);
    }
    watch.stage(QualityStage::CountingSegments, true, polars_streaming)?;
    let counts = counted_segment_totals(&watch.watched(lf), plan, polars_streaming)
        .map_err(|error| watch.failed(error))?;
    if counts.len() > crate::sampling::MAX_COUNTED_KEYS {
        kept.too_many.push(key);
        return Err(too_many_segments(plan));
    }
    let totals = labeled(&counts);
    kept.counted.push((key, counts));
    Ok(totals)
}

fn too_many_segments(plan: &DataQualityPlan) -> Report {
    Report::msg(format!(
        "More than {} segments {}; choose a coarser grain",
        crate::numfmt::group_chrome(crate::sampling::MAX_COUNTED_KEYS),
        plan.grain.label()
    ))
}

/// A finer window's counts summed into `every`'s windows, through the expression
/// that cuts every window, so the sum lands where a count by `every` would. Exact only
/// where [`window_nests`] says so; the caller asks it first.
fn roll_up_windows(finer: &SegmentCounts, every: &str) -> Result<SegmentCounts> {
    let mut rolled = SegmentCounts::new();
    let mut starts = Vec::with_capacity(finer.len());
    let mut rows = Vec::with_capacity(finer.len());
    for (raw, count) in finer {
        match raw {
            Some(raw) => {
                let start = chrono::NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S%.f")
                    .map_err(|_| Report::msg(format!("Window start {raw:?} is not a time")))?;
                starts.push(start.and_utc().timestamp_micros());
                rows.push(*count as u64);
            }
            // A row with no time is in no window at any width.
            None => *rolled.entry(None).or_default() += count,
        }
    }
    let finer = DataFrame::new(
        starts.len(),
        vec![
            Column::new("start".into(), starts)
                .cast(&DataType::Datetime(TimeUnit::Microseconds, None))?,
            Column::new("rows".into(), rows),
        ],
    )?;
    let coarse = finer
        .lazy()
        .select([time_window_start(col("start"), every), col("rows")])
        .collect()?;
    let (starts, rows) = (coarse.column("start")?, coarse.column("rows")?.u64()?);
    for (row, count) in rows.into_no_null_iter().enumerate() {
        let start = starts.get(row)?;
        let key = (!start.is_null()).then(|| crate::exact::str_value(&start).into_owned());
        *rolled.entry(key).or_default() += count as usize;
    }
    Ok(rolled)
}

fn compute_full_quality(
    lf: &LazyFrame,
    total_rows: usize,
    plan: &DataQualityPlan,
    source: Option<&QualitySourceContext>,
    schema: &Schema,
    polars_streaming: bool,
    watch: &QualityWatch,
) -> Result<DataQualityResults> {
    let full_schema = lf.clone().collect_schema()?;
    // Every pass reads through the watch, so a cancel stops it within a batch on the
    // streaming engine, and the rows each pass traverses are counted.
    let lf = &watch.watched(lf);
    let failed = |error: Report| watch.failed(error);
    watch.stage(
        QualityStage::ProfilingColumns,
        watch.scope_reads(true),
        polars_streaming,
    )?;
    // Text read as time is counted in the same pass as every column's profile.
    let mut exprs = build_profile_exprs(schema);
    exprs.extend(interpretation_exprs(plan, &full_schema));
    // The declared intent's counts too: sums over the same rows, in the same pass.
    exprs.extend(crate::quality_intent::intent_exprs(plan, schema));
    let aggregate = collect_lazy(lf.clone().select(exprs), polars_streaming)
        .map_err(|error| watch.failed(error))?;
    let mut columns = parse_profiles(&aggregate, schema, total_rows);
    add_dominance_lazy(lf, &mut columns, polars_streaming).map_err(failed)?;
    watch.stage(
        QualityStage::CheckingDuplicates,
        watch.scope_reads(true),
        polars_streaming,
    )?;
    let identity = profile_identity_lazy(
        lf,
        schema,
        total_rows,
        QualityPrecision::Exact,
        polars_streaming,
    )
    .map_err(failed)?;
    let texts = schema
        .iter_values()
        .any(|dtype| matches!(dtype, DataType::String | DataType::Categorical(..)));
    watch.stage(
        QualityStage::CheckingSpellings,
        watch.scope_reads(texts),
        polars_streaming,
    )?;
    let category_variants =
        profile_category_variants_lazy(lf, schema, polars_streaming).map_err(failed)?;
    // The declared key is one grouping of its columns: a pass of its own, which
    // Setup counts among the passes before Run.
    let keyed = !plan.intent.key.is_empty();
    watch.stage(
        QualityStage::CheckingKey,
        watch.scope_reads(keyed),
        polars_streaming,
    )?;
    let repeats =
        crate::quality_intent::key_repeats(lf, plan, schema, polars_streaming).map_err(failed)?;
    let intent = crate::quality_intent::IntentResults::from_counts(
        plan,
        schema,
        &aggregate,
        repeats,
        total_rows,
        QualityPrecision::Exact,
        None,
    )?
    .map(Box::new);
    let mut observations = observations_from_profiles(&columns, QualityPrecision::Exact);
    observations.extend(interpretation_observations(&aggregate, plan, &full_schema));
    observations.extend(identity_observations(&identity, &category_variants));
    if let Some(intent) = &intent {
        observations.extend(intent.observations());
    }
    crate::quality_intent::supersede(&mut observations, plan);
    // Only a run that already reads every value pays for the conflicting values, and
    // only that run's access plan promised the read. Each file is read on its own,
    // so a cancel stops them between files.
    if let Some(source) = source {
        if source.conflict_scan.is_some() {
            watch.stage(QualityStage::ReadingConflicts, true, true)?;
        }
        observations.extend(drift_observations(
            source,
            source.conflict_scan.as_ref(),
            polars_streaming,
            watch,
        ));
    }
    let whole = unsegmented(plan, source);
    watch.stage(
        QualityStage::ProfilingSegments,
        watch.scope_reads(!whole),
        polars_streaming,
    )?;
    let segments = if whole {
        // The whole scope is one segment, and its profile is the one just measured:
        // reading it again would be a second pass for the same numbers.
        vec![whole_segment(plan, total_rows, &columns, schema.len())]
    } else {
        profile_segments_lazy(lf, total_rows, plan, source, schema, polars_streaming)
            .map_err(failed)?
    };
    let intervals = !resolved_intervals(plan, &full_schema).is_empty();
    watch.stage(
        QualityStage::ComputingIntervals,
        watch.scope_reads(intervals),
        polars_streaming,
    )?;
    let temporal = profile_temporal_lazy(lf, plan, source, polars_streaming).map_err(failed)?;
    let shared = !shared_null_groups(&columns).is_empty();
    watch.stage(
        QualityStage::CheckingSharedNulls,
        watch.scope_reads(shared),
        polars_streaming,
    )?;
    let shared_nulls = profile_shared_nulls(lf, &columns, polars_streaming).map_err(failed)?;
    watch.stage(QualityStage::Assembling, false, false)?;
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
        shared_nulls,
        source_files: source.map(|source| source.file_names.len()),
        per_value: None,
        footers_read: source.map(|source| source.footers_read),
        reads: Some(watch.observed()),
        examples: Vec::new(),
        unsampled_segments: Vec::new(),
        intent,
        source: None,
        derived: Default::default(),
    })
}

/// Columns sharing a nonzero null count, by the count: the sets worth checking for
/// rows null in all of them.
fn shared_null_groups(columns: &[ColumnQualityProfile]) -> Vec<(usize, Vec<String>)> {
    let mut by_count = BTreeMap::<usize, Vec<String>>::new();
    for profile in columns.iter().filter(|profile| profile.null_count > 0) {
        by_count
            .entry(profile.null_count)
            .or_default()
            .push(profile.name.clone());
    }
    by_count
        .into_iter()
        .filter(|(_, names)| names.len() > 1)
        .collect()
}

/// For every set of two or more columns with the same nonzero null count, how many
/// rows are null in all of them.
///
/// Equal counts are only a hint; this is the check. It reads just those columns, once,
/// and is skipped entirely when no two columns share a count.
fn profile_shared_nulls(
    lf: &LazyFrame,
    columns: &[ColumnQualityProfile],
    polars_streaming: bool,
) -> Result<Vec<SharedNulls>> {
    let groups = shared_null_groups(columns);
    if groups.is_empty() {
        return Ok(Vec::new());
    }
    let exprs = groups
        .iter()
        .enumerate()
        .map(|(index, (_, names))| {
            names
                .iter()
                .map(|name| col(name.as_str()).is_null())
                .reduce(Expr::and)
                .expect("a group has two columns")
                .sum()
                .alias(format!("__quality_shared_null_{index}"))
        })
        .collect::<Vec<_>>();
    let counts = collect_lazy(lf.clone().select(exprs), polars_streaming).map_err(Report::from)?;
    Ok(groups
        .into_iter()
        .enumerate()
        .map(|(index, (null_rows, columns))| SharedNulls {
            columns,
            null_rows,
            rows_null_in_all: usize_value(&counts, &format!("__quality_shared_null_{index}")),
        })
        .collect())
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
            .map(|value| crate::exact::str_value(&value).to_string());
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
    precision: QualityPrecision,
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
        precision,
        examples: Vec::new(),
    })
}

const DUPLICATE_COPIES: &str = "__datui_quality_copies";

/// Groups of rows identical in every one of `keys`, with how many copies each has,
/// most copies first and then first seen first: the grouping the duplicate check
/// counts with.
fn duplicate_groups(lf: LazyFrame, keys: &[PlSmallStr]) -> LazyFrame {
    lf.group_by_stable(keys.iter().map(|key| col(key.clone())).collect::<Vec<_>>())
        .agg([len().alias(DUPLICATE_COPIES)])
        .filter(col(DUPLICATE_COPIES).gt(lit(1u32)))
        .sort(
            [DUPLICATE_COPIES],
            SortMultipleOptions::default()
                .with_order_descending(true)
                .with_maintain_order(true),
        )
}

/// The rows the duplicate check counted: every row equal to another in every one of
/// `keys`, copies together, most copies first. One pass, grouping as the check did.
///
/// Copies are equal in every key, so each group's key is its rows: it is repeated as
/// many times as it occurs rather than looked up in a second read.
pub fn duplicate_rows(
    lf: LazyFrame,
    keys: &[PlSmallStr],
    polars_streaming: bool,
) -> Result<DataFrame> {
    let groups =
        collect_lazy(duplicate_groups(lf, keys), polars_streaming).map_err(Report::from)?;
    let copies = groups
        .column(DUPLICATE_COPIES)?
        .cast(&DataType::UInt64)?
        .u64()?
        .into_no_null_iter()
        .collect::<Vec<_>>();
    let mut take = Vec::with_capacity(copies.iter().sum::<u64>() as usize);
    for (group, copies) in copies.into_iter().enumerate() {
        take.extend(std::iter::repeat_n(group as IdxSize, copies as usize));
    }
    let rows = groups.drop(DUPLICATE_COPIES)?;
    Ok(rows.take(&IdxCa::from_vec(PlSmallStr::EMPTY, take))?)
}

/// The most copied groups of rows kept in memory, rendered for the detail.
fn duplicate_examples(
    lf: &LazyFrame,
    schema: &Schema,
    polars_streaming: bool,
) -> Result<Vec<DuplicateExample>> {
    let keys = schema.iter_names().cloned().collect::<Vec<_>>();
    let groups = collect_lazy(
        duplicate_groups(lf.clone(), &keys).limit(MAX_FINDING_EXAMPLES as IdxSize),
        polars_streaming,
    )
    .map_err(Report::from)?;
    Ok((0..groups.height())
        .map(|row| DuplicateExample {
            copies: usize_value_at(&groups, DUPLICATE_COPIES, row),
            values: keys
                .iter()
                .map(|key| {
                    groups
                        .column(key)
                        .and_then(|column| column.get(row))
                        .map(|value| example_text(&value))
                        .unwrap_or_else(|_| "null".to_string())
                })
                .collect(),
        })
        .collect())
}

/// A value as the detail shows it: text quoted and cut, a null named.
fn example_text(value: &AnyValue<'_>) -> String {
    match value {
        AnyValue::Null => "null".to_string(),
        AnyValue::String(text) => crate::quality_report::quoted(text, 24),
        AnyValue::StringOwned(text) => crate::quality_report::quoted(text, 24),
        other => {
            let text = crate::exact::str_value(other).to_string();
            if crate::glyphs::display_width(&text) > 24 {
                format!(
                    "{}{}",
                    crate::glyphs::take_columns(&text, 23),
                    crate::glyphs::get().ellipsis
                )
            } else {
                text
            }
        }
    }
}

/// A few distinct values behind each text finding a sample can show: text its
/// reading does not parse, and text its time format does not read.
fn finding_examples(
    lf: &LazyFrame,
    columns: &[ColumnQualityProfile],
    observations: &[QualityObservation],
    polars_streaming: bool,
) -> Result<Vec<FindingExamples>> {
    let mut examples = Vec::new();
    for observation in observations {
        let profile = columns
            .iter()
            .find(|profile| profile.name == observation.column);
        let failed = match observation.kind {
            ObservationKind::ParseableText => profile.and_then(unparsed_text),
            ObservationKind::UnparsedTime => observation
                .time_format
                .as_ref()
                .map(TimeInterpretation::unparsed),
            _ => None,
        };
        let (Some(failed), Some(profile)) = (failed, profile) else {
            continue;
        };
        // The first failures, told apart here: a few hundred bound the work, and a
        // value repeated that often is the example anyway.
        let values = text_expr(col(observation.column.as_str()), &profile.dtype)
            .filter(failed)
            .head(Some(256))
            .alias("values");
        let found =
            collect_lazy(lf.clone().select([values]), polars_streaming).map_err(Report::from)?;
        let mut values = Vec::new();
        for value in (0..found.height()).filter_map(|row| string_value_at(&found, "values", row)) {
            let value = crate::quality_report::quoted(&value, 24);
            if !values.contains(&value) {
                values.push(value);
            }
            if values.len() == MAX_FINDING_EXAMPLES {
                break;
            }
        }
        if !values.is_empty() {
            examples.push(FindingExamples {
                kind: observation.kind,
                column: observation.column.clone(),
                values,
            });
        }
    }
    Ok(examples)
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
            fact: String::new(),
            normalized_category: None,
            files: Vec::new(),
            time_format: None,
            full_scale: None,
        });
    }
    observations.extend(variants.iter().map(|group| QualityObservation {
        kind: ObservationKind::CategoryVariants,
        column: group.column.clone(),
        affected_rows: group.rows_involved,
        evaluated_rows: identity.evaluated_rows,
        fact: String::new(),
        normalized_category: Some(group.normalized.clone()),
        files: Vec::new(),
        time_format: None,
        full_scale: None,
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
    sample_positions: Option<&[IdxSize]>,
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
                    .unwrap_or(row as IdxSize) as usize;
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
        QualityGrain::Partition(column) => group_by_value(df, column, &format!("{column}="))?,
        QualityGrain::TimeWindows { column, every } => {
            group_by_time_window(df, plan, column, every)?
        }
        QualityGrain::File => {
            if df.column(QUALITY_SOURCE_FILE_COLUMN).is_ok() {
                group_by_value(df, QUALITY_SOURCE_FILE_COLUMN, "file ")?
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

fn group_by_value(df: &DataFrame, column: &str, prefix: &str) -> Result<Vec<SegmentRows>> {
    let values = df.column(column)?;
    let mut groups: BTreeMap<String, Vec<u32>> = BTreeMap::new();
    let mut missing = Vec::new();
    for row in 0..df.height() {
        let value = values.get(row)?;
        if value.is_null() {
            missing.push(row as u32);
        } else {
            groups
                .entry(format!("{prefix}{}", crate::exact::str_value(&value)))
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
            label: format!("{prefix}∅"),
            indices: missing,
        });
    }
    Ok(result)
}

/// Where a row's window starts. Both the sampled and the full-scan path bucket
/// through this one expression, so a week never starts on a different day
/// depending on how much of it was read. A date past the calendar's range falls
/// in no window, as a null does: truncating it overflows.
fn time_window_start(value: Expr, every: &str) -> Expr {
    value
        .map(
            |c| {
                Ok(
                    crate::exact::calendar_without_out_of_range(c.as_materialized_series())?
                        .map_or(c, Column::from),
                )
            },
            |_, field| Ok(field.clone()),
        )
        .cast(DataType::Datetime(TimeUnit::Microseconds, None))
        .dt()
        .truncate(lit(every.to_string()))
}

fn group_by_time_window(
    df: &DataFrame,
    plan: &DataQualityPlan,
    column: &str,
    every: &str,
) -> Result<Vec<SegmentRows>> {
    let starts = df
        .clone()
        .lazy()
        .select([time_window_start(plan.time_value(column), every).alias(QUALITY_WINDOW_START)])
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
                .entry(crate::exact::str_value(&value).into_owned())
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

/// A window by where it starts, to the precision its width needs: an hour to the
/// minute, a day as its date, a week as the date it starts, a month as the month.
pub(crate) fn time_window_label(column: &str, every: &str, start: Option<&str>) -> String {
    let Some(start) = start else {
        return format!("{column} ∅");
    };
    let prefix = |length: usize| start.get(..length).unwrap_or(start).to_string();
    match every {
        "1h" => prefix(16),
        "1d" => prefix(10),
        "1w" => format!("week of {}", prefix(10)),
        "1mo" => prefix(7),
        _ => format!("{start} / {every}"),
    }
}

fn value_epoch_micros(value: AnyValue<'_>) -> Option<i64> {
    // A one-row segment's column can be a scalar, whose values come back owned.
    match value.as_borrowed() {
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
    positions: Option<&'a [IdxSize]>,
    totals: &'a BTreeMap<String, usize>,
}

/// Segment sizes known without reading them: a file's rows from its footer, when
/// the scope holds whole files, and a row chunk's from the scope's size. Others are
/// unknown on a sample, and are left unknown rather than estimated.
///
/// Partitions and time windows are counted instead: a grouped count reads only
/// the grain's column, a small read beside the sample's, and a day whose rows fell
/// by half is the first thing a daily check is for.
fn counted_segment_totals(
    lf: &LazyFrame,
    plan: &DataQualityPlan,
    polars_streaming: bool,
) -> Result<SegmentCounts> {
    const KEY: &str = "__quality_count_key";
    const ROWS: &str = "__quality_count_rows";
    let Some(key) = segment_count_key(plan) else {
        return Ok(SegmentCounts::new());
    };
    let counts = collect_lazy(
        lf.clone()
            .select([key.alias(KEY)])
            .group_by([col(KEY)])
            .agg([len().alias(ROWS)]),
        polars_streaming,
    )
    .map_err(Report::from)?;
    let keys = counts.column(KEY)?;
    let mut totals = BTreeMap::new();
    for row in 0..counts.height() {
        let raw = keys.get(row)?;
        // Keyed as the key reads, as a streamed count keys it: named as a segment only
        // when a run asks, so a finer window's count can be summed into a coarser one.
        let raw = (!raw.is_null()).then(|| crate::exact::str_value(&raw).into_owned());
        totals.insert(raw, usize_value_at(&counts, ROWS, row));
    }
    Ok(totals)
}

fn known_segment_totals(
    plan: &DataQualityPlan,
    total_rows: Option<usize>,
    source: Option<&QualitySourceContext>,
) -> BTreeMap<String, usize> {
    let mut totals = BTreeMap::new();
    match &plan.grain {
        QualityGrain::File => {
            let Some(source) = source else {
                return totals;
            };
            let whole_files = matches!(plan.scope, QualityScope::SourceFiles(_))
                || total_rows == Some(source.dataset_rows);
            if whole_files {
                for (index, name) in source.file_names.iter().enumerate() {
                    totals.insert(format!("file {name}"), source.file_rows(index));
                }
            }
        }
        QualityGrain::RowChunks(size) => {
            let (Some(total), size) = (total_rows, (*size).max(1)) else {
                return totals;
            };
            for chunk in 0..total.div_ceil(size) {
                let start = chunk * size;
                totals.insert(
                    format!("rows {}-{}", start + 1, (chunk + 1).saturating_mul(size)),
                    size.min(total - start),
                );
            }
        }
        _ => {}
    }
    totals
}

fn profile_segments(
    df: &DataFrame,
    total_rows: Option<usize>,
    plan: &DataQualityPlan,
    precision: QualityPrecision,
    schema: &Schema,
    sample: SegmentSampleProvenance<'_>,
    polars_streaming: bool,
) -> Result<(Vec<SegmentQualityProfile>, Vec<UnsampledSegment>)> {
    let groups = segment_rows(df, plan, sample.positions)?;
    // Every segment in one grouped query, keyed by the segment each row fell in.
    // A query per segment is thousands of them for a daily grain over years, and
    // each pays Polars' planning cost for a few dozen rows.
    let mut segment_of = vec![0u32; df.height()];
    for (index, group) in groups.iter().enumerate() {
        for row in &group.indices {
            segment_of[*row as usize] = index as u32;
        }
    }
    const SEGMENT: &str = "__quality_segment_index";
    let mut keyed = df.clone();
    keyed.with_column(Column::new(SEGMENT.into(), segment_of))?;
    let grouped = collect_lazy(
        keyed
            .lazy()
            .group_by([col(SEGMENT)])
            .agg(build_profile_exprs(schema)),
        polars_streaming,
    )
    .map_err(Report::from)?;
    let mut by_segment = vec![None; groups.len()];
    for row in 0..grouped.height() {
        let index = usize_value_at(&grouped, SEGMENT, row);
        if let Some(slot) = by_segment.get_mut(index) {
            *slot = Some(row);
        }
    }
    let mut profiles = Vec::with_capacity(groups.len());
    for (group, row) in groups.into_iter().zip(by_segment) {
        let evaluated_rows = group.indices.len();
        let Some(row) = row else {
            continue;
        };
        let columns = parse_profiles_at(&grouped, schema, evaluated_rows, row);
        let null_cells = columns
            .iter()
            .map(|column| column.null_count)
            .sum::<usize>();
        let denominator = evaluated_rows.saturating_mul(columns.len());
        let known_segment_rows = sample.totals.get(&group.label);
        profiles.push(SegmentQualityProfile {
            label: group.label,
            total_rows: if let Some(total) = known_segment_rows {
                Some(*total)
            } else if matches!(plan.grain, QualityGrain::Dataset) {
                total_rows
            } else if precision == QualityPrecision::Exact {
                Some(evaluated_rows)
            } else {
                None
            },
            evaluated_rows,
            columns,
            null_cells,
            null_rate: rate(null_cells, denominator),
            compared_with: None,
            largest_change: None,
            change_size: None,
        });
    }
    order_segments(&mut profiles);
    apply_comparisons(
        &mut profiles,
        plan.comparison,
        plan.baseline_segment.as_deref(),
        precision,
    );
    // What the count found and the sample did not: kept apart, so a segment with
    // rows the sample missed is never read as one with none.
    let drawn = profiles
        .iter()
        .map(|profile| profile.label.as_str())
        .collect::<std::collections::HashSet<_>>();
    let mut unsampled = sample
        .totals
        .iter()
        .filter(|(label, rows)| **rows > 0 && !drawn.contains(label.as_str()))
        .map(|(label, rows)| UnsampledSegment {
            label: label.clone(),
            total_rows: *rows,
        })
        .collect::<Vec<_>>();
    unsampled.sort_by(|left, right| segment_cmp(&left.label, &right.label));
    Ok((profiles, unsampled))
}

/// Segments in the order their names count: year=9 before year=10, part-2 before
/// part-10, and the rows no segment could place (`∅`) last. "Previous" means the
/// segment before in this order, so it has to be the order a person would read.
fn order_segments(segments: &mut [SegmentQualityProfile]) {
    segments.sort_by(|left, right| segment_cmp(&left.label, &right.label));
}

/// The order of two segments by their labels, as [`order_segments`] puts them.
pub(crate) fn segment_cmp(left: &str, right: &str) -> std::cmp::Ordering {
    left.ends_with('∅')
        .cmp(&right.ends_with('∅'))
        .then_with(|| natural_cmp(left, right))
}

/// Text compared with its runs of digits compared as numbers.
fn natural_cmp(left: &str, right: &str) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    let (mut left, mut right) = (left, right);
    loop {
        let (Some(l), Some(r)) = (left.chars().next(), right.chars().next()) else {
            return left.len().cmp(&right.len());
        };
        if l.is_ascii_digit() && r.is_ascii_digit() {
            let digits = |text: &str| {
                text.find(|c: char| !c.is_ascii_digit())
                    .unwrap_or(text.len())
            };
            let (l_end, r_end) = (digits(left), digits(right));
            let (l_num, r_num) = (
                left[..l_end].trim_start_matches('0'),
                right[..r_end].trim_start_matches('0'),
            );
            let order = l_num.len().cmp(&r_num.len()).then_with(|| l_num.cmp(r_num));
            if order != Ordering::Equal {
                return order;
            }
            left = &left[l_end..];
            right = &right[r_end..];
        } else {
            if l != r {
                return l.cmp(&r);
            }
            left = &left[l.len_utf8()..];
            right = &right[r.len_utf8()..];
        }
    }
}

fn profile_segments_lazy(
    lf: &LazyFrame,
    total_rows: usize,
    plan: &DataQualityPlan,
    source: Option<&QualitySourceContext>,
    schema: &Schema,
    polars_streaming: bool,
) -> Result<Vec<SegmentQualityProfile>> {
    if unsegmented(plan, source) {
        let aggregate = collect_lazy(
            lf.clone().select(build_profile_exprs(schema)),
            polars_streaming,
        )
        .map_err(Report::from)?;
        let columns = parse_profiles(&aggregate, schema, total_rows);
        return Ok(vec![whole_segment(
            plan,
            total_rows,
            &columns,
            schema.len(),
        )]);
    }

    let (grouped_lf, group) = grouped_frame(lf, plan, source)?;
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
            change_size: None,
        });
    }
    // Rows the grain could not place carry no order, so they follow the ones it could.
    let mut ordered = unassigned.into_iter().zip(segments).collect::<Vec<_>>();
    ordered.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| natural_cmp(&left.1.label, &right.1.label))
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
        plan.comparison,
        plan.baseline_segment.as_deref(),
        QualityPrecision::Exact,
    );
    Ok(segments)
}

/// Whether a full run's grain leaves the scope whole: the dataset grain, or files
/// where the view has lost which file a row came from.
fn unsegmented(plan: &DataQualityPlan, source: Option<&QualitySourceContext>) -> bool {
    matches!(plan.grain, QualityGrain::Dataset)
        || matches!(plan.grain, QualityGrain::File) && source.is_none()
}

/// The scope as its one segment, from its columns' profile.
fn whole_segment(
    plan: &DataQualityPlan,
    total_rows: usize,
    columns: &[ColumnQualityProfile],
    column_count: usize,
) -> SegmentQualityProfile {
    let null_cells = columns
        .iter()
        .map(|column| column.null_count)
        .sum::<usize>();
    let denominator = total_rows.saturating_mul(column_count);
    SegmentQualityProfile {
        label: if matches!(plan.grain, QualityGrain::File) {
            "file mapping unavailable for this view".to_string()
        } else {
            "current view".to_string()
        },
        total_rows: Some(total_rows),
        evaluated_rows: total_rows,
        columns: columns.to_vec(),
        null_cells,
        null_rate: rate(null_cells, denominator),
        compared_with: None,
        largest_change: None,
        change_size: None,
    }
}

fn grouped_frame(
    lf: &LazyFrame,
    plan: &DataQualityPlan,
    source: Option<&QualitySourceContext>,
) -> Result<(LazyFrame, Expr)> {
    match &plan.grain {
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
        QualityGrain::TimeWindows { column, every } => Ok((
            lf.clone(),
            time_window_start(plan.time_value(column), every),
        )),
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
        QualityGrain::Partition(column) => format!("{column}={}", raw.unwrap_or("∅")),
        QualityGrain::TimeWindows { column, every } => time_window_label(column, every, raw),
        QualityGrain::File => format!("file {}", raw.unwrap_or("∅")),
        QualityGrain::Dataset => "current view".to_string(),
    }
}

fn apply_comparisons(
    segments: &mut [SegmentQualityProfile],
    comparison: QualityComparison,
    baseline_segment: Option<&str>,
    precision: QualityPrecision,
) {
    for segment in segments.iter_mut() {
        segment.compared_with = None;
        segment.largest_change = None;
        segment.change_size = None;
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
            if let Some((what, size)) = change {
                segments[index].largest_change = Some(what);
                segments[index].change_size = Some(size);
            }
        }
    }
}

/// How far a measurement has to move between segments before it is worth naming, in
/// percentage points.
pub(crate) const MATERIAL_CHANGE_PP: f64 = 1.0;

/// How many standard errors apart two sampled rates must be before the difference
/// is named. A segment is dozens of columns and measures, and a daily grain is
/// thousands of segments: at three, sampling alone would name a change most days.
const NOISE_Z: f64 = 4.0;

/// Whether rates `a` of `n_a` rows and `b` of `n_b` rows differ by more than two
/// samples of those sizes would by chance (a two-proportion z-test).
pub fn beyond_noise(a: f64, n_a: usize, b: f64, n_b: usize) -> bool {
    if n_a == 0 || n_b == 0 {
        return false;
    }
    let (n_a, n_b) = (n_a as f64, n_b as f64);
    let pooled = (a * n_a + b * n_b) / (n_a + n_b);
    let error = (pooled * (1.0 - pooled) * (1.0 / n_a + 1.0 / n_b)).sqrt();
    error > 0.0 && (a - b).abs() / error >= NOISE_Z
}

/// The rates a segment is compared on. A distinct share is not one of them: it
/// falls as a segment grows, so two segments of different sizes differ by it
/// whatever their data.
const CHANGE_MEASURES: [QualityMetric; 4] = [
    QualityMetric::NullRate,
    QualityMetric::EmptyRate,
    QualityMetric::WhitespaceRate,
    QualityMetric::NonFiniteRate,
];

/// The clearest move between two segments, and its size.
///
/// #196 asks where a column's null rate or range shifts sharply, which is a
/// question about the sharpest single move rather than about the average of all of
/// them: one column going from never-null to always-null is the finding, and a mean
/// over sixty columns buries it. A row count that halved or doubled comes first:
/// for a feed split by day it is the loudest thing that can go wrong. On a sample,
/// a move is named only past sampling noise; a range that moved only on an exact
/// profile, since a sample's minimum and maximum move with the draw.
fn largest_material_change(
    segment: &SegmentQualityProfile,
    baseline: &SegmentQualityProfile,
    precision: QualityPrecision,
) -> Option<(String, f64)> {
    if let (Some(now), Some(before)) = (segment.total_rows, baseline.total_rows)
        && before > 0
    {
        let ratio = now as f64 / before as f64;
        if !(0.5..2.0).contains(&ratio) {
            let percent = (ratio - 1.0) * 100.0;
            return Some((
                format!("rows {} ({percent:+.0}%)", crate::numfmt::group_chrome(now)),
                percent.abs(),
            ));
        }
    }
    let sampled = precision != QualityPrecision::Exact;
    let mut largest: Option<(f64, String)> = None;
    let mut range: Option<String> = None;
    for (index, column) in segment.columns.iter().enumerate() {
        // Both profiles are built by walking the same schema, so the columns line up.
        // A linear search per column per segment is a square over the column count,
        // which is paid exactly where this feature is for: thousands of file segments
        // over hundreds of columns.
        let Some(prior) = baseline
            .columns
            .get(index)
            .filter(|other| other.name == column.name)
            .or_else(|| {
                baseline
                    .columns
                    .iter()
                    .find(|other| other.name == column.name)
            })
        else {
            continue;
        };
        for metric in CHANGE_MEASURES {
            let (Some(now), Some(before)) = (metric.value(column), metric.value(prior)) else {
                continue;
            };
            let change = (now - before) * 100.0;
            if change.abs() < MATERIAL_CHANGE_PP
                || sampled
                    && !beyond_noise(
                        now,
                        metric.denominator(column),
                        before,
                        metric.denominator(prior),
                    )
            {
                continue;
            }
            if largest
                .as_ref()
                .is_none_or(|(most, _)| change.abs() > most.abs())
            {
                largest = Some((change, format!("{} {}", column.name, metric.short_label())));
            }
        }
        if !sampled && range.is_none() && (column.min != prior.min || column.max != prior.max) {
            range = Some(format!(
                "{} range {} -> {}",
                column.name,
                range_label(prior),
                range_label(column)
            ));
        }
    }
    match (largest, range) {
        (Some((change, what)), _) => Some((format!("{what} {change:+.1} pp"), change.abs())),
        (None, Some(moved)) => Some((moved, 0.0)),
        (None, None) => None,
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

/// A role's column as a run reads it: the column's own name, where its time values
/// are, and where the rows are flagged whose text the column's format did not read.
struct TimedColumn {
    name: String,
    values: String,
    unparsed: Option<String>,
}

/// One interval a run measures: its roles, the columns they sit on, and the grain
/// its rows are cut by.
struct ResolvedInterval {
    start_role: TemporalRole,
    end_role: TemporalRole,
    start: String,
    end: String,
    grain: QualityGrain,
}

/// The measured intervals whose two roles sit on columns the run can read as time.
/// A role on text with no format measures nothing: its interval is left out rather
/// than read as all missing.
fn resolved_intervals(plan: &DataQualityPlan, schema: &Schema) -> Vec<ResolvedInterval> {
    let usable = |role| {
        plan.role_column(role)
            .filter(|column| plan.reads_as_time(column, schema))
            .map(str::to_string)
    };
    plan.interval_pairs()
        .into_iter()
        .filter_map(|(start_role, end_role)| {
            let (start, end) = (usable(start_role)?, usable(end_role)?);
            let grain = plan.interval_grain(&start, &end);
            Some(ResolvedInterval {
                start_role,
                end_role,
                start,
                end,
                grain,
            })
        })
        .collect()
}

/// The distinct grains `intervals` are cut by, in the order they first appear: one
/// grouping each, however many intervals share it.
fn interval_grains(intervals: &[ResolvedInterval]) -> Vec<QualityGrain> {
    let mut grains = Vec::new();
    for interval in intervals {
        if !grains.contains(&interval.grain) {
            grains.push(interval.grain.clone());
        }
    }
    grains
}

/// How many groupings a run's intervals take: one per distinct grain. A full run
/// reads the scope once for each.
pub fn interval_passes(plan: &DataQualityPlan, schema: &Schema) -> usize {
    interval_grains(&resolved_intervals(plan, schema)).len()
}

/// End minus start per row, as a duration: null where either is missing or unread.
/// Dates are midnight; a zoned time is its instant in UTC, and a time with no zone
/// is read as if it were UTC.
fn interval_duration(plan: &DataQualityPlan, start: &str, end: &str) -> Expr {
    let as_time = |column: &str| {
        plan.time_value(column)
            .cast(DataType::Datetime(TimeUnit::Microseconds, None))
    };
    as_time(end) - as_time(start)
}

/// [`interval_duration`] in microseconds, the unit its counts are taken in.
fn interval_micros(plan: &DataQualityPlan, start: &str, end: &str) -> Expr {
    interval_duration(plan, start, end)
        .dt()
        .total_microseconds(false)
}

fn profile_temporal(
    df: &DataFrame,
    plan: &DataQualityPlan,
    sample_positions: Option<&[IdxSize]>,
) -> Result<Vec<TemporalLatencyProfile>> {
    // Resolved before the rows are grouped, as the lazy path does: the default plan
    // assigns no roles at all, and splitting the sample into ten thousand segments to
    // discover that costs a DataFrame copy per segment and answers nothing.
    let resolved = resolved_intervals(plan, df.schema());
    if resolved.is_empty() {
        return Ok(Vec::new());
    }
    // Text read as time is parsed once, beside the text, with a flag on the rows the
    // format did not read, so an unread value is told apart from a missing one.
    let mut parsed = Vec::new();
    let mut timed = |column: &str| {
        let Some(format) = plan.time_format(column) else {
            return TimedColumn {
                name: column.to_string(),
                values: column.to_string(),
                unparsed: None,
            };
        };
        let values = format!("__datui_quality_time::{column}");
        let unparsed = format!("__datui_quality_unparsed::{column}");
        if !parsed
            .iter()
            .any(|(name, _): &(String, Expr)| *name == values)
        {
            parsed.push((values.clone(), format.expr()));
            parsed.push((unparsed.clone(), format.unparsed()));
        }
        TimedColumn {
            name: column.to_string(),
            values,
            unparsed: Some(unparsed),
        }
    };
    let intervals = resolved
        .iter()
        .map(|interval| (interval, timed(&interval.start), timed(&interval.end)))
        .collect::<Vec<_>>();
    let df = if parsed.is_empty() {
        df.clone()
    } else {
        df.clone()
            .lazy()
            .with_columns(
                parsed
                    .into_iter()
                    .map(|(name, expr)| expr.alias(name))
                    .collect::<Vec<_>>(),
            )
            .collect()?
    };
    // Each grain's segments are cut once, whichever intervals share it.
    let mut cut: Vec<(QualityGrain, Vec<(String, DataFrame)>)> = Vec::new();
    for grain in interval_grains(&resolved) {
        let grain_plan = DataQualityPlan {
            grain: grain.clone(),
            ..plan.clone()
        };
        let segments = segment_rows(&df, &grain_plan, sample_positions)?
            .into_iter()
            .map(|group| Ok((group.label, take_rows(&df, &group.indices)?)))
            .collect::<Result<Vec<_>>>()?;
        cut.push((grain, segments));
    }
    // One interval's segments together, in their order, then the next interval's.
    let mut profiles = Vec::new();
    for (interval, start, end) in &intervals {
        let Some((_, segments)) = cut.iter().find(|(grain, _)| *grain == interval.grain) else {
            continue;
        };
        for (label, segment) in segments {
            profiles.push(latency_profile(
                segment,
                label,
                (interval.start_role, start),
                (interval.end_role, end),
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
    let resolved = resolved_intervals(plan, &schema);
    if resolved.is_empty() {
        return Ok(Vec::new());
    }

    let unparsed = |column: &str| {
        plan.time_format(column)
            .map(|format| format.unparsed().sum())
            .unwrap_or_else(|| lit(0u32))
    };
    let mut profiles = (0..resolved.len()).map(|_| Vec::new()).collect::<Vec<_>>();
    // One collect per grain the intervals are cut by: usually one, and one more for
    // each clock that differs.
    for grain in interval_grains(&resolved) {
        let mut expressions = vec![len().alias("__quality_temporal_rows")];
        let members = resolved
            .iter()
            .enumerate()
            .filter(|(_, interval)| interval.grain == grain)
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        for index in &members {
            let interval = &resolved[*index];
            let prefix = format!("latency::{index}::");
            let micros = interval_micros(plan, &interval.start, &interval.end);
            // Seconds as the sampled path takes them: whole seconds, toward zero.
            let seconds = interval_duration(plan, &interval.start, &interval.end)
                .dt()
                .total_seconds(false);
            expressions.extend([
                // Missing is the stored value; text the format did not read is
                // counted on its own.
                col(interval.start.as_str())
                    .is_null()
                    .sum()
                    .alias(format!("{prefix}missing_start")),
                col(interval.end.as_str())
                    .is_null()
                    .sum()
                    .alias(format!("{prefix}missing_end")),
                unparsed(&interval.start).alias(format!("{prefix}unparsed_start")),
                unparsed(&interval.end).alias(format!("{prefix}unparsed_end")),
                micros
                    .clone()
                    .is_not_null()
                    .sum()
                    .alias(format!("{prefix}paired")),
                micros
                    .clone()
                    .lt(lit(0i64))
                    .sum()
                    .alias(format!("{prefix}negative")),
                micros
                    .clone()
                    .eq(lit(0i64))
                    .sum()
                    .alias(format!("{prefix}zero")),
                seconds
                    .clone()
                    .quantile(lit(0.50), QuantileMethod::Nearest)
                    .alias(format!("{prefix}p50")),
                seconds
                    .clone()
                    .quantile(lit(0.90), QuantileMethod::Nearest)
                    .alias(format!("{prefix}p90")),
                seconds
                    .clone()
                    .quantile(lit(0.95), QuantileMethod::Nearest)
                    .alias(format!("{prefix}p95")),
                seconds
                    .clone()
                    .quantile(lit(0.99), QuantileMethod::Nearest)
                    .alias(format!("{prefix}p99")),
                seconds.max().alias(format!("{prefix}max")),
            ]);
            if let Some(threshold) = plan.latency_threshold_seconds {
                expressions.push(
                    micros
                        .gt(lit(threshold.saturating_mul(1_000_000)))
                        .sum()
                        .alias(format!("{prefix}above")),
                );
            }
        }

        let grain_plan = DataQualityPlan {
            grain: grain.clone(),
            ..plan.clone()
        };
        let ungrouped = matches!(grain, QualityGrain::Dataset)
            || matches!(grain, QualityGrain::File) && source.is_none();
        let aggregate = if ungrouped {
            collect_lazy(lf.clone().select(expressions), polars_streaming).map_err(Report::from)?
        } else {
            let (grouped_lf, group) = grouped_frame(lf, &grain_plan, source)?;
            collect_lazy(
                grouped_lf
                    .group_by([group.alias("__quality_segment")])
                    .agg(expressions),
                polars_streaming,
            )
            .map_err(Report::from)?
        };

        for row in 0..aggregate.height() {
            let segment = if ungrouped {
                if matches!(grain, QualityGrain::File) {
                    "file mapping unavailable for this view".to_string()
                } else {
                    "current view".to_string()
                }
            } else {
                let raw = string_value_at(&aggregate, "__quality_segment", row);
                segment_label(&grain, raw.as_deref())
            };
            let evaluated_rows = usize_value_at(&aggregate, "__quality_temporal_rows", row);
            for index in &members {
                let interval = &resolved[*index];
                let prefix = format!("latency::{index}::");
                let count =
                    |name: &str| usize_value_at(&aggregate, &format!("{prefix}{name}"), row);
                let seconds =
                    |name: &str| optional_i64_at(&aggregate, &format!("{prefix}{name}"), row);
                profiles[*index].push(TemporalLatencyProfile {
                    segment: segment.clone(),
                    start_role: interval.start_role,
                    end_role: interval.end_role,
                    start_column: interval.start.clone(),
                    end_column: interval.end.clone(),
                    evaluated_rows,
                    paired_rows: count("paired"),
                    missing_start: count("missing_start"),
                    missing_end: count("missing_end"),
                    unparsed_start: count("unparsed_start"),
                    unparsed_end: count("unparsed_end"),
                    negative_count: count("negative"),
                    zero_count: count("zero"),
                    p50_seconds: seconds("p50"),
                    p90_seconds: seconds("p90"),
                    p95_seconds: seconds("p95"),
                    p99_seconds: seconds("p99"),
                    max_seconds: seconds("max"),
                    threshold_seconds: plan.latency_threshold_seconds,
                    above_threshold_count: plan.latency_threshold_seconds.map(|_| count("above")),
                });
            }
        }
    }
    let mut ordered = Vec::new();
    for (interval, mut segments) in resolved.iter().zip(profiles) {
        segments.sort_by(|left, right| left.segment.cmp(&right.segment));
        // The zero padding exists so a lexicographic sort orders chunks numerically,
        // and comes off once it has. Segments does the same thing in the same place;
        // leaving it on here had Trends and Segments name one chunk two ways.
        if matches!(interval.grain, QualityGrain::RowChunks(_)) {
            for profile in &mut segments {
                profile.segment = pretty_chunk_label(&profile.segment);
            }
        }
        ordered.extend(segments);
    }
    Ok(ordered)
}

fn latency_profile(
    df: &DataFrame,
    segment: &str,
    (start_role, start): (TemporalRole, &TimedColumn),
    (end_role, end): (TemporalRole, &TimedColumn),
    threshold_seconds: Option<i64>,
) -> Result<TemporalLatencyProfile> {
    let starts = df.column(&start.values)?;
    let ends = df.column(&end.values)?;
    let flags = |column: &TimedColumn| {
        column
            .unparsed
            .as_ref()
            .map(|name| df.column(name))
            .transpose()
    };
    let (start_flags, end_flags) = (flags(start)?, flags(end)?);
    let unread = |flags: Option<&Column>, row: usize| -> Result<bool> {
        Ok(match flags {
            Some(flags) => flags.get(row)? == AnyValue::Boolean(true),
            None => false,
        })
    };
    let mut missing_start = 0;
    let mut missing_end = 0;
    let mut unparsed_start = 0;
    let mut unparsed_end = 0;
    let mut micros = Vec::new();
    for row in 0..df.height() {
        let start_at = value_epoch_micros(starts.get(row)?);
        let end_at = value_epoch_micros(ends.get(row)?);
        if start_at.is_none() {
            if unread(start_flags, row)? {
                unparsed_start += 1;
            } else {
                missing_start += 1;
            }
        }
        if end_at.is_none() {
            if unread(end_flags, row)? {
                unparsed_end += 1;
            } else {
                missing_end += 1;
            }
        }
        if let (Some(start_at), Some(end_at)) = (start_at, end_at) {
            micros.push(end_at - start_at);
        }
    }
    // Counted on the exact difference, so half a second early is early; the
    // percentiles are whole seconds.
    let negative_count = micros.iter().filter(|value| **value < 0).count();
    let zero_count = micros.iter().filter(|value| **value == 0).count();
    let above_threshold_count = threshold_seconds.map(|threshold| {
        let threshold = threshold.saturating_mul(1_000_000);
        micros.iter().filter(|value| **value > threshold).count()
    });
    let mut seconds = micros
        .iter()
        .map(|value| value / 1_000_000)
        .collect::<Vec<_>>();
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
        start_column: start.name.clone(),
        end_column: end.name.clone(),
        evaluated_rows: df.height(),
        paired_rows: micros.len(),
        missing_start,
        missing_end,
        unparsed_start,
        unparsed_end,
        negative_count,
        zero_count,
        p50_seconds: percentile(50),
        p90_seconds: percentile(90),
        p95_seconds: percentile(95),
        p99_seconds: percentile(99),
        max_seconds: seconds.last().copied(),
        threshold_seconds,
        above_threshold_count,
    })
}

/// Two counts per text column read as time, in whatever pass profiles the columns:
/// its non-null values, and those the format does not read.
fn interpretation_exprs(plan: &DataQualityPlan, schema: &Schema) -> Vec<Expr> {
    plan.time_formats
        .iter()
        .enumerate()
        .filter(|(_, format)| schema.get(&format.column).is_some())
        .flat_map(|(index, format)| {
            [
                col(format.column.as_str())
                    .is_not_null()
                    .sum()
                    .alias(format!("__datui_time::{index}::values")),
                format
                    .unparsed()
                    .sum()
                    .alias(format!("__datui_time::{index}::unparsed")),
            ]
        })
        .collect()
}

/// Text the chosen format does not read, one observation per column that has any,
/// from the counts [`interpretation_exprs`] took.
fn interpretation_observations(
    counts: &DataFrame,
    plan: &DataQualityPlan,
    schema: &Schema,
) -> Vec<QualityObservation> {
    plan.time_formats
        .iter()
        .enumerate()
        .filter(|(_, format)| schema.get(&format.column).is_some())
        .filter_map(|(index, format)| {
            let values = optional_usize(counts, &format!("__datui_time::{index}::values"))?;
            let unparsed = optional_usize(counts, &format!("__datui_time::{index}::unparsed"))?;
            (unparsed > 0).then(|| QualityObservation {
                kind: ObservationKind::UnparsedTime,
                column: format.column.clone(),
                affected_rows: unparsed,
                evaluated_rows: values,
                fact: String::new(),
                normalized_category: None,
                files: Vec::new(),
                time_format: Some(format.clone()),
                full_scale: None,
            })
        })
        .collect()
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

/// One count the profile pass takes of each column it applies to: the alias suffix
/// it is read back by, its expression, and the profile field it fills. The pass and
/// the reader both go through [`MEASURES`], so a name cannot drift between them.
struct Measure {
    name: &'static str,
    applies: fn(&DataType) -> bool,
    expr: fn(Expr, &DataType) -> Expr,
    field: fn(&mut ColumnQualityProfile) -> &mut Option<usize>,
}

fn is_text(dtype: &DataType) -> bool {
    matches!(dtype, DataType::String | DataType::Categorical(..))
}

fn has_length(dtype: &DataType) -> bool {
    is_text(dtype) || matches!(dtype, DataType::List(_))
}

/// Text that parses as `reading`, nulls not counted.
fn parse_count(column: Expr, dtype: &DataType, reading: TextReading) -> Expr {
    let text = text_expr(column, dtype);
    parses_as(text.clone(), reading)
        .and(text.is_not_null())
        .sum()
}

/// A text value's length in characters, a list's in items.
fn length(column: Expr, dtype: &DataType) -> Expr {
    if matches!(dtype, DataType::List(_)) {
        column.list().len()
    } else {
        text_expr(column, dtype).str().len_chars()
    }
}

const MEASURES: [Measure; 13] = [
    Measure {
        name: "distinct",
        applies: |_| true,
        expr: |column, _| column.clone().filter(column.is_not_null()).n_unique(),
        field: |profile| &mut profile.distinct_count,
    },
    Measure {
        name: "empty",
        applies: is_text,
        expr: |column, dtype| text_expr(column, dtype).eq(lit("")).sum(),
        field: |profile| &mut profile.empty_count,
    },
    Measure {
        name: "whitespace",
        applies: is_text,
        expr: |column, dtype| {
            let text = text_expr(column, dtype);
            text.clone()
                .str()
                .strip_chars(lit(LiteralValue::untyped_null()))
                .eq(lit(""))
                .and(text.neq(lit("")))
                .sum()
        },
        field: |profile| &mut profile.whitespace_count,
    },
    Measure {
        name: "parse_int",
        applies: is_text,
        // Polars' cast is not strict: text that is not a whole number becomes null.
        expr: |column, dtype| {
            let text = text_expr(column, dtype);
            text.clone()
                .cast(DataType::Int64)
                .is_not_null()
                .and(text.is_not_null())
                .sum()
        },
        field: |profile| &mut profile.integer_parse_count,
    },
    Measure {
        name: "leading_zero",
        applies: is_text,
        expr: |column, dtype| {
            let text = text_expr(column, dtype);
            text.clone()
                .str()
                .starts_with(lit("0"))
                .and(text.clone().str().len_chars().gt(lit(1u32)))
                .and(text.cast(DataType::Int64).is_not_null())
                .sum()
        },
        field: |profile| &mut profile.leading_zero_count,
    },
    Measure {
        name: "parse_decimal",
        applies: is_text,
        expr: |column, dtype| parse_count(column, dtype, TextReading::Decimal),
        field: |profile| &mut profile.decimal_parse_count,
    },
    Measure {
        name: "parse_date",
        applies: is_text,
        expr: |column, dtype| parse_count(column, dtype, TextReading::Date),
        field: |profile| &mut profile.date_parse_count,
    },
    Measure {
        name: "parse_datetime",
        applies: is_text,
        expr: |column, dtype| parse_count(column, dtype, TextReading::Datetime),
        field: |profile| &mut profile.datetime_parse_count,
    },
    Measure {
        name: "min_length",
        applies: has_length,
        expr: |column, dtype| length(column, dtype).min(),
        field: |profile| &mut profile.min_length,
    },
    Measure {
        name: "max_length",
        applies: has_length,
        expr: |column, dtype| length(column, dtype).max(),
        field: |profile| &mut profile.max_length,
    },
    Measure {
        name: "nan",
        applies: DataType::is_float,
        expr: |column, _| column.cast(DataType::Float64).is_nan().sum(),
        field: |profile| &mut profile.nan_count,
    },
    Measure {
        name: "pos_inf",
        applies: DataType::is_float,
        expr: |column, _| column.cast(DataType::Float64).eq(lit(f64::INFINITY)).sum(),
        field: |profile| &mut profile.positive_infinity_count,
    },
    Measure {
        name: "neg_inf",
        applies: DataType::is_float,
        expr: |column, _| {
            column
                .cast(DataType::Float64)
                .eq(lit(f64::NEG_INFINITY))
                .sum()
        },
        field: |profile| &mut profile.negative_infinity_count,
    },
];

/// Every column's null count, range and [`MEASURES`], each aliased
/// `{column}::{measure}` for [`parse_profiles_at`].
fn build_profile_exprs(schema: &Schema) -> Vec<Expr> {
    let mut exprs = Vec::new();
    for (name, dtype) in schema.iter() {
        let column = col(name.as_str());
        let alias = |measure: &str| format!("{name}::{measure}");
        exprs.push(column.clone().null_count().alias(alias("null")));
        if supports_range(dtype) {
            exprs.push(column.clone().min().alias(alias("min")));
            exprs.push(column.clone().max().alias(alias("max")));
        }
        for measure in MEASURES.iter().filter(|measure| (measure.applies)(dtype)) {
            exprs.push((measure.expr)(column.clone(), dtype).alias(alias(measure.name)));
        }
    }
    exprs
}

/// Whether text parses as `reading`: the test the profile counts with, so a count
/// and the rows it opens agree. A whole number is counted among the decimals, and
/// fails where they fail.
fn parses_as(text: Expr, reading: TextReading) -> Expr {
    // Named formats, not inference: "parses as an ISO date" has to mean the same
    // thing on every column, including one where nothing does.
    let strptime = |format: &str| StrptimeOptions {
        format: Some(PlSmallStr::from(format)),
        strict: false,
        exact: true,
        cache: true,
    };
    match reading {
        TextReading::WholeNumber | TextReading::Decimal => {
            text.cast(DataType::Float64).is_not_null()
        }
        TextReading::Date => text.str().to_date(strptime("%Y-%m-%d")).is_not_null(),
        TextReading::Datetime => [
            "%Y-%m-%d %H:%M:%S%.f",
            "%Y-%m-%dT%H:%M:%S%.f%#z",
            "%Y-%m-%dT%H:%M:%S%.f",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S%#z",
            "%Y-%m-%dT%H:%M:%S",
        ]
        .into_iter()
        .map(|format| {
            text.clone()
                .str()
                .to_datetime(
                    Some(TimeUnit::Microseconds),
                    None,
                    strptime(format),
                    lit(PlSmallStr::from_static("raise")),
                )
                .is_not_null()
        })
        .reduce(Expr::or)
        .expect("at least one datetime format"),
    }
}

/// The rows of a parseable-text column its reading does not parse: non-null text
/// that stops a cast. `None` when the column has no reading.
pub fn unparsed_text(profile: &ColumnQualityProfile) -> Option<Expr> {
    let (_, reading) = text_reading(profile)?;
    let text = text_expr(col(profile.name.as_str()), &profile.dtype);
    Some(
        text.clone()
            .is_not_null()
            .and(parses_as(text, reading).not()),
    )
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
            let alias = |measure: &str| format!("{name}::{measure}");
            let mut profile = ColumnQualityProfile::unmeasured(name, dtype.clone(), evaluated_rows);
            profile.null_count = usize_value_at(aggregate, &alias("null"), row);
            profile.min = string_value_at(aggregate, &alias("min"), row);
            profile.max = string_value_at(aggregate, &alias("max"), row);
            for measure in &MEASURES {
                *(measure.field)(&mut profile) =
                    optional_usize_at(aggregate, &alias(measure.name), row);
            }
            profile
        })
        .collect()
}

/// The share of non-null text values that must parse before a text column is said
/// to hold numbers or dates. Below it the column is text that happens to contain a
/// few numbers, which is not a finding.
pub const TEXT_READING_SHARE: f64 = 0.95;

/// What the values of a text column parse as, most specific first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TextReading {
    WholeNumber,
    Decimal,
    Datetime,
    Date,
}

impl TextReading {
    pub fn label(self) -> &'static str {
        match self {
            Self::WholeNumber => "whole numbers",
            Self::Decimal => "decimal numbers",
            Self::Datetime => "ISO datetimes",
            Self::Date => "ISO dates",
        }
    }

    pub fn is_number(self) -> bool {
        matches!(self, Self::WholeNumber | Self::Decimal)
    }
}

/// The one typed reading a text column's values support, with how many parse.
///
/// A whole number also parses as a decimal and a datetime string may also parse as a
/// date, so the column gets one answer rather than three rows saying overlapping
/// things. Numbers are whole only when every number is.
pub fn text_reading(profile: &ColumnQualityProfile) -> Option<(usize, TextReading)> {
    let non_null = profile.non_null_rows();
    if non_null == 0 {
        return None;
    }
    let enough = |count: Option<usize>| {
        count.filter(|parsed| *parsed as f64 >= non_null as f64 * TEXT_READING_SHARE)
    };
    if let Some(parsed) = enough(profile.decimal_parse_count) {
        let reading = if profile.integer_parse_count == Some(parsed) {
            TextReading::WholeNumber
        } else {
            TextReading::Decimal
        };
        return Some((parsed, reading));
    }
    [
        (profile.datetime_parse_count, TextReading::Datetime),
        (profile.date_parse_count, TextReading::Date),
    ]
    .into_iter()
    .find_map(|(count, reading)| enough(count).map(|parsed| (parsed, reading)))
}

fn observations_from_profiles(
    columns: &[ColumnQualityProfile],
    precision: QualityPrecision,
) -> Vec<QualityObservation> {
    let mut observations = Vec::new();
    for profile in columns {
        if profile.null_count > 0 {
            observations.push(observation(
                ObservationKind::Nulls,
                profile,
                profile.null_count,
            ));
        }
        if let Some(count) = profile.empty_count.filter(|count| *count > 0) {
            observations.push(observation(ObservationKind::Empty, profile, count));
        }
        if let Some(count) = profile.whitespace_count.filter(|count| *count > 0) {
            observations.push(observation(ObservationKind::Whitespace, profile, count));
        }
        let non_finite = profile.nan_count.unwrap_or(0)
            + profile.positive_infinity_count.unwrap_or(0)
            + profile.negative_infinity_count.unwrap_or(0);
        if non_finite > 0 {
            observations.push(observation(ObservationKind::NonFinite, profile, non_finite));
        }
        if profile.distinct_count == Some(1) && profile.non_null_rows() > 0 {
            observations.push(observation(
                ObservationKind::Constant,
                profile,
                profile.non_null_rows(),
            ));
        }
        if let Some((parsed, _)) = text_reading(profile) {
            observations.push(observation(ObservationKind::ParseableText, profile, parsed));
        }
        // Near-unique and still repeating. Both numbers are already measured, so this
        // check costs the comparison and nothing else.
        //
        // Only on an exact profile: a distinct count does not extrapolate the way a
        // null rate does. An order id repeating ten times in a billion rows is unique
        // in every 50,000-row sample of it, and "sampled" under a claim that a column
        // is nearly a key does not take the claim back.
        //
        // Only where a key can live: integers and text. A float measure or a timestamp
        // is nearly unique by nature, and its repeats are coincidences, not duplicates.
        if precision == QualityPrecision::Exact
            && (profile.dtype.is_integer()
                || matches!(profile.dtype, DataType::String | DataType::Categorical(..)))
            && let (Some(distinct), Some(uniqueness)) =
                (profile.distinct_count, profile.uniqueness_rate())
            && (KEY_LIKE_UNIQUENESS..1.0).contains(&uniqueness)
        {
            // Rows beyond one per value, as `DuplicateRows` counts extras. Not the
            // rows that share a value, which is what the drill-in opens and always
            // more; the detail pane says which is which.
            let extras = profile.non_null_rows().saturating_sub(distinct);
            if extras > 0 {
                observations.push(observation(ObservationKind::KeyLike, profile, extras));
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
pub struct QualityConflictScan(pub crate::table::FileScan);

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
    watch: &QualityWatch,
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
                read_conflict_examples(scan, &column, &mut files, polars_streaming, watch);
            }
            let named = if tally.files > files.len() {
                format!(", largest {} named", files.len())
            } else {
                String::new()
            };
            let verb = match (kind, tally.files) {
                (ObservationKind::Absent, 1) => "has no such column",
                (ObservationKind::Absent, _) => "have no such column",
                (_, 1) => "holds a type the scan cannot read",
                (_, _) => "hold a type the scan cannot read",
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
                time_format: None,
                full_scale: None,
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
    watch: &QualityWatch,
) {
    let name = PlSmallStr::from(column);
    for file in files.iter_mut() {
        if watch.cancelled() {
            return;
        }
        let Ok(lf) = (scan.0)(
            std::slice::from_ref(&file.name),
            std::slice::from_ref(&name),
        ) else {
            continue;
        };
        let query = lf
            .select([crate::past_calendar::text_expr(
                col(column),
                CastOptions::NonStrict,
            )])
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

/// Read every sample of `audio` once and add what a recording's quality turns on to
/// `results`: clipping, runs of exact zeros, and DC offset, per channel. Only for a
/// full run whose scope is every frame of the file, which the caller decides: the
/// read is of the file, not of the view.
pub fn add_signal_observations(
    results: &mut DataQualityResults,
    audio: &crate::audio::AudioSource,
    watch: &QualityWatch,
) -> Result<()> {
    watch.stage(QualityStage::CheckingSignal, true, true)?;
    let reports = audio
        .signal_report(&|| watch.cancelled())?
        .ok_or_else(|| Report::msg(crate::sampling::CANCELLED))?;
    results
        .observations
        .extend(signal_observations(&reports, audio.header().sample_rate));
    Ok(())
}

/// Observations from [`crate::audio::SignalReport`]s: a channel with runs at full
/// scale, runs of exact zeros, or a mean 1% of full scale or more from zero.
pub fn signal_observations(
    reports: &[crate::audio::SignalReport],
    sample_rate: f64,
) -> Vec<QualityObservation> {
    let mut observations = Vec::new();
    let samples = |n: u64| {
        format!(
            "{} {}",
            crate::numfmt::group_chrome(n as usize),
            if n == 1 { "sample" } else { "samples" }
        )
    };
    let runs = |n: u64| if n == 1 { "run" } else { "runs" };
    for report in reports {
        let evaluated = report.frames as usize;
        let push = |observations: &mut Vec<QualityObservation>,
                    kind: ObservationKind,
                    affected: u64,
                    fact: String,
                    full_scale: Option<(f64, f64)>| {
            observations.push(QualityObservation {
                kind,
                column: report.channel.clone(),
                affected_rows: affected as usize,
                evaluated_rows: evaluated,
                fact,
                normalized_category: None,
                files: Vec::new(),
                time_format: None,
                full_scale,
            });
        };
        if report.clip_runs > 0 {
            push(
                &mut observations,
                ObservationKind::Clipping,
                report.in_clip_runs,
                format!(
                    "{} {} of {}+ samples at full scale; longest {}",
                    crate::numfmt::group_chrome(report.clip_runs as usize),
                    runs(report.clip_runs),
                    report.clip_run_min,
                    samples(report.longest_clip)
                ),
                Some(report.full_scale),
            );
        }
        if report.zero_runs > 0 {
            push(
                &mut observations,
                ObservationKind::ZeroRuns,
                report.in_zero_runs,
                format!(
                    "{} {} of exact zeros, {}+ samples; longest {} ({})",
                    crate::numfmt::group_chrome(report.zero_runs as usize),
                    runs(report.zero_runs),
                    report.zero_run_min,
                    samples(report.longest_zeros),
                    crate::widgets::info::clock(report.longest_zeros as f64 / sample_rate)
                ),
                None,
            );
        }
        let (low, high) = report.full_scale;
        let half_range = (high - low) / 2.0;
        let share = if half_range > 0.0 {
            report.mean.abs() / half_range
        } else {
            0.0
        };
        if share >= DC_OFFSET_SHARE {
            // Integers in their own units; float and normalized to four places.
            let mean = if half_range > 2.0 {
                format!("{:+.1}", report.mean)
            } else {
                format!("{:+.4}", report.mean)
            };
            push(
                &mut observations,
                ObservationKind::DcOffset,
                report.frames,
                format!("mean {mean} ({:.1}% of full scale)", share * 100.0),
                None,
            );
        }
    }
    observations
}

/// A channel's mean, as a share of full scale, from which it is called DC offset: 1%,
/// -40 dBFS, well above any dither or noise floor.
const DC_OFFSET_SHARE: f64 = 0.01;

fn observation(
    kind: ObservationKind,
    profile: &ColumnQualityProfile,
    affected_rows: usize,
) -> QualityObservation {
    QualityObservation {
        kind,
        column: profile.name.clone(),
        affected_rows,
        evaluated_rows: profile.evaluated_rows,
        fact: String::new(),
        normalized_category: None,
        files: Vec::new(),
        time_format: None,
        full_scale: None,
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
        Some(crate::exact::str_value(&value).to_string())
    }
}

#[cfg(test)]
pub(crate) mod fixtures;
#[cfg(test)]
mod tests;

/// Intervals between time roles: what they count, and out of what.
#[cfg(test)]
mod temporal_tests;

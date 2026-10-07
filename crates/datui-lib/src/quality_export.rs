//! A Data Quality report written to a file: the measurements on screen, the setup
//! they were measured with, and the source they were measured on. Built from the
//! results in memory; writing one reads nothing from the data.
//!
//! Two forms: versioned JSON for tools, with every measurement, and a short
//! Markdown report for people. The JSON schema is documented in
//! `docs/reference/data-quality.md`; a change that breaks a reader bumps
//! [`REPORT_VERSION`].

use crate::data_quality::{
    ColumnQualityProfile, DataQualityPlan, DataQualityResults, QualityPrecision, interval_label,
};
use crate::quality_report::{Outcome, Severity, build_report, checks, coverage, describe, verdict};
use crate::sampling::SampleMethod;
use serde::{Deserialize, Serialize};
use std::path::Path;

/// What the JSON says it is, so a reader can tell it from any other JSON.
pub const REPORT_FORMAT: &str = "datui-data-quality-report";

/// The JSON schema's version. Fields may be added within a version; one removed,
/// renamed or changed in meaning is a new version.
pub const REPORT_VERSION: u32 = 1;

/// The most file names the source identity lists.
const MAX_FILE_NAMES: usize = 100;

/// Which form of the report to write.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReportFormat {
    #[default]
    Json,
    Markdown,
}

impl ReportFormat {
    pub const ALL: [Self; 2] = [Self::Json, Self::Markdown];

    pub fn label(self) -> &'static str {
        match self {
            Self::Json => "JSON",
            Self::Markdown => "Markdown",
        }
    }

    pub fn extension(self) -> &'static str {
        match self {
            Self::Json => "json",
            Self::Markdown => "md",
        }
    }

    /// What the format holds, for the dialog's line.
    pub fn holds(self) -> &'static str {
        match self {
            Self::Json => "Every measurement, versioned, for tools",
            Self::Markdown => "The verdict, coverage, findings and setup, for people",
        }
    }
}

/// What a report was measured on, as far as datui can say without reading it: where
/// the data came from, what the view did to it, and for a local file its size and
/// modification time when the run began. No content hash: that would be a read.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceIdentity {
    /// The path or URL as opened.
    pub location: Option<String>,
    pub remote: bool,
    pub format: Option<String>,
    /// Files the dataset reads, when it reads several.
    pub files: Option<usize>,
    /// Their names, the first hundred.
    pub file_names: Vec<String>,
    /// A local file's size in bytes when the run began.
    pub bytes: Option<u64>,
    /// A local file's modification time when the run began, RFC 3339 in UTC.
    pub modified: Option<String>,
    /// What the view did to the rows: query, filters, sort, reshape. Empty for the
    /// data as loaded.
    pub view: Vec<String>,
}

impl SourceIdentity {
    /// Record `location`'s size and modification time, when it is one local file.
    /// A stat, not a read; run in the worker as the run begins.
    pub fn stat(&mut self) {
        let Some(location) = self.location.as_deref().filter(|_| !self.remote) else {
            return;
        };
        let Ok(meta) = std::fs::metadata(location) else {
            return;
        };
        if !meta.is_file() {
            return;
        }
        self.bytes = Some(meta.len());
        self.modified = meta.modified().ok().map(|time| {
            chrono::DateTime::<chrono::Utc>::from(time)
                .to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
        });
    }

    /// File names, cut to the first hundred.
    pub fn with_files(mut self, names: &[String]) -> Self {
        if names.len() > 1 {
            self.files = Some(names.len());
            self.file_names = names.iter().take(MAX_FILE_NAMES).cloned().collect();
        }
        self
    }
}

/// The report as JSON: [`REPORT_FORMAT`], version [`REPORT_VERSION`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReportFile {
    pub format: String,
    pub version: u32,
    /// The datui that wrote it.
    pub datui_version: String,
    /// When the file was written, RFC 3339 in UTC. Not when the data was read.
    pub exported_at: String,
    /// Where the rows came from. `None` for results no run labeled.
    pub source: Option<SourceIdentity>,
    pub setup: SetupJson,
    pub run: RunJson,
    /// The headline: `2 problems  1 note  9 of 12 columns clean`.
    pub verdict: String,
    pub coverage: CoverageJson,
    pub checks: Vec<CheckJson>,
    pub findings: Vec<FindingJson>,
    pub columns: Vec<ColumnJson>,
    pub duplicates: Option<DuplicatesJson>,
    pub segments: Vec<SegmentJson>,
    pub intervals: Vec<IntervalJson>,
    pub intent: Option<IntentJson>,
    /// The expected windows checked for gaps; `None` when none are stated.
    pub gaps: Option<GapsJson>,
}

/// The setup the report was measured with: every setting a run takes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SetupJson {
    /// `current view`, `whole source`, a partition, files or a row range.
    pub scope: String,
    /// `sample`, `full` or `metadata`.
    pub values: String,
    pub sample: SampleJson,
    /// `whole dataset`, `by file`, `by year`, `by day of date`, `in chunks of …`.
    pub grain: String,
    /// `none`, `previous` or `baseline`.
    pub comparison: String,
    pub baseline_segment: Option<String>,
    pub time_formats: Vec<TimeFormatJson>,
    pub time_roles: Vec<TimeRoleJson>,
    /// Measured intervals, `start to end` by role.
    pub intervals: Vec<String>,
    /// What puts an interval in a time window.
    pub window_by: String,
    pub latency_threshold_seconds: Option<i64>,
    pub intent: DeclaredJson,
    /// The windows rows are expected in, on a time-window grain; `None` unstated.
    pub expected: Option<ExpectedJson>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExpectedJson {
    /// Only Monday to Friday's hours or days are expected.
    pub weekdays: bool,
    /// The first expected time and the time expected windows end before, as typed;
    /// `None` takes the first or last window the run found.
    pub from: Option<String>,
    pub before: Option<String>,
}

/// The expected windows checked against the segments the run counted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GapsJson {
    /// `checked`; `no_values` (file metadata only), `no_windows` (no range stated
    /// and no window found) or `too_many` (more windows than are checked).
    pub status: String,
    /// The grain's column and window width: `1h`, `1d`, `1w`, `1mo`.
    pub column: String,
    pub every: String,
    /// `every hour`, `weekdays`, ...
    pub cadence: String,
    /// Windows in the stated range, for `too_many`.
    pub windows_in_range: Option<usize>,
    /// The first expected window's start and where the last ends, UTC.
    pub from: Option<String>,
    pub before: Option<String>,
    /// Windows expected in the range, and weekend windows left out of it.
    pub expected: Option<usize>,
    pub weekend: Option<usize>,
    pub with_rows: Option<usize>,
    pub empty: Option<usize>,
    pub not_sampled: Option<usize>,
    pub out_of_scope: Option<usize>,
    /// Whether a window with no rows found is known to be empty.
    pub counted: Option<bool>,
    /// Consecutive gap windows of one kind, in order.
    pub runs: Vec<GapRunJson>,
    /// Runs past the listed ones, counted and not listed.
    pub more_runs: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GapRunJson {
    /// `empty`, `not_sampled` or `out_of_scope`.
    pub kind: String,
    /// Where its first and last windows start, UTC.
    pub first: String,
    pub last: String,
    /// The windows it covers, in calendar terms: `2024-01-06 to 2024-01-07`.
    pub span: String,
    pub windows: usize,
    /// Rows the scope holds in it, for windows the sample missed, when counted.
    pub rows: Option<usize>,
}

fn utc(time: chrono::NaiveDateTime) -> String {
    time.format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

/// A gap run's kind as the JSON names it.
fn gap_kind(kind: crate::quality_trends::GapKind) -> &'static str {
    use crate::quality_trends::GapKind;
    match kind {
        GapKind::Empty => "empty",
        GapKind::Unsampled => "not_sampled",
        GapKind::OutOfScope => "out_of_scope",
    }
}

fn gaps_json(plan: &DataQualityPlan, results: &DataQualityResults) -> Option<GapsJson> {
    use crate::quality_trends::Gaps;
    let gaps = crate::quality_trends::expected_gaps(plan, results)?;
    let crate::data_quality::QualityGrain::TimeWindows { column, every } = &plan.grain else {
        return None;
    };
    let mut json = GapsJson {
        status: String::new(),
        column: column.clone(),
        every: every.clone(),
        cadence: plan
            .expected_windows()
            .map(|expected| expected.cadence_label(every))
            .unwrap_or_default(),
        windows_in_range: None,
        from: None,
        before: None,
        expected: None,
        weekend: None,
        with_rows: None,
        empty: None,
        not_sampled: None,
        out_of_scope: None,
        counted: None,
        runs: Vec::new(),
        more_runs: 0,
    };
    json.status = match gaps {
        Gaps::NoValues => "no_values",
        Gaps::NoWindows => "no_windows",
        Gaps::TooMany { windows } => {
            json.windows_in_range = Some(windows);
            "too_many"
        }
        Gaps::Checked(check) => {
            json.from = Some(utc(check.from));
            json.before = Some(utc(check.before));
            json.expected = Some(check.expected);
            json.weekend = Some(check.weekend);
            json.with_rows = Some(check.with_rows);
            json.empty = Some(check.empty);
            json.not_sampled = Some(check.unsampled);
            json.out_of_scope = Some(check.out_of_scope);
            json.counted = Some(check.counted);
            json.more_runs = check.more_runs;
            json.runs = check
                .runs
                .iter()
                .map(|run| GapRunJson {
                    kind: gap_kind(run.kind).to_string(),
                    first: utc(run.first),
                    last: utc(run.last),
                    span: crate::quality_trends::calendar_span(run.first, run.last, every),
                    windows: run.windows,
                    rows: run.rows,
                })
                .collect();
            "checked"
        }
    }
    .to_string();
    Some(json)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SampleJson {
    /// `Random`, `Equal per <column>`, `First rows` or `Every row`.
    pub method: String,
    /// Rows asked for: in all, or per value for an equal-per-value sample.
    pub rows: usize,
    /// The seed: the same seed, scope and source draw the same rows.
    pub seed: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimeFormatJson {
    pub column: String,
    /// `date` or `datetime`.
    pub kind: String,
    /// A strftime format.
    pub format: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimeRoleJson {
    pub role: String,
    pub column: String,
}

/// The declared intent, as Setup holds it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeclaredJson {
    pub key: Vec<String>,
    pub columns: Vec<ColumnIntentJson>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnIntentJson {
    pub column: String,
    pub required: bool,
    pub allowed: Vec<String>,
    pub min: Option<String>,
    pub max: Option<String>,
    /// `whole number` or `decimal` for text read as a number.
    pub read_as: Option<String>,
}

/// What the run read and how far its numbers reach.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunJson {
    /// `exact`, `sampled` or `metadata`.
    pub precision: String,
    /// Rows in scope, when known.
    pub total_rows: Option<usize>,
    /// Rows measured: the sample's, or every row in scope.
    pub evaluated_rows: usize,
    /// Rows an equal-per-value sample kept of each value.
    pub per_value: Option<usize>,
    pub source_files: Option<usize>,
    pub footers_read: Option<usize>,
    /// What the run's reads were seen to do; `None` when nothing watched them.
    pub reads: Option<ReadsJson>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadsJson {
    /// Stages that read the source.
    pub source_reads: usize,
    /// Of those, the ones whose rows were counted.
    pub counted: usize,
    /// Rows the counted reads passed through, over every pass.
    pub rows_traversed: usize,
    /// The local copy of a remote source a full scan's passes read, when they did.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local_copy: Option<LocalCopyJson>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalCopyJson {
    pub bytes: u64,
    pub objects: usize,
    /// This run fetched it; false when an earlier run did.
    pub fetched_by_this_run: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CoverageJson {
    pub exact: usize,
    pub sampled: usize,
    pub metadata: usize,
    pub skipped: usize,
    pub unavailable: Vec<UnavailableJson>,
    pub rows: Vec<String>,
    pub limits: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnavailableJson {
    pub reason: String,
    pub checks: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckJson {
    pub name: String,
    pub looks_for: String,
    pub applies_to: String,
    /// `passed`, `found`, `skipped` or `unavailable`.
    pub outcome: String,
    /// What was found, or why it was skipped or unavailable.
    pub detail: Option<String>,
    /// `exact`, `sampled` or `metadata`.
    pub basis: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FindingJson {
    /// `problem`, `note` or `clean`.
    pub severity: String,
    pub title: String,
    pub columns: Vec<String>,
    pub affected_rows: usize,
    pub evaluated_rows: usize,
    pub summary: String,
    /// The finding's numbers in a sentence, as its detail shows them.
    pub headline: String,
    pub evidence: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnJson {
    pub name: String,
    pub dtype: String,
    pub evaluated_rows: usize,
    pub null_count: usize,
    pub distinct_count: Option<usize>,
    pub empty_count: Option<usize>,
    pub whitespace_count: Option<usize>,
    pub nan_count: Option<usize>,
    pub positive_infinity_count: Option<usize>,
    pub negative_infinity_count: Option<usize>,
    pub min: Option<String>,
    pub max: Option<String>,
    pub dominant_value: Option<String>,
    pub dominant_count: Option<usize>,
    pub min_length: Option<usize>,
    pub max_length: Option<usize>,
    pub integer_parse_count: Option<usize>,
    pub decimal_parse_count: Option<usize>,
    pub date_parse_count: Option<usize>,
    pub datetime_parse_count: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DuplicatesJson {
    pub groups: usize,
    pub extra_rows: usize,
    pub rows_involved: usize,
    pub evaluated_rows: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SegmentJson {
    pub label: String,
    /// Rows in the segment, when counted.
    pub total_rows: Option<usize>,
    pub evaluated_rows: usize,
    pub null_cells: usize,
    pub null_rate: f64,
    pub compared_with: Option<String>,
    pub largest_change: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IntervalJson {
    pub interval: String,
    pub segment: String,
    pub start_column: String,
    pub end_column: String,
    pub rows: usize,
    /// Rows with both ends present and read: what negative, zero and over are of.
    pub both_ends: usize,
    pub missing_start: usize,
    pub missing_end: usize,
    pub unparsed_start: usize,
    pub unparsed_end: usize,
    pub negative: usize,
    pub zero: usize,
    pub p50_seconds: Option<i64>,
    pub p90_seconds: Option<i64>,
    pub p95_seconds: Option<i64>,
    pub p99_seconds: Option<i64>,
    pub max_seconds: Option<i64>,
    /// `duration > threshold`, strictly.
    pub threshold_seconds: Option<i64>,
    pub over_threshold: Option<usize>,
}

/// What the declared intent found.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IntentJson {
    /// False when the run read no values, so nothing declared was checked.
    pub measured: bool,
    pub precision: String,
    pub evaluated_rows: usize,
    pub key: Option<KeyJson>,
    pub columns: Vec<ColumnCheckJson>,
    /// Declared columns the scope did not have.
    pub absent: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyJson {
    pub columns: Vec<String>,
    /// Rows with no value in some part of the key.
    pub missing: usize,
    /// Key values held by more than one row.
    pub groups: usize,
    pub extra_rows: usize,
    pub rows_involved: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnCheckJson {
    pub column: String,
    pub dtype: String,
    /// Rows with a value, as stored.
    pub values: usize,
    pub missing: Option<usize>,
    pub unparsed: Option<usize>,
    pub outside: Option<usize>,
    /// Values the range compared.
    pub compared: Option<usize>,
    pub below: Option<usize>,
    pub above: Option<usize>,
    pub lowest: Option<String>,
    pub highest: Option<String>,
}

fn precision_label(precision: QualityPrecision) -> String {
    precision.label().to_string()
}

fn column_json(column: &ColumnQualityProfile) -> ColumnJson {
    ColumnJson {
        name: column.name.clone(),
        dtype: column.dtype.to_string(),
        evaluated_rows: column.evaluated_rows,
        null_count: column.null_count,
        distinct_count: column.distinct_count,
        empty_count: column.empty_count,
        whitespace_count: column.whitespace_count,
        nan_count: column.nan_count,
        positive_infinity_count: column.positive_infinity_count,
        negative_infinity_count: column.negative_infinity_count,
        min: column.min.clone(),
        max: column.max.clone(),
        dominant_value: column.dominant_value.clone(),
        dominant_count: column.dominant_count,
        min_length: column.min_length,
        max_length: column.max_length,
        integer_parse_count: column.integer_parse_count,
        decimal_parse_count: column.decimal_parse_count,
        date_parse_count: column.date_parse_count,
        datetime_parse_count: column.datetime_parse_count,
    }
}

fn setup_json(plan: &DataQualityPlan) -> SetupJson {
    SetupJson {
        scope: plan.scope.label(),
        values: plan.compute.label().to_string(),
        sample: SampleJson {
            method: plan.method.label(),
            rows: plan.dataset_rows,
            seed: plan.sample_seed,
        },
        grain: plan.grain.label(),
        comparison: plan.comparison.label().to_string(),
        baseline_segment: plan.baseline_segment.clone(),
        time_formats: plan
            .time_formats
            .iter()
            .map(|format| TimeFormatJson {
                column: format.column.clone(),
                kind: format.kind.label().to_string(),
                format: format.format.clone(),
            })
            .collect(),
        time_roles: plan
            .temporal_roles
            .iter()
            .map(|assignment| TimeRoleJson {
                role: assignment.role.label().to_string(),
                column: assignment.column.clone(),
            })
            .collect(),
        intervals: plan
            .interval_pairs()
            .into_iter()
            .map(interval_label)
            .collect(),
        window_by: plan.interval_clock.label().to_string(),
        latency_threshold_seconds: plan.latency_threshold_seconds,
        expected: plan.expected_windows().map(|expected| ExpectedJson {
            weekdays: expected.weekdays,
            from: expected.from.clone(),
            before: expected.before.clone(),
        }),
        intent: DeclaredJson {
            key: plan.intent.key.clone(),
            columns: plan
                .intent
                .columns
                .iter()
                .map(|intent| ColumnIntentJson {
                    column: intent.column.clone(),
                    required: intent.required,
                    allowed: intent.allowed.clone(),
                    min: intent.min.clone(),
                    max: intent.max.clone(),
                    read_as: intent.number.map(|number| number.label().to_string()),
                })
                .collect(),
        },
    }
}

fn outcome_json(outcome: &Outcome) -> (String, Option<String>) {
    match outcome {
        Outcome::Passed => ("passed".to_string(), None),
        Outcome::Found { detail, .. } => ("found".to_string(), Some(detail.clone())),
        Outcome::Skipped(reason) => ("skipped".to_string(), Some(reason.to_string())),
        Outcome::Unavailable(reason) => ("unavailable".to_string(), Some(reason.to_string())),
    }
}

/// The report as [`ReportFile`]: built from `results`, measured with `plan` on
/// `results.source`, at `exported_at`. Nothing here reads the data.
pub fn report_file(
    results: &DataQualityResults,
    plan: &DataQualityPlan,
    exported_at: &str,
) -> ReportFile {
    let report = build_report(results);
    let all_checks = checks(results, &report);
    let covered = coverage(results, &all_checks, plan);
    let severity = |severity: Severity| match severity {
        Severity::Problem => "problem",
        Severity::Note => "note",
        Severity::Clean => "clean",
    };
    ReportFile {
        format: REPORT_FORMAT.to_string(),
        version: REPORT_VERSION,
        datui_version: env!("CARGO_PKG_VERSION").to_string(),
        exported_at: exported_at.to_string(),
        source: results.source.as_deref().cloned(),
        setup: setup_json(plan),
        run: RunJson {
            precision: precision_label(results.precision),
            total_rows: results.total_rows,
            evaluated_rows: results.evaluated_rows,
            per_value: results.per_value,
            source_files: results.source_files,
            footers_read: results.footers_read,
            reads: results.reads.map(|reads| ReadsJson {
                source_reads: reads.reads,
                counted: reads.counted,
                rows_traversed: reads.rows,
                local_copy: reads.copy.map(|copy| LocalCopyJson {
                    bytes: copy.bytes,
                    objects: copy.objects,
                    fetched_by_this_run: copy.fetched,
                }),
            }),
        },
        verdict: verdict(&report),
        coverage: CoverageJson {
            exact: covered.exact,
            sampled: covered.sampled,
            metadata: covered.metadata,
            skipped: covered.skipped,
            unavailable: covered
                .unavailable
                .iter()
                .map(|(reason, names)| UnavailableJson {
                    reason: reason.to_string(),
                    checks: names.iter().map(|name| name.to_string()).collect(),
                })
                .collect(),
            rows: covered.rows.clone(),
            limits: covered.limits.clone(),
        },
        checks: all_checks
            .iter()
            .map(|check| {
                let (outcome, detail) = outcome_json(&check.outcome);
                CheckJson {
                    name: check.name.to_string(),
                    looks_for: check.looks_for.to_string(),
                    applies_to: check.applies_to.clone(),
                    outcome,
                    detail,
                    basis: precision_label(check.basis),
                }
            })
            .collect(),
        findings: report
            .findings
            .iter()
            .map(|finding| {
                let (headline, evidence) = describe(finding, results);
                FindingJson {
                    severity: severity(finding.severity).to_string(),
                    title: finding.title.to_string(),
                    columns: finding.columns.clone(),
                    affected_rows: finding.affected_rows,
                    evaluated_rows: finding.evaluated_rows,
                    summary: finding.summary.clone(),
                    headline,
                    evidence,
                }
            })
            .collect(),
        columns: results.columns.iter().map(column_json).collect(),
        duplicates: results.identity.as_ref().map(|identity| DuplicatesJson {
            groups: identity.duplicate_groups,
            extra_rows: identity.extra_rows,
            rows_involved: identity.rows_involved,
            evaluated_rows: identity.evaluated_rows,
        }),
        segments: results
            .segments
            .iter()
            .map(|segment| SegmentJson {
                label: segment.label.clone(),
                total_rows: segment.total_rows,
                evaluated_rows: segment.evaluated_rows,
                null_cells: segment.null_cells,
                null_rate: segment.null_rate,
                compared_with: segment.compared_with.clone(),
                largest_change: segment.largest_change.clone(),
            })
            .collect(),
        intervals: results
            .temporal
            .iter()
            .map(|interval| IntervalJson {
                interval: interval.label(),
                segment: interval.segment.clone(),
                start_column: interval.start_column.clone(),
                end_column: interval.end_column.clone(),
                rows: interval.evaluated_rows,
                both_ends: interval.paired_rows,
                missing_start: interval.missing_start,
                missing_end: interval.missing_end,
                unparsed_start: interval.unparsed_start,
                unparsed_end: interval.unparsed_end,
                negative: interval.negative_count,
                zero: interval.zero_count,
                p50_seconds: interval.p50_seconds,
                p90_seconds: interval.p90_seconds,
                p95_seconds: interval.p95_seconds,
                p99_seconds: interval.p99_seconds,
                max_seconds: interval.max_seconds,
                threshold_seconds: interval.threshold_seconds,
                over_threshold: interval.above_threshold_count,
            })
            .collect(),
        gaps: gaps_json(plan, results),
        intent: results.intent.as_ref().map(|intent| IntentJson {
            measured: intent.measured,
            precision: precision_label(intent.precision),
            evaluated_rows: intent.evaluated_rows,
            key: intent.key.as_ref().map(|key| KeyJson {
                columns: key.columns.clone(),
                missing: key.missing,
                groups: key.groups,
                extra_rows: key.extra_rows,
                rows_involved: key.rows_involved,
            }),
            columns: intent
                .columns
                .iter()
                .map(|check| ColumnCheckJson {
                    column: check.intent.column.clone(),
                    dtype: check.dtype.to_string(),
                    values: check.values,
                    missing: check.missing,
                    unparsed: check.unparsed,
                    outside: check.outside,
                    compared: check.compared,
                    below: check.below,
                    above: check.above,
                    lowest: check.lowest.clone(),
                    highest: check.highest.clone(),
                })
                .collect(),
            absent: intent.absent.clone(),
        }),
    }
}

/// The JSON report, pretty-printed.
pub fn to_json(file: &ReportFile) -> color_eyre::Result<String> {
    Ok(serde_json::to_string_pretty(file)?)
}

/// A cell of a Markdown table: pipes escaped, line breaks flattened.
fn cell(text: &str) -> String {
    text.replace('|', "\\|").replace('\n', " ")
}

/// The report for people: what was measured on, the verdict and how far it reaches,
/// the findings with their numbers and evidence, and the setup to run it again.
/// `file` is the report `results` and `plan` make; what it holds as text is read
/// from them, typed.
pub fn to_markdown(
    file: &ReportFile,
    results: &DataQualityResults,
    plan: &DataQualityPlan,
) -> String {
    let mut out = String::new();
    let mut line = |text: String| {
        out.push_str(&text);
        out.push('\n');
    };
    line("# Data quality report".to_string());
    line(String::new());
    if let Some(source) = &file.source {
        if let Some(location) = &source.location {
            let mut facts = Vec::new();
            if let Some(format) = &source.format {
                facts.push(format.clone());
            }
            if let Some(files) = source.files {
                facts.push(format!("{} files", crate::numfmt::group_chrome(files)));
            }
            if let Some(bytes) = source.bytes {
                facts.push(format!(
                    "{} bytes",
                    crate::numfmt::group_chrome(bytes as usize)
                ));
            }
            if let Some(modified) = &source.modified {
                facts.push(format!("modified {modified}"));
            }
            let facts = if facts.is_empty() {
                String::new()
            } else {
                format!(" ({})", facts.join(", "))
            };
            line(format!("- Source: `{location}`{facts}"));
        }
        if !source.view.is_empty() {
            line(format!("- View: {}", source.view.join("; ")));
        }
    }
    let count = crate::numfmt::group_chrome;
    let evaluated = results.evaluated_rows;
    let rows = match (results.precision, results.total_rows) {
        (QualityPrecision::Metadata, _) => "file metadata only, no values read".to_string(),
        (QualityPrecision::Exact, _) => format!("all {} rows, exact", count(evaluated)),
        (QualityPrecision::Sampled | QualityPrecision::Estimated, Some(total)) => {
            format!("{} of {} rows, sampled", count(evaluated), count(total))
        }
        (QualityPrecision::Sampled | QualityPrecision::Estimated, None) => {
            format!("{} rows, sampled", count(evaluated))
        }
    };
    line(format!("- Measured: {rows}"));
    line(format!(
        "- Written by datui {} at {}, from the report on screen; no data read",
        file.datui_version, file.exported_at
    ));
    line(String::new());
    // The verdict's parts sit two spaces apart on screen; a comma here.
    line(format!("**{}**", file.verdict.replace("  ", ", ")));
    line(String::new());

    line("## Coverage".to_string());
    line(String::new());
    let coverage = &file.coverage;
    let checks = [
        (coverage.exact, "exact"),
        (coverage.sampled, "sampled"),
        (coverage.metadata, "metadata"),
        (coverage.skipped, "skipped"),
        (
            coverage
                .unavailable
                .iter()
                .map(|unavailable| unavailable.checks.len())
                .sum(),
            "unavailable",
        ),
    ]
    .into_iter()
    .filter(|(count, _)| *count > 0)
    .map(|(count, label)| format!("{count} {label}"))
    .collect::<Vec<_>>();
    line(format!("- Checks: {}", checks.join(", ")));
    if !coverage.rows.is_empty() {
        line(format!("- Rows: {}", coverage.rows.join(", ")));
    }
    let limits = coverage
        .unavailable
        .iter()
        .map(|unavailable| format!("{}: {}", unavailable.checks.join(", "), unavailable.reason))
        .chain(coverage.limits.iter().cloned())
        .collect::<Vec<_>>();
    if !limits.is_empty() {
        line(format!("- Limits: {}", limits.join("; ")));
    }
    line(String::new());

    for (severity, heading) in [("problem", "Problems"), ("note", "Notes")] {
        let findings = file
            .findings
            .iter()
            .filter(|finding| finding.severity == severity)
            .collect::<Vec<_>>();
        if findings.is_empty() {
            continue;
        }
        line(format!("## {heading}"));
        line(String::new());
        for finding in findings {
            line(format!(
                "- **{}** ({}): {}",
                finding.title,
                finding.columns.join(", "),
                finding.headline
            ));
            for evidence in &finding.evidence {
                line(format!("  - {}", evidence.trim()));
            }
        }
        line(String::new());
    }
    if let Some(clean) = file
        .findings
        .iter()
        .find(|finding| finding.severity == "clean")
    {
        line("## Clean".to_string());
        line(String::new());
        line(clean.columns.join(", "));
        line(String::new());
    }

    line("## Checks".to_string());
    line(String::new());
    line("| Check | Covers | Read | Result |".to_string());
    line("|---|---|---|---|".to_string());
    for check in &file.checks {
        let result = match &check.detail {
            Some(detail) => format!("{}: {detail}", check.outcome),
            None => check.outcome.clone(),
        };
        line(format!(
            "| {} | {} | {} | {} |",
            cell(&check.name),
            cell(&check.applies_to),
            cell(&check.basis),
            cell(&result)
        ));
    }
    line(String::new());

    if !file.intervals.is_empty() {
        line("## Intervals".to_string());
        line(String::new());
        line("| Interval | Segment | Both ends | p50 s | p95 s | Negative | Over |".to_string());
        line("|---|---|---|---|---|---|---|".to_string());
        let seconds = |value: Option<i64>| value.map_or("-".to_string(), |value| value.to_string());
        for interval in &file.intervals {
            line(format!(
                "| {} | {} | {} | {} | {} | {} | {} |",
                cell(&interval.interval),
                cell(&interval.segment),
                interval.both_ends,
                seconds(interval.p50_seconds),
                seconds(interval.p95_seconds),
                interval.negative,
                interval
                    .over_threshold
                    .map_or("-".to_string(), |over| over.to_string())
            ));
        }
        line(String::new());
    }

    if let Some(gaps) = crate::quality_trends::expected_gaps(plan, results)
        && let crate::data_quality::QualityGrain::TimeWindows { column, every } = &plan.grain
    {
        use crate::quality_trends::Gaps;
        line("## Gaps".to_string());
        line(String::new());
        let cadence = plan
            .expected_windows()
            .map(|expected| expected.cadence_label(every))
            .unwrap_or_default();
        let expected = format!("Expected {cadence} by {column}");
        match gaps {
            Gaps::NoValues => line(format!("{expected}: file metadata only counts no windows")),
            Gaps::NoWindows => line(format!("{expected}: no window found and no range stated")),
            Gaps::TooMany { windows } => line(format!(
                "{expected}: {} windows in range, more than are checked",
                count(windows)
            )),
            Gaps::Checked(check) => {
                let mut facts = vec![
                    format!("{} with rows", count(check.with_rows)),
                    format!("{} empty", count(check.empty)),
                    format!("{} not sampled", count(check.unsampled)),
                    format!("{} out of scope", count(check.out_of_scope)),
                ];
                if check.weekend > 0 {
                    facts.push(format!(
                        "{} weekend windows not expected",
                        count(check.weekend)
                    ));
                }
                line(format!(
                    "{expected}, {} to before {}: {} windows; {}",
                    utc(check.from),
                    utc(check.before),
                    count(check.expected),
                    facts.join(", ")
                ));
                if !check.runs.is_empty() {
                    line(String::new());
                    line("| Windows | Gap | Count | Rows |".to_string());
                    line("|---|---|---|---|".to_string());
                    for run in &check.runs {
                        line(format!(
                            "| {} | {} | {} | {} |",
                            cell(&crate::quality_trends::calendar_span(
                                run.first, run.last, every
                            )),
                            gap_kind(run.kind).replace('_', " "),
                            run.windows,
                            run.rows.map_or("-".to_string(), |rows| rows.to_string())
                        ));
                    }
                    if check.more_runs > 0 {
                        line(String::new());
                        line(format!("{} more runs not listed", check.more_runs));
                    }
                }
            }
        }
        line(String::new());
    }

    line("## Setup".to_string());
    line(String::new());
    line("| Setting | Value |".to_string());
    line("|---|---|".to_string());
    let setup = &file.setup;
    let none = || "none".to_string();
    let joined = |items: Vec<String>| {
        if items.is_empty() {
            none()
        } else {
            items.join(", ")
        }
    };
    let mut rows = vec![
        ("Scope", setup.scope.clone()),
        ("Values", setup.values.clone()),
        (
            "Sample",
            // Every row takes no size and no seed, and the first rows no seed.
            match &plan.method {
                SampleMethod::EveryRow => plan.method.label(),
                SampleMethod::FirstRows => {
                    format!("{}, {} rows", plan.method.label(), count(plan.dataset_rows))
                }
                SampleMethod::PerPartition { .. } => format!(
                    "{}, {} rows per value, seed {}",
                    plan.method.label(),
                    count(plan.dataset_rows),
                    plan.sample_seed
                ),
                SampleMethod::Spread => format!(
                    "{}, {} rows, seed {}",
                    plan.method.label(),
                    count(plan.dataset_rows),
                    plan.sample_seed
                ),
            },
        ),
        (
            "Text as time",
            joined(
                setup
                    .time_formats
                    .iter()
                    .map(|format| {
                        format!("{} as {} `{}`", format.column, format.kind, format.format)
                    })
                    .collect(),
            ),
        ),
        (
            "Time roles",
            joined(
                setup
                    .time_roles
                    .iter()
                    .map(|role| format!("{} = {}", role.role, role.column))
                    .collect(),
            ),
        ),
        ("Intervals", joined(setup.intervals.clone())),
        ("Grain", setup.grain.clone()),
        (
            "Compare",
            match &setup.baseline_segment {
                Some(segment) => format!("{} {segment}", setup.comparison),
                None => setup.comparison.clone(),
            },
        ),
        (
            "Latency over",
            setup
                .latency_threshold_seconds
                .map_or_else(none, |seconds| format!("{seconds} s")),
        ),
        ("Window by", setup.window_by.clone()),
        (
            "Expected",
            setup.expected.as_ref().map_or_else(none, |expected| {
                let windows = crate::data_quality::ExpectedWindows {
                    weekdays: expected.weekdays,
                    from: expected.from.clone(),
                    before: expected.before.clone(),
                };
                let every = match &plan.grain {
                    crate::data_quality::QualityGrain::TimeWindows { every, .. } => every.as_str(),
                    _ => "",
                };
                format!(
                    "{}, {}",
                    windows.cadence_label(every),
                    windows.range_label()
                )
            }),
        ),
        (
            "Key",
            if setup.intent.key.is_empty() {
                none()
            } else {
                setup.intent.key.join(", ")
            },
        ),
    ];
    for intent in &setup.intent.columns {
        let mut rules = Vec::new();
        if intent.required {
            rules.push("required".to_string());
        }
        if let Some(read_as) = &intent.read_as {
            rules.push(format!("read as {read_as}"));
        }
        if !intent.allowed.is_empty() {
            rules.push(format!(
                "one of {}",
                crate::quality_intent::format_allowed(&intent.allowed)
            ));
        }
        match (&intent.min, &intent.max) {
            (Some(min), Some(max)) => rules.push(format!("{min} to {max}")),
            (Some(min), None) => rules.push(format!("at least {min}")),
            (None, Some(max)) => rules.push(format!("at most {max}")),
            (None, None) => {}
        }
        rows.push(("Intent", format!("{}: {}", intent.column, rules.join("; "))));
    }
    for (setting, value) in rows {
        line(format!("| {setting} | {} |", cell(&value)));
    }
    out
}

/// The report in `format`, ready to write.
pub fn render(
    results: &DataQualityResults,
    plan: &DataQualityPlan,
    format: ReportFormat,
    exported_at: &str,
) -> color_eyre::Result<String> {
    let file = report_file(results, plan, exported_at);
    match format {
        ReportFormat::Json => to_json(&file),
        ReportFormat::Markdown => Ok(to_markdown(&file, results, plan)),
    }
}

/// Write the report to `path`, replacing a file there only under
/// [`Overwrite::Replace`](crate::output_file::Overwrite). The results are in
/// memory: this writes, and reads nothing.
pub fn write(
    path: &Path,
    results: &DataQualityResults,
    plan: &DataQualityPlan,
    format: ReportFormat,
    overwrite: crate::output_file::Overwrite,
) -> color_eyre::Result<()> {
    use std::io::Write;
    let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let text = render(results, plan, format, &now)?;
    let mut out = crate::output_file::OutputFile::create(path, overwrite)?;
    out.file().write_all(text.as_bytes())?;
    out.commit()?;
    Ok(())
}

/// The export dialog: where to write, and which form.
pub struct ExportForm {
    pub path: crate::widgets::text_input::TextInput,
    pub format: ReportFormat,
    /// The cursor is on the format rather than the path.
    pub on_format: bool,
    /// Why Enter did not write, until the next edit.
    pub error: Option<String>,
}

impl ExportForm {
    /// The dialog on `stem`'s report as JSON in the current directory.
    pub fn new(stem: &str, theme: &crate::config::Theme) -> Self {
        let mut path = crate::widgets::text_input::TextInput::new().with_theme(theme);
        path.suggest(format!("{stem}-quality.{}", ReportFormat::Json.extension()));
        path.set_focused(true);
        Self {
            path,
            format: ReportFormat::Json,
            on_format: false,
            error: None,
        }
    }

    /// The next or previous form; a path ending in the old form's extension takes
    /// the new one's.
    pub fn cycle_format(&mut self) {
        let next = match self.format {
            ReportFormat::Json => ReportFormat::Markdown,
            ReportFormat::Markdown => ReportFormat::Json,
        };
        let old = format!(".{}", self.format.extension());
        let value = self.path.value().to_string();
        if let Some(stem) = value.strip_suffix(&old) {
            let renamed = format!("{stem}.{}", next.extension());
            if self.path.is_suggested() {
                self.path.suggest(renamed);
            } else {
                self.path.set_value(renamed);
            }
        }
        self.format = next;
        self.error = None;
    }

    pub fn toggle_focus(&mut self) {
        self.on_format = !self.on_format;
        self.path.set_focused(!self.on_format);
    }

    /// Where to write and in which form: `~` and `$VAR` expanded, a typed `.json`
    /// or `.md` choosing the form, and the form's extension added when the path has
    /// none. A blank path is refused.
    pub fn target(&self) -> Result<(std::path::PathBuf, ReportFormat), String> {
        let typed = self.path.value().trim();
        if typed.is_empty() {
            return Err("Type a path to write to".to_string());
        }
        let mut path = crate::home::expand_user_path(typed);
        let typed_format = path
            .extension()
            .and_then(|extension| extension.to_str())
            .and_then(|extension| {
                ReportFormat::ALL
                    .into_iter()
                    .find(|format| extension.eq_ignore_ascii_case(format.extension()))
            });
        if path.extension().is_none() {
            path.set_extension(self.format.extension());
        }
        Ok((path, typed_format.unwrap_or(self.format)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_quality::QualityCompute;
    use crate::data_quality::fixtures::measure;
    use crate::quality_intent::{ColumnIntent, DeclaredIntent};
    use polars::prelude::*;

    fn measured() -> (DataQualityResults, DataQualityPlan) {
        let df = df!(
            "id" => &[1i64, 2, 2, 4],
            "status" => &[Some("open"), Some("void"), None, Some("open")],
        )
        .unwrap();
        let plan = DataQualityPlan {
            compute: QualityCompute::Full,
            intent: DeclaredIntent {
                key: vec!["id".to_string()],
                columns: vec![ColumnIntent {
                    required: true,
                    allowed: vec!["open".to_string(), "closed".to_string()],
                    ..ColumnIntent::new("status")
                }],
            },
            ..DataQualityPlan::default()
        };
        let mut results = measure(&df.lazy(), Some(4), &plan);
        results.source = Some(Box::new(SourceIdentity {
            location: Some("orders.parquet".to_string()),
            format: Some("Parquet".to_string()),
            view: vec!["filter: status = open".to_string()],
            ..SourceIdentity::default()
        }));
        (results, plan)
    }

    /// The JSON reads back into the schema it was written from, with its format and
    /// version, the setup, the intent and its findings.
    #[test]
    fn json_round_trips_through_its_schema() {
        let (results, plan) = measured();
        let text = render(&results, &plan, ReportFormat::Json, "2026-09-30T00:00:00Z").unwrap();
        let file: ReportFile = serde_json::from_str(&text).unwrap();
        assert_eq!(file, report_file(&results, &plan, "2026-09-30T00:00:00Z"));
        assert_eq!(file.format, REPORT_FORMAT);
        assert_eq!(file.version, REPORT_VERSION);
        assert_eq!(file.setup.sample.seed, plan.sample_seed);
        assert_eq!(file.setup.values, "full");
        assert_eq!(file.setup.intent.key, vec!["id"]);
        assert_eq!(file.run.precision, "exact");
        assert_eq!(file.run.evaluated_rows, 4);
        assert_eq!(
            file.source.as_ref().unwrap().location.as_deref(),
            Some("orders.parquet")
        );
        let key = file.intent.as_ref().unwrap().key.clone().unwrap();
        assert_eq!((key.groups, key.rows_involved), (1, 2));
        assert!(
            file.findings
                .iter()
                .any(|finding| finding.title == "Repeated key")
        );
        assert_eq!(file.checks[0].name, "Column intent");
        // A generic reader finds the version where the schema says.
        let value: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(value["version"], serde_json::json!(REPORT_VERSION));
        assert_eq!(value["format"], serde_json::json!(REPORT_FORMAT));
    }

    /// The Markdown names the source, the verdict, coverage, every finding with its
    /// numbers, and the setup to run it again.
    #[test]
    fn markdown_says_what_was_measured_and_how() {
        let (results, plan) = measured();
        let text = render(
            &results,
            &plan,
            ReportFormat::Markdown,
            "2026-09-30T00:00:00Z",
        )
        .unwrap();
        for expected in [
            "# Data quality report",
            "- Source: `orders.parquet` (Parquet)",
            "- View: filter: status = open",
            "- Measured: all 4 rows, exact",
            "## Coverage",
            "## Problems",
            "**Repeated key** (id): 2 of 4 rows (50.0%) share their key with another row",
            "**Not allowed** (status): 1 of 3 values (33.3%) are not allowed",
            "  - Allowed: open, closed",
            "| Column intent |",
            "| Key | id |",
            "| Intent | status: required; one of open, closed |",
            &format!("seed {}", plan.sample_seed),
        ] {
            assert!(text.contains(expected), "missing {expected:?} in\n{text}");
        }
        // Every row takes no size or seed, so the setup names none.
        let every_row = DataQualityPlan {
            method: crate::sampling::SampleMethod::EveryRow,
            ..plan
        };
        let text = render(
            &results,
            &every_row,
            ReportFormat::Markdown,
            "2026-09-30T00:00:00Z",
        )
        .unwrap();
        assert!(text.contains("| Sample | Every row |"), "{text}");
    }

    /// Expected windows go into the setup, and the gaps they find into the report:
    /// counts by kind and each run, in JSON and in the Markdown.
    #[test]
    fn gaps_are_exported_with_their_windows() {
        // 2024-01-01 to 2024-01-10, with no rows on the 4th and 5th.
        let days = [0, 1, 2, 5, 6, 7, 8, 9];
        let base = chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        let day = days
            .iter()
            .map(|offset| (base - epoch).num_days() as i32 + offset)
            .collect::<Vec<_>>();
        let lf = df!("day" => day, "value" => [1i64, 2, 3, 4, 5, 6, 7, 8])
            .unwrap()
            .lazy()
            .with_column(col("day").cast(DataType::Date));
        let plan = DataQualityPlan {
            compute: QualityCompute::Full,
            grain: crate::data_quality::QualityGrain::TimeWindows {
                column: "day".to_string(),
                every: "1d".to_string(),
            },
            expected: Some(crate::data_quality::ExpectedWindows {
                weekdays: false,
                from: Some("2024-01-01".to_string()),
                before: Some("2024-01-12".to_string()),
            }),
            ..DataQualityPlan::default()
        };
        let results = measure(&lf, None, &plan);
        let file = report_file(&results, &plan, "2026-09-30T00:00:00Z");
        let text = to_json(&file).unwrap();
        assert_eq!(serde_json::from_str::<ReportFile>(&text).unwrap(), file);
        let expected = file.setup.expected.as_ref().unwrap();
        assert_eq!(expected.from.as_deref(), Some("2024-01-01"));
        let gaps = file.gaps.as_ref().unwrap();
        assert_eq!(gaps.status, "checked");
        assert_eq!((gaps.column.as_str(), gaps.every.as_str()), ("day", "1d"));
        assert_eq!(gaps.from.as_deref(), Some("2024-01-01T00:00:00Z"));
        assert_eq!(gaps.before.as_deref(), Some("2024-01-12T00:00:00Z"));
        assert_eq!(
            (gaps.expected, gaps.with_rows, gaps.empty, gaps.not_sampled),
            (Some(11), Some(8), Some(3), Some(0))
        );
        assert_eq!(gaps.counted, Some(true));
        let runs = gaps
            .runs
            .iter()
            .map(|run| (run.kind.as_str(), run.span.as_str(), run.windows))
            .collect::<Vec<_>>();
        assert_eq!(
            runs,
            [
                ("empty", "2024-01-04 to 2024-01-05", 2),
                ("empty", "2024-01-11", 1)
            ]
        );

        let markdown = to_markdown(&file, &results, &plan);
        for expected in [
            "## Gaps",
            "Expected every day by day, 2024-01-01T00:00:00Z to before 2024-01-12T00:00:00Z: 11 windows; 8 with rows, 3 empty, 0 not sampled, 0 out of scope",
            "| 2024-01-04 to 2024-01-05 | empty | 2 | - |",
            "| Expected | every day, 2024-01-01 to before 2024-01-12 |",
        ] {
            assert!(
                markdown.contains(expected),
                "missing {expected:?} in\n{markdown}"
            );
        }

        // No windows stated: no gaps, and the setup says none.
        let unstated = DataQualityPlan {
            expected: None,
            ..plan
        };
        let file = report_file(&results, &unstated, "2026-09-30T00:00:00Z");
        assert!(file.gaps.is_none() && file.setup.expected.is_none());
        assert!(!to_markdown(&file, &results, &unstated).contains("## Gaps"));
    }

    #[test]
    fn the_path_takes_the_forms_extension() {
        let theme =
            crate::config::Theme::from_config(&crate::config::AppConfig::default().theme).unwrap();
        let mut form = ExportForm::new("orders", &theme);
        assert_eq!(form.path.value(), "orders-quality.json");
        form.cycle_format();
        assert_eq!(form.format, ReportFormat::Markdown);
        assert_eq!(form.path.value(), "orders-quality.md");
        // Still the form's own suggestion, so typing replaces it.
        assert!(form.path.is_suggested());
        form.path.set_value("report");
        assert_eq!(
            form.target().unwrap(),
            (
                std::path::PathBuf::from("report.md"),
                ReportFormat::Markdown
            )
        );
        // A typed extension chooses the form.
        form.path.set_value("report.json");
        assert_eq!(form.target().unwrap().1, ReportFormat::Json);
        form.path.set_value("  ");
        assert!(form.target().is_err());
    }
}

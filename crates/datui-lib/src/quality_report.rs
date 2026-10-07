//! Data Quality results as a report: what is likely wrong, what depends on intent, and
//! which columns have nothing to report. Each check kind is one row of
//! [`ObservationKind::spec`] (names, severity, rank, advice, grouping, wording). Built
//! once per result; the engine, cache and drill-in work on observations, which a
//! finding only groups and ranks.

use crate::data_quality::{
    ColumnQualityProfile, DataQualityPlan, DataQualityResults, ObservationKind, QualityObservation,
    QualityPrecision, QualityScope, TextReading, text_reading,
};
use crate::numfmt;
use polars::prelude::Expr;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Severity {
    /// Likely a defect, and one that changes results without saying so.
    Problem,
    /// Fine or not depending on what the column is for.
    Note,
    /// Nothing to report.
    Clean,
}

impl Severity {
    pub fn heading(self) -> &'static str {
        match self {
            Self::Problem => "Problems",
            Self::Note => "Notes",
            Self::Clean => "Clean",
        }
    }
}

/// A kind read more than one way (how much is missing; what parsing text holds), each
/// with its own title and sometimes severity, rank and advice.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Variant {
    /// Null in every row checked.
    AlwaysMissing,
    /// Null in more than half the rows.
    MostlyMissing,
    /// Several columns null on exactly the same rows.
    MissingTogether,
    /// Whole numbers with leading zeros or one fixed length: codes, not quantities.
    CodesAsText,
    DatesAsText,
}

impl Variant {
    pub fn title(self) -> &'static str {
        match self {
            Self::AlwaysMissing => "Always missing",
            Self::MostlyMissing => "Mostly missing",
            // No row lacks one of these columns without lacking them all.
            Self::MissingTogether => "Missing together",
            Self::CodesAsText => "Codes as text",
            Self::DatesAsText => "Dates as text",
        }
    }

    fn severity(self) -> Option<Severity> {
        match self {
            Self::AlwaysMissing => Some(Severity::Problem),
            Self::MostlyMissing | Self::MissingTogether | Self::CodesAsText | Self::DatesAsText => {
                None
            }
        }
    }

    fn rank(self) -> Option<u8> {
        match self {
            Self::AlwaysMissing => Some(3),
            Self::MostlyMissing => Some(9),
            Self::MissingTogether | Self::CodesAsText | Self::DatesAsText => None,
        }
    }

    fn advice(self) -> Option<&'static [&'static str]> {
        match self {
            Self::AlwaysMissing => Some(&["Carries nothing; check the load or a rename upstream"]),
            // Says how much is left, so it is written from the finding; see `advice`.
            Self::MostlyMissing => None,
            Self::MissingTogether => {
                Some(&["Likely one cause: a join with no match, or a source with gaps"])
            }
            Self::CodesAsText => Some(&["Fine as text; cast only for arithmetic"]),
            Self::DatesAsText => Some(&["Sorts as text; parse as a date to filter by range"]),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Finding {
    pub severity: Severity,
    /// `None` for the clean-columns entry.
    pub kind: Option<ObservationKind>,
    pub variant: Option<Variant>,
    /// From the kind and its variant: what the list calls it.
    pub title: &'static str,
    pub columns: Vec<String>,
    /// Indices into [`DataQualityResults::observations`].
    pub observations: Vec<usize>,
    /// Rows behind the finding; per column when several columns share the count.
    pub affected_rows: usize,
    pub evaluated_rows: usize,
    /// One short line for the list: counts and the telling detail.
    pub summary: String,
    /// Grouped columns that are missing on exactly the same rows.
    pub same_rows: bool,
}

/// The clean-columns entry's title.
pub const NO_FINDINGS: &str = "No findings";

impl Finding {
    /// The same finding: its kind, how it reads, and its columns.
    pub fn same_as(&self, other: &Finding) -> bool {
        self.kind == other.kind && self.variant == other.variant && self.columns == other.columns
    }

    /// "open, high, low +7", fitted to `width` display columns.
    pub fn columns_label(&self, width: usize) -> String {
        columns_label(&self.columns, width)
    }

    /// Rows matching any grouped observation: every row behind the finding (columns missing
    /// on the same rows match alike).
    pub fn evidence_predicate(&self, results: &DataQualityResults) -> Option<Expr> {
        self.observations
            .iter()
            .map(|index| {
                results
                    .observations
                    .get(*index)?
                    .evidence_predicate(results)
            })
            .collect::<Option<Vec<_>>>()?
            .into_iter()
            .reduce(Expr::or)
    }

    /// The files behind an absent column or a type conflict; those stand alone.
    pub fn evidence_scope(&self, results: &DataQualityResults) -> Option<QualityScope> {
        match self.observations.as_slice() {
            [index] => results.observations.get(*index)?.evidence_scope(),
            _ => None,
        }
    }

    /// Which rows are the finding's evidence, or why there are none to show.
    pub fn evidence(&self, results: &DataQualityResults) -> Result<EvidenceRows, String> {
        if let Some(scope) = self.evidence_scope(results) {
            return Ok(EvidenceRows::Files(scope));
        }
        let Some(kind) = self.kind else {
            return Err("No rows: every column here passed".to_string());
        };
        if results.precision == QualityPrecision::Metadata {
            return Err("No rows: values were not read".to_string());
        }
        match kind.spec().evidence {
            Evidence::Duplicates => return Ok(EvidenceRows::Duplicates),
            Evidence::Failures if self.failures(results) == Some(0) => {
                return Err("No rows: every value parses".to_string());
            }
            Evidence::Rows
            | Evidence::Always
            | Evidence::Failures
            | Evidence::SharingValue
            | Evidence::Uncounted => {}
        }
        self.evidence_predicate(results)
            .map(EvidenceRows::Matching)
            .ok_or_else(|| "No rows: nothing to filter on".to_string())
    }

    /// How many rows Enter shows when one count covers them all (one observation, identical
    /// rows per column, or non-overlapping spellings). `None` for an uncounted union or
    /// file rows.
    pub fn evidence_count(&self, results: &DataQualityResults) -> Option<usize> {
        match self.kind?.spec().evidence {
            Evidence::Duplicates => {
                Some(results.identity.as_ref()?.rows_involved).filter(|rows| *rows > 0)
            }
            Evidence::Failures => self.failures(results),
            Evidence::SharingValue | Evidence::Uncounted => None,
            Evidence::Always => Some(self.affected_rows),
            Evidence::Rows => {
                (self.observations.len() == 1 || self.same_rows).then_some(self.affected_rows)
            }
        }
    }

    /// Non-null text in a parseable-text column that its reading does not parse.
    pub fn failures(&self, results: &DataQualityResults) -> Option<usize> {
        if self.kind?.spec().evidence != Evidence::Failures {
            return None;
        }
        let profile = results
            .columns
            .iter()
            .find(|profile| Some(&profile.name) == self.columns.first())?;
        let (parsed, _) = text_reading(profile)?;
        Some(profile.non_null_rows().saturating_sub(parsed))
    }

    /// The check that makes this finding, by the name the Checks list gives it.
    /// `None` for the clean entry.
    pub fn check(&self) -> Option<&'static str> {
        Some(self.kind?.spec().check)
    }

    /// Missing values grouped across columns that go missing at different rates.
    pub fn varied(&self) -> bool {
        self.lists_columns()
            && self.kind.map(|kind| kind.spec().grouping) == Some(Grouping::Missing)
            && self.severity == Severity::Note
    }

    /// A count per column that the detail lists one column a line: several columns
    /// grouped by what they miss, not on the same rows.
    pub fn lists_columns(&self) -> bool {
        self.kind.is_some_and(|kind| {
            matches!(kind.spec().grouping, Grouping::Missing | Grouping::ByRows)
        }) && self.columns.len() > 1
            && !self.same_rows
    }

    /// Each column's count and rate, worst first, then the rows with any of them (uncounted:
    /// between the largest column's count and their sum).
    pub fn breakdown(&self, results: &DataQualityResults) -> Vec<String> {
        let rows = self
            .observations
            .iter()
            .filter_map(|index| results.observations.get(*index))
            .collect::<Vec<_>>();
        let name_width = rows
            .iter()
            .map(|row| crate::glyphs::display_width(&row.column))
            .max()
            .unwrap_or(0)
            .min(28);
        let plural = |count: usize| if count == 1 { "row" } else { "rows" };
        let mut lines = rows
            .iter()
            .map(|row| {
                let name = columns_label(std::slice::from_ref(&row.column), name_width);
                let pad = name_width.saturating_sub(crate::glyphs::display_width(&name));
                format!(
                    "{name}{}  {:>7}  {} {}",
                    " ".repeat(pad),
                    crate::numfmt::percent_of(row.affected_rows, row.evaluated_rows),
                    numfmt::group_chrome(row.affected_rows),
                    plural(row.affected_rows)
                )
            })
            .collect::<Vec<_>>();
        let least = rows.iter().map(|row| row.affected_rows).max().unwrap_or(0);
        let most = rows
            .iter()
            .map(|row| row.affected_rows)
            .sum::<usize>()
            .min(self.evaluated_rows.max(least));
        lines.push(if least == most {
            format!(
                "Rows with any of them: {} {}",
                numfmt::group_chrome(least),
                plural(least)
            )
        } else {
            format!(
                "Rows with any of them: {} to {}, not counted",
                numfmt::group_chrome(least),
                numfmt::group_chrome(most)
            )
        });
        lines
    }
}

/// The rows behind a finding.
#[derive(Debug, Clone)]
pub enum EvidenceRows {
    /// Rows whose values match.
    Matching(Expr),
    /// Every row the named files hold.
    Files(QualityScope),
    /// Rows equal to another in every column, copies together; see
    /// [`crate::data_quality::duplicate_rows`].
    Duplicates,
}

/// Which of a kind's observations one finding says together.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Grouping {
    /// Each observation is its own finding.
    Alone,
    /// Every column at once: one fact named once per column.
    All,
    /// Columns with the same count together.
    ByRows,
    /// One finding per column, of all its observations.
    ByColumn,
    /// Missing values: columns always missing together, mostly together, on the very same
    /// rows, and the rest as one finding with per-column rates.
    Missing,
}

/// What Enter opens of a finding, and how many rows that is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Evidence {
    /// The rows counted, when one count is all of them: one observation, or the
    /// same rows in every column.
    Rows,
    /// The rows counted, always: values of one column that never overlap, or one
    /// fact about rows named once per column.
    Always,
    /// Rows equal to another in every column.
    Duplicates,
    /// The values its reading does not parse.
    Failures,
    /// Every row that shares a repeated value: more than the rows beyond one per
    /// value the check counts.
    SharingValue,
    /// Files, or every sample: nothing counted to show.
    Uncounted,
}

/// One kind of check. Everything about a kind but its emitter and the predicate
/// beside it (`QualityObservation::evidence_predicate`) is here.
pub struct CheckSpec {
    /// The check that makes it, by the name the Checks list gives it.
    pub check: &'static str,
    /// The finding's title, unless a [`Variant`] reads it another way.
    pub title: &'static str,
    pub severity: Severity,
    /// Order within a severity: what costs the most rows of trust first.
    pub rank: u8,
    /// What it means for the data and what to do about it, a fragment a line. The
    /// reader knows what a null is; this says only what is particular.
    pub advice: &'static [&'static str],
    pub grouping: Grouping,
    pub evidence: Evidence,
    /// What the rows Enter opens are, after their count: " that have a copy".
    pub rows_are: &'static str,
    /// What its rows hold, after "N of M rows (P%)": "null".
    pub noun: &'static str,
    /// The finding's reading, when one kind reads several ways.
    variant: fn(&Group<'_>) -> Option<Variant>,
    /// The list's line: counts and the telling detail.
    summary: fn(&Group<'_>) -> String,
    /// The detail's sentence, with the evidence that makes it concrete.
    headline: fn(&Detail<'_>, &mut Vec<String>) -> String,
}

impl ObservationKind {
    /// The kind's row of the table.
    pub fn spec(self) -> &'static CheckSpec {
        match self {
            Self::Nulls => &CheckSpec {
                check: "Missing values",
                title: "Missing values",
                severity: Severity::Note,
                rank: 10,
                advice: &["Check: clustered in some files or dates (Segments, by file or window)"],
                grouping: Grouping::Missing,
                evidence: Evidence::Rows,
                rows_are: "",
                noun: "null",
                variant: missing_variant,
                summary: nulls_summary,
                headline: nulls_headline,
            },
            Self::Empty => &CheckSpec {
                check: "Blank text",
                title: "Empty text",
                severity: Severity::Problem,
                rank: 7,
                advice: &["Counted as filled; treat as null if it means missing"],
                grouping: Grouping::ByRows,
                evidence: Evidence::Rows,
                rows_are: "",
                noun: "empty strings",
                variant: no_variant,
                summary: rows_each_summary,
                headline: per_column_headline,
            },
            Self::Whitespace => &CheckSpec {
                check: "Blank text",
                title: "Blank text",
                severity: Severity::Problem,
                rank: 6,
                advice: &["Counted as filled; trim to null if it means missing"],
                grouping: Grouping::ByRows,
                evidence: Evidence::Rows,
                rows_are: "",
                noun: "only spaces or tabs",
                variant: no_variant,
                summary: rows_each_summary,
                headline: per_column_headline,
            },
            Self::NonFinite => &CheckSpec {
                check: "NaN or infinite",
                title: "NaN or infinite",
                severity: Severity::Problem,
                rank: 4,
                advice: &["Sums and means become NaN; check for division by zero upstream"],
                grouping: Grouping::Alone,
                evidence: Evidence::Rows,
                rows_are: "",
                noun: "NaN or infinite",
                variant: no_variant,
                summary: rows_summary,
                headline: non_finite_headline,
            },
            Self::Constant => &CheckSpec {
                check: "Single value",
                title: "Single value",
                severity: Severity::Note,
                rank: 13,
                advice: &["Tells no rows apart; a stuck feed if it should vary"],
                grouping: Grouping::All,
                evidence: Evidence::Rows,
                rows_are: "",
                noun: "",
                variant: no_variant,
                summary: constant_summary,
                headline: constant_headline,
            },
            Self::ParseableText => &CheckSpec {
                check: "Numbers as text",
                title: "Numbers as text",
                severity: Severity::Note,
                rank: 12,
                advice: &["Sorts as text (\"10\" before \"9\"); cast to a number to sum"],
                grouping: Grouping::Alone,
                evidence: Evidence::Failures,
                rows_are: " that do not parse",
                noun: "",
                variant: reading_variant,
                summary: parseable_summary,
                headline: parseable_headline,
            },
            Self::DuplicateRows => &CheckSpec {
                check: "Duplicate rows",
                title: "Duplicate rows",
                severity: Severity::Problem,
                rank: 2,
                advice: &["Counted more than once; check for a double load or a join fan-out"],
                grouping: Grouping::Alone,
                evidence: Evidence::Duplicates,
                rows_are: " that have a copy",
                noun: "",
                variant: no_variant,
                summary: duplicates_summary,
                headline: duplicates_headline,
            },
            Self::CategoryVariants => &CheckSpec {
                check: "Mixed spellings",
                title: "Mixed spellings",
                severity: Severity::Problem,
                rank: 5,
                advice: &["Group-bys and joins split them; trim and normalize case"],
                grouping: Grouping::ByColumn,
                evidence: Evidence::Always,
                rows_are: "",
                noun: "",
                variant: no_variant,
                summary: variants_summary,
                headline: variants_headline,
            },
            Self::Absent => &CheckSpec {
                check: "Missing in files",
                title: "Missing in files",
                severity: Severity::Problem,
                rank: 1,
                advice: &["Check: files written before the column existed"],
                grouping: Grouping::Alone,
                evidence: Evidence::Uncounted,
                rows_are: "",
                noun: "",
                variant: no_variant,
                summary: fact_summary,
                headline: files_headline,
            },
            Self::TypeConflict => &CheckSpec {
                check: "Type mismatch",
                title: "Type mismatch",
                severity: Severity::Problem,
                rank: 0,
                advice: &["Values dropped, not converted; read as text in Info, or fix the writer"],
                grouping: Grouping::Alone,
                evidence: Evidence::Uncounted,
                rows_are: "",
                noun: "",
                variant: no_variant,
                summary: fact_summary,
                headline: files_headline,
            },
            Self::KeyLike => &CheckSpec {
                check: "Nearly unique",
                title: "Nearly unique",
                severity: Severity::Note,
                rank: 11,
                advice: &["Duplicates if it is a key; expected if it is a measurement"],
                grouping: Grouping::Alone,
                evidence: Evidence::SharingValue,
                rows_are: "",
                noun: "",
                variant: no_variant,
                summary: key_like_summary,
                headline: key_like_headline,
            },
            Self::UnparsedTime => &CheckSpec {
                check: "Unparsed times",
                title: "Unparsed times",
                severity: Severity::Problem,
                rank: 3,
                advice: &[
                    "Left out of time windows and intervals, not counted as missing",
                    "Check: another format, or values that are not times (Setup, e)",
                ],
                grouping: Grouping::Alone,
                evidence: Evidence::Rows,
                rows_are: "",
                noun: "",
                variant: no_variant,
                summary: values_summary,
                headline: unparsed_time_headline,
            },
            Self::KeyRepeated => &CheckSpec {
                check: INTENT_CHECK,
                title: "Repeated key",
                severity: Severity::Problem,
                rank: 2,
                advice: &[
                    "A key names one row; joins on it fan out and counts double",
                    "Check: a double load, or a key that needs another column",
                ],
                grouping: Grouping::All,
                evidence: Evidence::Always,
                rows_are: "",
                noun: "",
                variant: no_variant,
                summary: key_repeated_summary,
                headline: key_repeated_headline,
            },
            Self::KeyMissing => &CheckSpec {
                check: INTENT_CHECK,
                title: "Incomplete key",
                severity: Severity::Problem,
                rank: 2,
                advice: &["Rows with no key cannot be joined or told apart by it"],
                grouping: Grouping::All,
                evidence: Evidence::Always,
                rows_are: "",
                noun: "",
                variant: no_variant,
                summary: rows_summary,
                headline: key_missing_headline,
            },
            Self::RequiredMissing => &CheckSpec {
                check: INTENT_CHECK,
                title: "Required, missing",
                severity: Severity::Problem,
                rank: 2,
                advice: &["Check: the load, or rows the source writes without it"],
                grouping: Grouping::Alone,
                evidence: Evidence::Rows,
                rows_are: "",
                noun: "",
                variant: no_variant,
                summary: rows_summary,
                headline: required_headline,
            },
            Self::NotAllowed => &CheckSpec {
                check: INTENT_CHECK,
                title: "Not allowed",
                severity: Severity::Problem,
                rank: 2,
                advice: &["Check: a new value upstream, or the allowed list (Setup, e)"],
                grouping: Grouping::Alone,
                evidence: Evidence::Rows,
                rows_are: "",
                noun: "",
                variant: no_variant,
                summary: values_summary,
                headline: not_allowed_headline,
            },
            Self::OutOfRange => &CheckSpec {
                check: INTENT_CHECK,
                title: "Out of range",
                severity: Severity::Problem,
                rank: 2,
                advice: &["Check: units, placeholders such as -1 or 9999, or the range (Setup, e)"],
                grouping: Grouping::Alone,
                evidence: Evidence::Rows,
                rows_are: "",
                noun: "",
                variant: no_variant,
                summary: values_summary,
                headline: out_of_range_headline,
            },
            Self::UnparsedNumber => &CheckSpec {
                check: INTENT_CHECK,
                title: "Unparsed numbers",
                severity: Severity::Problem,
                rank: 2,
                advice: &["A cast makes them null; check the values or the reading (Setup, e)"],
                grouping: Grouping::Alone,
                evidence: Evidence::Rows,
                rows_are: "",
                noun: "",
                variant: no_variant,
                summary: values_summary,
                headline: unparsed_number_headline,
            },
            Self::Clipping => &CheckSpec {
                check: "Clipping",
                title: "Clipping",
                severity: Severity::Problem,
                rank: 4,
                advice: &[
                    "Cut flat at the limit; lower the gain at the source, nothing restores it",
                ],
                grouping: Grouping::Alone,
                evidence: Evidence::Rows,
                rows_are: "",
                noun: "in runs at full scale",
                variant: no_variant,
                summary: fact_summary,
                headline: signal_headline,
            },
            Self::ZeroRuns => &CheckSpec {
                check: "Runs of zeros",
                title: "Runs of zeros",
                severity: Severity::Note,
                rank: 8,
                advice: &["Dropouts mid-recording; digital silence if at the start or end"],
                grouping: Grouping::Alone,
                evidence: Evidence::Rows,
                rows_are: "",
                noun: "in runs of exact zeros",
                variant: no_variant,
                summary: fact_summary,
                headline: signal_headline,
            },
            Self::DcOffset => &CheckSpec {
                check: "DC offset",
                title: "DC offset",
                severity: Severity::Note,
                rank: 12,
                advice: &["A constant bias that eats headroom; a high-pass filter removes it"],
                grouping: Grouping::Alone,
                evidence: Evidence::Uncounted,
                rows_are: "",
                noun: "",
                variant: no_variant,
                summary: fact_summary,
                headline: dc_offset_headline,
            },
        }
    }
}

/// How the findings list is narrowed and ordered. Presentation only: it reads the
/// report on screen, and nothing is measured again.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FindingsView {
    /// Only findings that name this column.
    pub column: Option<String>,
    /// Only findings this check made, by [`Finding::check`].
    pub check: Option<&'static str>,
    pub order: FindingOrder,
}

/// The order findings are listed in within each severity.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum FindingOrder {
    /// What costs the most trust first; see [`build_report`].
    #[default]
    Ranked,
    /// Most rows affected first.
    Rows,
    /// Highest share of the rows checked first.
    Rate,
}

impl FindingOrder {
    pub fn next(self) -> Self {
        match self {
            Self::Ranked => Self::Rows,
            Self::Rows => Self::Rate,
            Self::Rate => Self::Ranked,
        }
    }

    /// The chip that switches to this order.
    pub fn chip(self) -> &'static str {
        match self {
            Self::Ranked => "Ranked",
            Self::Rows => "By rows",
            Self::Rate => "By rate",
        }
    }
}

impl FindingsView {
    pub fn narrowed(&self) -> bool {
        self.column.is_some() || self.check.is_some()
    }

    fn admits(&self, finding: &Finding) -> bool {
        self.column
            .as_ref()
            .is_none_or(|column| finding.columns.contains(column))
            && self
                .check
                .is_none_or(|check| finding.check() == Some(check))
    }

    /// Indices into `report.findings`, in the order listed. Severity leads in every
    /// order, so Problems stay above Notes; within one, a tie keeps the rank.
    pub fn shown(&self, report: &QualityReport) -> Vec<usize> {
        let findings = &report.findings;
        let mut shown = (0..findings.len())
            .filter(|index| self.admits(&findings[*index]))
            .collect::<Vec<_>>();
        let by = |left: &usize, right: &usize| {
            let (left, right) = (&findings[*left], &findings[*right]);
            left.severity
                .cmp(&right.severity)
                .then_with(|| match self.order {
                    FindingOrder::Ranked => std::cmp::Ordering::Equal,
                    FindingOrder::Rows => right.affected_rows.cmp(&left.affected_rows),
                    // Shares compared exactly: a/b against c/d as a·d against c·b.
                    FindingOrder::Rate => (right.affected_rows as u128
                        * left.evaluated_rows.max(1) as u128)
                        .cmp(&(left.affected_rows as u128 * right.evaluated_rows.max(1) as u128)),
                })
        };
        shown.sort_by(by);
        shown
    }

    /// The finding at `position` in the list as shown.
    pub fn selected<'a>(&self, report: &'a QualityReport, position: usize) -> Option<&'a Finding> {
        self.shown(report)
            .get(position)
            .and_then(|index| report.findings.get(*index))
    }

    /// What narrows and orders the list, for the line above it: "price · Missing
    /// values · by rows". Empty when the list is the report as ranked.
    pub fn describe(&self) -> Vec<String> {
        let mut parts = Vec::new();
        if let Some(column) = &self.column {
            parts.push(format!("column {column}"));
        }
        if let Some(check) = self.check {
            parts.push(check.to_string());
        }
        match self.order {
            FindingOrder::Ranked => {}
            FindingOrder::Rows => parts.push("by rows".to_string()),
            FindingOrder::Rate => parts.push("by rate".to_string()),
        }
        parts
    }
}

/// The columns a report can be narrowed to, each with how many findings name it,
/// in the results' column order.
pub fn column_choices(
    report: &QualityReport,
    results: &DataQualityResults,
) -> Vec<(String, usize)> {
    results
        .columns
        .iter()
        .map(|profile| {
            let count = report
                .findings
                .iter()
                .filter(|finding| finding.kind.is_some() && finding.columns.contains(&profile.name))
                .count();
            (profile.name.clone(), count)
        })
        .collect()
}

/// The checks that made a finding in `report`, each with how many, most important
/// first.
pub fn check_choices(report: &QualityReport) -> Vec<(&'static str, usize)> {
    let mut choices: Vec<(&'static str, usize)> = Vec::new();
    for check in report.findings.iter().filter_map(Finding::check) {
        match choices.iter_mut().find(|(name, _)| *name == check) {
            Some((_, count)) => *count += 1,
            None => choices.push((check, 1)),
        }
    }
    choices
}

#[derive(Debug, Clone, Default)]
pub struct QualityReport {
    pub findings: Vec<Finding>,
    pub problems: usize,
    pub notes: usize,
    pub clean_columns: usize,
    pub total_columns: usize,
    /// Worst severity per column, in the results' column order.
    pub column_status: Vec<Severity>,
    /// Finding titles per column, in the results' column order.
    pub column_findings: Vec<Vec<&'static str>>,
    /// No values were read, so the absence of a finding means nothing.
    pub metadata_only: bool,
    /// Values were to be read and the scope had none: no check had anything to look
    /// at, so nothing is clean either.
    pub no_rows: bool,
}

/// The report and checks a [`DataQualityResults`] reads as, built once on first ask and
/// kept with the results (their inputs never change after install). A copy starts
/// empty, so a changed copy reads fresh.
#[derive(Debug, Default)]
pub struct ReportCache(std::sync::OnceLock<Built>);

impl Clone for ReportCache {
    fn clone(&self) -> Self {
        Self::default()
    }
}

#[derive(Debug)]
struct Built {
    report: QualityReport,
    checks: Vec<Check>,
}

#[cfg(test)]
thread_local! {
    /// Reports this thread has built for a cache, for the tests that count them.
    pub(crate) static REPORTS_BUILT: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

impl ReportCache {
    fn get(&self, results: &DataQualityResults) -> &Built {
        self.0.get_or_init(|| {
            #[cfg(test)]
            REPORTS_BUILT.with(|built| built.set(built.get() + 1));
            let report = build_report(results);
            let checks = checks(results, &report);
            Built { report, checks }
        })
    }
}

impl DataQualityResults {
    /// Change these results in place: the report and checks are built again from
    /// what `edit` leaves.
    pub fn edit<R>(&mut self, edit: impl FnOnce(&mut Self) -> R) -> R {
        let out = edit(self);
        self.derived = ReportCache::default();
        out
    }

    /// The findings these results read as; see [`ReportCache`].
    pub fn report(&self) -> &QualityReport {
        &self.derived.get(self).report
    }

    /// Every check, most important first; see [`ReportCache`].
    pub fn checks(&self) -> &[Check] {
        &self.derived.get(self).checks
    }
}

pub fn build_report(results: &DataQualityResults) -> QualityReport {
    let mut findings = Vec::new();
    // Group observations that say the same thing so the list says it once.
    let mut groups = BTreeMap::<(u8, usize, String), Vec<usize>>::new();
    for (index, observation) in results.observations.iter().enumerate() {
        let kind = observation.kind as u8 + 1;
        let key = match observation.kind.spec().grouping {
            Grouping::Missing => {
                let always_missing = observation.evaluated_rows > 0
                    && observation.affected_rows == observation.evaluated_rows;
                let mostly_missing = observation.affected_rows * 2 > observation.evaluated_rows;
                // Columns missing on the very same rows are one fact about those rows;
                // any other missing values are one finding, with each column's rate
                // inside it.
                let same_rows = results.shared_nulls.iter().any(|shared| {
                    shared.same_rows()
                        && shared.columns.len() > 1
                        && shared.null_rows == observation.affected_rows
                        && shared.columns.contains(&observation.column)
                });
                if always_missing {
                    (0, 0, String::new())
                } else if mostly_missing {
                    (kind, usize::MAX, "mostly".to_string())
                } else if same_rows {
                    (kind, observation.affected_rows, String::new())
                } else {
                    (kind, usize::MAX, String::new())
                }
            }
            Grouping::ByRows => (kind, observation.affected_rows, String::new()),
            Grouping::All => (kind, 0, String::new()),
            Grouping::ByColumn => (kind, 0, observation.column.clone()),
            // The index keeps it from merging.
            Grouping::Alone => (u8::MAX, index, String::new()),
        };
        groups.entry(key).or_default().push(index);
    }
    for indices in groups.into_values() {
        findings.push(finding(results, &indices));
    }

    let mut status = vec![Severity::Clean; results.columns.len()];
    let mut titles = vec![Vec::new(); results.columns.len()];
    let position = results
        .columns
        .iter()
        .enumerate()
        .map(|(index, profile)| (profile.name.as_str(), index))
        .collect::<BTreeMap<_, _>>();
    for finding in &findings {
        for column in &finding.columns {
            if let Some(&index) = position.get(column.as_str()) {
                status[index] = status[index].min(finding.severity);
                if !titles[index].contains(&finding.title) {
                    titles[index].push(finding.title);
                }
            }
        }
    }

    findings.sort_by(|left, right| {
        left.severity
            .cmp(&right.severity)
            .then_with(|| rank(left).cmp(&rank(right)))
            .then_with(|| right.affected_rows.cmp(&left.affected_rows))
            .then_with(|| left.columns.cmp(&right.columns))
    });
    let problems = findings
        .iter()
        .filter(|finding| finding.severity == Severity::Problem)
        .count();
    let notes = findings.len() - problems;
    let metadata_only = results.precision == QualityPrecision::Metadata;
    let no_rows = !metadata_only && results.evaluated_rows == 0;
    let clean = results
        .columns
        .iter()
        .zip(&status)
        .filter(|(_, severity)| **severity == Severity::Clean)
        .map(|(profile, _)| profile.name.clone())
        .collect::<Vec<_>>();
    let clean_columns = if no_rows { 0 } else { clean.len() };
    if !clean.is_empty() && !metadata_only && !no_rows {
        findings.push(Finding {
            severity: Severity::Clean,
            kind: None,
            variant: None,
            title: NO_FINDINGS,
            summary: format!(
                "{} {}",
                numfmt::group_chrome(clean.len()),
                if clean.len() == 1 {
                    "column"
                } else {
                    "columns"
                }
            ),
            columns: clean,
            observations: Vec::new(),
            affected_rows: 0,
            evaluated_rows: results.evaluated_rows,
            same_rows: false,
        });
    }
    QualityReport {
        findings,
        problems,
        notes,
        clean_columns,
        total_columns: results.columns.len(),
        column_status: status,
        column_findings: titles,
        metadata_only,
        no_rows,
    }
}

/// Order within a severity: what costs the most rows of trust first.
fn rank(finding: &Finding) -> u8 {
    match (finding.variant.and_then(Variant::rank), finding.kind) {
        (Some(rank), _) => rank,
        (None, Some(kind)) => kind.spec().rank,
        (None, None) => 20,
    }
}

/// One finding's observations, as [`finding`] reads them.
struct Group<'a> {
    results: &'a DataQualityResults,
    indices: &'a [usize],
    first: &'a QualityObservation,
    profile: Option<&'a ColumnQualityProfile>,
    reading: Option<TextReading>,
    /// Several columns.
    grouped: bool,
    same_rows: bool,
    variant: Option<Variant>,
}

impl Group<'_> {
    /// "1,234 rows (2.0%)", with `each` after the noun.
    fn rows_each(&self, count: usize, each: &str) -> String {
        format!(
            "{} {}{each} ({})",
            numfmt::group_chrome(count),
            if count == 1 { "row" } else { "rows" },
            crate::numfmt::percent_of(count, self.first.evaluated_rows)
        )
    }

    fn rows(&self) -> String {
        self.rows_each(self.first.affected_rows, "")
    }
}

fn finding(results: &DataQualityResults, indices: &[usize]) -> Finding {
    let mut indices = indices.to_vec();
    let spec = results.observations[indices[0]].kind.spec();
    // Missing values list their columns, and mixed spellings their values, worst
    // first.
    if matches!(spec.grouping, Grouping::Missing | Grouping::ByColumn) {
        indices.sort_by_key(|index| std::cmp::Reverse(results.observations[*index].affected_rows));
    }
    let indices = indices.as_slice();
    let first = &results.observations[indices[0]];
    let mut columns = Vec::new();
    for index in indices {
        let column = &results.observations[*index].column;
        if !columns.contains(column) {
            columns.push(column.clone());
        }
    }
    let grouped = columns.len() > 1;
    let profile = results
        .columns
        .iter()
        .find(|profile| profile.name == first.column);
    let shared = results.shared_nulls.iter().find(|shared| {
        shared.null_rows == first.affected_rows
            && columns.iter().all(|column| shared.columns.contains(column))
    });
    let same_rows =
        spec.grouping == Grouping::Missing && grouped && shared.is_some_and(|s| s.same_rows());
    let mut group = Group {
        results,
        indices,
        first,
        profile,
        reading: profile.and_then(text_reading).map(|(_, reading)| reading),
        grouped,
        same_rows,
        variant: None,
    };
    group.variant = (spec.variant)(&group);
    let affected_rows = match spec.grouping {
        // One group per normalized value; the column's cost is all of them.
        Grouping::ByColumn => indices
            .iter()
            .map(|index| results.observations[*index].affected_rows)
            .sum(),
        Grouping::Alone | Grouping::All | Grouping::ByRows | Grouping::Missing => {
            first.affected_rows
        }
    };
    Finding {
        severity: group
            .variant
            .and_then(Variant::severity)
            .unwrap_or(spec.severity),
        kind: Some(first.kind),
        variant: group.variant,
        title: group.variant.map_or(spec.title, Variant::title),
        columns,
        observations: indices.to_vec(),
        affected_rows,
        evaluated_rows: first.evaluated_rows,
        summary: (spec.summary)(&group),
        same_rows,
    }
}

fn no_variant(_: &Group<'_>) -> Option<Variant> {
    None
}

fn missing_variant(group: &Group<'_>) -> Option<Variant> {
    let first = group.first;
    if first.evaluated_rows > 0 && first.affected_rows == first.evaluated_rows {
        Some(Variant::AlwaysMissing)
    } else if first.affected_rows * 2 > first.evaluated_rows {
        Some(Variant::MostlyMissing)
    } else if group.same_rows {
        Some(Variant::MissingTogether)
    } else {
        None
    }
}

fn reading_variant(group: &Group<'_>) -> Option<Variant> {
    match (group.reading, group.profile) {
        (Some(reading), Some(profile)) if reading.is_number() && is_code(profile) => {
            Some(Variant::CodesAsText)
        }
        (Some(TextReading::Datetime | TextReading::Date), _) => Some(Variant::DatesAsText),
        (Some(TextReading::WholeNumber | TextReading::Decimal) | None, _) => None,
    }
}

fn nulls_summary(group: &Group<'_>) -> String {
    let first = group.first;
    if group.variant == Some(Variant::AlwaysMissing) {
        "no value in any row".to_string()
    } else if group.same_rows {
        group.rows()
    } else if group.grouped {
        let fewest =
            group.results.observations[group.indices[group.indices.len() - 1]].affected_rows;
        let (low, high) = (
            crate::numfmt::percent_of(fewest, first.evaluated_rows),
            crate::numfmt::percent_of(first.affected_rows, first.evaluated_rows),
        );
        if low == high {
            group.rows_each(first.affected_rows, " each")
        } else {
            format!("{low} to {high} per column")
        }
    } else {
        group.rows()
    }
}

fn rows_each_summary(group: &Group<'_>) -> String {
    if group.grouped {
        group.rows_each(group.first.affected_rows, " each")
    } else {
        group.rows()
    }
}

fn rows_summary(group: &Group<'_>) -> String {
    group.rows()
}

/// "12 values (0.4%)".
fn values_summary(group: &Group<'_>) -> String {
    let first = group.first;
    format!(
        "{} {} ({})",
        numfmt::group_chrome(first.affected_rows),
        if first.affected_rows == 1 {
            "value"
        } else {
            "values"
        },
        crate::numfmt::percent_of(first.affected_rows, first.evaluated_rows)
    )
}

/// What only the engine can say: the files, or a channel's runs or mean. Across
/// channels, the worst channel's.
fn fact_summary(group: &Group<'_>) -> String {
    group.first.fact.clone()
}

fn constant_summary(group: &Group<'_>) -> String {
    if group.grouped {
        return "one value each".to_string();
    }
    group
        .profile
        .and_then(|profile| profile.dominant_value.as_ref())
        .map(|value| format!("always {}", quoted(value, 24)))
        .unwrap_or_else(|| "one value".to_string())
}

fn parseable_summary(group: &Group<'_>) -> String {
    match (group.reading, group.profile) {
        (Some(_), Some(profile)) if is_code(profile) => code_shape(profile),
        (Some(reading), Some(profile)) => format!(
            "{} parse as {}",
            crate::numfmt::percent_of(group.first.affected_rows, profile.non_null_rows()),
            reading.label()
        ),
        (None, _) | (_, None) => group.rows(),
    }
}

fn duplicates_summary(group: &Group<'_>) -> String {
    match group.results.identity.as_ref() {
        Some(identity) => format!(
            "{} extra {}",
            numfmt::group_chrome(identity.extra_rows),
            if identity.extra_rows == 1 {
                "copy"
            } else {
                "copies"
            }
        ),
        None => group.rows(),
    }
}

fn variants_summary(group: &Group<'_>) -> String {
    // Several values: the count is the finding, and the detail lists them.
    if group.indices.len() > 1 {
        return format!("{} values spelled more than one way", group.indices.len());
    }
    group
        .results
        .category_variants
        .iter()
        .find(|variants| Some(&variants.normalized) == group.first.normalized_category.as_ref())
        .map(|variants| {
            variants
                .variants
                .iter()
                .take(2)
                .map(|(value, _)| quoted(value, 24))
                .collect::<Vec<_>>()
                .join(" vs ")
        })
        .unwrap_or_default()
}

fn key_like_summary(group: &Group<'_>) -> String {
    let unique = group
        .profile
        .and_then(ColumnQualityProfile::uniqueness_rate)
        .map(|rate| format!(" ({} unique)", crate::numfmt::percent(rate)))
        .unwrap_or_default();
    format!(
        "{} repeated{unique}",
        numfmt::group_chrome(group.first.affected_rows)
    )
}

fn key_repeated_summary(group: &Group<'_>) -> String {
    match group.results.intent.as_ref().and_then(|i| i.key.as_ref()) {
        Some(key) => format!(
            "{} {} repeat; {} extra {}",
            numfmt::group_chrome(key.groups),
            if key.groups == 1 { "value" } else { "values" },
            numfmt::group_chrome(key.extra_rows),
            if key.extra_rows == 1 { "row" } else { "rows" }
        ),
        None => group.rows(),
    }
}

/// Whole numbers written with a leading zero keep it only as text, which makes them
/// a code rather than a quantity.
fn is_code(profile: &ColumnQualityProfile) -> bool {
    profile.leading_zero_count.is_some_and(|count| count > 0)
        || (profile.min_length.is_some()
            && profile.min_length == profile.max_length
            && profile.min_length.is_some_and(|length| length > 1))
}

fn code_shape(profile: &ColumnQualityProfile) -> String {
    let zeros = profile.leading_zero_count.unwrap_or(0);
    match (profile.min_length, profile.max_length) {
        (Some(min), Some(max)) if min == max && zeros > 0 => {
            format!("{min} digits, leading zeros")
        }
        (Some(min), Some(max)) if min == max => format!("{min} digits each"),
        _ => format!("{} with a leading zero", numfmt::group_chrome(zeros)),
    }
}

pub(crate) fn quoted(value: &str, width: usize) -> String {
    let text = format!("{value:?}");
    if crate::glyphs::display_width(&text) <= width {
        text
    } else {
        let mut cut = String::new();
        for ch in text.chars() {
            if crate::glyphs::display_width(&cut) + 2 > width {
                break;
            }
            cut.push(ch);
        }
        format!("{cut}{}", crate::glyphs::get().ellipsis)
    }
}

/// Column names joined until `width`, then "+N" for the rest.
pub fn columns_label(columns: &[String], width: usize) -> String {
    let mut label = String::new();
    for (index, column) in columns.iter().enumerate() {
        let candidate = if label.is_empty() {
            column.clone()
        } else {
            format!("{label}, {column}")
        };
        let rest = columns.len() - index - 1;
        let suffix = if rest > 0 {
            format!(" +{rest}")
        } else {
            String::new()
        };
        let needed =
            crate::glyphs::display_width(&candidate) + crate::glyphs::display_width(&suffix);
        if !label.is_empty() && needed > width {
            return format!("{label} +{}", rest + 1);
        }
        label = candidate;
    }
    label
}

/// What one check found: nothing, something, nothing because there was nothing for
/// it to look at, or nothing because this run could not look.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Passed,
    Found {
        tier: Severity,
        detail: String,
    },
    /// Nothing in the data for it: no column of its kind, or one file.
    Skipped(&'static str),
    /// It applies, and this run could not answer it: no values read, or a sample
    /// where the answer needs every row.
    Unavailable(&'static str),
}

impl Outcome {
    pub fn ran(&self) -> bool {
        matches!(self, Self::Passed | Self::Found { .. })
    }
}

/// One check the run makes: what it looks for, how far it reached, what it found.
/// The list is the answer to "checked for what?" when nothing turned up.
#[derive(Debug, Clone)]
pub struct Check {
    pub name: &'static str,
    pub looks_for: &'static str,
    pub applies_to: String,
    pub outcome: Outcome,
    /// What its numbers were read from: every row, a sample, or file footers.
    pub basis: QualityPrecision,
}

/// How many checks the collapsed list shows before "more".
pub const CHECKS_SHOWN: usize = 6;

/// Why a value check could not run on a scope with no rows.
const NO_ROWS: &str = "no rows to check";

/// Every check, most important first.
pub fn checks(results: &DataQualityResults, report: &QualityReport) -> Vec<Check> {
    use polars::prelude::DataType;
    let columns = &results.columns;
    let count = |filter: &dyn Fn(&ColumnQualityProfile) -> bool| {
        columns.iter().filter(|profile| filter(profile)).count()
    };
    let text = |profile: &ColumnQualityProfile| {
        matches!(profile.dtype, DataType::String | DataType::Categorical(..))
    };
    let all = columns.len();
    let floats = count(&|profile| profile.dtype.is_float());
    let texts = count(&text);
    let keys = count(&|profile| profile.dtype.is_integer() || text(profile));
    let reach = |count: usize, kind: &str| {
        let noun = if count == 1 { "column" } else { "columns" };
        if kind.is_empty() {
            format!("{} {noun}", numfmt::group_chrome(count))
        } else {
            format!("{} {kind} {noun}", numfmt::group_chrome(count))
        }
    };
    let values_read = !report.metadata_only;
    // Columns behind the findings a check makes. Nothing to look at is known from
    // the schema, whatever the run read.
    let outcome = |check: &str, applies: usize, none: &'static str| {
        if applies == 0 {
            return Outcome::Skipped(none);
        }
        if !values_read {
            return Outcome::Unavailable("values not read");
        }
        if report.no_rows {
            return Outcome::Unavailable(NO_ROWS);
        }
        found(report, check)
    };
    let files = results.source_files.filter(|files| *files > 1);
    let by_files = |check: &str| match files {
        Some(_) => found(report, check),
        None => Outcome::Skipped("needs several files"),
    };
    // A dataset too large to read every footer is checked over the footers read.
    let files_reach = match (files, results.footers_read) {
        (Some(files), Some(read)) if read < files => format!(
            "{} of {} files",
            numfmt::group_chrome(read),
            numfmt::group_chrome(files)
        ),
        (Some(files), _) => format!("{} files", numfmt::group_chrome(files)),
        (None, _) => "files".to_string(),
    };
    let values = results.precision;
    let mut checks = Vec::new();
    // Declared, so first: the question the user asked before the ones datui asks.
    if let Some(intent) = &results.intent {
        checks.push(Check {
            name: INTENT_CHECK,
            looks_for: "values against the key and rules declared",
            applies_to: reach(intent.declared.len(), "declared"),
            outcome: if !intent.measured {
                Outcome::Unavailable("values not read")
            } else if report.no_rows {
                Outcome::Unavailable(NO_ROWS)
            } else {
                found(report, INTENT_CHECK)
            },
            basis: intent.precision,
        });
    }
    checks.extend([
        Check {
            name: "Missing values",
            looks_for: "nulls in any column",
            applies_to: reach(all, ""),
            outcome: outcome("Missing values", all, "no columns"),
            basis: values,
        },
        Check {
            name: "NaN or infinite",
            looks_for: "NaN or +/-infinity in float columns",
            applies_to: reach(floats, "float"),
            outcome: outcome("NaN or infinite", floats, "no float columns"),
            basis: values,
        },
        Check {
            name: "Duplicate rows",
            looks_for: "rows identical in every column",
            applies_to: "whole rows".to_string(),
            outcome: match results.identity.as_ref() {
                _ if !values_read => Outcome::Unavailable("values not read"),
                _ if report.no_rows => Outcome::Unavailable(NO_ROWS),
                Some(identity) if identity.extra_rows > 0 => Outcome::Found {
                    tier: Severity::Problem,
                    detail: format!("{} extra rows", numfmt::group_chrome(identity.extra_rows)),
                },
                Some(_) => Outcome::Passed,
                None => Outcome::Unavailable("not measured"),
            },
            basis: values,
        },
        Check {
            name: "Blank text",
            looks_for: "text that is empty or only whitespace",
            applies_to: reach(texts, "text"),
            outcome: outcome("Blank text", texts, "no text columns"),
            basis: values,
        },
        Check {
            name: "Mixed spellings",
            looks_for: "one value in several cases or spacings",
            applies_to: reach(texts, "text"),
            outcome: outcome("Mixed spellings", texts, "no text columns"),
            basis: values,
        },
        Check {
            name: "Type mismatch",
            looks_for: "a column typed differently by some files",
            applies_to: files_reach.clone(),
            outcome: by_files("Type mismatch"),
            basis: QualityPrecision::Metadata,
        },
        Check {
            name: "Missing in files",
            looks_for: "a column some files do not have",
            applies_to: files_reach,
            outcome: by_files("Missing in files"),
            basis: QualityPrecision::Metadata,
        },
        Check {
            name: "Numbers as text",
            looks_for: "text that reads as numbers or dates",
            applies_to: reach(texts, "text"),
            outcome: outcome("Numbers as text", texts, "no text columns"),
            basis: values,
        },
        Check {
            name: "Nearly unique",
            looks_for: "a would-be key whose values repeat",
            applies_to: reach(keys, "integer/text"),
            outcome: if keys > 0 && values_read && results.precision != QualityPrecision::Exact {
                Outcome::Unavailable("needs every row checked")
            } else {
                match outcome("Nearly unique", keys, "no integer or text columns") {
                    // A declared key's repeats replace the note on that column; the
                    // check found them all the same.
                    Outcome::Passed if declared_key_repeats(results) => Outcome::Found {
                        tier: Severity::Problem,
                        detail: "the declared key repeats".to_string(),
                    },
                    outcome => outcome,
                }
            },
            basis: values,
        },
        Check {
            name: "Single value",
            looks_for: "a column with one value throughout",
            applies_to: reach(all, ""),
            outcome: outcome("Single value", all, "no columns"),
            basis: values,
        },
    ]);
    checks
}

/// Whether a one-column declared key repeats: the case whose "Nearly unique" note
/// the key's own finding replaces.
fn declared_key_repeats(results: &DataQualityResults) -> bool {
    results
        .intent
        .as_ref()
        .and_then(|intent| intent.key.as_ref())
        .is_some_and(|key| key.columns.len() == 1 && key.rows_involved > 0)
}

/// The check the declared intent makes, by the name the Checks list gives it.
pub const INTENT_CHECK: &str = "Column intent";

/// What the findings `check` makes found: how many columns, at their worst severity.
fn found(report: &QualityReport, check: &str) -> Outcome {
    let mut columns = Vec::new();
    let mut tier = Severity::Note;
    for finding in report
        .findings
        .iter()
        .filter(|finding| finding.check() == Some(check))
    {
        tier = tier.min(finding.severity);
        for column in &finding.columns {
            if !columns.contains(column) {
                columns.push(column.clone());
            }
        }
    }
    match columns.len() {
        0 => Outcome::Passed,
        count => Outcome::Found {
            tier,
            detail: format!(
                "{} {}",
                numfmt::group_chrome(count),
                if count == 1 { "column" } else { "columns" }
            ),
        },
    }
}

/// A segment with fewer sampled rows than this is thin: only a large change in it
/// clears the sampling noise, and a clean one says little.
pub const THIN_SEGMENT_ROWS: usize = 30;

/// How far the findings reach, beside them on every report: the checks that ran
/// and what they read, the ones that did not and why, the rows behind the numbers,
/// and what else bounds them. From what the run measured and saw; nothing here
/// reads.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Coverage {
    /// Checks that ran over every row in scope.
    pub exact: usize,
    /// Checks that ran over a sample.
    pub sampled: usize,
    /// Checks that ran over file footers.
    pub metadata: usize,
    /// Checks with nothing in the data to look at.
    pub skipped: usize,
    /// Checks that apply and this run could not answer: each reason, and the checks
    /// it kept from running.
    pub unavailable: Vec<(&'static str, Vec<&'static str>)>,
    /// The rows the numbers are over, with their denominator, and what the run's
    /// reads were seen to traverse.
    pub rows: Vec<String>,
    /// What else bounds the findings: thin segments, footers not read, time roles
    /// that measure nothing.
    pub limits: Vec<String>,
}

impl Coverage {
    /// Checks by what they read, then the ones that did not run: "6 sampled ·
    /// 2 metadata · 1 skipped · 1 unavailable".
    pub fn checks(&self) -> Vec<String> {
        let unavailable = self
            .unavailable
            .iter()
            .map(|(_, names)| names.len())
            .sum::<usize>();
        [
            (self.exact, QualityPrecision::Exact.label()),
            (self.sampled, QualityPrecision::Sampled.label()),
            (self.metadata, QualityPrecision::Metadata.label()),
            (self.skipped, "skipped"),
            (unavailable, "unavailable"),
        ]
        .into_iter()
        .filter(|(count, _)| *count > 0)
        .map(|(count, label)| format!("{} {label}", numfmt::group_chrome(count)))
        .collect()
    }

    /// Why each unavailable check did not run, then the other limits.
    pub fn limits(&self) -> Vec<String> {
        self.unavailable
            .iter()
            .map(|(reason, names)| match names.as_slice() {
                [name] => format!("{name}: {reason}"),
                _ => format!("{} checks: {reason}", names.len()),
            })
            .chain(self.limits.iter().cloned())
            .collect()
    }
}

pub fn coverage(
    results: &DataQualityResults,
    checks: &[Check],
    plan: &DataQualityPlan,
) -> Coverage {
    let mut coverage = Coverage::default();
    for check in checks {
        match &check.outcome {
            Outcome::Skipped(_) => coverage.skipped += 1,
            Outcome::Unavailable(reason) => {
                match coverage
                    .unavailable
                    .iter_mut()
                    .find(|(known, _)| known == reason)
                {
                    Some((_, names)) => names.push(check.name),
                    None => coverage.unavailable.push((reason, vec![check.name])),
                }
            }
            Outcome::Passed | Outcome::Found { .. } => match check.basis {
                QualityPrecision::Exact => coverage.exact += 1,
                QualityPrecision::Metadata => coverage.metadata += 1,
                QualityPrecision::Sampled => coverage.sampled += 1,
            },
        }
    }

    let count = numfmt::group_chrome;
    let evaluated = results.evaluated_rows;
    coverage
        .rows
        .push(match (results.precision, results.total_rows) {
            (QualityPrecision::Metadata, _) => "none read, file metadata only".to_string(),
            _ if evaluated == 0 => "none: the scope has no rows".to_string(),
            (QualityPrecision::Exact, _) => format!("all {} read, exact", count(evaluated)),
            (_, Some(total)) => format!(
                "{} of {} sampled ({})",
                count(evaluated),
                count(total),
                crate::numfmt::percent_of(evaluated, total)
            ),
            (_, None) => format!("{} sampled, total not counted", count(evaluated)),
        });
    if let Some(each) = results.per_value {
        coverage
            .rows
            .push(format!("up to {} per value", count(each)));
    }
    // Only what the reads were seen to do: a read that counted nothing says nothing.
    if let Some(reads) = results
        .reads
        .filter(|_| results.precision != QualityPrecision::Metadata)
    {
        if reads.reads == 0 {
            coverage.rows.push("no source read".to_string());
        } else if reads.counted == reads.reads {
            coverage
                .rows
                .push(format!("{} traversed", count(reads.rows)));
        } else if reads.counted > 0 {
            coverage
                .rows
                .push(format!("at least {} traversed", count(reads.rows)));
        }
        if let Some(copy) = reads.copy {
            let bytes = crate::numfmt::bytes(copy.bytes);
            coverage.rows.push(if copy.fetched {
                format!("passes read a local copy, fetched once ({bytes})")
            } else {
                format!("passes read a local copy fetched earlier ({bytes})")
            });
        }
    }

    let segments = &results.segments;
    if matches!(results.precision, QualityPrecision::Sampled) && segments.len() > 1 {
        let thin = segments
            .iter()
            .filter(|segment| segment.evaluated_rows < THIN_SEGMENT_ROWS)
            .count();
        if thin > 0 {
            coverage.limits.push(format!(
                "{} of {} segments under {THIN_SEGMENT_ROWS} sampled rows",
                count(thin),
                count(segments.len())
            ));
        }
    }
    if !results.unsampled_segments.is_empty() {
        coverage.limits.push(format!(
            "{} segments with rows, none sampled",
            count(results.unsampled_segments.len())
        ));
    }
    if let (Some(files), Some(read)) = (results.source_files, results.footers_read)
        && read < files
    {
        coverage.limits.push(format!(
            "footers of {} of {} files read",
            count(read),
            count(files)
        ));
    }
    if let Some(intent) = results.intent.as_ref().filter(|intent| intent.measured) {
        // A key with no repeat in a sample is unique among those rows, and no more.
        if intent.key.is_some() && intent.precision != QualityPrecision::Exact {
            coverage.limits.push(format!(
                "key repeats among {} sampled rows only",
                count(intent.evaluated_rows)
            ));
        }
    }
    if let Some(intent) = &results.intent
        && !intent.absent.is_empty()
    {
        coverage.limits.push(format!(
            "intent on {}: not in scope",
            columns_label(&intent.absent, 24)
        ));
    }
    if !plan.temporal_roles.is_empty() && plan.interval_pairs().is_empty() {
        coverage
            .limits
            .push("time roles form no interval".to_string());
    }
    coverage
}

/// What a finding means for the data and what to do about it, a fragment a line.
pub fn advice(finding: &Finding) -> Vec<String> {
    let Some(kind) = finding.kind else {
        return Vec::new();
    };
    if finding.variant == Some(Variant::MostlyMissing) {
        let rest = finding.evaluated_rows.saturating_sub(finding.affected_rows);
        return vec![
            if finding.columns.len() == 1 {
                format!(
                    "Aggregates and joins see only {}",
                    crate::numfmt::percent_of(rest, finding.evaluated_rows)
                )
            } else {
                "Aggregates and joins see only the filled rows".to_string()
            },
            "Check: filled only for some rows, or stopped at some point".to_string(),
        ];
    }
    finding
        .variant
        .and_then(Variant::advice)
        .unwrap_or(kind.spec().advice)
        .iter()
        .map(|line| line.to_string())
        .collect()
}

/// A finding as its detail reads it.
struct Detail<'a> {
    finding: &'a Finding,
    results: &'a DataQualityResults,
    /// "1,234 of 61,700 rows (2.0%)".
    of: String,
}

impl Detail<'_> {
    fn profile(&self, name: &str) -> Option<&ColumnQualityProfile> {
        self.results
            .columns
            .iter()
            .find(|profile| profile.name == name)
    }

    fn observation(&self, index: usize) -> &QualityObservation {
        &self.results.observations[index]
    }

    /// "1,234 of 61,700 values (2.0%) {what}".
    fn values(&self, what: &str) -> String {
        let finding = self.finding;
        format!(
            "{} of {} values ({}) {what}",
            numfmt::group_chrome(finding.affected_rows),
            numfmt::group_chrome(finding.evaluated_rows),
            crate::numfmt::percent_of(finding.affected_rows, finding.evaluated_rows)
        )
    }
}

/// The finding's numbers in a fragment, then the evidence that makes it concrete:
/// the values, the spellings, the files.
pub fn describe(finding: &Finding, results: &DataQualityResults) -> (String, Vec<String>) {
    let count = numfmt::group_chrome;
    let mut evidence = Vec::new();
    let Some(kind) = finding.kind else {
        let headline = format!(
            "{} {} passed every check",
            count(finding.columns.len()),
            if finding.columns.len() == 1 {
                "column"
            } else {
                "columns"
            }
        );
        return (headline, evidence);
    };
    let detail = Detail {
        finding,
        results,
        of: format!(
            "{} of {} rows ({})",
            count(finding.affected_rows),
            count(finding.evaluated_rows),
            crate::numfmt::percent_of(finding.affected_rows, finding.evaluated_rows)
        ),
    };
    let headline = (kind.spec().headline)(&detail, &mut evidence);
    (headline, evidence)
}

fn nulls_headline(detail: &Detail<'_>, evidence: &mut Vec<String>) -> String {
    let (finding, of) = (detail.finding, &detail.of);
    if finding.severity == Severity::Problem {
        format!(
            "Null in all {} rows checked",
            numfmt::group_chrome(finding.evaluated_rows)
        )
    } else if finding.same_rows {
        if finding.columns.len() == 2 {
            evidence.push("No row misses one without the other".to_string());
            format!("{of} null in both columns")
        } else {
            evidence.push("No row misses one of them without the others".to_string());
            format!("{of} null in all {} columns", finding.columns.len())
        }
    } else if finding.varied() {
        // Each column's own rate, worst first: the list row gave only the range.
        evidence.extend(finding.breakdown(detail.results));
        format!("Null rate in {} columns:", finding.columns.len())
    } else {
        per_column_headline(detail, evidence)
    }
}

/// "{of} {noun}", or the same of each column, one a line, when several columns are
/// grouped by what they miss.
fn per_column_headline(detail: &Detail<'_>, evidence: &mut Vec<String>) -> String {
    let (finding, of) = (detail.finding, &detail.of);
    let noun = finding.kind.map_or("", |kind| kind.spec().noun);
    if finding.lists_columns() {
        evidence.extend(finding.breakdown(detail.results));
        format!("{of} {noun} in each column:")
    } else {
        format!("{of} {noun}")
    }
}

fn non_finite_headline(detail: &Detail<'_>, evidence: &mut Vec<String>) -> String {
    let count = numfmt::group_chrome;
    if let Some(profile) = detail.profile(&detail.finding.columns[0]) {
        evidence.push(format!(
            "NaN {}, +inf {}, -inf {}",
            count(profile.nan_count.unwrap_or(0)),
            count(profile.positive_infinity_count.unwrap_or(0)),
            count(profile.negative_infinity_count.unwrap_or(0))
        ));
    }
    format!("{} NaN or infinite", detail.of)
}

fn constant_headline(detail: &Detail<'_>, evidence: &mut Vec<String>) -> String {
    let finding = detail.finding;
    let rows = numfmt::group_chrome(finding.evaluated_rows);
    if finding.columns.len() > 1 {
        for column in &finding.columns {
            if let Some(value) = detail
                .profile(column)
                .and_then(|p| p.dominant_value.as_ref())
            {
                evidence.push(format!("{column}: always {}", quoted(value, 40)));
            }
        }
        return format!("One value per column in {rows} rows");
    }
    match detail
        .profile(&finding.columns[0])
        .and_then(|p| p.dominant_value.as_ref())
    {
        Some(value) => format!("Always {} in {rows} rows", quoted(value, 40)),
        None => format!("One value in {rows} rows"),
    }
}

fn parseable_headline(detail: &Detail<'_>, evidence: &mut Vec<String>) -> String {
    let count = numfmt::group_chrome;
    let column = &detail.finding.columns[0];
    let profile = detail.profile(column);
    let (Some(profile), Some((parsed, reading))) = (profile, profile.and_then(text_reading)) else {
        return detail.finding.summary.clone();
    };
    if let (Some(min), Some(max)) = (profile.min_length, profile.max_length) {
        evidence.push(if min == max {
            format!("{min} characters each")
        } else {
            format!("{min} to {max} characters")
        });
    }
    if let Some(zeros) = profile.leading_zero_count.filter(|zeros| *zeros > 0) {
        evidence.push(format!("{} with a leading zero", count(zeros)));
    }
    if let (Some(min), Some(max)) = (&profile.min, &profile.max) {
        evidence.push(format!("From {} to {}", quoted(min, 24), quoted(max, 24)));
    }
    let failed = profile.non_null_rows().saturating_sub(parsed);
    if failed > 0 {
        let examples = detail
            .results
            .examples_of(ObservationKind::ParseableText, column);
        evidence.push(if examples.is_empty() {
            format!("{} do not parse", count(failed))
        } else {
            crate::glyphs::fit(
                &format!(
                    "{} do not parse, such as {}",
                    count(failed),
                    examples.join(", ")
                ),
                EXAMPLE_WIDTH,
            )
        });
    }
    format!(
        "{} of {} values ({}) parse as {}",
        count(parsed),
        count(profile.non_null_rows()),
        crate::numfmt::percent_of(parsed, profile.non_null_rows()),
        reading.label()
    )
}

fn duplicates_headline(detail: &Detail<'_>, evidence: &mut Vec<String>) -> String {
    let count = numfmt::group_chrome;
    let Some(identity) = detail.results.identity.as_ref() else {
        return detail.finding.summary.clone();
    };
    evidence.push(format!(
        "{} of {} rows ({}) have a copy",
        count(identity.rows_involved),
        count(identity.evaluated_rows),
        crate::numfmt::percent_of(identity.rows_involved, identity.evaluated_rows)
    ));
    if !identity.examples.is_empty() {
        evidence.push("Most copied:".to_string());
    }
    for example in &identity.examples {
        evidence.push(crate::glyphs::fit(
            &format!(
                "{}{}  {}",
                crate::glyphs::get().times,
                example.copies,
                example.values.join(", ")
            ),
            EXAMPLE_WIDTH,
        ));
    }
    format!(
        "{} rows repeated; {} extra {}",
        count(identity.duplicate_groups),
        count(identity.extra_rows),
        if identity.extra_rows == 1 {
            "copy"
        } else {
            "copies"
        }
    )
}

fn variants_headline(detail: &Detail<'_>, evidence: &mut Vec<String>) -> String {
    let count = numfmt::group_chrome;
    for index in &detail.finding.observations {
        let normalized = detail.observation(*index).normalized_category.as_ref();
        let Some(group) = detail
            .results
            .category_variants
            .iter()
            .find(|group| Some(&group.normalized) == normalized)
        else {
            continue;
        };
        let mut variants = group.variants.iter().collect::<Vec<_>>();
        variants.sort_by_key(|(_, rows)| std::cmp::Reverse(*rows));
        evidence.push(
            variants
                .into_iter()
                .take(4)
                .map(|(value, rows)| format!("{} ({})", quoted(value, 28), count(*rows)))
                .collect::<Vec<_>>()
                .join("  "),
        );
    }
    let values = detail.finding.observations.len();
    format!(
        "{} {} spelled more than one way, {}",
        count(values),
        if values == 1 { "value" } else { "values" },
        detail.of
    )
}

fn key_like_headline(detail: &Detail<'_>, evidence: &mut Vec<String>) -> String {
    let count = numfmt::group_chrome;
    let Some(profile) = detail.profile(&detail.finding.columns[0]) else {
        return detail.finding.summary.clone();
    };
    if let (Some(value), Some(times)) = (&profile.dominant_value, profile.dominant_count) {
        evidence.push(format!(
            "Most repeated: {} ({} times)",
            quoted(value, 32),
            count(times)
        ));
    }
    format!(
        "{} distinct in {} rows; {} repeats",
        count(profile.distinct_count.unwrap_or(0)),
        count(profile.non_null_rows()),
        count(detail.finding.affected_rows)
    )
}

fn unparsed_time_headline(detail: &Detail<'_>, evidence: &mut Vec<String>) -> String {
    let observation = detail.observation(detail.finding.observations[0]);
    if let Some(format) = &observation.time_format {
        evidence.push(format!("Read as {} for this study only", format.label()));
    }
    let examples = detail
        .results
        .examples_of(ObservationKind::UnparsedTime, &observation.column);
    if !examples.is_empty() {
        evidence.push(crate::glyphs::fit(
            &format!("Such as {}", examples.join(", ")),
            EXAMPLE_WIDTH,
        ));
    }
    detail.values("do not parse")
}

/// Each channel's runs or mean, then `{of} {noun}`.
fn signal_headline(detail: &Detail<'_>, evidence: &mut Vec<String>) -> String {
    signal_evidence(detail, evidence);
    let noun = detail.finding.kind.map_or("", |kind| kind.spec().noun);
    format!("{} {noun}", detail.of)
}

fn dc_offset_headline(detail: &Detail<'_>, evidence: &mut Vec<String>) -> String {
    signal_evidence(detail, evidence);
    "Mean away from zero".to_string()
}

fn signal_evidence(detail: &Detail<'_>, evidence: &mut Vec<String>) {
    for index in &detail.finding.observations {
        let observation = detail.observation(*index);
        evidence.push(format!("{}: {}", observation.column, observation.fact));
    }
}

fn files_headline(detail: &Detail<'_>, evidence: &mut Vec<String>) -> String {
    let count = numfmt::group_chrome;
    let finding = detail.finding;
    let observation = detail.observation(finding.observations[0]);
    // Read from the footers, so the denominator is the whole loaded source whatever
    // the plan's scope was.
    evidence.push(format!(
        "{} of {} rows of the loaded source ({})",
        count(finding.affected_rows),
        count(finding.evaluated_rows),
        crate::numfmt::percent_of(finding.affected_rows, finding.evaluated_rows)
    ));
    for file in observation.files.iter().take(4) {
        let stored = file
            .stored_type
            .as_ref()
            .map(|dtype| format!(" as {dtype}"))
            .unwrap_or_default();
        let examples = if file.examples.is_empty() {
            String::new()
        } else {
            format!(
                ": {}",
                file.examples
                    .iter()
                    .map(|value| quoted(value, 20))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };
        evidence.push(format!(
            "#{} {} ({} rows){stored}{examples}",
            file.number,
            file.name,
            count(file.rows)
        ));
    }
    if observation.files.len() > 4 {
        evidence.push(format!(
            "{} more {}",
            observation.files.len() - 4,
            crate::glyphs::get().ellipsis
        ));
    }
    upper_first(&observation.fact)
}

/// How wide a line of examples runs before it is cut: a row of many columns would
/// otherwise wrap over the whole detail.
const EXAMPLE_WIDTH: usize = 72;

/// A declared rule's violation, as the intent measured it: the count against what it
/// is out of, the rule as declared, and what the rows in memory showed of it. A
/// sample's numbers say so.
struct Declared<'a> {
    intent: &'a crate::quality_intent::IntentResults,
    sampled: bool,
    /// "rows", or "sampled rows".
    rows_word: &'static str,
    /// The finding's share of what it is out of.
    share: String,
    check: Option<&'a crate::quality_intent::ColumnCheck>,
    column: &'a str,
}

impl<'a> Declared<'a> {
    fn of(detail: &Detail<'a>) -> Option<Self> {
        let intent = detail.results.intent.as_ref()?;
        let finding = detail.finding;
        let column = finding.columns.first().map(String::as_str).unwrap_or("");
        let sampled = intent.precision != QualityPrecision::Exact;
        Some(Self {
            intent,
            sampled,
            rows_word: if sampled { "sampled rows" } else { "rows" },
            share: crate::numfmt::percent_of(finding.affected_rows, finding.evaluated_rows),
            check: intent.column(column),
            column,
        })
    }
}

/// Values and their rows: `"void" (3)  "x" (1)`.
fn value_examples(values: &[(String, usize)]) -> String {
    values
        .iter()
        .map(|(value, rows)| format!("{} ({})", quoted(value, 24), numfmt::group_chrome(*rows)))
        .collect::<Vec<_>>()
        .join("  ")
}

fn key_repeated_headline(detail: &Detail<'_>, evidence: &mut Vec<String>) -> String {
    let count = numfmt::group_chrome;
    let Some(declared) = Declared::of(detail) else {
        return detail.finding.summary.clone();
    };
    let Some(key) = declared.intent.key.as_ref() else {
        return detail.finding.summary.clone();
    };
    evidence.push(format!("Declared key: {}", key.columns.join(", ")));
    evidence.push(format!(
        "{} {} held by more than one row; {} rows beyond one per value",
        count(key.groups),
        if key.groups == 1 { "value" } else { "values" },
        count(key.extra_rows)
    ));
    if declared.sampled {
        evidence.push(
            "Each repeat here is one in the data; unsampled rows are not checked".to_string(),
        );
    }
    format!(
        "{} of {} {} ({}) share their key with another row",
        count(key.rows_involved),
        count(declared.intent.evaluated_rows),
        declared.rows_word,
        declared.share
    )
}

fn key_missing_headline(detail: &Detail<'_>, evidence: &mut Vec<String>) -> String {
    let count = numfmt::group_chrome;
    let Some(declared) = Declared::of(detail) else {
        return detail.finding.summary.clone();
    };
    let Some(key) = declared.intent.key.as_ref() else {
        return detail.finding.summary.clone();
    };
    evidence.push(format!("Declared key: {}", key.columns.join(", ")));
    format!(
        "{} of {} {} ({}) have no value in part of the key",
        count(key.missing),
        count(declared.intent.evaluated_rows),
        declared.rows_word,
        declared.share
    )
}

fn required_headline(detail: &Detail<'_>, evidence: &mut Vec<String>) -> String {
    let count = numfmt::group_chrome;
    let Some(declared) = Declared::of(detail) else {
        return detail.finding.summary.clone();
    };
    let column = declared.column;
    evidence.push(format!("Declared required: {column}"));
    format!(
        "{} of {} {} ({}) have no {column}",
        count(detail.finding.affected_rows),
        count(detail.finding.evaluated_rows),
        declared.rows_word,
        declared.share
    )
}

fn not_allowed_headline(detail: &Detail<'_>, evidence: &mut Vec<String>) -> String {
    let Some(declared) = Declared::of(detail) else {
        return detail.finding.summary.clone();
    };
    if let Some(check) = declared.check {
        evidence.push(format!("Allowed: {}", check.intent.allowed_label(8)));
        if !check.outside_examples.is_empty() {
            evidence.push(format!(
                "Found: {}",
                value_examples(&check.outside_examples)
            ));
        }
    }
    detail.values("are not allowed")
}

fn out_of_range_headline(detail: &Detail<'_>, evidence: &mut Vec<String>) -> String {
    let count = numfmt::group_chrome;
    let Some(declared) = Declared::of(detail) else {
        return detail.finding.summary.clone();
    };
    if let Some(check) = declared.check {
        evidence.push(format!(
            "Range: {}",
            check.intent.range_label().unwrap_or_default()
        ));
        if let Some(below) = check.below.filter(|below| *below > 0) {
            let lowest = check
                .lowest
                .as_ref()
                .map(|value| format!(", lowest {value}"))
                .unwrap_or_default();
            evidence.push(format!("Below: {}{lowest}", count(below)));
        }
        if let Some(above) = check.above.filter(|above| *above > 0) {
            let highest = check
                .highest
                .as_ref()
                .map(|value| format!(", highest {value}"))
                .unwrap_or_default();
            evidence.push(format!("Above: {}{highest}", count(above)));
        }
    }
    detail.values("outside the range")
}

fn unparsed_number_headline(detail: &Detail<'_>, evidence: &mut Vec<String>) -> String {
    let Some(declared) = Declared::of(detail) else {
        return detail.finding.summary.clone();
    };
    let reading = declared
        .check
        .and_then(|check| check.intent.number)
        .map_or("number", crate::quality_intent::NumberReading::label);
    if let Some(check) = declared
        .check
        .filter(|check| !check.unparsed_examples.is_empty())
    {
        evidence.push(format!(
            "Such as: {}",
            value_examples(&check.unparsed_examples)
        ));
    }
    evidence.push(format!("Read as a {reading} for this study only"));
    detail.values(&format!("do not read as a {reading}"))
}

fn upper_first(text: &str) -> String {
    let mut chars = text.chars();
    match chars.next() {
        Some(first) => first.to_uppercase().chain(chars).collect(),
        None => String::new(),
    }
}

/// Headline counts, one line: the answer before the evidence.
pub fn verdict(report: &QualityReport) -> String {
    // Footers still say which files lack a column or hold it in another type, so a
    // metadata run can find problems; it cannot call a column clean.
    if report.metadata_only {
        return match report.problems {
            0 => "Values not read: file metadata only".to_string(),
            1 => "1 problem in file metadata; values not read".to_string(),
            count => format!(
                "{} problems in file metadata; values not read",
                numfmt::group_chrome(count)
            ),
        };
    }
    // No rows is no evidence: nothing passed, and nothing is clean.
    if report.no_rows {
        return match report.problems {
            0 => "No rows to check".to_string(),
            1 => "1 problem in file metadata; no rows to check".to_string(),
            count => format!(
                "{} problems in file metadata; no rows to check",
                numfmt::group_chrome(count)
            ),
        };
    }
    let clean = format!(
        "{} of {} columns clean",
        numfmt::group_chrome(report.clean_columns),
        numfmt::group_chrome(report.total_columns)
    );
    let notes = match report.notes {
        0 => String::new(),
        1 => "1 note  ".to_string(),
        count => format!("{} notes  ", numfmt::group_chrome(count)),
    };
    match report.problems {
        0 => format!("No problems found  {notes}{clean}"),
        1 => format!("1 problem  {notes}{clean}"),
        count => format!("{} problems  {notes}{clean}", numfmt::group_chrome(count)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// What a finding is, for comparing: its kind and how it reads.
    fn reads(finding: &Finding) -> (Option<ObservationKind>, Option<Variant>) {
        (finding.kind, finding.variant)
    }
    use crate::data_quality::SharedNulls;
    use crate::data_quality::fixtures::{observation, profile, results_with};
    use polars::prelude::DataType;

    /// Sixteen columns missing on the same rows are one fact, and it says so.
    #[test]
    fn nulls_with_one_count_become_one_finding() {
        let names = ["open", "high", "low", "close"];
        let mut columns = names
            .iter()
            .map(|name| profile(name, DataType::Float64))
            .collect::<Vec<_>>();
        columns.push(profile("ticker", DataType::String));
        let mut results = results_with(
            columns,
            names
                .iter()
                .map(|name| observation(ObservationKind::Nulls, name, 4))
                .collect(),
        );
        results.shared_nulls = vec![SharedNulls {
            columns: names.iter().map(|name| name.to_string()).collect(),
            null_rows: 4,
            rows_null_in_all: 4,
        }];
        let report = build_report(&results);
        assert_eq!(report.findings.len(), 2, "one note and the clean entry");
        let missing = &report.findings[0];
        assert_eq!(
            reads(missing),
            (Some(ObservationKind::Nulls), Some(Variant::MissingTogether))
        );
        assert_eq!(missing.severity, Severity::Note);
        assert_eq!(missing.columns.len(), 4);
        assert!(missing.same_rows);
        assert_eq!(missing.summary, "4 rows (4.0%)");
        assert_eq!(report.findings[1].severity, Severity::Clean);
        assert_eq!(report.findings[1].columns, vec!["ticker".to_string()]);
        assert_eq!(report.problems, 0);
        assert!(verdict(&report).starts_with("No problems found"));
    }

    /// Missing values at different rates are one finding listing each column worst
    /// first; columns missing in most rows are their own note, ranked above it.
    #[test]
    fn missing_values_collapse_and_mostly_missing_leads() {
        let names = ["a", "b", "c", "d"];
        let results = results_with(
            names
                .iter()
                .map(|name| profile(name, DataType::Float64))
                .collect(),
            vec![
                observation(ObservationKind::Nulls, "a", 3),
                observation(ObservationKind::Nulls, "b", 40),
                observation(ObservationKind::Nulls, "c", 12),
                observation(ObservationKind::Nulls, "d", 80),
            ],
        );
        let report = build_report(&results);
        assert_eq!(report.findings.len(), 2);
        let mostly = &report.findings[0];
        assert_eq!(
            reads(mostly),
            (Some(ObservationKind::Nulls), Some(Variant::MostlyMissing))
        );
        assert_eq!(mostly.columns, vec!["d".to_string()]);
        let missing = &report.findings[1];
        assert_eq!(reads(missing), (Some(ObservationKind::Nulls), None));
        assert_eq!(missing.columns, vec!["b", "c", "a"]);
        assert_eq!(missing.summary, "3.0% to 40.0% per column");
        let (headline, evidence) = describe(missing, &results);
        assert_eq!(headline, "Null rate in 3 columns:");
        assert_eq!(evidence[0], "b    40.0%  40 rows");
        assert_eq!(evidence[2], "a     3.0%  3 rows");
    }

    #[test]
    fn problems_rank_before_notes_and_mark_their_columns() {
        let results = results_with(
            vec![
                profile("price", DataType::Float64),
                profile("region", DataType::String),
            ],
            vec![
                observation(ObservationKind::Nulls, "region", 3),
                observation(ObservationKind::NonFinite, "price", 2),
            ],
        );
        let report = build_report(&results);
        assert_eq!(
            reads(&report.findings[0]),
            (Some(ObservationKind::NonFinite), None)
        );
        assert_eq!(report.findings[0].severity, Severity::Problem);
        assert_eq!(
            reads(&report.findings[1]),
            (Some(ObservationKind::Nulls), None)
        );
        assert_eq!(
            report.column_status,
            vec![Severity::Problem, Severity::Note]
        );
        assert_eq!(report.clean_columns, 0);
        assert_eq!(verdict(&report), "1 problem  1 note  0 of 2 columns clean");
    }

    #[test]
    fn a_column_with_no_values_at_all_is_a_problem() {
        let results = results_with(
            vec![profile("legacy", DataType::String)],
            vec![observation(ObservationKind::Nulls, "legacy", 100)],
        );
        let report = build_report(&results);
        assert_eq!(
            reads(&report.findings[0]),
            (Some(ObservationKind::Nulls), Some(Variant::AlwaysMissing))
        );
        assert_eq!(report.findings[0].severity, Severity::Problem);
    }

    /// Fixed-width digits with leading zeros are a code, and the report says the
    /// column is fine as text rather than asking for a cast.
    #[test]
    fn zero_padded_digits_read_as_codes() {
        let mut code = profile("industry", DataType::String);
        code.integer_parse_count = Some(100);
        code.decimal_parse_count = Some(100);
        code.leading_zero_count = Some(20);
        code.min_length = Some(4);
        code.max_length = Some(4);
        let results = results_with(
            vec![code],
            vec![observation(ObservationKind::ParseableText, "industry", 100)],
        );
        let finding = &build_report(&results).findings[0];
        assert_eq!(
            reads(finding),
            (
                Some(ObservationKind::ParseableText),
                Some(Variant::CodesAsText)
            )
        );
        assert_eq!(finding.summary, "4 digits, leading zeros");
    }

    /// Footers can show a problem without reading a value; nothing can be called
    /// clean that way.
    #[test]
    fn a_metadata_run_reports_footer_problems_and_no_clean_columns() {
        let mut results = results_with(
            vec![
                profile("fee", DataType::Float64),
                profile("id", DataType::Int64),
            ],
            vec![observation(ObservationKind::Absent, "fee", 20)],
        );
        results.precision = QualityPrecision::Metadata;
        let report = build_report(&results);
        assert_eq!(report.findings.len(), 1, "no clean entry");
        assert_eq!(
            reads(&report.findings[0]),
            (Some(ObservationKind::Absent), None)
        );
        assert_eq!(
            verdict(&report),
            "1 problem in file metadata; values not read"
        );
    }

    /// The checks say what they covered, what they found, and what they could not
    /// look at and why, so a clean result is one the reader can trust.
    #[test]
    fn checks_report_reach_findings_and_what_did_not_run() {
        let mut results = results_with(
            vec![
                profile("price", DataType::Float64),
                profile("region", DataType::String),
                profile("id", DataType::Int64),
            ],
            vec![observation(ObservationKind::NonFinite, "price", 2)],
        );
        results.precision = QualityPrecision::Sampled;
        let report = build_report(&results);
        let list = checks(&results, &report);
        let by_name = |name: &str| list.iter().find(|check| check.name == name).unwrap();
        assert_eq!(list[0].name, "Missing values", "most important first");
        assert_eq!(by_name("Missing values").outcome, Outcome::Passed);
        assert_eq!(by_name("Missing values").applies_to, "3 columns");
        assert_eq!(by_name("NaN or infinite").applies_to, "1 float column");
        assert_eq!(
            by_name("NaN or infinite").outcome,
            Outcome::Found {
                tier: Severity::Problem,
                detail: "1 column".to_string()
            }
        );
        assert_eq!(
            by_name("Nearly unique").outcome,
            Outcome::Unavailable("needs every row checked"),
            "a sample cannot say a column is nearly a key"
        );
        assert_eq!(
            by_name("Type mismatch").outcome,
            Outcome::Skipped("needs several files")
        );

        results.precision = QualityPrecision::Metadata;
        results.source_files = Some(3);
        let report = build_report(&results);
        let list = checks(&results, &report);
        let by_name = |name: &str| list.iter().find(|check| check.name == name).unwrap();
        assert_eq!(
            by_name("Missing values").outcome,
            Outcome::Unavailable("values not read")
        );
        assert_eq!(by_name("Type mismatch").outcome, Outcome::Passed);
        assert_eq!(by_name("Type mismatch").applies_to, "3 files");
    }

    /// Coverage tells a clean sample from an exhaustive run: which checks ran over
    /// what, which had nothing to look at, which this run could not answer and why,
    /// and what else bounds the result, each count beside its denominator.
    #[test]
    fn coverage_separates_checked_skipped_and_unavailable() {
        use crate::data_quality::{
            ObservedReads, SegmentQualityProfile, TemporalRole, TemporalRoleAssignment,
        };
        let columns = vec![
            profile("price", DataType::Float64),
            profile("region", DataType::String),
            profile("id", DataType::Int64),
        ];
        let plan = DataQualityPlan::default();
        let measured = |mut results: DataQualityResults| {
            results.identity = Some(crate::data_quality::IdentityProfile {
                duplicate_groups: 0,
                extra_rows: 0,
                rows_involved: 0,
                evaluated_rows: results.evaluated_rows,
                examples: Vec::new(),
            });
            results
        };

        // A clean sampled run of one file, its sample streamed from 1,000 rows.
        let mut sampled = measured(results_with(columns.clone(), Vec::new()));
        sampled.precision = QualityPrecision::Sampled;
        sampled.total_rows = Some(1_000);
        sampled.reads = Some(ObservedReads {
            reads: 1,
            counted: 1,
            rows: 1_000,
            copy: None,
        });
        let report = build_report(&sampled);
        assert_eq!(report.problems + report.notes, 0, "a clean report");
        let found = coverage(&sampled, &checks(&sampled, &report), &plan);
        assert_eq!(
            found.checks(),
            ["7 sampled", "2 skipped", "1 unavailable"],
            "{found:?}"
        );
        assert_eq!(
            found.rows,
            ["100 of 1,000 sampled (10.0%)", "1,000 traversed"]
        );
        assert_eq!(found.limits(), ["Nearly unique: needs every row checked"]);

        // Thin segments, footers read for only some files, roles that pair nothing.
        let segment = |label: &str, rows: usize| SegmentQualityProfile {
            label: label.to_string(),
            total_rows: Some(500),
            evaluated_rows: rows,
            columns: Vec::new(),
            null_cells: 0,
            null_rate: 0.0,
            compared_with: None,
            largest_change: None,
            change_size: None,
        };
        sampled.segments = vec![segment("a", 90), segment("b", 10), segment("c", 0)];
        sampled.source_files = Some(400);
        sampled.footers_read = Some(100);
        let roles = DataQualityPlan {
            temporal_roles: vec![TemporalRoleAssignment {
                role: TemporalRole::Event,
                column: "id".to_string(),
                timezone: None,
            }],
            ..plan.clone()
        };
        let report = build_report(&sampled);
        let list = checks(&sampled, &report);
        let type_mismatch = list.iter().find(|c| c.name == "Type mismatch").unwrap();
        assert_eq!(type_mismatch.applies_to, "100 of 400 files");
        assert_eq!(type_mismatch.basis, QualityPrecision::Metadata);
        let found = coverage(&sampled, &list, &roles);
        assert_eq!(found.checks(), ["7 sampled", "2 metadata", "1 unavailable"]);
        assert_eq!(
            found.limits(),
            [
                "Nearly unique: needs every row checked",
                "2 of 3 segments under 30 sampled rows",
                "footers of 100 of 400 files read",
                "time roles form no interval",
            ]
        );

        // Every row read: nothing unavailable, and the passes' rows beside the total.
        let mut full = measured(results_with(columns.clone(), Vec::new()));
        full.reads = Some(ObservedReads {
            reads: 4,
            counted: 3,
            rows: 300,
            copy: None,
        });
        let report = build_report(&full);
        let found = coverage(&full, &checks(&full, &report), &plan);
        assert_eq!(found.checks(), ["8 exact", "2 skipped"]);
        assert_eq!(
            found.rows,
            ["all 100 read, exact", "at least 300 traversed"]
        );
        assert!(found.limits().is_empty());

        // No values read: the footers are all that was checked.
        let mut metadata = measured(results_with(columns, Vec::new()));
        metadata.precision = QualityPrecision::Metadata;
        metadata.source_files = Some(3);
        metadata.footers_read = Some(3);
        metadata.reads = Some(ObservedReads::default());
        let report = build_report(&metadata);
        let found = coverage(&metadata, &checks(&metadata, &report), &plan);
        assert_eq!(found.checks(), ["2 metadata", "8 unavailable"]);
        assert_eq!(found.rows, ["none read, file metadata only"]);
        assert_eq!(found.limits(), ["8 checks: values not read"]);

        // A check with no column of its kind is skipped whatever was read: the schema
        // says so without the values.
        let floats_only = vec![profile("price", DataType::Float64)];
        let mut metadata = measured(results_with(floats_only.clone(), Vec::new()));
        metadata.precision = QualityPrecision::Metadata;
        let report = build_report(&metadata);
        let found = coverage(&metadata, &checks(&metadata, &report), &plan);
        assert_eq!(found.checks(), ["6 skipped", "4 unavailable"], "{found:?}");
        let mut sampled = measured(results_with(floats_only, Vec::new()));
        sampled.precision = QualityPrecision::Sampled;
        let report = build_report(&sampled);
        let list = checks(&sampled, &report);
        let nearly = list.iter().find(|c| c.name == "Nearly unique").unwrap();
        assert_eq!(
            nearly.outcome,
            Outcome::Skipped("no integer or text columns")
        );
    }

    #[test]
    fn column_labels_fit_and_count_the_rest() {
        let columns = ["open", "high", "low", "close"].map(String::from);
        assert_eq!(columns_label(&columns, 40), "open, high, low, close");
        assert_eq!(columns_label(&columns, 14), "open, high +2");
        assert_eq!(columns_label(&columns[..1], 2), "open");
    }

    /// Narrowing and ordering read the report as it is: Problems stay above Notes in
    /// every order, a column or a check keeps only the findings that name it, and the
    /// rows a finding counts or the share they are decide the order within a
    /// severity.
    #[test]
    fn findings_narrow_and_order_without_measuring() {
        let mut results = results_with(
            vec![
                profile("price", DataType::Float64),
                profile("region", DataType::String),
                profile("note", DataType::String),
                profile("id", DataType::Int64),
            ],
            vec![
                observation(ObservationKind::NonFinite, "price", 2),
                observation(ObservationKind::Nulls, "region", 3),
                observation(ObservationKind::Nulls, "note", 30),
                observation(ObservationKind::Whitespace, "region", 9),
            ],
        );
        // A share over a smaller denominator: fewer rows, higher rate.
        results.observations[0].evaluated_rows = 4;
        let report = build_report(&results);
        let titles = |view: &FindingsView| {
            view.shown(&report)
                .into_iter()
                .map(|index| reads(&report.findings[index]))
                .collect::<Vec<_>>()
        };
        let ranked = FindingsView::default();
        assert_eq!(
            titles(&ranked),
            [
                (Some(ObservationKind::NonFinite), None),
                (Some(ObservationKind::Whitespace), None),
                (Some(ObservationKind::Nulls), None),
                (None, None)
            ]
        );
        let rows = FindingsView {
            order: FindingOrder::Rows,
            ..FindingsView::default()
        };
        assert_eq!(
            titles(&rows),
            [
                (Some(ObservationKind::Whitespace), None),
                (Some(ObservationKind::NonFinite), None),
                (Some(ObservationKind::Nulls), None),
                (None, None)
            ],
            "most rows first, Problems still above Notes"
        );
        let rate = FindingsView {
            order: FindingOrder::Rate,
            ..FindingsView::default()
        };
        assert_eq!(
            titles(&rate)[0],
            (Some(ObservationKind::NonFinite), None),
            "2 of 4 beats 9 of 100"
        );

        let region = FindingsView {
            column: Some("region".to_string()),
            ..FindingsView::default()
        };
        assert_eq!(
            titles(&region),
            [
                (Some(ObservationKind::Whitespace), None),
                (Some(ObservationKind::Nulls), None)
            ]
        );
        assert!(region.narrowed());
        let clean = FindingsView {
            column: Some("id".to_string()),
            ..FindingsView::default()
        };
        assert_eq!(titles(&clean), [(None, None)], "a clean column is clean");
        let missing = FindingsView {
            check: Some("Missing values"),
            ..FindingsView::default()
        };
        assert_eq!(titles(&missing), [(Some(ObservationKind::Nulls), None)]);
        assert_eq!(
            missing.selected(&report, 0).map(reads),
            Some((Some(ObservationKind::Nulls), None))
        );
        assert_eq!(
            check_choices(&report),
            [
                ("NaN or infinite", 1),
                ("Blank text", 1),
                ("Missing values", 1)
            ]
        );
        assert_eq!(
            column_choices(&report, &results)
                .into_iter()
                .map(|(_, count)| count)
                .collect::<Vec<_>>(),
            [1, 2, 1, 0]
        );
    }

    /// A finding over several columns lists each column's own count, and says the
    /// rows with any of them are a range nobody counted, not their sum.
    #[test]
    fn grouped_findings_break_down_by_column_and_bound_the_union() {
        let results = results_with(
            vec![
                profile("a", DataType::String),
                profile("b", DataType::String),
            ],
            vec![
                observation(ObservationKind::Empty, "a", 6),
                observation(ObservationKind::Empty, "b", 6),
            ],
        );
        let report = build_report(&results);
        let finding = &report.findings[0];
        assert!(finding.lists_columns());
        assert_eq!(
            finding.evidence_count(&results),
            None,
            "a union nobody counted"
        );
        let (headline, evidence) = describe(finding, &results);
        assert_eq!(
            headline,
            "6 of 100 rows (6.0%) empty strings in each column:"
        );
        assert_eq!(evidence[0], "a     6.0%  6 rows");
        assert_eq!(
            evidence.last().unwrap(),
            "Rows with any of them: 6 to 12, not counted"
        );
    }

    /// A parseable-text finding's rows are the values that stop a cast, counted
    /// from the profile; with none, there is nothing to open, and it says why.
    #[test]
    fn parse_failures_are_the_evidence_of_text_that_parses() {
        let mut text = profile("amount", DataType::String);
        text.null_count = 4;
        text.integer_parse_count = Some(95);
        text.decimal_parse_count = Some(95);
        let mut results = results_with(
            vec![text],
            vec![observation(ObservationKind::ParseableText, "amount", 95)],
        );
        results.examples = vec![crate::data_quality::FindingExamples {
            kind: ObservationKind::ParseableText,
            column: "amount".to_string(),
            values: vec!["\"n/a\"".to_string()],
        }];
        let report = build_report(&results);
        let finding = &report.findings[0];
        assert_eq!(finding.check(), Some("Numbers as text"));
        assert_eq!(finding.failures(&results), Some(1));
        assert_eq!(finding.evidence_count(&results), Some(1));
        assert!(matches!(
            finding.evidence(&results),
            Ok(EvidenceRows::Matching(_))
        ));
        let (_, evidence) = describe(finding, &results);
        assert!(
            evidence.contains(&"1 do not parse, such as \"n/a\"".to_string()),
            "{evidence:?}"
        );

        results.columns[0].integer_parse_count = Some(96);
        results.columns[0].decimal_parse_count = Some(96);
        results.observations[0].affected_rows = 96;
        let report = build_report(&results);
        let reason = report.findings[0].evidence(&results).unwrap_err();
        assert!(reason.contains("every value parses"), "{reason}");
    }

    /// Duplicate rows open as a group, and the count is every row with a copy.
    #[test]
    fn duplicate_rows_open_every_row_with_a_copy() {
        let mut results = results_with(
            vec![profile("id", DataType::Int64)],
            vec![observation(
                ObservationKind::DuplicateRows,
                "all columns",
                5,
            )],
        );
        results.identity = Some(crate::data_quality::IdentityProfile {
            duplicate_groups: 2,
            extra_rows: 3,
            rows_involved: 5,
            evaluated_rows: 100,
            examples: vec![crate::data_quality::DuplicateExample {
                copies: 3,
                values: vec!["7".to_string()],
            }],
        });
        let report = build_report(&results);
        let finding = &report.findings[0];
        assert!(matches!(
            finding.evidence(&results),
            Ok(EvidenceRows::Duplicates)
        ));
        assert_eq!(finding.evidence_count(&results), Some(5));
        let (headline, evidence) = describe(finding, &results);
        assert_eq!(headline, "2 rows repeated; 3 extra copies");
        assert_eq!(evidence[0], "5 of 100 rows (5.0%) have a copy");
        assert_eq!(evidence[2], format!("{}3  7", crate::glyphs::get().times));
    }
}

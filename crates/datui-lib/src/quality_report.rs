//! Data Quality results read as a report: what is likely wrong, what depends on
//! intent, and which columns have nothing to report.
//!
//! Derived from [`DataQualityResults`] whenever it is drawn, so the engine, the
//! session cache and the evidence drill-in keep working on observations; a finding
//! only groups and ranks them.

use crate::data_quality::{
    ColumnQualityProfile, DataQualityPlan, DataQualityResults, ObservationKind, QualityPrecision,
    QualityScope, TextReading, text_reading,
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

#[derive(Debug, Clone)]
pub struct Finding {
    pub severity: Severity,
    /// `None` for the clean-columns entry.
    pub kind: Option<ObservationKind>,
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

impl Finding {
    /// "open, high, low +7", fitted to `width` display columns.
    pub fn columns_label(&self, width: usize) -> String {
        columns_label(&self.columns, width)
    }

    /// Rows matching any of the grouped observations. Grouped columns missing on the
    /// same rows match the same rows either way; otherwise "any" is what the list
    /// promised: every row behind the finding.
    pub fn evidence_predicate(&self, results: &DataQualityResults) -> Option<Expr> {
        self.observations
            .iter()
            .map(|index| {
                let observation = results.observations.get(*index)?;
                match observation.kind {
                    // The values that stop a cast: text the reading does not parse.
                    ObservationKind::ParseableText => {
                        let profile = results
                            .columns
                            .iter()
                            .find(|profile| profile.name == observation.column)?;
                        crate::data_quality::unparsed_text(profile)
                    }
                    _ => observation.evidence_predicate(),
                }
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
        match self.kind {
            None => Err("No rows: every column here passed".to_string()),
            _ if results.precision == QualityPrecision::Metadata => {
                Err("No rows: values were not read".to_string())
            }
            Some(ObservationKind::DuplicateRows) => Ok(EvidenceRows::Duplicates),
            Some(ObservationKind::ParseableText) if self.failures(results) == Some(0) => {
                Err("No rows to show: every value parses, so nothing stops a cast".to_string())
            }
            _ => self
                .evidence_predicate(results)
                .map(EvidenceRows::Matching)
                .ok_or_else(|| "No rows: this finding names no rows to filter on".to_string()),
        }
    }

    /// How many rows Enter shows, where one count is all of them: one observation,
    /// the same rows in every column, or spellings of one column, which never
    /// overlap. `None` for a union nobody counted, and for the files' rows.
    pub fn evidence_count(&self, results: &DataQualityResults) -> Option<usize> {
        match self.kind? {
            ObservationKind::DuplicateRows => {
                Some(results.identity.as_ref()?.rows_involved).filter(|rows| *rows > 0)
            }
            ObservationKind::ParseableText => self.failures(results),
            // Rows beyond one per value are the count; the rows sharing a value are
            // what opens, and always more.
            ObservationKind::KeyLike | ObservationKind::Absent | ObservationKind::TypeConflict => {
                None
            }
            ObservationKind::CategoryVariants => Some(self.affected_rows),
            _ if self.observations.len() == 1 || self.same_rows => Some(self.affected_rows),
            _ => None,
        }
    }

    /// Non-null text in a parseable-text column that its reading does not parse.
    pub fn failures(&self, results: &DataQualityResults) -> Option<usize> {
        if self.kind != Some(ObservationKind::ParseableText) {
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
        Some(match self.kind? {
            ObservationKind::Nulls => "Missing values",
            ObservationKind::NonFinite => "NaN or infinite",
            ObservationKind::DuplicateRows => "Duplicate rows",
            ObservationKind::Empty | ObservationKind::Whitespace => "Blank text",
            ObservationKind::CategoryVariants => "Mixed spellings",
            ObservationKind::TypeConflict => "Type mismatch",
            ObservationKind::Absent => "Missing in files",
            ObservationKind::ParseableText => "Numbers as text",
            ObservationKind::KeyLike => "Nearly unique",
            ObservationKind::Constant => "Single value",
            ObservationKind::UnparsedTime => "Unparsed times",
        })
    }

    /// Missing values grouped across columns that go missing at different rates.
    pub fn varied(&self) -> bool {
        self.kind == Some(ObservationKind::Nulls)
            && self.columns.len() > 1
            && !self.same_rows
            && self.severity == Severity::Note
    }

    /// A count per column that the detail lists one column a line: several columns
    /// grouped by what they miss, not on the same rows.
    pub fn lists_columns(&self) -> bool {
        matches!(
            self.kind,
            Some(ObservationKind::Nulls | ObservationKind::Empty | ObservationKind::Whitespace)
        ) && self.columns.len() > 1
            && !self.same_rows
    }

    /// Each column's count and rate, one a line, worst first; then the rows with
    /// any of them, which no check counted: at least the largest column's count and
    /// at most their sum.
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
                    percent(row.affected_rows, row.evaluated_rows),
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
            Self::Rows => "By Rows",
            Self::Rate => "By Rate",
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
}

pub fn build_report(results: &DataQualityResults) -> QualityReport {
    let mut findings = Vec::new();
    // Group observations that say the same thing so the list says it once.
    let mut groups = BTreeMap::<(u8, usize, String), Vec<usize>>::new();
    for (index, observation) in results.observations.iter().enumerate() {
        let always_missing = observation.kind == ObservationKind::Nulls
            && observation.evaluated_rows > 0
            && observation.affected_rows == observation.evaluated_rows;
        let mostly_missing = observation.affected_rows * 2 > observation.evaluated_rows;
        // Columns missing on the very same rows are one fact about those rows; any
        // other missing values are one finding, with each column's rate inside it.
        let same_rows = results.shared_nulls.iter().any(|shared| {
            shared.same_rows()
                && shared.columns.len() > 1
                && shared.null_rows == observation.affected_rows
                && shared.columns.contains(&observation.column)
        });
        let kind = observation.kind as u8 + 1;
        let key = match observation.kind {
            ObservationKind::Nulls if always_missing => (0, 0, String::new()),
            ObservationKind::Nulls if mostly_missing => (kind, usize::MAX, "mostly".to_string()),
            ObservationKind::Nulls if same_rows => (kind, observation.affected_rows, String::new()),
            ObservationKind::Nulls => (kind, usize::MAX, String::new()),
            ObservationKind::Empty | ObservationKind::Whitespace => {
                (kind, observation.affected_rows, String::new())
            }
            ObservationKind::Constant => (kind, 0, String::new()),
            ObservationKind::CategoryVariants => (kind, 0, observation.column.clone()),
            // Everything else stands alone; the index keeps it from merging.
            _ => (u8::MAX, index, String::new()),
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
    let clean = results
        .columns
        .iter()
        .zip(&status)
        .filter(|(_, severity)| **severity == Severity::Clean)
        .map(|(profile, _)| profile.name.clone())
        .collect::<Vec<_>>();
    let clean_columns = clean.len();
    if !clean.is_empty() && !metadata_only {
        findings.push(Finding {
            severity: Severity::Clean,
            kind: None,
            title: "No findings",
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
    }
}

/// Order within a severity: what costs the most rows of trust first.
fn rank(finding: &Finding) -> u8 {
    match finding.kind {
        Some(ObservationKind::TypeConflict) => 0,
        Some(ObservationKind::Absent) => 1,
        Some(ObservationKind::DuplicateRows) => 2,
        Some(ObservationKind::UnparsedTime) => 3,
        Some(ObservationKind::Nulls) if finding.severity == Severity::Problem => 3,
        Some(ObservationKind::Nulls) if finding.title == "Mostly missing" => 9,
        Some(ObservationKind::NonFinite) => 4,
        Some(ObservationKind::CategoryVariants) => 5,
        Some(ObservationKind::Whitespace) => 6,
        Some(ObservationKind::Empty) => 7,
        Some(ObservationKind::Nulls) => 10,
        Some(ObservationKind::KeyLike) => 11,
        Some(ObservationKind::ParseableText) => 12,
        Some(ObservationKind::Constant) => 13,
        None => 20,
    }
}

fn finding(results: &DataQualityResults, indices: &[usize]) -> Finding {
    let mut indices = indices.to_vec();
    // Missing values list their columns, and mixed spellings their values, worst
    // first.
    if matches!(
        results.observations[indices[0]].kind,
        ObservationKind::Nulls | ObservationKind::CategoryVariants
    ) {
        indices.sort_by_key(|index| std::cmp::Reverse(results.observations[*index].affected_rows));
    }
    let indices = indices.as_slice();
    let first = &results.observations[indices[0]];
    let kind = first.kind;
    let mut columns = Vec::new();
    for index in indices {
        let column = &results.observations[*index].column;
        if !columns.contains(column) {
            columns.push(column.clone());
        }
    }
    let grouped = columns.len() > 1;
    let always_missing = kind == ObservationKind::Nulls
        && first.evaluated_rows > 0
        && first.affected_rows == first.evaluated_rows;
    let mostly_missing = kind == ObservationKind::Nulls
        && !always_missing
        && first.affected_rows * 2 > first.evaluated_rows;
    let severity = match kind {
        ObservationKind::Nulls if always_missing => Severity::Problem,
        ObservationKind::Nulls
        | ObservationKind::Constant
        | ObservationKind::ParseableText
        | ObservationKind::KeyLike => Severity::Note,
        ObservationKind::Empty
        | ObservationKind::Whitespace
        | ObservationKind::NonFinite
        | ObservationKind::DuplicateRows
        | ObservationKind::CategoryVariants
        | ObservationKind::Absent
        | ObservationKind::TypeConflict
        | ObservationKind::UnparsedTime => Severity::Problem,
    };
    let profile = results
        .columns
        .iter()
        .find(|profile| profile.name == first.column);
    let reading = profile.and_then(text_reading).map(|(_, reading)| reading);
    let shared = results.shared_nulls.iter().find(|shared| {
        shared.null_rows == first.affected_rows
            && columns.iter().all(|column| shared.columns.contains(column))
    });
    let same_rows =
        kind == ObservationKind::Nulls && grouped && shared.is_some_and(|s| s.same_rows());
    let title = match kind {
        ObservationKind::Nulls if always_missing => "Always missing",
        ObservationKind::Nulls if mostly_missing => "Mostly missing",
        // No row lacks one of these columns without lacking them all.
        ObservationKind::Nulls if same_rows => "Missing together",
        ObservationKind::Nulls => "Missing values",
        ObservationKind::Empty => "Empty text",
        ObservationKind::Whitespace => "Blank text",
        ObservationKind::NonFinite => "NaN or infinite",
        ObservationKind::Constant => "Single value",
        ObservationKind::ParseableText => match (reading, profile) {
            (Some(reading), Some(profile)) if reading.is_number() && is_code(profile) => {
                "Codes as text"
            }
            (Some(TextReading::Datetime | TextReading::Date), _) => "Dates as text",
            _ => "Numbers as text",
        },
        ObservationKind::DuplicateRows => "Duplicate rows",
        ObservationKind::CategoryVariants => "Mixed spellings",
        ObservationKind::Absent => "Missing in files",
        ObservationKind::TypeConflict => "Type mismatch",
        ObservationKind::KeyLike => "Nearly unique",
        ObservationKind::UnparsedTime => "Unparsed times",
    };
    let affected_rows = match kind {
        // One group per normalized value; the column's cost is all of them.
        ObservationKind::CategoryVariants => indices
            .iter()
            .map(|index| results.observations[*index].affected_rows)
            .sum(),
        _ => first.affected_rows,
    };
    let rows_each = |count: usize, each: &str| {
        format!(
            "{} {}{each} ({})",
            numfmt::group_chrome(count),
            if count == 1 { "row" } else { "rows" },
            percent(count, first.evaluated_rows)
        )
    };
    let rows = |count: usize| rows_each(count, "");
    let summary = match kind {
        ObservationKind::Nulls if always_missing => "no value in any row".to_string(),
        ObservationKind::Nulls if same_rows => rows(first.affected_rows),
        ObservationKind::Nulls if grouped => {
            let fewest = results.observations[indices[indices.len() - 1]].affected_rows;
            let (low, high) = (
                percent(fewest, first.evaluated_rows),
                percent(first.affected_rows, first.evaluated_rows),
            );
            if low == high {
                rows_each(first.affected_rows, " each")
            } else {
                format!("{low} to {high} per column")
            }
        }
        ObservationKind::Nulls | ObservationKind::Empty | ObservationKind::Whitespace
            if grouped =>
        {
            rows_each(first.affected_rows, " each")
        }
        ObservationKind::Nulls
        | ObservationKind::Empty
        | ObservationKind::Whitespace
        | ObservationKind::NonFinite => rows(first.affected_rows),
        ObservationKind::UnparsedTime => format!(
            "{} {} ({})",
            numfmt::group_chrome(first.affected_rows),
            if first.affected_rows == 1 {
                "value"
            } else {
                "values"
            },
            percent(first.affected_rows, first.evaluated_rows)
        ),
        ObservationKind::Constant if grouped => "one value each".to_string(),
        ObservationKind::Constant => profile
            .and_then(|profile| profile.dominant_value.as_ref())
            .map(|value| format!("always {}", quoted(value, 24)))
            .unwrap_or_else(|| "one value".to_string()),
        ObservationKind::ParseableText => match (reading, profile) {
            (Some(_), Some(profile)) if is_code(profile) => code_shape(profile),
            (Some(reading), Some(profile)) => format!(
                "{} parse as {}",
                percent(first.affected_rows, profile.non_null_rows()),
                reading.label()
            ),
            _ => first.fact.clone(),
        },
        ObservationKind::DuplicateRows => match results.identity.as_ref() {
            Some(identity) => format!(
                "{} extra {}",
                numfmt::group_chrome(identity.extra_rows),
                if identity.extra_rows == 1 {
                    "copy"
                } else {
                    "copies"
                }
            ),
            None => first.fact.clone(),
        },
        ObservationKind::CategoryVariants => {
            let example = results
                .category_variants
                .iter()
                .find(|group| Some(&group.normalized) == first.normalized_category.as_ref())
                .map(|group| {
                    group
                        .variants
                        .iter()
                        .take(2)
                        .map(|(value, _)| quoted(value, 24))
                        .collect::<Vec<_>>()
                        .join(" vs ")
                })
                .unwrap_or_default();
            // Several values: the count is the finding, and the detail lists them.
            if indices.len() == 1 {
                example
            } else {
                format!("{} values spelled more than one way", indices.len())
            }
        }
        ObservationKind::KeyLike => {
            let unique = profile
                .and_then(ColumnQualityProfile::uniqueness_rate)
                .map(|rate| format!(" ({:.1}% unique)", rate * 100.0))
                .unwrap_or_default();
            format!(
                "{} repeated{unique}",
                numfmt::group_chrome(first.affected_rows)
            )
        }
        ObservationKind::Absent | ObservationKind::TypeConflict => first.fact.clone(),
    };
    Finding {
        severity,
        kind: Some(kind),
        title,
        columns,
        observations: indices.to_vec(),
        affected_rows,
        evaluated_rows: first.evaluated_rows,
        summary,
        same_rows,
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

pub fn percent(count: usize, of: usize) -> String {
    if of == 0 {
        return "-".to_string();
    }
    let value = count as f64 / of as f64 * 100.0;
    // Two places below 1% so a small share never rounds to a misleading 0.0%.
    if value > 0.0 && value < 1.0 {
        format!("{value:.2}%")
    } else {
        format!("{value:.1}%")
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
    // Columns behind the findings a check produces, by the findings' titles.
    // Nothing to look at is known from the schema, whatever the run read.
    let outcome = |titles: &[&str], applies: usize, none: &'static str| {
        if applies == 0 {
            return Outcome::Skipped(none);
        }
        if !values_read {
            return Outcome::Unavailable("values not read");
        }
        found(report, titles)
    };
    let files = results.source_files.filter(|files| *files > 1);
    let by_files = |titles: &[&str]| match files {
        Some(_) => found(report, titles),
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
    vec![
        Check {
            name: "Missing values",
            looks_for: "nulls in any column",
            applies_to: reach(all, ""),
            outcome: outcome(
                &[
                    "Missing values",
                    "Missing together",
                    "Mostly missing",
                    "Always missing",
                ],
                all,
                "no columns",
            ),
            basis: values,
        },
        Check {
            name: "NaN or infinite",
            looks_for: "NaN or +/-infinity in float columns",
            applies_to: reach(floats, "float"),
            outcome: outcome(&["NaN or infinite"], floats, "no float columns"),
            basis: values,
        },
        Check {
            name: "Duplicate rows",
            looks_for: "rows identical in every column",
            applies_to: "whole rows".to_string(),
            outcome: match results.identity.as_ref() {
                _ if !values_read => Outcome::Unavailable("values not read"),
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
            outcome: outcome(&["Blank text", "Empty text"], texts, "no text columns"),
            basis: values,
        },
        Check {
            name: "Mixed spellings",
            looks_for: "one value in several cases or spacings",
            applies_to: reach(texts, "text"),
            outcome: outcome(&["Mixed spellings"], texts, "no text columns"),
            basis: values,
        },
        Check {
            name: "Type mismatch",
            looks_for: "a column typed differently by some files",
            applies_to: files_reach.clone(),
            outcome: by_files(&["Type mismatch"]),
            basis: QualityPrecision::Metadata,
        },
        Check {
            name: "Missing in files",
            looks_for: "a column some files do not have",
            applies_to: files_reach,
            outcome: by_files(&["Missing in files"]),
            basis: QualityPrecision::Metadata,
        },
        Check {
            name: "Numbers as text",
            looks_for: "text that reads as numbers or dates",
            applies_to: reach(texts, "text"),
            outcome: outcome(
                &["Numbers as text", "Dates as text", "Codes as text"],
                texts,
                "no text columns",
            ),
            basis: values,
        },
        Check {
            name: "Nearly unique",
            looks_for: "a would-be key whose values repeat",
            applies_to: reach(keys, "integer/text"),
            outcome: if keys > 0 && values_read && results.precision != QualityPrecision::Exact {
                Outcome::Unavailable("needs every row checked")
            } else {
                outcome(&["Nearly unique"], keys, "no integer or text columns")
            },
            basis: values,
        },
        Check {
            name: "Single value",
            looks_for: "a column with one value throughout",
            applies_to: reach(all, ""),
            outcome: outcome(&["Single value"], all, "no columns"),
            basis: values,
        },
    ]
}

fn found(report: &QualityReport, titles: &[&str]) -> Outcome {
    let mut columns = Vec::new();
    let mut tier = Severity::Note;
    for finding in report
        .findings
        .iter()
        .filter(|finding| finding.kind.is_some() && titles.contains(&finding.title))
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
                QualityPrecision::Sampled | QualityPrecision::Estimated => coverage.sampled += 1,
            },
        }
    }

    let count = numfmt::group_chrome;
    let evaluated = results.evaluated_rows;
    coverage
        .rows
        .push(match (results.precision, results.total_rows) {
            (QualityPrecision::Metadata, _) => "none read, file metadata only".to_string(),
            (QualityPrecision::Exact, _) => format!("all {} read, exact", count(evaluated)),
            (_, Some(total)) => format!(
                "{} of {} sampled ({})",
                count(evaluated),
                count(total),
                percent(evaluated, total)
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
    }

    let segments = &results.segments;
    if matches!(
        results.precision,
        QualityPrecision::Sampled | QualityPrecision::Estimated
    ) && segments.len() > 1
    {
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
    if !plan.temporal_roles.is_empty() && plan.interval_pairs().is_empty() {
        coverage
            .limits
            .push("time roles form no interval".to_string());
    }
    coverage
}

/// What a finding means for the data and what to do about it, a fragment a line.
/// The reader knows what a null is; this says only what is particular.
pub fn advice(finding: &Finding) -> Vec<String> {
    let lines: &[&str] = match (finding.kind, finding.title) {
        (Some(ObservationKind::Nulls), "Always missing") => {
            &["Carries nothing; check the load or a rename upstream"]
        }
        (Some(ObservationKind::Nulls), "Mostly missing") => {
            let rest = finding.evaluated_rows.saturating_sub(finding.affected_rows);
            return vec![
                if finding.columns.len() == 1 {
                    format!(
                        "Aggregates and joins see only {}",
                        percent(rest, finding.evaluated_rows)
                    )
                } else {
                    "Aggregates and joins see only the filled rows".to_string()
                },
                "Check: filled only for some rows, or stopped at some point".to_string(),
            ];
        }
        (Some(ObservationKind::Nulls), "Missing together") => {
            &["Likely one cause: a join with no match, or a source with gaps"]
        }
        (Some(ObservationKind::Nulls), _) => {
            &["Check: clustered in some files or dates (Segments, by file or window)"]
        }
        (Some(ObservationKind::Empty), _) => {
            &["Counted as filled; treat as null if it means missing"]
        }
        (Some(ObservationKind::Whitespace), _) => {
            &["Counted as filled; trim to null if it means missing"]
        }
        (Some(ObservationKind::NonFinite), _) => {
            &["Sums and means become NaN; check for division by zero upstream"]
        }
        (Some(ObservationKind::Constant), _) => {
            &["Tells no rows apart; a stuck feed if it should vary"]
        }
        (Some(ObservationKind::ParseableText), "Codes as text") => {
            &["Fine as text; cast only for arithmetic"]
        }
        (Some(ObservationKind::ParseableText), "Dates as text") => {
            &["Sorts as text; parse as a date to filter by range"]
        }
        (Some(ObservationKind::ParseableText), _) => {
            &["Sorts as text (\"10\" before \"9\"); cast to a number to sum"]
        }
        (Some(ObservationKind::DuplicateRows), _) => {
            &["Counted more than once; check for a double load or a join fan-out"]
        }
        (Some(ObservationKind::CategoryVariants), _) => {
            &["Group-bys and joins split them; trim and normalize case"]
        }
        (Some(ObservationKind::Absent), _) => &["Check: files written before the column existed"],
        (Some(ObservationKind::TypeConflict), _) => {
            &["Values dropped, not converted; read as text in Info, or fix the writer"]
        }
        (Some(ObservationKind::KeyLike), _) => {
            &["Duplicates if it is a key; expected if it is a measurement"]
        }
        (Some(ObservationKind::UnparsedTime), _) => &[
            "Left out of time windows and intervals, not counted as missing",
            "Check: another format, or values that are not times (Setup, e)",
        ],
        (None, _) => &[],
    };
    lines.iter().map(|line| line.to_string()).collect()
}

/// The finding's numbers in a fragment, then the evidence that makes it concrete:
/// the values, the spellings, the files.
pub fn describe(finding: &Finding, results: &DataQualityResults) -> (String, Vec<String>) {
    let count = |value: usize| numfmt::group_chrome(value);
    let of = format!(
        "{} of {} rows ({})",
        count(finding.affected_rows),
        count(finding.evaluated_rows),
        percent(finding.affected_rows, finding.evaluated_rows)
    );
    let profile = |name: &str| results.columns.iter().find(|profile| profile.name == name);
    let observation = |index: &usize| &results.observations[*index];
    let grouped = finding.columns.len() > 1;
    let mut evidence = Vec::new();
    let headline = match finding.kind {
        None => format!(
            "{} {} passed every check",
            count(finding.columns.len()),
            if finding.columns.len() == 1 {
                "column"
            } else {
                "columns"
            }
        ),
        Some(ObservationKind::Nulls) if finding.severity == Severity::Problem => {
            format!("Null in all {} rows checked", count(finding.evaluated_rows))
        }
        Some(ObservationKind::Nulls) if finding.same_rows => {
            if finding.columns.len() == 2 {
                evidence.push("No row misses one without the other".to_string());
                format!("{of} null in both columns")
            } else {
                evidence.push("No row misses one of them without the others".to_string());
                format!("{of} null in all {} columns", finding.columns.len())
            }
        }
        Some(ObservationKind::Nulls) if finding.varied() => {
            // Each column's own rate, worst first: the list row gave only the range.
            evidence.extend(finding.breakdown(results));
            format!("Null rate in {} columns:", finding.columns.len())
        }
        Some(ObservationKind::Nulls | ObservationKind::Empty | ObservationKind::Whitespace)
            if finding.lists_columns() =>
        {
            evidence.extend(finding.breakdown(results));
            let what = match finding.kind {
                Some(ObservationKind::Nulls) => "null",
                Some(ObservationKind::Empty) => "empty strings",
                _ => "only spaces or tabs",
            };
            format!("{of} {what} in each column:")
        }
        Some(ObservationKind::Nulls) => format!("{of} null"),
        Some(ObservationKind::Empty) => format!("{of} empty strings"),
        Some(ObservationKind::Whitespace) => format!("{of} only spaces or tabs"),
        Some(ObservationKind::NonFinite) => {
            if let Some(profile) = profile(&finding.columns[0]) {
                evidence.push(format!(
                    "NaN {}, +inf {}, -inf {}",
                    count(profile.nan_count.unwrap_or(0)),
                    count(profile.positive_infinity_count.unwrap_or(0)),
                    count(profile.negative_infinity_count.unwrap_or(0))
                ));
            }
            format!("{of} NaN or infinite")
        }
        Some(ObservationKind::Constant) if grouped => {
            for column in &finding.columns {
                if let Some(value) = profile(column).and_then(|p| p.dominant_value.as_ref()) {
                    evidence.push(format!("{column}: always {}", quoted(value, 40)));
                }
            }
            format!(
                "One value per column in {} rows",
                count(finding.evaluated_rows)
            )
        }
        Some(ObservationKind::Constant) => {
            match profile(&finding.columns[0]).and_then(|p| p.dominant_value.as_ref()) {
                Some(value) => format!(
                    "Always {} in {} rows",
                    quoted(value, 40),
                    count(finding.evaluated_rows)
                ),
                None => format!("One value in {} rows", count(finding.evaluated_rows)),
            }
        }
        Some(ObservationKind::ParseableText) => {
            let column = &finding.columns[0];
            let profile = profile(column);
            let reading = profile.and_then(text_reading);
            if let (Some(profile), Some((parsed, reading))) = (profile, reading) {
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
                    let examples = results.examples_of(ObservationKind::ParseableText, column);
                    evidence.push(if examples.is_empty() {
                        format!("{} do not parse", count(failed))
                    } else {
                        cut(
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
                    percent(parsed, profile.non_null_rows()),
                    reading.label()
                )
            } else {
                observation(&finding.observations[0]).fact.clone()
            }
        }
        Some(ObservationKind::DuplicateRows) => match results.identity.as_ref() {
            Some(identity) => {
                evidence.push(format!(
                    "{} of {} rows ({}) have a copy",
                    count(identity.rows_involved),
                    count(identity.evaluated_rows),
                    percent(identity.rows_involved, identity.evaluated_rows)
                ));
                if !identity.examples.is_empty() {
                    evidence.push("Most copied:".to_string());
                }
                for example in &identity.examples {
                    evidence.push(cut(
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
            None => observation(&finding.observations[0]).fact.clone(),
        },
        Some(ObservationKind::CategoryVariants) => {
            for index in &finding.observations {
                let normalized = observation(index).normalized_category.as_ref();
                let Some(group) = results
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
            let values = finding.observations.len();
            format!(
                "{} {} spelled more than one way, {of}",
                count(values),
                if values == 1 { "value" } else { "values" }
            )
        }
        Some(ObservationKind::KeyLike) => {
            let column = &finding.columns[0];
            if let Some(profile) = profile(column) {
                if let (Some(value), Some(times)) =
                    (&profile.dominant_value, profile.dominant_count)
                {
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
                    count(finding.affected_rows)
                )
            } else {
                observation(&finding.observations[0]).fact.clone()
            }
        }
        Some(ObservationKind::UnparsedTime) => {
            let observation = observation(&finding.observations[0]);
            if let Some(format) = &observation.time_format {
                evidence.push(format!("Read as {} for this study only", format.label()));
            }
            let examples = results.examples_of(ObservationKind::UnparsedTime, &observation.column);
            if !examples.is_empty() {
                evidence.push(cut(
                    &format!("Such as {}", examples.join(", ")),
                    EXAMPLE_WIDTH,
                ));
            }
            format!(
                "{} of {} values ({}) do not parse",
                count(finding.affected_rows),
                count(finding.evaluated_rows),
                percent(finding.affected_rows, finding.evaluated_rows)
            )
        }
        Some(ObservationKind::Absent | ObservationKind::TypeConflict) => {
            let observation = observation(&finding.observations[0]);
            // Read from the footers, so the denominator is the whole loaded source
            // whatever the plan's scope was.
            evidence.push(format!(
                "{} of {} rows of the loaded source ({})",
                count(finding.affected_rows),
                count(finding.evaluated_rows),
                percent(finding.affected_rows, finding.evaluated_rows)
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
    };
    (headline, evidence)
}

/// How wide a line of examples runs before it is cut: a row of many columns would
/// otherwise wrap over the whole detail.
const EXAMPLE_WIDTH: usize = 72;

/// `text` cut to `width` display columns, the ellipsis glyph marking the cut.
fn cut(text: &str, width: usize) -> String {
    if crate::glyphs::display_width(text) <= width {
        return text.to_string();
    }
    let ellipsis = crate::glyphs::get().ellipsis;
    format!(
        "{}{ellipsis}",
        crate::glyphs::take_columns(
            text,
            width.saturating_sub(crate::glyphs::display_width(ellipsis))
        )
    )
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
            0 => "Values not read: only file metadata was checked".to_string(),
            1 => "1 problem in file metadata; values not read".to_string(),
            count => format!(
                "{} problems in file metadata; values not read",
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
    use crate::data_quality::{DataQualityPlan, QualityObservation, SharedNulls};
    use polars::prelude::DataType;

    fn profile(name: &str, dtype: DataType) -> ColumnQualityProfile {
        ColumnQualityProfile {
            name: name.to_string(),
            dtype,
            evaluated_rows: 100,
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

    fn observation(kind: ObservationKind, column: &str, affected: usize) -> QualityObservation {
        QualityObservation {
            kind,
            column: column.to_string(),
            affected_rows: affected,
            evaluated_rows: 100,
            fact: String::new(),
            normalized_category: None,
            files: Vec::new(),
            time_format: None,
        }
    }

    fn results(
        columns: Vec<ColumnQualityProfile>,
        observations: Vec<QualityObservation>,
    ) -> DataQualityResults {
        let plan = DataQualityPlan::default();
        let mut results =
            DataQualityResults::empty(Some(100), &plan, &polars::prelude::Schema::default());
        results.evaluated_rows = 100;
        results.precision = QualityPrecision::Exact;
        results.columns = columns;
        results.observations = observations;
        results
    }

    /// Sixteen columns missing on the same rows are one fact, and it says so.
    #[test]
    fn nulls_with_one_count_become_one_finding() {
        let names = ["open", "high", "low", "close"];
        let mut columns = names
            .iter()
            .map(|name| profile(name, DataType::Float64))
            .collect::<Vec<_>>();
        columns.push(profile("ticker", DataType::String));
        let mut results = results(
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
        assert_eq!(missing.title, "Missing together");
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
        let results = results(
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
        assert_eq!(mostly.title, "Mostly missing");
        assert_eq!(mostly.columns, vec!["d".to_string()]);
        let missing = &report.findings[1];
        assert_eq!(missing.title, "Missing values");
        assert_eq!(missing.columns, vec!["b", "c", "a"]);
        assert_eq!(missing.summary, "3.0% to 40.0% per column");
        let (headline, evidence) = describe(missing, &results);
        assert_eq!(headline, "Null rate in 3 columns:");
        assert_eq!(evidence[0], "b    40.0%  40 rows");
        assert_eq!(evidence[2], "a     3.0%  3 rows");
    }

    #[test]
    fn problems_rank_before_notes_and_mark_their_columns() {
        let results = results(
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
        assert_eq!(report.findings[0].title, "NaN or infinite");
        assert_eq!(report.findings[0].severity, Severity::Problem);
        assert_eq!(report.findings[1].title, "Missing values");
        assert_eq!(
            report.column_status,
            vec![Severity::Problem, Severity::Note]
        );
        assert_eq!(report.clean_columns, 0);
        assert_eq!(verdict(&report), "1 problem  1 note  0 of 2 columns clean");
    }

    #[test]
    fn a_column_with_no_values_at_all_is_a_problem() {
        let results = results(
            vec![profile("legacy", DataType::String)],
            vec![observation(ObservationKind::Nulls, "legacy", 100)],
        );
        let report = build_report(&results);
        assert_eq!(report.findings[0].title, "Always missing");
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
        let results = results(
            vec![code],
            vec![observation(ObservationKind::ParseableText, "industry", 100)],
        );
        let finding = &build_report(&results).findings[0];
        assert_eq!(finding.title, "Codes as text");
        assert_eq!(finding.summary, "4 digits, leading zeros");
    }

    /// Footers can show a problem without reading a value; nothing can be called
    /// clean that way.
    #[test]
    fn a_metadata_run_reports_footer_problems_and_no_clean_columns() {
        let mut results = results(
            vec![
                profile("fee", DataType::Float64),
                profile("id", DataType::Int64),
            ],
            vec![observation(ObservationKind::Absent, "fee", 20)],
        );
        results.precision = QualityPrecision::Metadata;
        let report = build_report(&results);
        assert_eq!(report.findings.len(), 1, "no clean entry");
        assert_eq!(report.findings[0].title, "Missing in files");
        assert_eq!(
            verdict(&report),
            "1 problem in file metadata; values not read"
        );
    }

    /// The checks say what they covered, what they found, and what they could not
    /// look at and why, so a clean result is one the reader can trust.
    #[test]
    fn checks_report_reach_findings_and_what_did_not_run() {
        let mut results = results(
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
                precision: results.precision,
                examples: Vec::new(),
            });
            results
        };

        // A clean sampled run of one file, its sample streamed from 1,000 rows.
        let mut sampled = measured(results(columns.clone(), Vec::new()));
        sampled.precision = QualityPrecision::Sampled;
        sampled.total_rows = Some(1_000);
        sampled.reads = Some(ObservedReads {
            reads: 1,
            counted: 1,
            rows: 1_000,
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
        let mut full = measured(results(columns.clone(), Vec::new()));
        full.reads = Some(ObservedReads {
            reads: 4,
            counted: 3,
            rows: 300,
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
        let mut metadata = measured(results(columns, Vec::new()));
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
        let mut metadata = measured(results(floats_only.clone(), Vec::new()));
        metadata.precision = QualityPrecision::Metadata;
        let report = build_report(&metadata);
        let found = coverage(&metadata, &checks(&metadata, &report), &plan);
        assert_eq!(found.checks(), ["6 skipped", "4 unavailable"], "{found:?}");
        let mut sampled = measured(results(floats_only, Vec::new()));
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
        let mut results = results(
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
                .map(|index| report.findings[index].title)
                .collect::<Vec<_>>()
        };
        let ranked = FindingsView::default();
        assert_eq!(
            titles(&ranked),
            [
                "NaN or infinite",
                "Blank text",
                "Missing values",
                "No findings"
            ]
        );
        let rows = FindingsView {
            order: FindingOrder::Rows,
            ..FindingsView::default()
        };
        assert_eq!(
            titles(&rows),
            [
                "Blank text",
                "NaN or infinite",
                "Missing values",
                "No findings"
            ],
            "most rows first, Problems still above Notes"
        );
        let rate = FindingsView {
            order: FindingOrder::Rate,
            ..FindingsView::default()
        };
        assert_eq!(titles(&rate)[0], "NaN or infinite", "2 of 4 beats 9 of 100");

        let region = FindingsView {
            column: Some("region".to_string()),
            ..FindingsView::default()
        };
        assert_eq!(titles(&region), ["Blank text", "Missing values"]);
        assert!(region.narrowed());
        let clean = FindingsView {
            column: Some("id".to_string()),
            ..FindingsView::default()
        };
        assert_eq!(titles(&clean), ["No findings"], "a clean column is clean");
        let missing = FindingsView {
            check: Some("Missing values"),
            ..FindingsView::default()
        };
        assert_eq!(titles(&missing), ["Missing values"]);
        assert_eq!(
            missing.selected(&report, 0).map(|finding| finding.title),
            Some("Missing values")
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
        let results = results(
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
        let mut results = results(
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
        let mut results = results(
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
            precision: QualityPrecision::Exact,
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

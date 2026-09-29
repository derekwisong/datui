//! Data Quality results read as a report: what is likely wrong, what depends on
//! intent, and which columns have nothing to report.
//!
//! Derived from [`DataQualityResults`] whenever it is drawn, so the engine, the
//! session cache and the evidence drill-in keep working on observations; a finding
//! only groups and ranks them.

use crate::data_quality::{
    ColumnQualityProfile, DataQualityPlan, DataQualityResults, ObservationKind, QualityCompute,
    QualityGrain, QualityPrecision, QualityScope, TextReading, text_reading,
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
            .map(|index| results.observations.get(*index)?.evidence_predicate())
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

    /// Whether Enter can open the rows: files are named exactly at any budget, and a
    /// value predicate is exact only on an exact profile.
    pub fn can_open_rows(&self, results: &DataQualityResults) -> bool {
        self.evidence_scope(results).is_some()
            || (results.precision == QualityPrecision::Exact
                && self.evidence_predicate(results).is_some())
    }
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
        let kind = observation.kind as u8 + 1;
        let key = match observation.kind {
            ObservationKind::Nulls if always_missing => (0, 0, String::new()),
            ObservationKind::Nulls | ObservationKind::Empty | ObservationKind::Whitespace => {
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
        Some(ObservationKind::Nulls) if finding.severity == Severity::Problem => 3,
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
        | ObservationKind::TypeConflict => Severity::Problem,
    };
    let profile = results
        .columns
        .iter()
        .find(|profile| profile.name == first.column);
    let reading = profile.and_then(text_reading).map(|(_, reading)| reading);
    let title = match kind {
        ObservationKind::Nulls if always_missing => "Always missing",
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
    };
    let shared = results.shared_nulls.iter().find(|shared| {
        shared.null_rows == first.affected_rows
            && columns.iter().all(|column| shared.columns.contains(column))
    });
    let same_rows =
        kind == ObservationKind::Nulls && grouped && shared.is_some_and(|s| s.same_rows());
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
        ObservationKind::Nulls if same_rows => format!("same {}", rows(first.affected_rows)),
        ObservationKind::Nulls | ObservationKind::Empty | ObservationKind::Whitespace
            if grouped =>
        {
            rows_each(first.affected_rows, " each")
        }
        ObservationKind::Nulls
        | ObservationKind::Empty
        | ObservationKind::Whitespace
        | ObservationKind::NonFinite => rows(first.affected_rows),
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
                format!("{} values spelled 2+ ways", indices.len())
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

fn quoted(value: &str, width: usize) -> String {
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

/// What one check found: nothing, something, or nothing because it could not look.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Passed,
    Found { tier: Severity, detail: String },
    NotRun(&'static str),
}

/// One check the run makes: what it looks for, how far it reached, what it found.
/// The list is the answer to "checked for what?" when nothing turned up.
#[derive(Debug, Clone)]
pub struct Check {
    pub name: &'static str,
    pub looks_for: &'static str,
    pub applies_to: String,
    pub outcome: Outcome,
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
    let outcome = |titles: &[&str], applies: usize, none: &'static str| {
        if !values_read {
            return Outcome::NotRun("values not read");
        }
        if applies == 0 {
            return Outcome::NotRun(none);
        }
        found(report, titles)
    };
    let files = results.source_files.filter(|files| *files > 1);
    let by_files = |titles: &[&str]| match files {
        Some(_) => found(report, titles),
        None => Outcome::NotRun("needs several files"),
    };
    let files_reach = files
        .map(|files| format!("{} files", numfmt::group_chrome(files)))
        .unwrap_or_else(|| "files".to_string());
    vec![
        Check {
            name: "Missing values",
            looks_for: "nulls in any column",
            applies_to: reach(all, ""),
            outcome: outcome(&["Missing values", "Always missing"], all, "no columns"),
        },
        Check {
            name: "NaN or infinite",
            looks_for: "NaN or +/-infinity in float columns",
            applies_to: reach(floats, "float"),
            outcome: outcome(&["NaN or infinite"], floats, "no float columns"),
        },
        Check {
            name: "Duplicate rows",
            looks_for: "rows identical in every column",
            applies_to: "whole rows".to_string(),
            outcome: match results.identity.as_ref() {
                _ if !values_read => Outcome::NotRun("values not read"),
                Some(identity) if identity.extra_rows > 0 => Outcome::Found {
                    tier: Severity::Problem,
                    detail: format!("{} extra rows", numfmt::group_chrome(identity.extra_rows)),
                },
                Some(_) => Outcome::Passed,
                None => Outcome::NotRun("not measured"),
            },
        },
        Check {
            name: "Blank text",
            looks_for: "text that is empty or only whitespace",
            applies_to: reach(texts, "text"),
            outcome: outcome(&["Blank text", "Empty text"], texts, "no text columns"),
        },
        Check {
            name: "Mixed spellings",
            looks_for: "one value in several cases or spacings",
            applies_to: reach(texts, "text"),
            outcome: outcome(&["Mixed spellings"], texts, "no text columns"),
        },
        Check {
            name: "Type mismatch",
            looks_for: "a column typed differently by some files",
            applies_to: files_reach.clone(),
            outcome: by_files(&["Type mismatch"]),
        },
        Check {
            name: "Missing in files",
            looks_for: "a column some files do not have",
            applies_to: files_reach,
            outcome: by_files(&["Missing in files"]),
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
        },
        Check {
            name: "Nearly unique",
            looks_for: "a would-be key whose values repeat",
            applies_to: reach(keys, "integer/text"),
            outcome: if values_read && results.precision != QualityPrecision::Exact {
                Outcome::NotRun("needs every row checked")
            } else {
                outcome(&["Nearly unique"], keys, "no integer or text columns")
            },
        },
        Check {
            name: "Single value",
            looks_for: "a column with one value throughout",
            applies_to: reach(all, ""),
            outcome: outcome(&["Single value"], all, "no columns"),
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

/// Plain words for each kind of finding: what it is, why it matters, what to do.
pub struct Explanation {
    pub what: &'static str,
    pub why: &'static str,
    pub check: &'static str,
}

pub fn explain(finding: &Finding) -> Explanation {
    match (finding.kind, finding.title) {
        (Some(ObservationKind::Nulls), "Always missing") => Explanation {
            what: "Every row checked has no value in this column.",
            why: "The column carries no information.",
            check: "Whether it failed to load, was renamed upstream, or is only filled in elsewhere.",
        },
        (Some(ObservationKind::Nulls), _) => Explanation {
            what: "Rows with no value in this column.",
            why: "Counts, sums and averages skip them, and joins on them never match.",
            check: "Whether they cluster in certain rows, dates or files.",
        },
        (Some(ObservationKind::Empty), _) => Explanation {
            what: "Text values that are the empty string.",
            why: "They look filled in, so they are not counted as missing, but carry nothing.",
            check: "Whether the source means missing; if so, treat them as null.",
        },
        (Some(ObservationKind::Whitespace), _) => Explanation {
            what: "Text values made only of spaces or tabs.",
            why: "They look filled in, so they are not counted as missing, but carry nothing.",
            check: "Whether the source means missing; if so, trim them to null.",
        },
        (Some(ObservationKind::NonFinite), _) => Explanation {
            what: "Floating-point values that are NaN or infinite.",
            why: "One NaN turns a sum or mean into NaN; infinities distort min, max and averages.",
            check: "Division by zero or a failed calculation upstream.",
        },
        (Some(ObservationKind::Constant), _) => Explanation {
            what: "Every value in the rows checked is the same.",
            why: "The column tells no rows apart. If it should vary, the feed may be stuck.",
            check: "Whether that is by design. A sample can miss a rare second value; a full run settles it.",
        },
        (Some(ObservationKind::ParseableText), "Codes as text") => Explanation {
            what: "Text that reads as numbers, written with leading zeros or at a fixed width.",
            why: "That is the shape of a code, such as a ZIP or an industry code; as text it keeps its zeros.",
            check: "Nothing, if it is a code. Convert it only if you need to do arithmetic with it.",
        },
        (Some(ObservationKind::ParseableText), "Dates as text") => Explanation {
            what: "Text whose values read as ISO dates or datetimes.",
            why: "As text they sort as strings and cannot be filtered by a date range.",
            check: "Whether to parse the column as a date when loading.",
        },
        (Some(ObservationKind::ParseableText), _) => Explanation {
            what: "Text whose values read as numbers.",
            why: "Text sorts as strings (\"10\" before \"9\") and cannot be summed or averaged.",
            check: "Whether to cast it to a number when loading.",
        },
        (Some(ObservationKind::DuplicateRows), _) => Explanation {
            what: "Rows that are identical in every column.",
            why: "Counts and sums include them more than once.",
            check: "A file loaded twice, or a join that matched more rows than expected.",
        },
        (Some(ObservationKind::CategoryVariants), _) => Explanation {
            what: "Values that differ only in letter case or surrounding spaces.",
            why: "Grouping and joining treat each spelling as a different value.",
            check: "Whether to trim and normalize case before grouping.",
        },
        (Some(ObservationKind::Absent), _) => Explanation {
            what: "Some files have no such column, so their rows read it as missing.",
            why: "The column's gaps come from the files, not from the values.",
            check: "Whether those files predate the column.",
        },
        (Some(ObservationKind::TypeConflict), _) => Explanation {
            what: "Some files store this column in a type the dataset cannot read.",
            why: "Those files' values are dropped from the column, not converted.",
            check: "Read the column as text from Dataset Info, or fix the writer.",
        },
        (Some(ObservationKind::KeyLike), _) => Explanation {
            what: "Almost every value is different, but some repeat.",
            why: "If the column identifies rows (an ID or key), each repeat is a duplicate record.",
            check: "Whether it is meant to be unique. If it is a measurement, this is expected.",
        },
        (None, _) => Explanation {
            what: "No check found anything in these columns.",
            why: "",
            check: "",
        },
    }
}

/// The finding in one sentence with its numbers, then the evidence that makes it
/// concrete: the values, the spellings, the files.
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
            "{} {} passed every check.",
            count(finding.columns.len()),
            if finding.columns.len() == 1 {
                "column"
            } else {
                "columns"
            }
        ),
        Some(ObservationKind::Nulls) if finding.severity == Severity::Problem => format!(
            "{} no value in any of the {} rows checked.",
            if grouped {
                "These columns have"
            } else {
                "This column has"
            },
            count(finding.evaluated_rows)
        ),
        Some(ObservationKind::Nulls) if finding.same_rows => format!(
            "{of} have no value in any of these {} columns: the same rows every time.",
            finding.columns.len()
        ),
        Some(ObservationKind::Nulls) if grouped => {
            format!("{of} have no value, in each of these columns.")
        }
        Some(ObservationKind::Nulls) => format!("{of} have no value."),
        Some(ObservationKind::Empty) => format!("{of} hold an empty string."),
        Some(ObservationKind::Whitespace) => format!("{of} hold only spaces or tabs."),
        Some(ObservationKind::NonFinite) => {
            if let Some(profile) = profile(&finding.columns[0]) {
                evidence.push(format!(
                    "NaN {}, +infinity {}, -infinity {}",
                    count(profile.nan_count.unwrap_or(0)),
                    count(profile.positive_infinity_count.unwrap_or(0)),
                    count(profile.negative_infinity_count.unwrap_or(0))
                ));
            }
            format!("{of} are not ordinary numbers.")
        }
        Some(ObservationKind::Constant) => {
            for column in &finding.columns {
                if let Some(value) = profile(column).and_then(|p| p.dominant_value.as_ref()) {
                    evidence.push(format!("{column} is always {}", quoted(value, 40)));
                }
            }
            if grouped {
                "Each of these columns holds a single value in the rows checked.".to_string()
            } else {
                "Every value in the rows checked is the same.".to_string()
            }
        }
        Some(ObservationKind::ParseableText) => {
            let column = &finding.columns[0];
            let profile = profile(column);
            let reading = profile.and_then(text_reading);
            if let (Some(profile), Some((parsed, reading))) = (profile, reading) {
                if let (Some(min), Some(max)) = (profile.min_length, profile.max_length) {
                    evidence.push(if min == max {
                        format!("Every value is {min} characters long")
                    } else {
                        format!("Lengths run from {min} to {max} characters")
                    });
                }
                if let Some(zeros) = profile.leading_zero_count.filter(|zeros| *zeros > 0) {
                    evidence.push(format!("{} written with a leading zero", count(zeros)));
                }
                if let (Some(min), Some(max)) = (&profile.min, &profile.max) {
                    evidence.push(format!(
                        "Values run from {} to {}",
                        quoted(min, 24),
                        quoted(max, 24)
                    ));
                }
                format!(
                    "{} of {} values ({}) read as {}.",
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
            Some(identity) => format!(
                "{} rows appear more than once; removing the repeats would drop {} rows.",
                count(identity.duplicate_groups),
                count(identity.extra_rows)
            ),
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
                evidence.push(
                    group
                        .variants
                        .iter()
                        .take(4)
                        .map(|(value, rows)| format!("{} ({})", quoted(value, 28), count(*rows)))
                        .collect::<Vec<_>>()
                        .join("  "),
                );
            }
            let values = finding.observations.len();
            format!(
                "{} {} spelled more than one way, across {of}.",
                count(values),
                if values == 1 {
                    "value is"
                } else {
                    "values are"
                }
            )
        }
        Some(ObservationKind::KeyLike) => {
            let column = &finding.columns[0];
            if let Some(profile) = profile(column) {
                if let (Some(value), Some(times)) =
                    (&profile.dominant_value, profile.dominant_count)
                {
                    evidence.push(format!(
                        "Most repeated: {} appears {} times",
                        quoted(value, 32),
                        count(times)
                    ));
                }
                format!(
                    "{} distinct values in {} rows; {} rows repeat a value already seen.",
                    count(profile.distinct_count.unwrap_or(0)),
                    count(profile.non_null_rows()),
                    count(finding.affected_rows)
                )
            } else {
                observation(&finding.observations[0]).fact.clone()
            }
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
            format!("{}.", upper_first(&observation.fact))
        }
    };
    (headline, evidence)
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

/// What the numbers were measured on, in words: how many rows, picked how, from where.
pub fn coverage(results: &DataQualityResults, plan: &DataQualityPlan) -> String {
    let rows = |count: usize| {
        format!(
            "{} {}",
            numfmt::group_chrome(count),
            if count == 1 { "row" } else { "rows" }
        )
    };
    let scope = scope_phrase(&plan.scope);
    if results.precision == QualityPrecision::Metadata {
        return format!("No values read; file metadata only, {scope}");
    }
    let checked = results.evaluated_rows;
    let segments = results.segments.len();
    if results.precision == QualityPrecision::Exact
        || results.total_rows == Some(checked)
        || plan.compute == QualityCompute::Full
    {
        let all = if checked == 1 { "The only" } else { "All" };
        return format!("{all} {} {scope} checked", rows(checked));
    }
    if !matches!(plan.grain, QualityGrain::Dataset) {
        return format!(
            "Sampled {} across {} {}, {scope}",
            rows(checked),
            numfmt::group_chrome(segments),
            if segments == 1 { "segment" } else { "segments" }
        );
    }
    // Spread across the whole scope, which the sampler counts as it goes.
    match results.total_rows {
        Some(total) => format!(
            "Sampled {} of {} rows {scope}",
            numfmt::group_chrome(checked),
            numfmt::group_chrome(total)
        ),
        None => format!("Sampled {} {scope}", rows(checked)),
    }
}

fn scope_phrase(scope: &QualityScope) -> String {
    match scope {
        QualityScope::CurrentView => "in the current view".to_string(),
        QualityScope::WholeSource => "in the whole source".to_string(),
        QualityScope::FirstRows(rows) => format!("in rows 1-{}", numfmt::group_chrome(*rows)),
        QualityScope::ViewRows { start, end } => format!(
            "in rows {}-{}",
            numfmt::group_chrome(*start),
            numfmt::group_chrome(*end)
        ),
        QualityScope::SourceFiles(files) => format!(
            "in source {} {}",
            if files.len() == 1 { "file" } else { "files" },
            files
                .iter()
                .map(usize::to_string)
                .collect::<Vec<_>>()
                .join(",")
        ),
        QualityScope::SourcePartition { column, value } => {
            format!("where {column}={value}")
        }
        QualityScope::SourceTimeRange { column, start, end } => {
            format!("where {column} is in {start}..{end}")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_quality::{QualityObservation, SharedNulls};
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
        assert_eq!(missing.title, "Missing values");
        assert_eq!(missing.severity, Severity::Note);
        assert_eq!(missing.columns.len(), 4);
        assert!(missing.same_rows);
        assert_eq!(missing.summary, "same 4 rows (4.0%)");
        assert_eq!(report.findings[1].severity, Severity::Clean);
        assert_eq!(report.findings[1].columns, vec!["ticker".to_string()]);
        assert_eq!(report.problems, 0);
        assert!(verdict(&report).starts_with("No problems found"));
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
            Outcome::NotRun("needs every row checked"),
            "a sample cannot say a column is nearly a key"
        );
        assert_eq!(
            by_name("Type mismatch").outcome,
            Outcome::NotRun("needs several files")
        );

        results.precision = QualityPrecision::Metadata;
        results.source_files = Some(3);
        let report = build_report(&results);
        let list = checks(&results, &report);
        let by_name = |name: &str| list.iter().find(|check| check.name == name).unwrap();
        assert_eq!(
            by_name("Missing values").outcome,
            Outcome::NotRun("values not read")
        );
        assert_eq!(by_name("Type mismatch").outcome, Outcome::Passed);
        assert_eq!(by_name("Type mismatch").applies_to, "3 files");
    }

    #[test]
    fn column_labels_fit_and_count_the_rest() {
        let columns = ["open", "high", "low", "close"].map(String::from);
        assert_eq!(columns_label(&columns, 40), "open, high, low, close");
        assert_eq!(columns_label(&columns, 14), "open, high +2");
        assert_eq!(columns_label(&columns[..1], 2), "open");
    }

    #[test]
    fn coverage_names_the_sample_and_its_total() {
        let plan = DataQualityPlan::default();
        let mut results = results(Vec::new(), Vec::new());
        results.precision = QualityPrecision::Sampled;
        results.evaluated_rows = 10_000;
        results.total_rows = Some(200_000);
        assert_eq!(
            coverage(&results, &plan),
            "Sampled 10,000 of 200,000 rows in the current view"
        );
        results.precision = QualityPrecision::Exact;
        results.total_rows = Some(10_000);
        assert_eq!(
            coverage(&results, &plan),
            "All 10,000 rows in the current view checked"
        );
    }
}

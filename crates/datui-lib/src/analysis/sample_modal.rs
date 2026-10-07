//! The Sample form: the one place an analysis's rows are chosen, for every tool.
//! It edits a copy of the shared [`Sample`]; Enter applies it, Esc discards it.
//!
//! Every row is a setting whose value names itself: which rows ("All rows (36.8M)",
//! "Partitions", "Files", ...), how they are picked, how many, and the seed. Picking
//! a kind of rows shows only that kind's inputs, with the context they need (the
//! partition values that exist, the numbered files) and nothing else.

use crate::analysis::data_quality::QualityScope;
use crate::analysis::sampling::{Sample, SampleMethod};
use crate::widgets::text_input::TextInput;

/// Which rows the sample is drawn from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowsKind {
    /// The table as shown, query and filters applied.
    All,
    /// The loaded source, ignoring the query and filters. Offered only when those
    /// make it differ from the table as shown.
    Source,
    Partitions,
    Files,
    Range,
    Time,
}

/// A row of the form. Only the rows the chosen kind and method need are shown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SampleField {
    Rows,
    PartitionColumn,
    PartitionValues,
    Files,
    RangeFrom,
    RangeTo,
    TimeColumn,
    TimeFrom,
    TimeBefore,
    Method,
    By,
    Size,
    Seed,
}

impl SampleField {
    pub fn label(self) -> &'static str {
        match self {
            Self::Rows => "Rows from:",
            Self::PartitionColumn => "Partition:",
            Self::PartitionValues => "Values:",
            Self::Files => "Files:",
            Self::RangeFrom => "From row:",
            Self::RangeTo => "To row:",
            Self::TimeColumn => "Column:",
            Self::TimeFrom => "From:",
            Self::TimeBefore => "Before:",
            Self::Method => "Method:",
            Self::By => "Per value of:",
            Self::Size => "Sample size:",
            Self::Seed => "Random seed:",
        }
    }

    /// Whether the row is typed into rather than stepped with ←/→.
    pub fn is_text(self) -> bool {
        matches!(
            self,
            Self::PartitionValues
                | Self::Files
                | Self::RangeFrom
                | Self::RangeTo
                | Self::TimeFrom
                | Self::TimeBefore
                | Self::Size
                | Self::Seed
        )
    }
}

/// What the table knows that the form's choices depend on.
#[derive(Debug, Clone, Default)]
pub struct SampleContext {
    /// Rows in the table as shown, when counted.
    pub view_rows: Option<usize>,
    /// Whether a query, filter or reshape makes the table differ from its source.
    pub filtered: bool,
    /// The source's files, in inventory order.
    pub files: Vec<String>,
    pub partition_columns: Vec<String>,
    /// Values a partition column is known to hold, from the source's directory
    /// names, when there is no file inventory to read them from.
    pub partition_values: Vec<(String, Vec<String>)>,
    pub time_columns: Vec<String>,
    /// Columns an equal-per-value sample can split by.
    pub value_columns: Vec<String>,
}

pub struct SampleForm {
    /// Before a tool's first run the form sits in its empty pane, and Enter is what
    /// runs it; otherwise it floats over a result, and Enter applies a change.
    pub inline: bool,
    /// The form edits the view's sample, the step under its query: Every row is
    /// "No sample" there, which takes the sample away.
    pub view: bool,
    /// Bytes a row of the view takes, every column, as the table measured them:
    /// what the Size row's estimate of a sample of the view is worked out from.
    pub bytes_per_row: Option<usize>,
    /// The same of a row of the source, every column it has: a sample of the
    /// source (partitions, files, a time range, the source unfiltered) reads them.
    pub source_bytes_per_row: Option<usize>,
    /// The memory warning has been shown for the form as it stands: Enter again
    /// draws anyway. Any edit takes it back.
    pub anyway: bool,
    pub draft: Sample,
    pub kind: RowsKind,
    pub field: SampleField,
    pub context: SampleContext,
    pub partition_column: usize,
    pub partition_values: TextInput,
    pub files: TextInput,
    pub range_from: TextInput,
    pub range_to: TextInput,
    pub time_column: usize,
    pub time_from: TextInput,
    pub time_before: TextInput,
    /// The sample size as typed: `100,000`, `50k`, `2m`. Read on Enter.
    pub size: TextInput,
    /// Any number: the same seed draws the same rows, so 0 or 1 is a sample anyone
    /// can repeat.
    pub seed: TextInput,
    pub error: Option<String>,
    /// First source file shown in the numbered list under the Files row.
    pub file_offset: usize,
}

impl SampleForm {
    pub fn new(sample: &Sample, context: SampleContext, theme: &crate::config::Theme) -> Self {
        let input = || TextInput::new().with_theme(theme);
        let mut form = Self {
            inline: false,
            view: false,
            bytes_per_row: None,
            source_bytes_per_row: None,
            anyway: false,
            draft: sample.clone(),
            kind: RowsKind::All,
            field: SampleField::Rows,
            context,
            partition_column: 0,
            partition_values: input(),
            files: input(),
            range_from: input(),
            range_to: input(),
            time_column: 0,
            time_from: input(),
            time_before: input(),
            size: input(),
            seed: input(),
            error: None,
            file_offset: 0,
        };
        form.set_scope(&sample.scope);
        // Most seeds come from the clock, and a seed is a token, not text to
        // extend: typing one means a new one.
        form.seed.suggest(sample.seed.to_string());
        // A size is replaced more often than edited, as the seed is.
        form.size.suggest(crate::numfmt::group_chrome(sample.rows));
        form.sync_focus(true);
        form
    }

    /// Load a scope into the form's rows: its kind, and that kind's inputs.
    pub fn set_scope(&mut self, scope: &QualityScope) {
        self.kind = match scope {
            QualityScope::CurrentView => RowsKind::All,
            QualityScope::WholeSource => RowsKind::Source,
            QualityScope::FirstRows(end) => {
                self.range_from.set_value("1");
                self.range_to.set_value(end.to_string());
                RowsKind::Range
            }
            QualityScope::ViewRows { start, end } => {
                self.range_from.set_value(start.to_string());
                self.range_to.set_value(end.to_string());
                RowsKind::Range
            }
            QualityScope::SourceFiles(files) => {
                self.files.set_value(
                    files
                        .iter()
                        .map(usize::to_string)
                        .collect::<Vec<_>>()
                        .join(","),
                );
                RowsKind::Files
            }
            QualityScope::SourcePartition { column, value } => {
                if !self.context.partition_columns.contains(column) {
                    self.context.partition_columns.insert(0, column.clone());
                }
                self.partition_column = self
                    .context
                    .partition_columns
                    .iter()
                    .position(|name| name == column)
                    .unwrap_or(0);
                self.partition_values.set_value(value);
                RowsKind::Partitions
            }
            QualityScope::SourceTimeRange { column, start, end } => {
                if !self.context.time_columns.contains(column) {
                    self.context.time_columns.insert(0, column.clone());
                }
                self.time_column = self
                    .context
                    .time_columns
                    .iter()
                    .position(|name| name == column)
                    .unwrap_or(0);
                self.time_from.set_value(start);
                self.time_before.set_value(end);
                RowsKind::Time
            }
        };
    }

    /// The kinds of rows this table offers, in the order ←/→ steps them.
    pub fn kinds(&self) -> Vec<RowsKind> {
        let mut kinds = vec![RowsKind::All];
        if self.context.filtered {
            kinds.push(RowsKind::Source);
        }
        if !self.context.partition_columns.is_empty() {
            kinds.push(RowsKind::Partitions);
        }
        if self.context.files.len() > 1 {
            kinds.push(RowsKind::Files);
        }
        kinds.push(RowsKind::Range);
        if !self.context.time_columns.is_empty() {
            kinds.push(RowsKind::Time);
        }
        if !kinds.contains(&self.kind) {
            kinds.push(self.kind);
        }
        kinds
    }

    /// The rows on screen, top to bottom.
    pub fn fields(&self) -> Vec<SampleField> {
        // No sample has no rows to choose.
        if self.no_sample() {
            return vec![SampleField::Method];
        }
        let mut fields = vec![SampleField::Rows];
        fields.extend(match self.kind {
            RowsKind::All | RowsKind::Source => vec![],
            RowsKind::Partitions => {
                vec![SampleField::PartitionColumn, SampleField::PartitionValues]
            }
            RowsKind::Files => vec![SampleField::Files],
            RowsKind::Range => vec![SampleField::RangeFrom, SampleField::RangeTo],
            RowsKind::Time => vec![
                SampleField::TimeColumn,
                SampleField::TimeFrom,
                SampleField::TimeBefore,
            ],
        });
        fields.push(SampleField::Method);
        if matches!(self.draft.method, SampleMethod::PerPartition { .. }) {
            fields.push(SampleField::By);
        }
        if self.draft.method != SampleMethod::EveryRow {
            fields.push(SampleField::Size);
        }
        if matches!(
            self.draft.method,
            SampleMethod::Spread | SampleMethod::PerPartition { .. }
        ) {
            fields.push(SampleField::Seed);
        }
        fields
    }

    pub fn input(&self, field: SampleField) -> Option<&TextInput> {
        Some(match field {
            SampleField::PartitionValues => &self.partition_values,
            SampleField::Files => &self.files,
            SampleField::RangeFrom => &self.range_from,
            SampleField::RangeTo => &self.range_to,
            SampleField::TimeFrom => &self.time_from,
            SampleField::TimeBefore => &self.time_before,
            SampleField::Size => &self.size,
            SampleField::Seed => &self.seed,
            _ => return None,
        })
    }

    pub fn input_mut(&mut self, field: SampleField) -> Option<&mut TextInput> {
        Some(match field {
            SampleField::PartitionValues => &mut self.partition_values,
            SampleField::Files => &mut self.files,
            SampleField::RangeFrom => &mut self.range_from,
            SampleField::RangeTo => &mut self.range_to,
            SampleField::TimeFrom => &mut self.time_from,
            SampleField::TimeBefore => &mut self.time_before,
            SampleField::Size => &mut self.size,
            SampleField::Seed => &mut self.seed,
            _ => return None,
        })
    }

    /// Show the text cursor in the focused text row only, and only while the form
    /// has the cursor at all.
    pub fn sync_focus(&mut self, form_focused: bool) {
        let field = self.field;
        for candidate in [
            SampleField::PartitionValues,
            SampleField::Files,
            SampleField::RangeFrom,
            SampleField::RangeTo,
            SampleField::TimeFrom,
            SampleField::TimeBefore,
            SampleField::Size,
            SampleField::Seed,
        ] {
            if let Some(input) = self.input_mut(candidate) {
                input.set_focused(form_focused && candidate == field);
            }
        }
    }

    /// ←/→ on the focused row. Each ring steps both ways, so Left undoes Right.
    pub fn adjust(&mut self, forward: bool) {
        let step = |len: usize, at: usize| {
            if forward {
                (at + 1) % len
            } else {
                (at + len - 1) % len
            }
        };
        match self.field {
            SampleField::Rows => {
                let kinds = self.kinds();
                let at = kinds
                    .iter()
                    .position(|kind| *kind == self.kind)
                    .unwrap_or(0);
                self.kind = kinds[step(kinds.len(), at)];
                // A range starts as the whole table, so its rows say what they cover.
                if self.kind == RowsKind::Range && self.range_to.value().is_empty() {
                    self.range_from.suggest("1");
                    if let Some(rows) = self.context.view_rows {
                        self.range_to.suggest(rows.to_string());
                    }
                }
            }
            SampleField::PartitionColumn if !self.context.partition_columns.is_empty() => {
                self.partition_column =
                    step(self.context.partition_columns.len(), self.partition_column);
            }
            SampleField::TimeColumn if !self.context.time_columns.is_empty() => {
                self.time_column = step(self.context.time_columns.len(), self.time_column);
            }
            SampleField::Method => {
                let by = match &self.draft.method {
                    SampleMethod::PerPartition { column } => Some(column.clone()),
                    _ => None,
                }
                .or_else(|| self.context.value_columns.first().cloned());
                let mut methods = vec![SampleMethod::Spread];
                if let Some(column) = by {
                    methods.push(SampleMethod::PerPartition { column });
                }
                methods.push(SampleMethod::FirstRows);
                methods.push(SampleMethod::EveryRow);
                let at = methods
                    .iter()
                    .position(|method| {
                        std::mem::discriminant(method) == std::mem::discriminant(&self.draft.method)
                    })
                    .unwrap_or(0);
                self.draft.method = methods[step(methods.len(), at)].clone();
                // Rows per value, not in all: a size meant for the whole table would
                // keep nearly every row of every value.
                if matches!(self.draft.method, SampleMethod::PerPartition { .. })
                    && self.typed_size().unwrap_or(self.draft.rows) > PER_VALUE_ROWS
                {
                    self.draft.rows = PER_VALUE_ROWS;
                    self.size
                        .suggest(crate::numfmt::group_chrome(PER_VALUE_ROWS));
                }
            }
            SampleField::By => {
                let columns = &self.context.value_columns;
                if let SampleMethod::PerPartition { column } = &self.draft.method
                    && !columns.is_empty()
                {
                    let at = columns.iter().position(|name| name == column).unwrap_or(0);
                    self.draft.method = SampleMethod::PerPartition {
                        column: columns[step(columns.len(), at)].clone(),
                    };
                }
            }
            _ => {}
        }
    }

    /// The choice a ←/→ row shows, in words.
    pub fn choice(&self, field: SampleField) -> String {
        match field {
            SampleField::Rows => match self.kind {
                RowsKind::All => {
                    let count = self
                        .context
                        .view_rows
                        .map(|rows| format!(" ({})", crate::home::discover::format_rows(rows)))
                        .unwrap_or_default();
                    if self.context.filtered {
                        format!("All rows as filtered{count}")
                    } else {
                        format!("All rows{count}")
                    }
                }
                RowsKind::Source => "The source, unfiltered".to_string(),
                RowsKind::Partitions => "Partitions".to_string(),
                RowsKind::Files => "Files".to_string(),
                RowsKind::Range => "Row range".to_string(),
                RowsKind::Time => "Time range".to_string(),
            },
            SampleField::PartitionColumn => self
                .context
                .partition_columns
                .get(self.partition_column)
                .cloned()
                .unwrap_or_default(),
            SampleField::TimeColumn => self
                .context
                .time_columns
                .get(self.time_column)
                .cloned()
                .unwrap_or_default(),
            SampleField::Method => match &self.draft.method {
                SampleMethod::EveryRow if self.view => "No sample".to_string(),
                method => method.name().to_string(),
            },
            SampleField::By => match &self.draft.method {
                SampleMethod::PerPartition { column } => column.clone(),
                _ => String::new(),
            },
            SampleField::Seed => self.draft.seed.to_string(),
            _ => String::new(),
        }
    }

    /// The values of the chosen partition column that the source's files hold, from
    /// their `column=value` path segments, in order.
    pub fn partition_values_known(&self) -> Vec<String> {
        let Some(column) = self.context.partition_columns.get(self.partition_column) else {
            return Vec::new();
        };
        if let Some((_, values)) = self
            .context
            .partition_values
            .iter()
            .find(|(name, _)| name == column)
        {
            return values.clone();
        }
        let prefix = format!("{column}=");
        let mut values: Vec<String> = Vec::new();
        for file in &self.context.files {
            for segment in file.split(['/', '\\']) {
                if let Some(value) = segment.strip_prefix(&prefix)
                    && !values.iter().any(|known| known == value)
                {
                    values.push(value.to_string());
                }
            }
        }
        values
    }

    /// Refuse partition values the source does not hold, when it is known what it
    /// holds: a typo would otherwise match no rows and sample nothing, silently. A
    /// range's ends need only be the same kind of value (2015..2030 is fine for
    /// years), since a range is compared, not matched.
    fn check_partition_values(&self, column: &str, values: &str) -> Result<(), String> {
        let known = self.partition_values_known();
        if known.is_empty() {
            return Ok(());
        }
        let holds = match known.as_slice() {
            [only] => only.clone(),
            [first, .., last] => format!("{first} {} {last}", crate::glyphs::get().ellipsis),
            [] => String::new(),
        };
        if let Some((start, end)) = values.split_once("..") {
            let numeric = known.iter().all(|value| value.parse::<f64>().is_ok());
            let dated = known
                .iter()
                .all(|value| chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d").is_ok());
            let fits = |end: &str| {
                let end = end.trim();
                !end.is_empty()
                    && (!numeric || end.parse::<f64>().is_ok())
                    && (!dated || chrono::NaiveDate::parse_from_str(end, "%Y-%m-%d").is_ok())
            };
            if !fits(start) || !fits(end) {
                return Err(format!(
                    "A {column} range runs between two of its values, like {}..{}; it holds {holds}",
                    known[0],
                    known[known.len() - 1]
                ));
            }
            return Ok(());
        }
        for value in values.split(',').map(str::trim).filter(|v| !v.is_empty()) {
            if value != "∅" && !known.iter().any(|known| known == value) {
                return Err(format!(
                    "{column} has no partition {value:?}; it holds {holds}"
                ));
            }
        }
        Ok(())
    }

    /// The form takes the view's sample away: No sample is chosen.
    pub fn no_sample(&self) -> bool {
        self.view && self.draft.method == SampleMethod::EveryRow
    }

    /// Rows the sample will hold, when that can be told before it is drawn: the
    /// size, or the rows there are when fewer. Not for an equal-per-value sample,
    /// whose size is per value.
    pub fn rows_expected(&self) -> Option<usize> {
        let asked = match self.draft.method {
            SampleMethod::Spread | SampleMethod::FirstRows => self.typed_size()?,
            SampleMethod::EveryRow => self.context.view_rows?,
            SampleMethod::PerPartition { .. } => return None,
        };
        let there = match self.kind {
            RowsKind::All => self.context.view_rows,
            _ => None,
        };
        Some(there.map_or(asked, |rows| asked.min(rows)))
    }

    /// The bytes the sample will take, from the bytes per row of what it reads.
    pub fn estimate(&self) -> Option<u64> {
        let rows = self.rows_expected()? as u64;
        let per_row = match self.kind {
            RowsKind::All | RowsKind::Range => self.bytes_per_row?,
            _ => self.source_bytes_per_row?,
        };
        Some(rows.saturating_mul(per_row as u64))
    }

    /// An edit: what was said about the form as it stood no longer stands.
    pub fn edited(&mut self) {
        self.error = None;
        self.anyway = false;
    }

    /// The sample size typed, when it reads as one.
    fn typed_size(&self) -> Option<usize> {
        crate::analysis::sampling::parse_size(self.size.value()).ok()
    }

    /// The file numbers typed so far, for marking the list under the Files row.
    pub fn files_chosen(&self) -> Vec<usize> {
        self.files
            .value()
            .split(',')
            .filter_map(|part| part.trim().parse().ok())
            .collect()
    }

    /// The draft with the rows the form describes, or why it cannot say which.
    pub fn finish(&self) -> Result<Sample, String> {
        let scope = match self.kind {
            RowsKind::All => QualityScope::CurrentView,
            RowsKind::Source => QualityScope::WholeSource,
            RowsKind::Range => {
                let number = |input: &TextInput| {
                    input
                        .value()
                        .replace([',', '_', ' '], "")
                        .parse::<usize>()
                        .ok()
                };
                match (number(&self.range_from), number(&self.range_to)) {
                    (Some(from), Some(to)) if from >= 1 && to >= from => {
                        QualityScope::parse_command(&format!("rows {from}..{to}"))
                            .map_err(|e| e.to_string())?
                    }
                    _ => {
                        return Err(
                            "From row and To row are row numbers, From at least 1 and not past To"
                                .to_string(),
                        );
                    }
                }
            }
            RowsKind::Files => {
                let chosen = self.files_chosen();
                let count = self.context.files.len();
                if chosen.is_empty() {
                    return Err("Type the numbers of the files to read, like 1,3".to_string());
                }
                if let Some(file) = chosen.iter().find(|file| **file == 0 || **file > count) {
                    return Err(format!(
                        "There is no file {file}; the files are 1 to {count}"
                    ));
                }
                QualityScope::parse_command(&format!(
                    "files {}",
                    chosen
                        .iter()
                        .map(usize::to_string)
                        .collect::<Vec<_>>()
                        .join(",")
                ))
                .map_err(|e| e.to_string())?
            }
            RowsKind::Partitions => {
                let column = self.choice(SampleField::PartitionColumn);
                let values = self.partition_values.value().trim();
                if values.is_empty() {
                    return Err(format!("Type the {column} values to read"));
                }
                self.check_partition_values(&column, values)?;
                QualityScope::parse_command(&format!("partition {column}={values}"))
                    .map_err(|e| e.to_string())?
            }
            RowsKind::Time => {
                let column = self.choice(SampleField::TimeColumn);
                let (from, before) = (
                    self.time_from.value().trim(),
                    self.time_before.value().trim(),
                );
                QualityScope::parse_command(&format!("time {column}={from}..{before}")).map_err(
                    |_| {
                        "From and Before are dates (2024-01-31) or timestamps, From first"
                            .to_string()
                    },
                )?
            }
        };
        let mut sample = Sample {
            scope,
            ..self.draft.clone()
        };
        if self.fields().contains(&SampleField::Size) {
            sample.rows = crate::analysis::sampling::parse_size(self.size.value())
                .map_err(|e| e.to_string())?
                .min(u32::MAX as usize);
        }
        if self.fields().contains(&SampleField::Seed) {
            sample.seed = self
                .seed
                .value()
                .replace([',', '_', ' '], "")
                .parse()
                .map_err(|_| "Random seed is a whole number, like 0 or 42".to_string())?;
        }
        Ok(sample)
    }
}

/// Rows per value an equal-per-value sample starts at.
const PER_VALUE_ROWS: usize = 1_000;

/// A seed from the clock: another sample each time it is asked for. Six digits are
/// plenty to tell samples apart and short enough to read back or type into a note.
pub fn new_seed() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
        % 1_000_000
}

impl crate::form::Form for SampleForm {
    type Field = SampleField;

    fn fields(&self) -> Vec<(SampleField, crate::form::FieldKind)> {
        use crate::form::FieldKind;
        SampleForm::fields(self)
            .into_iter()
            .map(|field| {
                let kind = if field.is_text() {
                    FieldKind::Text
                } else {
                    FieldKind::Choice
                };
                (field, kind)
            })
            .collect()
    }

    fn focused(&self) -> SampleField {
        self.field
    }

    fn set_focused(&mut self, field: SampleField) {
        self.field = field;
        // Arriving on the seed selects it, so typing 1 makes it 1 rather than
        // appending to the number already there.
        if field == SampleField::Seed {
            self.seed.select_all();
        }
        if field == SampleField::Size {
            self.size.select_all();
        }
        self.sync_focus(true);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn form() -> SampleForm {
        SampleForm::new(
            &Sample::default(),
            SampleContext {
                view_rows: Some(36_800_000),
                filtered: false,
                files: (2016..2019)
                    .map(|year| format!("/data/weather/year={year}/0.parquet"))
                    .collect(),
                partition_columns: vec!["year".to_string()],
                partition_values: Vec::new(),
                time_columns: vec!["date".to_string()],
                value_columns: vec!["station".to_string()],
            },
            &crate::config::Theme::from_config(&crate::config::ThemeConfig::default()).unwrap(),
        )
    }

    /// The first row says what it will read in words, and each kind brings only its
    /// own inputs.
    #[test]
    fn each_kind_of_rows_shows_only_its_inputs() {
        let mut form = form();
        assert_eq!(form.choice(SampleField::Rows), "All rows (36.8M)");
        assert_eq!(form.fields()[1], SampleField::Method);
        form.adjust(true);
        assert_eq!(form.kind, RowsKind::Partitions);
        assert_eq!(
            form.fields()[1..3],
            [SampleField::PartitionColumn, SampleField::PartitionValues]
        );
        assert_eq!(form.partition_values_known(), vec!["2016", "2017", "2018"]);
        form.adjust(true);
        assert_eq!(form.kind, RowsKind::Files);
        form.adjust(true);
        assert_eq!(form.kind, RowsKind::Range);
        assert_eq!(
            (form.range_from.value(), form.range_to.value()),
            ("1", "36800000"),
            "a range starts as the whole table"
        );
        form.adjust(false);
        assert_eq!(form.kind, RowsKind::Files, "Left undoes Right");
        assert!(
            !form.kinds().contains(&RowsKind::Source),
            "unfiltered is the same as all"
        );
    }

    #[test]
    fn method_steps_both_ways_and_rows_appear_with_it() {
        let mut form = form();
        form.field = SampleField::Method;
        form.adjust(true);
        assert_eq!(
            form.draft.method,
            SampleMethod::PerPartition {
                column: "station".to_string()
            }
        );
        assert_eq!(form.size.value(), "1,000");
        assert!(form.fields().contains(&SampleField::By));
        form.adjust(true);
        assert_eq!(form.draft.method, SampleMethod::FirstRows);
        assert!(
            !form.fields().contains(&SampleField::Seed),
            "the head has no seed"
        );
        form.adjust(true);
        assert_eq!(form.draft.method, SampleMethod::EveryRow);
        assert!(!form.fields().contains(&SampleField::Size));
        form.adjust(false);
        assert_eq!(form.draft.method, SampleMethod::FirstRows);
    }

    /// The Files row is typed into, so its PgUp/PgDn come back as its own keys:
    /// the handler pages the numbered file list on `Text(Files)`, never `Other`.
    #[test]
    fn the_files_row_keeps_its_paging_keys() {
        use crate::form::{Form, FormKey};
        use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
        let mut form = form();
        while form.kind != RowsKind::Files {
            form.adjust(true);
        }
        assert!(form.focus(SampleField::Files));
        for code in [KeyCode::PageDown, KeyCode::PageUp] {
            assert_eq!(
                crate::form::key(&mut form, &KeyEvent::new(code, KeyModifiers::NONE)),
                FormKey::Text(SampleField::Files)
            );
        }
    }

    /// The size is typed, in shorthand or in full, and read on Enter; a size it
    /// cannot read says why and is not applied.
    #[test]
    fn the_size_is_typed() {
        let mut form = form();
        assert_eq!(form.size.value(), "100,000");
        while form.field != SampleField::Size {
            crate::form::Form::focus_next(&mut form);
        }
        let key = |c| {
            crossterm::event::KeyEvent::new(
                crossterm::event::KeyCode::Char(c),
                crossterm::event::KeyModifiers::NONE,
            )
        };
        for c in "250k".chars() {
            form.size.handle_key(&key(c), None);
        }
        assert_eq!(form.size.value(), "250k", "typing replaces the size");
        assert_eq!(form.finish().unwrap().rows, 250_000);
        form.size.set_value("2M");
        assert_eq!(form.finish().unwrap().rows, 2_000_000);
        form.size.set_value("lots");
        assert!(form.finish().unwrap_err().contains("50k"));
        form.size.set_value("0");
        assert!(form.finish().is_err());
        // Every row has no size to read.
        form.field = SampleField::Method;
        while form.draft.method != SampleMethod::EveryRow {
            form.adjust(true);
        }
        assert!(form.finish().is_ok());
    }

    /// Any number is a seed, typed over the one there; the same seed is the same
    /// sample, so 0 is one anyone can repeat.
    #[test]
    fn the_seed_is_typed() {
        let mut form = form();
        while form.field != SampleField::Seed {
            crate::form::Form::focus_next(&mut form);
        }
        assert!(form.field.is_text());
        let zero = crossterm::event::KeyEvent::new(
            crossterm::event::KeyCode::Char('0'),
            crossterm::event::KeyModifiers::NONE,
        );
        form.seed.handle_key(&zero, None);
        assert_eq!(form.finish().unwrap().seed, 0);
        form.seed.set_value("seven");
        assert!(form.finish().unwrap_err().contains("whole number"));
    }

    #[test]
    fn the_rows_described_become_a_scope() {
        let mut form = form();
        form.set_scope(&QualityScope::parse_command("partition year=2016..2017").unwrap());
        assert_eq!(form.kind, RowsKind::Partitions);
        assert_eq!(
            form.finish().unwrap().scope,
            QualityScope::SourcePartition {
                column: "year".to_string(),
                value: "2016..2017".to_string()
            }
        );
        form.kind = RowsKind::Range;
        form.range_from.set_value("2");
        form.range_to.set_value("5,000");
        assert_eq!(
            form.finish().unwrap().scope,
            QualityScope::ViewRows {
                start: 2,
                end: 5_000
            }
        );
        form.kind = RowsKind::Files;
        form.files.set_value("1, 4");
        assert!(form.finish().unwrap_err().contains("no file 4"));
    }

    /// A value the source does not hold is a typo, not an empty sample.
    #[test]
    fn partition_values_the_source_does_not_hold_are_refused() {
        let mut form = form();
        form.kind = RowsKind::Partitions;
        for garbage in ["asdf", "2016,20x7", "a..b", "2016.."] {
            form.partition_values.set_value(garbage);
            let error = form.finish().unwrap_err();
            assert!(error.contains("2016"), "{garbage}: {error}");
        }
        for fine in ["2017", "2016,2018", "2015..2030"] {
            form.partition_values.set_value(fine);
            assert!(form.finish().is_ok(), "{fine}");
        }
    }
}

//! The Sample form: the one place an analysis's rows are chosen, for every tool.
//! It edits a copy of the shared [`Sample`]; Enter applies it, Esc discards it.

use crate::data_quality::QualityScope;
use crate::sampling::{SAMPLE_SIZES, Sample, SampleMethod};
use crate::widgets::text_input::TextInput;

/// A row of the form. `By` is there only for a per-partition sample.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SampleField {
    Scope,
    Method,
    By,
    Rows,
    Seed,
}

impl SampleField {
    pub fn label(self) -> &'static str {
        match self {
            Self::Scope => "Rows from:",
            Self::Method => "Method:",
            Self::By => "Per:",
            Self::Rows => "Rows:",
            Self::Seed => "Seed:",
        }
    }
}

pub struct SampleForm {
    pub draft: Sample,
    pub field: SampleField,
    pub scope_input: TextInput,
    pub error: Option<String>,
    /// First source file shown in the numbered inventory under the scope row.
    pub file_offset: usize,
    /// Columns a per-partition sample can split by: partition columns first.
    pub columns: Vec<String>,
}

impl SampleForm {
    pub fn new(sample: &Sample, columns: Vec<String>, theme: &crate::config::Theme) -> Self {
        let mut scope_input = TextInput::new().with_theme(theme);
        scope_input.set_value(sample.scope.command());
        scope_input.set_focused(true);
        Self {
            draft: sample.clone(),
            field: SampleField::Scope,
            scope_input,
            error: None,
            file_offset: 0,
            columns,
        }
    }

    /// The rows on screen, top to bottom.
    pub fn fields(&self) -> Vec<SampleField> {
        let mut fields = vec![SampleField::Scope, SampleField::Method];
        if matches!(self.draft.method, SampleMethod::PerPartition { .. }) {
            fields.push(SampleField::By);
        }
        if self.draft.method != SampleMethod::EveryRow {
            fields.push(SampleField::Rows);
        }
        if matches!(
            self.draft.method,
            SampleMethod::Spread | SampleMethod::PerPartition { .. }
        ) {
            fields.push(SampleField::Seed);
        }
        fields
    }

    pub fn move_field(&mut self, forward: bool) {
        let fields = self.fields();
        let at = fields
            .iter()
            .position(|field| *field == self.field)
            .unwrap_or(0);
        let next = if forward {
            (at + 1).min(fields.len() - 1)
        } else {
            at.saturating_sub(1)
        };
        self.field = fields[next];
        self.scope_input
            .set_focused(self.field == SampleField::Scope);
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
            SampleField::Scope => {}
            SampleField::Method => {
                let by = match &self.draft.method {
                    SampleMethod::PerPartition { column } => Some(column.clone()),
                    _ => None,
                }
                .or_else(|| self.columns.first().cloned());
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
                    && self.draft.rows > PER_VALUE_ROWS
                {
                    self.draft.rows = PER_VALUE_ROWS;
                }
            }
            SampleField::By => {
                if let SampleMethod::PerPartition { column } = &self.draft.method
                    && !self.columns.is_empty()
                {
                    let at = self
                        .columns
                        .iter()
                        .position(|name| name == column)
                        .unwrap_or(0);
                    self.draft.method = SampleMethod::PerPartition {
                        column: self.columns[step(self.columns.len(), at)].clone(),
                    };
                }
            }
            SampleField::Rows => {
                // A configured size that is none of these stays in the ring.
                let mut sizes = SAMPLE_SIZES.to_vec();
                if !sizes.contains(&self.draft.rows) {
                    sizes.push(self.draft.rows);
                    sizes.sort_unstable();
                }
                let at = sizes
                    .iter()
                    .position(|rows| *rows == self.draft.rows)
                    .unwrap_or(0);
                self.draft.rows = sizes[step(sizes.len(), at)];
            }
            SampleField::Seed => self.draft.seed = new_seed(),
        }
    }

    /// The draft with its typed scope, or why the scope does not parse.
    pub fn finish(&self) -> Result<Sample, String> {
        let scope =
            QualityScope::parse_command(self.scope_input.value()).map_err(|e| e.to_string())?;
        Ok(Sample {
            scope,
            ..self.draft.clone()
        })
    }
}

/// Rows per value a per-partition sample starts at.
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

#[cfg(test)]
mod tests {
    use super::*;

    fn form() -> SampleForm {
        SampleForm::new(
            &Sample::default(),
            vec!["year".to_string(), "ticker".to_string()],
            &crate::config::Theme::from_config(&crate::config::ThemeConfig::default()).unwrap(),
        )
    }

    #[test]
    fn method_steps_both_ways_and_rows_appear_with_it() {
        let mut form = form();
        form.field = SampleField::Method;
        form.adjust(true);
        assert_eq!(
            form.draft.method,
            SampleMethod::PerPartition {
                column: "year".to_string()
            }
        );
        assert!(form.fields().contains(&SampleField::By));
        form.adjust(true);
        assert_eq!(form.draft.method, SampleMethod::FirstRows);
        assert!(
            !form.fields().contains(&SampleField::Seed),
            "the head has no seed"
        );
        form.adjust(true);
        assert_eq!(form.draft.method, SampleMethod::EveryRow);
        assert!(!form.fields().contains(&SampleField::Rows));
        form.adjust(false);
        assert_eq!(form.draft.method, SampleMethod::FirstRows);
    }

    #[test]
    fn a_typed_scope_is_parsed_on_finish() {
        let mut form = form();
        form.scope_input.set_value("partition year=2020..2022");
        assert_eq!(
            form.finish().unwrap().scope,
            QualityScope::SourcePartition {
                column: "year".to_string(),
                value: "2020..2022".to_string()
            }
        );
        form.scope_input.set_value("nonsense");
        assert!(form.finish().is_err());
    }
}

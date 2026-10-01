//! The form Data Quality Setup opens on one column to declare what it must hold:
//! part of the key, required, read as a number, an allowed set, a range. It stages
//! into Setup's draft, which only Run reads with.

use crate::data_quality::TimeInterpretation;
use crate::quality_intent::{
    ColumnIntent, DeclaredIntent, NumberReading, ValueKind, allows_set, check_intent,
    parse_allowed, reads_as_number,
};
use crate::widgets::text_input::TextInput;
use polars::prelude::DataType;

/// The form's rows, top to bottom. Which are shown depends on the column's type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntentField {
    Key,
    Required,
    ReadAs,
    Allowed,
    Minimum,
    Maximum,
}

impl IntentField {
    pub fn label(self) -> &'static str {
        match self {
            Self::Key => "Key:",
            Self::Required => "Required:",
            Self::ReadAs => "Read as:",
            Self::Allowed => "Allowed:",
            Self::Minimum => "Minimum:",
            Self::Maximum => "Maximum:",
        }
    }

    pub fn is_text(self) -> bool {
        matches!(self, Self::Allowed | Self::Minimum | Self::Maximum)
    }
}

/// One column's declaration, being edited.
pub struct IntentForm {
    pub column: String,
    pub dtype: DataType,
    /// How Text as time reads the column, when it does: the reading here, set there.
    pub time: Option<TimeInterpretation>,
    pub field: IntentField,
    pub in_key: bool,
    pub required: bool,
    pub number: Option<NumberReading>,
    pub allowed: TextInput,
    pub min: TextInput,
    pub max: TextInput,
    /// Why Enter did not apply, until the next edit.
    pub error: Option<String>,
}

impl IntentForm {
    pub fn new(
        column: &str,
        dtype: DataType,
        time: Option<TimeInterpretation>,
        declared: &DeclaredIntent,
        theme: &crate::config::Theme,
    ) -> Self {
        let input = || TextInput::new().with_theme(theme);
        let intent = declared
            .column(column)
            .cloned()
            .unwrap_or_else(|| ColumnIntent::new(column));
        let mut form = Self {
            column: column.to_string(),
            dtype,
            time,
            field: IntentField::Key,
            in_key: declared.key.iter().any(|name| name == column),
            required: intent.required,
            number: intent.number,
            allowed: input(),
            min: input(),
            max: input(),
            error: None,
        };
        form.allowed.set_value(intent.allowed.join(", "));
        form.min.set_value(intent.min.unwrap_or_default());
        form.max.set_value(intent.max.unwrap_or_default());
        form.sync_focus();
        form
    }

    /// What the column's values are, read as the form says now.
    pub fn value_kind(&self) -> ValueKind {
        ValueKind::of(&self.dtype, self.number, self.time.as_ref())
    }

    /// The rows this column takes: a set where values are codes, a reading where it
    /// is text, a range where values have an order.
    pub fn fields(&self) -> Vec<IntentField> {
        let mut fields = vec![IntentField::Key, IntentField::Required];
        if reads_as_number(&self.dtype) {
            fields.push(IntentField::ReadAs);
        }
        if allows_set(&self.dtype) {
            fields.push(IntentField::Allowed);
        }
        if self.value_kind().ranges() {
            fields.extend([IntentField::Minimum, IntentField::Maximum]);
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
        self.sync_focus();
    }

    /// Space, or ←→, on a row that is a choice: the key and required boxes, and the
    /// reading, which a Text as time format holds when there is one.
    pub fn adjust(&mut self, forward: bool) {
        match self.field {
            IntentField::Key => self.in_key = !self.in_key,
            IntentField::Required => self.required = !self.required,
            IntentField::ReadAs if self.time.is_none() => {
                let choices = [
                    None,
                    Some(NumberReading::Whole),
                    Some(NumberReading::Decimal),
                ];
                let at = choices
                    .iter()
                    .position(|choice| *choice == self.number)
                    .unwrap_or(0);
                let next = if forward {
                    (at + 1) % choices.len()
                } else {
                    (at + choices.len() - 1) % choices.len()
                };
                self.number = choices[next];
            }
            _ => {}
        }
        self.error = None;
    }

    /// The reading as the row echoes it.
    pub fn reading_label(&self) -> String {
        match (&self.time, self.number) {
            (Some(time), _) => format!("{} (Text as time)", time.label()),
            (None, Some(number)) => number.label().to_string(),
            (None, None) => "as stored".to_string(),
        }
    }

    pub fn input(&self, field: IntentField) -> Option<&TextInput> {
        match field {
            IntentField::Allowed => Some(&self.allowed),
            IntentField::Minimum => Some(&self.min),
            IntentField::Maximum => Some(&self.max),
            _ => None,
        }
    }

    pub fn input_mut(&mut self) -> Option<&mut TextInput> {
        match self.field {
            IntentField::Allowed => Some(&mut self.allowed),
            IntentField::Minimum => Some(&mut self.min),
            IntentField::Maximum => Some(&mut self.max),
            _ => None,
        }
    }

    /// The text field under the cursor shows its cursor; the others do not.
    pub fn sync_focus(&mut self) {
        let field = self.field;
        self.allowed.set_focused(field == IntentField::Allowed);
        self.min.set_focused(field == IntentField::Minimum);
        self.max.set_focused(field == IntentField::Maximum);
    }

    /// Whether the cursor is in a text field, so typed keys are text.
    pub fn typing(&self) -> bool {
        self.field.is_text()
    }

    /// The declaration as the form stands, and whether the column is in the key; or
    /// why it cannot be measured. Rows the type does not take are left out.
    pub fn finish(&self) -> Result<(bool, ColumnIntent), String> {
        let fields = self.fields();
        let text = |field: IntentField, input: &TextInput| {
            let value = input.value().trim();
            (fields.contains(&field) && !value.is_empty()).then(|| value.to_string())
        };
        let allowed = if fields.contains(&IntentField::Allowed) {
            parse_allowed(&self.dtype, self.allowed.value())?
        } else {
            Vec::new()
        };
        let intent = ColumnIntent {
            column: self.column.clone(),
            required: self.required,
            allowed,
            min: text(IntentField::Minimum, &self.min),
            max: text(IntentField::Maximum, &self.max),
            number: self.number.filter(|_| self.time.is_none()),
        };
        check_intent(&intent, &self.dtype, self.time.as_ref())?;
        Ok((self.in_key, intent))
    }

    /// Put the declaration into `declared`, or say why not.
    pub fn apply(&self, declared: &mut DeclaredIntent) -> Result<(), String> {
        let (in_key, intent) = self.finish()?;
        declared.set_key(&self.column, in_key);
        declared.set(intent);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn theme() -> crate::config::Theme {
        crate::config::Theme::from_config(&crate::config::AppConfig::default().theme).unwrap()
    }

    /// The rows follow the type: text takes a reading and a set, and a range once it
    /// reads as a number; a float takes a range and no set.
    #[test]
    fn rows_follow_the_columns_type() {
        let declared = DeclaredIntent::default();
        let mut text = IntentForm::new("code", DataType::String, None, &declared, &theme());
        assert_eq!(
            text.fields(),
            vec![
                IntentField::Key,
                IntentField::Required,
                IntentField::ReadAs,
                IntentField::Allowed
            ]
        );
        text.field = IntentField::ReadAs;
        text.adjust(true);
        assert_eq!(text.number, Some(NumberReading::Whole));
        assert!(text.fields().contains(&IntentField::Minimum));
        let float = IntentForm::new("amount", DataType::Float64, None, &declared, &theme());
        assert_eq!(
            float.fields(),
            vec![
                IntentField::Key,
                IntentField::Required,
                IntentField::Minimum,
                IntentField::Maximum
            ]
        );
    }

    /// Apply stages the declaration; a bound the type cannot read is refused with
    /// the reason, and nothing changes.
    #[test]
    fn apply_stages_or_says_why_not() {
        let mut declared = DeclaredIntent::default();
        let mut form = IntentForm::new("amount", DataType::Float64, None, &declared, &theme());
        form.adjust(true);
        form.field = IntentField::Minimum;
        form.min.set_value("ten");
        assert!(form.apply(&mut declared).is_err());
        assert!(declared.is_empty());
        form.min.set_value("10");
        form.apply(&mut declared).unwrap();
        assert_eq!(declared.key, vec!["amount"]);
        assert_eq!(declared.columns[0].min.as_deref(), Some("10"));
        // Reopened, the form shows what was staged.
        let again = IntentForm::new("amount", DataType::Float64, None, &declared, &theme());
        assert!(again.in_key);
        assert_eq!(again.min.value(), "10");
    }
}

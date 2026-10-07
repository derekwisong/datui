//! A column's type changed in the table, and a datetime made from columns: the same
//! names, formats and rules a delimited spec's `[columns]` takes ([`crate::column_types`]),
//! as a view step before the filters.
//!
//! [`RetypeModal`] picks the type, then, for a date, time or datetime, a format that
//! reads the column's first values, or one typed. [`CombineModal`] is the spec's
//! `{ from = [...], as = "datetime" }`.

use crate::column_types::{ColumnType, DerivedKind, TYPE_NAMES, dtype_label};
use crate::widgets::ui::PickerState;
use polars::prelude::DataType;

/// The first choice of the type list: the column as the read gave it.
pub const AS_READ: &str = "as read";

/// Where the type picker is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Stage {
    /// Which type.
    Type,
    /// Which format for `ty`, a temporal type: `formats[i]` for line `i`, `None` for
    /// the line that infers it from the values.
    Format {
        ty: DataType,
        formats: Vec<Option<String>>,
    },
}

/// The type picker over the table.
#[derive(Debug, Clone)]
pub struct RetypeModal {
    pub column: String,
    /// The column's type as read, before the view's.
    pub as_read: DataType,
    /// Values of the column on screen, the first that are not blank: what the formats
    /// are judged by and previewed on.
    pub examples: Vec<String>,
    pub stage: Stage,
    pub picker: PickerState,
}

/// What Enter on the picker chose.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Chosen {
    /// The column as read: the view's type taken away.
    AsRead,
    Type(ColumnType),
    /// A temporal type: its format is next.
    Format,
    Nothing,
}

impl RetypeModal {
    /// The picker for `column`, now `current` (the view's type, or none), read as
    /// `as_read`, with `examples` from the rows on screen.
    pub fn new(
        column: String,
        as_read: DataType,
        current: Option<&ColumnType>,
        examples: Vec<String>,
    ) -> Self {
        let mut items = vec![AS_READ.to_string()];
        items.extend(TYPE_NAMES.iter().map(|n| n.to_string()));
        let mut picker = PickerState::new(items);
        let at = current
            .and_then(|ty| TYPE_NAMES.iter().position(|n| *n == ty.name()))
            .map_or(0, |i| i + 1);
        picker.select_original(at);
        Self {
            column,
            as_read,
            examples,
            stage: Stage::Type,
            picker,
        }
    }

    /// Enter: the type chosen, or the format.
    pub fn choose(&mut self) -> Chosen {
        match &self.stage {
            Stage::Type => {
                let Some(i) = self.picker.selected_original() else {
                    return Chosen::Nothing;
                };
                if i == 0 {
                    return Chosen::AsRead;
                }
                let Ok(ty) = ColumnType::named(TYPE_NAMES[i - 1], None) else {
                    return Chosen::Nothing;
                };
                if ty.is_temporal() {
                    self.offer_formats(ty.dtype);
                    return Chosen::Format;
                }
                Chosen::Type(ty)
            }
            Stage::Format { ty, formats } => {
                let typed = self.picker.filter.trim();
                let format = match self.picker.selected_original() {
                    // What was typed narrowed to nothing: it is the format.
                    None if typed.contains('%') => Some(typed.to_string()),
                    None => return Chosen::Nothing,
                    Some(i) => formats[i].clone(),
                };
                Chosen::Type(ColumnType {
                    dtype: ty.clone(),
                    format,
                })
            }
        }
    }

    /// Esc: back from the formats to the types. `false` when there is nowhere back to.
    pub fn back(&mut self) -> bool {
        if matches!(self.stage, Stage::Type) {
            return false;
        }
        *self = Self::new(
            std::mem::take(&mut self.column),
            self.as_read.clone(),
            None,
            std::mem::take(&mut self.examples),
        );
        true
    }

    /// The formats of `dtype` that read the first example, each with what it makes of
    /// it, after a line that infers the format; every format when none reads it.
    fn offer_formats(&mut self, dtype: DataType) {
        let first = self.examples.first().cloned().unwrap_or_default();
        let mut fitting = crate::column_types::formats_reading(&dtype, &first);
        if fitting.is_empty() {
            fitting = match dtype {
                DataType::Date => crate::column_types::DATE_FORMATS.to_vec(),
                DataType::Time => crate::column_types::TIME_FORMATS.to_vec(),
                _ => crate::column_types::DATETIME_FORMATS.to_vec(),
            };
        }
        let arrow = crate::glyphs::get().arrow_right;
        let shown = |ty: &ColumnType| match crate::column_types::preview(ty, &first) {
            Some(read) if !first.is_empty() => format!("  {first} {arrow} {read}"),
            _ => String::new(),
        };
        let infer = ColumnType {
            dtype: dtype.clone(),
            format: None,
        };
        let mut items = vec![format!("infer from the values{}", shown(&infer))];
        let mut formats = vec![None];
        for format in fitting {
            let ty = ColumnType {
                dtype: dtype.clone(),
                format: Some(format.to_string()),
            };
            items.push(format!("{format}{}", shown(&ty)));
            formats.push(Some(format.to_string()));
        }
        self.picker = PickerState::new(items);
        // The first that reads, ahead of inferring.
        self.picker
            .select_original(if formats.len() > 1 { 1 } else { 0 });
        self.stage = Stage::Format { ty: dtype, formats };
    }

    /// The format typed, and what it makes of the first example, while it is one.
    pub fn typed_format(&self) -> Option<(String, Option<String>)> {
        let Stage::Format { ty, .. } = &self.stage else {
            return None;
        };
        let typed = self.picker.filter.trim();
        if !typed.contains('%') {
            return None;
        }
        let first = self.examples.first()?;
        let read = crate::column_types::preview(
            &ColumnType {
                dtype: ty.clone(),
                format: Some(typed.to_string()),
            },
            first,
        );
        Some((typed.to_string(), read))
    }

    pub fn title(&self) -> String {
        match &self.stage {
            Stage::Type => format!(
                "Type of {}, read as {}",
                self.column,
                dtype_label(&self.as_read)
            ),
            Stage::Format { ty, .. } => format!("{} Format", title_case(&dtype_label(ty))),
        }
    }
}

fn title_case(word: &str) -> String {
    let mut chars = word.chars();
    chars
        .next()
        .map(|c| c.to_uppercase().chain(chars).collect())
        .unwrap_or_default()
}

/// The fields of the combine form.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CombineField {
    Date,
    Time,
    Offset,
    Kind,
    Name,
}

/// "Combine into datetime": the columns a datetime is made from, and its name.
#[derive(Debug, Clone)]
pub struct CombineModal {
    pub focus: CombineField,
    /// The columns that can be a date, a time or an offset: text, dates and times.
    pub columns: Vec<String>,
    pub date: String,
    pub time: Option<String>,
    pub offset: Option<String>,
    pub kind: DerivedKind,
    pub name: crate::widgets::text_input::TextInput,
    /// The open picker and the field it is for.
    pub picker: Option<(CombineField, PickerState)>,
    /// Why Enter did not apply, said on the form's own line.
    pub problem: Option<String>,
}

/// What a column picker offers before the columns where a column may be left out.
pub const NONE: &str = "none";

impl CombineModal {
    /// The form, made from `date`, among `columns`. A column whose name says time is
    /// the time, if there is one; the result is named so no column is.
    pub fn new(date: String, columns: Vec<String>, taken: &[String]) -> Self {
        let time = columns
            .iter()
            .find(|c| **c != date && c.to_lowercase().contains("time"))
            .cloned();
        let mut name = "datetime".to_string();
        let mut n = 2;
        while taken.contains(&name) {
            name = format!("datetime_{n}");
            n += 1;
        }
        Self {
            focus: CombineField::Date,
            columns,
            date,
            time,
            offset: None,
            kind: DerivedKind::Datetime,
            name: {
                let mut input = crate::widgets::text_input::TextInput::new();
                input.set_value(name);
                input
            },
            picker: None,
            problem: None,
        }
    }

    /// The columns the result is made from, in the order `as` reads them.
    pub fn from(&self) -> Vec<String> {
        let mut from = vec![self.date.clone()];
        if self.kind == DerivedKind::Datetime {
            from.extend(self.time.clone());
            // An offset needs a time before it: `from` is date, time, offset.
            if self.time.is_some() {
                from.extend(self.offset.clone());
            }
        }
        from
    }

    /// The derived column it makes, as a spec's `[columns]` entry would.
    pub fn derived(&self) -> Result<crate::column_types::Derived, String> {
        let name = self.name.value().trim();
        if name.is_empty() {
            return Err("the new column needs a name".to_string());
        }
        Ok(crate::column_types::Derived {
            name: name.to_string(),
            from: self.from(),
            kind: self.kind,
            format: None,
        })
    }

    /// What Enter makes, in a line: `datetime = datetime from Lcl Date, Lcl Time`.
    pub fn spec_line(&self) -> String {
        format!(
            "{} = {} from {}",
            self.name.value().trim(),
            self.kind.name(),
            self.from().join(", ")
        )
    }

    /// The open picker's choices for `field`: the columns, after `none` where the
    /// column may be left out.
    pub fn open_picker(&mut self) {
        let field = self.focus;
        let optional = matches!(field, CombineField::Time | CombineField::Offset);
        let mut items = Vec::new();
        if optional {
            items.push(NONE.to_string());
        }
        items.extend(self.columns.iter().cloned());
        let current = match field {
            CombineField::Date => Some(self.date.clone()),
            CombineField::Time => self.time.clone(),
            CombineField::Offset => self.offset.clone(),
            _ => return,
        };
        let mut picker = PickerState::new(items);
        let at = current
            .and_then(|c| picker.items().iter().position(|i| *i == c))
            .unwrap_or(0);
        picker.select_original(at);
        self.picker = Some((field, picker));
    }

    /// The picker's choice, into its field.
    pub fn picker_choose(&mut self) {
        let Some((field, picker)) = self.picker.take() else {
            return;
        };
        let Some(chosen) = picker
            .selected_original()
            .map(|i| picker.items()[i].clone())
        else {
            return;
        };
        let column = (chosen != NONE).then_some(chosen);
        match field {
            CombineField::Date => {
                if let Some(c) = column {
                    self.date = c;
                }
            }
            CombineField::Time => self.time = column,
            CombineField::Offset => self.offset = column,
            _ => {}
        }
    }

    pub fn step_kind(&mut self, delta: i8) {
        self.kind = crate::form::step_value(&DerivedKind::ALL, self.kind, delta);
        crate::form::Form::settle_focus(self);
    }
}

impl crate::form::Form for CombineModal {
    type Field = CombineField;

    fn shown_picker(&mut self) -> Option<(&mut crate::widgets::ui::PickerState, bool)> {
        self.picker.as_mut().map(|(_, p)| (p, false))
    }

    fn dismiss_picker(&mut self) {
        self.picker = None;
    }

    fn pick(&mut self, _toggle: bool) {
        self.picker_choose();
    }

    fn fields(&self) -> Vec<(CombineField, crate::form::FieldKind)> {
        use crate::form::FieldKind;
        let mut fields = vec![
            (CombineField::Date, FieldKind::Picker { multi: false }),
            (CombineField::Kind, FieldKind::Choice),
        ];
        if self.kind == DerivedKind::Datetime {
            fields.insert(1, (CombineField::Time, FieldKind::Picker { multi: false }));
            fields.insert(
                2,
                (CombineField::Offset, FieldKind::Picker { multi: false }),
            );
        }
        fields.push((CombineField::Name, FieldKind::Text));
        fields
    }

    fn focused(&self) -> CombineField {
        self.focus
    }

    fn set_focused(&mut self, field: CombineField) {
        self.focus = field;
        self.name.set_focused(field == CombineField::Name);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_number_type_is_chosen_at_once_and_a_date_asks_its_format() {
        let mut modal = RetypeModal::new(
            "when".into(),
            DataType::String,
            None,
            vec!["03/04/2024".into()],
        );
        assert_eq!(modal.picker.items()[0], "as read");
        assert_eq!(modal.title(), "Type of when, read as str");
        modal
            .picker
            .select_original(TYPE_NAMES.iter().position(|n| *n == "i64").unwrap() + 1);
        assert_eq!(
            modal.choose(),
            Chosen::Type(ColumnType::named("i64", None).unwrap())
        );
        modal
            .picker
            .select_original(TYPE_NAMES.iter().position(|n| *n == "date").unwrap() + 1);
        assert_eq!(modal.choose(), Chosen::Format);
        let items = modal.picker.items().to_vec();
        assert!(
            items
                .iter()
                .any(|i| i.starts_with("%d/%m/%Y") && i.ends_with("2024-04-03")),
            "{items:?}"
        );
        assert!(items.iter().any(|i| i.starts_with("%m/%d/%Y")), "{items:?}");
        // The first that reads is chosen by default.
        let Chosen::Type(ty) = modal.choose() else {
            panic!("a format");
        };
        assert_eq!(ty.format.as_deref(), Some("%d/%m/%Y"));
        // A format typed that no line has is the format.
        modal.picker.filter = "%d.%m.%Y".into();
        modal.picker.clear_filter();
        for c in "%d %m %Y".chars() {
            modal.picker.type_char(c);
        }
        assert_eq!(
            modal.choose(),
            Chosen::Type(ColumnType::named("date", Some("%d %m %Y".into())).unwrap())
        );
        assert!(modal.back());
        assert!(matches!(modal.stage, Stage::Type));
        assert!(!modal.back());
    }

    #[test]
    fn the_combine_form_makes_the_specs_derived_column() {
        let columns = vec![
            "Lcl Date".to_string(),
            "Lcl Time".to_string(),
            "UTCOfst".to_string(),
        ];
        let mut modal = CombineModal::new("Lcl Date".into(), columns.clone(), &columns);
        assert_eq!(modal.time.as_deref(), Some("Lcl Time"));
        modal.offset = Some("UTCOfst".into());
        let derived = modal.derived().unwrap();
        assert_eq!(derived.from, columns);
        assert_eq!(derived.kind, DerivedKind::Datetime);
        assert_eq!(derived.name, "datetime");
        modal.step_kind(1);
        assert_eq!(modal.kind, DerivedKind::Date);
        assert_eq!(modal.from(), ["Lcl Date"], "a date from one column");
        modal.name.set_value(" ");
        assert!(modal.derived().is_err());
    }
}

//! Copy dialog state: scope × format, a column for the cell scope, and the
//! header toggle, with the last choices kept for the session so a repeat copy
//! is `y` `Enter`. The Python scope copies the view's pipeline as code rather
//! than its rows.

use crate::clipboard::CopyFormat;
use crate::widgets::ui::PickerState;

/// How much of the view one copy takes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CopyScope {
    Cell,
    #[default]
    Row,
    View,
    Table,
    /// The view's pipeline as a Python Polars script.
    Python,
}

impl CopyScope {
    pub const ALL: [Self; 5] = [Self::Cell, Self::Row, Self::View, Self::Table, Self::Python];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Cell => "Cell",
            Self::Row => "Row",
            Self::View => "View",
            Self::Table => "Table",
            Self::Python => "Python (Polars)",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CopyFocus {
    #[default]
    Scope,
    Column,
    Format,
    Header,
}

/// What the dialog needs to say what Enter will do; taken from the table when
/// the dialog opens, while the table cannot move under it.
#[derive(Debug, Clone, Copy, Default)]
pub struct CopyContext {
    /// The current row, as the row-numbers column would show it.
    pub row_number: usize,
    /// Rows and columns a view copy carries.
    pub view_rows: usize,
    pub view_cols: usize,
    /// Total rows, once the count has run.
    pub total_rows: Option<usize>,
}

pub struct CopyModal {
    pub focus: CopyFocus,
    pub scope: CopyScope,
    pub format: CopyFormat,
    /// Header per tabular scope. A row pasted mid-sheet rarely wants one; a
    /// view or table pasted whole usually does. Each remembers its last
    /// setting for the session.
    pub header_row: bool,
    pub header_view: bool,
    pub header_table: bool,
    /// The cell scope's column: the column cursor's when the dialog opens.
    pub column: Option<String>,
    pub available_columns: Vec<String>,
    /// The one Picker, open for the focused row; None while the form has the keys.
    pub picker: Option<PickerState>,
    /// Enter on an incomplete form re-accents the spec line instead of raising
    /// a modal; any other key clears it.
    pub attention: bool,
    pub context: CopyContext,
}

impl crate::app::form::Form for CopyModal {
    type Field = CopyFocus;

    fn shown_picker(&mut self) -> Option<(&mut crate::widgets::ui::PickerState, bool)> {
        self.picker.as_mut().map(|p| (p, false))
    }

    fn dismiss_picker(&mut self) {
        self.picker = None;
    }

    fn pick(&mut self, _toggle: bool) {
        self.picker_choose();
    }

    fn fields(&self) -> Vec<(CopyFocus, crate::app::form::FieldKind)> {
        use crate::app::form::FieldKind;
        self.row_order()
            .into_iter()
            .map(|row| {
                let kind = match row {
                    CopyFocus::Scope | CopyFocus::Format => FieldKind::Choice,
                    CopyFocus::Column => FieldKind::Picker { multi: false },
                    CopyFocus::Header => FieldKind::Checkbox,
                };
                (row, kind)
            })
            .collect()
    }

    fn focused(&self) -> CopyFocus {
        self.focus
    }

    fn set_focused(&mut self, field: CopyFocus) {
        self.focus = field;
    }
}

impl Default for CopyModal {
    fn default() -> Self {
        Self {
            focus: CopyFocus::Scope,
            scope: CopyScope::default(),
            format: CopyFormat::default(),
            header_row: false,
            header_view: true,
            header_table: true,
            column: None,
            available_columns: Vec::new(),
            picker: None,
            attention: false,
            context: CopyContext::default(),
        }
    }
}

impl CopyModal {
    pub fn new() -> Self {
        Self::default()
    }

    /// Open over the current table. Scope, format and headers are sticky; the
    /// cell scope's column is the column cursor's, `current`.
    pub fn open(&mut self, columns: Vec<String>, current: Option<&str>, context: CopyContext) {
        self.focus = CopyFocus::Scope;
        self.picker = None;
        self.attention = false;
        self.column = current
            .filter(|current| columns.iter().any(|c| c == current))
            .map(str::to_string);
        self.available_columns = columns;
        self.context = context;
    }

    pub fn close(&mut self) {
        self.picker = None;
    }

    /// The most rows any scope offers: the dialog's height, whatever the scope.
    pub const MOST_ROWS: u16 = 3;

    /// The rows the dialog offers, in Tab order. The cell scope trades the
    /// format and header rows for the column row; a Markdown table without its
    /// header row is not a table, so Markdown drops the header row too.
    pub fn row_order(&self) -> Vec<CopyFocus> {
        match self.scope {
            CopyScope::Cell => vec![CopyFocus::Scope, CopyFocus::Column],
            // Code has no format or header to choose.
            CopyScope::Python => vec![CopyFocus::Scope],
            _ => {
                let mut order = vec![CopyFocus::Scope, CopyFocus::Format];
                if self.format != CopyFormat::Markdown {
                    order.push(CopyFocus::Header);
                }
                order
            }
        }
    }

    /// Step the scope. The scope decides which rows exist, so a focus the new scope
    /// does not offer goes back to the scope row.
    pub fn step_scope(&mut self, delta: i8) {
        self.scope = crate::app::form::step_value(&CopyScope::ALL, self.scope, delta);
        if !self.row_order().contains(&self.focus) {
            self.focus = CopyFocus::Scope;
        }
    }

    /// Step the format. Markdown has no header row, so focus there moves back.
    pub fn step_format(&mut self, delta: i8) {
        self.format = crate::app::form::step_value(&CopyFormat::ALL, self.format, delta);
        if !self.row_order().contains(&self.focus) {
            self.focus = CopyFocus::Format;
        }
    }

    /// ←/→ on the column row: the next or previous column.
    pub fn step_column(&mut self, delta: i8) {
        let columns = &self.available_columns;
        if columns.is_empty() {
            return;
        }
        let at = self
            .column
            .as_ref()
            .and_then(|c| columns.iter().position(|name| name == c));
        let next = match at {
            Some(at) => crate::app::form::step_index(at, columns.len(), delta),
            None if delta < 0 => columns.len() - 1,
            None => 0,
        };
        self.column = Some(columns[next].clone());
    }

    /// The header setting the chosen scope carries.
    pub fn header(&self) -> bool {
        match self.scope {
            CopyScope::Cell | CopyScope::Python => false,
            CopyScope::Row => self.header_row,
            CopyScope::View => self.header_view,
            CopyScope::Table => self.header_table,
        }
    }

    pub fn toggle_header(&mut self) {
        match self.scope {
            CopyScope::Cell | CopyScope::Python => {}
            CopyScope::Row => self.header_row = !self.header_row,
            CopyScope::View => self.header_view = !self.header_view,
            CopyScope::Table => self.header_table = !self.header_table,
        }
    }

    /// Rows edited through the Picker: the column, a list too long to step. Scope
    /// and format step; the header row is a toggle.
    pub fn is_picker_row(&self, focus: CopyFocus) -> bool {
        focus == CopyFocus::Column
    }

    /// What the focused row's Picker offers.
    pub fn picker_items(&self) -> Vec<String> {
        match self.focus {
            CopyFocus::Column => self.available_columns.clone(),
            CopyFocus::Scope | CopyFocus::Format | CopyFocus::Header => Vec::new(),
        }
    }

    /// Open the Picker for the focused row, cursor on the current choice.
    pub fn open_picker(&mut self) {
        if !self.is_picker_row(self.focus) {
            return;
        }
        let items = self.picker_items();
        let current = self.column.as_deref();
        let mut state = PickerState::new(items.clone());
        if let Some(current) = current
            && let Some(i) = items.iter().position(|item| item == current)
        {
            state.select_original(i);
        }
        self.picker = Some(state);
    }

    /// Enter in the column Picker takes the cursor's item and closes it.
    pub fn picker_choose(&mut self) {
        let Some(state) = self.picker.take() else {
            return;
        };
        let Some(i) = state.selected_original() else {
            return;
        };
        if self.focus == CopyFocus::Column {
            self.column = self.available_columns.get(i).cloned();
        }
    }

    /// Why Enter cannot copy yet, or None when it can.
    pub fn validation_error(&self) -> Option<&'static str> {
        match self.scope {
            CopyScope::Cell if self.column.is_none() => Some("pick a column to copy"),
            _ => None,
        }
    }

    /// What Enter will do, echoed above the footer, or the gap that stops it.
    pub fn spec_line(&self) -> Result<String, String> {
        let ctx = &self.context;
        match self.scope {
            CopyScope::Cell => match &self.column {
                Some(column) => Ok(format!(
                    "Copy cell {column} of row {}",
                    thousands(ctx.row_number)
                )),
                None => Err("pick a column to copy".to_string()),
            },
            CopyScope::Row => Ok(format!(
                "Copy row {} as {}",
                thousands(ctx.row_number),
                self.format.as_str()
            )),
            CopyScope::View => Ok(format!(
                "Copy the {} {} {} view as {}",
                thousands(ctx.view_rows),
                crate::glyphs::get().times,
                thousands(ctx.view_cols),
                self.format.as_str()
            )),
            CopyScope::Table => match ctx.total_rows {
                Some(total) => Ok(format!(
                    "Copy all {} rows as {}",
                    thousands(total),
                    self.format.as_str()
                )),
                None => Ok(format!("Copy every row as {}", self.format.as_str())),
            },
            CopyScope::Python => Ok("Copy the view as a Python (Polars) script".to_string()),
        }
    }
}

/// `1234567` as `1,234,567`, for the spec line.
pub fn thousands(n: usize) -> String {
    let digits = n.to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    for (i, ch) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(ch);
    }
    out
}

#[cfg(test)]
mod tests;

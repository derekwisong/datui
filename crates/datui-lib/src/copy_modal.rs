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
    pub active: bool,
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

impl Default for CopyModal {
    fn default() -> Self {
        Self {
            active: false,
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
        self.active = true;
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
        self.active = false;
        self.picker = None;
    }

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

    pub fn next_focus(&mut self) {
        let order = self.row_order();
        let pos = order.iter().position(|&f| f == self.focus).unwrap_or(0);
        self.focus = order[(pos + 1) % order.len()];
    }

    pub fn prev_focus(&mut self) {
        let order = self.row_order();
        let pos = order.iter().position(|&f| f == self.focus).unwrap_or(0);
        self.focus = order[(pos + order.len() - 1) % order.len()];
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

    /// Rows edited through the Picker; the header row is a plain toggle.
    pub fn is_picker_row(&self, focus: CopyFocus) -> bool {
        matches!(
            focus,
            CopyFocus::Scope | CopyFocus::Format | CopyFocus::Column
        )
    }

    /// What the focused row's Picker offers.
    pub fn picker_items(&self) -> Vec<String> {
        match self.focus {
            CopyFocus::Scope => CopyScope::ALL
                .iter()
                .map(|s| s.as_str().to_string())
                .collect(),
            CopyFocus::Format => CopyFormat::ALL
                .iter()
                .map(|f| f.as_str().to_string())
                .collect(),
            CopyFocus::Column => self.available_columns.clone(),
            CopyFocus::Header => Vec::new(),
        }
    }

    /// Open the Picker for the focused row, cursor on the current choice.
    pub fn open_picker(&mut self) {
        if !self.is_picker_row(self.focus) {
            return;
        }
        let items = self.picker_items();
        let current = match self.focus {
            CopyFocus::Scope => Some(self.scope.as_str()),
            CopyFocus::Format => Some(self.format.as_str()),
            CopyFocus::Column => self.column.as_deref(),
            CopyFocus::Header => None,
        };
        let mut state = PickerState::new(items.clone());
        if let Some(current) = current
            && let Some(i) = items.iter().position(|item| item == current)
        {
            state.select_original(i);
        }
        self.picker = Some(state);
    }

    /// Enter in the Picker takes the cursor's item and closes it.
    pub fn picker_choose(&mut self) {
        let Some(state) = self.picker.take() else {
            return;
        };
        let Some(i) = state.selected_original() else {
            return;
        };
        match self.focus {
            CopyFocus::Scope => {
                self.scope = CopyScope::ALL[i];
                // The scope decides which rows exist; a focus the new scope
                // does not offer would strand Tab.
                if !self.row_order().contains(&self.focus) {
                    self.focus = CopyFocus::Scope;
                }
            }
            CopyFocus::Format => self.format = CopyFormat::ALL[i],
            CopyFocus::Column => {
                self.column = self.available_columns.get(i).cloned();
            }
            CopyFocus::Header => {}
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
mod tests {
    use super::*;

    #[test]
    fn the_scope_decides_which_rows_exist() {
        let mut modal = CopyModal::new();
        modal.scope = CopyScope::Cell;
        assert_eq!(modal.row_order(), vec![CopyFocus::Scope, CopyFocus::Column]);
        modal.scope = CopyScope::View;
        assert_eq!(
            modal.row_order(),
            vec![CopyFocus::Scope, CopyFocus::Format, CopyFocus::Header]
        );
        // Markdown always carries its header, so the toggle goes away.
        modal.format = CopyFormat::Markdown;
        assert_eq!(modal.row_order(), vec![CopyFocus::Scope, CopyFocus::Format]);
    }

    #[test]
    fn choices_are_sticky_across_opens_and_the_column_is_the_cursors() {
        let mut modal = CopyModal::new();
        let ab = || vec!["a".to_string(), "b".to_string()];
        modal.open(ab(), Some("a"), CopyContext::default());
        assert_eq!(modal.column.as_deref(), Some("a"));
        modal.scope = CopyScope::View;
        modal.format = CopyFormat::Markdown;
        modal.column = Some("a".into());
        modal.close();
        modal.open(ab(), Some("b"), CopyContext::default());
        assert_eq!(modal.scope, CopyScope::View);
        assert_eq!(modal.format, CopyFormat::Markdown);
        assert_eq!(modal.column.as_deref(), Some("b"), "the cursor moved to b");
        modal.close();
        modal.open(vec!["a".into()], None, CopyContext::default());
        assert_eq!(modal.column, None);
    }

    #[test]
    fn headers_remember_per_scope() {
        let mut modal = CopyModal::new();
        modal.scope = CopyScope::Row;
        assert!(!modal.header(), "a row pasted mid-sheet wants no header");
        modal.toggle_header();
        assert!(modal.header());
        modal.scope = CopyScope::View;
        assert!(modal.header(), "a view pasted whole wants one");
        modal.scope = CopyScope::Row;
        assert!(modal.header(), "the row's own setting survived the visit");
    }

    #[test]
    fn a_cell_copy_needs_a_column_and_the_spec_says_so() {
        let mut modal = CopyModal::new();
        modal.scope = CopyScope::Cell;
        assert!(modal.validation_error().is_some());
        assert!(modal.spec_line().is_err());
        modal.column = Some("city".into());
        modal.context.row_number = 1235;
        assert_eq!(modal.spec_line().unwrap(), "Copy cell city of row 1,235");
    }

    #[test]
    fn picking_a_scope_that_hides_the_focused_row_moves_focus_home() {
        let mut modal = CopyModal::new();
        modal.available_columns = vec!["a".into()];
        modal.scope = CopyScope::View;
        modal.focus = CopyFocus::Header;
        modal.focus = CopyFocus::Scope;
        modal.open_picker();
        // Cursor lands on the current scope (View); move up twice to Cell.
        let picker = modal.picker.as_mut().unwrap();
        picker.move_up();
        picker.move_up();
        modal.picker_choose();
        assert_eq!(modal.scope, CopyScope::Cell);
        assert!(modal.row_order().contains(&modal.focus));
    }

    #[test]
    fn the_python_scope_has_no_format_or_header() {
        let mut modal = CopyModal::new();
        modal.scope = CopyScope::Python;
        assert_eq!(modal.row_order(), vec![CopyFocus::Scope]);
        assert!(!modal.header());
        assert_eq!(
            modal.spec_line().unwrap(),
            "Copy the view as a Python (Polars) script"
        );
        modal.focus = CopyFocus::Scope;
        modal.next_focus();
        assert_eq!(modal.focus, CopyFocus::Scope, "Tab has nowhere else to go");
    }

    #[test]
    fn thousands_groups_digits() {
        assert_eq!(thousands(0), "0");
        assert_eq!(thousands(999), "999");
        assert_eq!(thousands(1000), "1,000");
        assert_eq!(thousands(1234567), "1,234,567");
    }
}

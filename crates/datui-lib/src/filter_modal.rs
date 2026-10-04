//! The Filters tab: one row per statement, and a three-step inline editor —
//! column via Picker, operator via a short Picker, value as text.

use crate::widgets::text_input::TextInput;
use crate::widgets::ui::PickerState;

#[derive(Debug, Clone, PartialEq, Eq, Copy, serde::Serialize, serde::Deserialize)]
pub enum FilterOperator {
    Eq,
    NotEq,
    Gt,
    Lt,
    GtEq,
    LtEq,
    Contains,
    NotContains,
    IsNull,
    IsNotNull,
}

impl FilterOperator {
    pub fn as_str(&self) -> &'static str {
        match self {
            FilterOperator::Eq => "=",
            FilterOperator::NotEq => "!=",
            FilterOperator::Gt => ">",
            FilterOperator::Lt => "<",
            FilterOperator::GtEq => ">=",
            FilterOperator::LtEq => "<=",
            FilterOperator::Contains => "contains",
            FilterOperator::NotContains => "!contains",
            FilterOperator::IsNull => "is null",
            FilterOperator::IsNotNull => "not null",
        }
    }

    /// Whether the statement compares with a value; the null tests take none.
    pub fn takes_value(&self) -> bool {
        !matches!(self, FilterOperator::IsNull | FilterOperator::IsNotNull)
    }

    pub fn iterator() -> impl Iterator<Item = FilterOperator> {
        [
            FilterOperator::Eq,
            FilterOperator::NotEq,
            FilterOperator::Gt,
            FilterOperator::Lt,
            FilterOperator::GtEq,
            FilterOperator::LtEq,
            FilterOperator::Contains,
            FilterOperator::NotContains,
            FilterOperator::IsNull,
            FilterOperator::IsNotNull,
        ]
        .iter()
        .copied()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Copy, serde::Serialize, serde::Deserialize)]
pub enum LogicalOperator {
    And,
    Or,
}

impl LogicalOperator {
    pub fn as_str(&self) -> &'static str {
        match self {
            LogicalOperator::And => "AND",
            LogicalOperator::Or => "OR",
        }
    }

    pub fn toggled(&self) -> Self {
        match self {
            LogicalOperator::And => LogicalOperator::Or,
            LogicalOperator::Or => LogicalOperator::And,
        }
    }

    pub fn iterator() -> impl Iterator<Item = LogicalOperator> {
        [LogicalOperator::And, LogicalOperator::Or].iter().copied()
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct FilterStatement {
    pub column: String,
    pub operator: FilterOperator,
    pub value: String,
    /// How this statement joins the one before it; meaningless on the first.
    pub logical_op: LogicalOperator,
}

/// Where the inline editor stands: the three steps walk left to right on one row.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum FilterEditStep {
    #[default]
    Column,
    Operator,
    Value,
}

/// One statement under edit. Enter advances a step and commits from the value;
/// Esc abandons the edit and only the edit.
pub struct FilterEditor {
    /// `Some(i)` rewrites statement `i`; `None` appends a new one.
    pub editing: Option<usize>,
    pub step: FilterEditStep,
    pub column: PickerState,
    pub operator: PickerState,
    pub value: TextInput,
    pub logical: LogicalOperator,
}

#[derive(Default)]
pub struct FilterModal {
    pub statements: Vec<FilterStatement>,
    pub available_columns: Vec<String>,
    /// The row the cursor is on: an index into `statements`, or one past the
    /// end for the trailing "add filter" row.
    pub cursor: usize,
    pub editor: Option<FilterEditor>,
    /// The table's column cursor, where a new filter's column starts.
    pub current_column: Option<String>,
}

impl FilterModal {
    pub fn new() -> Self {
        Self::default()
    }

    fn operator_names() -> Vec<String> {
        FilterOperator::iterator()
            .map(|op| op.as_str().to_string())
            .collect()
    }

    /// Rows the cursor can rest on: every statement plus the add row.
    pub fn row_count(&self) -> usize {
        self.statements.len() + 1
    }

    pub fn on_add_row(&self) -> bool {
        self.cursor >= self.statements.len()
    }

    pub fn move_cursor_up(&mut self) {
        self.cursor = if self.cursor == 0 {
            self.row_count() - 1
        } else {
            self.cursor - 1
        };
    }

    pub fn move_cursor_down(&mut self) {
        self.cursor = (self.cursor + 1) % self.row_count();
    }

    /// Start the editor on the cursor's row: pre-filled over a statement, empty
    /// on the add row.
    pub fn open_editor(&mut self, theme: &crate::config::Theme, history_limit: usize) {
        if self.available_columns.is_empty() {
            return;
        }
        let mut column = PickerState::new(self.available_columns.clone());
        let mut operator = PickerState::new(Self::operator_names());
        let mut value = TextInput::new()
            .with_history_limit(history_limit)
            .with_theme(theme);
        value.set_focused(false);
        let (editing, logical) = if self.on_add_row() {
            if let Some(i) = self
                .current_column
                .as_ref()
                .and_then(|current| self.available_columns.iter().position(|c| c == current))
            {
                column.select_original(i);
            }
            (None, LogicalOperator::And)
        } else {
            let statement = &self.statements[self.cursor];
            if let Some(i) = self
                .available_columns
                .iter()
                .position(|c| *c == statement.column)
            {
                column.select_original(i);
            }
            if let Some(i) = FilterOperator::iterator().position(|op| op == statement.operator) {
                operator.select_original(i);
            }
            value.set_value(&statement.value);
            (Some(self.cursor), statement.logical_op)
        };
        self.editor = Some(FilterEditor {
            editing,
            step: FilterEditStep::Column,
            column,
            operator,
            value,
            logical,
        });
    }

    pub fn cancel_editor(&mut self) {
        self.editor = None;
    }

    /// Commit the editor's statement; the edit dies if its column picker chose
    /// nothing (a filter narrowed to no match).
    pub fn commit_editor(&mut self) {
        let Some(editor) = self.editor.take() else {
            return;
        };
        let Some(column_idx) = editor.column.selected_original() else {
            return;
        };
        let operator = editor
            .operator
            .selected_original()
            .and_then(|i| FilterOperator::iterator().nth(i))
            .unwrap_or(FilterOperator::Eq);
        let statement = FilterStatement {
            column: self.available_columns[column_idx].clone(),
            operator,
            // A null test keeps no stale value from an earlier operator.
            value: if operator.takes_value() {
                editor.value.value().to_string()
            } else {
                String::new()
            },
            logical_op: editor.logical,
        };
        match editor.editing {
            Some(i) if i < self.statements.len() => self.statements[i] = statement,
            _ => {
                self.statements.push(statement);
                self.cursor = self.statements.len();
            }
        }
    }

    /// Move statement `i` one place earlier or later. Each and/or stays where it
    /// was, between the same two places, so `a or b` reordered is `b or a`, not
    /// `b and a`. Returns where it is now.
    pub fn move_statement(&mut self, i: usize, earlier: bool) -> usize {
        let to = if earlier {
            i.checked_sub(1)
        } else {
            Some(i + 1).filter(|to| *to < self.statements.len())
        };
        match to {
            Some(to) if i < self.statements.len() => {
                let (a, b) = (
                    self.statements[i].logical_op,
                    self.statements[to].logical_op,
                );
                self.statements.swap(i, to);
                self.statements[i].logical_op = a;
                self.statements[to].logical_op = b;
                to
            }
            _ => i,
        }
    }

    /// Delete the statement under the cursor; the add row deletes nothing.
    pub fn delete_at_cursor(&mut self) {
        if self.cursor < self.statements.len() {
            self.statements.remove(self.cursor);
            self.cursor = self.cursor.min(self.statements.len());
        }
    }

    /// Flip and/or on the cursor's row. The first statement joins nothing, and
    /// the add row is not a statement, so both are left alone.
    pub fn toggle_logical_at_cursor(&mut self) {
        if self.cursor > 0
            && let Some(statement) = self.statements.get_mut(self.cursor)
        {
            statement.logical_op = statement.logical_op.toggled();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn modal() -> FilterModal {
        let mut m = FilterModal::new();
        m.available_columns = vec!["salary".into(), "department".into(), "name".into()];
        m
    }

    fn theme() -> crate::config::Theme {
        crate::config::Theme::from_config(&crate::config::ThemeConfig::default()).unwrap()
    }

    #[test]
    fn a_null_test_keeps_no_value() {
        let mut m = modal();
        m.open_editor(&theme(), 10);
        {
            let editor = m.editor.as_mut().unwrap();
            editor.operator.select_original(
                FilterOperator::iterator()
                    .position(|op| op == FilterOperator::IsNull)
                    .unwrap(),
            );
            editor.value.set_value("left over");
        }
        m.commit_editor();
        let s = &m.statements[0];
        assert_eq!(s.operator, FilterOperator::IsNull);
        assert!(!s.operator.takes_value());
        assert_eq!(s.value, "");
    }

    #[test]
    fn adding_walks_column_operator_value_and_appends() {
        let mut m = modal();
        assert!(m.on_add_row());
        m.open_editor(&theme(), 10);
        {
            let editor = m.editor.as_mut().unwrap();
            // Type-to-narrow reaches the column without arrow-cycling the list.
            editor.column.type_char('d');
            editor.column.type_char('e');
            editor.step = FilterEditStep::Operator;
            editor.operator.select_original(2); // >
            editor.value.set_value("100");
        }
        m.commit_editor();
        assert_eq!(m.statements.len(), 1);
        let s = &m.statements[0];
        assert_eq!(s.column, "department");
        assert_eq!(s.operator, FilterOperator::Gt);
        assert_eq!(s.value, "100");
        assert!(m.on_add_row(), "the cursor lands back on the add row");
    }

    #[test]
    fn editing_rewrites_in_place_and_keeps_the_conjunction() {
        let mut m = modal();
        m.statements = vec![
            FilterStatement {
                column: "salary".into(),
                operator: FilterOperator::Gt,
                value: "1".into(),
                logical_op: LogicalOperator::And,
            },
            FilterStatement {
                column: "name".into(),
                operator: FilterOperator::Eq,
                value: "ann".into(),
                logical_op: LogicalOperator::Or,
            },
        ];
        m.cursor = 1;
        m.open_editor(&theme(), 10);
        {
            let editor = m.editor.as_mut().unwrap();
            assert_eq!(editor.editing, Some(1));
            assert_eq!(editor.value.value(), "ann", "the row arrives pre-filled");
            editor.value.set_value("bob");
        }
        m.commit_editor();
        assert_eq!(m.statements.len(), 2, "edited, not appended");
        assert_eq!(m.statements[1].value, "bob");
        assert_eq!(m.statements[1].logical_op, LogicalOperator::Or);
    }

    #[test]
    fn delete_and_conjunction_act_on_the_cursor_row_only() {
        let mut m = modal();
        m.statements = vec![
            FilterStatement {
                column: "salary".into(),
                operator: FilterOperator::Gt,
                value: "1".into(),
                logical_op: LogicalOperator::And,
            },
            FilterStatement {
                column: "name".into(),
                operator: FilterOperator::Eq,
                value: "ann".into(),
                logical_op: LogicalOperator::And,
            },
        ];
        m.cursor = 0;
        m.toggle_logical_at_cursor();
        assert_eq!(
            m.statements[0].logical_op,
            LogicalOperator::And,
            "the first statement joins nothing"
        );
        m.cursor = 1;
        m.toggle_logical_at_cursor();
        assert_eq!(m.statements[1].logical_op, LogicalOperator::Or);

        m.cursor = 2;
        m.delete_at_cursor();
        assert_eq!(m.statements.len(), 2, "the add row deletes nothing");
        m.cursor = 0;
        m.delete_at_cursor();
        assert_eq!(m.statements.len(), 1);
        assert_eq!(m.statements[0].column, "name");
    }

    #[test]
    fn moving_a_filter_keeps_each_and_or_in_its_place() {
        let mut m = modal();
        m.statements = vec![
            FilterStatement {
                column: "salary".into(),
                operator: FilterOperator::Gt,
                value: "1".into(),
                logical_op: LogicalOperator::And,
            },
            FilterStatement {
                column: "name".into(),
                operator: FilterOperator::Eq,
                value: "ann".into(),
                logical_op: LogicalOperator::Or,
            },
        ];
        assert_eq!(m.move_statement(1, true), 0);
        assert_eq!(m.statements[0].column, "name");
        assert_eq!(
            m.statements[1].logical_op,
            LogicalOperator::Or,
            "still an or"
        );
        assert_eq!(m.move_statement(0, true), 0, "the first stays first");
    }

    #[test]
    fn an_editor_with_no_columns_never_opens() {
        let mut m = FilterModal::new();
        m.open_editor(&theme(), 10);
        assert!(m.editor.is_none());
    }

    #[test]
    fn a_narrowed_to_nothing_column_commits_nothing() {
        let mut m = modal();
        m.open_editor(&theme(), 10);
        {
            let editor = m.editor.as_mut().unwrap();
            for c in "zzz".chars() {
                editor.column.type_char(c);
            }
        }
        m.commit_editor();
        assert!(m.statements.is_empty());
        assert!(m.editor.is_none(), "the edit still ends");
    }
}

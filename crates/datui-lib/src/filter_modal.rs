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
    /// A find's match kept as a filter: the text, in any case until a capital is
    /// typed. With [`ANY_COLUMN`], in any column.
    Has,
    /// A find's regex kept as a filter.
    HasRegex,
    /// A find's letters in order kept as a filter: `smth` keeps `Smith`.
    HasFuzzy,
}

/// The column of a filter kept from a find over every column: a row passes when any
/// of its columns matches.
pub const ANY_COLUMN: &str = "*";

/// How the column picker and the sidebar name [`ANY_COLUMN`].
pub const ANY_COLUMN_LABEL: &str = "any column shown";

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
            FilterOperator::Has => "has",
            FilterOperator::HasRegex => "has regex",
            FilterOperator::HasFuzzy => "has letters",
        }
    }

    /// A find kept as a filter: its value is matched as the find matched it, as text
    /// whatever the column's type.
    pub fn is_find(&self) -> bool {
        matches!(
            self,
            FilterOperator::Has | FilterOperator::HasRegex | FilterOperator::HasFuzzy
        )
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
            FilterOperator::Has,
            FilterOperator::HasRegex,
            FilterOperator::HasFuzzy,
        ]
        .iter()
        .copied()
    }
}

/// What a column holds, as far as the operators that read it go.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Operand {
    /// Text: every operator reads it.
    Text,
    /// Numbers, dates and times: compared, never searched as text.
    Ordered,
    /// True or false: equal or not, or null.
    Boolean,
    /// Anything else (lists, structs, bytes): no operator is ruled out.
    #[default]
    Any,
}

impl Operand {
    pub fn of(dtype: &polars::prelude::DataType) -> Self {
        use polars::prelude::DataType;
        match dtype {
            DataType::String => Self::Text,
            DataType::Boolean => Self::Boolean,
            d if d.is_categorical() || d.is_enum() => Self::Text,
            d if d.is_numeric() || d.is_temporal() => Self::Ordered,
            _ => Self::Any,
        }
    }

    /// Whether the operator picker offers `op` for a column of this kind.
    pub fn offers(self, op: FilterOperator) -> bool {
        use FilterOperator::*;
        match self {
            Self::Text | Self::Any => true,
            Self::Ordered => !matches!(op, Contains | NotContains | Has | HasRegex | HasFuzzy),
            Self::Boolean => matches!(op, Eq | NotEq | IsNull | IsNotNull),
        }
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
    /// The columns a find kept over every column ([`ANY_COLUMN`]) searches: the
    /// ones shown when it was kept, so the table, the script and a saved view match
    /// the same cells whatever the layout later. Empty for every other statement.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub columns: Vec<String>,
    pub column: String,
    pub operator: FilterOperator,
    pub value: String,
    /// How this statement joins the one before it; meaningless on the first.
    pub logical_op: LogicalOperator,
}

impl FilterStatement {
    /// How the statement reads in a line of text: `prcp > 0`, `name has "smith"`,
    /// `has /^US/` for a find kept over every column.
    pub fn describe(&self) -> String {
        let value = match self.operator {
            FilterOperator::IsNull | FilterOperator::IsNotNull => String::new(),
            FilterOperator::HasRegex => format!(" /{}/", self.value),
            FilterOperator::Has
            | FilterOperator::HasFuzzy
            | FilterOperator::Contains
            | FilterOperator::NotContains => format!(" \"{}\"", self.value),
            _ => format!(" {}", self.value),
        };
        let op = match self.operator {
            FilterOperator::HasRegex => "has",
            op => op.as_str(),
        };
        if self.column == ANY_COLUMN {
            format!("{op}{value}")
        } else {
            format!("{} {op}{value}", self.column)
        }
    }
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
    /// The operators the operator picker lists, in its order: those the column's
    /// type takes.
    pub operators: Vec<FilterOperator>,
    pub value: TextInput,
    pub logical: LogicalOperator,
}

impl FilterEditor {
    /// The operator chosen in the picker.
    pub fn selected_operator(&self) -> Option<FilterOperator> {
        self.operator
            .selected_original()
            .and_then(|i| self.operators.get(i))
            .copied()
    }

    /// List `operators` in the operator picker, keeping the choice where it is
    /// still offered.
    fn set_operators(&mut self, operators: Vec<FilterOperator>) {
        let chosen = self.selected_operator();
        let mut picker =
            PickerState::new(operators.iter().map(|op| op.as_str().to_string()).collect());
        if let Some(i) = chosen.and_then(|op| operators.iter().position(|o| *o == op)) {
            picker.select_original(i);
        }
        self.operator = picker;
        self.operators = operators;
    }
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
    /// The statements in effect on the table when the sidebar opened.
    pub applied: Vec<FilterStatement>,
    /// What each of `available_columns` holds, in the same order.
    pub operands: Vec<Operand>,
}

impl FilterModal {
    pub fn new() -> Self {
        Self::default()
    }

    /// The operators column choice `i` takes: by its type, and only a find's over
    /// any column shown.
    pub fn operators_for(&self, i: usize) -> Vec<FilterOperator> {
        let operand = self.operands.get(i).copied().unwrap_or_default();
        FilterOperator::iterator()
            .filter(|op| {
                if i >= self.available_columns.len() {
                    op.is_find()
                } else {
                    operand.offers(*op)
                }
            })
            .collect()
    }

    /// After the column step: the operator picker lists what the chosen column
    /// takes.
    pub fn retarget_operators(&mut self) {
        let Some(column) = self
            .editor
            .as_ref()
            .and_then(|editor| editor.column.selected_original())
        else {
            return;
        };
        let operators = self.operators_for(column);
        if let Some(editor) = self.editor.as_mut() {
            editor.set_operators(operators);
        }
    }

    /// Whether the statements staged differ from those in effect.
    pub fn has_unapplied_changes(&self) -> bool {
        self.statements != self.applied
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

    /// The column picker's choices: every column, then [`ANY_COLUMN_LABEL`], which
    /// a find's operators (`has`, `has regex`, `has letters`) take.
    fn column_choices(&self) -> Vec<String> {
        let mut choices = self.available_columns.clone();
        choices.push(ANY_COLUMN_LABEL.to_string());
        choices
    }

    /// The column choice `i` of the picker names: a column, or [`ANY_COLUMN`].
    pub fn column_at(&self, i: usize) -> String {
        self.available_columns
            .get(i)
            .cloned()
            .unwrap_or_else(|| ANY_COLUMN.to_string())
    }

    /// How a statement's column reads in the list: a find over every column is
    /// "any column shown" while it searches the columns shown, else counts them.
    pub fn column_label(&self, statement: &FilterStatement) -> String {
        if statement.column != ANY_COLUMN {
            return statement.column.clone();
        }
        if statement.columns.is_empty() || statement.columns == self.available_columns {
            ANY_COLUMN_LABEL.to_string()
        } else {
            format!("{} columns", statement.columns.len())
        }
    }

    /// Start the editor on the cursor's row: pre-filled over a statement, empty
    /// on the add row.
    pub fn open_editor(&mut self, theme: &crate::config::Theme, history_limit: usize) {
        if self.available_columns.is_empty() {
            return;
        }
        let mut column = PickerState::new(self.column_choices());
        let mut value = TextInput::new()
            .with_history_limit(history_limit)
            .with_theme(theme);
        value.set_focused(false);
        let (editing, logical, operator) = if self.on_add_row() {
            if let Some(i) = self
                .current_column
                .as_ref()
                .and_then(|current| self.available_columns.iter().position(|c| c == current))
            {
                column.select_original(i);
            }
            (None, LogicalOperator::And, None)
        } else {
            let statement = &self.statements[self.cursor];
            // A find kept over every column comes back as "any column shown".
            let at = if statement.column == ANY_COLUMN {
                Some(self.available_columns.len())
            } else {
                self.available_columns
                    .iter()
                    .position(|c| *c == statement.column)
            };
            if let Some(i) = at {
                column.select_original(i);
            }
            value.set_value(&statement.value);
            (
                Some(self.cursor),
                statement.logical_op,
                Some(statement.operator),
            )
        };
        // The column's operators, and the statement's own even where its type
        // would not offer it (a find kept on a number).
        let mut operators = column
            .selected_original()
            .map(|i| self.operators_for(i))
            .unwrap_or_else(|| FilterOperator::iterator().collect());
        if let Some(op) = operator
            && !operators.contains(&op)
        {
            operators.push(op);
        }
        let mut picker =
            PickerState::new(operators.iter().map(|op| op.as_str().to_string()).collect());
        if let Some(i) = operator.and_then(|op| operators.iter().position(|o| *o == op)) {
            picker.select_original(i);
        }
        self.editor = Some(FilterEditor {
            editing,
            step: FilterEditStep::Column,
            column,
            operator: picker,
            operators,
            value,
            logical,
        });
    }

    pub fn cancel_editor(&mut self) {
        self.editor = None;
    }

    /// Commit the editor's statement; the edit dies if its column picker chose
    /// nothing (a filter narrowed to no match). Any column shown takes only a
    /// find's operators: with another, the editor stays open on the operator.
    pub fn commit_editor(&mut self) {
        let Some(mut editor) = self.editor.take() else {
            return;
        };
        let Some(column_idx) = editor.column.selected_original() else {
            return;
        };
        let operator = editor.selected_operator().unwrap_or(FilterOperator::Eq);
        let column = self.column_at(column_idx);
        if column == ANY_COLUMN && !operator.is_find() {
            editor.step = FilterEditStep::Operator;
            self.editor = Some(editor);
            return;
        }
        // Over every column: the columns it searched before, else the ones shown.
        let columns = if column == ANY_COLUMN {
            editor
                .editing
                .and_then(|i| self.statements.get(i))
                .filter(|s| s.column == ANY_COLUMN && !s.columns.is_empty())
                .map(|s| s.columns.clone())
                .unwrap_or_else(|| self.available_columns.clone())
        } else {
            Vec::new()
        };
        let statement = FilterStatement {
            columns,
            column,
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

    /// A find kept over every column edits as one: its column comes back as "any
    /// column shown", and stays `*` when only the value changes; other operators
    /// do not take it.
    #[test]
    fn a_kept_find_over_every_column_edits_as_one() {
        let mut m = modal();
        m.statements = vec![FilterStatement {
            columns: Vec::new(),
            column: ANY_COLUMN.into(),
            operator: FilterOperator::HasFuzzy,
            value: "chkn".into(),
            logical_op: LogicalOperator::And,
        }];
        m.cursor = 0;
        m.open_editor(&theme(), 10);
        {
            let editor = m.editor.as_mut().unwrap();
            let picked = editor.column.selected_original().unwrap();
            assert_eq!(m.available_columns.len(), picked, "the last choice");
            editor.value.set_value("chicken");
        }
        m.commit_editor();
        assert!(m.editor.is_none());
        assert_eq!(m.statements[0].column, ANY_COLUMN);
        assert_eq!(m.statements[0].operator, FilterOperator::HasFuzzy);
        assert_eq!(m.statements[0].value, "chicken");

        // `=` over every column means nothing: only a find's operators are
        // offered, and one that is not waits on the operator.
        m.cursor = 0;
        m.open_editor(&theme(), 10);
        let editor = m.editor.as_mut().unwrap();
        assert!(editor.operators.iter().all(FilterOperator::is_find));
        editor.operators[0] = FilterOperator::Eq;
        editor.operator.select_original(0);
        m.commit_editor();
        let editor = m.editor.as_ref().expect("still editing");
        assert_eq!(editor.step, FilterEditStep::Operator);
        assert_eq!(m.statements[0].operator, FilterOperator::HasFuzzy);
    }

    /// The operator picker lists what the column's type takes: no text search on
    /// a number, only equality and nulls on a flag, and a find's operators over
    /// any column shown. A statement keeps its own operator even where its type
    /// would not offer it.
    #[test]
    fn operators_follow_the_columns_type() {
        let mut m = modal();
        m.available_columns.push("active".into());
        m.operands = vec![
            Operand::Ordered,
            Operand::Text,
            Operand::Text,
            Operand::Boolean,
        ];
        m.current_column = Some("salary".into());
        m.open_editor(&theme(), 10);
        let offered = |m: &FilterModal| m.editor.as_ref().unwrap().operators.clone();
        assert!(offered(&m).contains(&FilterOperator::GtEq));
        assert!(!offered(&m).contains(&FilterOperator::Contains));
        // Another column chosen: its operators, the choice kept where it can be.
        {
            let editor = m.editor.as_mut().unwrap();
            let eq = editor
                .operators
                .iter()
                .position(|op| *op == FilterOperator::Eq);
            editor.operator.select_original(eq.unwrap());
            editor.column.select_original(1);
        }
        m.retarget_operators();
        assert!(offered(&m).contains(&FilterOperator::Contains));
        assert_eq!(
            m.editor.as_ref().unwrap().selected_operator(),
            Some(FilterOperator::Eq)
        );
        m.editor.as_mut().unwrap().column.select_original(3);
        m.retarget_operators();
        assert_eq!(
            offered(&m),
            [
                FilterOperator::Eq,
                FilterOperator::NotEq,
                FilterOperator::IsNull,
                FilterOperator::IsNotNull
            ]
        );
        m.editor.as_mut().unwrap().column.select_original(4);
        m.retarget_operators();
        assert!(offered(&m).iter().all(FilterOperator::is_find));
        m.cancel_editor();

        // A find kept on a number edits with its own operator still there.
        m.statements = vec![FilterStatement {
            columns: Vec::new(),
            column: "salary".into(),
            operator: FilterOperator::Has,
            value: "12".into(),
            logical_op: LogicalOperator::And,
        }];
        m.cursor = 0;
        m.open_editor(&theme(), 10);
        assert_eq!(
            m.editor.as_ref().unwrap().selected_operator(),
            Some(FilterOperator::Has)
        );
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
                columns: Vec::new(),
                column: "salary".into(),
                operator: FilterOperator::Gt,
                value: "1".into(),
                logical_op: LogicalOperator::And,
            },
            FilterStatement {
                columns: Vec::new(),
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
                columns: Vec::new(),
                column: "salary".into(),
                operator: FilterOperator::Gt,
                value: "1".into(),
                logical_op: LogicalOperator::And,
            },
            FilterStatement {
                columns: Vec::new(),
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
                columns: Vec::new(),
                column: "salary".into(),
                operator: FilterOperator::Gt,
                value: "1".into(),
                logical_op: LogicalOperator::And,
            },
            FilterStatement {
                columns: Vec::new(),
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

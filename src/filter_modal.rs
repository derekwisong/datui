use ratatui::widgets::ListState;

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
        }
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

    pub fn iterator() -> impl Iterator<Item = LogicalOperator> {
        [LogicalOperator::And, LogicalOperator::Or].iter().copied()
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FilterStatement {
    pub column: String,
    pub operator: FilterOperator,
    pub value: String,
    pub logical_op: LogicalOperator,
}

#[derive(Debug, Default, PartialEq, Eq, Clone, Copy)]
pub enum FilterFocus {
    #[default]
    Column,
    Operator,
    Value,
    Logical,
    Add,
    Statements,
    Confirm,
    Clear,
}

#[derive(Default)]
pub struct FilterModal {
    pub active: bool,
    pub statements: Vec<FilterStatement>,
    pub available_columns: Vec<String>,

    pub new_column_idx: usize,
    pub new_operator_idx: usize,
    pub new_value: String,
    pub new_logical_idx: usize,

    pub focus: FilterFocus,
    pub list_state: ListState,
}

impl FilterModal {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_statement(&mut self) {
        if self.available_columns.is_empty() {
            return;
        }
        let op = FilterOperator::iterator()
            .nth(self.new_operator_idx)
            .unwrap();
        let log = LogicalOperator::iterator()
            .nth(self.new_logical_idx)
            .unwrap();
        let col = self.available_columns[self.new_column_idx].clone();

        self.statements.push(FilterStatement {
            column: col,
            operator: op,
            value: self.new_value.clone(),
            logical_op: log,
        });

        self.new_value.clear();
        self.focus = FilterFocus::Column;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_filter_modal_new() {
        let modal = FilterModal::new();
        assert!(!modal.active);
        assert!(modal.statements.is_empty());
        assert!(modal.available_columns.is_empty());
        assert_eq!(modal.new_column_idx, 0);
        assert_eq!(modal.new_operator_idx, 0);
        assert_eq!(modal.new_value, "");
        assert_eq!(modal.new_logical_idx, 0);
        assert_eq!(modal.focus, FilterFocus::Column);
    }

    #[test]
    fn test_filter_modal_add_statement() {
        let mut modal = FilterModal::new();
        modal.available_columns = vec!["a".to_string(), "b".to_string()];
        modal.new_column_idx = 1;
        modal.new_operator_idx = 2; // Gt
        modal.new_value = "10".to_string();
        modal.new_logical_idx = 1; // Or
        modal.add_statement();

        assert_eq!(modal.statements.len(), 1);
        let statement = &modal.statements[0];
        assert_eq!(statement.column, "b");
        assert_eq!(statement.operator, FilterOperator::Gt);
        assert_eq!(statement.value, "10");
        assert_eq!(statement.logical_op, LogicalOperator::Or);

        assert_eq!(modal.new_value, "");
        assert_eq!(modal.focus, FilterFocus::Column);
    }

    #[test]
    fn test_add_statement_no_columns() {
        let mut modal = FilterModal::new();
        modal.new_value = "test".to_string();
        modal.add_statement();
        assert!(modal.statements.is_empty());
    }
}

use super::*;

fn modal() -> FilterModal {
    FilterModal {
        available_columns: vec!["salary".into(), "department".into(), "name".into()],
        ..Default::default()
    }
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
    let mut m = FilterModal::default();
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

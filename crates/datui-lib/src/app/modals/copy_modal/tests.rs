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
fn stepping_to_a_scope_that_hides_the_focused_row_moves_focus_home() {
    let mut modal = CopyModal::new();
    modal.available_columns = vec!["a".into()];
    modal.scope = CopyScope::View;
    modal.focus = CopyFocus::Header;
    // View steps back twice to Cell, which has no header row.
    modal.step_scope(-1);
    modal.step_scope(-1);
    assert_eq!(modal.scope, CopyScope::Cell);
    assert_eq!(modal.focus, CopyFocus::Scope);
    modal.step_scope(-1);
    assert_eq!(modal.scope, CopyScope::Python, "the scope wraps");
}

#[test]
fn markdown_takes_focus_off_the_header_row() {
    let mut modal = CopyModal::new();
    modal.scope = CopyScope::View;
    modal.format = CopyFormat::Csv;
    modal.focus = CopyFocus::Header;
    modal.step_format(1);
    assert_eq!(modal.format, CopyFormat::Markdown);
    assert_eq!(modal.focus, CopyFocus::Format);
}

#[test]
fn the_column_row_steps_through_the_columns() {
    let mut modal = CopyModal::new();
    modal.available_columns = vec!["a".into(), "b".into()];
    modal.step_column(1);
    assert_eq!(
        modal.column.as_deref(),
        Some("a"),
        "unset starts at the first"
    );
    modal.step_column(1);
    assert_eq!(modal.column.as_deref(), Some("b"));
    modal.step_column(1);
    assert_eq!(modal.column.as_deref(), Some("a"), "and wraps");
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
    crate::app::form::Form::focus_next(&mut modal);
    assert_eq!(modal.focus, CopyFocus::Scope, "Tab has nowhere else to go");
}

#[test]
fn thousands_groups_digits() {
    assert_eq!(thousands(0), "0");
    assert_eq!(thousands(999), "999");
    assert_eq!(thousands(1000), "1,000");
    assert_eq!(thousands(1234567), "1,234,567");
}

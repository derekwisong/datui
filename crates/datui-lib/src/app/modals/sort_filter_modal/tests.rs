use super::*;
use crate::app::modals::filter_modal::{FilterOperator, FilterStatement, LogicalOperator};
use crate::app::modals::sort_modal::SortColumn;
use crate::widgets::column_widths::WidthChoice;

fn column(name: &str, place: usize, sort: Option<(usize, bool)>) -> SortColumn {
    SortColumn {
        name: name.to_string(),
        sort_order: sort.map(|(o, _)| o),
        sort_descending: sort.is_some_and(|(_, d)| d),
        display_order: place,
        is_locked: false,
        is_to_be_locked: false,
        is_visible: true,
        width: WidthChoice::Auto,
        shown_width: None,
    }
}

fn statement(column: &str) -> FilterStatement {
    FilterStatement {
        columns: Vec::new(),
        column: column.to_string(),
        operator: FilterOperator::Eq,
        value: "x".to_string(),
        logical_op: LogicalOperator::And,
    }
}

fn modal() -> SortFilterModal {
    let mut m = SortFilterModal::new();
    m.sort.columns = vec![
        column("a", 0, None),
        column("b", 1, Some((2, true))),
        column("c", 2, Some((1, false))),
    ];
    m.filter.statements = vec![statement("a"), statement("c")];
    m.filter.available_columns = vec!["a".into(), "b".into(), "c".into()];
    let theme =
        crate::config::Theme::from_config(&crate::config::ThemeConfig::default()).expect("theme");
    m.open(10, &theme, Some("a"));
    m
}

#[test]
fn it_opens_on_the_first_entry_in_effect() {
    let m = modal();
    assert_eq!(m.active_tab, SortFilterTab::InEffect);
    assert_eq!(m.focus, SortFilterField::Sort(0));
    let mut empty = SortFilterModal::new();
    let theme =
        crate::config::Theme::from_config(&crate::config::ThemeConfig::default()).expect("theme");
    empty.open(10, &theme, None);
    assert_eq!(empty.focus, SortFilterField::AddSort);
}

#[test]
fn the_rows_walk_sorts_then_filters_then_back_to_the_tabs() {
    let mut m = modal();
    let walked: Vec<SortFilterField> = (0..7)
        .map(|_| {
            let at = m.focus;
            m.focus_next();
            at
        })
        .collect();
    assert_eq!(
        walked,
        [
            SortFilterField::Sort(0),
            SortFilterField::Sort(1),
            SortFilterField::AddSort,
            SortFilterField::Filter(0),
            SortFilterField::Filter(1),
            SortFilterField::AddFilter,
            SortFilterField::TabBar,
        ]
    );
}

#[test]
fn a_sort_entry_flips_moves_and_goes() {
    let mut m = modal();
    // Sort(0) is c, ascending.
    m.sort.flip_sort(0);
    assert_eq!(
        m.sort.sorted_columns_and_directions(),
        (vec!["c".to_string(), "b".to_string()], vec![true, true])
    );
    m.move_focused(false);
    assert_eq!(m.focus, SortFilterField::Sort(1), "focus goes with it");
    assert_eq!(m.sort.get_sorted_columns(), ["b", "c"]);
    m.remove_focused();
    assert_eq!(m.sort.get_sorted_columns(), ["b"]);
    assert_eq!(m.focus, SortFilterField::Sort(0));
    m.remove_focused();
    assert_eq!(m.focus, SortFilterField::AddSort);
}

#[test]
fn adding_a_sort_offers_the_unsorted_columns_from_the_cursor() {
    let mut m = modal();
    m.focus = SortFilterField::AddSort;
    m.open_sort_picker();
    let picker = m.sort_picker.as_ref().expect("open");
    assert_eq!(picker.items(), ["a"], "b and c are sorted already");
    m.choose_sort();
    assert_eq!(m.sort.get_sorted_columns(), ["c", "b", "a"]);
    assert_eq!(m.focus, SortFilterField::Sort(2));
}

#[test]
fn a_filter_entry_moves_and_goes() {
    let mut m = modal();
    m.set_focused(SortFilterField::Filter(1));
    assert_eq!(m.filter.cursor, 1);
    m.move_focused(true);
    assert_eq!(m.filter.statements[0].column, "c");
    assert_eq!(m.focus, SortFilterField::Filter(0));
    m.remove_focused();
    assert_eq!(m.filter.statements.len(), 1);
    m.remove_focused();
    assert_eq!(m.focus, SortFilterField::AddFilter);
    assert!(m.filter.on_add_row());
}

#[test]
fn the_columns_tab_walks_find_then_every_column() {
    let mut m = modal();
    m.switch_tab();
    assert_eq!(m.focus, SortFilterField::TabBar);
    m.focus_next();
    assert_eq!(m.focus, SortFilterField::Find);
    m.focus_next();
    m.focus_next();
    assert_eq!(m.focus, SortFilterField::Column(1));
    assert_eq!(
        m.sort.table_state.selected(),
        Some(1),
        "the list cursor follows"
    );
}

#[test]
fn clearing_drops_every_sort_and_filter() {
    let mut m = modal();
    m.clear_in_effect();
    assert!(m.sort.get_sorted_columns().is_empty());
    assert!(m.filter.statements.is_empty());
    assert_eq!(m.focus, SortFilterField::AddSort);
}

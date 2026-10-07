use super::*;

fn modal() -> SortModal {
    SortModal::default()
}

fn columns(names: &[&str]) -> Vec<SortColumn> {
    names
        .iter()
        .enumerate()
        .map(|(i, name)| SortColumn {
            name: name.to_string(),
            sort_order: None,
            sort_descending: false,
            display_order: i,
            is_locked: false,
            is_to_be_locked: false,
            is_visible: true,
            width: WidthChoice::Auto,
            shown_width: Some(10),
        })
        .collect()
}

#[test]
fn test_sort_modal_new() {
    let modal = modal();
    assert_eq!(modal.filter_input.value(), "");
    assert!(modal.columns.is_empty());
    assert!(modal.table_state.selected().is_none());
}

#[test]
fn test_filtered_columns() {
    let mut modal = modal();
    modal.columns = columns(&["Apple", "Banana", "Orange"]);
    modal.filter_input.set_value("an");
    let filtered = modal.filtered_columns();
    assert_eq!(filtered.len(), 2);
    assert_eq!(filtered[0].1.name, "Banana");
    assert_eq!(filtered[1].1.name, "Orange");
}

/// The list is worked out once per change of the find text, the names or the
/// order, however often it is asked for.
#[test]
fn the_filtered_list_is_worked_out_once_per_change() {
    let builds = |m: &SortModal| m.shown_builds.load(std::sync::atomic::Ordering::Relaxed);
    let mut modal = modal();
    modal.columns = columns(&["Apple", "Banana", "Orange"]);
    for _ in 0..5 {
        modal.filtered_columns();
    }
    assert_eq!(builds(&modal), 1);
    modal.filter_input.set_value("an");
    modal.filtered_columns();
    modal.filtered_columns();
    assert_eq!(builds(&modal), 2);
    modal.columns[1].display_order = 9;
    let names: Vec<&str> = modal
        .filtered_columns()
        .iter()
        .map(|(_, c)| c.name.as_str())
        .collect();
    assert_eq!(names, ["Orange", "Banana"]);
    assert_eq!(builds(&modal), 3);
}

/// Space walks one column through none → ascending → descending → none,
/// each column carrying its own direction.
#[test]
fn space_cycles_a_column_through_the_three_states() {
    let mut modal = modal();
    modal.columns = columns(&["A", "B"]);
    modal.table_state.select(Some(0));

    modal.cycle_sort();
    assert_eq!(modal.columns[0].sort_order, Some(1));
    assert!(!modal.columns[0].sort_descending, "first press: ascending");

    modal.cycle_sort();
    assert_eq!(modal.columns[0].sort_order, Some(1));
    assert!(modal.columns[0].sort_descending, "second press: descending");

    modal.cycle_sort();
    assert_eq!(modal.columns[0].sort_order, None, "third press: out");
    assert!(!modal.columns[0].sort_descending);
}

/// A column leaving the sort renumbers the ones after it, whatever their
/// directions, and the directions travel with their columns.
#[test]
fn leaving_the_sort_renumbers_and_keeps_directions() {
    let mut modal = modal();
    modal.columns = columns(&["A", "B", "C"]);
    modal.table_state.select(Some(1)); // B ascending, order 1
    modal.cycle_sort();
    modal.table_state.select(Some(0)); // A order 2, then descending
    modal.cycle_sort();
    modal.cycle_sort();
    modal.table_state.select(Some(2)); // C order 3
    modal.cycle_sort();

    let (names, directions) = modal.sorted_columns_and_directions();
    assert_eq!(names, ["B", "A", "C"]);
    assert_eq!(directions, [false, true, false]);

    // B cycles out (asc → desc → none): A and C move up, directions intact.
    modal.table_state.select(Some(1));
    modal.cycle_sort();
    modal.cycle_sort();
    let (names, directions) = modal.sorted_columns_and_directions();
    assert_eq!(names, ["A", "C"]);
    assert_eq!(directions, [true, false]);
}

/// `0` takes a sorted column out and stages the change; on an unsorted
/// column it changes nothing and stages nothing.
#[test]
fn zero_removes_a_column_and_stages_the_change() {
    let mut modal = modal();
    modal.columns = columns(&["A", "B"]);
    modal.columns[0].sort_order = Some(1);
    modal.table_state.select(Some(1));
    modal.jump_selection_to_order(0);
    assert!(!modal.has_unapplied_changes, "B was not sorted");

    modal.table_state.select(Some(0));
    modal.jump_selection_to_order(0);
    assert_eq!(modal.columns[0].sort_order, None);
    assert!(modal.has_unapplied_changes);
}

/// A digit past the end of the order changes nothing and says which
/// positions exist.
#[test]
fn a_digit_past_the_end_says_why() {
    let mut modal = modal();
    modal.columns = columns(&["A", "B", "C"]);
    modal.columns[0].sort_order = Some(1);
    modal.table_state.select(Some(1));
    modal.jump_selection_to_order(5);
    assert_eq!(modal.columns[1].sort_order, None);
    assert!(!modal.has_unapplied_changes);
    assert_eq!(
        modal.status.as_deref(),
        Some("Position 5 is past the end; use 1-2.")
    );

    // The sorted column itself has only the positions already there, and
    // its own position is no change.
    modal.table_state.select(Some(0));
    modal.jump_selection_to_order(2);
    assert_eq!(
        modal.status.as_deref(),
        Some("Position 2 is past the end; use 1.")
    );
    modal.jump_selection_to_order(1);
    assert_eq!(modal.columns[0].sort_order, Some(1));
    assert!(!modal.has_unapplied_changes);
}

#[test]
fn test_move_selection_up() {
    let mut modal = modal();
    modal.columns = columns(&["A", "B"]);
    modal.columns[0].sort_order = Some(2);
    modal.columns[1].sort_order = Some(1);
    modal.table_state.select(Some(0)); // Select "A"
    modal.move_selection_up();
    assert_eq!(modal.columns[0].sort_order, Some(1));
    assert_eq!(modal.columns[1].sort_order, Some(2));
}

#[test]
fn test_move_selection_down() {
    let mut modal = modal();
    modal.columns = columns(&["A", "B"]);
    modal.columns[0].sort_order = Some(2);
    modal.columns[1].sort_order = Some(1);
    modal.table_state.select(Some(1)); // Select "B"
    modal.move_selection_down();
    assert_eq!(modal.columns[0].sort_order, Some(1));
    assert_eq!(modal.columns[1].sort_order, Some(2));
}

#[test]
fn the_sort_list_flips_moves_and_drops_entries() {
    let mut modal = modal();
    modal.columns = columns(&["A", "B", "C"]);
    assert_eq!(modal.add_sort("C"), Some(0));
    assert_eq!(modal.add_sort("A"), Some(1));
    assert_eq!(
        modal.add_sort("A"),
        Some(1),
        "a sorted column keeps its place"
    );
    assert_eq!(modal.get_sorted_columns(), ["C", "A"]);
    modal.flip_sort(1);
    assert_eq!(modal.sorted_columns_and_directions().1, [false, true]);
    assert_eq!(modal.move_sort_entry(1, true), 0);
    assert_eq!(modal.get_sorted_columns(), ["A", "C"]);
    assert_eq!(modal.move_sort_entry(0, true), 0, "the first stays first");
    modal.remove_sort_entry(0);
    assert_eq!(modal.get_sorted_columns(), ["C"]);
    assert_eq!(modal.columns[2].sort_order, Some(1), "renumbered");
}

#[test]
fn the_sort_cycles_both_ways() {
    let mut modal = modal();
    modal.columns = columns(&["A"]);
    modal.table_state.select(Some(0));
    modal.cycle_sort_back();
    assert_eq!(modal.sorted_columns_and_directions().1, [true]);
    modal.cycle_sort_back();
    assert_eq!(modal.sorted_columns_and_directions().1, [false]);
    modal.cycle_sort_back();
    assert!(modal.get_sorted_columns().is_empty());
}

#[test]
fn test_clear_selection() {
    let mut modal = modal();
    modal.columns = columns(&["A", "B"]);
    modal.columns[0].sort_order = Some(1);
    modal.columns[0].sort_descending = true;
    modal.columns[1].sort_order = Some(2);
    modal.clear_selection();
    assert!(modal.columns[0].sort_order.is_none());
    assert!(!modal.columns[0].sort_descending);
    assert!(modal.columns[1].sort_order.is_none());
}

/// Narrower and wider step from the width drawn, fit and automatic stage as
/// asked, and each change is staged rather than applied.
#[test]
fn width_changes_are_staged_on_the_column_under_the_cursor() {
    let mut modal = modal();
    modal.columns = columns(&["a", "b"]);
    modal.table_state.select(Some(1));
    modal.change_width(WidthChoice::wider);
    assert_eq!(modal.columns[1].width, WidthChoice::Manual(14));
    assert!(modal.has_unapplied_changes);
    modal.change_width(WidthChoice::narrower);
    modal.change_width(WidthChoice::narrower);
    assert_eq!(modal.columns[1].width, WidthChoice::Manual(6));
    modal.change_width(|_, _| WidthChoice::Fit);
    assert_eq!(modal.columns[1].width, WidthChoice::Fit);
    modal.change_width(|_, _| WidthChoice::Auto);
    assert_eq!(modal.columns[0].width, WidthChoice::Auto);
    assert_eq!(
        modal.width_choices(),
        vec![
            ("a".to_string(), WidthChoice::Auto),
            ("b".to_string(), WidthChoice::Auto)
        ]
    );
    modal.change_width(WidthChoice::wider);
    modal.clear_selection();
    assert_eq!(modal.columns[1].width, WidthChoice::Auto);
}

fn names(order: &[&str]) -> Vec<String> {
    order.iter().map(|s| s.to_string()).collect()
}

#[test]
fn hiding_and_showing_keeps_the_column_in_place() {
    let mut modal = modal();
    modal.columns = columns(&["A", "B", "C", "D"]);
    modal.table_state.select(Some(1));
    modal.toggle_visibility();
    assert_eq!(modal.get_column_order(), names(&["A", "C", "D"]));
    // The list does not move under the cursor: the next row is still C.
    let listed: Vec<&str> = modal
        .filtered_columns()
        .iter()
        .map(|(_, c)| c.name.as_str())
        .collect();
    assert_eq!(listed, ["A", "B", "C", "D"]);
    modal.toggle_visibility();
    assert_eq!(modal.get_column_order(), names(&["A", "B", "C", "D"]));
}

#[test]
fn a_hidden_column_keeps_its_lock_but_is_not_counted() {
    let mut modal = modal();
    modal.columns = columns(&["A", "B", "C", "D"]);
    modal.table_state.select(Some(2));
    modal.toggle_lock_at_column();
    assert_eq!(modal.get_locked_columns_count(), 3);
    modal.table_state.select(Some(1));
    modal.toggle_visibility();
    assert_eq!(modal.get_locked_columns_count(), 2, "A and C stay frozen");
    modal.toggle_visibility();
    assert_eq!(modal.get_locked_columns_count(), 3, "B is frozen again");
}

#[test]
fn hidden_columns_go_back_after_the_column_they_followed() {
    let all = names(&["a", "b", "c", "d", "e"]);
    // Last applied as c, a, b, d, e with b and d hidden.
    let reference = names(&["c", "a", "b", "d", "e"]);
    assert_eq!(
        order_with_hidden(&names(&["c", "a", "e"]), &all, &reference),
        names(&["c", "a", "b", "d", "e"])
    );
    // Nothing applied from the sidebar: schema order places them.
    assert_eq!(
        order_with_hidden(&names(&["c", "e"]), &all, &[]),
        names(&["a", "b", "c", "d", "e"])
    );
}

use super::*;

const ROOM: Room = Room {
    width: 30,
    lead: 0,
    padding: 2,
};

fn widths(ws: &[u16]) -> impl FnMut(usize) -> Option<u16> + '_ {
    |i| ws.get(i).copied()
}

#[test]
fn on_screen_labels() {
    let on = OnScreen {
        first: 41,
        last: 47,
        cursor: 43,
        total: 300,
    };
    assert_eq!(on.label(false), "col 43 of 300");
    assert_eq!(on.label(true), "col 43/300");
}

#[test]
fn whole_columns_count_padding_and_lead() {
    // 8 + 2 + 8 + 2 + 8 = 28 fits in 30; a fourth does not.
    let ws = [8, 8, 8, 8];
    assert_eq!(whole_from(0, 4, ROOM, &mut widths(&ws)), Some(3));
    let lead = Room { lead: 3, ..ROOM };
    assert_eq!(whole_from(0, 4, lead, &mut widths(&ws)), Some(2));
    // Exactly the room is whole.
    assert_eq!(whole_from(0, 1, ROOM, &mut widths(&[30])), Some(1));
    assert_eq!(whole_from(0, 1, ROOM, &mut widths(&[31])), Some(0));
}

#[test]
fn page_right_starts_at_the_first_column_not_whole() {
    let ws = [8; 12];
    // 0..3 whole, so the next page starts at 3.
    assert_eq!(
        plan(ColumnMove::PageRight, 0, 12, ROOM, widths(&ws)),
        Some(3)
    );
    assert_eq!(
        plan(ColumnMove::PageRight, 3, 12, ROOM, widths(&ws)),
        Some(6)
    );
    // A column cut at the edge starts the next page, so it is read whole there.
    let ws = [10, 10, 15, 4, 4];
    assert_eq!(
        plan(ColumnMove::PageRight, 0, 5, ROOM, widths(&ws)),
        Some(2)
    );
}

#[test]
fn page_right_never_leaves_a_short_last_page() {
    let ws = [8; 10];
    // From 6 the next page would start at 9 and show one column; the last page
    // holds 7, 8 and 9.
    assert_eq!(
        plan(ColumnMove::PageRight, 6, 10, ROOM, widths(&ws)),
        Some(7)
    );
    // From the last page there is nowhere to go.
    assert_eq!(
        plan(ColumnMove::PageRight, 7, 10, ROOM, widths(&ws)),
        Some(7)
    );
}

#[test]
fn a_column_wider_than_the_room_still_moves_one_at_a_time() {
    let ws = [50, 50, 50];
    assert_eq!(
        plan(ColumnMove::PageRight, 0, 3, ROOM, widths(&ws)),
        Some(1)
    );
    assert_eq!(
        plan(ColumnMove::PageRight, 1, 3, ROOM, widths(&ws)),
        Some(2)
    );
    assert_eq!(plan(ColumnMove::PageLeft, 2, 3, ROOM, widths(&ws)), Some(1));
    assert_eq!(plan(ColumnMove::Last, 0, 3, ROOM, widths(&ws)), Some(2));
}

#[test]
fn page_left_ends_at_the_column_before_the_first() {
    let ws = [8; 12];
    assert_eq!(
        plan(ColumnMove::PageLeft, 6, 12, ROOM, widths(&ws)),
        Some(3)
    );
    assert_eq!(
        plan(ColumnMove::PageLeft, 2, 12, ROOM, widths(&ws)),
        Some(0)
    );
    assert_eq!(
        plan(ColumnMove::PageLeft, 0, 12, ROOM, widths(&ws)),
        Some(0)
    );
    // Mixed widths: 5 and 4 fit before 6 (20 + 2 + 4 + 2 + 2 = 30); 3 does not.
    let ws = [2, 2, 2, 9, 2, 4, 20, 1];
    assert_eq!(plan(ColumnMove::PageLeft, 7, 8, ROOM, widths(&ws)), Some(4));
}

#[test]
fn last_fills_the_room_ending_at_the_last_column() {
    let ws = [8; 10];
    assert_eq!(plan(ColumnMove::Last, 0, 10, ROOM, widths(&ws)), Some(7));
    // Scrolled past the last page, it comes back to fill the room.
    assert_eq!(plan(ColumnMove::Last, 9, 10, ROOM, widths(&ws)), Some(7));
    // Everything fits: nothing scrolls.
    assert_eq!(plan(ColumnMove::Last, 0, 3, ROOM, widths(&ws)), Some(0));
}

#[test]
fn reveal_keeps_a_column_already_whole_on_screen() {
    let ws = [8; 10];
    assert_eq!(
        plan(ColumnMove::Reveal(2), 0, 10, ROOM, widths(&ws)),
        Some(0)
    );
    assert_eq!(
        plan(ColumnMove::Reveal(3), 0, 10, ROOM, widths(&ws)),
        Some(3)
    );
    assert_eq!(
        plan(ColumnMove::Reveal(1), 4, 10, ROOM, widths(&ws)),
        Some(1)
    );
    // On the last page it lands there rather than alone at the left.
    assert_eq!(
        plan(ColumnMove::Reveal(9), 0, 10, ROOM, widths(&ws)),
        Some(7)
    );
}

#[test]
fn keep_scrolls_only_as_far_as_the_column() {
    let ws = [8; 10];
    // Whole on screen (0..=2 fit in 30): nothing moves.
    for column in 0..3 {
        assert_eq!(
            plan(ColumnMove::Keep(column), 0, 10, ROOM, widths(&ws)),
            Some(0)
        );
    }
    // Past the right edge: it becomes the last whole column.
    assert_eq!(plan(ColumnMove::Keep(3), 0, 10, ROOM, widths(&ws)), Some(1));
    assert_eq!(plan(ColumnMove::Keep(9), 0, 10, ROOM, widths(&ws)), Some(7));
    // Left of the screen: it becomes the first.
    assert_eq!(plan(ColumnMove::Keep(2), 5, 10, ROOM, widths(&ws)), Some(2));
    // A column wider than the room stands alone.
    let wide = [8, 8, 50, 8];
    assert_eq!(
        plan(ColumnMove::Keep(2), 0, 4, ROOM, widths(&wide)),
        Some(2)
    );
    // Only the columns up to it are measured.
    let mut seen = Vec::new();
    plan(ColumnMove::Keep(4), 2, 10, ROOM, |i| {
        seen.push(i);
        Some(8)
    });
    assert!(seen.iter().all(|&i| i <= 4), "{seen:?}");
}

#[test]
fn no_columns_and_one_column() {
    let ws: [u16; 0] = [];
    for mv in [
        ColumnMove::PageLeft,
        ColumnMove::PageRight,
        ColumnMove::Last,
    ] {
        assert_eq!(plan(mv, 0, 0, ROOM, widths(&ws)), Some(0));
    }
    let ws = [80];
    for mv in [
        ColumnMove::PageLeft,
        ColumnMove::PageRight,
        ColumnMove::Last,
    ] {
        assert_eq!(plan(mv, 0, 1, ROOM, widths(&ws)), Some(0));
    }
}

#[test]
fn an_unknown_width_defers_the_plan() {
    let known = |i: usize| (i < 4).then_some(8);
    // The next page is known, but the last page is not.
    assert_eq!(plan(ColumnMove::PageRight, 0, 10, ROOM, known), None);
    assert_eq!(plan(ColumnMove::Last, 0, 10, ROOM, known), None);
    // Paging back over drawn columns needs nothing more.
    assert_eq!(plan(ColumnMove::PageLeft, 4, 10, ROOM, known), Some(1));
}

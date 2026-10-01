//! Sideways moves across the main table's scrolling columns, planned from the widths
//! the columns are drawn at.
//!
//! A plan only adds up widths: it formats and reads nothing. A width the table has not
//! drawn in this view (since its widths were last relearned) is unknown, and a plan
//! that needs one says so with `None`; the next draw then measures that column from
//! the rows on screen and plans again. Indices here are scrolling indices: 0 is the
//! first column right of the frozen ones.

/// The room the scrolling columns are laid out in, as the table last drew it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Room {
    /// Cells from the frozen separator (or the left edge) to the right edge.
    pub width: u16,
    /// Cells before the first column: the gap after the frozen separator.
    pub lead: u16,
    /// Cells between two columns.
    pub padding: u16,
}

/// Which shown columns the table drew, counted from 1 in the table's order: frozen
/// columns first, hidden columns not at all. `first` and `last` are the scrolling
/// columns on screen; the frozen ones are always on screen before them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OnScreen {
    pub first: usize,
    pub last: usize,
    pub total: usize,
}

impl OnScreen {
    /// The widest label this table can have: the bar reserves it, so paging moves
    /// nothing on the bar as the numbers change.
    pub fn widest_label(&self, compact: bool) -> String {
        Self {
            first: self.total,
            last: self.total + 1,
            total: self.total,
        }
        .label(compact)
    }

    /// `cols 41-47 of 300`, or `cols 41-47/300` where the bar is short of room.
    pub fn label(&self, compact: bool) -> String {
        let range = if self.last > self.first {
            format!("{}-{}", self.first, self.last)
        } else {
            self.first.to_string()
        };
        if compact {
            format!("cols {range}/{}", self.total)
        } else {
            format!("cols {range} of {}", self.total)
        }
    }
}

/// A sideways move. The pages need the columns' widths to land; the rest do not, and
/// are moves here so that one typed behind a page waiting on a draw lands after it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnMove {
    /// One column left.
    StepLeft,
    /// One column right.
    StepRight,
    /// The first scrolling column.
    First,
    /// The page before: the column left of the first one shown ends it.
    PageLeft,
    /// The page after: the first column not shown whole starts it, or the last page
    /// starts it when that is nearer, so the last page is never short.
    PageRight,
    /// The last page: the last column ends it, with as many before it as fit.
    Last,
    /// Show this column: left where it is when already whole on screen, else first,
    /// or on the last page when it is on that page.
    Reveal(usize),
}

/// How many columns from `start` are drawn whole, the way the table lays them out:
/// left to right, each whole while it fits in what is left.
pub fn whole_from(
    start: usize,
    count: usize,
    room: Room,
    width: &mut impl FnMut(usize) -> Option<u16>,
) -> Option<usize> {
    let mut used = room.lead;
    let mut whole = 0;
    for i in start..count {
        let w = width(i)?;
        if used.saturating_add(w) > room.width {
            break;
        }
        used = used.saturating_add(w).saturating_add(room.padding);
        whole += 1;
    }
    Some(whole)
}

/// The first column of the page that ends with `end` drawn whole: as many columns
/// before it as fit, and `end` alone when nothing else does.
pub fn start_ending_at(
    end: usize,
    room: Room,
    width: &mut impl FnMut(usize) -> Option<u16>,
) -> Option<usize> {
    let mut used = room.lead.saturating_add(width(end)?);
    let mut start = end;
    while start > 0 {
        let next = used
            .saturating_add(room.padding)
            .saturating_add(width(start - 1)?);
        if next > room.width {
            break;
        }
        used = next;
        start -= 1;
    }
    Some(start)
}

/// Where the scrolling columns start after `mv`, from `current` among `count`. Every
/// move that can go somewhere does: a page moves at least one column, even past a
/// column wider than the room. `None` when a width it needs has not been drawn.
pub fn plan(
    mv: ColumnMove,
    current: usize,
    count: usize,
    room: Room,
    mut width: impl FnMut(usize) -> Option<u16>,
) -> Option<usize> {
    let Some(last) = count.checked_sub(1) else {
        return Some(0);
    };
    let current = current.min(last);
    match mv {
        ColumnMove::StepLeft => Some(current.saturating_sub(1)),
        ColumnMove::StepRight => Some((current + 1).min(last)),
        ColumnMove::First => Some(0),
        ColumnMove::PageLeft if current == 0 => Some(0),
        ColumnMove::PageLeft => start_ending_at(current - 1, room, &mut width),
        ColumnMove::PageRight if current == last => Some(current),
        ColumnMove::PageRight => {
            let whole = whole_from(current, count, room, &mut width)?;
            if current + whole >= count {
                return Some(current);
            }
            let next = current + whole.max(1);
            let last_page = start_ending_at(last, room, &mut width)?;
            Some(next.min(last_page).max(current + 1))
        }
        ColumnMove::Last => start_ending_at(last, room, &mut width),
        ColumnMove::Reveal(column) => {
            let column = column.min(last);
            if column >= current && column - current < whole_from(current, count, room, &mut width)?
            {
                return Some(current);
            }
            Some(column.min(start_ending_at(last, room, &mut width)?))
        }
    }
}

#[cfg(test)]
mod tests {
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
            total: 300,
        };
        assert_eq!(on.label(false), "cols 41-47 of 300");
        assert_eq!(on.label(true), "cols 41-47/300");
        let one = OnScreen { last: 41, ..on };
        assert_eq!(one.label(false), "cols 41 of 300");
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
}

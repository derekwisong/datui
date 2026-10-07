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
/// columns on screen; the frozen ones are always on screen before them. `cursor` is
/// the column cursor's column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OnScreen {
    pub first: usize,
    pub last: usize,
    pub cursor: usize,
    pub total: usize,
}

impl OnScreen {
    /// `col 43 of 300`, or `col 43/300` where the bar is short of room.
    pub fn label(&self, compact: bool) -> String {
        if compact {
            format!("col {}/{}", self.cursor, self.total)
        } else {
            format!("col {} of {}", self.cursor, self.total)
        }
    }
}

/// A sideways move. The pages need the columns' widths to land; the rest do not, and
/// are moves here so that one typed behind a page waiting on a draw lands after it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnMove {
    /// One column left: tests step the view; the app moves the cursor instead.
    #[cfg(test)]
    StepLeft,
    /// One column right.
    #[cfg(test)]
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
    /// Keep this column whole on screen, scrolling as little as it takes: first when
    /// it is left of the screen, last when it is right of it.
    Keep(usize),
}

/// A move of the column cursor. The view follows only as far as the cursor needs:
/// it scrolls when the cursor would leave the screen, and pages move both.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CursorMove {
    /// The shown column before the cursor's, frozen ones included.
    Left,
    /// The shown column after the cursor's.
    Right,
    /// The first shown column, and the view back to the start.
    First,
    /// The last shown column, on the last page.
    Last,
    /// A page left, the cursor on its first column.
    PageLeft,
    /// A page right, the cursor on its first column.
    PageRight,
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
        #[cfg(test)]
        ColumnMove::StepLeft => Some(current.saturating_sub(1)),
        #[cfg(test)]
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
        ColumnMove::Keep(column) => {
            let column = column.min(last);
            if column <= current {
                return Some(column);
            }
            // Only the columns up to this one are measured: the rest cannot change it.
            if column - current < whole_from(current, column + 1, room, &mut width)? {
                return Some(current);
            }
            start_ending_at(column, room, &mut width)
        }
    }
}

#[cfg(test)]
mod tests;

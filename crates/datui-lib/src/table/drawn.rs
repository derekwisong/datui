//! Where the last frame drew the table, so a click can be told what it landed on.

use ratatui::layout::Rect;

use super::DataTableState;

/// The table as the last frame drew it: what a click on it lands on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DrawnTable {
    /// The whole table, rail and header included.
    pub(crate) area: Rect,
    pub(crate) header: u16,
    /// The first row drawn and how many under the header.
    pub(crate) start_row: usize,
    pub(crate) rows: usize,
    pub(crate) columns: DrawnColumns,
}

/// Each column drawn: its cells across, `[from, to)`, and its name.
pub type DrawnColumns = Vec<(u16, u16, String)>;

/// What a click on the table lands on: the row on screen, counted from the top (none
/// on the header), and the column (none on the rail or the row numbers).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CellHit {
    pub row: Option<usize>,
    pub column: Option<String>,
}

impl DataTableState {
    /// Forget where the table was drawn: a frame that does not draw it leaves nothing
    /// there to click.
    pub fn forget_drawn(&mut self) {
        self.drawn = None;
    }

    /// What the cell at `(x, y)` showed in the last frame. `None` off the table, or
    /// below its last row.
    pub fn drawn_cell(&self, x: u16, y: u16) -> Option<CellHit> {
        let drawn = self.drawn.as_ref()?;
        if !drawn.area.contains(ratatui::layout::Position { x, y }) {
            return None;
        }
        let below_header = usize::from(y - drawn.area.y).checked_sub(usize::from(drawn.header));
        let row = match below_header {
            Some(row) if row >= drawn.rows => return None,
            row => row,
        };
        let column = drawn
            .columns
            .iter()
            .find(|(from, to, _)| (*from..*to).contains(&x))
            .map(|(_, _, name)| name.clone());
        Some(CellHit { row, column })
    }

    /// The column whose right edge the header cell at `(x, y)` is: the first cell
    /// of the gap after a column's last, where a drag resizes it.
    pub fn drawn_edge(&self, x: u16, y: u16) -> Option<String> {
        let drawn = self.drawn.as_ref()?;
        let header = drawn.area.y..drawn.area.y + drawn.header;
        if !header.contains(&y) || !drawn.area.contains(ratatui::layout::Position { x, y }) {
            return None;
        }
        if let Some(name) = Self::right_edge(drawn, x) {
            return Some(name);
        }
        if drawn
            .columns
            .iter()
            .any(|(from, to, _)| (*from..*to).contains(&x))
        {
            return None;
        }
        drawn
            .columns
            .iter()
            .find(|(_, to, _)| *to == x)
            .map(|(_, _, name)| name.clone())
    }

    /// The edge of a column that reaches the table's right side, filled or cut
    /// there: no gap follows it, so its last header cell is its edge.
    fn right_edge(drawn: &DrawnTable, x: u16) -> Option<String> {
        let (_, to, name) = drawn.columns.iter().max_by_key(|(_, to, _)| *to)?;
        (*to >= drawn.area.right() && x + 1 == *to).then(|| name.clone())
    }

    /// The column drawn across `x`, whatever the row: where a header dragged sideways
    /// is over.
    pub fn drawn_column_across(&self, x: u16) -> Option<String> {
        let drawn = self.drawn.as_ref()?;
        drawn
            .columns
            .iter()
            .find(|(from, to, _)| (*from..*to).contains(&x))
            .map(|(_, _, name)| name.clone())
    }

    /// The header rows as drawn and each column's cells across, `[from, to)`: where a
    /// header drag draws its drop mark.
    pub fn drawn_header(&self) -> Option<(Rect, DrawnColumns)> {
        let drawn = self.drawn.as_ref()?;
        Some((
            Rect {
                height: drawn.header.min(drawn.area.height),
                ..drawn.area
            },
            drawn.columns.clone(),
        ))
    }

    /// Put the cursor on what a click landed on: the row, when the rows drawn are
    /// still the view's, and the column. Returns whether it is there now, row and
    /// column both.
    pub fn point_at(&mut self, hit: &CellHit) -> bool {
        let mut landed = true;
        if let Some(row) = hit.row {
            if let Some(drawn) = self.drawn.as_ref()
                && drawn.start_row == self.start_row
                && row < drawn.rows
            {
                self.table_state.select(Some(row));
            } else {
                landed = false;
            }
        }
        if let Some(name) = &hit.column {
            self.set_current_column(name);
            landed &= self.current_column() == Some(name.as_str());
        }
        landed
    }
}

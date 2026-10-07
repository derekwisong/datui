//! State of the Value Counts screen: which column, the counts read for the view,
//! the order, the cursor, and the count in flight.

use crate::analysis::sampling::ReadWatch;
use crate::analysis::value_counts::{LineKind, Order, ValueCounts};
use std::collections::HashMap;
use std::sync::Arc;

/// A count being read.
#[derive(Debug, Clone)]
pub struct Computing {
    pub column: String,
    /// Every row, asked for after a sample.
    pub exact: bool,
    /// How the read is told to stop, and what it has read.
    pub watch: ReadWatch,
    /// Where each of the dataset's files starts, with the total last, when the read
    /// is of the files in order: how many of them the rows read have reached.
    pub file_starts: Option<std::sync::Arc<Vec<usize>>>,
}

impl Computing {
    /// The files the rows read so far have reached, and the files there are.
    pub fn files_reached(&self) -> Option<(usize, usize)> {
        let starts = self.file_starts.as_ref()?;
        let files = starts.len().checked_sub(1)?;
        let seen = self.watch.rows_seen()?;
        let reached = starts[..files].partition_point(|&start| start <= seen);
        Some((reached.min(files), files))
    }
}

#[derive(Debug, Default)]
pub struct ValueCountsModal {
    /// The view's columns, in its order, as the screen opened: what ←→ step through.
    pub columns: Vec<String>,
    /// The column on screen, an index into `columns`.
    pub at: usize,
    pub order: Order,
    /// The line the cursor is on, and the first line drawn.
    pub selected: usize,
    pub offset: usize,
    /// Lines the last frame drew: what PgUp and PgDn move.
    pub page: usize,
    /// Where the last frame's listing (or histogram) began, under the summary.
    pub body_top: u16,
    pub computing: Option<Computing>,
    /// Why the column on screen could not be counted.
    pub failed: Option<(String, String)>,
    /// The frame (`len_generation`) the counts held are of.
    frame: Option<u64>,
    /// Counts read for the frame, by column: going back to a column reads nothing.
    held: HashMap<String, Arc<ValueCounts>>,
    /// Set by a drill from this screen: Esc out of that drill comes back here.
    pub drill_return: bool,
    /// The histogram or the listing, as chosen with `c`; `None` until then, which
    /// shows a number column's histogram and anything else's listing.
    pub view: Option<CountsView>,
}

/// What the screen shows of the counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CountsView {
    Listing,
    Histogram,
}

impl ValueCountsModal {
    /// Open on column `at` of `columns`, for the view `frame`. The counts held for
    /// another frame are of other rows, and go.
    pub fn open(&mut self, columns: Vec<String>, at: usize, frame: u64) {
        if self.frame != Some(frame) || self.columns != columns {
            self.held.clear();
        }
        self.frame = Some(frame);
        self.columns = columns;
        self.at = at.min(self.columns.len().saturating_sub(1));
        self.computing = None;
        self.failed = None;
        self.drill_return = false;
        self.view = None;
        self.reset_cursor();
    }

    /// The view came back as it was (Esc out of a drill), under a new frame number.
    pub fn rebase(&mut self, frame: u64) {
        self.frame = Some(frame);
    }

    pub fn column(&self) -> Option<&str> {
        self.columns.get(self.at).map(String::as_str)
    }

    /// The counts of the column on screen, once read.
    pub fn current(&self) -> Option<&Arc<ValueCounts>> {
        self.held.get(self.column()?)
    }

    pub fn hold(&mut self, counts: ValueCounts) {
        self.held.insert(counts.column.clone(), Arc::new(counts));
    }

    /// Whether the column on screen is being counted.
    pub fn counting(&self) -> bool {
        self.computing
            .as_ref()
            .is_some_and(|c| Some(c.column.as_str()) == self.column())
    }

    /// Step to the column `by` places over. Whether it moved.
    pub fn step(&mut self, by: isize) -> bool {
        let to = self.at.saturating_add_signed(by);
        if to == self.at || to >= self.columns.len() {
            return false;
        }
        self.at = to;
        self.failed = None;
        self.view = None;
        self.reset_cursor();
        true
    }

    /// Whether the counts on screen show as a histogram: a number column's do, until
    /// `c` turns to the listing.
    pub fn shows_histogram(&self) -> bool {
        let has = self.current().is_some_and(|c| c.histogram.is_some());
        has && self.view != Some(CountsView::Listing)
    }

    /// `c`: between the histogram and the listing, where the column has both.
    pub fn toggle_view(&mut self) {
        if self.current().is_some_and(|c| c.histogram.is_some()) {
            self.view = Some(if self.shows_histogram() {
                CountsView::Listing
            } else {
                CountsView::Histogram
            });
        }
    }

    pub fn toggle_order(&mut self) {
        self.order = self.order.toggled();
        self.reset_cursor();
    }

    fn reset_cursor(&mut self) {
        self.selected = 0;
        self.offset = 0;
    }

    fn lines(&self) -> usize {
        self.current()
            .map(|c| c.lines(self.order).len())
            .unwrap_or(0)
    }

    /// Move the cursor `by` lines, staying on the listing.
    pub fn move_by(&mut self, by: isize) {
        let last = self.lines().saturating_sub(1);
        self.selected = self.selected.saturating_add_signed(by).min(last);
    }

    pub fn move_to_end(&mut self) {
        self.selected = self.lines().saturating_sub(1);
    }

    pub fn move_to_start(&mut self) {
        self.selected = 0;
    }

    /// What the line under the cursor stands for.
    pub fn selected_kind(&self) -> Option<LineKind> {
        let counts = self.current()?;
        counts.lines(self.order).get(self.selected).map(|l| l.kind)
    }

    /// Keep the cursor among the `height` lines drawn.
    pub fn scroll_into_view(&mut self, height: usize) {
        let height = height.max(1);
        self.page = height;
        if self.selected < self.offset {
            self.offset = self.selected;
        } else if self.selected >= self.offset + height {
            self.offset = self.selected + 1 - height;
        }
        let last = self.lines().saturating_sub(height);
        self.offset = self.offset.min(last);
    }
}

#[cfg(test)]
mod tests;

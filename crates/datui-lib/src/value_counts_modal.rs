//! State of the Value Counts screen: which column, the counts read for the view,
//! the order, the cursor, and the count in flight.

use crate::sampling::ReadWatch;
use crate::value_counts::{LineKind, Order, ValueCounts};
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
mod tests {
    use super::*;
    use polars::prelude::*;

    fn counts(column: &str, values: &[i32]) -> ValueCounts {
        crate::value_counts::Plan {
            lf: DataFrame::new_infer_height(vec![Column::new(column.into(), values)])
                .unwrap()
                .lazy(),
            column: column.to_string(),
            read: crate::value_counts::Read::Exact,
            known_total: None,
            streaming: false,
        }
        .run(&ReadWatch::default())
        .unwrap()
    }

    #[test]
    fn counts_are_held_per_column_and_dropped_with_the_frame() {
        let mut modal = ValueCountsModal::default();
        let columns = vec!["a".to_string(), "b".to_string()];
        modal.open(columns.clone(), 0, 1);
        modal.hold(counts("a", &[1, 1, 2]));
        assert!(modal.current().is_some());
        assert!(modal.step(1));
        assert!(modal.current().is_none(), "b is not counted yet");
        assert!(!modal.step(1), "no column past the last");
        assert!(modal.step(-1));
        assert_eq!(modal.current().unwrap().summary.rows, 3);

        modal.open(columns.clone(), 0, 1);
        assert!(modal.current().is_some(), "the same view keeps its counts");
        modal.open(columns, 0, 2);
        assert!(modal.current().is_none(), "another view's counts go");
    }

    /// A number column opens as its histogram; `c` turns to the listing and back,
    /// and the next column starts from its own type again.
    #[test]
    fn numbers_open_as_a_histogram_and_c_toggles() {
        let mut modal = ValueCountsModal::default();
        modal.open(vec!["n".to_string(), "s".to_string()], 0, 1);
        modal.hold(counts("n", &[1, 1, 2, 3, 3, 3]));
        assert!(modal.shows_histogram());
        let histogram = modal.current().unwrap().histogram.clone().unwrap();
        assert_eq!(
            histogram.bins.iter().map(|b| b.count).collect::<Vec<_>>(),
            [2.0, 1.0, 3.0],
            "a bin per value of a short integer range"
        );
        modal.toggle_view();
        assert!(!modal.shows_histogram());
        modal.toggle_view();
        assert!(modal.shows_histogram());
        modal.toggle_view();
        assert!(modal.step(1));
        let text = crate::value_counts::Plan {
            lf: DataFrame::new_infer_height(vec![Column::new("s".into(), ["a", "b"])])
                .unwrap()
                .lazy(),
            column: "s".to_string(),
            read: crate::value_counts::Read::Exact,
            known_total: None,
            streaming: false,
        }
        .run(&ReadWatch::default())
        .unwrap();
        modal.hold(text);
        assert!(!modal.shows_histogram(), "text has no histogram");
        modal.toggle_view();
        assert!(!modal.shows_histogram());
        assert!(modal.step(-1));
        assert!(modal.shows_histogram(), "back to the number's default");
    }

    #[test]
    fn the_cursor_stays_on_the_listing_and_in_view() {
        let mut modal = ValueCountsModal::default();
        modal.open(vec!["a".to_string()], 0, 1);
        modal.hold(counts("a", &[1, 2, 3, 4, 5, 6]));
        modal.move_by(10);
        assert_eq!(modal.selected, 5);
        modal.scroll_into_view(3);
        assert_eq!(modal.offset, 3);
        modal.move_by(-10);
        modal.scroll_into_view(3);
        assert_eq!((modal.selected, modal.offset), (0, 0));
        modal.toggle_order();
        assert_eq!(modal.order, Order::Value);
    }
}

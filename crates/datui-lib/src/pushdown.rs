//! Sources that read a window of rows themselves, and sources that filter and sort.
//!
//! Polars hands an anonymous scan no row offset, so a window deep in a view of one would
//! read every row before it. A [`Windowed`] source starts the window where it is asked
//! to. A [`Pushdown`] source goes further: it runs the sidebar's filters and sort itself
//! (a SQLite table, whose indexes do the work) and gives the view as a frame, a window
//! of it at a time, and a count.

use std::sync::Arc;

use polars::prelude::{LazyFrame, PolarsResult};

use crate::filter_modal::FilterStatement;

/// A source that reads rows `[start, start + len)` of a view without the ones before.
///
/// `Any`, so a reader whose source the app asks more of than rows can find its own
/// again: an audio file's, whose signal a quality run checks.
pub trait Windowed: Send + Sync + std::any::Any {
    /// The rows as a frame, read when it is collected.
    fn window(&self, start: usize, len: usize) -> PolarsResult<LazyFrame>;
}

/// How many rows a view holds, counted by its source. Blocks.
pub type Counter = Arc<dyn Fn() -> PolarsResult<usize> + Send + Sync>;

/// A view the source runs itself.
#[derive(Clone)]
pub struct PushedView {
    /// Every row of the view, in its order.
    pub lf: LazyFrame,
    /// The view's rows a window at a time.
    pub window: Arc<dyn Windowed>,
    /// Its row count.
    pub counter: Counter,
}

/// A source that filters and sorts. What it notices while reading is said through
/// [`Pushdown::notes`], which may grow once a pass over the whole source is done.
pub trait Pushdown: Send + Sync {
    /// The view with `filters` and the sort `sort` (column, descending; nulls last and
    /// ties in the source's order), or the source's order backward when `reversed` and
    /// there is no sort. `None` when the source cannot run it, and Polars does.
    fn view(
        &self,
        filters: &[FilterStatement],
        sort: &[(String, bool)],
        reversed: bool,
    ) -> Option<PushedView>;

    /// What reading the source has found to say.
    fn notes(&self) -> Vec<crate::notes::Note>;

    /// The source itself, for its tests.
    fn as_any(&self) -> &dyn std::any::Any;
}

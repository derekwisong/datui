//! Sources that read a window of rows themselves, and sources that filter and sort.
//!
//! Polars hands an anonymous scan no row offset, so a window deep in a view of one would
//! read every row before it. A [`Windowed`] source starts the window where it is asked
//! to. A [`Pushdown`] source goes further: it runs the sidebar's filters and sort itself
//! (a SQLite table, whose indexes do the work) and gives the view as a frame, a window
//! of it at a time, and a count.

use std::sync::Arc;

use polars::prelude::{Expr, LazyFrame, Operator, PolarsResult};

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
    #[cfg(test)]
    fn as_any(&self) -> &dyn std::any::Any;
}

/// What of a predicate Polars pushed into an anonymous scan the scan can evaluate.
///
/// A sort with a limit (a top-k) pushes a dynamic predicate, its running bound, into
/// the scan as an `Expr::Display`, which panics when it is turned back into a plan
/// (pola-rs/polars#28629; #28643 strips it for Python IO plugins only). The bound is
/// a hint the top-k applies itself, so the terms holding it are dropped.
pub(crate) fn evaluable(predicate: Option<Expr>) -> Option<Expr> {
    fn terms(e: Expr, out: &mut Vec<Expr>) {
        match e {
            Expr::BinaryExpr {
                left,
                op: Operator::And | Operator::LogicalAnd,
                right,
            } => {
                terms(Arc::unwrap_or_clone(left), out);
                terms(Arc::unwrap_or_clone(right), out);
            }
            e => out.push(e),
        }
    }
    let mut all = Vec::new();
    terms(predicate?, &mut all);
    all.into_iter()
        .filter(|term| !term.into_iter().any(|e| matches!(e, Expr::Display { .. })))
        .reduce(|left, right| left.and(right))
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::prelude::{col, lit};

    fn bound() -> Expr {
        Expr::Display {
            inputs: vec![col("id")],
            fmt_str: Box::new("dynamic_pred: 1".into()),
        }
    }

    #[test]
    fn a_top_k_bound_is_dropped_from_a_pushed_predicate() {
        let kept = col("id").lt(lit(4));
        assert_eq!(
            evaluable(Some(kept.clone().and(bound()))),
            Some(kept.clone())
        );
        assert_eq!(
            evaluable(Some(bound().and(kept.clone()))),
            Some(kept.clone())
        );
        assert_eq!(evaluable(Some(bound())), None);
        assert_eq!(evaluable(Some(kept.clone())), Some(kept));
        assert_eq!(evaluable(None), None);
    }
}

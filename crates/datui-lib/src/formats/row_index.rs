//! Lazy frames decoded over a row index, for readers whose rows sit at places worked
//! out from the row number: audio frames, fixed records, and the like.
//!
//! The plan has no scan in it. It is a row index over a frame with no columns, only a
//! height, and one elementwise expression per column that decodes that column's values
//! for the rows the index names, from an `Arc` of the source. So a slice anywhere
//! decodes only its own rows, a query that names one column decodes only that column,
//! and the streaming engine decodes a morsel at a time: filters, sorts and group-bys
//! stream. The in-memory engine still builds the index whole up to a slice's end, 4
//! bytes a row, which is why an untouched view reads its window straight from the
//! source through [`crate::formats::pushdown::Windowed`].
//!
//! A Polars `AnonymousScan` gives none of this: Polars hands it no row offset, and the
//! streaming engine of Polars 0.55 cannot run one.

use polars::prelude::*;
use std::sync::Arc;

/// The row index the plan decodes from; the select over it leaves it out.
pub const INDEX: &str = "__datui_row";

/// The most rows a source can show: Polars counts rows in 32 bits.
pub const MAX_ROWS: usize = IdxSize::MAX as usize;

/// A table whose values are decoded from their row numbers.
pub trait RowSource: Send + Sync + 'static {
    /// Rows on hand, at most [`MAX_ROWS`].
    fn height(&self) -> usize;

    fn schema(&self) -> SchemaRef;

    /// The schema's `column`th column for the rows `index` names, in that order. The
    /// index comes from the plan's row index, so it has no nulls and every row is
    /// under [`Self::height`]; a source still refuses one that is not, rather than
    /// read past its bytes.
    fn decode(&self, column: usize, index: &IdxCa) -> PolarsResult<Column>;
}

/// `source` as a lazy frame that Polars can stream, slice and prune. See the module.
pub fn lazy<S: RowSource>(source: &Arc<S>) -> LazyFrame {
    lazy_with_height(source, source.height())
}

/// [`lazy`] with `height` rows, for a source that grows: the frame of no columns at
/// its root is replaced with a taller one as rows arrive (`crate::formats::lines::bound`).
pub fn lazy_with_height<S: RowSource>(source: &Arc<S>, height: usize) -> LazyFrame {
    frame(source, height, false)
}

/// [`lazy_with_height`], each row carrying its place in the source in [`INDEX`]: the
/// number `#` shows, kept through a sort or a filter. Hidden from the view as the
/// dataset's row index always is.
pub fn lazy_numbered<S: RowSource>(source: &Arc<S>, height: usize) -> LazyFrame {
    frame(source, height, true)
}

fn frame<S: RowSource>(source: &Arc<S>, height: usize, numbered: bool) -> LazyFrame {
    let height = DataFrame::empty_with_height(height.min(MAX_ROWS)).lazy();
    frame_over(source, height, numbered)
}

/// [`lazy_numbered`] over `height`, a frame of no columns whose height is the rows:
/// for a source whose height is known only when the frame runs.
pub fn lazy_numbered_over<S: RowSource>(source: &Arc<S>, height: LazyFrame) -> LazyFrame {
    frame_over(source, height, true)
}

fn frame_over<S: RowSource>(source: &Arc<S>, height: LazyFrame, numbered: bool) -> LazyFrame {
    let base = height.with_row_index(INDEX, None);
    let mut exprs: Vec<Expr> = source
        .schema()
        .iter()
        .enumerate()
        .map(|(column, (name, dtype))| {
            let source = Arc::clone(source);
            let field = Field::new(name.clone(), dtype.clone());
            col(INDEX)
                .map(
                    move |c: Column| source.decode(column, c.as_materialized_series().idx()?),
                    move |_, _| Ok(field.clone()),
                )
                .alias(name.clone())
        })
        .collect();
    if numbered {
        exprs.push(col(INDEX));
    }
    base.select(exprs)
}

/// The rows `index` names as row numbers, or an error for a null or for a row at or
/// past `rows`.
pub fn checked(index: &IdxCa, rows: usize) -> PolarsResult<std::borrow::Cow<'_, [IdxSize]>> {
    polars_ensure!(
        index.null_count() == 0,
        ComputeError: "a row index has a missing row"
    );
    if let Some(max) = index.max() {
        polars_ensure!(
            (max as usize) < rows,
            OutOfBounds: "row {max} is past the {rows} rows on hand"
        );
    }
    Ok(match index.cont_slice() {
        Ok(rows) => std::borrow::Cow::Borrowed(rows),
        Err(_) => std::borrow::Cow::Owned(index.into_no_null_iter().collect()),
    })
}

#[cfg(test)]
mod tests;

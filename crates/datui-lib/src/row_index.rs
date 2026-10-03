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
//! source through [`crate::pushdown::Windowed`].
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
    let base = DataFrame::empty_with_height(source.height().min(MAX_ROWS))
        .lazy()
        .with_row_index(INDEX, None);
    let exprs: Vec<Expr> = source
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
mod tests {
    use super::*;

    /// Row `i` of column `c` is `i * 10 + c`, and the source counts what it decodes.
    struct Counting {
        rows: usize,
        decoded: std::sync::atomic::AtomicUsize,
    }

    impl RowSource for Counting {
        fn height(&self) -> usize {
            self.rows
        }

        fn schema(&self) -> SchemaRef {
            Arc::new(Schema::from_iter([
                Field::new("a".into(), DataType::Int64),
                Field::new("b".into(), DataType::Int64),
            ]))
        }

        fn decode(&self, column: usize, index: &IdxCa) -> PolarsResult<Column> {
            let rows = checked(index, self.rows)?;
            self.decoded
                .fetch_add(rows.len(), std::sync::atomic::Ordering::Relaxed);
            Ok(Int64Chunked::from_iter_values(
                PlSmallStr::EMPTY,
                rows.iter().map(|&i| i as i64 * 10 + column as i64),
            )
            .into_column())
        }
    }

    fn counting(rows: usize) -> Arc<Counting> {
        Arc::new(Counting {
            rows,
            decoded: Default::default(),
        })
    }

    #[test]
    fn a_slice_decodes_only_its_rows_and_columns() {
        let source = counting(1_000_000);
        let df = lazy(&source)
            .select([col("b")])
            .slice(999_998, 10)
            .collect()
            .unwrap();
        assert_eq!(df.get_column_names(), ["b"]);
        assert_eq!(
            df.column("b").unwrap().i64().unwrap().to_vec(),
            [Some(9_999_981), Some(9_999_991)]
        );
        assert_eq!(source.decoded.load(std::sync::atomic::Ordering::Relaxed), 2);
    }

    #[test]
    fn filters_sorts_and_group_bys_run_on_the_streaming_engine() {
        let source = counting(100_000);
        let lf = lazy(&source);
        let streaming = |lf: LazyFrame| crate::statistics::collect_lazy(lf, true).unwrap();
        let n = streaming(
            lf.clone()
                .filter(col("a").gt(lit(500_000i64)))
                .select([len()]),
        );
        assert_eq!(
            n.column("len").unwrap().get(0).unwrap(),
            AnyValue::UInt32(49_999)
        );
        let top = streaming(
            lf.clone()
                .sort(
                    ["a"],
                    SortMultipleOptions::default().with_order_descending(true),
                )
                .limit(2),
        );
        assert_eq!(
            top.column("b").unwrap().i64().unwrap().to_vec(),
            [Some(999_991), Some(999_981)]
        );
        let groups = streaming(
            lf.group_by([(col("a") % lit(20i64)).alias("k")])
                .agg([len()])
                .sort(["k"], Default::default()),
        );
        assert_eq!(groups.height(), 2);
        assert_eq!(
            groups.column("len").unwrap().u32().unwrap().to_vec(),
            [Some(50_000), Some(50_000)]
        );
    }

    #[test]
    fn a_row_past_the_end_or_missing_is_refused() {
        let index = IdxCa::from_slice("i".into(), &[0, 5]);
        assert!(checked(&index, 6).is_ok());
        assert!(checked(&index, 5).is_err());
        let missing = IdxCa::from_slice_options("i".into(), &[Some(0), None]);
        assert!(checked(&missing, 6).is_err());
    }
}

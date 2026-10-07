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
    let streaming = |lf: LazyFrame| crate::analysis::statistics::collect_lazy(lf, true).unwrap();
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

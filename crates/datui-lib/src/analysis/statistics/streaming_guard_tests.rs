use super::*;

fn frame() -> LazyFrame {
    df!("i" => (0..1_000i64).collect::<Vec<_>>(), "j" => (0..1_000i64).map(|v| v % 7).collect::<Vec<_>>())
            .unwrap()
            .lazy()
            .with_columns([
                col("i").cast(DataType::Decimal(38, 2)).alias("d"),
                col("i").cast(DataType::Int128).alias("w"),
            ])
}

struct Anonymous;

impl AnonymousScan for Anonymous {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self, _: Option<usize>) -> PolarsResult<SchemaRef> {
        Ok(Arc::new(Schema::from_iter([Field::new(
            "a".into(),
            DataType::Int64,
        )])))
    }

    fn scan(&self, _: AnonymousScanArgs) -> PolarsResult<DataFrame> {
        df!("a" => [1i64, 2, 3])
    }
}

/// An anonymous scan (a SQLite table) has no streaming implementation in Polars
/// 0.55, so a query over one runs on the in-memory engine whatever is asked.
#[test]
fn an_anonymous_scan_stays_off_the_streaming_engine() {
    let lf = LazyFrame::anonymous_scan(Arc::new(Anonymous), Default::default())
        .unwrap()
        .filter(col("a").gt(lit(1i64)));
    assert!(!may_stream(&lf, true));
    assert!(may_stream(&frame(), true));
    assert_eq!(collect_lazy(lf, true).unwrap().height(), 2);
}

/// The shapes Polars 0.55's streaming top-k panics on go to the in-memory engine
/// and read; the ones it runs stay on the streaming engine.
#[test]
fn only_a_top_k_by_one_wide_key_leaves_the_streaming_engine() {
    let lf = frame();
    let desc = SortMultipleOptions::default().with_order_descending(true);
    for key in ["d", "w"] {
        let top = [
            lf.clone().sort([key], desc.clone()).slice(0, 3),
            lf.clone()
                .filter(col("j").eq(lit(1)))
                .sort([key], Default::default())
                .limit(3),
            lf.clone()
                .group_by([col(key)])
                .agg([len()])
                .sort([key], desc.clone())
                .slice(0, 3),
        ];
        for query in top {
            assert!(sorts_by_one_wide_key(&query), "{key}");
            assert_eq!(collect_lazy(query, true).unwrap().height(), 3);
        }
        let streams = [
            lf.clone().sort([key], desc.clone()),
            lf.clone().sort([key], desc.clone()).slice(10, 3),
            lf.clone().sort([key, "j"], desc.clone()).slice(0, 3),
            lf.clone()
                .sort([key], desc.clone().with_maintain_order(true))
                .slice(0, 3),
            lf.clone().sort(["i"], desc.clone()).slice(0, 3),
        ];
        for query in streams {
            assert!(!sorts_by_one_wide_key(&query), "{key}");
            query
                .collect_with_engine(Engine::Streaming)
                .unwrap()
                .unwrap_single();
        }
    }
}

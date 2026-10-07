use super::*;

/// `n` rows of `id` and a `score` that climbs with it, as a Parquet file: a head
/// sample of it is biased, and a spread one is not.
fn climbing(dir: &std::path::Path, n: i64) -> LazyFrame {
    let ids: Vec<i64> = (0..n).collect();
    let scores: Vec<f64> = ids.iter().map(|i| *i as f64).collect();
    let mut df = df!("id" => ids, "score" => scores).unwrap();
    let path = dir.join("climbing.parquet");
    ParquetWriter::new(std::fs::File::create(&path).unwrap())
        .with_row_group_size(Some(1_000))
        .finish(&mut df)
        .unwrap();
    LazyFrame::scan_parquet(PlRefPath::try_from_path(&path).unwrap(), Default::default()).unwrap()
}

fn mean(df: &DataFrame) -> f64 {
    df.column("score")
        .unwrap()
        .as_materialized_series()
        .mean()
        .unwrap()
}

#[test]
fn a_small_table_is_read_whole() {
    let dir = tempfile::tempdir().unwrap();
    let rows = analysis_rows(&climbing(dir.path(), 500), Some(1_000), None, 1, false).unwrap();
    assert_eq!(rows.df.height(), 500);
    assert_eq!(rows.total_rows, 500);
    assert_eq!(rows.sample_size, None);
}

#[test]
fn a_parquet_scan_is_sampled_in_blocks_across_all_of_it() {
    let dir = tempfile::tempdir().unwrap();
    let lf = climbing(dir.path(), 100_000);
    assert!(slices_reach_into_the_scan(&lf), "a Parquet scan seeks");
    let rows = analysis_rows(&lf, Some(5_000), None, 7, false).unwrap();
    assert_eq!(rows.df.height(), 5_000);
    assert_eq!(rows.total_rows, 100_000);
    assert_eq!(rows.sample_size, Some(5_000));
    // The table's mean is 49,999.5; a head sample's would be 2,499.5.
    assert!(
        (mean(&rows.df) - 49_999.5).abs() < 2_500.0,
        "{}",
        mean(&rows.df)
    );
    // In table order, and reaching its last stretch.
    let ids = rows.df.column("id").unwrap().i64().unwrap();
    assert!(ids.into_no_null_iter().is_sorted());
    assert!(ids.max().unwrap() > 95_000);
    // The same seed, the same sample; another seed, another.
    let again = analysis_rows(&lf, Some(5_000), None, 7, false).unwrap();
    assert!(rows.df.equals(&again.df));
    let other = analysis_rows(&lf, Some(5_000), None, 8, false).unwrap();
    assert!(!rows.df.equals(&other.df));
}

/// Seeded runs, and a table read whole because it is under twice the sample, say
/// where each kept row sat: the row's id, in a table whose id is its position.
/// The runs see too few rows to count; the whole read counts what it read.
#[test]
fn a_block_sample_says_where_its_rows_sat() {
    let tens = (col("id") / lit(10_000i64)).alias("tens");
    for (rows, n) in [(100_000, 5_000), (10_000, 6_000)] {
        let dir = tempfile::tempdir().unwrap();
        let lf = climbing(dir.path(), rows);
        let read = sample_rows_counting(&lf, Some(n), None, 7, false, None, Some(&tens)).unwrap();
        let ids: Vec<IdxSize> = read
            .rows
            .df
            .column("id")
            .unwrap()
            .i64()
            .unwrap()
            .into_no_null_iter()
            .map(|id| id as IdxSize)
            .collect();
        assert_eq!(ids.len(), n);
        assert_eq!(ids, read.positions, "{rows} rows, n={n}");
        if rows < 2 * n as i64 {
            assert_eq!(
                read.counted,
                Some(crate::analysis::sampling::Counted::Totals(
                    [(Some("0".to_string()), 10_000)].into()
                ))
            );
        } else {
            assert_eq!(read.counted, None, "runs see too few rows to count");
        }
    }
}

/// Odd and small sizes: exactly `n` distinct rows, reaching the end of the table.
/// Runs rounded up and then cut to `n` once dropped the last blocks; runs wider
/// than their stretch overlapped.
#[test]
fn a_block_sample_of_any_size_is_n_distinct_rows_across_the_table() {
    let dir = tempfile::tempdir().unwrap();
    let lf = climbing(dir.path(), 10_000);
    for (n, seed) in [
        (60, 1),
        (101, 2),
        (1_234, 3),
        (4_999, 4),
        (5_001, 5),
        (9_999, 6),
    ] {
        let rows = analysis_rows(&lf, Some(n), None, seed, false).unwrap();
        let ids: Vec<i64> = rows
            .df
            .column("id")
            .unwrap()
            .i64()
            .unwrap()
            .into_no_null_iter()
            .collect();
        assert_eq!(ids.len(), n, "n={n}");
        let unique: std::collections::HashSet<_> = ids.iter().collect();
        assert_eq!(unique.len(), n, "no duplicates at n={n}");
        assert!(ids.is_sorted(), "table order at n={n}");
        assert!(
            *ids.last().unwrap() > 9_000,
            "reaches the end at n={n}: {ids:?}"
        );
    }
}

#[test]
fn only_a_single_file_scan_is_sampled_in_blocks() {
    let dir = tempfile::tempdir().unwrap();
    let one = climbing(dir.path(), 1_000);
    // A column stubbed above the scan, as binary columns are: still seekable.
    let stubbed = one
        .clone()
        .select([col("id"), lit(NULL).cast(DataType::Binary).alias("blob")]);
    assert!(slices_reach_into_the_scan(&stubbed));
    // Two files: each slice would open the footers of those before it.
    let two = concat([one.clone(), one.clone()], UnionArgs::default()).unwrap();
    assert!(!slices_reach_into_the_scan(&two));
    let glob = LazyFrame::scan_parquet(
        PlRefPath::try_from_path(&dir.path().join("*.parquet")).unwrap(),
        Default::default(),
    )
    .unwrap();
    std::fs::copy(
        dir.path().join("climbing.parquet"),
        dir.path().join("again.parquet"),
    )
    .unwrap();
    assert!(!slices_reach_into_the_scan(&glob), "two files in one scan");
    assert!(!slices_reach_into_the_scan(
        &one.clone().sort(["id"], Default::default())
    ));
}

#[test]
fn a_filtered_view_is_sampled_in_one_uniform_pass() {
    let dir = tempfile::tempdir().unwrap();
    let lf = climbing(dir.path(), 100_000).filter(col("id").gt_eq(lit(50_000)));
    assert!(
        !slices_reach_into_the_scan(&lf),
        "a slice of a filter reads what is ahead of it"
    );
    let rows = analysis_rows(&lf, Some(5_000), None, 7, false).unwrap();
    assert_eq!(rows.df.height(), 5_000);
    assert_eq!(rows.total_rows, 50_000, "the pass counts as it goes");
    assert!(
        (mean(&rows.df) - 74_999.5).abs() < 1_500.0,
        "{}",
        mean(&rows.df)
    );
    assert!(
        rows.df.column(SAMPLE_POSITION).is_err(),
        "the position column does not leak"
    );
    let again = analysis_rows(&lf, Some(5_000), None, 7, false).unwrap();
    assert!(rows.df.equals(&again.df), "seeded");
}

#[test]
fn no_sample_size_reads_every_row() {
    let dir = tempfile::tempdir().unwrap();
    let rows = analysis_rows(&climbing(dir.path(), 20_000), None, None, 1, false).unwrap();
    assert_eq!(rows.df.height(), 20_000);
    assert_eq!(rows.sample_size, None);
}

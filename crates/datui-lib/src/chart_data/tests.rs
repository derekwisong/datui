use super::*;
use crate::widgets::axis_numbers::format_bar_value;

fn all_rows() -> ChartSampling {
    ChartSampling::rows(Some(10_000))
}

fn xy(lf: &LazyFrame, x: &str, ys: &[&str], sampling: &ChartSampling) -> ChartDataResult {
    let schema = lf.clone().collect_schema().unwrap();
    let ys: Vec<String> = ys.iter().map(|s| s.to_string()).collect();
    prepare_chart_data(lf, schema.as_ref(), x, &ys, sampling, false).unwrap()
}

/// A line over more rows than its sample size draws each step's lowest and
/// highest value: every peak survives, where a sample of a waveform misses them.
#[test]
fn a_long_line_is_drawn_as_its_envelope() {
    let n = 100_000usize;
    let x: Vec<i64> = (0..n as i64).collect();
    let y: Vec<f64> = (0..n)
        .map(|i| match i {
            // One spike, one row wide, that a sample would almost surely miss.
            54_321 => 9.0,
            _ => ((i as f64) / 50.0).sin(),
        })
        .collect();
    let lf = df!("x" => &x, "y" => &y).unwrap().lazy();
    let schema = lf.clone().collect_schema().unwrap();
    let sampling = ChartSampling::rows(Some(1_000));
    let result =
        prepare_chart_data(&lf, schema.as_ref(), "x", &["y".into()], &sampling, true).unwrap();
    assert_eq!(
        result.rows,
        RowsRead {
            total_rows: n,
            sample_size: None,
            envelope_steps: Some(500),
            seed: None,
        }
    );
    let points = &result.series[0];
    assert!(points.len() <= 1_000, "{} points", points.len());
    let top = points.iter().map(|p| p.1).fold(f64::MIN, f64::max);
    assert_eq!(top, 9.0, "the spike is kept");
    let bottom = points.iter().map(|p| p.1).fold(f64::MAX, f64::min);
    assert!(bottom < -0.99, "so is every trough: {bottom}");
    assert!(points.windows(2).all(|w| w[0].0 <= w[1].0), "in X order");
    assert_eq!(
        chart_notes(&result.rows, None, "·"),
        ["min and max of 100k rows in 500 steps"]
    );

    // Under the sample size, every row is drawn as it was.
    let sampling = ChartSampling::rows(Some(200_000));
    let result =
        prepare_chart_data(&lf, schema.as_ref(), "x", &["y".into()], &sampling, true).unwrap();
    assert_eq!(result.rows.envelope_steps, None);
    assert_eq!(result.series[0].len(), n);
}

/// An envelope reads the whole view twice: never over an object store in place,
/// where the sample reads a few row groups; and both passes stop when the chart
/// is no longer wanted.
#[test]
fn an_envelope_is_sampled_instead_where_full_reads_cost_and_stops_when_cancelled() {
    let n = 10_000i64;
    let lf = df!("x" => (0..n).collect::<Vec<_>>(), "y" => (0..n).collect::<Vec<_>>())
        .unwrap()
        .lazy();
    let schema = lf.clone().collect_schema().unwrap();
    let remote = ChartSampling {
        full_passes: false,
        ..ChartSampling::rows(Some(100))
    };
    let result =
        prepare_chart_data(&lf, schema.as_ref(), "x", &["y".into()], &remote, true).unwrap();
    assert_eq!(result.rows.envelope_steps, None);
    assert_eq!(result.rows.sample_size, Some(100));

    let cancelled = ChartSampling::rows(Some(100));
    cancelled.cancel.store(true, Ordering::Relaxed);
    let err = prepare_chart_data(&lf, schema.as_ref(), "x", &["y".into()], &cancelled, true)
        .err()
        .expect("a cancelled envelope is not drawn");
    assert_eq!(err.to_string(), "chart cancelled");
}

/// A temporal X is placed by its ordinal, as a sampled line places it.
#[test]
fn an_envelope_places_temporal_x_by_its_ordinal() {
    let days: Vec<i32> = (0..1_000).collect();
    let lf = df!("d" => &days, "y" => (0..1_000).map(f64::from).collect::<Vec<_>>())
        .unwrap()
        .lazy()
        .with_column(col("d").cast(DataType::Date));
    let schema = lf.clone().collect_schema().unwrap();
    let result = prepare_chart_data(
        &lf,
        schema.as_ref(),
        "d",
        &["y".into()],
        &ChartSampling::rows(Some(100)),
        true,
    )
    .unwrap();
    assert_eq!(result.rows.envelope_steps, Some(50));
    let points = &result.series[0];
    assert_eq!(points.first(), Some(&(0.0, 0.0)));
    assert_eq!(points.last().map(|p| p.1), Some(999.0));
    assert!(
        points.iter().all(|&(x, y)| y >= x && y < x + 20.0),
        "{points:?}"
    );
}

/// A step where a series has no value breaks its line, as a null does.
#[test]
fn an_envelope_breaks_where_a_series_has_no_values() {
    let x: Vec<i64> = (0..100).collect();
    let y: Vec<Option<f64>> = (0..100)
        .map(|i| (!(40..60).contains(&i)).then_some(i as f64))
        .collect();
    let lf = df!("x" => &x, "y" => &y).unwrap().lazy();
    let schema = lf.clone().collect_schema().unwrap();
    let sampling = ChartSampling::rows(Some(20));
    let result =
        prepare_chart_data(&lf, schema.as_ref(), "x", &["y".into()], &sampling, true).unwrap();
    assert_eq!(result.rows.envelope_steps, Some(10));
    assert_eq!(result.breaks[0].len(), 1, "one gap: {:?}", result.series[0]);
}

/// A row whose X is not a number is left out whole, Y and all.
#[test]
fn an_envelope_leaves_out_rows_with_no_x() {
    let x: Vec<f64> = (0..100)
        .map(|i| if i % 10 == 0 { f64::NAN } else { i as f64 })
        .collect();
    let y: Vec<f64> = (0..100).map(|i| i as f64).collect();
    let lf = df!("x" => &x, "y" => &y).unwrap().lazy();
    let schema = lf.clone().collect_schema().unwrap();
    let sampling = ChartSampling::rows(Some(20));
    let result =
        prepare_chart_data(&lf, schema.as_ref(), "x", &["y".into()], &sampling, true).unwrap();
    let ys: Vec<f64> = result.series[0].iter().map(|p| p.1).collect();
    assert!(!ys.contains(&0.0) && !ys.contains(&50.0), "{ys:?}");
    assert_eq!(ys.iter().cloned().fold(f64::MIN, f64::max), 99.0);
}

#[test]
fn prepare_empty_y_columns() {
    let lf = df!("x" => &[1.0_f64, 2.0], "y" => &[10.0, 20.0])
        .unwrap()
        .lazy();
    let result = xy(&lf, "x", &[], &all_rows());
    assert!(result.series.is_empty());
    assert_eq!(result.x_axis_kind, XAxisTemporalKind::Numeric);
}

#[test]
fn prepare_small_data() {
    let lf = df!(
        "x" => &[1.0_f64, 2.0, 3.0],
        "a" => &[10.0_f64, 20.0, 30.0],
        "b" => &[100.0_f64, 200.0, 300.0]
    )
    .unwrap()
    .lazy();
    let result = xy(&lf, "x", &["a", "b"], &all_rows());
    assert_eq!(result.series.len(), 2);
    assert_eq!(
        result.series[0],
        vec![(1.0, 10.0), (2.0, 20.0), (3.0, 30.0)]
    );
    assert_eq!(
        result.series[1],
        vec![(1.0, 100.0), (2.0, 200.0), (3.0, 300.0)]
    );
    assert_eq!(result.x_axis_kind, XAxisTemporalKind::Numeric);
    assert_eq!(
        result.rows,
        RowsRead {
            total_rows: 3,
            sample_size: None,
            envelope_steps: None,
            seed: None,
        },
        "every row read: nothing to say"
    );
    assert!(chart_notes(&result.rows, None, "·").is_empty());
}

#[test]
fn prepare_skips_nan() {
    let lf = df!(
        "x" => &[1.0_f64, 2.0, 3.0],
        "y" => &[10.0_f64, f64::NAN, 30.0]
    )
    .unwrap()
    .lazy();
    let result = xy(&lf, "x", &["y"], &all_rows());
    assert_eq!(result.series[0], vec![(1.0, 10.0), (3.0, 30.0)]);
}

/// Over the limit, a chart reads a sample spread across the table, not its head,
/// and says how many rows it read of how many.
#[test]
fn over_the_limit_a_chart_reads_a_spread_sample_and_says_so() {
    let n = 50_000_i64;
    let lf = df!(
        "x" => (0..n).collect::<Vec<_>>(),
        "y" => (0..n).map(|v| v * 2).collect::<Vec<_>>()
    )
    .unwrap()
    .lazy();
    let result = xy(&lf, "x", &["y"], &ChartSampling::rows(Some(1_000)));
    let points = &result.series[0];
    assert_eq!(points.len(), 1_000);
    let last_x = points.last().unwrap().0;
    assert!(
        last_x > (n as f64) * 0.9,
        "the sample reaches the end of the table, got {last_x}"
    );
    assert_eq!(
        result.rows,
        RowsRead {
            total_rows: n as usize,
            sample_size: Some(1_000),
            envelope_steps: None,
            seed: Some(crate::sampling::Sample::default().seed),
        }
    );
    // The seed draws the same sample again; the terminal joins it as ASCII.
    let seed = crate::sampling::Sample::default().seed;
    assert_eq!(
        chart_notes(&result.rows, None, "·"),
        [format!("sample of 1,000 of 50k rows · seed {seed}")]
    );
    assert_eq!(
        chart_notes(&result.rows, None, "-"),
        [format!("sample of 1,000 of 50k rows - seed {seed}")]
    );

    // No limit reads every row.
    let every = xy(&lf, "x", &["y"], &ChartSampling::rows(None));
    assert_eq!(every.series[0].len(), n as usize);
    assert_eq!(every.rows.sample_size, None);
}

/// One Parquet file is sampled in runs across it: the chart's columns stay a plan
/// the sampler can seek in, so the chart does not read the file to draw from it.
#[test]
fn a_parquet_file_is_sampled_in_runs() {
    let dir = tempfile::tempdir().unwrap();
    let n = 100_000_i64;
    let mut df = df!(
        "id" => (0..n).collect::<Vec<_>>(),
        "fare" => (0..n).map(|v| v as f64).collect::<Vec<_>>(),
        "other" => vec!["x"; n as usize]
    )
    .unwrap();
    let path = dir.path().join("trips.parquet");
    ParquetWriter::new(std::fs::File::create(&path).unwrap())
        .with_row_group_size(Some(1_000))
        .finish(&mut df)
        .unwrap();
    let lf = LazyFrame::scan_parquet(PlRefPath::try_from_path(&path).unwrap(), Default::default())
        .unwrap();
    assert!(crate::sampling::slices_reach_into_the_scan(
        &lf.clone().select([col("id"), col("fare")])
    ));
    let data = prepare_histogram_data(
        &lf,
        "fare",
        10,
        ValueRange::All,
        &ChartSampling::rows(Some(2_000)),
    )
    .unwrap();
    assert_eq!(
        data.rows,
        RowsRead {
            total_rows: n as usize,
            sample_size: Some(2_000),
            envelope_steps: None,
            seed: Some(crate::sampling::Sample::default().seed),
        }
    );
    assert!(data.x_max > 90_000.0, "reaches the end: {}", data.x_max);
}

/// The same seed and size draw the same rows; the chart and the analysis tools
/// share the sampler, so they agree on what a sample is.
#[test]
fn the_sample_is_seeded() {
    let lf = df!("x" => (0..20_000_i64).collect::<Vec<_>>(), "y" => vec![1.0_f64; 20_000])
        .unwrap()
        .lazy();
    let a = xy(&lf, "x", &["y"], &ChartSampling::rows(Some(500)));
    let b = xy(&lf, "x", &["y"], &ChartSampling::rows(Some(500)));
    assert_eq!(a.series, b.series);
    let other = ChartSampling {
        seed: 7,
        ..ChartSampling::rows(Some(500))
    };
    let c = xy(&lf, "x", &["y"], &other);
    assert_ne!(a.series, c.series);
}

/// Another option over columns already read draws from the rows held, without
/// reading the file again; a new column is read with the held ones, and another
/// sample size reads afresh.
#[test]
fn rows_already_read_are_not_read_again() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("fares.csv");
    let write = |a: i64, b: i64| {
        let rows: String = (0..100).map(|i| format!("{},{}\n", i + a, i + b)).collect();
        std::fs::write(&path, format!("a,b\n{rows}")).unwrap();
    };
    write(0, 0);
    let lf = LazyCsvReader::new(PlRefPath::try_from_path(&path).unwrap())
        .finish()
        .unwrap();
    let sampling = ChartSampling::rows(Some(10_000));
    let first = prepare_histogram_data(&lf, "a", 10, ValueRange::All, &sampling).unwrap();
    assert_eq!(first.x_min, 0.0);

    write(1_000, 1_000);
    let held = prepare_histogram_data(&lf, "a", 5, ValueRange::Percentile1To99, &sampling).unwrap();
    assert!(
        held.x_max < 100.0,
        "drawn from the rows held: {}",
        held.x_max
    );
    let boxed = prepare_box_plot_data(&lf, "a", ValueRange::All, &sampling).unwrap();
    assert_eq!(boxed.stats[0].max, 99.0);

    let with_b = prepare_histogram_data(&lf, "b", 10, ValueRange::All, &sampling).unwrap();
    assert_eq!(with_b.x_min, 1_000.0, "b was not held: read");
    let a_again = prepare_histogram_data(&lf, "a", 10, ValueRange::All, &sampling).unwrap();
    assert_eq!(a_again.x_min, 1_000.0, "read along with b");

    write(5_000, 5_000);
    let other_size = ChartSampling {
        limit: Some(50),
        ..sampling.clone()
    };
    let resampled = prepare_histogram_data(&lf, "a", 10, ValueRange::All, &other_size).unwrap();
    assert!(resampled.x_min >= 5_000.0, "another size reads afresh");
}

/// A line is drawn in X order whatever order the rows are in, as a pivot leaves
/// them.
#[test]
fn points_come_in_x_order() {
    let lf = df!(
        "year" => &[2001_i64, 1999, 2003, 2000, 2002],
        "count" => &[1.0_f64, 2.0, 3.0, 4.0, 5.0]
    )
    .unwrap()
    .lazy();
    let result = xy(&lf, "year", &["count"], &all_rows());
    let xs: Vec<f64> = result.series[0].iter().map(|p| p.0).collect();
    assert_eq!(xs, [1999.0, 2000.0, 2001.0, 2002.0, 2003.0]);
    assert_eq!(result.series[0][0], (1999.0, 2.0));
    assert!(result.breaks[0].is_empty());
}

/// Picking the X column as a Y series charts it against itself rather than failing
/// on a repeated column.
#[test]
fn x_as_a_y_series_charts_rather_than_failing() {
    let lf = df!("x" => &[1.0_f64, 2.0], "y" => &[3.0_f64, 4.0])
        .unwrap()
        .lazy();
    let result = xy(&lf, "x", &["x", "y"], &all_rows());
    assert_eq!(result.series[0], vec![(1.0, 1.0), (2.0, 2.0)]);
    assert_eq!(result.series[1], vec![(1.0, 3.0), (2.0, 4.0)]);
}

/// An X or Y column the frame does not have is an error to show, not an empty chart.
#[test]
fn a_missing_x_or_y_column_is_an_error() {
    let lf = df!("x" => &[1.0_f64], "y" => &[2.0_f64]).unwrap().lazy();
    let schema = lf.clone().collect_schema().unwrap();
    for (x, y) in [("missing", "y"), ("x", "gone")] {
        let result = prepare_chart_data(&lf, schema.as_ref(), x, &[y.into()], &all_rows(), false);
        assert!(result.is_err(), "{x} {y}");
    }
}

/// One series' nulls drop that series' points only; a null X drops the row; a
/// line breaks at a gap instead of bridging it.
#[test]
fn nulls_drop_per_series_and_break_the_line() {
    let lf = df!(
        "year" => &[Some(1880_i64), Some(1881), Some(1882), None, Some(1883), Some(1884)],
        "emma" => &[Some(10.0_f64), Some(11.0), Some(12.0), Some(99.0), Some(13.0), Some(14.0)],
        "jennifer" => &[None, None, Some(5.0_f64), Some(99.0), None, Some(7.0)]
    )
    .unwrap()
    .lazy();
    let result = xy(&lf, "year", &["emma", "jennifer"], &all_rows());
    assert_eq!(
        result.series[0],
        vec![
            (1880.0, 10.0),
            (1881.0, 11.0),
            (1882.0, 12.0),
            (1883.0, 13.0),
            (1884.0, 14.0)
        ],
        "Emma keeps the years Jennifer is missing; the null year is gone"
    );
    assert!(result.breaks[0].is_empty());
    assert_eq!(result.series[1], vec![(1882.0, 5.0), (1884.0, 7.0)]);
    assert_eq!(result.breaks[1], [1], "1883 is missing: the line breaks");
    assert_eq!(
        segments(&result.series[1], &result.breaks[1]),
        vec![&[(1882.0, 5.0)][..], &[(1884.0, 7.0)][..]]
    );
}

#[test]
fn segments_split_at_breaks() {
    let points = [(0.0, 0.0), (1.0, 1.0), (2.0, 2.0), (3.0, 3.0)];
    assert_eq!(segments(&points, &[]), vec![&points[..]]);
    assert_eq!(
        segments(&points, &[1, 3]),
        vec![&points[..1], &points[1..3], &points[3..]]
    );
    assert!(segments(&[], &[]).is_empty());
}

/// Temporal X charts as its ordinal, nulls dropped the same way.
#[test]
fn a_date_x_is_ordinal() {
    let lf = df!("d" => &[Some(1_i32), None, Some(0)], "y" => &[1.0_f64, 2.0, 3.0])
        .unwrap()
        .lazy()
        .with_column(col("d").cast(DataType::Date));
    let result = xy(&lf, "d", &["y"], &all_rows());
    assert_eq!(result.x_axis_kind, XAxisTemporalKind::Date);
    assert_eq!(result.series[0], vec![(0.0, 3.0), (1.0, 1.0)]);
}

fn with_outliers() -> LazyFrame {
    // 1..=100 and two far outliers.
    let mut v: Vec<f64> = (1..=100).map(f64::from).collect();
    v.push(-10_000.0);
    v.push(50_000.0);
    df!("fare" => v).unwrap().lazy()
}

/// The percentile range leaves the tails out of a histogram and counts them.
#[test]
fn a_histogram_range_clips_the_tails_and_counts_them() {
    let lf = with_outliers();
    let all = prepare_histogram_data(&lf, "fare", 10, ValueRange::All, &all_rows()).unwrap();
    assert_eq!(all.x_min, -10_000.0);
    assert!(all.clipped.is_none());

    let clipped =
        prepare_histogram_data(&lf, "fare", 10, ValueRange::Percentile1To99, &all_rows()).unwrap();
    // Of 102 values the 1st percentile falls at 1.01 and the 99th at 99.99: each
    // tail loses its outlier and the value next to it.
    assert_eq!((clipped.x_min, clipped.x_max), (2.0, 99.0));
    let outside = clipped.clipped.unwrap().outside;
    assert_eq!(outside, 4);
    let counted: f64 = clipped.bins.iter().map(|b| b.count).sum();
    assert_eq!(counted as usize + outside, 102);
    assert_eq!(
        chart_notes(&clipped.rows, clipped.clipped.as_ref(), "·"),
        ["4 values outside p1-p99"]
    );
}

#[test]
fn box_plot_and_kde_take_the_range_too() {
    let lf = with_outliers();
    let boxed =
        prepare_box_plot_data(&lf, "fare", ValueRange::Percentile1To99, &all_rows()).unwrap();
    assert!(boxed.stats[0].min > 0.0 && boxed.stats[0].max <= 100.0);
    assert!(boxed.clipped.unwrap().outside >= 2);

    let kde = prepare_kde_data(&lf, "fare", 1.0, ValueRange::Percentile1To99, &all_rows()).unwrap();
    assert!(kde.x_min > -1_000.0 && kde.x_max < 1_000.0);
    assert!(kde.clipped.unwrap().outside >= 2);

    let whole = prepare_box_plot_data(&lf, "fare", ValueRange::All, &all_rows()).unwrap();
    assert_eq!(whole.stats[0].min, -10_000.0);
}

/// A value column read on its own: another column's nulls do not remove its values.
#[test]
fn a_histogram_keeps_every_value_of_its_column() {
    let lf = df!("a" => &[Some(1.0_f64), Some(2.0), None, Some(4.0)])
        .unwrap()
        .lazy();
    let data = prepare_histogram_data(&lf, "a", 5, ValueRange::All, &all_rows()).unwrap();
    let counted: f64 = data.bins.iter().map(|b| b.count).sum();
    assert_eq!(counted, 3.0);
}

#[test]
fn prepare_x_range_numeric() {
    let lf = df!("x" => &[10.0_f64, 20.0, 5.0, 30.0]).unwrap().lazy();
    let schema = lf.clone().collect_schema().unwrap();
    let r = prepare_chart_x_range(&lf, schema.as_ref(), "x", &all_rows()).unwrap();
    assert_eq!(r.x_min, 5.0);
    assert_eq!(r.x_max, 30.0);
    assert_eq!(r.x_axis_kind, XAxisTemporalKind::Numeric);
}

#[test]
fn prepare_x_range_empty_returns_placeholder() {
    let lf = df!("x" => &[1.0_f64]).unwrap().lazy().slice(0, 0);
    let schema = lf.clone().collect_schema().unwrap();
    let r = prepare_chart_x_range(&lf, schema.as_ref(), "x", &all_rows()).unwrap();
    assert_eq!(r.x_min, 0.0);
    assert_eq!(r.x_max, 1.0);
}

fn bars(lf: &LazyFrame, order: BarOrder, cap: usize) -> BarData {
    prepare_bar_data(lf, "carrier", "delay", order, cap, &all_rows()).unwrap()
}

fn labels(data: &BarData) -> Vec<Option<&str>> {
    data.bars.iter().map(|b| b.label.as_deref()).collect()
}

/// Bars come largest first, or in the category's own order; past the cap the rest
/// are counted rather than kept.
#[test]
fn bars_order_by_value_or_label_and_cap_the_rest() {
    let lf = df!(
        "carrier" => &["UA", "AA", "DL", "B6", "AS"],
        "delay" => &[3.5_f64, 0.4, 1.6, 9.5, -9.9]
    )
    .unwrap()
    .lazy();
    let by_value = bars(&lf, BarOrder::Value, BAR_CAP);
    assert_eq!(
        labels(&by_value),
        [Some("B6"), Some("UA"), Some("DL"), Some("AA"), Some("AS")]
    );
    assert_eq!(by_value.bars[4].value, -9.9);
    assert_eq!(by_value.more, 0);

    let by_label = bars(&lf, BarOrder::Label, BAR_CAP);
    assert_eq!(
        labels(&by_label),
        [Some("AA"), Some("AS"), Some("B6"), Some("DL"), Some("UA")]
    );

    let capped = bars(&lf, BarOrder::Value, 2);
    assert_eq!(labels(&capped), [Some("B6"), Some("UA")]);
    assert_eq!(capped.more, 3, "the three smallest are counted, not drawn");
    let capped = bars(&lf, BarOrder::Label, 2);
    assert_eq!(labels(&capped), [Some("AA"), Some("AS")]);
    assert_eq!(capped.more, 3);
}

/// An integer category orders as numbers, not text; a null category is a bar of its
/// own, last in label order; a null value leaves its category out and is counted.
#[test]
fn bar_categories_keep_their_type_and_nulls_are_counted() {
    let lf = df!(
        "carrier" => &[Some(10_i64), Some(9), None, Some(100), Some(2)],
        "delay" => &[Some(1.0_f64), Some(2.0), Some(3.0), None, Some(1.0)]
    )
    .unwrap()
    .lazy();
    let data = bars(&lf, BarOrder::Label, BAR_CAP);
    assert_eq!(labels(&data), [Some("2"), Some("9"), Some("10"), None]);
    assert_eq!(data.no_value, 1, "100 has no value");

    let data = bars(&lf, BarOrder::Value, BAR_CAP);
    assert_eq!(
        labels(&data),
        [None, Some("9"), Some("10"), Some("2")],
        "ties keep table order"
    );
}

/// A date category past the calendar is labeled by its stored number, as the
/// table shows it, where the cast to text panicked (#506); bars still order as
/// dates.
#[test]
fn a_date_category_past_the_calendar_is_labeled_by_its_stored_number() {
    let paris = TimeZone::opt_try_new(Some("Europe/Paris")).unwrap();
    let datetime = |unit, zone: Option<TimeZone>| {
        Series::new("at".into(), [i64::MIN + 1, 0])
            .cast(&DataType::Datetime(unit, zone))
            .unwrap()
    };
    for (at, labels_in_order) in [
        (
            Series::new("at".into(), [i32::MAX, 0])
                .cast(&DataType::Date)
                .unwrap(),
            ["1970-01-01", "2147483647 days since 1970-01-01"],
        ),
        (
            datetime(TimeUnit::Milliseconds, None),
            [
                "-9223372036854775807 ms since 1970-01-01 UTC",
                "1970-01-01 00:00:00.000",
            ],
        ),
        (
            datetime(TimeUnit::Microseconds, paris),
            [
                "-9223372036854775807 us since 1970-01-01 UTC",
                "1970-01-01 01:00:00.000000+01:00",
            ],
        ),
    ] {
        let lf =
            DataFrame::new_infer_height(vec![at.into_column(), Column::new("n".into(), [1i64, 2])])
                .unwrap()
                .lazy();
        let by_value =
            prepare_bar_data(&lf, "at", "n", BarOrder::Label, BAR_CAP, &all_rows()).unwrap();
        let counted = prepare_bar_counts(&lf, "at", BarOrder::Label, BAR_CAP, &all_rows()).unwrap();
        for data in [by_value, counted] {
            assert_eq!(labels(&data), labels_in_order.map(Some));
        }
    }
}

/// A category that repeats is refused with the way out, not summed or averaged.
#[test]
fn a_repeated_category_is_refused() {
    let lf = df!(
        "species" => &["Adelie", "Adelie", "Gentoo"],
        "body_mass_g" => &[3750_i64, 3800, 5000]
    )
    .unwrap()
    .lazy();
    let err = prepare_bar_data(
        &lf,
        "species",
        "body_mass_g",
        BarOrder::Value,
        BAR_CAP,
        &all_rows(),
    )
    .unwrap_err()
    .to_string();
    assert!(
        err.contains("species repeats: 2 categories in 3 rows"),
        "{err}"
    );
    assert!(
        err.contains("SELECT species, AVG(body_mass_g) FROM df GROUP BY species"),
        "SQL first: {err}"
    );
    assert!(
        err.contains("(or select avg body_mass_g by species)"),
        "{err}"
    );

    // A name SQL cannot read bare is quoted, and the q form, which cannot, is left out.
    let lf = df!("Species" => &["a", "a"], "mass g" => &[1_i64, 2])
        .unwrap()
        .lazy();
    let err = prepare_bar_data(
        &lf,
        "Species",
        "mass g",
        BarOrder::Value,
        BAR_CAP,
        &all_rows(),
    )
    .unwrap_err()
    .to_string();
    assert!(
            err.ends_with(
                r#"SELECT "Species", AVG("mass g") FROM df GROUP BY "Species", or choose Count for the rows per category"#
            ),
            "{err}"
        );
}

/// Another order draws from the rows already read: the file is not read again.
#[test]
fn a_new_bar_order_does_not_read_again() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("delays.csv");
    std::fs::write(&path, "carrier,delay\nUA,3.5\nAA,0.4\n").unwrap();
    let lf = LazyCsvReader::new(PlRefPath::try_from_path(&path).unwrap())
        .finish()
        .unwrap();
    let sampling = all_rows();
    let first =
        prepare_bar_data(&lf, "carrier", "delay", BarOrder::Value, BAR_CAP, &sampling).unwrap();
    assert_eq!(labels(&first), [Some("UA"), Some("AA")]);
    std::fs::write(&path, "carrier,delay\nZZ,1.0\n").unwrap();
    let again =
        prepare_bar_data(&lf, "carrier", "delay", BarOrder::Label, BAR_CAP, &sampling).unwrap();
    assert_eq!(
        labels(&again),
        [Some("AA"), Some("UA")],
        "from the rows held"
    );
}

/// Booleans and categoricals are categories too.
#[test]
fn booleans_and_categoricals_chart_as_categories() {
    let lf = df!("flag" => &[true, false], "n" => &[5_i64, 7])
        .unwrap()
        .lazy();
    let data = prepare_bar_data(&lf, "flag", "n", BarOrder::Label, BAR_CAP, &all_rows()).unwrap();
    assert_eq!(labels(&data), [Some("false"), Some("true")]);

    let lf = df!("kind" => &["b", "a"], "n" => &[5_i64, 7])
        .unwrap()
        .lazy()
        .with_column(col("kind").cast(DataType::from_categories(Categories::global())));
    let schema = lf.clone().collect_schema().unwrap();
    assert!(is_category_dtype(schema.get("kind").unwrap()));
    let data = prepare_bar_data(&lf, "kind", "n", BarOrder::Value, BAR_CAP, &all_rows()).unwrap();
    assert_eq!(labels(&data), [Some("a"), Some("b")]);
    assert!(!is_category_dtype(&DataType::Float64));
    assert!(is_category_dtype(&DataType::UInt8));
}

/// Bar values print in the table's number format: an integer column whole, any
/// other to the format's places or two, the same for every bar.
#[test]
fn bar_values_follow_the_table_number_format() {
    use crate::numfmt::NumberFormat;
    let plain = NumberFormat::PLAIN;
    let thousands = NumberFormat::preset("thousands").unwrap();
    let european = NumberFormat::preset("european").unwrap();
    assert_eq!(format_bar_value(1_234_567.0, true, &plain), "1234567");
    assert_eq!(format_bar_value(1_234_567.0, true, &thousands), "1,234,567");
    assert_eq!(format_bar_value(22.0, false, &plain), "22.00");
    assert_eq!(format_bar_value(-9.9296, false, &plain), "-9.93");
    assert_eq!(format_bar_value(4213.7, false, &thousands), "4,213.70");
    assert_eq!(format_bar_value(4213.7, false, &european), "4.213,70");
    let one_place = NumberFormat {
        float_precision: Some(1),
        ..thousands
    };
    assert_eq!(format_bar_value(4213.74, false, &one_place), "4,213.7");
    assert_eq!(format_bar_value(0.0, false, &plain), "0.00");
    assert_eq!(format_bar_value(0.001, false, &plain), "1.00e-3");

    let lf = df!("carrier" => &["UA", "AA"], "delay" => &[1234.5_f64, 7.0])
        .unwrap()
        .lazy();
    let data = bars(&lf, BarOrder::Value, BAR_CAP);
    let mut settings = crate::numfmt::NumberFormatSettings {
        format: NumberFormat::preset("thousands").unwrap(),
        ..Default::default()
    };
    assert_eq!(data.value_labels(&settings), ["1,234.50", "7.00"]);
    settings.enabled = false;
    assert_eq!(
        data.value_labels(&settings),
        ["1234.50", "7.00"],
        "F turns it off"
    );
}

fn species(n_adelie: usize, n_gentoo: usize, n_chinstrap: usize, n_null: usize) -> LazyFrame {
    let mut species: Vec<Option<&str>> = Vec::new();
    // Interleaved, so no stretch of the table is one species.
    let mut left = [
        (Some("Adelie"), n_adelie),
        (Some("Gentoo"), n_gentoo),
        (Some("Chinstrap"), n_chinstrap),
        (None, n_null),
    ];
    while left.iter().any(|(_, n)| *n > 0) {
        for (name, n) in &mut left {
            if *n > 0 {
                species.push(*name);
                *n -= 1;
            }
        }
    }
    df!("species" => species).unwrap().lazy()
}

fn counts(data: &BarData) -> Vec<(Option<&str>, f64)> {
    data.bars
        .iter()
        .map(|b| (b.label.as_deref(), b.value))
        .collect()
}

/// Count is exact over the whole view, not a count of the sample: more rows than
/// the sample size are all counted, a null category is a bar of its own, and the
/// note says the counts are of every row.
#[test]
fn counts_are_exact_past_the_sample_size() {
    let lf = species(30_000, 15_000, 4_999, 1);
    let sampling = ChartSampling::rows(Some(1_000));
    let data = prepare_bar_counts(&lf, "species", BarOrder::Value, BAR_CAP, &sampling).unwrap();
    assert_eq!(
        counts(&data),
        [
            (Some("Adelie"), 30_000.0),
            (Some("Gentoo"), 15_000.0),
            (Some("Chinstrap"), 4_999.0),
            (None, 1.0)
        ]
    );
    assert_eq!(data.counted, Some(50_000), "counted past the sample size");
    assert_eq!(data.rows.sample_size, None);
    assert_eq!(data.value_column, "count");
    assert_eq!(
        data.value_labels(&crate::numfmt::NumberFormatSettings {
            format: crate::numfmt::NumberFormat::preset("thousands").unwrap(),
            ..Default::default()
        }),
        ["30,000", "15,000", "4,999", "1"],
        "whole numbers"
    );

    let by_label = prepare_bar_counts(&lf, "species", BarOrder::Label, BAR_CAP, &sampling).unwrap();
    assert_eq!(
        counts(&by_label),
        [
            (Some("Adelie"), 30_000.0),
            (Some("Chinstrap"), 4_999.0),
            (Some("Gentoo"), 15_000.0),
            (None, 1.0)
        ],
        "the null category last"
    );

    // Every row read, or a view under the sample size: nothing to say.
    let every = prepare_bar_counts(
        &lf,
        "species",
        BarOrder::Value,
        BAR_CAP,
        &ChartSampling::rows(None),
    )
    .unwrap();
    assert_eq!(every.counted, None);
    let small = species(152, 124, 68, 0);
    let data =
        prepare_bar_counts(&small, "species", BarOrder::Value, BAR_CAP, &all_rows()).unwrap();
    assert_eq!(
        counts(&data),
        [
            (Some("Adelie"), 152.0),
            (Some("Gentoo"), 124.0),
            (Some("Chinstrap"), 68.0)
        ]
    );
    assert_eq!(data.counted, None);
}

/// Equal counts come A to Z; past the bar cap the rest are counted, not drawn; past
/// the category cap the count stops and says so rather than drawing a part.
#[test]
fn counts_cap_their_bars_and_stop_past_the_category_cap() {
    let lf = df!("carrier" => &["UA", "B6", "AA", "AA", "DL", "B6", "AA"])
        .unwrap()
        .lazy();
    let data = prepare_bar_counts(&lf, "carrier", BarOrder::Value, 2, &all_rows()).unwrap();
    assert_eq!(counts(&data), [(Some("AA"), 3.0), (Some("B6"), 2.0)]);
    assert_eq!(data.more, 2, "DL and UA are counted, not drawn");
    let data = prepare_bar_counts(&lf, "carrier", BarOrder::Value, BAR_CAP, &all_rows()).unwrap();
    assert_eq!(
        counts(&data)[2..],
        [(Some("DL"), 1.0), (Some("UA"), 1.0)],
        "ties A to Z"
    );

    let err = count_bars(&lf, "carrier", BarOrder::Value, BAR_CAP, 3, &all_rows())
        .unwrap_err()
        .to_string();
    assert_eq!(
        err,
        "more than 3 categories of carrier: counting stopped. Count by a column with \
             fewer values"
    );
    let data = count_bars(&lf, "carrier", BarOrder::Value, BAR_CAP, 4, &all_rows()).unwrap();
    assert_eq!(data.bars.len(), 4, "four is not more than four");
}

/// Batches are added up category by category, merged as they pile up; the read is
/// told to stop as soon as the categories pass the cap.
#[test]
fn a_tally_merges_batches_and_stops_past_its_cap() {
    let batch = |ids: std::ops::Range<i64>| df!("id" => ids.collect::<Vec<_>>()).unwrap();
    let mut tally = Tally::new("id", 200_000);
    assert!(!tally.observe(&batch(0..70_000)).unwrap());
    assert_eq!(tally.merged, 70_000, "merged once the batches pile up");
    assert!(!tally.observe(&batch(0..10)).unwrap());
    let Counted::All { counts, rows } = tally.finish().unwrap() else {
        panic!("under the cap");
    };
    assert_eq!(rows, 70_010);
    let counts = counts.unwrap();
    assert_eq!(counts.height(), 70_000);
    let total: u64 = counts
        .column(COUNT_COLUMN)
        .unwrap()
        .u64()
        .unwrap()
        .sum()
        .unwrap();
    assert_eq!(total, 70_010);

    let mut tally = Tally::new("id", 1_000);
    assert!(
        tally.observe(&batch(0..70_000)).unwrap(),
        "past the cap: stop reading"
    );
    assert!(matches!(tally.finish().unwrap(), Counted::TooMany));
}

/// Through the streamed pass: past the cap the read stops and the count says so; a
/// cancelled count is an error and is not held as the view's counts.
#[test]
fn a_streamed_count_stops_past_its_cap_or_when_cancelled() {
    let ids = df!("id" => (0..200_000i64).collect::<Vec<_>>())
        .unwrap()
        .lazy();
    let cancel = Arc::default();
    assert!(matches!(
        stream_counts(&ids, "id", 1_000, &cancel).unwrap(),
        Counted::TooMany
    ));

    let lf = species(30_000, 15_000, 4_999, 1);
    let sampling = ChartSampling::rows(Some(1_000));
    sampling.cancel.store(true, Ordering::Relaxed);
    let err = prepare_bar_counts(&lf, "species", BarOrder::Value, BAR_CAP, &sampling)
        .unwrap_err()
        .to_string();
    assert_eq!(err, "count cancelled");
    assert!(sampling.held.0.lock().unwrap().counts.is_empty());
    sampling.cancel.store(false, Ordering::Relaxed);
    let data = prepare_bar_counts(&lf, "species", BarOrder::Value, BAR_CAP, &sampling).unwrap();
    assert_eq!(data.counted, Some(50_000));
}

/// When the rows held are the whole view, Count counts them rather than reading
/// again; a count is held too, so another order does not count again.
#[test]
fn counts_come_from_the_rows_held_and_are_held() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("flights.csv");
    std::fs::write(&path, "carrier,delay\nUA,1\nUA,2\nAA,3\n").unwrap();
    let lf = LazyCsvReader::new(PlRefPath::try_from_path(&path).unwrap())
        .finish()
        .unwrap();
    let sampling = all_rows();
    prepare_histogram_data(&lf, "delay", 10, ValueRange::All, &sampling).unwrap();
    std::fs::write(&path, "carrier,delay\nZZ,1\n").unwrap();
    // The rows held have no carrier: read, one count per category.
    let data = prepare_bar_counts(&lf, "carrier", BarOrder::Value, BAR_CAP, &sampling).unwrap();
    assert_eq!(counts(&data), [(Some("ZZ"), 1.0)]);

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("flights.csv");
    std::fs::write(&path, "carrier,delay\nUA,1\nUA,2\nAA,3\n").unwrap();
    let lf = LazyCsvReader::new(PlRefPath::try_from_path(&path).unwrap())
        .finish()
        .unwrap();
    let sampling = all_rows();
    // Refused, carriers repeat; the rows it read stay held.
    assert!(
        prepare_bar_data(&lf, "carrier", "delay", BarOrder::Value, BAR_CAP, &sampling).is_err()
    );
    std::fs::write(&path, "carrier,delay\nZZ,1\n").unwrap();
    let data = prepare_bar_counts(&lf, "carrier", BarOrder::Value, BAR_CAP, &sampling).unwrap();
    assert_eq!(
        counts(&data),
        [(Some("UA"), 2.0), (Some("AA"), 1.0)],
        "counted from the rows held"
    );
    // Without the rows, only the count held can say this.
    sampling.held.0.lock().unwrap().rows = None;
    let data = prepare_bar_counts(&lf, "carrier", BarOrder::Label, BAR_CAP, &sampling).unwrap();
    assert_eq!(
        counts(&data),
        [(Some("AA"), 1.0), (Some("UA"), 2.0)],
        "another order from the count held"
    );

    // A view the sample size takes whole is read as a chart reads it, and its rows
    // held for the next chart.
    let sampling = ChartSampling {
        known_total: Some(3),
        ..all_rows()
    };
    std::fs::write(&path, "carrier,delay\nUA,1\nUA,2\nAA,3\n").unwrap();
    let data = prepare_bar_counts(&lf, "carrier", BarOrder::Value, BAR_CAP, &sampling).unwrap();
    assert_eq!(counts(&data), [(Some("UA"), 2.0), (Some("AA"), 1.0)]);
    let holding = sampling.held.0.lock().unwrap();
    let held = holding.rows.as_ref().expect("the rows read are held");
    assert_eq!(held.df.column("carrier").unwrap().len(), 3);
}

// ----- Aggregates, buckets, cumulative, color -----

/// Daily rows of two symbols over three months, as a lazy frame: `date`,
/// `symbol`, `ret` (A gains 1 a day, B 2).
fn returns() -> LazyFrame {
    let days: Vec<i32> = (0..90).collect();
    let n = days.len();
    let mut df = df!(
            "date" => days.iter().chain(&days).map(|d| 19723 + d).collect::<Vec<i32>>(),
            "symbol" => std::iter::repeat_n("A", n).chain(std::iter::repeat_n("B", n)).collect::<Vec<_>>(),
            "ret" => std::iter::repeat_n(1.0, n).chain(std::iter::repeat_n(2.0, n)).collect::<Vec<f64>>()
        )
        .unwrap();
    df.apply("date", |c| c.cast(&DataType::Date).unwrap())
        .unwrap();
    df.lazy()
}

fn aggregate(
    lf: &LazyFrame,
    unit: crate::chart_modal::TimeUnit,
    aggregate: crate::chart_modal::Aggregate,
    cumulative: crate::chart_modal::Cumulative,
    color: Option<ColorSplit<'_>>,
) -> GroupedSeries {
    let schema = lf.clone().collect_schema().unwrap();
    let ys = ["ret".to_string()];
    prepare_aggregate_xy(
        lf,
        schema.as_ref(),
        &AggregateSpec {
            x: "date",
            time_unit: unit,
            ys: &ys,
            aggregate,
            quantile: 90,
            cumulative,
            color,
        },
        &all_rows(),
    )
    .unwrap()
}

/// A month bucket makes one point per month, the aggregate over every row in
/// it, per color group, at the month's first day.
#[test]
fn a_time_bucket_aggregates_every_row_per_month_and_color() {
    use crate::chart_modal::{Aggregate, Cumulative, TimeUnit};
    let lf = returns();
    let groups = [Some("A".to_string()), Some("B".to_string())];
    let split = ColorSplit {
        column: "symbol",
        groups: &groups,
        other: false,
    };
    let sum = aggregate(
        &lf,
        TimeUnit::Month,
        Aggregate::Sum,
        Cumulative::Off,
        Some(split),
    );
    assert_eq!(sum.names, ["A", "B"]);
    assert_eq!(sum.x_axis_kind, XAxisTemporalKind::Date);
    assert_eq!(sum.rows.total_rows, 180, "every row, no sample");
    // 2024-01-01 is day 19723: January, February (29 days in 2024), March.
    let xs: Vec<f64> = sum.series[0].iter().map(|p| p.0).collect();
    assert_eq!(xs, [19723.0, 19754.0, 19783.0]);
    let a: Vec<f64> = sum.series[0].iter().map(|p| p.1).collect();
    let b: Vec<f64> = sum.series[1].iter().map(|p| p.1).collect();
    assert_eq!(a, [31.0, 29.0, 30.0]);
    assert_eq!(b, [62.0, 58.0, 60.0]);

    let mean = aggregate(
        &lf,
        TimeUnit::Month,
        Aggregate::Mean,
        Cumulative::Off,
        Some(split),
    );
    assert!(mean.series[1].iter().all(|p| p.1 == 2.0));
    let count = aggregate(
        &lf,
        TimeUnit::Quarter,
        Aggregate::Count,
        Cumulative::Off,
        None,
    );
    assert_eq!(count.names, ["count"]);
    assert_eq!(
        count.series[0],
        [(19723.0, 180.0)],
        "one quarter, both symbols"
    );
    let weeks = aggregate(&lf, TimeUnit::Week, Aggregate::Max, Cumulative::Off, None);
    // 2024-01-01 is a Monday: 90 days are 13 weeks less a day, in 13 buckets.
    assert_eq!(weeks.series[0].len(), 13);
    assert!(weeks.series[0].iter().all(|p| p.1 == 2.0));
}

/// Cumulative runs along X after the aggregate: a running sum, or returns
/// compounded.
#[test]
fn cumulative_sums_or_compounds_along_x() {
    use crate::chart_modal::{Aggregate, Cumulative, TimeUnit};
    let lf = returns();
    let groups = [Some("A".to_string())];
    let split = ColorSplit {
        column: "symbol",
        groups: &groups,
        other: false,
    };
    let running = aggregate(
        &lf,
        TimeUnit::Month,
        Aggregate::Sum,
        Cumulative::Sum,
        Some(split),
    );
    let ys: Vec<f64> = running.series[0].iter().map(|p| p.1).collect();
    assert_eq!(ys, [31.0, 60.0, 90.0]);
    assert_eq!(running.names, ["A"], "only the groups picked");

    let mut points = vec![(0.0, 0.1), (1.0, 0.1), (2.0, -0.5)];
    accumulate(&mut points, Cumulative::Compound);
    let ys: Vec<f64> = points.iter().map(|p| (p.1 * 1e6).round() / 1e6).collect();
    assert_eq!(ys, [0.1, 0.21, -0.395]);
    let mut points = vec![(0.0, 3.0), (1.0, 4.0)];
    accumulate(&mut points, Cumulative::Off);
    assert_eq!(points, [(0.0, 3.0), (1.0, 4.0)]);
}

/// Compound runs over every row, not over a bucket's mean: 1% a day for 90 days,
/// bucketed by month, is 1.01^31 - 1 at January's end, 1.01^60 - 1 at
/// February's (29 days in 2024), 1.01^90 - 1 at March's.
#[test]
fn compound_runs_over_the_rows_of_each_bucket() {
    use crate::chart_modal::{Aggregate, Cumulative, TimeUnit};
    let mut df = df!(
        "date" => (0..90).map(|d| 19723 + d).collect::<Vec<i32>>(),
        "ret" => vec![0.01; 90]
    )
    .unwrap();
    df.apply("date", |c| c.cast(&DataType::Date).unwrap())
        .unwrap();
    let lf = df.lazy();
    for how in [Aggregate::Mean, Aggregate::Sum, Aggregate::Max] {
        let out = aggregate(&lf, TimeUnit::Month, how, Cumulative::Compound, None);
        let ys: Vec<f64> = out.series[0].iter().map(|p| p.1).collect();
        let want = [
            1.01f64.powi(31) - 1.0,
            1.01f64.powi(60) - 1.0,
            1.01f64.powi(90) - 1.0,
        ];
        for (y, w) in ys.iter().zip(want) {
            assert!((y - w).abs() < 1e-9, "{how:?}: {ys:?}");
        }
    }
    let rows = aggregate(
        &lf,
        TimeUnit::Month,
        Aggregate::Count,
        Cumulative::Compound,
        None,
    );
    let ys: Vec<f64> = rows.series[0].iter().map(|p| p.1).collect();
    assert_eq!(ys, [31.0, 60.0, 90.0], "a count runs as a count");
}

/// A group with no values is a gap, not the zero a sum of nothing is.
#[test]
fn a_bucket_with_no_values_is_a_gap() {
    use crate::chart_modal::{Aggregate, Cumulative, TimeUnit};
    let lf = df!(
        "date" => [0i32, 0, 1, 2],
        "ret" => [Some(1.0), Some(2.0), None, Some(4.0)]
    )
    .unwrap()
    .lazy()
    .with_column(col("date").cast(DataType::Date));
    let out = aggregate(&lf, TimeUnit::Day, Aggregate::Sum, Cumulative::Off, None);
    assert_eq!(out.series[0], [(0.0, 3.0), (2.0, 4.0)]);
    assert_eq!(out.breaks[0], [1], "the line breaks over day 1");
}

/// An X of nearly as many values as rows is refused before the group-by, which
/// would hold a group per row.
#[test]
fn an_x_of_too_many_values_is_refused_first() {
    use crate::chart_modal::{Aggregate, Cumulative};
    let n = AGGREGATE_POINTS_MAX as i64 + 10_000;
    let lf = df!("x" => (0..n).collect::<Vec<i64>>(), "ret" => vec![1.0; n as usize])
        .unwrap()
        .lazy();
    let schema = lf.clone().collect_schema().unwrap();
    let ys = ["ret".to_string()];
    let err = prepare_aggregate_xy(
        &lf,
        schema.as_ref(),
        &AggregateSpec {
            x: "x",
            time_unit: crate::chart_modal::TimeUnit::None,
            ys: &ys,
            aggregate: Aggregate::Mean,
            quantile: 90,
            cumulative: Cumulative::Off,
            color: None,
        },
        &all_rows(),
    )
    .unwrap_err();
    assert!(err.to_string().contains("values of x"), "{err}");
}

/// Color takes the values with the most rows, one per palette color, equal
/// counts in the column's order; a pick takes its values, in the order picked.
#[test]
fn color_takes_the_largest_groups_or_the_ones_picked() {
    let values: Vec<String> = (0..9)
        .flat_map(|i| std::iter::repeat_n(format!("v{i}"), 10 + i))
        .chain(std::iter::once("v0".to_string()))
        .collect();
    let lf = df!("c" => values).unwrap().lazy();
    let rows = value_rows(&lf, "c", &all_rows()).unwrap();
    assert_eq!(rows.values.len(), 9);
    assert_eq!(rows.rows, 10 + 11 + 12 + 13 + 14 + 15 + 16 + 17 + 18 + 1);
    assert_eq!(rows.values[0], (Some("v8".to_string()), 18));
    let top = color_groups(&rows, &[], 7);
    assert_eq!(
        top,
        ["v8", "v7", "v6", "v5", "v4", "v3", "v2"]
            .map(|v| Some(v.to_string()))
            .to_vec()
    );
    // v0 has 11 rows, as many as v1: the column's order breaks the tie.
    assert_eq!(rows.values[7], (Some("v0".to_string()), 11));
    let picked = [Some("v1".to_string()), None];
    assert_eq!(color_groups(&rows, &picked, 7), picked);
    // A terminal of fewer colors draws fewer.
    assert_eq!(color_groups(&rows, &[], 3).len(), 3);
    assert_eq!(color_groups(&rows, &picked, 1), [Some("v1".to_string())]);
}

/// A line or scatter split by color without an aggregate: the sampled rows, a
/// series per group in X order, rows of no group left out.
#[test]
fn a_color_splits_the_sampled_points() {
    let lf = df!(
        "x" => [3i64, 1, 2, 1, 2],
        "y" => [30.0, 10.0, 20.0, 1.0, 2.0],
        "c" => ["a", "a", "a", "b", "z"]
    )
    .unwrap()
    .lazy();
    let schema = lf.clone().collect_schema().unwrap();
    let groups = [Some("a".to_string()), Some("b".to_string())];
    let split = ColorSplit {
        column: "c",
        groups: &groups,
        other: false,
    };
    let out = prepare_xy_by(&lf, schema.as_ref(), "x", "y", split, &all_rows()).unwrap();
    assert_eq!(out.names, ["a", "b"]);
    assert_eq!(out.series[0], [(1.0, 10.0), (2.0, 20.0), (3.0, 30.0)]);
    assert_eq!(out.series[1], [(1.0, 1.0)]);
}

/// Stdev, a quantile, first and last per X: the sample deviation (none for a
/// group of one, so no point), a linearly interpolated percentile, and the first
/// and last value in the rows' order, nulls passed over, which a sort sets.
#[test]
fn stdev_quantile_first_and_last_per_x() {
    use crate::chart_modal::{Aggregate, Cumulative, TimeUnit};
    // Read order is not value order: x=1 reads 4, 1, 3, 2.
    let lf = df!(
        "x" => [1i64, 1, 1, 1, 2, 3, 3],
        "y" => [Some(4.0), Some(1.0), Some(3.0), Some(2.0), Some(9.0), Some(5.0), None],
        "t" => [3i64, 1, 4, 2, 1, 2, 1],
        "c" => ["a", "b", "a", "b", "a", "a", "a"]
    )
    .unwrap()
    .lazy();
    let schema = lf.clone().collect_schema().unwrap();
    let ys = ["y".to_string()];
    let run = |lf: &LazyFrame, aggregate, quantile| {
        let out = prepare_aggregate_xy(
            lf,
            schema.as_ref(),
            &AggregateSpec {
                x: "x",
                time_unit: TimeUnit::None,
                ys: &ys,
                aggregate,
                quantile,
                cumulative: Cumulative::Off,
                color: None,
            },
            &all_rows(),
        )
        .unwrap();
        out.series[0].clone()
    };
    // x=1: 4, 1, 3, 2, mean 2.5, sample deviation sqrt(5/3).
    let stdev = run(&lf, Aggregate::Stdev, 90);
    assert_eq!(stdev.len(), 1, "x=2 and x=3 have one value each: {stdev:?}");
    assert!((stdev[0].1 - (5.0f64 / 3.0).sqrt()).abs() < 1e-12);
    // p90 of 1, 2, 3, 4: 3 + 0.7 = 3.7; p25: 1 + 0.75 = 1.75.
    let p90 = run(&lf, Aggregate::Quantile, 90);
    assert!((p90[0].1 - 3.7).abs() < 1e-12, "{p90:?}");
    assert_eq!(p90[1], (2.0, 9.0));
    let p25 = run(&lf, Aggregate::Quantile, 25);
    assert!((p25[0].1 - 1.75).abs() < 1e-12, "{p25:?}");
    // In read order: x=1 starts at 4 and ends at 2; x=3's last value is null,
    // so its last is the value before.
    assert_eq!(
        run(&lf, Aggregate::First, 90),
        [(1.0, 4.0), (2.0, 9.0), (3.0, 5.0)]
    );
    assert_eq!(
        run(&lf, Aggregate::Last, 90),
        [(1.0, 2.0), (2.0, 9.0), (3.0, 5.0)]
    );
    // Sorted by t: x=1 reads 1, 2, 4, 3.
    let sorted = lf.clone().sort(["t"], Default::default());
    assert_eq!(run(&sorted, Aggregate::First, 90)[0], (1.0, 1.0));
    assert_eq!(run(&sorted, Aggregate::Last, 90)[0], (1.0, 3.0));
    // A bar of the last per category, split by a color.
    let groups = [Some("a".to_string()), Some("b".to_string())];
    let bars = prepare_bar_aggregate(
        &lf,
        &lf.clone().collect_schema().unwrap(),
        &BarAggregate {
            category: "x",
            value: Some("y"),
            aggregate: Aggregate::Last,
            quantile: 90,
            color: Some(ColorSplit {
                column: "c",
                groups: &groups,
                other: false,
            }),
            order: BarOrder::Label,
            cap: BAR_CAP,
        },
        &all_rows(),
    )
    .unwrap();
    assert_eq!(bars.bars[0].by_group, [Some(3.0), Some(2.0)]);
    assert_eq!(bars.value_column, "last y");
    let p = prepare_bar_aggregate(
        &lf,
        &lf.clone().collect_schema().unwrap(),
        &BarAggregate {
            category: "x",
            value: Some("y"),
            aggregate: Aggregate::Quantile,
            quantile: 90,
            color: None,
            order: BarOrder::Label,
            cap: BAR_CAP,
        },
        &all_rows(),
    )
    .unwrap();
    assert_eq!(p.value_column, "p90 y");
}

/// A distinct count of a string Y per X: nulls are no value, a group of only
/// nulls is a gap; per color, and within a time bucket.
#[test]
fn distinct_counts_any_y_per_x() {
    use crate::chart_modal::{Aggregate, Cumulative, TimeUnit};
    let mut df = df!(
        "date" => [19723i32, 19723, 19723, 19724, 19724, 19754, 19755],
        "name" => [Some("Ann"), Some("Bo"), Some("Ann"), None, None, Some("Cy"), Some("Di")],
        "sex" => ["F", "M", "F", "F", "M", "M", "M"]
    )
    .unwrap();
    df.apply("date", |c| c.cast(&DataType::Date).unwrap())
        .unwrap();
    let lf = df.lazy();
    let schema = lf.clone().collect_schema().unwrap();
    let ys = ["name".to_string()];
    let distinct = |unit, color| {
        prepare_aggregate_xy(
            &lf,
            schema.as_ref(),
            &AggregateSpec {
                x: "date",
                time_unit: unit,
                ys: &ys,
                aggregate: Aggregate::Distinct,
                quantile: 90,
                cumulative: Cumulative::Off,
                color,
            },
            &all_rows(),
        )
        .unwrap()
    };
    let by_day = distinct(TimeUnit::Day, None);
    let ys_of = |s: &[(f64, f64)]| s.iter().map(|p| p.1).collect::<Vec<_>>();
    // Day 1: Ann, Bo; day 2: only nulls, a gap; then Cy, then Di.
    assert_eq!(ys_of(&by_day.series[0]), [2.0, 1.0, 1.0]);
    assert_eq!(by_day.breaks[0], [1], "the day of nulls breaks the line");
    let by_month = distinct(TimeUnit::Month, None);
    assert_eq!(ys_of(&by_month.series[0]), [2.0, 2.0], "Ann, Bo; Cy, Di");
    let groups = [Some("F".to_string()), Some("M".to_string())];
    let split = ColorSplit {
        column: "sex",
        groups: &groups,
        other: false,
    };
    let colored = distinct(TimeUnit::Month, Some(split));
    assert_eq!(colored.names, ["F", "M"]);
    assert_eq!(ys_of(&colored.series[0]), [1.0], "Ann");
    assert_eq!(ys_of(&colored.series[1]), [1.0, 2.0], "Bo; Cy, Di");
    // A bar of distinct names per sex: whole numbers.
    let bars = prepare_bar_aggregate(
        &lf,
        &lf.clone().collect_schema().unwrap(),
        &BarAggregate {
            category: "sex",
            value: Some("name"),
            aggregate: Aggregate::Distinct,
            quantile: 90,
            color: None,
            order: BarOrder::Label,
            cap: BAR_CAP,
        },
        &all_rows(),
    )
    .unwrap();
    let values: Vec<f64> = bars.bars.iter().map(|b| b.value).collect();
    assert_eq!(values, [1.0, 3.0]);
    assert!(bars.value_dtype.is_integer());
    assert_eq!(bars.value_column, "distinct name");
}

/// With Other, every value without a group of its own (a null among them) is one
/// more series, last: a scatter keeps every point, a line and a bar aggregate the
/// rest as they do a group. Off, those rows are left out.
#[test]
fn other_gathers_every_value_without_a_series() {
    use crate::chart_modal::{Aggregate, Cumulative, TimeUnit};
    let lf = df!(
        "x" => [1i64, 1, 2, 2, 3, 3],
        "y" => [10.0, 1.0, 20.0, 2.0, 30.0, 4.0],
        "c" => [Some("a"), Some("b"), Some("a"), Some("z"), Some("a"), None]
    )
    .unwrap()
    .lazy();
    let schema = lf.clone().collect_schema().unwrap();
    let groups = [Some("a".to_string())];
    let split = |other| ColorSplit {
        column: "c",
        groups: &groups,
        other,
    };
    // Scatter.
    let out = prepare_xy_by(&lf, schema.as_ref(), "x", "y", split(true), &all_rows()).unwrap();
    assert_eq!(out.names, ["a", OTHER]);
    assert!(out.other);
    assert_eq!(out.series[1], [(1.0, 1.0), (2.0, 2.0), (3.0, 4.0)]);
    let out = prepare_xy_by(&lf, schema.as_ref(), "x", "y", split(false), &all_rows()).unwrap();
    assert_eq!(out.names, ["a"]);
    assert!(!out.other);
    // A line of the sum per x.
    let ys = ["y".to_string()];
    let line = |other| {
        prepare_aggregate_xy(
            &lf,
            schema.as_ref(),
            &AggregateSpec {
                x: "x",
                time_unit: TimeUnit::None,
                ys: &ys,
                aggregate: Aggregate::Sum,
                quantile: 90,
                cumulative: Cumulative::Off,
                color: Some(split(other)),
            },
            &all_rows(),
        )
        .unwrap()
    };
    let on = line(true);
    assert_eq!(on.names, ["a", OTHER]);
    assert_eq!(on.series[1], [(1.0, 1.0), (2.0, 2.0), (3.0, 4.0)]);
    assert_eq!(on.rows.total_rows, 6, "every row is in a series");
    let off = line(false);
    assert_eq!(off.names, ["a"]);
    assert_eq!(off.rows.total_rows, 3);
    // A bar of the mean per x.
    let bars = |other| {
        let spec = BarAggregate {
            category: "x",
            value: Some("y"),
            aggregate: Aggregate::Mean,
            quantile: 90,
            color: Some(split(other)),
            order: BarOrder::Label,
            cap: BAR_CAP,
        };
        prepare_bar_aggregate(
            &lf,
            &lf.clone().collect_schema().unwrap(),
            &spec,
            &all_rows(),
        )
        .unwrap()
    };
    let on = bars(true);
    assert_eq!(on.groups, ["a", OTHER]);
    assert!(on.other);
    let by: Vec<Vec<Option<f64>>> = on.bars.iter().map(|b| b.by_group.clone()).collect();
    assert_eq!(
        by,
        [
            vec![Some(10.0), Some(1.0)],
            vec![Some(20.0), Some(2.0)],
            vec![Some(30.0), Some(4.0)]
        ]
    );
    let off = bars(false);
    assert_eq!(off.groups, ["a"]);
    assert!(off.bars.iter().all(|b| b.by_group.len() == 1));
}

/// A bar of the mean per category, split by color: a value per group in each
/// bar, ordered by the largest group; a count needs no value column.
#[test]
fn bars_aggregate_per_category_and_color() {
    use crate::chart_modal::Aggregate;
    let lf = df!(
        "carrier" => ["UA", "UA", "UA", "AA", "AA"],
        "origin" => ["EWR", "EWR", "JFK", "EWR", "JFK"],
        "delay" => [10.0, 20.0, 5.0, 1.0, 50.0]
    )
    .unwrap()
    .lazy();
    let groups = [Some("EWR".to_string()), Some("JFK".to_string())];
    let split = ColorSplit {
        column: "origin",
        groups: &groups,
        other: false,
    };
    let spec = BarAggregate {
        category: "carrier",
        value: Some("delay"),
        aggregate: Aggregate::Mean,
        quantile: 90,
        color: Some(split),
        order: BarOrder::Value,
        cap: BAR_CAP,
    };
    let data = prepare_bar_aggregate(
        &lf,
        &lf.clone().collect_schema().unwrap(),
        &spec,
        &all_rows(),
    )
    .unwrap();
    assert_eq!(data.groups, ["EWR", "JFK"]);
    assert_eq!(data.value_column, "mean delay");
    assert_eq!(data.rows.total_rows, 5);
    let bars: Vec<(Option<&str>, Vec<Option<f64>>)> = data
        .bars
        .iter()
        .map(|b| (b.label.as_deref(), b.by_group.clone()))
        .collect();
    assert_eq!(
        bars,
        [
            (Some("AA"), vec![Some(1.0), Some(50.0)]),
            (Some("UA"), vec![Some(15.0), Some(5.0)])
        ],
        "AA's largest group is larger"
    );
    let count = BarAggregate {
        value: None,
        aggregate: Aggregate::Count,
        quantile: 90,
        ..spec
    };
    let data = prepare_bar_aggregate(
        &lf,
        &lf.clone().collect_schema().unwrap(),
        &count,
        &all_rows(),
    )
    .unwrap();
    assert_eq!(data.bars[0].label.as_deref(), Some("UA"));
    assert_eq!(data.bars[0].value, 3.0, "a count adds up across groups");
    assert!(data.value_dtype.is_integer(), "counts print whole");
    // A NaN draws nothing; a category whose values are all null has no bar.
    let odd = df!(
        "carrier" => ["UA", "AA", "DL"],
        "delay" => [Some(f64::NAN), None, Some(1.0)]
    )
    .unwrap()
    .lazy();
    let mean = BarAggregate {
        color: None,
        ..spec
    };
    let data = prepare_bar_aggregate(
        &odd,
        &odd.clone().collect_schema().unwrap(),
        &mean,
        &all_rows(),
    )
    .unwrap();
    let labels: Vec<Option<&str>> = data.bars.iter().map(|b| b.label.as_deref()).collect();
    assert_eq!(labels, [Some("DL")]);
    assert_eq!(data.no_value, 2);
    let sum = BarAggregate {
        aggregate: Aggregate::Sum,
        quantile: 90,
        color: None,
        ..spec
    };
    let data = prepare_bar_aggregate(
        &lf,
        &lf.clone().collect_schema().unwrap(),
        &sum,
        &all_rows(),
    )
    .unwrap();
    assert_eq!(
        data.bars
            .iter()
            .map(|b| (b.label.as_deref(), b.value))
            .collect::<Vec<_>>(),
        [(Some("AA"), 51.0), (Some("UA"), 35.0)]
    );
}

/// A histogram split by color: every group on the same bins, each a share of
/// its own rows when asked.
#[test]
fn a_histogram_splits_into_groups_on_shared_bins() {
    let lf = df!(
        "v" => [0.0, 1.0, 2.0, 3.0, 0.0, 0.0],
        "g" => ["a", "a", "a", "a", "b", "b"]
    )
    .unwrap()
    .lazy();
    let groups = [Some("a".to_string()), Some("b".to_string())];
    let split = ColorSplit {
        column: "g",
        groups: &groups,
        other: false,
    };
    let data =
        prepare_histogram_by(&lf, "v", 3, ValueRange::All, true, Some(split), &all_rows()).unwrap();
    assert_eq!(data.bins.len(), 3);
    assert_eq!(data.groups.len(), 2);
    assert_eq!(data.groups[0].counts, [0.25, 0.25, 0.5]);
    assert_eq!(data.groups[1].counts, [1.0, 0.0, 0.0]);
    assert_eq!(data.max_count, 1.0);
    let total: f64 = data.bins.iter().map(|b| b.count).sum();
    assert!((total - 1.0).abs() < 1e-9, "the whole is a share too");
}

/// A box per category, the categories given.
#[test]
fn a_box_per_category() {
    let lf = df!(
        "v" => [1.0, 2.0, 3.0, 10.0, 20.0],
        "k" => ["x", "x", "x", "y", "y"]
    )
    .unwrap()
    .lazy();
    let groups = [Some("y".to_string()), Some("x".to_string())];
    let split = ColorSplit {
        column: "k",
        groups: &groups,
        other: false,
    };
    let data = prepare_box_by(&lf, "v", split, ValueRange::All, &all_rows()).unwrap();
    let names: Vec<&str> = data.stats.iter().map(|s| s.name.as_str()).collect();
    assert_eq!(names, ["y", "x"]);
    assert_eq!(data.stats[1].median, 2.0);
    assert_eq!((data.y_min, data.y_max), (1.0, 20.0));
}

use super::*;

fn table(rows: i64) -> LazyFrame {
    df!("value" => (0..rows).collect::<Vec<_>>())
        .unwrap()
        .lazy()
}

fn live() -> Live {
    Live {
        rows: Arc::new(SampleRows::default()),
        notify: Arc::new(|| {}),
        memory: MemoryCheck::off(),
        watch: ReadWatch::default(),
        bytes_per_row: None,
    }
}

fn values(df: &DataFrame) -> Vec<i64> {
    df.column("value")
        .unwrap()
        .i64()
        .unwrap()
        .into_no_null_iter()
        .collect()
}

/// A Bernoulli sample keeps about n rows (n ± √n, well inside four of them), each
/// row in the order the source holds it, and the same seed keeps the same rows.
#[test]
fn bernoulli_keeps_about_n_rows_in_order_and_repeats() {
    let (n, total) = (2_000usize, 100_000usize);
    let one = live();
    bernoulli(&table(total as i64), n, total, 42_891, &one).unwrap();
    let kept = one.rows.in_source_order().unwrap().unwrap();
    let spread = 4.0 * (n as f64).sqrt();
    assert!(
        (kept.height() as f64 - n as f64).abs() < spread,
        "{} rows",
        kept.height()
    );
    let rows = values(&kept);
    assert!(
        rows.windows(2).all(|pair| pair[0] < pair[1]),
        "source order"
    );
    assert!(kept.column(POSITION).is_err(), "no helper column leaks");
    let two = live();
    bernoulli(&table(total as i64), n, total, 42_891, &two).unwrap();
    assert_eq!(values(&two.rows.in_source_order().unwrap().unwrap()), rows);
    let other = live();
    bernoulli(&table(total as i64), n, total, 7, &other).unwrap();
    assert_ne!(
        values(&other.rows.in_source_order().unwrap().unwrap()),
        rows
    );
}

/// The bar keeps every row when the sample is the table, and none of a table
/// asked for none.
#[test]
fn the_bernoulli_bar_spans_none_to_all() {
    let batch = df!(POSITION => (0..1_000 as IdxSize).collect::<Vec<_>>(), "value" => (0..1_000i64).collect::<Vec<_>>()).unwrap();
    let (_, all) = bernoulli_keep(&batch, 1, bernoulli_bar(1_000, 1_000)).unwrap();
    assert_eq!(all.height(), 1_000);
    let (_, none) = bernoulli_keep(&batch, 1, bernoulli_bar(0, 1_000)).unwrap();
    assert_eq!(none.height(), 0);
}

/// Chunks show in the order they arrive, and once the draw ends, in the order
/// the source holds them.
#[test]
fn chunks_arrive_in_any_order_and_end_in_source_order() {
    let rows = SampleRows::default();
    let chunk = |from: i64| df!("value" => (from..from + 3).collect::<Vec<_>>()).unwrap();
    rows.push(30, chunk(30));
    rows.push(10, chunk(10));
    let first = rows.take_new();
    assert_eq!(
        first.iter().flat_map(values).collect::<Vec<_>>(),
        [30, 31, 32, 10, 11, 12]
    );
    rows.push(20, chunk(20));
    let next = rows.take_new();
    assert_eq!(next.len(), 1, "only what arrived since");
    assert_eq!(rows.rows(), 9);
    assert_eq!(
        values(&rows.in_source_order().unwrap().unwrap()),
        [10, 11, 12, 20, 21, 22, 30, 31, 32]
    );
}

/// Every row and the head stream their batches in; the head stops at its size.
#[test]
fn every_row_and_the_head_stream_in() {
    let all = live();
    let sample = Sample {
        method: SampleMethod::EveryRow,
        ..Sample::default()
    };
    let drawn = draw(&table(5_000), &sample, None, None, false, &all).unwrap();
    assert_eq!((drawn.total, all.rows.rows()), (Some(5_000), 5_000));
    let head = live();
    let sample = Sample {
        method: SampleMethod::FirstRows,
        rows: 120,
        ..Sample::default()
    };
    draw(&table(5_000), &sample, Some(5_000), None, false, &head).unwrap();
    assert_eq!(
        values(&head.rows.in_source_order().unwrap().unwrap()),
        (0..120).collect::<Vec<_>>()
    );
}

/// A known total draws by chance row by row; an unknown one keeps a reservoir
/// of exactly n, one chunk at the end.
#[test]
fn a_known_total_draws_about_n_and_an_unknown_one_exactly_n() {
    let sample = Sample {
        rows: 500,
        ..Sample::default()
    };
    let known = live();
    let drawn = draw(&table(20_000), &sample, Some(20_000), None, false, &known).unwrap();
    assert!(drawn.about);
    let unknown = live();
    let drawn = draw(&table(20_000), &sample, None, None, false, &unknown).unwrap();
    assert!(!drawn.about);
    assert_eq!((drawn.total, unknown.rows.rows()), (Some(20_000), 500));
}

/// Memory that will not hold the rest stops the draw and keeps the rows so far,
/// and says why, naming the setting. Only the streaming engine reads in batches.
#[cfg(feature = "streaming")]
#[test]
fn low_memory_stops_the_draw_and_keeps_what_it_has() {
    let mut low = live();
    low.memory = MemoryCheck {
        limit: Limit::Available,
        probe: Arc::new(|| Some(1)),
    };
    let sample = Sample {
        method: SampleMethod::EveryRow,
        ..Sample::default()
    };
    // Five frames read one after another: batches, not one frame whole.
    let parts: Vec<LazyFrame> = (0..5).map(|_| table(100_000)).collect();
    let lf = concat(parts, UnionArgs::default()).unwrap();
    let drawn = draw(&lf, &sample, Some(500_000), None, false, &low).unwrap();
    assert!(drawn.cut);
    let held = low.rows.rows();
    assert!(held > 0 && held < 500_000, "{held}");
    let reason = low.rows.stopped().unwrap();
    assert!(reason.contains("memory ran low"), "{reason}");
    assert!(reason.contains(MEMORY_SETTING), "{reason}");
}

/// A path given draws that way whatever is known now: a reservoir with the count
/// in, and a Bernoulli sample of the total recorded with none. The same seed and
/// path keep the same rows.
#[test]
fn a_recorded_path_draws_the_same_rows_whatever_is_known_now() {
    let sample = Sample {
        rows: 500,
        ..Sample::default()
    };
    let rows = |live: &Live| values(&live.rows.in_source_order().unwrap().unwrap());
    let reservoir = live();
    draw(&table(20_000), &sample, None, None, false, &reservoir).unwrap();
    let counted = live();
    let drawn = draw(
        &table(20_000),
        &sample,
        Some(20_000),
        Some(DrawPath::Reservoir),
        false,
        &counted,
    )
    .unwrap();
    assert_eq!(drawn.path, Some(DrawPath::Reservoir));
    assert_eq!(rows(&counted), rows(&reservoir));

    let known = live();
    let drawn = draw(&table(20_000), &sample, Some(20_000), None, false, &known).unwrap();
    assert_eq!(drawn.path, Some(DrawPath::Bernoulli { of: 20_000 }));
    let uncounted = live();
    let path = drawn.path;
    draw(&table(20_000), &sample, None, path, false, &uncounted).unwrap();
    assert_eq!(rows(&uncounted), rows(&known));
}

/// A reservoir holds its rows to the end: past the memory there is, it stops
/// there, keeps what it holds, and says why as a live draw does.
#[cfg(feature = "streaming")]
#[test]
fn a_reservoir_past_the_memory_stops_and_keeps_what_it_holds() {
    let check = MemoryCheck {
        limit: Limit::Fixed(1),
        probe: Arc::new(|| None),
    };
    let mut low = live();
    low.watch = ReadWatch::judging_held(Arc::new(move |bytes, rows| {
        check.holds_too_much(bytes, rows)
    }));
    let parts: Vec<LazyFrame> = (0..5).map(|_| table(100_000)).collect();
    let lf = concat(parts, UnionArgs::default()).unwrap();
    let sample = Sample {
        rows: 400_000,
        ..Sample::default()
    };
    let drawn = draw(&lf, &sample, None, None, false, &low).unwrap();
    assert!(drawn.cut);
    assert!(low.rows.rows() > 0);
    assert!(drawn.total.unwrap() < 500_000, "it stopped early");
    let reason = low.rows.stopped().unwrap();
    assert!(reason.contains(MEMORY_SETTING), "{reason}");
}

/// Without the streaming engine the read is one batch, whole before the draw sees
/// it: there is no partway to stop at, so the rows read are kept.
#[cfg(not(feature = "streaming"))]
#[test]
fn without_streaming_the_read_arrives_whole() {
    let mut low = live();
    low.memory = MemoryCheck {
        limit: Limit::Available,
        probe: Arc::new(|| Some(1)),
    };
    let sample = Sample {
        method: SampleMethod::EveryRow,
        ..Sample::default()
    };
    let parts: Vec<LazyFrame> = (0..5).map(|_| table(100_000)).collect();
    let lf = concat(parts, UnionArgs::default()).unwrap();
    let drawn = draw(&lf, &sample, Some(500_000), None, false, &low).unwrap();
    assert!(!drawn.cut);
    assert_eq!(low.rows.rows(), 500_000);
    assert_eq!(low.rows.take_new().len(), 1, "one batch");
}

/// Before a draw, an estimate past the room is refused with the way through.
#[test]
fn an_estimate_past_the_room_is_refused_with_the_way_through() {
    let check = MemoryCheck {
        limit: Limit::Available,
        probe: Arc::new(|| Some(4 << 30)),
    };
    let refused = check.refuses(6 << 30).unwrap();
    assert!(refused.contains("available now"), "{refused}");
    assert!(refused.contains("Enter again"), "{refused}");
    assert!(
        refused.contains("-c analysis.sample_memory_limit"),
        "{refused}"
    );
    assert!(check.refuses(1 << 30).is_none());
    assert!(MemoryCheck::off().refuses(u64::MAX).is_none());
    let fixed = MemoryCheck {
        limit: Limit::Fixed(1 << 20),
        probe: Arc::new(|| None),
    };
    assert!(fixed.refuses(2 << 20).unwrap().contains(MEMORY_SETTING));
}

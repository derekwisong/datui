use super::*;

fn records(bytes: Vec<u8>, columns: Vec<ColumnLayout>, rows: usize) -> Arc<FixedRecords> {
    Arc::new(FixedRecords::new(vec![Arc::new(Bytes::Owned(bytes))], columns, rows).unwrap())
}

fn column(name: &str, start: usize, stride: usize, physical: Physical) -> ColumnLayout {
    ColumnLayout::new(
        name,
        start,
        stride,
        physical,
        physical.width().unwrap_or(stride),
    )
}

/// How long each integer width takes to decode, odd widths beside native ones:
/// `cargo test --release -p datui-lib --lib fixed_records::tests::time_integer_widths
/// -- --ignored --nocapture`
#[test]
#[ignore = "a timing, not a check"]
fn time_integer_widths() {
    const ROWS: usize = 10_000_000;
    const STRIDE: usize = 16;
    let bytes: Vec<u8> = (0..ROWS * STRIDE).map(|i| (i * 31 % 251) as u8).collect();
    for physical in [
        Physical::Unsigned(2),
        Physical::Unsigned(3),
        Physical::Unsigned(4),
        Physical::Signed(3),
        Physical::Unsigned(5),
        Physical::Signed(6),
        Physical::Unsigned(8),
    ] {
        for big_endian in [false, true] {
            let layout = ColumnLayout {
                big_endian,
                ..column("v", 1, STRIDE, physical)
            };
            let started = std::time::Instant::now();
            let decoded = decode(&bytes, &layout, ROWS).unwrap();
            println!(
                "{physical:?} {}: {:?} ({})",
                if big_endian { "be" } else { "le" },
                started.elapsed(),
                decoded.dtype()
            );
        }
    }
}

#[test]
fn strided_columns_decode_in_both_byte_orders() {
    // Two records of (u16, i32): 1, -2 then 3, -4, little endian.
    let mut bytes = Vec::new();
    for (a, b) in [(1u16, -2i32), (3, -4)] {
        bytes.extend(a.to_le_bytes());
        bytes.extend(b.to_le_bytes());
    }
    let lf = records(
        bytes.clone(),
        vec![
            column("a", 0, 6, Physical::Unsigned(2)),
            column("b", 2, 6, Physical::Signed(4)),
        ],
        usize::MAX,
    )
    .lazy();
    let df = lf.collect().unwrap();
    assert_eq!(df.height(), 2);
    assert_eq!(
        df.column("b").unwrap().i32().unwrap().to_vec(),
        [Some(-2), Some(-4)]
    );
    let mut big = column("a", 0, 6, Physical::Unsigned(2));
    big.big_endian = true;
    let df = records(bytes, vec![big], usize::MAX).collect(9).unwrap();
    assert_eq!(
        df.column("a").unwrap().u16().unwrap().to_vec(),
        [Some(256), Some(768)]
    );
}

#[test]
fn odd_widths_sign_extend_and_sentinels_read_null() {
    // Two 3-byte signed samples, -2 and 8,388,607, then the s3 minimum.
    let bytes = vec![0xfe, 0xff, 0xff, 0xff, 0xff, 0x7f, 0x00, 0x00, 0x80];
    let mut s3 = column("s", 0, 3, Physical::Signed(3));
    let df = records(bytes.clone(), vec![s3.clone()], usize::MAX)
        .collect(9)
        .unwrap();
    assert_eq!(
        df.column("s").unwrap().i32().unwrap().to_vec(),
        [Some(-2), Some(8_388_607), Some(-8_388_608)]
    );
    s3.null = Some(Null::Min);
    let df = records(bytes, vec![s3], usize::MAX).collect(9).unwrap();
    assert_eq!(df.column("s").unwrap().null_count(), 1);
    // Five-byte unsigned big-endian and six-byte signed, each its own fast path.
    let u5 = ColumnLayout {
        big_endian: true,
        ..column("u5", 0, 11, Physical::Unsigned(5))
    };
    let s6 = column("s6", 5, 11, Physical::Signed(6));
    let mut bytes = vec![0x01, 0, 0, 0, 0x02];
    bytes.extend((-3i64).to_le_bytes()[..6].iter());
    let df = records(bytes, vec![u5, s6], 1).collect(1).unwrap();
    assert_eq!(
        df.column("u5").unwrap().u64().unwrap().get(0),
        Some(0x01_0000_0002)
    );
    assert_eq!(df.column("s6").unwrap().i64().unwrap().get(0), Some(-3));
    let mut u2 = column("u", 0, 2, Physical::Unsigned(2));
    u2.null = Some(Null::Max);
    let df = records(vec![0xff, 0xff, 1, 0], vec![u2], usize::MAX)
        .collect(9)
        .unwrap();
    assert_eq!(
        df.column("u").unwrap().u16().unwrap().to_vec(),
        [None, Some(1)]
    );
    // A native signed width with a sentinel value, and one the type cannot hold.
    let mut s4 = column("s", 0, 4, Physical::Signed(4));
    s4.null = Some(Null::Value(-1));
    let bytes: Vec<u8> = [-1i32, 7].iter().flat_map(|v| v.to_le_bytes()).collect();
    let df = records(bytes.clone(), vec![s4.clone()], usize::MAX)
        .collect(9)
        .unwrap();
    assert_eq!(
        df.column("s").unwrap().i32().unwrap().to_vec(),
        [None, Some(7)]
    );
    s4.null = Some(Null::Value(1 << 40));
    let df = records(bytes, vec![s4], usize::MAX).collect(9).unwrap();
    assert_eq!(df.column("s").unwrap().null_count(), 0);
}

#[test]
fn a_count_makes_an_array_and_factor_offset_a_float() {
    // Two records of three u1 channels.
    let mut channels = column("ch", 0, 3, Physical::Unsigned(1));
    channels.count = 3;
    channels.logical = Logical::Linear {
        factor: 0.5,
        offset: -1.0,
    };
    let df = records(vec![0, 2, 4, 6, 8, 10], vec![channels], usize::MAX)
        .collect(9)
        .unwrap();
    let ch = df.column("ch").unwrap();
    assert_eq!(ch.dtype(), &DataType::Array(Box::new(DataType::Float64), 3));
    assert_eq!(ch.get(1).unwrap().to_string(), "[2.0, 3.0, 4.0]");
}

#[test]
fn dates_and_times_of_day() {
    let mut ymd = column("d", 0, 4, Physical::Unsigned(4));
    ymd.logical = Logical::Yyyymmdd;
    let mut bytes = 20240229u32.to_le_bytes().to_vec();
    bytes.extend(20241301u32.to_le_bytes());
    let df = records(bytes, vec![ymd], usize::MAX).collect(9).unwrap();
    let d = df.column("d").unwrap();
    assert_eq!(d.get(0).unwrap().to_string(), "2024-02-29");
    assert_eq!(d.null_count(), 1, "month 13 is no date");
    let mut tod = column("t", 0, 6, Physical::Unsigned(6));
    tod.big_endian = true;
    tod.logical = Logical::TimeOfDay {
        ns_per_unit: 1,
        date_ns: None,
    };
    let ns: u64 = 34_200_000_000_000; // 09:30
    let df = records(ns.to_be_bytes()[2..].to_vec(), vec![tod], usize::MAX)
        .collect(9)
        .unwrap();
    assert_eq!(
        df.column("t").unwrap().get(0).unwrap().to_string(),
        "09:30:00"
    );
}

#[test]
fn projection_and_n_rows_reach_the_scan() {
    let bytes: Vec<u8> = (0u8..40).collect();
    let lf = records(
        bytes,
        vec![
            column("a", 0, 4, Physical::Unsigned(1)),
            column("b", 1, 4, Physical::Unsigned(1)),
        ],
        usize::MAX,
    )
    .lazy();
    let df = lf.clone().select([col("b")]).limit(3).collect().unwrap();
    assert_eq!(df.get_column_names(), ["b"]);
    assert_eq!(df.height(), 3);
    let count = lf.select([len()]).collect().unwrap();
    assert_eq!(
        count.column("len").unwrap().get(0).unwrap(),
        AnyValue::UInt32(10)
    );
}

#[test]
fn a_window_starts_where_it_is_asked() {
    let bytes: Vec<u8> = (0u8..40).collect();
    let records = records(
        bytes,
        vec![column("a", 0, 4, Physical::Unsigned(1))],
        usize::MAX,
    );
    let df = records.window(8, 5).unwrap();
    assert_eq!(
        df.column("a").unwrap().u8().unwrap().to_vec(),
        [Some(32), Some(36)]
    );
    assert_eq!(records.window(99, 5).unwrap().height(), 0);
}

/// A layout that asks for more than its bytes hold is refused, never read past.
#[test]
fn a_read_past_the_bytes_is_an_error_not_a_panic() {
    let u2 = column("u", 0, 2, Physical::Unsigned(2));
    assert!(decode(&[1, 0, 2, 0], &u2, 2).is_ok());
    assert!(decode(&[1, 0, 2, 0], &u2, 3).is_err());
    let mut huge = u2.clone();
    huge.count = usize::MAX;
    assert!(decode(&[0; 4], &huge, 0).is_err());
    assert!(FixedRecords::new(vec![Arc::new(Bytes::Owned(vec![0; 4]))], vec![huge], 1).is_err());
    let mut far = u2;
    far.start = usize::MAX;
    let records = records(vec![0; 4], vec![far], usize::MAX);
    assert_eq!(records.rows(), 0);
}

/// A mapped file cut short after it was opened is refused at the next read, rather
/// than read past its new end. Windows refuses to cut a mapped file (os error 1224),
/// so there it cannot shrink.
#[test]
fn a_file_that_shrank_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("f.bin");
    std::fs::write(&path, [7u8; 64]).unwrap();
    let bytes = Arc::new(Bytes::map(&path).unwrap());
    let records = Arc::new(
        FixedRecords::new(
            vec![bytes],
            vec![column("a", 0, 1, Physical::Unsigned(1))],
            usize::MAX,
        )
        .unwrap(),
    );
    assert_eq!(records.collect(64).unwrap().height(), 64);
    let cut = std::fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .unwrap()
        .set_len(8);
    if cfg!(windows) {
        assert_eq!(cut.unwrap_err().raw_os_error(), Some(1224));
        assert_eq!(records.collect(64).unwrap().height(), 64);
        return;
    }
    cut.unwrap();
    let err = records.lazy().collect().unwrap_err();
    assert!(err.to_string().contains("shorter"), "{err}");
}

/// Records of (u4 id, s2 group, u1 flag): id `i`, group `i % 7 - 3`, flag `i % 2`.
fn numbered(rows: u32) -> Arc<FixedRecords> {
    let mut bytes = Vec::new();
    for i in 0..rows {
        bytes.extend(i.to_le_bytes());
        bytes.extend(((i % 7) as i16 - 3).to_le_bytes());
        bytes.push((i % 2) as u8);
    }
    records(
        bytes,
        vec![
            column("id", 0, 7, Physical::Unsigned(4)),
            column("group", 4, 7, Physical::Signed(2)),
            column("flag", 6, 7, Physical::Bool),
        ],
        usize::MAX,
    )
}

/// Filters, sorts and group-bys run on the streaming engine, as the app runs them
/// with streaming on, and agree with the in-memory engine.
#[test]
fn queries_stream_and_agree_with_the_in_memory_engine() {
    let lf = numbered(1_000).lazy();
    let queries = [
        lf.clone()
            .filter(col("flag").and(col("group").gt(lit(0i16))))
            .select([len()]),
        lf.clone()
            .sort(
                ["group", "id"],
                SortMultipleOptions::default().with_order_descending(true),
            )
            .limit(5),
        lf.clone()
            .group_by([col("group")])
            .agg([col("id").sum(), len()])
            .sort(["group"], Default::default()),
        lf.clone().slice(990, 50),
    ];
    for query in queries {
        let streamed = crate::analysis::statistics::collect_lazy(query.clone(), true).unwrap();
        let in_memory = query.collect().unwrap();
        assert!(
            streamed.equals_missing(&in_memory),
            "{streamed}\n{in_memory}"
        );
    }
    let groups = crate::analysis::statistics::collect_lazy(
        lf.group_by([col("group")])
            .agg([len()])
            .sort(["group"], Default::default()),
        true,
    )
    .unwrap();
    assert_eq!(groups.height(), 7);
    assert_eq!(
        groups.column("group").unwrap().i16().unwrap().get(0),
        Some(-3)
    );
}

/// A slice deep in the file decodes the rows asked for, the same as the window
/// that reads them straight, and a callback sink runs over the frame as the copy
/// and the chart counts ask.
#[test]
fn a_deep_slice_reads_its_own_rows() {
    let records = numbered(100_000);
    let window = records.window(99_990, 50).unwrap();
    assert_eq!(window.height(), 10);
    for streaming in [false, true] {
        let sliced =
            crate::analysis::statistics::collect_lazy(records.lazy().slice(99_990, 50), streaming)
                .unwrap();
        assert!(window.equals_missing(&sliced), "{sliced}");
    }
    assert_eq!(
        window.column("id").unwrap().u32().unwrap().get(0),
        Some(99_990)
    );
    let got = Arc::new(std::sync::Mutex::new(0usize));
    let seen = got.clone();
    let sink = records
        .lazy()
        .filter(col("flag"))
        .sink_batches(
            PlanCallback::new(move |batch: DataFrame| {
                *seen.lock().unwrap() += batch.height();
                Ok(false)
            }),
            true,
            None,
        )
        .unwrap();
    crate::analysis::statistics::collect_lazy(sink, true).unwrap();
    assert_eq!(*got.lock().unwrap(), 50_000);
}

/// A `scale` column is Decimal, whose single-key top-k the streaming engine of
/// Polars 0.55 cannot run (it panics); the first page of a sort by it still reads.
#[test]
fn a_sort_by_a_decimal_column_reads_its_first_page() {
    let mut price = column("price", 0, 4, Physical::Unsigned(4));
    price.logical = Logical::Decimal { scale: 2 };
    let bytes: Vec<u8> = (0u32..1_000).flat_map(|v| v.to_le_bytes()).collect();
    let lf = records(bytes, vec![price], usize::MAX).lazy();
    let page = crate::analysis::statistics::collect_lazy(
        lf.sort(
            ["price"],
            SortMultipleOptions::default().with_order_descending(true),
        )
        .slice(0, 3),
        true,
    )
    .unwrap();
    assert_eq!(
        page.column("price").unwrap().get(0).unwrap().to_string(),
        "9.99"
    );
}

/// The frame finds a column by its place, so two of one name are refused.
#[test]
fn two_columns_of_one_name_are_refused() {
    let bytes = Arc::new(Bytes::Owned(vec![0; 8]));
    let a = column("a", 0, 2, Physical::Unsigned(1));
    let Err(err) = FixedRecords::new(vec![bytes], vec![a.clone(), a], usize::MAX) else {
        panic!("two columns named a were taken");
    };
    assert!(err.to_string().contains("same name"), "{err}");
}

/// The decoder refuses an index past the bytes rather than read past them.
#[test]
fn decoding_rows_checks_the_index() {
    let u1 = column("u", 0, 1, Physical::Unsigned(1));
    let index = IdxCa::from_slice("i".into(), &[3, 0, 3]);
    let col = decode_rows(&[5, 6, 7, 8], &u1, &index).unwrap();
    assert_eq!(col.u8().unwrap().to_vec(), [Some(8), Some(5), Some(8)]);
    assert!(decode_rows(&[5, 6, 7], &u1, &index).is_err());
}

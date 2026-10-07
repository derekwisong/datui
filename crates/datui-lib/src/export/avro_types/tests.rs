use super::*;
use polars::io::avro::AvroReader;

fn written(lf: LazyFrame) -> Vec<u8> {
    let mut df = lazy_for_avro(lf).unwrap().collect().unwrap();
    let mut bytes = Vec::new();
    write(&mut df, &mut bytes).unwrap();
    bytes
}

fn round_trip(lf: LazyFrame) -> DataFrame {
    AvroReader::new(std::io::Cursor::new(written(lf)))
        .finish()
        .unwrap()
}

fn category() -> DataType {
    DataType::from_categories(Categories::global())
}

/// Every type the writer lacks comes back as the one it was cast to, with
/// its values; the types it has come back untouched.
#[test]
fn every_type_avro_lacks_is_written() {
    let lf = df!(
        "n" => [Some(3_600_000_000i64), None],
        "s" => [Some("a"), Some("b")],
    )
    .unwrap()
    .lazy()
    .select([
        col("n").alias("kept"),
        col("n").cast(DataType::Int16).alias("i16"),
        col("n").cast(DataType::UInt32).alias("u32"),
        col("n").cast(DataType::UInt64).alias("u64"),
        col("n").cast(DataType::Int128).alias("i128"),
        lit(327.68).cast(DataType::Decimal(10, 2)).alias("dec"),
        col("n")
            .cast(DataType::Datetime(TimeUnit::Nanoseconds, None))
            .alias("ns"),
        col("n")
            .cast(DataType::Datetime(
                TimeUnit::Microseconds,
                TimeZone::opt_try_new(Some("America/New_York")).unwrap(),
            ))
            .alias("zoned"),
        col("n").cast(DataType::Time).alias("time"),
        col("n")
            .cast(DataType::Duration(TimeUnit::Milliseconds))
            .alias("ms"),
        col("s").cast(category()).alias("cat"),
        lit(NULL).alias("null"),
        col("n")
            .fill_null(0)
            .implode(true)
            .cast(DataType::Array(Box::new(DataType::Int64), 2))
            .alias("arr"),
        col("s").cast(category()).implode(true).alias("cats"),
        as_struct(vec![
            col("s").cast(category()),
            col("n").cast(DataType::Time),
        ])
        .alias("point"),
    ]);
    let back = round_trip(lf);
    let dtype = |name: &str| back.column(name).unwrap().dtype().clone();
    let first = |name: &str| back.column(name).unwrap().get(0).unwrap().into_static();
    assert_eq!(dtype("kept"), DataType::Int64);
    assert_eq!(dtype("i16"), DataType::Int32);
    assert_eq!(dtype("u32"), DataType::Int64);
    assert_eq!(dtype("u64"), DataType::Int64);
    assert_eq!(first("i128"), AnyValue::StringOwned("3600000000".into()));
    assert_eq!(
        first("dec"),
        AnyValue::StringOwned("327.68".into()),
        "the writer's own decimal reads back as -327.68"
    );
    assert_eq!(
        first("ns"),
        AnyValue::Datetime(3_600_000, TimeUnit::Microseconds, None)
    );
    assert_eq!(
        first("zoned"),
        AnyValue::Datetime(3_600_000_000, TimeUnit::Microseconds, None),
        "the UTC instant, not the wall time in New York"
    );
    assert_eq!(first("time"), AnyValue::Int64(3_600_000), "microseconds");
    assert_eq!(
        first("ms"),
        AnyValue::Int64(3_600_000_000_000),
        "microseconds, whatever the unit"
    );
    assert_eq!(dtype("cat"), DataType::String);
    assert_eq!(first("cat"), AnyValue::StringOwned("a".into()));
    assert_eq!(dtype("null"), DataType::String);
    assert_eq!(dtype("arr"), DataType::List(Box::new(DataType::Int64)));
    assert_eq!(dtype("cats"), DataType::List(Box::new(DataType::String)));
    assert_eq!(
        dtype("point"),
        DataType::Struct(vec![
            Field::new("s".into(), DataType::String),
            Field::new("n".into(), DataType::Int64),
        ])
    );
}

/// A name Avro refuses is made valid; a valid one never moves, even when a
/// renamed one would land on it.
#[test]
fn names_are_made_valid_and_unique() {
    let names = avro_names(["my col", "2024", "a-b", "a_b", "délai", "", "_2024", "ok_1"]);
    assert_eq!(
        names,
        [
            "my_col", "_2024_2", "a_b_2", "a_b", "d_lai", "_", "_2024", "ok_1"
        ]
    );
    assert!(!renames("ok_1", &DataType::Int64));
    assert!(renames("my col", &DataType::Int64));
    let fields = |name: &str| {
        DataType::List(Box::new(DataType::Struct(vec![Field::new(
            name.into(),
            DataType::Int64,
        )])))
    };
    assert!(renames("ok", &fields("x y")));
    assert!(!renames("ok", &fields("x_y")));
}

/// Struct fields are renamed inside a list too, with their values and
/// nulls, and the columns under them.
#[test]
fn struct_fields_are_renamed_at_any_depth() {
    let lf = df!("n" => [Some(1i64), None, Some(3)])
        .unwrap()
        .lazy()
        .select([
            as_struct(vec![
                col("n").alias("x y"),
                (col("n") * lit(10)).alias("x-y"),
            ])
            .implode(true)
            .alias("my list"),
            when(col("n").is_null())
                .then(lit(NULL).cast(DataType::Struct(vec![Field::new(
                    "1st".into(),
                    DataType::Int64,
                )])))
                .otherwise(as_struct(vec![col("n").alias("1st")]))
                .alias("point"),
        ]);
    let back = round_trip(lf);
    assert_eq!(
        back.column("my_list").unwrap().dtype(),
        &DataType::List(Box::new(DataType::Struct(vec![
            Field::new("x_y".into(), DataType::Int64),
            Field::new("x_y_2".into(), DataType::Int64),
        ])))
    );
    let items = back
        .column("my_list")
        .unwrap()
        .list()
        .unwrap()
        .get_as_series(0)
        .unwrap();
    let field = |name: &str| {
        let values = items.struct_().unwrap().field_by_name(name).unwrap();
        values.i64().unwrap().iter().collect::<Vec<_>>()
    };
    assert_eq!(field("x_y"), [Some(1), None, Some(3)]);
    assert_eq!(field("x_y_2"), [Some(10), None, Some(30)]);
    let point = back.column("point").unwrap();
    assert_eq!(point.null_count(), 1, "{point:?}");
    let first = point.struct_().unwrap().field_by_name("_1st").unwrap();
    assert_eq!(first.i64().unwrap().get(2), Some(3));
}

/// A renamed field keeps its original name as its doc, nested ones too, and
/// a frame of several chunks is one header and a block for each.
#[test]
fn originals_are_docs_and_chunks_share_one_header() {
    let part = df!("my col" => [1i64], "ok" => [2i64])
        .unwrap()
        .lazy()
        .with_column(as_struct(vec![col("ok").alias("x y")]).alias("point"))
        .collect()
        .unwrap();
    let mut df = part.clone();
    df.vstack_mut(&part).unwrap();
    assert_eq!(df.first_col_n_chunks(), 2);
    let mut bytes = Vec::new();
    write(&mut df, &mut bytes).unwrap();

    let record = avro_schema::read::read_metadata(&mut std::io::Cursor::new(&bytes))
        .unwrap()
        .record;
    assert_eq!(record.name, RECORD_NAME);
    let docs = |fields: &[AvroField]| -> Vec<(String, Option<String>)> {
        fields
            .iter()
            .map(|f| (f.name.clone(), f.doc.clone()))
            .collect()
    };
    assert_eq!(
        docs(&record.fields),
        [
            ("my_col".to_string(), Some("my col".to_string())),
            ("ok".to_string(), None),
            ("point".to_string(), None),
        ]
    );
    let AvroSchema::Union(branches) = &record.fields[2].schema else {
        panic!("{:?}", record.fields[2].schema);
    };
    let AvroSchema::Record(point) = &branches[1] else {
        panic!("{branches:?}");
    };
    assert_eq!(
        docs(&point.fields),
        [("x_y".to_string(), Some("x y".to_string()))]
    );

    let back = AvroReader::new(std::io::Cursor::new(bytes))
        .finish()
        .unwrap();
    let my_col = back.column("my_col").unwrap().i64().unwrap();
    assert_eq!(my_col.iter().collect::<Vec<_>>(), [Some(1), Some(1)]);
}

/// Blocks are cut by size, not by chunk: a hundred one-row chunks are one
/// block, and one chunk of about 3 MiB is three, and every row reads back.
#[test]
fn blocks_are_cut_by_size() {
    use avro_schema::read::fallible_streaming_iterator::FallibleStreamingIterator;
    fn blocks(df: &mut DataFrame) -> Vec<(usize, usize)> {
        let mut bytes = Vec::new();
        write(df, &mut bytes).unwrap();
        let back = AvroReader::new(std::io::Cursor::new(&bytes))
            .finish()
            .unwrap();
        assert!(back.equals(df), "{back:?}");
        let mut reader = std::io::Cursor::new(bytes);
        let marker = avro_schema::read::read_metadata(&mut reader)
            .unwrap()
            .marker;
        let mut iter = avro_schema::read::block_iterator(reader, None, marker);
        let mut sizes = Vec::new();
        while let Some(block) = iter.next().unwrap() {
            sizes.push((block.number_of_rows, block.data.len()));
        }
        sizes
    }
    let text = "x".repeat(1000);
    let row = df!("s" => [text.as_str()]).unwrap();
    let mut many = row.clone();
    for _ in 0..99 {
        many.vstack_mut(&row).unwrap();
    }
    assert_eq!(many.first_col_n_chunks(), 100);
    let sizes = blocks(&mut many);
    assert_eq!(sizes.len(), 1, "{sizes:?}");
    assert_eq!(sizes[0].0, 100);

    let mut big = df!("s" => vec![text.as_str(); 3000]).unwrap();
    assert_eq!(big.first_col_n_chunks(), 1);
    let sizes = blocks(&mut big);
    assert_eq!(sizes.len(), 3, "{sizes:?}");
    assert_eq!(sizes.iter().map(|(rows, _)| rows).sum::<usize>(), 3000);
    for (_, size) in &sizes[..2] {
        assert!(
            (BLOCK_BYTES..BLOCK_BYTES + 1100).contains(size),
            "{sizes:?}"
        );
    }
}

/// A failed write reports the I/O error, not avro-schema's "OutOfSpec".
#[test]
fn a_write_error_says_what_failed() {
    struct Full;
    impl Write for Full {
        fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
            Err(std::io::Error::other("disk full"))
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut df = df!("n" => [1i64]).unwrap();
    let err = write(&mut df, Full).unwrap_err().to_string();
    assert!(err.contains("disk full"), "{err}");
}

/// A value that does not fit fails by column name rather than turning null.
#[test]
fn an_unsigned_value_past_i64_fails_by_name() {
    let lf = df!("big" => [u64::MAX]).unwrap().lazy();
    let err = lazy_for_avro(lf)
        .unwrap()
        .collect()
        .unwrap_err()
        .to_string();
    assert!(err.contains("'big'"), "{err}");
}

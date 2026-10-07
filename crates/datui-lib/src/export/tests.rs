use super::*;
use polars::prelude::*;
use std::io::Read;
use std::path::Path;

const COMPRESSIONS: [Option<CompressionFormat>; 5] = [
    None,
    Some(CompressionFormat::Gzip),
    Some(CompressionFormat::Zstd),
    Some(CompressionFormat::Bzip2),
    Some(CompressionFormat::Xz),
];

fn frame() -> DataFrame {
    df!(
        "id" => (0..5_000).collect::<Vec<i64>>(),
        "name" => (0..5_000).map(|i| format!("row {i}")).collect::<Vec<_>>(),
        "score" => (0..5_000).map(|i| (i % 7 == 0).then_some(i as f64 / 3.0)).collect::<Vec<_>>(),
    )
    .unwrap()
}

fn options(format: ExportFormat, compression: Option<CompressionFormat>) -> ExportOptions {
    let mut options = ExportOptions {
        csv_delimiter: b',',
        csv_include_header: true,
        source_file: false,
        csv_compression: None,
        json_compression: None,
        ndjson_compression: None,
    };
    match format {
        ExportFormat::Csv | ExportFormat::Tsv | ExportFormat::Psv => {
            options.csv_compression = compression
        }
        ExportFormat::Json => options.json_compression = compression,
        ExportFormat::Ndjson => options.ndjson_compression = compression,
        _ => assert!(compression.is_none()),
    }
    options
}

/// Every format, and every compression of those that have one.
fn combinations() -> Vec<(ExportFormat, Option<CompressionFormat>)> {
    ExportFormat::ALL
        .iter()
        .flat_map(|&format| {
            let compressions: &[Option<CompressionFormat>] = if format.supports_compression() {
                &COMPRESSIONS
            } else {
                &[None]
            };
            compressions.iter().map(move |&c| (format, c))
        })
        .collect()
}

fn decompress(bytes: Vec<u8>, compression: Option<CompressionFormat>) -> Vec<u8> {
    let mut out = Vec::new();
    match compression {
        None => return bytes,
        Some(CompressionFormat::Gzip) => flate2::read::GzDecoder::new(&bytes[..])
            .read_to_end(&mut out)
            .unwrap(),
        Some(CompressionFormat::Zstd) => zstd::Decoder::new(&bytes[..])
            .unwrap()
            .read_to_end(&mut out)
            .unwrap(),
        Some(CompressionFormat::Bzip2) => bzip2::read::BzDecoder::new(&bytes[..])
            .read_to_end(&mut out)
            .unwrap(),
        Some(CompressionFormat::Xz) => xz2::read::XzDecoder::new(&bytes[..])
            .read_to_end(&mut out)
            .unwrap(),
    };
    out
}

fn read_back(bytes: Vec<u8>, format: ExportFormat) -> DataFrame {
    let cursor = std::io::Cursor::new(bytes);
    match format {
        ExportFormat::Csv | ExportFormat::Tsv | ExportFormat::Psv => CsvReadOptions::default()
            .map_parse_options(|p| p.with_separator(format.preset_delimiter().unwrap_or(b',')))
            .into_reader_with_file_handle(cursor)
            .finish(),
        ExportFormat::Parquet => ParquetReader::new(cursor).finish(),
        ExportFormat::Json => JsonReader::new(cursor).finish(),
        ExportFormat::Ndjson => JsonReader::new(cursor)
            .with_json_format(JsonFormat::JsonLines)
            .finish(),
        ExportFormat::Ipc => IpcReader::new(cursor).finish(),
        ExportFormat::Avro => polars::io::avro::AvroReader::new(cursor).finish(),
    }
    .unwrap()
}

fn encoded(format: ExportFormat, compression: Option<CompressionFormat>) -> Vec<u8> {
    let mut bytes = Vec::new();
    encode(
        &mut frame(),
        format,
        &options(format, compression),
        &mut bytes,
    )
    .unwrap();
    bytes
}

/// A sink that takes `capacity` bytes and then fails, and can fail its flush.
struct Faulty {
    capacity: usize,
    written: usize,
    fail_flush: bool,
}

impl Faulty {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            written: 0,
            fail_flush: false,
        }
    }
}

impl Write for Faulty {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let room = self.capacity - self.written;
        if room == 0 {
            return Err(io::Error::other("injected write failure"));
        }
        let n = buf.len().min(room);
        self.written += n;
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.fail_flush {
            return Err(io::Error::other("injected flush failure"));
        }
        Ok(())
    }
}

fn encode_into(
    format: ExportFormat,
    compression: Option<CompressionFormat>,
    sink: &mut Faulty,
) -> Result<()> {
    encode(&mut frame(), format, &options(format, compression), sink)
}

/// Export `df` the way the app does, with the streaming engine on or off.
fn write(df: DataFrame, request: &ExportRequest, streaming: bool) -> Result<()> {
    run(df.lazy(), request, streaming, |_| {})
}

/// Through the file path the app takes, over a file agreed to be replaced, by
/// either engine and so by both routes.
#[test]
fn every_format_and_compression_round_trips() {
    let expected = frame();
    let dir = tempfile::tempdir().unwrap();
    for streaming in [true, false] {
        for (format, compression) in combinations() {
            let path = dir.path().join("out");
            std::fs::write(&path, b"old").unwrap();
            let request = ExportRequest {
                options: options(format, compression),
                ..request(&path, format, Overwrite::Replace)
            };
            let case = format!("{format:?} {compression:?} streaming={streaming}");
            write(frame(), &request, streaming).unwrap();
            assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
            let bytes = decompress(std::fs::read(&path).unwrap(), compression);
            let back = read_back(bytes, format);
            assert_eq!(back.shape(), expected.shape(), "{case}");
            assert_eq!(
                back.column("id").unwrap().cast(&DataType::Int64).unwrap(),
                *expected.column("id").unwrap(),
                "{case}"
            );
            assert_eq!(
                back.column("score").unwrap().null_count(),
                expected.column("score").unwrap().null_count(),
                "{case}"
            );
        }
    }
}

/// Fixed records export on the streaming route, decoded a batch at a time.
#[test]
fn fixed_records_export_with_streaming_asked_for() {
    use crate::fixed_records::{Bytes, ColumnLayout, FixedRecords, Physical};
    let records = || {
        let bytes = std::sync::Arc::new(Bytes::Owned((0u8..32).collect()));
        let column = ColumnLayout::new("a", 0, 4, Physical::Unsigned(4), 4);
        std::sync::Arc::new(FixedRecords::new(vec![bytes], vec![column], usize::MAX).unwrap())
            .lazy()
    };
    let dir = tempfile::tempdir().unwrap();
    for format in [ExportFormat::Parquet, ExportFormat::Csv] {
        let path = dir.path().join("out");
        let request = request(&path, format, Overwrite::Replace);
        let lf = records().filter(col("a").gt(lit(0x0302_0100u32)));
        run(lf, &request, true, |_| {}).unwrap();
        let back = read_back(std::fs::read(&path).unwrap(), format);
        assert_eq!(back.height(), 7, "{format:?}");
    }
}

/// A preset writes its own delimiter, whatever was typed for CSV, on both routes.
#[test]
fn presets_write_their_delimiter() {
    let dir = tempfile::tempdir().unwrap();
    for (format, separator) in [(ExportFormat::Tsv, '\t'), (ExportFormat::Psv, '|')] {
        for streaming in [false, true] {
            let path = dir.path().join("out");
            let request = request(&path, format, Overwrite::Replace);
            assert_eq!(request.options.csv_delimiter, b',');
            let lf = df!("a" => [1i64, 2], "b" => ["x", "y"]).unwrap().lazy();
            run(lf, &request, streaming, |_| {}).unwrap();
            let text = std::fs::read_to_string(&path).unwrap();
            assert_eq!(
                text,
                format!("a{separator}b\n1{separator}x\n2{separator}y\n"),
                "{format:?} streaming={streaming}"
            );
        }
    }
}

/// The serializer itself refuses: CSV has no list type unprepared.
#[test]
fn a_serializer_error_is_an_error() {
    for compression in COMPRESSIONS {
        assert!(
            encode(
                &mut nested(),
                ExportFormat::Csv,
                &options(ExportFormat::Csv, compression),
                io::sink(),
            )
            .is_err(),
            "{compression:?}"
        );
    }
}

/// The sink fails part way through the body.
#[test]
fn a_write_failure_part_way_is_an_error() {
    for (format, compression) in combinations() {
        let mut sink = Faulty::new(16);
        assert!(
            encode_into(format, compression, &mut sink).is_err(),
            "{format:?} {compression:?}"
        );
    }
}

/// One byte short: the last byte out is the end of the file. Uncompressed,
/// that is the buffer's final flush; compressed, the encoder's trailer,
/// written only by its finish. Both were lost at a drop before.
#[test]
fn a_failure_finishing_the_file_is_an_error() {
    for (format, compression) in combinations() {
        let size = encoded(format, compression).len();
        let mut sink = Faulty::new(size - 1);
        assert!(
            encode_into(format, compression, &mut sink).is_err(),
            "{format:?} {compression:?}"
        );
        let mut exact = Faulty::new(size);
        encode_into(format, compression, &mut exact)
            .unwrap_or_else(|e| panic!("{format:?} {compression:?} at its size: {e}"));
    }
}

#[test]
fn a_failed_final_flush_is_an_error() {
    for (format, compression) in combinations() {
        let mut sink = Faulty::new(usize::MAX);
        sink.fail_flush = true;
        assert!(
            encode_into(format, compression, &mut sink).is_err(),
            "{format:?} {compression:?}"
        );
    }
}

fn request(path: &Path, format: ExportFormat, overwrite: Overwrite) -> ExportRequest {
    ExportRequest {
        path: path.to_path_buf(),
        format,
        options: options(format, None),
        overwrite,
    }
}

/// A frame CSV cannot write unprepared: a list column.
fn nested() -> DataFrame {
    let mut df = df!("a" => [1i64, 2]).unwrap();
    df.with_column(Column::new(
        "list".into(),
        [
            Series::new("".into(), [1i64]),
            Series::new("".into(), [2i64]),
        ],
    ))
    .unwrap();
    df
}

/// Rows enough for three of the streaming engine's 100,000-row morsels.
const MANY: i64 = 300_000;

/// `MANY` ids, whose plan fails once it reaches id `at`: after the streamed
/// route has written the batches before it.
fn failing_at(at: i64) -> LazyFrame {
    df!("id" => (0..MANY).collect::<Vec<_>>())
        .unwrap()
        .lazy()
        .with_column(col("id").map(
            move |c| {
                if c.i64()?.max().is_some_and(|id| id >= at) {
                    polars_bail!(ComputeError: "injected plan failure");
                }
                Ok(c)
            },
            |_, field| Ok(field.clone()),
        ))
}

/// Every way an export can go, by the route each takes.
fn routes() -> Vec<(&'static str, ExportFormat, Option<CompressionFormat>, bool)> {
    vec![
        ("out.csv", ExportFormat::Csv, None, true),
        ("out.parquet", ExportFormat::Parquet, None, true),
        ("out.csv", ExportFormat::Csv, None, false),
        (
            "out.csv.gz",
            ExportFormat::Csv,
            Some(CompressionFormat::Gzip),
            true,
        ),
        ("out.json", ExportFormat::Json, None, true),
    ]
}

/// A failure part way through the plan, over an approved overwrite, leaves
/// the old file's bytes and mode and no temporary file; over a new file, no
/// file at all. By every route.
#[test]
fn a_failed_export_keeps_the_destination() {
    for (name, format, compression, streaming) in routes() {
        let case = format!("{name} streaming={streaming}");
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(name);
        std::fs::write(&path, b"old").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o604)).unwrap();
        }
        let replace = ExportRequest {
            options: options(format, compression),
            ..request(&path, format, Overwrite::Replace)
        };
        let err = run(failing_at(250_000), &replace, streaming, |_| {}).unwrap_err();
        assert!(format!("{err:?}").contains("injected"), "{case}: {err:?}");
        assert_eq!(std::fs::read(&path).unwrap(), b"old", "{case}");
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1, "{case}");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o604, "{case}");
        }

        let fresh = dir.path().join(format!("new-{name}"));
        let forbid = ExportRequest {
            path: fresh.clone(),
            overwrite: Overwrite::Forbid,
            ..replace
        };
        assert!(run(failing_at(250_000), &forbid, streaming, |_| {}).is_err());
        assert!(!fresh.exists(), "{case}: no partial file");
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1, "{case}");
    }
}

/// A panic in the plan part way through unwinds out of the export, which
/// the worker reports as a failure; the destination is as it was, and the
/// next export by the same route writes.
#[test]
fn a_panic_part_way_keeps_the_destination() {
    for (name, format, compression, streaming) in routes() {
        let case = format!("{name} streaming={streaming}");
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(name);
        std::fs::write(&path, b"old").unwrap();
        let lf = df!("id" => (0..MANY).collect::<Vec<_>>())
            .unwrap()
            .lazy()
            .with_column(col("id").map(
                |c| {
                    if c.i64()?.max().is_some_and(|id| id >= 250_000) {
                        panic!("injected panic");
                    }
                    Ok(c)
                },
                |_, field| Ok(field.clone()),
            ));
        let request = ExportRequest {
            options: options(format, compression),
            ..request(&path, format, Overwrite::Replace)
        };
        let ended = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            run(lf, &request, streaming, |_| {})
        }));
        assert!(!matches!(ended, Ok(Ok(()))), "{case}");
        assert_eq!(std::fs::read(&path).unwrap(), b"old", "{case}");
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1, "{case}");

        run(frame().lazy(), &request, streaming, |_| {})
            .unwrap_or_else(|e| panic!("{case}: the next export: {e}"));
        let bytes = decompress(std::fs::read(&path).unwrap(), compression);
        assert_eq!(
            read_back(bytes, format).height(),
            frame().height(),
            "{case}"
        );
    }
}

#[test]
fn a_written_export_replaces_the_destination() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("out.parquet");
    std::fs::write(&path, b"old").unwrap();
    write(
        frame(),
        &request(&path, ExportFormat::Parquet, Overwrite::Replace),
        true,
    )
    .unwrap();
    let back = read_back(std::fs::read(&path).unwrap(), ExportFormat::Parquet);
    assert_eq!(back.shape(), frame().shape());
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
}

/// Uncompressed CSV (and its presets) and Parquet stream when the engine is on;
/// every other format and compression, and every export with it off, is collected.
#[test]
fn only_uncompressed_csv_and_parquet_stream() {
    for (format, compression) in combinations() {
        for streaming in [true, false] {
            let request = ExportRequest {
                options: options(format, compression),
                ..request(Path::new("out"), format, Overwrite::Forbid)
            };
            let streams = cfg!(feature = "streaming")
                && streaming
                && compression.is_none()
                && (format.is_delimited() || format == ExportFormat::Parquet);
            assert_eq!(
                request.route(streaming) == Route::Streamed,
                streams,
                "{format:?} {compression:?} streaming={streaming}"
            );
        }
    }
}

/// A view with what a CSV has to get right: nulls, quotes, separators and
/// line breaks inside text, floats, dates, datetimes, booleans, and the list,
/// binary and duration columns [`ExportFormat::prepare`] turns into text.
fn awkward() -> DataFrame {
    let n = 2_000;
    let mut df = df!(
            "id" => (0..n).collect::<Vec<i64>>(),
            "text" => (0..n).map(|i| match i % 5 {
                0 => None,
                1 => Some("plain".to_string()),
                2 => Some(format!("a, \"quoted\" {i}")),
                3 => Some("semi;colon\ttab".to_string()),
                _ => Some(format!("two\nlines {i}")),
            }).collect::<Vec<_>>(),
            "x" => (0..n).map(|i| (i % 3 != 0).then_some(i as f64 / 7.0)).collect::<Vec<_>>(),
            "flag" => (0..n).map(|i| (i % 4 != 0).then_some(i % 2 == 0)).collect::<Vec<_>>(),
            "day" => (0..n).map(|i| i as i32).collect::<Vec<_>>(),
            "at" => (0..n).map(|i| i * 3_600_000).collect::<Vec<i64>>(),
            "took" => (0..n).map(|i| (i % 7 != 0).then_some((i - 1_000) * 1_234_567)).collect::<Vec<_>>(),
        )
        .unwrap();
    df.apply("took", |c| {
        c.cast(&DataType::Duration(TimeUnit::Microseconds)).unwrap()
    })
    .unwrap();
    df.apply("day", |c| c.cast(&DataType::Date).unwrap())
        .unwrap();
    df.apply("at", |c| {
        c.cast(&DataType::Datetime(TimeUnit::Milliseconds, None))
            .unwrap()
    })
    .unwrap();
    // Datetimes in the other units and a zone, as the CSV writer's text, and
    // one past the calendar in a single batch, as its stored number.
    let paris = TimeZone::opt_try_new(Some("Europe/Paris")).unwrap();
    let stamps: Vec<Option<i64>> = (0..n)
        .map(|i| match i {
            1_500 => Some(i64::MIN + 1),
            i if i % 9 == 0 => None,
            i => Some(i * 3_600_000_123),
        })
        .collect();
    for (name, dtype) in [
        (
            "at_us_tz",
            DataType::Datetime(TimeUnit::Microseconds, paris),
        ),
        ("at_ns", DataType::Datetime(TimeUnit::Nanoseconds, None)),
    ] {
        let column = Series::new(name.into(), &stamps).cast(&dtype).unwrap();
        df.with_column(column.into_column()).unwrap();
    }
    let tags: Vec<Option<Series>> = (0..n)
        .map(|i| (i % 6 != 0).then(|| Series::new("".into(), [format!("t{i}"), "x,y".into()])))
        .collect();
    df.with_column(Column::new("tags".into(), tags)).unwrap();
    let raw: Vec<Option<Vec<u8>>> = (0..n)
        .map(|i| (i % 5 != 0).then(|| vec![0, 0xff, i as u8]))
        .collect();
    df.with_column(Column::new("raw".into(), raw)).unwrap();
    df
}

/// The views an export is asked for: as loaded, filtered, sorted with its
/// columns reordered, a query's text of its dates, and filtered to nothing.
fn views() -> Vec<(&'static str, LazyFrame)> {
    let lf = awkward().lazy();
    vec![
        ("as loaded", lf.clone()),
        ("filtered", lf.clone().filter(col("x").gt(lit(100.0)))),
        (
            "sorted and reordered",
            lf.clone()
                .sort(
                    ["text"],
                    SortMultipleOptions::default().with_nulls_last(true),
                )
                .select([
                    col("x"),
                    col("tags"),
                    col("took"),
                    col("text"),
                    col("id"),
                    col("at"),
                    col("at_us_tz"),
                ]),
        ),
        (
            "a query's text",
            lf.clone().select([
                col("id"),
                crate::past_calendar::guard_expr(col("at_us_tz").cast(DataType::String), None),
                crate::past_calendar::guard_expr(
                    col("at").dt().to_string("%Y").alias("year"),
                    None,
                ),
            ]),
        ),
        ("empty", lf.filter(lit(false))),
    ]
}

fn exported(lf: LazyFrame, request: &ExportRequest, streaming: bool) -> Vec<u8> {
    run(lf, request, streaming, |_| {}).unwrap();
    std::fs::read(&request.path).unwrap()
}

/// The streamed CSV is the collected CSV, byte for byte, under each delimiter
/// and header choice.
#[test]
fn a_streamed_csv_is_the_collected_csv() {
    let dir = tempfile::tempdir().unwrap();
    for (view, lf) in views() {
        for (delimiter, header) in [(b',', true), (b';', false), (b'\t', true)] {
            let mut request = request(
                &dir.path().join("out.csv"),
                ExportFormat::Csv,
                Overwrite::Replace,
            );
            request.options.csv_delimiter = delimiter;
            request.options.csv_include_header = header;
            let streamed = exported(lf.clone(), &request, true);
            let collected = exported(lf.clone(), &request, false);
            let case = format!("{view}, {:?}, header={header}", delimiter as char);
            assert_eq!(
                String::from_utf8_lossy(&streamed),
                String::from_utf8_lossy(&collected),
                "{case}"
            );
            if view == "empty" && header {
                assert!(
                    !streamed.is_empty(),
                    "{case}: an empty view still has its header"
                );
            }
            if view == "as loaded" || view == "a query's text" {
                let text = String::from_utf8_lossy(&streamed);
                assert!(
                    text.contains("-9223372036854775807 us since 1970-01-01 UTC"),
                    "{case}"
                );
            }
        }
    }
}

/// Durations reach a CSV as ISO 8601 by every route, the text a JSON export
/// of the same view holds; a null is an empty field.
#[test]
fn durations_export_as_iso_8601_by_every_route() {
    use crate::nested_json::tests::{duration_text, durations};
    let rows = duration_text()[0].1.len();
    let mut expected = String::from("ms,us,ns\n");
    for row in 0..rows {
        let cells: Vec<&str> = duration_text()
            .iter()
            .map(|(_, text)| text[row].unwrap_or(""))
            .collect();
        expected.push_str(&cells.join(","));
        expected.push('\n');
    }

    let dir = tempfile::tempdir().unwrap();
    for (name, compression, streaming) in [
        ("streamed.csv", None, true),
        ("collected.csv", None, false),
        ("compressed.csv.gz", Some(CompressionFormat::Gzip), true),
    ] {
        let request = ExportRequest {
            options: options(ExportFormat::Csv, compression),
            ..request(&dir.path().join(name), ExportFormat::Csv, Overwrite::Forbid)
        };
        let bytes = decompress(
            exported(durations().lazy(), &request, streaming),
            compression,
        );
        assert_eq!(String::from_utf8(bytes).unwrap(), expected, "{name}");
    }

    let request = request(
        &dir.path().join("out.json"),
        ExportFormat::Json,
        Overwrite::Forbid,
    );
    let back = read_back(
        exported(durations().lazy(), &request, false),
        ExportFormat::Json,
    );
    for (name, text) in duration_text() {
        let json = back.column(name).unwrap().str().unwrap();
        assert_eq!(json.iter().collect::<Vec<_>>(), text, "{name}");
    }
}

/// The Parquet file's own description of its columns: the column types
/// another reader sees, and the Arrow schema Polars and pyarrow read back.
fn parquet_schema(bytes: &[u8]) -> (String, Option<String>) {
    let meta =
        polars_parquet::parquet::read::read_metadata(&mut std::io::Cursor::new(bytes)).unwrap();
    let arrow = meta
        .key_value_metadata
        .iter()
        .flatten()
        .find(|kv| kv.key == "ARROW:schema")
        .and_then(|kv| kv.value.clone());
    (format!("{:?}", meta.schema_descr.columns()), arrow)
}

/// The streamed Parquet is the collected one as a reader sees it: the same
/// column types and Arrow schema, and the same rows in the same order.
#[test]
fn a_streamed_parquet_reads_as_the_collected_one() {
    let dir = tempfile::tempdir().unwrap();
    let request = request(
        &dir.path().join("out.parquet"),
        ExportFormat::Parquet,
        Overwrite::Replace,
    );
    for (view, lf) in views() {
        let streamed = exported(lf.clone(), &request, true);
        let collected = exported(lf.clone(), &request, false);
        assert_eq!(
            parquet_schema(&streamed),
            parquet_schema(&collected),
            "{view}"
        );
        let streamed = read_back(streamed, ExportFormat::Parquet);
        let collected = read_back(collected, ExportFormat::Parquet);
        assert_eq!(streamed.schema(), collected.schema(), "{view}");
        assert!(streamed.equals_missing(&collected), "{view}");
        assert_eq!(streamed.height(), lf.collect().unwrap().height(), "{view}");
    }
}

/// The streamed route never builds a frame of the whole output: no batch the
/// plan hands on is more than a morsel. The collected route, by contrast, has
/// every row in one frame, which is what shows the probe can see it.
#[test]
fn a_streamed_export_never_holds_the_whole_output() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    let dir = tempfile::tempdir().unwrap();
    for format in [ExportFormat::Csv, ExportFormat::Parquet] {
        for streaming in [true, false] {
            let tallest = Arc::new(AtomicUsize::new(0));
            let probe = tallest.clone();
            let lf = df!("id" => (0..MANY).collect::<Vec<_>>())
                .unwrap()
                .lazy()
                .with_column(col("id").map(
                    move |c| {
                        probe.fetch_max(c.len(), Ordering::Relaxed);
                        Ok(c)
                    },
                    |_, field| Ok(field.clone()),
                ));
            let request = request(&dir.path().join("out"), format, Overwrite::Replace);
            let back = read_back(exported(lf, &request, streaming), format);
            assert_eq!(back.height(), MANY as usize);
            let tallest = tallest.load(Ordering::Relaxed);
            if request.route(streaming) == Route::Streamed {
                assert!(
                    tallest < MANY as usize,
                    "{format:?}: a batch of {tallest} rows"
                );
            } else {
                assert_eq!(tallest, MANY as usize, "{format:?}");
            }
        }
    }
}

/// `written` hears 0 as the write starts, then counts that only grow and
/// never pass the file's size.
#[test]
fn an_export_reports_what_it_has_written() {
    use std::sync::{Arc, Mutex};
    let dir = tempfile::tempdir().unwrap();
    for streaming in [true, false] {
        let heard = Arc::new(Mutex::new(Vec::new()));
        let log = heard.clone();
        let request = request(
            &dir.path().join("out.csv"),
            ExportFormat::Csv,
            Overwrite::Replace,
        );
        let lf = df!("id" => (0..MANY).collect::<Vec<_>>()).unwrap().lazy();
        run(lf, &request, streaming, move |bytes| {
            log.lock().unwrap().push(bytes)
        })
        .unwrap();
        let size = std::fs::metadata(&request.path).unwrap().len();
        let heard = heard.lock().unwrap();
        assert_eq!(heard.first(), Some(&0), "streaming={streaming}");
        assert!(heard.windows(2).all(|w| w[0] <= w[1]), "{heard:?}");
        assert!(heard.iter().all(|&b| b <= size), "{heard:?} of {size}");
    }
}

/// The streamed route's writer boundary: a write that fails part way and a
/// failed close are the sink's errors, and the file at its exact size is not.
#[cfg(feature = "streaming")]
mod sink {
    use super::*;
    use polars::io::utils::file::{Writable, WritableTrait};

    impl WritableTrait for Faulty {
        fn close(&mut self) -> io::Result<()> {
            self.flush()
        }

        fn sync_all(&self) -> io::Result<()> {
            Ok(())
        }

        fn sync_data(&self) -> io::Result<()> {
            Ok(())
        }
    }

    fn sink_into(format: ExportFormat, faulty: Faulty) -> Result<()> {
        let lf = format.prepare(frame().lazy()).unwrap();
        sink(
            lf,
            format,
            &options(format, None),
            Writable::Dyn(Box::new(faulty)),
        )
    }

    /// What the sink writes in all.
    fn size(format: ExportFormat) -> usize {
        use std::sync::{Arc, Mutex};
        #[derive(Clone, Default)]
        struct Tally(Arc<Mutex<usize>>);
        impl Write for Tally {
            fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
                *self.0.lock().unwrap() += buf.len();
                Ok(buf.len())
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        impl WritableTrait for Tally {
            fn close(&mut self) -> io::Result<()> {
                Ok(())
            }
            fn sync_all(&self) -> io::Result<()> {
                Ok(())
            }
            fn sync_data(&self) -> io::Result<()> {
                Ok(())
            }
        }
        let tally = Tally::default();
        let lf = format.prepare(frame().lazy()).unwrap();
        sink(
            lf,
            format,
            &options(format, None),
            Writable::Dyn(Box::new(tally.clone())),
        )
        .unwrap();
        *tally.0.lock().unwrap()
    }

    #[test]
    fn a_write_failure_part_way_is_an_error() {
        for format in [ExportFormat::Csv, ExportFormat::Parquet] {
            assert!(sink_into(format, Faulty::new(16)).is_err(), "{format:?}");
            let size = size(format);
            assert!(
                sink_into(format, Faulty::new(size - 1)).is_err(),
                "{format:?} last byte"
            );
            sink_into(format, Faulty::new(size))
                .unwrap_or_else(|e| panic!("{format:?} at its size: {e}"));
        }
    }

    #[test]
    fn a_failed_close_is_an_error() {
        for format in [ExportFormat::Csv, ExportFormat::Parquet] {
            let mut faulty = Faulty::new(usize::MAX);
            faulty.fail_flush = true;
            assert!(sink_into(format, faulty).is_err(), "{format:?}");
        }
    }
}

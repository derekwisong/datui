use ::polars::prelude::*;

use super::csv::{self, StringTypes};
use super::*;
use crate::ParseStringsTarget;

fn piped(head: &[u8]) -> Option<FileFormat> {
    sniff(head, None, Asked::Pipe, |_| true)
}

/// Text formats are known by their first bytes, and text no format claims is none.
#[test]
fn text_formats_by_their_first_bytes() {
    let said = [
        (
            &b"8=FIX.4.4\x019=5\x0135=0\x0110=000\x01\n"[..],
            FileFormat::Fix,
        ),
        (
            b"$timescale 1ns $end\n$scope module top $end\n",
            FileFormat::Vcd,
        ),
        (
            b"aspirin\n  RDKit\n\n  0  0  0  0  0  0  0  0  0  0999 V2000\nM  END\n$$$$\n",
            FileFormat::Sdf,
        ),
        (b"$GPGGA,1,2", FileFormat::Nmea),
        (b"<?xml version=\"1.0\"?>\n<gpx>", FileFormat::Gpx),
    ];
    for (head, format) in said {
        assert_eq!(piped(head), Some(format), "{format:?}");
    }
    assert_eq!(piped(b"a,b\n1,2\n"), None);
}

/// A file read through its compression is named under it and known by its bytes
/// inside it; a name that says a format needs no look.
#[test]
fn a_compressed_file_by_its_name_or_what_it_holds() {
    use std::io::Write;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("session.log.gz");
    let mut gz =
        flate2::write::GzEncoder::new(std::fs::File::create(&path).unwrap(), Default::default());
    gz.write_all(b"20260101-00:00:00 : 8=FIX.4.2|9=5|35=0|10=000|\n")
        .unwrap();
    gz.finish().unwrap();
    assert_eq!(sniff_open(&path, None), Some(FileFormat::Fix));
    for (name, format) in [
        ("lib.sdf.gz", Some(FileFormat::Sdf)),
        ("a.nmea.gz", Some(FileFormat::Nmea)),
        ("a.csv.gz", None),
    ] {
        assert_eq!(sniff_open(&dir.path().join(name), None), format, "{name}");
    }
}

/// Signatures are believed where they say: an executable is never listed, ORC's
/// three letters are never piped, and only a file of tables is one.
#[test]
fn a_signature_is_believed_where_it_says() {
    let elf = b"\x7fELF\x02\x01\x01\0\0\0\0\0\0\0\0\0";
    assert_eq!(piped(elf), Some(FileFormat::Elf));
    assert_eq!(sniff(elf, None, Asked::Listing, |_| true), None);
    assert_eq!(
        sniff(elf, None, Asked::Tables, |_| true),
        Some(FileFormat::Elf)
    );
    assert_eq!(piped(b"ORC\x00"), None);
    assert_eq!(
        sniff(b"ORC\x00", None, Asked::Listing, |_| true),
        Some(FileFormat::Orc)
    );
    let npy = b"\x93NUMPY\x01\x00";
    assert_eq!(piped(npy), Some(FileFormat::Numpy));
    assert_eq!(sniff(npy, None, Asked::Tables, |_| true), None);
}

/// The copying page's reader table names every format by its title and the Polars
/// call Copy as Python reads it with, or `df = ...` where it has none.
#[test]
fn the_copy_docs_name_each_format_s_reader() {
    let page =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs/user-guide/copying.md");
    let text = std::fs::read_to_string(&page).expect("the copying page");
    let start = text.find("| Format | Reader |").expect("the reader table");
    let rows: Vec<(String, String)> = text[start..]
        .lines()
        .skip(2)
        .take_while(|l| l.starts_with('|'))
        .map(|l| {
            let (format, reader) = l.trim_matches('|').split_once(" | ").expect("two cells");
            (format.trim().to_string(), reader.trim().to_string())
        })
        .collect();
    let said: Vec<&str> = rows.iter().map(|(f, _)| f.as_str()).collect();
    let titles: Vec<&str> = FileFormat::ALL.iter().map(|f| f.title()).collect();
    assert_eq!(said, titles, "one row per format, in --format's order");
    for ((title, reader), format) in rows.iter().zip(FileFormat::ALL) {
        let expected = of(format)
            .python
            .as_ref()
            .map_or("`df = ...`".to_string(), |p| format!("`{}`", p.call));
        assert_eq!(*reader, expected, "{title}");
    }
}

/// A reader does what its descriptor says: a format whose tables are listed has a
/// way to list them, and only such a format does; one read into files of its own
/// converts; a prefix is scanned in place only where the descriptor says it is.
#[test]
fn readers_agree_with_their_descriptors() {
    for format in FileFormat::ALL {
        let reader = of(format);
        assert_eq!(
            reader.tables.is_some(),
            format.holds_tables(),
            "{}",
            format.name()
        );
        // A format read into files of its own has a conversion to do it.
        if format.reads_into() {
            assert!(reader.convert.is_some(), "{}", format.name());
        }
        // A tab the facts fill is one the descriptor names.
        if reader.facts.is_some() {
            assert!(format.summary_tab().is_some(), "{}", format.name());
        }
        #[cfg(feature = "cloud")]
        if reader.bucket_scan.is_some() {
            assert!(format.reads_bucket_prefix(), "{}", format.name());
        }
    }
}

fn names(df: &DataFrame) -> Vec<String> {
    df.get_column_names()
        .iter()
        .map(|name| name.to_string())
        .collect()
}

fn mixed_strings() -> LazyFrame {
    df!(
        "id" => &[1i64, 2, 3, 4],
        "amount" => &[" 10 ", "20", "", " 40"],
        "day" => &["2024-01-01", "2024-01-02", " ", "2024-01-04"],
        "at" => &["2024-01-01T10:00:00Z", "2024-01-01T11:00:00Z", "", "2024-01-01T12:00:00Z"],
        "score" => &[0.5f64, 1.5, 2.5, 3.5],
        "word" => &["a", " b", "c ", ""],
        "empty" => &[None::<&str>, None, None, None],
    )
    .unwrap()
    .lazy()
}

/// String inference reads only the columns it types, and reads them exactly as
/// the whole-frame sample it replaced did: trimmed, blanks null, same rows.
#[test]
fn the_inference_sample_holds_only_its_targets() {
    let targets: Vec<String> = ["amount", "day", "at", "word", "empty"]
        .map(String::from)
        .to_vec();
    let sample = csv::string_inference_sample(mixed_strings(), &targets, 3).unwrap();
    assert_eq!(names(&sample), ["amount", "day", "at", "word", "empty"]);
    assert_eq!(sample.height(), 3);

    // The sample as it was taken before: every column, the targets normalized.
    let blank = lit(PlSmallStr::from_static(""));
    let wide = mixed_strings()
        .limit(3)
        .with_columns(
            targets
                .iter()
                .map(|c| {
                    col(c.as_str())
                        .str()
                        .strip_chars(lit(PlSmallStr::from_static(" \t\n\r")))
                })
                .collect::<Vec<_>>(),
        )
        .with_columns(
            targets
                .iter()
                .map(|c| {
                    when(col(c.as_str()).eq(blank.clone()))
                        .then(Null {}.lit())
                        .otherwise(col(c.as_str()))
                        .alias(c.as_str())
                })
                .collect::<Vec<_>>(),
        )
        .collect()
        .unwrap();
    assert_eq!(wide.width(), 7);
    assert!(sample.equals_missing(&wide.select(targets.iter().map(String::as_str)).unwrap()));

    let one = csv::string_inference_sample(mixed_strings(), &["day".to_string()], 1_000).unwrap();
    assert_eq!(names(&one), ["day"]);
    assert_eq!(one.height(), 4);
}

/// The frame string inference returns keeps every column in its place, and types
/// the targets as before: numbers, dates, timestamps, text left as text, an
/// all-null column left alone.
#[test]
fn string_inference_keeps_the_whole_frame() {
    let utc = DataType::Datetime(TimeUnit::Microseconds, Some(TimeZone::UTC));
    let typed = |target: &ParseStringsTarget, types: StringTypes| {
        csv::type_string_columns(
            mixed_strings(),
            target,
            1_000,
            types,
            &mut Vec::new(),
            &[],
            &mut Vec::new(),
        )
        .unwrap()
        .collect()
        .unwrap()
    };
    let all = StringTypes {
        dates: true,
        numbers: true,
    };

    let df = typed(&ParseStringsTarget::All, all);
    let schema = df.schema();
    let order = names(&df);
    assert_eq!(
        order,
        ["id", "amount", "day", "at", "score", "word", "empty"]
    );
    assert_eq!(schema.get("id"), Some(&DataType::Int64));
    assert_eq!(schema.get("amount"), Some(&DataType::Int64));
    assert_eq!(schema.get("day"), Some(&DataType::Date));
    assert_eq!(schema.get("at"), Some(&utc));
    assert_eq!(schema.get("score"), Some(&DataType::Float64));
    assert_eq!(schema.get("word"), Some(&DataType::String));
    assert_eq!(schema.get("empty"), Some(&DataType::String));
    let amount: Vec<Option<i64>> = df.column("amount").unwrap().i64().unwrap().iter().collect();
    assert_eq!(amount, [Some(10), Some(20), None, Some(40)]);
    assert_eq!(df.column("day").unwrap().null_count(), 1);
    assert_eq!(df.column("at").unwrap().null_count(), 1);
    let word: Vec<Option<&str>> = df.column("word").unwrap().str().unwrap().iter().collect();
    assert_eq!(word, [Some("a"), Some("b"), Some("c"), Some("")]);
    let score: Vec<Option<f64>> = df.column("score").unwrap().f64().unwrap().iter().collect();
    assert_eq!(score, [Some(0.5), Some(1.5), Some(2.5), Some(3.5)]);

    // Named columns: only those that are text are typed; the rest are as read.
    let some = typed(
        &ParseStringsTarget::Columns(vec!["day".into(), "id".into(), "missing".into()]),
        all,
    );
    assert_eq!(names(&some), order);
    assert_eq!(some.schema().get("day"), Some(&DataType::Date));
    assert_eq!(some.schema().get("amount"), Some(&DataType::String));
    assert_eq!(
        some.column("amount").unwrap().str().unwrap().get(0),
        Some(" 10 ")
    );

    // Nothing to type: the frame comes back as it was.
    let none = typed(&ParseStringsTarget::Columns(vec!["id".into()]), all);
    assert!(none.equals_missing(&mixed_strings().collect().unwrap()));

    // JSON: dates only. Numbers in strings stay text, untrimmed.
    let json = super::polars::apply_parse_dates_to_json_lazyframe(
        mixed_strings(),
        &crate::OpenOptions::default(),
        &mut Vec::new(),
    )
    .unwrap()
    .collect()
    .unwrap();
    assert_eq!(names(&json), order);
    assert_eq!(json.schema().get("day"), Some(&DataType::Date));
    assert_eq!(json.schema().get("at"), Some(&utc));
    assert_eq!(json.schema().get("amount"), Some(&DataType::String));
    assert_eq!(
        json.column("word").unwrap().str().unwrap().get(1),
        Some(" b")
    );
}

#[test]
fn test_from_parquet() {
    // Ensure sample data is generated before running test
    let path = crate::tests::sample_data_dir().join("people.parquet");
    let schema = super::polars::parquet(&path)
        .unwrap()
        .collect_schema()
        .unwrap();
    assert!(!schema.is_empty());
}

#[test]
fn test_from_ipc() {
    use ::polars::prelude::IpcWriter;
    use std::io::BufWriter;
    let mut df = df!(
        "x" => &[1_i32, 2, 3],
        "y" => &["a", "b", "c"]
    )
    .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("datui_test_ipc.arrow");
    let file = std::fs::File::create(&path).unwrap();
    let mut writer = BufWriter::new(file);
    IpcWriter::new(&mut writer).finish(&mut df).unwrap();
    drop(writer);
    let schema = super::polars::ipc(&path).unwrap().collect_schema().unwrap();
    assert_eq!(schema.len(), 2);
    assert!(schema.contains("x"));
    assert!(schema.contains("y"));
}

#[test]
fn test_from_avro() {
    use ::polars::io::avro::AvroWriter;
    use std::io::BufWriter;
    let mut df = df!(
        "id" => &[1_i32, 2, 3],
        "name" => &["alice", "bob", "carol"]
    )
    .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("datui_test_avro.avro");
    let file = std::fs::File::create(&path).unwrap();
    let mut writer = BufWriter::new(file);
    AvroWriter::new(&mut writer).finish(&mut df).unwrap();
    drop(writer);
    let schema = super::polars::avro(&path)
        .unwrap()
        .collect_schema()
        .unwrap();
    assert_eq!(schema.len(), 2);
    assert!(schema.contains("id"));
    assert!(schema.contains("name"));
}

#[test]
fn test_from_orc() {
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use orc_rust::ArrowWriterBuilder;
    use std::io::BufWriter;
    use std::sync::Arc;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let id_array = Arc::new(Int64Array::from(vec![1_i64, 2, 3]));
    let name_array = Arc::new(StringArray::from(vec!["a", "b", "c"]));
    let batch = RecordBatch::try_new(schema.clone(), vec![id_array, name_array]).unwrap();

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("datui_test_orc.orc");
    let file = std::fs::File::create(&path).unwrap();
    let writer = BufWriter::new(file);
    let mut orc_writer = ArrowWriterBuilder::new(writer, schema).try_build().unwrap();
    orc_writer.write(&batch).unwrap();
    orc_writer.close().unwrap();

    let schema = super::polars::orc(&path).unwrap().collect_schema().unwrap();
    assert_eq!(schema.len(), 2);
    assert!(schema.contains("id"));
    assert!(schema.contains("name"));
}

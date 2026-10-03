use crate::*;
use polars::prelude::*;
use std::io::Write;

/// What an open of `path` reads it as, by the route the scan takes, beside whether
/// the frame it built holds its rows in memory.
fn opened(path: &Path) -> (Option<crate::ReadMode>, Option<bool>) {
    let options = OpenOptions::default();
    let mut report = ReadReport::default();
    let scan = App::build_local_lazyframe(
        &[path.to_path_buf()],
        &options,
        &mut report,
        &crate::formats::Registry::default(),
    )
    .unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    let format = scan.format(report.format.or(options.format));
    let mode = scan.read_mode(format, report.format_read.is_some(), &options);
    // A frame of rows already in memory is a `DF` node in the plan. Audio's frame
    // is one too: an empty frame of its row count, mapped to the file's samples.
    let in_memory = match &scan {
        Scan::Frame(lf) if report.audio.is_none() => {
            Some(lf.describe_plan().unwrap().contains("DF ["))
        }
        _ => None,
    };
    (mode, in_memory)
}

/// Streams, once converted, are scanned again as the IPC file they became; the open
/// still reads them converted. IPC files read in place beside nothing else are lazy.
#[test]
fn a_converted_stream_scanned_again_is_converted() {
    use crate::ipc_stream::Part;
    let scan = Scan::from(df!("a" => [1i64]).unwrap().lazy());
    let with = |parts: Vec<Part>| OpenOptions {
        arrow_parts: Some(Arc::new(parts)),
        ..OpenOptions::default()
    };
    let converted = with(vec![
        Part::InPlace(PathBuf::from("a.arrow")),
        Part::Converted {
            source: PathBuf::from("s.arrow"),
            offset: 0,
            rows: 1,
        },
    ]);
    let arrow = Some(FileFormat::Arrow);
    assert_eq!(
        scan.read_mode(arrow, false, &converted),
        Some(crate::ReadMode::Converted)
    );
    let in_place = with(vec![Part::InPlace(PathBuf::from("s3://b/a.arrow"))]);
    assert_eq!(
        scan.read_mode(arrow, false, &in_place),
        Some(crate::ReadMode::Lazy)
    );
}

/// Each reader does what `FileFormat::read_mode` says of its format: a frame of
/// rows in memory for the in-memory formats and never for the lazy ones, and a
/// copy for the converted ones. Excel and ORC are left out for want of a writer
/// here; both readers collect the whole sheet or file into a frame.
#[test]
fn every_reader_reads_as_its_format_says() {
    use crate::{ReadMode, Stored};
    let dir = tempfile::tempdir().unwrap();
    let mut df = df!("a" => [1i64, 2, 3], "b" => ["x", "y", "z"]).unwrap();
    let write = |name: &str, bytes: &[u8]| {
        let path = dir.path().join(name);
        std::fs::write(&path, bytes).unwrap();
        path
    };
    let mut files: Vec<(PathBuf, FileFormat, Stored)> = Vec::new();

    let path = dir.path().join("t.parquet");
    ParquetWriter::new(std::fs::File::create(&path).unwrap())
        .finish(&mut df)
        .unwrap();
    files.push((path, FileFormat::Parquet, Stored::Plain));
    let path = dir.path().join("t.arrow");
    IpcWriter::new(std::fs::File::create(&path).unwrap())
        .finish(&mut df)
        .unwrap();
    files.push((path, FileFormat::Arrow, Stored::Plain));
    let path = dir.path().join("t.avro");
    polars::io::avro::AvroWriter::new(std::fs::File::create(&path).unwrap())
        .finish(&mut df)
        .unwrap();
    files.push((path, FileFormat::Avro, Stored::Plain));
    let stream = crate::ipc_stream::tests::stream(&df, None, false);
    files.push((
        write("stream.arrow", &stream),
        FileFormat::Arrow,
        Stored::Stream,
    ));
    files.push((
        write("t.csv", b"a,b\n1,x\n2,y\n"),
        FileFormat::Csv,
        Stored::Plain,
    ));
    files.push((
        write("t.tsv", b"a\tb\n1\tx\n"),
        FileFormat::Tsv,
        Stored::Plain,
    ));
    files.push((
        write("t.psv", b"a|b\n1|x\n"),
        FileFormat::Psv,
        Stored::Plain,
    ));
    files.push((
        write("t.json", br#"[{"a": 1, "b": "x"}]"#),
        FileFormat::Json,
        Stored::Plain,
    ));
    files.push((
        write("t.jsonl", b"{\"a\": 1}\n{\"a\": 2}\n"),
        FileFormat::Jsonl,
        Stored::Plain,
    ));
    let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    gz.write_all(b"a,b\n1,x\n").unwrap();
    files.push((
        write("t.csv.gz", &gz.finish().unwrap()),
        FileFormat::Csv,
        Stored::Compressed { in_memory: false },
    ));
    files.push((
        write(
            "t.nmea",
            b"$GPGGA,123519,4807.038,N,01131.000,E,1,08,0.9,545.4,M,46.9,M,,*47\r\n",
        ),
        FileFormat::Nmea,
        Stored::Plain,
    ));
    files.push((
        write(
            "t.gpx",
            br#"<?xml version="1.0"?><gpx version="1.1"><wpt lat="1" lon="2"></wpt></gpx>"#,
        ),
        FileFormat::Gpx,
        Stored::Plain,
    ));
    let header = r#"{"w":{"dtype":"F32","shape":[1],"data_offsets":[0,4]}}"#;
    files.push((
        write(
            "t.safetensors",
            &crate::model_files::tests::safetensors_bytes(header, 4),
        ),
        FileFormat::Safetensors,
        Stored::Plain,
    ));
    let mut wav = b"RIFF\0\0\0\0WAVEfmt \x10\0\0\0".to_vec();
    // PCM, one channel, 8 kHz, 16 bits; then two frames.
    for field in [
        &1u16.to_le_bytes()[..],
        &1u16.to_le_bytes(),
        &8000u32.to_le_bytes(),
    ] {
        wav.extend_from_slice(field);
    }
    for field in [
        &16000u32.to_le_bytes()[..],
        &2u16.to_le_bytes(),
        &16u16.to_le_bytes(),
    ] {
        wav.extend_from_slice(field);
    }
    wav.extend_from_slice(b"data\x04\0\0\0\x01\0\x02\0");
    let size = (wav.len() - 8) as u32;
    wav[4..8].copy_from_slice(&size.to_le_bytes());
    files.push((write("t.wav", &wav), FileFormat::Audio, Stored::Plain));
    // One note on and off, then the end of the track.
    let track = b"\0\x90\x3c\x40\x10\x80\x3c\0\0\xff\x2f\0";
    let midi = crate::midi::tests::smf(0, 96, &[track]);
    files.push((write("t.mid", &midi), FileFormat::Midi, Stored::Plain));
    #[cfg(feature = "sqlite")]
    {
        let path = dir.path().join("t.db");
        rusqlite::Connection::open(&path)
            .unwrap()
            .execute_batch("CREATE TABLE t (a INTEGER, b TEXT); INSERT INTO t VALUES (1, 'x');")
            .unwrap();
        files.push((path, FileFormat::Sqlite, Stored::Plain));
    }

    for (path, format, stored) in files {
        let expected = format.read_mode(stored);
        let (mode, in_memory) = opened(&path);
        assert_eq!(mode, expected, "{}", path.display());
        if let Some(in_memory) = in_memory {
            assert_eq!(
                in_memory,
                expected == Some(ReadMode::InMemory),
                "{}: a frame in memory is what the format says",
                path.display()
            );
        } else {
            assert_ne!(
                expected,
                Some(ReadMode::InMemory),
                "{}: read through a copy",
                path.display()
            );
        }
    }
}

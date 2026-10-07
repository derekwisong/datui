use super::*;
use crate::unfinished::Unfinished;
use polars::prelude::*;
use polars_arrow::io::ipc::write::{Compression, StreamWriter};
use std::sync::atomic::AtomicBool;

fn frame(from: i64, n: i64) -> DataFrame {
    df!(
        "id" => (from..from + n).collect::<Vec<_>>(),
        "text" => (from..from + n).map(|i| format!("row {i}")).collect::<Vec<_>>(),
    )
    .unwrap()
}

/// `df` as a stream of batches of three rows, as pyarrow and Hugging Face write it.
pub(crate) fn stream(df: &DataFrame, compression: Option<Compression>, legacy: bool) -> Vec<u8> {
    let mut out = Vec::new();
    let mut writer = StreamWriter::new(&mut out, WriteOptions { compression });
    writer
        .start(&df.schema().to_arrow(CompatLevel::newest()), None)
        .unwrap();
    for at in (0..df.height()).step_by(3) {
        let part = df.slice(at as i64, 3);
        for batch in part.iter_chunks(CompatLevel::newest(), false) {
            writer.write(&batch, None).unwrap();
        }
    }
    writer.finish().unwrap();
    if legacy { strip_markers(&out) } else { out }
}

/// The same stream without the continuation markers, as Arrow wrote before 0.15.
fn strip_markers(stream: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut at = 0;
    while at < stream.len() {
        assert_eq!(stream[at..at + 4], CONTINUATION);
        let length = i32::from_le_bytes(stream[at + 4..at + 8].try_into().unwrap());
        out.extend_from_slice(&stream[at + 4..at + 8]);
        at += 8;
        if length == 0 {
            break;
        }
        let message = &stream[at..at + length as usize];
        let body = MessageRef::read_as_root(message)
            .unwrap()
            .body_length()
            .unwrap() as usize;
        out.extend_from_slice(&stream[at..at + length as usize + body]);
        at += length as usize + body;
    }
    out
}

fn writer(stop: bool) -> Writer {
    Unfinished::default().writer(Arc::new(AtomicBool::new(stop)))
}

fn rows(path: &Path) -> DataFrame {
    LazyFrame::scan_ipc(
        PlRefPath::try_from_path(path).unwrap(),
        Default::default(),
        Default::default(),
    )
    .unwrap()
    .collect()
    .unwrap()
}

#[test]
fn a_stream_is_told_by_its_first_message() {
    let df = frame(0, 5);
    let marked = stream(&df, None, false);
    let legacy = stream(&df, None, true);
    assert!(is_stream_head(&marked));
    assert!(is_stream_head(&legacy));
    assert!(is_stream_head(&marked[..8]), "the marker and a length");
    assert!(
        !is_stream_head(&legacy[..8]),
        "no marker, and too little to parse"
    );
    let mut file = Vec::new();
    polars::io::ipc::IpcWriter::new(&mut file)
        .finish(&mut df.clone())
        .unwrap();
    for not in [
        &file[..],
        b"id,text\n0,a\n",
        b"\xff\xff\xff\xff\x00\x00\x00\x00",
        b"\xff\xff\xff\xff\xff\xff\xff\x7f",
        b"\x10\x00\x00\x00garbage garbage garbage",
        b"",
    ] {
        assert!(!is_stream_head(not), "{not:?}");
    }
}

/// A schema message too long to read whole at a glance is still told by its bytes,
/// with or without the marker; a file that only starts with a small number, as
/// many binary files do, is read no further than its front.
#[test]
fn a_long_schema_is_read_but_a_lookalike_is_not() {
    let names: Vec<String> = (0..3000)
        .map(|i| format!("a_long_column_name_{i:05}"))
        .collect();
    let df = DataFrame::new(
        1,
        names
            .iter()
            .map(|n| Column::new(n.as_str().into(), [1i32]))
            .collect(),
    )
    .unwrap();
    for legacy in [false, true] {
        let bytes = stream(&df, None, legacy);
        let at = if legacy { 0 } else { 4 };
        let length = i32::from_le_bytes(bytes[at..at + 4].try_into().unwrap()) as usize;
        assert!(length > SCHEMA_PREFIX, "{length}");
        assert!(is_stream(&bytes[..]), "legacy: {legacy}");
    }

    struct Counted<'a>(&'a [u8], usize);
    impl Read for Counted<'_> {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            let n = self.0.read(buf)?;
            self.1 += n;
            Ok(n)
        }
    }
    let mut lookalike = vec![7u8; 12 << 20];
    lookalike[..4].copy_from_slice(&(10i32 << 20).to_le_bytes());
    let mut source = Counted(&lookalike, 0);
    assert!(!is_stream(&mut source));
    assert!(source.1 <= 4 + SCHEMA_PREFIX, "read {}", source.1);
}

/// Streams of every kind, compressed or not, with or without the markers, become
/// one IPC file holding their rows in order; the bytes read are counted.
#[test]
fn streams_convert_to_one_ipc_file() {
    let dir = tempfile::tempdir().unwrap();
    let kinds = [
        (None, false),
        (Some(Compression::LZ4), false),
        (Some(Compression::ZSTD(Default::default())), false),
        (None, true),
    ];
    let mut paths = Vec::new();
    for (i, (compression, legacy)) in kinds.into_iter().enumerate() {
        let path = dir.path().join(format!("data-{i:05}-of-00004.arrow"));
        std::fs::write(
            &path,
            stream(&frame(i as i64 * 10, 10), compression, legacy),
        )
        .unwrap();
        assert!(is_stream_file(&path), "{path:?}");
        paths.push(path);
    }
    let out = tempfile::tempdir().unwrap();
    let read = AtomicU64::new(0);
    let converted = convert(&paths, Some(out.path()), &writer(false), &read).unwrap();
    let file = converted.file;
    assert!(!is_stream_file(file.path()), "an IPC file now");
    assert_eq!(
        converted.parts[1],
        Part::Converted {
            source: paths[1].clone(),
            offset: 10,
            rows: 10
        }
    );
    let df = rows(file.path());
    assert_eq!(df.height(), 40);
    assert_eq!(
        df.column("id").unwrap().i64().unwrap().get(25),
        Some(25),
        "in order"
    );
    let total: u64 = paths
        .iter()
        .map(|p| std::fs::metadata(p).unwrap().len())
        .sum();
    assert_eq!(read.load(Ordering::Relaxed), total);
    let path = file.path().to_path_buf();
    drop(file);
    assert!(!path.exists());
}

/// A stopped open writes nothing and leaves nothing; a shard with other columns,
/// and a stream that is not one, are refused by name.
#[test]
fn a_stop_or_a_bad_stream_leaves_no_file() {
    let dir = tempfile::tempdir().unwrap();
    let good = dir.path().join("a.arrow");
    std::fs::write(&good, stream(&frame(0, 9), None, false)).unwrap();
    let out = tempfile::tempdir().unwrap();
    let empty = || std::fs::read_dir(out.path()).unwrap().next().is_none();
    let read = AtomicU64::new(0);

    let stopped = convert(
        std::slice::from_ref(&good),
        Some(out.path()),
        &writer(true),
        &read,
    );
    assert!(stopped.is_err());
    assert!(empty());

    let other = dir.path().join("b.arrow");
    let df = df!("x" => [1.5f64]).unwrap();
    std::fs::write(&other, stream(&df, None, false)).unwrap();
    let error = convert(
        &[good.clone(), other.clone()],
        Some(out.path()),
        &writer(false),
        &read,
    )
    .unwrap_err()
    .to_string();
    assert!(
        error.contains("b.arrow has different columns from"),
        "{error}"
    );
    assert!(empty());

    let cut = dir.path().join("cut.arrow");
    let bytes = stream(&frame(0, 9), None, false);
    std::fs::write(&cut, &bytes[..bytes.len() / 2]).unwrap();
    let error = convert(&[cut], Some(out.path()), &writer(false), &read)
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("cut.arrow\": Not a readable Arrow IPC stream"),
        "{error}"
    );
    assert!(empty());
}

/// A stream with no name to go on is Arrow by its bytes, to discovery and to a pipe.
#[test]
fn discovery_and_a_pipe_know_a_stream_by_its_bytes() {
    let dir = tempfile::tempdir().unwrap();
    for (name, legacy) in [("part-0", false), ("part-1", true)] {
        let bytes = stream(&frame(0, 4), None, legacy);
        let path = dir.path().join(name);
        std::fs::write(&path, &bytes).unwrap();
        assert_eq!(
            crate::discover::sniff_format(&path),
            Some(crate::FileFormat::Arrow),
            "{name}"
        );
        assert_eq!(
            crate::stdin::sniff(&bytes),
            (crate::FileFormat::Arrow, None),
            "{name}"
        );
    }
}

/// A column type Polars has not implemented makes a stream it cannot read, told
/// as one by its bytes and refused by the conversion rather than panicking it.
#[test]
fn a_stream_polars_cannot_read_is_refused() {
    let bytes =
        include_bytes!("../../../../../fuzz/corpus/ipc_stream_head/regression-run-end-encoded");
    assert!(is_stream_head(bytes));
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ree.arrow");
    std::fs::write(&path, bytes).unwrap();
    let error = convert(
        &[path],
        Some(dir.path()),
        &writer(false),
        &AtomicU64::new(0),
    )
    .unwrap_err()
    .to_string();
    assert!(
        error.contains("a column type Polars cannot read"),
        "{error}"
    );
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
}

/// A record batch Polars panics on, rather than erring, is a stream it cannot read,
/// refused by name with nothing left behind.
#[test]
fn a_damaged_record_batch_is_refused() {
    let df = df!(
        "id" => (0..50i64).collect::<Vec<_>>(),
        "t" => (0..50).map(|i| format!("r{i}")).collect::<Vec<_>>(),
    )
    .unwrap();
    let mut bytes = Vec::new();
    let mut out = StreamWriter::new(&mut bytes, WriteOptions { compression: None });
    out.start(&df.schema().to_arrow(CompatLevel::newest()), None)
        .unwrap();
    for batch in df.iter_chunks(CompatLevel::newest(), false) {
        out.write(&batch, None).unwrap();
    }
    out.finish().unwrap();
    // In the record batch's message, so that Polars cannot read its length, which
    // it unwraps (found by mutating this stream).
    assert_eq!(bytes[251], 0);
    bytes[251] = 0x21;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("damaged.arrow");
    std::fs::write(&path, &bytes).unwrap();
    let out = tempfile::tempdir().unwrap();
    let error = convert(
        &[path],
        Some(out.path()),
        &writer(false),
        &AtomicU64::new(0),
    )
    .unwrap_err()
    .to_string();
    assert!(
        error.contains("damaged.arrow\": Not a readable Arrow IPC stream"),
        "{error}"
    );
    assert!(std::fs::read_dir(out.path()).unwrap().next().is_none());
}

/// A temp directory with less free than the streams refuses the conversion before
/// writing, and one that fills up says where, both with what to do.
#[test]
fn a_full_temp_directory_says_so() {
    let dir = Path::new("/scratch");
    assert!(room(10, Some(10), dir).is_ok());
    assert!(room(10, None, dir).is_ok(), "free space unknown");
    let error = room(2 << 30, Some(1 << 30), dir).unwrap_err().to_string();
    assert!(
        error.contains("needs 2.0 GiB free in /scratch, which has 1.0 GiB"),
        "{error}"
    );
    assert!(error.contains("--temp-dir"), "{error}");

    let full =
        polars::error::PolarsError::from(std::io::Error::from(std::io::ErrorKind::StorageFull));
    let error = out_of_room(full.into(), dir).to_string();
    assert!(error.starts_with("/scratch ran out of space"), "{error}");
    assert!(error.contains("--temp-dir"), "{error}");
    let other = out_of_room(eyre!("something else"), dir).to_string();
    assert_eq!(other, "something else");
}

/// Only the first file says whether a list of Arrow files is converted. The
/// conversion copies only the streams; an IPC file among them keeps its place and
/// is read where it is.
#[test]
fn only_the_streams_among_ipc_files_are_converted() {
    let dir = tempfile::tempdir().unwrap();
    let streamed = dir.path().join("s.arrow");
    std::fs::write(&streamed, stream(&frame(0, 3), None, false)).unwrap();
    let file = dir.path().join("f.arrow");
    polars::io::ipc::IpcWriter::new(std::fs::File::create(&file).unwrap())
        .finish(&mut frame(3, 4))
        .unwrap();
    assert!(!starts_with_stream(std::slice::from_ref(&file)));
    assert!(starts_with_stream(&[streamed.clone(), file.clone()]));
    assert!(!starts_with_stream(&[file.clone(), streamed.clone()]));
    assert!(any_stream(&[file.clone(), streamed.clone()]));
    assert!(!any_stream(std::slice::from_ref(&file)));
    let out = tempfile::tempdir().unwrap();
    let converted_part = Part::Converted {
        source: streamed.clone(),
        offset: 0,
        rows: 3,
    };
    for (paths, parts) in [
        (
            [streamed.clone(), file.clone()],
            [converted_part.clone(), Part::InPlace(file.clone())],
        ),
        (
            [file.clone(), streamed.clone()],
            [Part::InPlace(file.clone()), converted_part.clone()],
        ),
    ] {
        let read = AtomicU64::new(0);
        let converted = convert(&paths, Some(out.path()), &writer(false), &read).unwrap();
        assert_eq!(converted.parts, parts);
        let df = rows(converted.file.path());
        assert_eq!(df.height(), 3, "the stream's rows only");
        let total: u64 = paths
            .iter()
            .map(|p| std::fs::metadata(p).unwrap().len())
            .sum();
        assert_eq!(read.load(Ordering::Relaxed), total);
    }
}

use super::*;
use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

fn files_in(dir: &Path) -> usize {
    std::fs::read_dir(dir).unwrap().count()
}

fn options_in(dir: &Path) -> OpenOptions {
    OpenOptions {
        temp_dir: Some(dir.to_path_buf()),
        ..Default::default()
    }
}

/// Each format by its first bytes; whitespace and a byte-order mark before JSON
/// are not the data's first character.
#[test]
fn the_first_bytes_say_the_format() {
    let cases: [(&[u8], FileFormat, Option<CompressionFormat>); 37] = [
        (b"PAR1\x15\x04", FileFormat::Parquet, None),
        (b"\x93NUMPY\x01\x00", FileFormat::Numpy, None),
        (b"\x7fELF\x02\x01\x01", FileFormat::Elf, None),
        (b"ULog\x01\x12\x35\x01", FileFormat::Ulog, None),
        (b"(1.000100) can0 123#DEADBEEF\n", FileFormat::Candump, None),
        (b"\xa3\x95\x80\x80\x59FMT\0", FileFormat::Dataflash, None),
        (b"SQLite format 3\0\x10\x00", FileFormat::Sqlite, None),
        (b"8=FIX.4.4|9=5|35=0|10=000|\n", FileFormat::Fix, None),
        (b"$version Verilator $end\n", FileFormat::Vcd, None),
        (
            b"aspirin\n\n\n  1  0  0  0  0  0  0  0  0  0999 V2000\n",
            FileFormat::Sdf,
            None,
        ),
        (b"GGUF\x03\x00\x00\x00", FileFormat::Gguf, None),
        (b"MThd\0\0\0\x06\0\x01", FileFormat::Midi, None),
        (
            b"\x02\x00\x00\x00\x00\x00\x00\x00{}",
            FileFormat::Safetensors,
            None,
        ),
        (b"$GPGGA,123519,4807.038,N", FileFormat::Nmea, None),
        // A CSV header with the shape of a sentence.
        (b"$USD,$EUR\n1,2\n", FileFormat::Csv, None),
        (
            b"<?xml version=\"1.0\"?>\n<gpx version=\"1.1\">",
            FileFormat::Gpx,
            None,
        ),
        (b"RIFF\x24\x00\x00\x00WAVEfmt ", FileFormat::Audio, None),
        (b"FORM\x00\x00\x00\x2eAIFFCOMM", FileFormat::Audio, None),
        (b"ARROW1\x00\x00", FileFormat::Arrow, None),
        // An Arrow IPC stream, cut short of its schema message.
        (b"\xff\xff\xff\xff\x10\x01\x00\x00", FileFormat::Arrow, None),
        (b"Obj\x01\x04", FileFormat::Avro, None),
        (
            b"\x1f\x8b\x08\x00",
            FileFormat::Text,
            Some(CompressionFormat::Gzip),
        ),
        (
            b"\x28\xb5\x2f\xfd\x04",
            FileFormat::Text,
            Some(CompressionFormat::Zstd),
        ),
        (b"BZh91AY", FileFormat::Text, Some(CompressionFormat::Bzip2)),
        (
            b"\xfd7zXZ\x00\x00",
            FileFormat::Text,
            Some(CompressionFormat::Xz),
        ),
        (b"  \n[{\"a\": 1}]", FileFormat::Json, None),
        (b"\xef\xbb\xbf{\"a\": 1}\n", FileFormat::Jsonl, None),
        (b"a,b\n1,2\n", FileFormat::Csv, None),
        // One field a line, a log, a header alone: lines.
        (b"1\n2\n3\n", FileFormat::Text, None),
        (
            b"Oct  3 12:00:01 host sshd[1]: Accepted\n",
            FileFormat::Text,
            None,
        ),
        (b"id,name\n", FileFormat::Text, None),
        // A pretty-printed object, as `curl` gets from an API, is not one per line.
        (b"{\n  \"a\": 1\n}\n", FileFormat::Json, None),
        (b"{\"a\": 1}  \r\n{\"a\": 2}", FileFormat::Jsonl, None),
        (b"{\"a\": 1}", FileFormat::Jsonl, None),
        (b"id\tname\n1\tx\n", FileFormat::Tsv, None),
        (b"id\tname,first\n", FileFormat::Text, None),
        (b"id,name\n1,a\tb\n", FileFormat::Csv, None),
    ];
    for (head, format, compression) in cases {
        assert_eq!(sniff(head), (format, compression), "{head:?}");
    }
}

/// A terminal is the user and a device (`/dev/null`, `NUL`) holds nothing; only
/// a pipe or a file is read (#567).
#[test]
fn only_a_pipe_or_a_file_carries_data() {
    assert!(carries_data(false, false), "a pipe or a file");
    assert!(!carries_data(true, false), "a terminal");
    assert!(!carries_data(false, true), "/dev/null or NUL");
    assert!(!carries_data(true, true), "a console");
}

/// `-` is standard input wherever it is named, alone; no paths and a pipe on
/// standard input select it, and a terminal there does not.
#[test]
fn stdin_is_chosen_by_a_dash_or_a_pipe() {
    assert_eq!(paths_or_stdin(Vec::new(), true), vec![PathBuf::from("-")]);
    assert!(paths_or_stdin(Vec::new(), false).is_empty());
    let listed = vec![PathBuf::from("a.csv")];
    assert_eq!(paths_or_stdin(listed.clone(), true), listed);

    assert_eq!(refuse(&listed, false), None);
    assert_eq!(refuse(&[PathBuf::from("-")], true), None);
    assert!(refuse(&[PathBuf::from("-")], false).is_some());
    assert!(refuse(&[PathBuf::from("-"), PathBuf::from("a.csv")], true).is_some());
    assert_eq!(named(Path::new("-")), PathBuf::from("stdin"));
    assert_eq!(named(Path::new("./-")), PathBuf::from("./-"));
    assert!(!is_stdin(&as_file(PathBuf::from("-"))));
    assert_eq!(as_file(PathBuf::from("a.csv")), PathBuf::from("a.csv"));
}

/// Spooled whole, counted, and its format read off the file; what the user named
/// wins over the bytes.
#[test]
fn a_reader_is_spooled_whole_and_its_format_read() {
    let dir = tempfile::tempdir().unwrap();
    let read = AtomicU64::new(0);
    let body = b"[{\"a\": 1}]".to_vec();
    let (file, options) = spool(
        move || Ok((std::io::Cursor::new(body), None)),
        options_in(dir.path()),
        &Writer::default(),
        &read,
    )
    .unwrap();
    assert_eq!(std::fs::read(file.path()).unwrap(), b"[{\"a\": 1}]");
    assert_eq!(read.load(Ordering::Relaxed), 10);
    assert_eq!(options.format, Some(FileFormat::Json));
    assert_eq!(options.compression, None);

    let named = OpenOptions {
        compression: Some(CompressionFormat::Zstd),
        ..options_in(dir.path())
    };
    let (_, options) = spool(
        || Ok((std::io::Cursor::new(b"\x1f\x8b".to_vec()), None)),
        named,
        &Writer::default(),
        &AtomicU64::new(0),
    )
    .unwrap();
    // Not zstd after all: nothing inside says more than lines.
    assert_eq!(options.format, Some(FileFormat::Text));
    assert_eq!(options.compression, Some(CompressionFormat::Zstd));

    // A delimited format named alone: its compression still comes from the bytes.
    let named = OpenOptions {
        format: Some(FileFormat::Tsv),
        ..options_in(dir.path())
    };
    let (_, options) = spool(
        || Ok((std::io::Cursor::new(b"\x1f\x8b".to_vec()), None)),
        named,
        &Writer::default(),
        &AtomicU64::new(0),
    )
    .unwrap();
    assert_eq!(options.format, Some(FileFormat::Tsv));
    assert_eq!(options.compression, Some(CompressionFormat::Gzip));

    // Compressed and unnamed: what is inside says CSV on evidence, lines otherwise.
    for (body, format) in [
        (&b"id,name\n1,a\n2,b\n"[..], FileFormat::Csv),
        (b"started\n\nstopped, after 2s\n", FileFormat::Text),
    ] {
        let mut gz = flate2::write::GzEncoder::new(Vec::new(), Default::default());
        gz.write_all(body).unwrap();
        let gz = gz.finish().unwrap();
        let (_, options) = spool(
            move || Ok((std::io::Cursor::new(gz), None)),
            options_in(dir.path()),
            &Writer::default(),
            &AtomicU64::new(0),
        )
        .unwrap();
        assert_eq!(options.format, Some(format));
        assert_eq!(options.compression, Some(CompressionFormat::Gzip));
        assert!(options.format_guessed);
    }

    let empty = spool(
        || Ok((std::io::empty(), None)),
        options_in(dir.path()),
        &Writer::default(),
        &AtomicU64::new(0),
    );
    assert!(empty.is_err(), "nothing piped in is said");
}

/// A producer that goes quiet mid-stream: what came is counted, and stopping the
/// open ends the read and removes the partial file, though the pipe stays open.
#[test]
fn a_stop_mid_spool_removes_the_partial_file() {
    let dir = tempfile::tempdir().unwrap();
    let (reader, mut pipe) = std::io::pipe().unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let writer = crate::unfinished::Unfinished::default().writer(stop.clone());
    let read = Arc::new(AtomicU64::new(0));
    let worker = {
        let (writer, read) = (writer.clone(), read.clone());
        let options = options_in(dir.path());
        std::thread::spawn(move || spool(move || Ok((reader, None)), options, &writer, &read))
    };
    pipe.write_all(b"a,b\n1,2\n").unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    while read.load(Ordering::Relaxed) < 8 {
        assert!(
            std::time::Instant::now() < deadline,
            "the bytes never landed"
        );
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    assert_eq!(files_in(dir.path()), 1, "the partial file is there");
    stop.store(true, Ordering::Relaxed);
    let answer = worker.join().unwrap();
    assert!(answer.is_err(), "a stopped read is not a dataset");
    assert_eq!(files_in(dir.path()), 0, "and its file is gone");
    drop(pipe);
}

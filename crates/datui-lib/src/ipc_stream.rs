//! Arrow IPC streams: the format Hugging Face `datasets` writes its cache in.
//!
//! A stream is the IPC file format without the `ARROW1` magic and the footer that says
//! where each record batch is, so Polars cannot scan it. An open converts the stream,
//! or every shard of a directory of them, once into one IPC file in the temp
//! directory and scans that, holding one record batch at a time: 0.2 GiB at its peak
//! for a 3.0 GiB stream. Read eagerly instead, that stream held 3.6 GiB for as long as
//! it was open, and the same rows with ZSTD buffers, 1.3 GiB on disk, the same 3.6 GiB.

use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use color_eyre::{Result, eyre::eyre};
use polars_arrow::io::ipc::format::ipc::planus::ReadAsRoot;
use polars_arrow::io::ipc::format::ipc::{MessageHeaderRef, MessageRef};
use polars_arrow::io::ipc::read::{StreamReader, StreamState, read_stream_metadata};
use polars_arrow::io::ipc::write::{FileWriter, WriteOptions};

use crate::download::TempDownload;
use crate::unfinished::Writer;

/// What a stream's messages start with since Arrow 0.15. Older streams start with the
/// schema message's length.
const CONTINUATION: [u8; 4] = [0xff; 4];

/// The longest schema message read to tell a stream from other bytes. A schema with
/// thousands of columns and its metadata fits well inside.
const MAX_SCHEMA: usize = 16 << 20;

/// Whether `head`, the first bytes of a file, begins an Arrow IPC stream: a schema
/// message, after the continuation marker or, in a stream older than it, without.
///
/// Where `head` holds the whole message it has to be a schema message. Where it is cut
/// short, the marker followed by a length is taken as a stream; an older stream, with
/// no marker to go on, is not. The message's fields are not read: Polars panics on a
/// column type it has not implemented, and this runs on any file being opened.
pub fn is_stream_head(head: &[u8]) -> bool {
    let marked = head.starts_with(&CONTINUATION);
    let rest = if marked { &head[4..] } else { head };
    let Some(length) = rest.get(..4) else {
        return false;
    };
    let length = i32::from_le_bytes([length[0], length[1], length[2], length[3]]);
    let Ok(length) = usize::try_from(length) else {
        return false;
    };
    if length == 0 || length > MAX_SCHEMA {
        return false;
    }
    let Some(message) = rest.get(4..4 + length) else {
        return marked;
    };
    begins_schema(message)
}

/// How much of a long schema message is read to see that it begins like one before the
/// rest is: the schema table is near the front, its fields and metadata after it.
const SCHEMA_PREFIX: usize = 64 << 10;

/// Whether the file at `path` is an Arrow IPC stream, by its contents.
pub fn is_stream_file(path: &Path) -> bool {
    File::open(path).is_ok_and(is_stream)
}

/// Whether `source` holds an Arrow IPC stream. A file that only happens to start with
/// a small number, as many binary files do, is read no further than [`SCHEMA_PREFIX`].
fn is_stream(mut source: impl Read) -> bool {
    let mut head = Vec::new();
    // The marker and the length, then the message the length names.
    if (&mut source).take(8).read_to_end(&mut head).is_err() {
        return false;
    }
    let at = if head.starts_with(&CONTINUATION) {
        4
    } else {
        0
    };
    let Some(length) = head
        .get(at..at + 4)
        .map(|b| i32::from_le_bytes([b[0], b[1], b[2], b[3]]))
        .and_then(|l| usize::try_from(l).ok())
        .filter(|l| (1..=MAX_SCHEMA).contains(l))
    else {
        return false;
    };
    let mut read_to = |end: usize, head: &mut Vec<u8>| {
        let more = end.saturating_sub(head.len()) as u64;
        (&mut source).take(more).read_to_end(head).is_ok()
    };
    let start = at + 4;
    if length > SCHEMA_PREFIX
        && !(read_to(start + SCHEMA_PREFIX, &mut head) && begins_schema(&head[start..]))
    {
        return false;
    }
    read_to(start + length, &mut head) && is_stream_head(&head)
}

/// Whether `message`, all or the front of one, is a schema message as far as it goes.
fn begins_schema(message: &[u8]) -> bool {
    matches!(
        MessageRef::read_as_root(message).and_then(|m| m.header()),
        Ok(Some(MessageHeaderRef::Schema(_)))
    )
}

/// The Arrow `paths` that are streams, when they all are, or the reason a mix of
/// streams and IPC files cannot be read as one table. `None` when none is a stream.
pub fn streams_among(paths: &[PathBuf]) -> Option<Result<Vec<PathBuf>>> {
    let streams: Vec<bool> = paths.iter().map(|p| is_stream_file(p)).collect();
    if !streams.iter().any(|s| *s) {
        return None;
    }
    if let Some(at) = streams.iter().position(|s| !s) {
        return Some(Err(eyre!(
            "{} is an Arrow IPC file among Arrow IPC streams. Open the streams or the files on their own.",
            paths[at].display()
        )));
    }
    Some(Ok(paths.to_vec()))
}

/// Convert the streams at `paths`, in order, into one IPC file in `temp_dir` (the
/// system temp directory when `None`), created and claimed through `writer` and
/// removed if the open stops or this fails. `read` counts the bytes of the streams
/// read so far.
///
/// One record batch is in memory at a time. Buffers compressed with LZ4 or ZSTD are
/// written out uncompressed, so the file maps and scans like any other.
pub(crate) fn convert(
    paths: &[PathBuf],
    temp_dir: Option<&Path>,
    writer: &Writer,
    read: &AtomicU64,
) -> Result<TempDownload> {
    let stopped = || eyre!("Converting the Arrow stream was stopped.");
    let Some((file, claim)) = writer.create(|| TempDownload::create(temp_dir, Some("arrow")))?
    else {
        return Err(stopped());
    };
    match write_file(paths, file.as_file(), &|| writer.stopped(), read) {
        Ok(true) => Ok(TempDownload::held(file, Some(claim))),
        unfinished => {
            // The file before the claim, so a sweep never finds it let go but there.
            drop(file);
            drop(claim);
            Err(unfinished.err().unwrap_or_else(stopped))
        }
    }
}

/// Write the batches of every stream at `paths` to `out` as one IPC file. `false`
/// when `stopped` said so first.
fn write_file(
    paths: &[PathBuf],
    out: &File,
    stopped: &dyn Fn() -> bool,
    read: &AtomicU64,
) -> Result<bool> {
    let columns = |schema: &polars_arrow::datatypes::ArrowSchema| {
        schema
            .iter_values()
            .map(|f| (f.name.clone(), f.dtype.clone()))
            .collect::<Vec<_>>()
    };
    let mut file: Option<(FileWriter<BufWriter<&File>>, &Path, Vec<_>)> = None;
    let mut before = 0u64;
    for path in paths {
        let unreadable = |e: &dyn std::fmt::Display| {
            eyre!("{} is not a readable Arrow IPC stream: {e}", path.display())
        };
        let source = File::open(path)?;
        let size = source.metadata()?.len();
        let mut reader = BufReader::with_capacity(
            1 << 20,
            Counting {
                inner: source,
                at: 0,
                before,
                read,
            },
        );
        // Polars panics on a column type it has not implemented, such as run-end
        // encoding: that is a stream it cannot read, not a crash.
        let metadata = crate::logging::catch_panic(|| read_stream_metadata(&mut reader))
            .map_err(|_| unreadable(&"it has a column type Polars cannot read"))?
            .map_err(|e| unreadable(&e))?;
        match &file {
            None => {
                let mut writer = FileWriter::try_new(
                    BufWriter::with_capacity(1 << 20, out),
                    Arc::new(metadata.schema.clone()),
                    Some(metadata.ipc_schema.fields.clone()),
                    WriteOptions { compression: None },
                )?;
                if let Some(custom) = &metadata.custom_schema_metadata {
                    writer.set_custom_schema_metadata(Arc::new(custom.clone()));
                }
                file = Some((writer, path, columns(&metadata.schema)));
            }
            Some((_, first, first_columns)) => {
                if columns(&metadata.schema) != *first_columns {
                    return Err(eyre!(
                        "{} has different columns from {}, so they cannot be read as one table.",
                        path.display(),
                        first.display()
                    ));
                }
            }
        }
        let (writer, _, _) = file.as_mut().expect("set just above");
        let mut batches = StreamReader::new(reader, metadata, None);
        loop {
            if stopped() {
                return Ok(false);
            }
            // Polars also panics on some malformed record batches, rather than erring.
            let Some(state) = crate::logging::catch_panic(|| batches.next())
                .map_err(|_| unreadable(&"a record batch in it is damaged"))?
            else {
                break;
            };
            match state.map_err(|e| unreadable(&e))? {
                StreamState::Some(batch) => writer.write(&batch, None)?,
                // The end of a stream written without its end-of-stream marker.
                StreamState::Waiting => break,
            }
        }
        before += size;
        read.store(before, Ordering::Relaxed);
    }
    let Some((mut writer, _, _)) = file else {
        return Err(eyre!("No Arrow IPC stream to read."));
    };
    writer.finish()?;
    let mut out = writer.into_inner();
    out.flush()?;
    Ok(true)
}

/// A stream being read, counting its bytes into the open's progress.
struct Counting<'a> {
    inner: File,
    at: u64,
    /// The bytes of the streams before this one.
    before: u64,
    read: &'a AtomicU64,
}

impl Read for Counting<'_> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.at += n as u64;
        self.read.store(self.before + self.at, Ordering::Relaxed);
        Ok(n)
    }
}

impl Seek for Counting<'_> {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.at = self.inner.seek(pos)?;
        Ok(self.at)
    }
}

#[cfg(test)]
mod tests {
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
    fn stream(df: &DataFrame, compression: Option<Compression>, legacy: bool) -> Vec<u8> {
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
        let file = convert(&paths, Some(out.path()), &writer(false), &read).unwrap();
        assert!(!is_stream_file(file.path()), "an IPC file now");
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
            error.contains("cut.arrow is not a readable Arrow IPC stream"),
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
            include_bytes!("../../../fuzz/corpus/ipc_stream_head/regression-run-end-encoded");
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
            error.contains("damaged.arrow is not a readable Arrow IPC stream"),
            "{error}"
        );
        assert!(std::fs::read_dir(out.path()).unwrap().next().is_none());
    }

    /// Streams mixed with IPC files are not one table; IPC files alone are not streams.
    #[test]
    fn streams_among_ipc_files() {
        let dir = tempfile::tempdir().unwrap();
        let streamed = dir.path().join("s.arrow");
        std::fs::write(&streamed, stream(&frame(0, 3), None, false)).unwrap();
        let file = dir.path().join("f.arrow");
        polars::io::ipc::IpcWriter::new(std::fs::File::create(&file).unwrap())
            .finish(&mut frame(0, 3))
            .unwrap();
        assert!(streams_among(std::slice::from_ref(&file)).is_none());
        assert_eq!(
            streams_among(std::slice::from_ref(&streamed))
                .unwrap()
                .unwrap(),
            std::slice::from_ref(&streamed)
        );
        let error = streams_among(&[streamed, file]).unwrap().unwrap_err();
        assert!(error.to_string().contains("f.arrow is an Arrow IPC file"));
    }
}

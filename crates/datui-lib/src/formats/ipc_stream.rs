//! Arrow IPC streams, the format Hugging Face `datasets` caches in: the IPC file format
//! without the `ARROW1` magic and footer, so Polars cannot scan it. An open converts the
//! stream (or a directory's stream shards) once into a temp IPC file and scans that; IPC
//! files among the shards are scanned in place ([`Part`]). One record batch is held at a
//! time: 0.2 GiB peak for a 3.0 GiB stream that read eagerly took 3.6 GiB.

use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use color_eyre::{Result, eyre::eyre};
use polars_arrow::io::ipc::format::ipc::planus::ReadAsRoot;
use polars_arrow::io::ipc::format::ipc::{MessageHeaderRef, MessageRef};
use polars_arrow::io::ipc::read::{StreamReader, StreamState, read_stream_metadata};
use polars_arrow::io::ipc::write::{FileWriter, WriteOptions};

use crate::cloud::download::TempDownload;
use crate::error_display::{FileError, user_message_from_io};
use crate::loading::unfinished::Writer;

/// What a stream's messages start with since Arrow 0.15. Older streams start with the
/// schema message's length.
const CONTINUATION: [u8; 4] = [0xff; 4];

/// The longest schema message read to tell a stream from other bytes. A schema with
/// thousands of columns and its metadata fits well inside.
const MAX_SCHEMA: usize = 16 << 20;

/// Whether `head` begins an Arrow IPC stream: a schema message, with or (older streams)
/// without the continuation marker. A whole message must be a schema message; a cut one
/// counts if marked. Fields are not parsed: Polars panics on unimplemented column types,
/// and this runs on any opened file.
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

/// Whether Arrow `paths` are converted: when the first is a stream (only it is opened).
/// A stream behind an IPC file is found by [`any_stream`] once the scan fails on it.
pub fn starts_with_stream(paths: &[PathBuf]) -> bool {
    paths.first().is_some_and(|p| is_stream_file(p))
}

/// Whether any of `paths` is a stream: asked once a scan of them as IPC files failed.
pub fn any_stream(paths: &[PathBuf]) -> bool {
    paths.iter().any(|p| is_stream_file(p))
}

/// Where one input's rows are read from once its streams are converted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Part {
    /// An IPC file, scanned where it is: a local path or an object's URL.
    InPlace(PathBuf),
    /// A stream, `source`, converted: `rows` rows of the converted file from `offset`.
    Converted {
        source: PathBuf,
        offset: u64,
        rows: u64,
    },
}

/// What a conversion wrote: the IPC file of the streams, and each input's place.
#[derive(Debug)]
pub(crate) struct Converted {
    pub file: TempDownload,
    pub parts: Vec<Part>,
}

/// Whether an Arrow file starts as an IPC file does, with its footer at the end.
pub(crate) fn is_ipc_file_head(head: &[u8]) -> bool {
    head.starts_with(b"ARROW1")
}

/// Convert the Arrow streams at `paths`, in order, into one IPC file in `temp_dir`
/// (system temp if `None`), claimed via `writer` and removed on stop or failure. IPC
/// files among them are scanned in place. `read` counts bytes looked at. One batch in
/// memory at a time; LZ4 and ZSTD buffers are written uncompressed so the file maps.
pub(crate) fn convert(
    paths: &[PathBuf],
    temp_dir: Option<&Path>,
    writer: &Writer,
    read: &AtomicU64,
) -> Result<Converted> {
    let mut merge = Merge::create(temp_dir, writer)?;
    let mut parts = Vec::with_capacity(paths.len());
    let mut before = 0;
    for path in paths {
        let mut source = File::open(path)?;
        let size = source.metadata()?.len();
        let mut head = Vec::with_capacity(6);
        (&mut source).take(6).read_to_end(&mut head)?;
        if is_ipc_file_head(&head) {
            parts.push(Part::InPlace(path.clone()));
        } else {
            source.seek(SeekFrom::Start(0))?;
            has_room(size, temp_dir)?;
            parts.push(merge.append(source, path, before, read)?);
        }
        before += size;
        read.store(before, Ordering::Relaxed);
    }
    Ok(Converted {
        file: merge.finish()?,
        parts,
    })
}

/// One IPC file being written from the batches of Arrow streams, appended one at a
/// time: what a conversion writes, and what a bucket's streams are read into as they
/// download. Dropped unfinished, the file goes.
pub(crate) struct Merge<'a> {
    /// The writer, the first input's name and its columns, once one is appended. Its
    /// handle on the file is let go before the file is removed.
    out: Option<(FileWriter<BufWriter<File>>, PathBuf, Columns)>,
    /// The file, then its claim: dropped in that order.
    file: tempfile::NamedTempFile,
    claim: crate::loading::unfinished::Claim,
    dir: PathBuf,
    writer: &'a Writer,
    /// The rows written so far.
    rows: u64,
}

type Columns = Vec<(
    polars::prelude::PlSmallStr,
    polars_arrow::datatypes::ArrowDataType,
)>;

fn columns(schema: &polars_arrow::datatypes::ArrowSchema) -> Columns {
    schema
        .iter_values()
        .map(|f| (f.name.clone(), f.dtype.clone()))
        .collect()
}

impl<'a> Merge<'a> {
    /// The empty file, in `temp_dir`, claimed through `writer`.
    pub(crate) fn create(temp_dir: Option<&Path>, writer: &'a Writer) -> Result<Self> {
        let Some((file, claim)) =
            writer.create(|| TempDownload::create(temp_dir, Some("arrow")))?
        else {
            return Err(stopped());
        };
        Ok(Self {
            file,
            claim,
            dir: temp_dir
                .map(Path::to_path_buf)
                .unwrap_or_else(std::env::temp_dir),
            writer,
            out: None,
            rows: 0,
        })
    }

    /// Append the batches of the stream `source` reads, which errors call `name`,
    /// counting its bytes into `read` after the `before` read ahead of it. Its place
    /// in the file is returned.
    pub(crate) fn append(
        &mut self,
        source: impl Read,
        name: &Path,
        before: u64,
        read: &AtomicU64,
    ) -> Result<Part> {
        let offset = self.rows;
        match self.batches(source, name, before, read) {
            Ok(true) => Ok(Part::Converted {
                source: name.to_path_buf(),
                offset,
                rows: self.rows - offset,
            }),
            Ok(false) => Err(stopped()),
            Err(e) => Err(out_of_room(e, &self.dir)),
        }
    }

    /// The finished file, held with its claim.
    pub(crate) fn finish(self) -> Result<TempDownload> {
        let Some((mut out, _, _)) = self.out else {
            return Err(eyre!("No Arrow IPC stream to read."));
        };
        let finished = out
            .finish()
            .map_err(color_eyre::Report::from)
            .and_then(|()| Ok(out.into_inner().flush()?));
        if let Err(e) = finished {
            return Err(out_of_room(e, &self.dir));
        }
        Ok(TempDownload::held(self.file, Some(self.claim)))
    }

    /// `false` when the open was stopped first.
    fn batches(
        &mut self,
        source: impl Read,
        name: &Path,
        before: u64,
        read: &AtomicU64,
    ) -> Result<bool> {
        let mut reader = Forward {
            inner: BufReader::with_capacity(
                1 << 20,
                Counting {
                    inner: source,
                    at: 0,
                    before,
                    read,
                },
            ),
            at: 0,
        };
        let unreadable = move |e: &dyn std::fmt::Display| -> color_eyre::Report {
            FileError::new(name, format!("not a readable Arrow IPC stream: {e}")).into()
        };
        // A read that failed under the stream (a download cut off) says so, not that
        // the stream is damaged.
        let failed = move |e: polars::prelude::PolarsError| match e {
            polars::prelude::PolarsError::IO { error, .. } => {
                FileError::new(name, user_message_from_io(&error, None)).into()
            }
            e => unreadable(&e),
        };
        // Polars panics on a column type it has not implemented, such as run-end
        // encoding: that is a stream it cannot read, not a crash.
        let metadata = crate::logging::catch_panic(|| read_stream_metadata(&mut reader))
            .map_err(|_| unreadable(&"it has a column type Polars cannot read"))?
            .map_err(failed)?;
        self.start(
            name,
            &metadata.schema,
            &metadata.ipc_schema.fields,
            metadata.custom_schema_metadata.as_ref(),
        )?;
        let mut batches = StreamReader::new(reader, metadata, None);
        let (out, _, _) = self.out.as_mut().expect("started just above");
        loop {
            if self.writer.stopped() {
                return Ok(false);
            }
            // Polars also panics on some malformed record batches, rather than erring.
            let next = crate::logging::catch_panic(|| batches.next())
                .map_err(|_| unreadable(&"a record batch in it is damaged"))?;
            let batch = match next {
                Some(Ok(StreamState::Some(batch))) => batch,
                // The end of a stream written without its end-of-stream marker.
                Some(Ok(StreamState::Waiting)) | None => break,
                Some(Err(e)) => return Err(failed(e)),
            };
            self.rows += batch.len() as u64;
            out.write(&batch, None)?;
        }
        Ok(true)
    }

    /// Start the file with the first input's schema, or check a later one has the same
    /// columns.
    fn start(
        &mut self,
        name: &Path,
        schema: &polars_arrow::datatypes::ArrowSchema,
        fields: &[polars_arrow::io::ipc::IpcField],
        custom: Option<&polars_arrow::datatypes::Metadata>,
    ) -> Result<()> {
        match &self.out {
            None => {
                let mut out = FileWriter::try_new(
                    BufWriter::with_capacity(1 << 20, self.file.as_file().try_clone()?),
                    Arc::new(schema.clone()),
                    Some(fields.to_vec()),
                    WriteOptions { compression: None },
                )?;
                if let Some(custom) = custom {
                    out.set_custom_schema_metadata(Arc::new(custom.clone()));
                }
                self.out = Some((out, name.to_path_buf(), columns(schema)));
            }
            Some((_, first, first_columns)) => {
                if columns(schema) != *first_columns {
                    return Err(eyre!(
                        "{} has different columns from {}, so they cannot be read as one table.",
                        name.display(),
                        first.display()
                    ));
                }
            }
        }
        Ok(())
    }
}

fn stopped() -> color_eyre::Report {
    eyre!("Converting the Arrow stream was stopped.")
}

/// What to do about a temp directory too small for the copy.
const ELSEWHERE: &str = "Choose another place with --temp-dir or the temp_dir setting.";

/// Whether `temp_dir` (the system temp directory when `None`) has room for a copy of
/// `needs` bytes of Arrow; see [`room`].
pub(crate) fn has_room(needs: u64, temp_dir: Option<&Path>) -> Result<()> {
    let dir = temp_dir
        .map(Path::to_path_buf)
        .unwrap_or_else(std::env::temp_dir);
    room(needs, crate::cloud::local_copy::free_space(&dir), &dir)
}

/// The copy is about the size of the streams, larger where their buffers are
/// compressed: refused before any of it is written where `dir` has less free.
fn room(needs: u64, free: Option<u64>, dir: &Path) -> Result<()> {
    match free {
        Some(free) if free < needs => Err(eyre!(
            "Converting the Arrow stream needs {} free in {}, which has {}. {ELSEWHERE}",
            crate::numfmt::bytes(needs),
            dir.display(),
            crate::numfmt::bytes(free),
        )),
        _ => Ok(()),
    }
}

/// A write that ran out of space, said so with where and what to do.
fn out_of_room(error: color_eyre::Report, dir: &Path) -> color_eyre::Report {
    let full = error.chain().any(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .is_some_and(|e| e.kind() == std::io::ErrorKind::StorageFull)
    });
    if full {
        eyre!(
            "{} ran out of space for the converted Arrow stream, which is written uncompressed. {ELSEWHERE}",
            dir.display()
        )
    } else {
        error
    }
}

/// A stream being read, counting its bytes into the open's progress.
struct Counting<'a, R> {
    inner: R,
    at: u64,
    /// The bytes of the inputs before this one.
    before: u64,
    read: &'a AtomicU64,
}

impl<R: Read> Read for Counting<'_, R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.at += n as u64;
        self.read.store(self.before + self.at, Ordering::Relaxed);
        Ok(n)
    }
}

/// A stream read front to back, by the stream reader that seeks: only forward, over
/// the padding after a message, so a download can be read as it arrives.
struct Forward<R> {
    inner: R,
    at: u64,
}

impl<R: BufRead> Read for Forward<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.at += n as u64;
        Ok(n)
    }
}

impl<R: BufRead> Seek for Forward<R> {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let to = match pos {
            SeekFrom::Start(to) => Some(to),
            SeekFrom::Current(by) => self.at.checked_add_signed(by),
            SeekFrom::End(_) => None,
        };
        let skip = to
            .and_then(|to| to.checked_sub(self.at))
            .ok_or_else(|| std::io::Error::other("an Arrow stream is read front to back"))?;
        let skipped = std::io::copy(&mut (&mut self.inner).take(skip), &mut std::io::sink())?;
        self.at += skipped;
        Ok(self.at)
    }
}

#[cfg(test)]
pub(crate) mod tests;

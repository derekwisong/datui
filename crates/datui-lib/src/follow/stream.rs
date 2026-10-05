//! Following an Arrow IPC stream (`.arrows`, or one piped in): its record batches are
//! counted as their messages complete, as a delimited file's records are, and read
//! straight from the stream by a scan of datui's own, since Polars scans only IPC
//! files. A page reads the batches from the mark before it; a filter or a count reads
//! the stream a batch at a time, keeping only what it asked for.

use std::fs::File;
use std::io::{BufReader, Cursor, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use polars::prelude::*;
use polars_arrow::datatypes::ArrowSchema;
use polars_arrow::io::ipc::format::ipc::planus::ReadAsRoot;
use polars_arrow::io::ipc::format::ipc::{MessageHeaderRef, MessageRef};
use polars_arrow::io::ipc::read::{StreamReader, StreamState, read_stream_metadata};

/// What a stream's messages start with since Arrow 0.15; an older stream's start with
/// their length.
const CONTINUATION: [u8; 4] = [0xff; 4];

/// The longest message header believed: a longer one is damage, not a stream.
const LONGEST_HEADER: usize = 64 << 20;

/// The name a stream's scan carries in a plan.
const SCAN_NAME: &str = "ARROW STREAM";

/// One whole message of a stream.
#[derive(Debug, PartialEq)]
pub(super) enum Message {
    Schema,
    Batch {
        rows: u64,
    },
    Dictionary,
    /// The end-of-stream marker: nothing follows.
    End,
    Other,
}

/// The next message of the stream `reader` holds, of which `left` bytes are written:
/// what it is and how many bytes it takes, its body included. `None` when it is not
/// all there yet. Only its header is read; the body is skipped.
pub(super) fn next_message<R: Read>(
    reader: &mut BufReader<R>,
    left: u64,
) -> std::io::Result<Option<(Message, u64)>>
where
    BufReader<R>: Seek,
{
    let damaged = |what: &str| std::io::Error::new(std::io::ErrorKind::InvalidData, what);
    if left < 4 {
        return Ok(None);
    }
    let mut word = [0u8; 4];
    reader.read_exact(&mut word)?;
    let mut prefix = 4u64;
    if word == CONTINUATION {
        if left < 8 {
            return Ok(None);
        }
        reader.read_exact(&mut word)?;
        prefix = 8;
    }
    let size = i32::from_le_bytes(word);
    if size == 0 {
        return Ok(Some((Message::End, prefix)));
    }
    let size = usize::try_from(size)
        .ok()
        .filter(|&size| size <= LONGEST_HEADER)
        .ok_or_else(|| damaged("an Arrow stream message has an impossible length"))?;
    if left < prefix + size as u64 {
        return Ok(None);
    }
    let mut header = vec![0u8; size];
    reader.read_exact(&mut header)?;
    let message = MessageRef::read_as_root(&header)
        .map_err(|_| damaged("an Arrow stream message is damaged"))?;
    let body = message
        .body_length()
        .ok()
        .and_then(|b| u64::try_from(b).ok())
        .ok_or_else(|| damaged("an Arrow stream message has an impossible length"))?;
    let whole = prefix + size as u64 + body;
    if left < whole {
        return Ok(None);
    }
    reader.seek_relative(body as i64)?;
    let kind = match message.header() {
        Ok(Some(MessageHeaderRef::Schema(_))) => Message::Schema,
        Ok(Some(MessageHeaderRef::RecordBatch(batch))) => Message::Batch {
            rows: batch
                .length()
                .ok()
                .and_then(|n| u64::try_from(n).ok())
                .ok_or_else(|| damaged("an Arrow record batch has an impossible length"))?,
        },
        Ok(Some(MessageHeaderRef::DictionaryBatch(_))) => Message::Dictionary,
        _ => Message::Other,
    };
    Ok(Some((kind, whole)))
}

/// Whether the file at `path` starts with a whole Arrow stream schema message: enough
/// of a stream arriving on standard input to show.
pub(super) fn begins_with_schema(path: &Path) -> bool {
    let Ok(file) = File::open(path) else {
        return false;
    };
    let Ok(len) = file.metadata().map(|m| m.len()) else {
        return false;
    };
    let mut reader = BufReader::new(file);
    matches!(
        next_message(&mut reader, len),
        Ok(Some((Message::Schema, _)))
    )
}

/// A stream's schema: the message it starts with, which a read from its middle is
/// given first, and the columns it names.
pub(crate) struct StreamSchema {
    message: Vec<u8>,
    arrow: ArrowSchema,
    schema: SchemaRef,
}

/// Read the schema at the start of the stream `file`. Refused for a stream with
/// dictionary-encoded columns: a read from its middle would lack the dictionaries.
fn schema_of(file: &mut File) -> Result<StreamSchema, String> {
    let unreadable = |e: &dyn std::fmt::Display| format!("Not a readable Arrow IPC stream: {e}");
    file.seek(SeekFrom::Start(0)).map_err(|e| unreadable(&e))?;
    let mut reader = BufReader::new(&mut *file);
    // Polars panics on a column type it has not implemented, as the conversion knows.
    let metadata = crate::logging::catch_panic(|| read_stream_metadata(&mut reader))
        .map_err(|_| unreadable(&"it has a column type Polars cannot read"))?
        .map_err(|e| unreadable(&e))?;
    if has_dictionary(&metadata.ipc_schema.fields) {
        return Err(
            "An Arrow stream with dictionary-encoded columns cannot be followed as it grows."
                .to_string(),
        );
    }
    let length = reader.stream_position().map_err(|e| unreadable(&e))?;
    drop(reader);
    let mut message = Vec::new();
    file.seek(SeekFrom::Start(0)).map_err(|e| unreadable(&e))?;
    (&mut *file)
        .take(length)
        .read_to_end(&mut message)
        .map_err(|e| unreadable(&e))?;
    let schema = Arc::new(Schema::from_arrow_schema(&metadata.schema));
    Ok(StreamSchema {
        message,
        arrow: metadata.schema,
        schema,
    })
}

fn has_dictionary(fields: &[polars_arrow::io::ipc::IpcField]) -> bool {
    fields
        .iter()
        .any(|field| field.dictionary_id.is_some() || has_dictionary(&field.fields))
}

/// The scan of a followed stream: its batches from the start, as many rows as the
/// bound asks for.
pub(crate) struct StreamScan {
    path: PathBuf,
    schema: Arc<StreamSchema>,
    /// The handle a deleted stream is read through.
    held: Option<Arc<File>>,
}

/// The scan of the Arrow IPC stream at `path`, for following it.
pub(crate) fn scan(path: &Path) -> Result<LazyFrame, String> {
    let mut file = File::open(path).map_err(|e| format!("Could not open the stream: {e}"))?;
    let schema = Arc::new(schema_of(&mut file)?);
    let scan = StreamScan {
        path: path.to_path_buf(),
        schema: schema.clone(),
        held: None,
    };
    LazyFrame::anonymous_scan(
        Arc::new(scan),
        ScanArgsAnonymous {
            schema: Some(schema.schema.clone()),
            name: SCAN_NAME,
            ..Default::default()
        },
    )
    .map_err(|e| format!("Could not read the stream: {e}"))
}

impl StreamScan {
    /// The stream scan `scan` is, if it is one.
    pub(super) fn in_plan(scan: &polars::lazy::dsl::DslPlan) -> Option<&StreamScan> {
        use polars::lazy::dsl::{DslPlan, FileScanDsl};
        let DslPlan::Scan { scan_type, .. } = scan else {
            return None;
        };
        let FileScanDsl::Anonymous { function, .. } = &**scan_type else {
            return None;
        };
        function.as_any().downcast_ref::<StreamScan>()
    }

    /// The stream scan `scan` is, when it reads the file at `path` by its name.
    pub(super) fn of<'a>(
        scan: &'a polars::lazy::dsl::DslPlan,
        path: &str,
    ) -> Option<&'a StreamScan> {
        Self::in_plan(scan)
            .filter(|s| s.held.is_none() && super::same_file(&s.path.to_string_lossy(), path))
    }

    pub(super) fn schema(&self) -> &Arc<StreamSchema> {
        &self.schema
    }

    /// This scan, reading through `file`: the stream was deleted.
    pub(super) fn held(&self, file: &File) -> Option<StreamScan> {
        Some(StreamScan {
            path: self.path.clone(),
            schema: self.schema.clone(),
            held: Some(Arc::new(file.try_clone().ok()?)),
        })
    }
}

impl AnonymousScan for StreamScan {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self, _infer_schema_length: Option<usize>) -> PolarsResult<SchemaRef> {
        Ok(self.schema.schema.clone())
    }

    fn allows_predicate_pushdown(&self) -> bool {
        true
    }

    fn allows_projection_pushdown(&self) -> bool {
        true
    }

    fn scan(&self, mut args: AnonymousScanArgs) -> PolarsResult<DataFrame> {
        args.predicate = crate::pushdown::evaluable(args.predicate.take());
        let file = match &self.held {
            Some(held) => held.try_clone()?,
            None => File::open(&self.path)?,
        };
        let mut file = file;
        file.seek(SeekFrom::Start(0))?;
        decode(
            BufReader::with_capacity(1 << 20, file),
            &self.schema,
            args.n_rows,
            args.with_columns.as_deref(),
            args.predicate.as_ref(),
        )
    }
}

/// The rows of `bytes`, a run of a stream's batch messages: at most `n_rows`, read
/// after `schema`'s message as a stream of their own.
pub(super) fn decode_run(
    bytes: Vec<u8>,
    schema: &StreamSchema,
    n_rows: usize,
) -> PolarsResult<DataFrame> {
    let mut whole = schema.message.clone();
    whole.extend(bytes);
    decode(Cursor::new(whole), schema, Some(n_rows), None, None)
}

/// The rows of the stream `reader` holds from its schema message on, a batch at a time:
/// at most `n_rows`, only the columns named, and only the rows `predicate` keeps. A
/// message still being written ends the rows.
fn decode(
    mut reader: impl Read + Seek,
    schema: &StreamSchema,
    n_rows: Option<usize>,
    columns: Option<&[PlSmallStr]>,
    predicate: Option<&Expr>,
) -> PolarsResult<DataFrame> {
    let metadata = read_stream_metadata(&mut reader)?;
    let projection: Option<Vec<usize>> = columns.map(|columns| {
        columns
            .iter()
            .filter_map(|name| schema.arrow.index_of(name.as_str()))
            .collect()
    });
    let fields: Vec<_> = match &projection {
        Some(at) => at
            .iter()
            .filter_map(|&i| schema.arrow.get_at_index(i).map(|(_, f)| f))
            .collect(),
        None => schema.arrow.iter_values().collect(),
    };
    let out_schema: Schema = fields
        .iter()
        .map(|f| (f.name.clone(), DataType::from_arrow_field(f)))
        .collect();
    let mut out = DataFrame::empty_with_schema(&out_schema);
    let mut batches = StreamReader::new(reader, metadata, projection);
    let wanted = n_rows.unwrap_or(usize::MAX);
    while out.height() < wanted {
        let batch = match batches.next() {
            Some(Ok(StreamState::Some(batch))) => batch,
            Some(Ok(StreamState::Waiting)) | None => break,
            Some(Err(PolarsError::IO { error, .. }))
                if error.kind() == std::io::ErrorKind::UnexpectedEof =>
            {
                break;
            }
            Some(Err(e)) => return Err(e),
        };
        let height = batch.len();
        let columns = fields
            .iter()
            .zip(batch.into_arrays())
            .map(|(field, array)| Series::try_from((*field, array)).map(Column::from))
            .collect::<PolarsResult<Vec<_>>>()?;
        let mut df = DataFrame::new(height, columns)?;
        if let Some(predicate) = predicate {
            df = df.lazy().filter(predicate.clone()).collect()?;
        }
        let room = wanted - out.height();
        if df.height() > room {
            df = df.slice(0, room);
        }
        out.vstack_mut(&df)?;
    }
    out.rechunk_mut();
    Ok(out)
}

/// `df` as an Arrow IPC stream's messages, `rows` rows to a batch: its schema
/// message, then each batch's, with no end marker. For tests that write a stream a
/// batch at a time.
#[doc(hidden)]
pub fn stream_messages(df: &DataFrame, rows: usize) -> (Vec<u8>, Vec<Vec<u8>>) {
    use polars_arrow::io::ipc::write::{StreamWriter, WriteOptions};
    let mut out = Vec::new();
    let mut writer = StreamWriter::new(&mut out, WriteOptions { compression: None });
    writer
        .start(&df.schema().to_arrow(CompatLevel::newest()), None)
        .expect("a schema writes to memory");
    for at in (0..df.height()).step_by(rows.max(1)) {
        for batch in df
            .slice(at as i64, rows)
            .iter_chunks(CompatLevel::newest(), false)
        {
            writer
                .write(&batch, None)
                .expect("a batch writes to memory");
        }
    }
    drop(writer);
    let mut reader = BufReader::new(Cursor::new(&out));
    let mut messages = Vec::new();
    let mut at = 0u64;
    while let Ok(Some((_, size))) = next_message(&mut reader, out.len() as u64 - at) {
        messages.push(out[at as usize..(at + size) as usize].to_vec());
        at += size;
    }
    let schema = messages.remove(0);
    (schema, messages)
}

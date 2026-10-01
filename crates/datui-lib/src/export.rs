//! Writing the view to an export file.
//!
//! [`run`] takes the view's plan to a committed file. Uncompressed CSV and
//! Parquet stream: Polars' sink encodes the plan batch by batch into the
//! [`OutputFile`], and no frame of the whole output is built. Everything else —
//! compressed CSV, JSON, NDJSON, IPC, Avro, any export with the streaming
//! engine off, and builds without it — collects the plan and [`encode`]s the
//! frame. Either way the destination changes only at the commit, after the
//! last byte, the encoder's finish and the final flush have succeeded.
//!
//! Streaming bounds what the export holds, not what the plan needs: a sort, a
//! group-by or a join still gathers its input before its first row leaves.

use std::io::{self, BufWriter, Write};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use color_eyre::Result;
use polars::prelude::{
    CsvWriter, DataFrame, IpcWriter, JsonFormat, JsonWriter, LazyFrame, ParquetWriter, SerWriter,
};

use crate::CompressionFormat;
use crate::export_modal::ExportFormat;
use crate::output_file::{OutputFile, Overwrite};

#[derive(Debug, Clone)]
pub struct ExportOptions {
    pub csv_delimiter: u8,
    pub csv_include_header: bool,
    /// Add a column naming the file each row came from, so a cell that is absent
    /// rather than null can still be told apart once the data has left datui.
    pub source_file: bool,
    pub csv_compression: Option<CompressionFormat>,
    pub json_compression: Option<CompressionFormat>,
    pub ndjson_compression: Option<CompressionFormat>,
}

impl ExportOptions {
    /// The compression chosen for `format`; None for the formats without one.
    pub fn compression(&self, format: ExportFormat) -> Option<CompressionFormat> {
        match format {
            ExportFormat::Csv => self.csv_compression,
            ExportFormat::Json => self.json_compression,
            ExportFormat::Ndjson => self.ndjson_compression,
            ExportFormat::Parquet | ExportFormat::Ipc | ExportFormat::Avro => None,
        }
    }
}

/// One export: the file, its form, and whether it may replace a file there.
#[derive(Debug, Clone)]
pub struct ExportRequest {
    pub path: PathBuf,
    pub format: ExportFormat,
    pub options: ExportOptions,
    pub overwrite: Overwrite,
}

/// How an export's rows reach its file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Route {
    /// Sunk from the plan in batches by the streaming engine.
    Streamed,
    /// Collected into one frame, then encoded.
    Collected,
}

impl ExportRequest {
    /// The route this export takes. `polars_streaming` is the user's engine
    /// setting: with it off, nothing streams.
    pub fn route(&self, polars_streaming: bool) -> Route {
        let sinkable = matches!(self.format, ExportFormat::Csv | ExportFormat::Parquet)
            && self.options.compression(self.format).is_none();
        if cfg!(feature = "streaming") && polars_streaming && sinkable {
            Route::Streamed
        } else {
            Route::Collected
        }
    }
}

/// How often a running export reports the bytes it has written.
const PROGRESS_EVERY: Duration = Duration::from_millis(250);

/// Run an export: plan `lf` as the format needs ([`ExportFormat::prepare`]),
/// write it to the request's path by its [`Route`], and commit. `written` hears
/// the bytes written so far: 0 as the write starts, then a few times a second.
/// The destination changes only if every byte was written.
pub fn run(
    lf: LazyFrame,
    request: &ExportRequest,
    polars_streaming: bool,
    mut written: impl FnMut(u64) + Send + 'static,
) -> Result<()> {
    let lf = request.format.prepare(lf)?;
    // Before the plan runs, so a destination that cannot be written fails first.
    let mut out = OutputFile::create(&request.path, request.overwrite)?;
    match request.route(polars_streaming) {
        #[cfg(feature = "streaming")]
        Route::Streamed => {
            written(0);
            let file = Counted::new(out.file().try_clone()?, written);
            sink(lf, request.format, &request.options, file.into_writable())?;
        }
        _ => {
            let mut df = crate::statistics::collect_lazy(lf, polars_streaming)?;
            written(0);
            let file = Counted::new(out.file(), written);
            encode(&mut df, request.format, &request.options, file)?;
        }
    }
    out.commit()?;
    Ok(())
}

/// Sink `lf` into `writable` with the streaming engine. The sink writes,
/// finishes and closes the writable before this returns; a failure on the way —
/// the plan's, the encoder's, a write's or the close's — is its error.
#[cfg(feature = "streaming")]
fn sink(
    lf: LazyFrame,
    format: ExportFormat,
    options: &ExportOptions,
    writable: polars::io::utils::file::Writable,
) -> Result<()> {
    use polars::prelude::{
        CompatLevel, CsvWriterOptions, Engine, FileWriteFormat, ParquetWriteOptions,
        SerializeOptions, SinkDestination, SinkTarget, SpecialEq, UnifiedSinkArgs,
    };
    use std::sync::{Arc, Mutex};

    // What the frame writers of the collected route write.
    let file_format = match format {
        ExportFormat::Csv => FileWriteFormat::Csv(CsvWriterOptions {
            include_header: options.csv_include_header,
            serialize_options: Arc::new(SerializeOptions {
                separator: options.csv_delimiter,
                ..SerializeOptions::default()
            }),
            ..CsvWriterOptions::default()
        }),
        // `ParquetWriter` writes the newest Arrow types (string views); the
        // sink's default is the oldest.
        ExportFormat::Parquet => FileWriteFormat::Parquet(Arc::new(ParquetWriteOptions {
            compat_level: Some(CompatLevel::newest()),
            ..ParquetWriteOptions::default()
        })),
        other => unreachable!("{other:?} does not stream"),
    };
    let target = SinkTarget::Dyn(SpecialEq::new(Arc::new(Mutex::new(Some(writable)))));
    lf.sink(
        SinkDestination::File { target },
        file_format,
        UnifiedSinkArgs::default(),
    )?
    .collect_with_engine(Engine::Streaming)?;
    Ok(())
}

/// A writer that counts what passes through it and reports the count, at most
/// every [`PROGRESS_EVERY`].
struct Counted<W, F> {
    inner: W,
    bytes: u64,
    reported: Instant,
    report: F,
}

impl<W: Write, F: FnMut(u64)> Counted<W, F> {
    fn new(inner: W, report: F) -> Self {
        Self {
            inner,
            bytes: 0,
            reported: Instant::now(),
            report,
        }
    }
}

impl<W: Write, F: FnMut(u64)> Write for Counted<W, F> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.bytes += n as u64;
        if self.reported.elapsed() >= PROGRESS_EVERY {
            self.reported = Instant::now();
            (self.report)(self.bytes);
        }
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

/// The sink's handle on the temporary file, counted. Closing flushes, which a
/// file needs nothing for: the sync is [`OutputFile::commit`]'s, and the
/// descriptor closes when the handle drops.
#[cfg(feature = "streaming")]
impl<F: FnMut(u64) + Send + 'static> Counted<std::fs::File, F> {
    fn into_writable(self) -> polars::io::utils::file::Writable {
        polars::io::utils::file::Writable::Dyn(Box::new(self))
    }
}

#[cfg(feature = "streaming")]
impl<F: FnMut(u64)> polars::io::utils::file::WritableTrait for Counted<std::fs::File, F> {
    fn close(&mut self) -> io::Result<()> {
        self.inner.flush()
    }

    fn sync_all(&self) -> io::Result<()> {
        self.inner.sync_all()
    }

    fn sync_data(&self) -> io::Result<()> {
        self.inner.sync_data()
    }
}

/// Encode `df` into `sink` and finish: every buffer is drained, the encoder's
/// last block and trailer written and `sink` flushed before this returns, so a
/// failure in any of them is an error here rather than one lost when a writer
/// is dropped.
pub fn encode<W: Write>(
    df: &mut DataFrame,
    format: ExportFormat,
    options: &ExportOptions,
    sink: W,
) -> Result<()> {
    // The buffer sits in front of the encoder, which batches its own output.
    let mut sink = match options.compression(format) {
        None => {
            let mut buffered = BufWriter::new(sink);
            serialize(df, format, options, &mut buffered)?;
            buffered
                .into_inner()
                .map_err(io::IntoInnerError::into_error)?
        }
        Some(compression) => {
            let mut buffered = BufWriter::new(Encoder::new(compression, sink)?);
            serialize(df, format, options, &mut buffered)?;
            let encoder = buffered
                .into_inner()
                .map_err(io::IntoInnerError::into_error)?;
            encoder.finish()?
        }
    };
    sink.flush()?;
    Ok(())
}

fn serialize(
    df: &mut DataFrame,
    format: ExportFormat,
    options: &ExportOptions,
    out: &mut impl Write,
) -> Result<()> {
    match format {
        ExportFormat::Csv => CsvWriter::new(out)
            .with_separator(options.csv_delimiter)
            .include_header(options.csv_include_header)
            .finish(df)?,
        ExportFormat::Parquet => {
            ParquetWriter::new(out).finish(df)?;
        }
        ExportFormat::Json => JsonWriter::new(out)
            .with_json_format(JsonFormat::Json)
            .finish(df)?,
        ExportFormat::Ndjson => JsonWriter::new(out)
            .with_json_format(JsonFormat::JsonLines)
            .finish(df)?,
        ExportFormat::Ipc => IpcWriter::new(out).finish(df)?,
        ExportFormat::Avro => crate::avro_types::write(df, out)?,
    }
    Ok(())
}

/// A compression encoder owned as its concrete type, so its `finish` — which
/// writes the final block and trailer — is called and its error kept. Boxed as
/// `dyn Write`, the finish ran in a drop that discards errors.
enum Encoder<W: Write> {
    Gzip(flate2::write::GzEncoder<W>),
    Zstd(zstd::Encoder<'static, W>),
    Bzip2(bzip2::write::BzEncoder<W>),
    Xz(xz2::write::XzEncoder<W>),
}

impl<W: Write> Encoder<W> {
    fn new(compression: CompressionFormat, out: W) -> io::Result<Self> {
        Ok(match compression {
            CompressionFormat::Gzip => Self::Gzip(flate2::write::GzEncoder::new(
                out,
                flate2::Compression::default(),
            )),
            CompressionFormat::Zstd => Self::Zstd(zstd::Encoder::new(out, 0)?),
            CompressionFormat::Bzip2 => Self::Bzip2(bzip2::write::BzEncoder::new(
                out,
                bzip2::Compression::default(),
            )),
            CompressionFormat::Xz => Self::Xz(xz2::write::XzEncoder::new(out, 6)),
        })
    }

    fn finish(self) -> io::Result<W> {
        match self {
            Self::Gzip(e) => e.finish(),
            Self::Zstd(e) => e.finish(),
            Self::Bzip2(e) => e.finish(),
            Self::Xz(e) => e.finish(),
        }
    }
}

impl<W: Write> Write for Encoder<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Self::Gzip(e) => e.write(buf),
            Self::Zstd(e) => e.write(buf),
            Self::Bzip2(e) => e.write(buf),
            Self::Xz(e) => e.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Gzip(e) => e.flush(),
            Self::Zstd(e) => e.flush(),
            Self::Bzip2(e) => e.flush(),
            Self::Xz(e) => e.flush(),
        }
    }
}

#[cfg(test)]
mod tests {
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
            ExportFormat::Csv => options.csv_compression = compression,
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
            ExportFormat::Csv => CsvReader::new(cursor).finish(),
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

    /// Uncompressed CSV and Parquet stream when the engine is on; every other
    /// format and compression, and every export with it off, is collected.
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
                    && matches!(format, ExportFormat::Csv | ExportFormat::Parquet);
                assert_eq!(
                    request.route(streaming) == Route::Streamed,
                    streams,
                    "{format:?} {compression:?} streaming={streaming}"
                );
            }
        }
    }

    /// A view with what a CSV has to get right: nulls, quotes, separators and
    /// line breaks inside text, floats, dates, times, booleans, and the list and
    /// binary columns [`ExportFormat::prepare`] turns into text.
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
        )
        .unwrap();
        df.apply("day", |c| c.cast(&DataType::Date).unwrap())
            .unwrap();
        df.apply("at", |c| {
            c.cast(&DataType::Datetime(TimeUnit::Milliseconds, None))
                .unwrap()
        })
        .unwrap();
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
    /// columns reordered, and filtered to nothing.
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
                    .select([col("x"), col("tags"), col("text"), col("id"), col("at")]),
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
            }
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
}

//! Writing the view to an export file. [`run`] takes the plan to a committed file:
//! uncompressed CSV and Parquet stream through Polars' sink into the [`OutputFile`];
//! everything else (compressed CSV, JSON, NDJSON, IPC, Avro, or no streaming engine)
//! collects and [`encode`]s. The destination changes only at commit, after the last
//! byte, encoder finish and flush succeed. Streaming bounds the export, not the plan: a
//! sort, group-by or join still gathers its input first.

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
            ExportFormat::Csv | ExportFormat::Tsv | ExportFormat::Psv => self.csv_compression,
            ExportFormat::Json => self.json_compression,
            ExportFormat::Ndjson => self.ndjson_compression,
            ExportFormat::Parquet | ExportFormat::Ipc | ExportFormat::Avro => None,
        }
    }

    /// The delimiter `format` writes: a preset's own, else the one chosen for CSV.
    pub fn delimiter(&self, format: ExportFormat) -> u8 {
        format.preset_delimiter().unwrap_or(self.csv_delimiter)
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
        let sinkable = (self.format.is_delimited() || self.format == ExportFormat::Parquet)
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

/// Run an export: plan `lf` for the format ([`ExportFormat::prepare`]), write by its
/// [`Route`], commit. `written` hears bytes so far (0 at start, then a few times a
/// second). The destination changes only if everything was written.
pub fn run(
    lf: LazyFrame,
    request: &ExportRequest,
    polars_streaming: bool,
    mut written: impl FnMut(u64) + Send + 'static,
) -> Result<()> {
    let lf = request.format.prepare(lf)?;
    let polars_streaming = crate::statistics::may_stream(&lf, polars_streaming);
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
        ExportFormat::Csv | ExportFormat::Tsv | ExportFormat::Psv => {
            FileWriteFormat::Csv(CsvWriterOptions {
                include_header: options.csv_include_header,
                serialize_options: Arc::new(SerializeOptions {
                    separator: options.delimiter(format),
                    ..SerializeOptions::default()
                }),
                ..CsvWriterOptions::default()
            })
        }
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

/// Encode `df` into `sink` and finish (drain buffers, write the last block and trailer,
/// flush), so failures surface here rather than at drop.
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
        ExportFormat::Csv | ExportFormat::Tsv | ExportFormat::Psv => CsvWriter::new(out)
            .with_separator(options.delimiter(format))
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
mod tests;

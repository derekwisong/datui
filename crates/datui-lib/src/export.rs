//! Writing a collected frame to an export file.
//!
//! [`write`] encodes into an [`OutputFile`] and commits it only after the
//! serializer, the compression encoder's finish and the last flush have all
//! succeeded, so an error at any of them reaches the app and leaves the
//! destination as it was. [`encode`] is that sequence over any writer.

use std::io::{self, BufWriter, Write};
use std::path::PathBuf;

use color_eyre::Result;
use polars::prelude::{
    CsvWriter, DataFrame, IpcWriter, JsonFormat, JsonWriter, ParquetWriter, SerWriter,
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

/// Write `df`, already prepared by [`ExportFormat::prepare`], to the request's
/// path. The destination changes only if every byte was written.
pub fn write(df: &mut DataFrame, request: &ExportRequest) -> Result<()> {
    let mut out = OutputFile::create(&request.path, request.overwrite)?;
    encode(df, request.format, &request.options, out.file())?;
    out.commit()?;
    Ok(())
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

    /// Through the file path the app takes, over a file agreed to be replaced.
    #[test]
    fn every_format_and_compression_round_trips() {
        let expected = frame();
        let dir = tempfile::tempdir().unwrap();
        for (format, compression) in combinations() {
            let path = dir.path().join("out");
            std::fs::write(&path, b"old").unwrap();
            let request = ExportRequest {
                options: options(format, compression),
                ..request(&path, format, Overwrite::Replace)
            };
            write(&mut frame(), &request).unwrap();
            assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
            let bytes = decompress(std::fs::read(&path).unwrap(), compression);
            let back = read_back(bytes, format);
            assert_eq!(back.shape(), expected.shape(), "{format:?} {compression:?}");
            assert_eq!(
                back.column("id").unwrap().cast(&DataType::Int64).unwrap(),
                *expected.column("id").unwrap(),
                "{format:?} {compression:?}"
            );
            assert_eq!(
                back.column("score").unwrap().null_count(),
                expected.column("score").unwrap().null_count(),
                "{format:?} {compression:?}"
            );
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

    /// A serializer failure over an approved overwrite leaves the old file and
    /// no temporary one; over a new file, no file at all.
    #[test]
    fn a_failed_export_keeps_the_destination() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.csv");
        std::fs::write(&path, b"old").unwrap();
        let err = write(
            &mut nested(),
            &request(&path, ExportFormat::Csv, Overwrite::Replace),
        );
        assert!(err.is_err());
        assert_eq!(std::fs::read(&path).unwrap(), b"old");
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);

        let fresh = dir.path().join("new.csv");
        assert!(
            write(
                &mut nested(),
                &request(&fresh, ExportFormat::Csv, Overwrite::Forbid)
            )
            .is_err()
        );
        assert!(!fresh.exists(), "no partial file");
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[test]
    fn a_written_export_replaces_the_destination() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.parquet");
        std::fs::write(&path, b"old").unwrap();
        write(
            &mut frame(),
            &request(&path, ExportFormat::Parquet, Overwrite::Replace),
        )
        .unwrap();
        let back = read_back(std::fs::read(&path).unwrap(), ExportFormat::Parquet);
        assert_eq!(back.shape(), frame().shape());
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
    }
}
